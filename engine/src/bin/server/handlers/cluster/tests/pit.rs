use super::*;

// ---- cooperative cancellation (ADR-099, cluster mode) --------------------------

#[tokio::test]
async fn cluster_search_explicit_zero_timeout_cancels_and_408s() {
    let state = test_state(&[(1, "michael jordan".to_string())]);

    // An explicit timeout_ms arms the per-title cooperative check in
    // percolate_blocking; 0ms is expired before the first title evaluates, so the
    // request 408s deterministically and the cancellation is RECORDED (the counter
    // lives in the blocking closure, so it counts even after the 408 went out).
    let (code, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({
                "documents": [{"title": "michael jordan rookie"}, {"title": "some other"}],
                "include_source": false,
                "timeout_ms": 0,
            }),
        ),
    )
    .await;
    assert_eq!(code, StatusCode::REQUEST_TIMEOUT, "got body: {body}");

    for _ in 0..200 {
        if state
            .prom
            .match_cancellations_total
            .with_label_values(&["search"])
            .get()
            >= 1
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        state
            .prom
            .match_cancellations_total
            .with_label_values(&["search"])
            .get()
            >= 1,
        "the cancelled cluster percolate must record that its work stopped"
    );

    // A shard failure is a 502, never masked by cancellation: the un-armed default
    // path still serves fine (sanity that the seam did not disturb normal serving).
    let (ok_code, ok_body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({
                "document": {"title": "michael jordan rookie"},
                "include_source": false,
            }),
        ),
    )
    .await;
    assert_eq!(ok_code, StatusCode::OK, "unarmed serving intact: {ok_body}");
    assert_eq!(ok_body["hits"]["total"], 1);
}

/// ADR-113 over the coordinator HTTP surface: open a PIT, page a ranked search
/// to exhaustion following `next_cursor` (concat ≡ the one-shot over the same
/// PIT, totals page-invariant), then a `resize` stales the cursor as the one
/// deliberate read-surface 409 — and an explicit DELETE releases the registry.
#[tokio::test]
async fn cluster_v2_pit_pages_concatenate_and_stale_after_resize() {
    let queries: Vec<(u64, String)> = (1..=20).map(|i| (i, "topps chrome".to_string())).collect();
    let tags: Vec<Vec<(String, String)>> = queries
        .iter()
        .map(|(id, _)| vec![("priority".to_string(), ((id % 6) * 10).to_string())])
        .collect();
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build_with_tags(
        Normalizer::default_vocab().expect("vocab"),
        &cfg,
        &queries,
        &tags,
    )
    .expect("tagged cluster");
    let state = state_from_cluster(cluster);

    let (status, opened) = send(&state, req("POST", "/v2/_pit", &serde_json::json!({}))).await;
    assert_eq!(status, StatusCode::OK);
    let pit = opened["pit_id"].as_str().expect("pit token").to_string();

    let body = |extra: serde_json::Value| {
        let mut base = serde_json::json!({
            "document": {"title": "2020 topps chrome update"},
            "include_source": false,
            "rank": {"priority_field": "priority"},
        });
        for (k, v) in extra.as_object().expect("extra") {
            base[k] = v.clone();
        }
        base
    };

    let (status, one_shot) = send(
        &state,
        req(
            "POST",
            "/v2/_search",
            &body(serde_json::json!({"size": 1_000, "pit": {"id": pit}})),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let expected: Vec<(u64, i64)> = one_shot["hits"]["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|hit| {
            (
                hit["_id"].as_u64().unwrap(),
                hit["_score"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(expected.len(), 20);
    assert!(
        one_shot["next_cursor"].is_null(),
        "short page ends the stream"
    );

    let mut pages: Vec<(u64, i64)> = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let page_body = match &cursor {
            None => body(serde_json::json!({"size": 7, "pit": {"id": pit}})),
            Some(token) => body(serde_json::json!({"size": 7, "cursor": token})),
        };
        let (status, page) = send(&state, req("POST", "/v2/_search", &page_body)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            page["hits"]["total"], one_shot["hits"]["total"],
            "pinned totals are page-invariant"
        );
        pages.extend(
            page["hits"]["hits"]
                .as_array()
                .expect("hits")
                .iter()
                .map(|hit| {
                    (
                        hit["_id"].as_u64().unwrap(),
                        hit["_score"].as_i64().unwrap(),
                    )
                }),
        );
        match page["next_cursor"].as_str() {
            Some(token) => cursor = Some(token.to_string()),
            None => break,
        }
    }
    assert_eq!(pages, expected, "concatenated pages equal the one-shot");
    let live_cursor = cursor;

    // A resize (placement-generation bump) stales any further cursor use as
    // the ADR-113 read-surface 409 — never a silently mixed page. (The paging
    // loop above ended with next_cursor null; re-page from the pit instead.)
    state.cluster.write().resize(4).expect("resize");
    let (status, stale) = send(
        &state,
        req(
            "POST",
            "/v2/_search",
            &body(serde_json::json!({"size": 7, "pit": {"id": pit}})),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(stale["error"]["type"], "stale_cursor");
    let _ = live_cursor;

    // Close after staleness: goal state already achieved, closed:false, 200.
    let (status, closed) = send(
        &state,
        req("DELETE", "/v2/_pit", &serde_json::json!({"pit_id": pit})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(closed["closed"], false);
}
