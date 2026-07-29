use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_health_shards_and_cluster_ops() {
    let state = test_state(&seed());

    let (status, body) = send(&state, req_empty("GET", "/_stats")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["took"].is_u64(), "{body}");
    assert!(body["took_ms"].is_f64(), "{body}");
    assert_eq!(
        body["_shards"],
        serde_json::json!({"total": 3, "successful": 3, "failed": 0})
    );
    assert_eq!(body["mode"], "cluster");
    assert_eq!(body["shards"], 3);
    assert!(body["total_queries"].as_u64().expect("count") >= 3);
    assert_eq!(body["pending_repairs"], 0);

    let (status, body) = send(&state, req_empty("GET", "/_health")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "green");

    let (status, _) = send(&state, req_empty("GET", "/_cat/shards")).await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = send(&state, req_empty("GET", "/_cat/shards?format=json")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_array().expect("rows").len(), 3);

    // Register and deregister an as-yet-unassigned node, then rebalance — the
    // safe in-process control-plane round trip.
    let (status, _) = send(
        &state,
        req(
            "POST",
            "/_cluster/nodes",
            &serde_json::json!({"id": 7, "addr": "http://127.0.0.1:50057"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(&state, req_empty("DELETE", "/_cluster/nodes/7")).await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = send(&state, req_empty("POST", "/_cluster/rebalance")).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = send(&state, req_empty("POST", "/_cluster/resync")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["repaired"], 0);

    // Percolation still correct after the control-plane churn (zero-FN posture).
    let (_, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({"document": {"title": "1994 acme"}}),
        ),
    )
    .await;
    assert_eq!(body["hits"]["total"], 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_stats_transport_is_strict() {
    let state = test_state(&seed());

    let (status, body) = send(&state, req_empty("GET", "/_stats?level=shards")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "validation_error");

    let (status, body) = send(
        &state,
        Request::builder()
            .method("GET")
            .uri("/_stats")
            .body(Body::from("not empty"))
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "validation_error");

    let (status, body) = send(&state, req_empty("POST", "/_stats")).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{body}");
    assert_eq!(body["error"]["type"], "method_not_allowed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_cat_segments_is_strict_and_fails_with_an_alternative() {
    let state = test_state(&seed());

    let (status, body) = send(&state, req_empty("GET", "/_cat/segments?format=json")).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{body}");
    assert_eq!(
        body["error"]["type"], "not_supported_in_cluster_mode",
        "{body}"
    );
    assert!(
        body["error"]["reason"]
            .as_str()
            .expect("reason")
            .contains("/_cat/shards"),
        "{body}"
    );

    let (status, body) = send(
        &state,
        Request::builder()
            .method("GET")
            .uri("/_cat/segments")
            .body(Body::from("not empty"))
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "validation_error");

    let (status, body) = send(&state, req_empty("GET", "/_cat/segments?unknown=true")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "validation_error");

    let (status, body) = send(&state, req_empty("POST", "/_cat/segments")).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{body}");
    assert_eq!(body["error"]["type"], "method_not_allowed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vocab_alias_makes_both_forms_match() {
    let state = test_state(&seed());
    // Declare an equivalence (ADR-054 expansion): ns ≡ northstar.
    let vocab = serde_json::json!({
        "equivalences": [["ns", "northstar"]]
    });
    let (status, body) = send(&state, req("PUT", "/_vocab", &vocab)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["acknowledged"], true);

    // A query in one form must now match a title in the other.
    let (status, _) = send(
        &state,
        req(
            "PUT",
            "/_doc/30",
            &serde_json::json!({"query": "northstar 1994"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (_, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({"document": {"title": "ns 1994"}}),
        ),
    )
    .await;
    let ids: Vec<u64> = body["hits"]["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|h| h["_id"].as_u64().expect("id"))
        .collect();
    assert!(
        ids.contains(&30),
        "alias must make both forms match: {ids:?}"
    );

    let (status, body) = send(&state, req_empty("GET", "/_vocab")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["equivalences"].as_array().is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn filtered_search_narrows_by_tags() {
    let state = test_state(&[]);
    // Tagged adds (post-build tags resolve synthetically — same TagIds everywhere).
    let (status, _) = send(
        &state,
        req(
            "PUT",
            "/_doc/41",
            &serde_json::json!({"query": "1994 acme", "tags": {"category": "items"}}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = send(
        &state,
        req(
            "PUT",
            "/_doc/42",
            &serde_json::json!({"query": "1994 acme", "tags": {"category": "comics"}}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Unfiltered: both; filtered: one (filtering only removes).
    let (_, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({"document": {"title": "1994 acme"}}),
        ),
    )
    .await;
    assert_eq!(body["hits"]["total"], 2);
    let (_, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({
                "document": {"title": "1994 acme"},
                "filter": {"category": "items"}
            }),
        ),
    )
    .await;
    assert_eq!(body["hits"]["total"], 1);
    assert_eq!(body["hits"]["hits"][0]["_id"], 41);

    // A tagged cluster ACCEPTS a vocab change (ADR-074): the rebuild carries each query's
    // stored TagIds, so the filter still narrows identically afterwards.
    let (status, body) = send(
        &state,
        req(
            "PUT",
            "/_vocab",
            &serde_json::json!({"equivalences": [["a","b"]]}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({
                "document": {"title": "1994 acme"},
                "filter": {"category": "items"}
            }),
        ),
    )
    .await;
    assert_eq!(
        body["hits"]["total"], 1,
        "the synthetic tag must survive the rebuild: {body}"
    );
    assert_eq!(body["hits"]["hits"][0]["_id"], 41);
}
