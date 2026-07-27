use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_reports_cluster_mode() {
    let state = test_state(&seed());
    let (status, body) = send(&state, req_empty("GET", "/")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["name"], "reverse-rusty");
    assert_eq!(body["cluster_name"], "reverse-rusty");
    assert_eq!(body["cluster_uuid"], "_na_");
    assert_eq!(body["version"]["distribution"], "reverse-rusty");
    assert_eq!(body["version"]["number"], env!("CARGO_PKG_VERSION"));
    assert_eq!(body["mode"], "cluster");
    assert_eq!(body["shards"], 3);

    let response = router(&state)
        .oneshot(req_empty("HEAD", "/"))
        .await
        .expect("router response");
    assert_eq!(response.status(), StatusCode::OK);
    assert!(axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("HEAD body")
        .is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_search_delete_round_trip() {
    let state = test_state(&seed());

    // Create.
    let (status, body) = send(
        &state,
        req(
            "PUT",
            "/_doc/10",
            &serde_json::json!({"query": "1996 skybox"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["_index"], "queries");
    assert_eq!(body["_id"], 10);
    assert_eq!(body["_version"], 1);
    assert_eq!(body["result"], "created");
    assert!(body.get("error").is_none());

    // Search finds it (with per-request include_broad).
    let (status, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({"document": {"title": "1996 skybox premium"}, "include_broad": true}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ids: Vec<u64> = body["hits"]["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|h| h["_id"].as_u64().expect("id"))
        .collect();
    assert!(ids.contains(&10), "hits: {ids:?}");

    // Replace (upsert): old stops matching, new matches; 200 updated.
    let (status, body) = send(
        &state,
        req(
            "PUT",
            "/_doc/10",
            &serde_json::json!({"query": "1997 metal"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_index"], "queries");
    assert_eq!(body["_version"], 1);
    assert_eq!(body["result"], "updated");
    let (_, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({"document": {"title": "1996 skybox premium"}}),
        ),
    )
    .await;
    let old_hits: Vec<u64> = body["hits"]["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|h| h["_id"].as_u64().expect("id"))
        .collect();
    assert!(!old_hits.contains(&10), "old version must stop matching");

    // GET returns the new source.
    let (status, body) = send(&state, req_empty("GET", "/_doc/10")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["_source"]["query"], "1997 metal");

    // Delete; then 404.
    let (status, _) = send(&state, req_empty("DELETE", "/_doc/10")).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(&state, req_empty("GET", "/_doc/10")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_doc_matches_single_node_contract_and_rejects_controls_before_mutation() {
    let state = test_state(&seed());
    for (id, refresh) in [(80, "false"), (81, "true"), (82, "wait_for")] {
        let (status, _) = send(
            &state,
            req(
                "PUT",
                &format!("/_doc/{id}"),
                &serde_json::json!({"query":format!("zzdelete{id}"), "version": 7}),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);

        let (status, body) = send(
            &state,
            req_empty("DELETE", &format!("/_doc/{id}?refresh={refresh}")),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["_index"], "queries");
        assert_eq!(body["_id"], id);
        assert_eq!(body["result"], "deleted");
        assert_eq!(body["deleted_count"], 1);
        assert!(body.get("_version").is_none());
        assert!(body.get("_shards").is_none());

        let (status, _) = send(&state, req_empty("GET", &format!("/_doc/{id}"))).await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "every refresh policy must publish before response"
        );
    }

    let (status, missing) = send(&state, req_empty("DELETE", "/_doc/80")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(missing["_index"], "queries");
    assert_eq!(missing["result"], "not_found");
    assert!(missing.get("deleted_count").is_none());

    let (status, _) = send(
        &state,
        req(
            "PUT",
            "/_doc/90",
            &serde_json::json!({"query":"wayne gretzky"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    for suffix in [
        "refresh=immediate",
        "routing=custom",
        "version=1",
        "refresh=true&refresh=false",
    ] {
        let (status, body) = send(&state, req_empty("DELETE", &format!("/_doc/90?{suffix}"))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
        assert_eq!(
            send(&state, req_empty("GET", "/_doc/90")).await.0,
            StatusCode::OK,
            "invalid controls must not delete the live query"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_doc_create_only_and_query_parameter_contract_match_single_node() {
    let state = test_state(&seed());
    let first = send(
        &state,
        req(
            "PUT",
            "/_doc/70?op_type=create&refresh=wait_for",
            &serde_json::json!({"query":"michael jordan","version":7}),
        ),
    );
    let second = send(
        &state,
        req(
            "PUT",
            "/_doc/70?op_type=create&refresh=true",
            &serde_json::json!({"query":"lebron james","version":8}),
        ),
    );
    let (a, b) = tokio::join!(first, second);
    let mut statuses = [a.0, b.0];
    statuses.sort_by_key(StatusCode::as_u16);
    assert_eq!(statuses, [StatusCode::CREATED, StatusCode::CONFLICT]);
    let (created, conflict) = if a.0 == StatusCode::CREATED {
        (a.1, b.1)
    } else {
        (b.1, a.1)
    };
    assert_eq!(created["_index"], "queries");
    assert!(
        created["_version"] == 7 || created["_version"] == 8,
        "the winning caller's display version is returned"
    );
    assert_eq!(
        conflict["error"]["type"],
        "version_conflict_engine_exception"
    );

    let (status, current) = send(&state, req_empty("GET", "/_doc/70")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        current["_source"]["query"] == "michael jordan"
            || current["_source"]["query"] == "lebron james",
        "one complete create body wins"
    );
    assert_eq!(current["_version"], created["_version"]);

    let (status, malformed_conflict) = send(
        &state,
        req(
            "PUT",
            "/_doc/70?op_type=create",
            &serde_json::json!({"query":"("}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        malformed_conflict["error"]["type"],
        "version_conflict_engine_exception"
    );

    let (status, invalid) = send(
        &state,
        req(
            "PUT",
            "/_doc/71?routing=custom",
            &serde_json::json!({"query":"wayne gretzky"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{invalid}");
    assert_eq!(invalid["error"]["type"], "illegal_argument_exception");
    let (status, _) = send(&state, req_empty("HEAD", "/_doc/71")).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "unsupported parameters reject before mutation"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_doc_reads_back_post_freeze_tags_filters_and_head_status() {
    // The seed freezes an empty tag dictionary. These tags therefore use the
    // synthetic-id path internally; GET must read the canonical raw metadata
    // retained with the source, not attempt an impossible TagId reverse lookup.
    let state = test_state(&seed());
    let (status, _) = send(
        &state,
        req(
            "PUT",
            "/_doc/71",
            &serde_json::json!({
                "query": "topps chrome",
                "version": 9,
                "tags": {"tenant": "acme", "colors": ["red", "blue"]}
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = send(&state, req_empty("GET", "/_doc/71")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_index"], "queries");
    assert_eq!(body["_version"], 9);
    assert_eq!(body["_source"]["query"], "topps chrome");
    assert_eq!(body["_source"]["tags"]["tenant"], "acme");
    assert_eq!(
        body["_source"]["tags"]["colors"],
        serde_json::json!(["blue", "red"])
    );

    let (status, body) = send(
        &state,
        req_empty("GET", "/_doc/71?_source_includes=tags.tenant"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["_source"],
        serde_json::json!({"tags": {"tenant": "acme"}})
    );

    for (path, expected) in [
        ("/_doc/71", StatusCode::OK),
        ("/_doc/72", StatusCode::NOT_FOUND),
    ] {
        let (status, body) = send(&state, req_empty("HEAD", path)).await;
        assert_eq!(status, expected);
        assert!(body.is_null(), "HEAD must be bodyless");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejections_are_loud_not_silent() {
    let state = test_state(&seed());

    // Class-D upsert → 400 naming the boundary; the prior version (none) untouched.
    let (status, body) = send(
        &state,
        req("PUT", "/_doc/11", &serde_json::json!({"query": "-onlyneg"})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["result"], "rejected");

    // explain → 400, never silently un-explained. (`rank` is SUPPORTED since ADR-075 —
    // covered by `ranked_search_orders_by_score`.)
    let (status, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({"document": {"title": "x"}, "explain": true}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"]["reason"]
        .as_str()
        .expect("reason")
        .contains("explain"));

    // Compaction aliases + PUT /_settings → 501 with the alternative named.
    for uri in ["/_compact", "/_forcemerge"] {
        let (status, body) = send(&state, req_empty("POST", uri)).await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        assert!(body["error"]["reason"]
            .as_str()
            .expect("reason")
            .contains("_checkpoint"));
    }
    let (status, _) = send(
        &state,
        req("PUT", "/_settings", &serde_json::json!({"max_segments": 4})),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
}
