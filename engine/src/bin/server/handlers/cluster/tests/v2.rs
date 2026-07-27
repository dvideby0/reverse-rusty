use super::*;

#[test]
fn write_error_metrics_and_responses_share_the_same_status_classification() {
    for (error, expected) in [
        (
            ShardError::Config("unseeded directory".into()),
            StatusCode::BAD_REQUEST,
        ),
        (ShardError::Remote("down".into()), StatusCode::BAD_GATEWAY),
        (
            ShardError::Log("unavailable".into()),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
    ] {
        assert_eq!(shard_error_status(&error), expected);
        assert_eq!(
            shard_error_response("document write rejected", &error).status(),
            expected
        );
    }
}

#[tokio::test]
async fn cluster_v2_search_is_exact_enriched_and_reports_routed_positions() {
    let state = test_state(&[
        (3, "topps chrome".to_string()),
        (1, "topps chrome".to_string()),
        (2, "topps chrome".to_string()),
    ]);
    let (status, json) = send(
        &state,
        req(
            "POST",
            "/v2/_search",
            &serde_json::json!({
                "document": {"title": "2020 topps chrome update"},
                "explain": true
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert!(json["took"].is_u64(), "{json}");
    assert_eq!(json["timed_out"], false);
    assert!(json["took_ms"].is_f64(), "{json}");
    assert_eq!(json["complete"], true);
    assert_eq!(
        json["hits"]["total"],
        serde_json::json!({"value":3,"relation":"eq"})
    );
    let ids: Vec<u64> = json["hits"]["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|hit| hit["_id"].as_u64().expect("id"))
        .collect();
    assert_eq!(ids, vec![1, 2, 3], "score ties break by logical id");
    assert!(json["hits"]["hits"][0]["_source"]["query"].is_string());
    assert!(json["hits"]["hits"][0]["_explanation"].is_object());
    let routed = json["_shards"]["total"].as_u64().expect("routed shards");
    assert!((1..=3).contains(&routed));
    assert_eq!(json["_shards"]["successful"], routed);
    assert_eq!(json["_shards"]["failed"], 0);
}

#[tokio::test]
async fn cluster_v2_search_supports_shared_url_controls_and_strict_errors() {
    let state = test_state(&[
        (3, "topps chrome".to_string()),
        (1, "topps chrome".to_string()),
        (2, "topps chrome".to_string()),
    ]);
    let (status, json) = send(
        &state,
        req(
            "POST",
            "/v2/_search?size=1&_source=false&timeout=1s&track_total_hits=1",
            &serde_json::json!({"document": {"title": "topps chrome"}}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(json["hits"]["hits"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        json["hits"]["total"],
        serde_json::json!({"value": 1, "relation": "gte"})
    );
    assert!(json["hits"]["hits"][0].get("_source").is_none(), "{json}");

    for (label, path, request) in [
        (
            "unknown query parameter",
            "/v2/_search?preference=local",
            serde_json::json!({"document": {"title": "topps chrome"}}),
        ),
        (
            "duplicate size",
            "/v2/_search?size=1",
            serde_json::json!({
                "document": {"title": "topps chrome"},
                "size": 2
            }),
        ),
        (
            "unknown document field",
            "/v2/_search",
            serde_json::json!({
                "document": {"title": "topps chrome", "sku": "ABC-1"}
            }),
        ),
    ] {
        let (status, json) = send(&state, req("POST", path, &request)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{label}: {json}");
        assert_eq!(json["status"], 400, "{label}: {json}");
        assert_eq!(json["error"]["type"], "validation_error", "{label}: {json}");
    }
}

#[tokio::test]
async fn cluster_v2_mpercolate_is_exact_strict_and_compatibly_shaped() {
    let state = test_state(&[
        (3, "topps chrome".to_string()),
        (1, "topps chrome".to_string()),
        (2, "topps chrome".to_string()),
    ]);
    let (status, json) = send(
        &state,
        req(
            "POST",
            "/v2/_mpercolate",
            &serde_json::json!({
                "documents": [
                    {"title": "topps chrome"},
                    {"title": "no match"}
                ],
                "_source": false,
                "timeout": "1s",
                "track_total_hits": 1,
                "allow_partial_search_results": false
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert!(json["took"].is_u64(), "{json}");
    assert!(json["took_ms"].is_f64(), "{json}");
    assert_eq!(json["responses"].as_array().map(Vec::len), Some(2));
    assert_eq!(json["responses"][0]["timed_out"], false, "{json}");
    assert_eq!(json["responses"][0]["status"], 200, "{json}");
    assert_eq!(
        json["responses"][0]["hits"]["total"],
        serde_json::json!({"value": 1, "relation": "gte"})
    );
    assert!(
        json["responses"][0]["hits"]["hits"][0]
            .get("_source")
            .is_none(),
        "{json}"
    );

    for (label, path, request) in [
        (
            "unknown query parameter",
            "/v2/_mpercolate?size=1",
            serde_json::json!({"documents": [{"title": "topps chrome"}]}),
        ),
        (
            "unknown top-level field",
            "/v2/_mpercolate",
            serde_json::json!({
                "documents": [{"title": "topps chrome"}],
                "preference": "local"
            }),
        ),
        (
            "unknown document field",
            "/v2/_mpercolate",
            serde_json::json!({
                "documents": [{"title": "topps chrome", "sku": "ABC-1"}]
            }),
        ),
    ] {
        let (status, json) = send(&state, req("POST", path, &request)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{label}: {json}");
        assert_eq!(json["status"], 400, "{label}: {json}");
        assert_eq!(json["error"]["type"], "validation_error", "{label}: {json}");
    }
}

#[tokio::test]
async fn cluster_v2_search_enforces_validation_deadline_and_enrichment_cap() {
    let state = test_state(&[(1, "topps chrome".to_string())]);
    let (status, json) = send(
        &state,
        req(
            "POST",
            "/v2/_search",
            &serde_json::json!({
                "document": {"title": "topps chrome"},
                "allow_partial_results": true
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");

    let mut capped = test_state(&[(1, "topps chrome".to_string())]);
    Arc::get_mut(&mut capped)
        .expect("unique state")
        .max_ranked_enrichment_bytes = 1;
    let (status, json) = send(
        &capped,
        req(
            "POST",
            "/v2/_search",
            &serde_json::json!({"document": {"title": "topps chrome"}}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{json}");
    assert_eq!(json["error"]["type"], "rank_enrichment_limit");

    let mut queued = test_state(&[(1, "topps chrome".to_string())]);
    Arc::get_mut(&mut queued)
        .expect("unique state")
        .ranked_search_permits = Arc::new(tokio::sync::Semaphore::new(0));
    let (status, json) = send(
        &queued,
        req(
            "POST",
            "/v2/_search",
            &serde_json::json!({
                "document": {"title": "topps chrome"},
                "timeout_ms": 1
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::REQUEST_TIMEOUT, "{json}");
    assert_eq!(queued.prom.ranked_search_permits_in_use.get(), 0);
}

#[tokio::test]
async fn cluster_v2_uses_typed_priority_from_post_freeze_http_writes() {
    let state = test_state(&[]);
    for (id, priority) in [(2u64, -5), (1, 20)] {
        let (status, json) = send(
            &state,
            req(
                "PUT",
                &format!("/_doc/{id}"),
                &serde_json::json!({
                    "query": "zzhttprank",
                    "rank_fields": {"priority": priority}
                }),
            ),
        )
        .await;
        assert!(status.is_success(), "{json}");
    }
    let (status, json) = send(
        &state,
        req(
            "POST",
            "/v2/_search",
            &serde_json::json!({
                "document": {"title": "zzhttprank"},
                "include_source": false
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(json["hits"]["hits"][0]["_id"], 1);
    assert_eq!(json["hits"]["hits"][0]["_score"], 20);
    assert_eq!(json["hits"]["hits"][1]["_id"], 2);
    assert_eq!(json["hits"]["hits"][1]["_score"], -5);
}

#[tokio::test]
async fn cluster_v2_missing_winner_source_is_a_no_partial_502() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "reverse_rusty_cluster_v2_missing_source_{}_{}",
        std::process::id(),
        nonce
    ));
    let cfg = ClusterConfig {
        num_shards: 3,
        data_dir: Some(dir.clone()),
        ..ClusterConfig::default()
    };
    {
        let cluster = ClusterEngine::build(
            Normalizer::default_vocab().expect("vocab"),
            &cfg,
            &[(1, "topps chrome".to_string())],
        )
        .expect("durable cluster");
        cluster.flush().expect("flush");
        cluster.checkpoint().expect("checkpoint");
    }
    for shard in 0..3 {
        let _ = std::fs::remove_file(dir.join(format!("shard_{shard:03}")).join("sources.dat"));
    }
    let cluster = ClusterEngine::open(
        dir.clone(),
        Normalizer::default_vocab().expect("vocab"),
        None,
    )
    .expect("source-less cluster reopen");
    let state = state_from_cluster(cluster);

    let (status, json) = send(&state, req_empty("HEAD", "/_doc/1")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "coordinator HEAD must use exact-index liveness even without sources.dat"
    );
    assert!(json.is_null(), "HEAD must be bodyless");

    let (status, json) = send(&state, req_empty("GET", "/_doc/1")).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{json}");
    assert_eq!(json["error"]["type"], "source_unavailable");

    let (status, json) = send(
        &state,
        req(
            "POST",
            "/v2/_search",
            &serde_json::json!({"document": {"title": "topps chrome"}}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{json}");
    assert_eq!(json["error"]["type"], "source_unavailable");
    assert!(json.get("hits").is_none(), "partial hits must never escape");

    let (status, json) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({
                "document": {"title": "topps chrome"},
                "include_source": true
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{json}");
    assert_eq!(json["error"]["type"], "source_unavailable");
    assert!(json.get("hits").is_none(), "partial hits must never escape");
    let _ = std::fs::remove_dir_all(dir);
}
