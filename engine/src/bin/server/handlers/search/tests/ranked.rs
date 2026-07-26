use super::*;

#[tokio::test]
async fn v2_defaults_rank_by_priority_and_enrich_winners_only() {
    let state = state_with(ranked_engine(), false);
    let response = v2_search(
        State(Arc::clone(&state)),
        Json(v2_body(serde_json::json!({
            "document": {"title": "2020 topps chrome update"}
        }))),
    )
    .await
    .expect("v2 response");
    let json = serde_json::to_value(response.0).expect("response json");
    assert_eq!(json["complete"], true);
    assert_eq!(json["query_scope"], "standard");
    assert_eq!(
        json["_shards"],
        serde_json::json!({"total":1,"successful":1,"failed":0})
    );
    assert_eq!(
        json["hits"]["total"],
        serde_json::json!({"value":3,"relation":"eq"})
    );
    assert_eq!(json["hits"]["hits"][0]["_id"], 2);
    assert_eq!(json["hits"]["hits"][0]["_score"], 50);
    assert!(json["hits"]["hits"][0]["_source"]["query"].is_string());
}

#[tokio::test]
async fn v2_threshold_size_zero_and_unsupported_modes_are_explicit() {
    let state = state_with(ranked_engine(), false);
    let response = v2_search(
        State(Arc::clone(&state)),
        Json(v2_body(serde_json::json!({
            "document": {"title": "topps chrome"},
            "size": 0,
            "track_total_hits_up_to": 1,
            "include_source": false
        }))),
    )
    .await
    .expect("count-only response");
    let json = serde_json::to_value(response.0).expect("response json");
    assert_eq!(json["hits"]["hits"], serde_json::json!([]));
    assert_eq!(
        json["hits"]["total"],
        serde_json::json!({"value":1,"relation":"gte"})
    );

    let error = v2_search(
        State(Arc::clone(&state)),
        Json(v2_body(serde_json::json!({
            "document": {"title": "topps chrome"},
            "result_mode": "all"
        }))),
    )
    .await
    .err()
    .expect("all is deferred");
    assert_eq!(error.0, axum::http::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn v2_enforces_rank_bounds_and_unknown_fields() {
    let state = state_with(ranked_engine(), false);
    for body in [
        serde_json::json!({
            "document": {"title": "topps chrome"},
            "size": 10001
        }),
        serde_json::json!({
            "document": {"title": "topps chrome"},
            "track_total_hits_up_to": 10001
        }),
        serde_json::json!({
            "document": {"title": "topps chrome"},
            "rank": {"priority_field": "price"}
        }),
        serde_json::json!({
            "document": {"title": "topps chrome"},
            "from": 1
        }),
    ] {
        let error = v2_search(State(Arc::clone(&state)), Json(v2_body(body)))
            .await
            .err()
            .expect("request must reject");
        assert_eq!(error.0, axum::http::StatusCode::BAD_REQUEST);
    }
}

#[tokio::test]
async fn v2_source_enrichment_is_fail_closed_and_can_be_disabled() {
    use reverse_rusty::config::EngineConfig;
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "reverse_rusty_v2_source_failure_{}_{}",
        std::process::id(),
        nonce
    ));
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    {
        let mut engine =
            Engine::with_config(Normalizer::default_vocab().expect("vocab"), config.clone());
        engine
            .try_insert_live_ranked(
                "topps chrome",
                1,
                1,
                &[("priority".into(), "9".into())],
                Some(reverse_rusty::RankValues { priority: 9 }),
            )
            .expect("ranked insert");
        engine.flush();
    }
    let source_name = reverse_rusty::storage::read_manifest(&dir.join("manifest.bin"))
        .expect("manifest")
        .source_file_name;
    std::fs::remove_file(dir.join(source_name)).expect("remove source store");
    let engine = Engine::open(Normalizer::default_vocab().expect("vocab"), config)
        .expect("source-less reopen");
    let state = state_with(engine, false);

    let error = v2_search(
        State(Arc::clone(&state)),
        Json(v2_body(serde_json::json!({
            "document": {"title": "topps chrome"}
        }))),
    )
    .await
    .err()
    .expect("default source enrichment must fail closed");
    assert_eq!(error.0, axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    let error_json = serde_json::to_value(error.1 .0).expect("error json");
    assert_eq!(error_json["error"]["type"], "source_unavailable");

    let response = v2_search(
        State(Arc::clone(&state)),
        Json(v2_body(serde_json::json!({
            "document": {"title": "topps chrome"},
            "include_source": false
        }))),
    )
    .await
    .expect("source-disabled request");
    let response_json = serde_json::to_value(response.0).expect("response json");
    assert_eq!(
        response_json["hits"]["hits"].as_array().map(Vec::len),
        Some(1)
    );

    let explanation_error = v2_search(
        State(Arc::clone(&state)),
        Json(v2_body(serde_json::json!({
            "document": {"title": "topps chrome"},
            "include_source": false,
            "explain": true
        }))),
    )
    .await
    .err()
    .expect("requested explanation must fail closed without source");
    let explanation_json = serde_json::to_value(explanation_error.1 .0).expect("error json");
    assert_eq!(explanation_json["error"]["type"], "explanation_unavailable");
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn v2_deadline_includes_ranked_permit_queue() {
    let mut state = state_with(ranked_engine(), false);
    Arc::get_mut(&mut state)
        .expect("unique state")
        .ranked_search_permits = Arc::new(tokio::sync::Semaphore::new(0));
    let error = v2_search(
        State(Arc::clone(&state)),
        Json(v2_body(serde_json::json!({
            "document": {"title": "topps chrome"},
            "timeout_ms": 1
        }))),
    )
    .await
    .err()
    .expect("permit queue must consume the deadline");
    assert_eq!(error.0, axum::http::StatusCode::REQUEST_TIMEOUT);
    assert_eq!(state.prom.ranked_search_permits_in_use.get(), 0);
}

#[tokio::test]
async fn v2_enrichment_cap_is_shared_and_fail_closed() {
    let mut state = state_with(ranked_engine(), false);
    Arc::get_mut(&mut state)
        .expect("unique state")
        .max_ranked_enrichment_bytes = 1;
    let error = v2_search(
        State(Arc::clone(&state)),
        Json(v2_body(serde_json::json!({
            "document": {"title": "topps chrome"}
        }))),
    )
    .await
    .err()
    .expect("winner source exceeds one-byte enrichment cap");
    assert_eq!(error.0, axum::http::StatusCode::PAYLOAD_TOO_LARGE);
    let json = serde_json::to_value(error.1 .0).expect("error json");
    assert_eq!(json["error"]["type"], "rank_enrichment_limit");
}
