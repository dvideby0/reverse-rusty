use super::*;

async fn routed_v2_search(
    state: &Arc<AppState>,
    path: &str,
    body: serde_json::Value,
) -> (axum::http::StatusCode, serde_json::Value) {
    use axum::body::Body;
    use axum::http::Request;
    use axum::routing::post;
    use axum::Router;
    use tower::ServiceExt;

    let response = Router::new()
        .route("/v2/_search", post(super::super::v2::v2_search_route))
        .with_state(Arc::clone(state))
        .oneshot(
            Request::post(path)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let body = serde_json::from_slice(&bytes).expect("JSON response");
    (status, body)
}

#[tokio::test]
async fn v2_defaults_rank_by_priority_and_enrich_winners_only() {
    let state = state_with(ranked_engine(), false);
    let response = v2_search(
        State(Arc::clone(&state)),
        Json(v2_body(serde_json::json!({
            "document": {"title": "2020 acme chrome update"}
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
    assert!(json["took"].is_u64(), "{json}");
    assert_eq!(json["timed_out"], false);
    assert!(json["took_ms"].is_f64(), "{json}");
    assert_eq!(
        json["hits"]["total"],
        serde_json::json!({"value":3,"relation":"eq"})
    );
    assert_eq!(json["hits"]["hits"][0]["_id"], 2);
    assert_eq!(json["hits"]["hits"][0]["_score"], 50);
    assert!(json["hits"]["hits"][0]["_source"]["query"].is_string());
}

#[tokio::test]
async fn v2_selects_loaded_cpu_profile_and_rejects_unknown_profile() {
    let mut engine = reverse_rusty::segment::Engine::new(
        reverse_rusty::Normalizer::default_vocab().expect("vocab"),
    );
    engine.insert_live("acme", 1, 1);
    engine.insert_live("acme chrome pro", 2, 1);
    let mut state = state_with(engine, false);
    let profiles = reverse_rusty::RankProfiles::from_json_slice(
        br#"{
          "version": 1,
          "profiles": {
            "linear_v1": {
              "kind": "linear",
              "weights": [
                {"feature": "query_positive_terms", "weight": 100}
              ]
            }
          }
        }"#,
    )
    .expect("profiles");
    Arc::get_mut(&mut state)
        .expect("unique state")
        .rank_profiles = Arc::new(profiles);

    let (status, json) = routed_v2_search(
        &state,
        "/v2/_search",
        serde_json::json!({
            "document": {"title": "acme chrome pro update"},
            "rank": {"profile": "linear_v1", "priority_field": "priority"}
        }),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{json}");
    assert_eq!(json["hits"]["hits"][0]["_id"], 2, "{json}");
    assert_eq!(json["hits"]["hits"][0]["_score"], 300, "{json}");

    let (status, json) = routed_v2_search(
        &state,
        "/v2/_search",
        serde_json::json!({
            "document": {"title": "acme"},
            "rank": {"profile": "missing_v1"}
        }),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST, "{json}");
    assert_eq!(json["error"]["type"], "unknown_rank_profile", "{json}");
}

#[tokio::test]
async fn v2_route_supports_es_controls_and_rejects_ambiguous_or_unknown_input() {
    let state = state_with(ranked_engine(), false);
    let (status, json) = routed_v2_search(
        &state,
        "/v2/_search?size=1&_source=false&explain=true&timeout=1s&track_total_hits=1",
        serde_json::json!({"document": {"title": "acme chrome"}}),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{json}");
    assert_eq!(json["hits"]["hits"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        json["hits"]["total"],
        serde_json::json!({"value": 1, "relation": "gte"})
    );
    assert!(json["hits"]["hits"][0].get("_source").is_none(), "{json}");
    assert!(
        json["hits"]["hits"][0]["_explanation"].is_object(),
        "{json}"
    );

    for (label, path, body) in [
        (
            "unknown query parameter",
            "/v2/_search?preference=local",
            serde_json::json!({"document": {"title": "acme chrome"}}),
        ),
        (
            "duplicate size",
            "/v2/_search?size=1",
            serde_json::json!({"document": {"title": "acme chrome"}, "size": 2}),
        ),
        (
            "duplicate source alias",
            "/v2/_search",
            serde_json::json!({
                "document": {"title": "acme chrome"},
                "include_source": true,
                "_source": false
            }),
        ),
        (
            "duplicate timeout alias",
            "/v2/_search",
            serde_json::json!({
                "document": {"title": "acme chrome"},
                "timeout_ms": 1,
                "timeout": "1s"
            }),
        ),
        (
            "boolean total alias",
            "/v2/_search",
            serde_json::json!({
                "document": {"title": "acme chrome"},
                "track_total_hits": true
            }),
        ),
        (
            "unknown top-level field",
            "/v2/_search",
            serde_json::json!({
                "document": {"title": "acme chrome"},
                "preference": "local"
            }),
        ),
        (
            "unknown document field",
            "/v2/_search",
            serde_json::json!({
                "document": {"title": "acme chrome", "sku": "ABC-1"}
            }),
        ),
        (
            "unknown rank field",
            "/v2/_search",
            serde_json::json!({
                "document": {"title": "acme chrome"},
                "rank": {"priority_field": "priority", "mode": "sum"}
            }),
        ),
        (
            "unknown boost field",
            "/v2/_search",
            serde_json::json!({
                "document": {"title": "acme chrome"},
                "rank": {
                    "boosts": [{
                        "key": "tenant",
                        "value": "acme",
                        "boost": 1,
                        "weight": 1
                    }]
                }
            }),
        ),
        (
            "unknown pit field",
            "/v2/_search",
            serde_json::json!({
                "document": {"title": "acme chrome"},
                "pit": {"id": "opaque", "keep_alive": "1m"}
            }),
        ),
    ] {
        let (status, json) = routed_v2_search(&state, path, body).await;
        assert_eq!(
            status,
            axum::http::StatusCode::BAD_REQUEST,
            "{label}: {json}"
        );
        assert_eq!(json["status"], 400, "{label}: {json}");
        assert_eq!(json["error"]["type"], "validation_error", "{label}: {json}");
    }
}

#[tokio::test]
async fn v2_route_preserves_content_type_and_body_limit_statuses() {
    use axum::body::Body;
    use axum::extract::DefaultBodyLimit;
    use axum::http::Request;
    use axum::routing::post;
    use axum::Router;
    use tower::ServiceExt;

    let state = state_with(ranked_engine(), false);
    let body = serde_json::json!({"document": {"title": "acme chrome"}});
    let app = Router::new()
        .route("/v2/_search", post(super::super::v2::v2_search_route))
        .with_state(Arc::clone(&state));
    let response = app
        .clone()
        .oneshot(
            Request::post("/v2/_search")
                .body(Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(
        response.status(),
        axum::http::StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON response");
    assert_eq!(json["status"], 415, "{json}");

    let response = app
        .clone()
        .oneshot(
            Request::post("/v2/_search")
                .header("content-type", "application/json")
                .body(Body::from("{"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON response");
    assert_eq!(json["status"], 400, "{json}");
    assert_eq!(json["error"]["type"], "validation_error", "{json}");

    let response = app
        .layer(DefaultBodyLimit::max(16))
        .oneshot(
            Request::post("/v2/_search")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), axum::http::StatusCode::PAYLOAD_TOO_LARGE);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON response");
    assert_eq!(json["status"], 413, "{json}");
}

#[tokio::test]
async fn v2_threshold_size_zero_and_unsupported_modes_are_explicit() {
    let state = state_with(ranked_engine(), false);
    let response = v2_search(
        State(Arc::clone(&state)),
        Json(v2_body(serde_json::json!({
            "document": {"title": "acme chrome"},
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
            "document": {"title": "acme chrome"},
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
            "document": {"title": "acme chrome"},
            "size": 10001
        }),
        serde_json::json!({
            "document": {"title": "acme chrome"},
            "track_total_hits_up_to": 10001
        }),
        serde_json::json!({
            "document": {"title": "acme chrome"},
            "rank": {"priority_field": "price"}
        }),
        serde_json::json!({
            "document": {"title": "acme chrome"},
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
                "acme chrome",
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
            "document": {"title": "acme chrome"}
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
            "document": {"title": "acme chrome"},
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
            "document": {"title": "acme chrome"},
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
            "document": {"title": "acme chrome"},
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
            "document": {"title": "acme chrome"}
        }))),
    )
    .await
    .err()
    .expect("winner source exceeds one-byte enrichment cap");
    assert_eq!(error.0, axum::http::StatusCode::PAYLOAD_TOO_LARGE);
    let json = serde_json::to_value(error.1 .0).expect("error json");
    assert_eq!(json["error"]["type"], "rank_enrichment_limit");
}
