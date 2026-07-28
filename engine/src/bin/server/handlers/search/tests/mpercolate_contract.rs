use super::*;

async fn routed_mpercolate(
    state: &Arc<AppState>,
    uri: &str,
    body: serde_json::Value,
) -> (axum::http::StatusCode, serde_json::Value) {
    use axum::body::Body;
    use axum::http::Request;
    use axum::routing::post;
    use axum::Router;
    use tower::ServiceExt;

    let app = Router::new()
        .route("/_mpercolate", post(mpercolate_route))
        .with_state(Arc::clone(state));
    let response = app
        .oneshot(
            Request::post(uri)
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
async fn accepts_truthful_es_controls_and_returns_batch_metadata() {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("vocab"));
    engine.try_insert_live("acme chrome", 7, 1).expect("insert");
    let state = state_with(engine, false);
    let (status, response) = routed_mpercolate(
        &state,
        "/_mpercolate",
        serde_json::json!({
            "query": {
                "percolate": {
                    "field": "query",
                    "documents": [{"title": "2020 acme chrome"}]
                }
            },
            "_source": false,
            "timeout": "1s",
            "explain": false,
            "allow_partial_search_results": false
        }),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{response}");
    assert!(response["took"].is_u64(), "{response}");
    assert!(response["took_ms"].is_f64(), "{response}");
    assert_eq!(response["responses"][0]["timed_out"], false);
    assert_eq!(response["responses"][0]["status"], 200);
    assert_eq!(
        response["responses"][0]["hits"]["hits"][0]["_index"],
        "queries"
    );
    assert_eq!(response["responses"][0]["hits"]["hits"][0]["_id"], 7);
    assert!(
        response["responses"][0]["hits"]["hits"][0]
            .get("_source")
            .is_none(),
        "_source=false must suppress source"
    );
}

#[tokio::test]
async fn rejects_ignored_or_ambiguous_input_as_json_400s() {
    let state = state_with(
        Engine::new(Normalizer::default_vocab().expect("vocab")),
        false,
    );
    for (label, uri, body) in [
        (
            "unknown query parameter",
            "/_mpercolate?size=1",
            serde_json::json!({"documents": []}),
        ),
        (
            "unknown body field",
            "/_mpercolate",
            serde_json::json!({"documents": [], "preference": "local"}),
        ),
        (
            "unknown document field",
            "/_mpercolate",
            serde_json::json!({"documents": [{"title": "x", "ignored": true}]}),
        ),
        (
            "mixed native and ES shapes",
            "/_mpercolate",
            serde_json::json!({
                "documents": [{"title": "x"}],
                "query": {"percolate": {"field": "query", "document": {"title": "x"}}}
            }),
        ),
        (
            "wrong percolator field",
            "/_mpercolate",
            serde_json::json!({
                "query": {"percolate": {"field": "other", "document": {"title": "x"}}}
            }),
        ),
        (
            "source aliases together",
            "/_mpercolate",
            serde_json::json!({
                "documents": [],
                "include_source": true,
                "_source": true
            }),
        ),
        (
            "timeout aliases together",
            "/_mpercolate",
            serde_json::json!({
                "documents": [],
                "timeout_ms": 1,
                "timeout": "1ms"
            }),
        ),
        (
            "unsupported explain",
            "/_mpercolate",
            serde_json::json!({"documents": [], "explain": true}),
        ),
        (
            "unsupported partial results",
            "/_mpercolate",
            serde_json::json!({
                "documents": [],
                "allow_partial_search_results": true
            }),
        ),
    ] {
        let (status, response) = routed_mpercolate(&state, uri, body).await;
        assert_eq!(
            status,
            axum::http::StatusCode::BAD_REQUEST,
            "{label}: {response}"
        );
        assert_eq!(response["status"], 400, "{label}: {response}");
        assert_eq!(
            response["error"]["type"], "validation_error",
            "{label}: {response}"
        );
    }
}

#[tokio::test]
async fn returns_structured_extractor_errors_and_post_only_allow() {
    use axum::body::Body;
    use axum::extract::DefaultBodyLimit;
    use axum::http::Request;
    use axum::routing::post;
    use axum::Router;
    use tower::ServiceExt;

    let state = state_with(
        Engine::new(Normalizer::default_vocab().expect("vocab")),
        false,
    );
    let body = serde_json::json!({"documents": [{"title": "acme chrome"}]}).to_string();

    let app = Router::new()
        .route("/_mpercolate", post(mpercolate_route))
        .with_state(Arc::clone(&state));
    let response = app
        .oneshot(
            Request::post("/_mpercolate")
                .body(Body::from(body.clone()))
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

    let app = Router::new()
        .route("/_mpercolate", post(mpercolate_route))
        .with_state(Arc::clone(&state));
    let response = app
        .oneshot(
            Request::post("/_mpercolate")
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

    let app = Router::new()
        .route("/_mpercolate", post(mpercolate_route))
        .layer(DefaultBodyLimit::max(16))
        .with_state(Arc::clone(&state));
    let response = app
        .oneshot(
            Request::post("/_mpercolate")
                .header("content-type", "application/json")
                .body(Body::from(body))
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

    let app = Router::new()
        .route("/_mpercolate", post(mpercolate_route))
        .with_state(state);
    let response = app
        .oneshot(
            Request::get("/_mpercolate")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(
        response.status(),
        axum::http::StatusCode::METHOD_NOT_ALLOWED
    );
    assert_eq!(
        response
            .headers()
            .get("allow")
            .and_then(|value| value.to_str().ok()),
        Some("POST")
    );
}

#[tokio::test]
async fn fails_loud_when_a_matched_source_is_unavailable() {
    use reverse_rusty::config::EngineConfig;

    let dir = std::env::temp_dir().join(format!(
        "reverse-rusty-mpercolate-source-guard-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        retain_source: false,
        ..EngineConfig::default()
    };
    {
        let mut engine =
            Engine::with_config(Normalizer::default_vocab().expect("vocab"), config.clone());
        engine.try_insert_live("acme chrome", 7, 1).expect("insert");
        engine.flush();
    }
    let source_name = reverse_rusty::storage::read_manifest(&dir.join("manifest.bin"))
        .expect("manifest")
        .source_file_name;
    std::fs::remove_file(dir.join(source_name)).expect("remove source store");
    let engine = Engine::open(Normalizer::default_vocab().expect("vocab"), config).expect("reopen");
    assert!(engine.snapshot().has_live_query(7));
    let state = state_with(engine, false);
    let request: MPercolateBody = serde_json::from_value(serde_json::json!({
        "documents": [{"title": "acme chrome"}]
    }))
    .expect("body");
    let error = mpercolate(State(state), Json(request))
        .await
        .err()
        .expect("missing source must fail");
    assert_eq!(error.0, axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    let json = serde_json::to_value(error.1 .0).expect("serialize error");
    assert_eq!(json["error"]["type"], "source_unavailable");

    let _ = std::fs::remove_dir_all(dir);
}
