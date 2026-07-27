use super::*;

async fn routed_search(
    state: &Arc<AppState>,
    method: &str,
    uri: &str,
    body: serde_json::Value,
) -> (axum::http::StatusCode, serde_json::Value) {
    use axum::body::Body;
    use axum::http::Request;
    use axum::routing::get;
    use axum::Router;
    use tower::ServiceExt;

    let app = Router::new()
        .route("/_search", get(search_route).post(search_route))
        .with_state(Arc::clone(state));
    let response = app
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
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
async fn get_search_accepts_es_controls_and_returns_compatibility_metadata() {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("vocab"));
    engine
        .try_insert_live("topps chrome", 7, 1)
        .expect("insert");
    let state = state_with(engine, false);
    let (status, response) = routed_search(
        &state,
        "GET",
        "/_search?size=1&_source=false&explain=true&timeout=1s",
        serde_json::json!({
            "query": {
                "percolate": {
                    "field": "query",
                    "document": {"title": "2020 topps chrome"}
                }
            }
        }),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{response}");
    assert!(response["took"].is_u64(), "{response}");
    assert_eq!(response["timed_out"], false);
    assert!(response["took_ms"].is_f64(), "{response}");
    assert_eq!(response["hits"]["hits"][0]["_index"], "queries");
    assert_eq!(response["hits"]["hits"][0]["_id"], 7);
    assert!(response["hits"]["hits"][0]["_explanation"].is_object());
    assert!(
        response["hits"]["hits"][0].get("_source").is_none(),
        "_source=false must suppress source"
    );
}

#[tokio::test]
async fn search_rejects_unknown_and_duplicate_controls_as_json_400s() {
    let state = state_with(
        Engine::new(Normalizer::default_vocab().expect("vocab")),
        false,
    );
    for (label, uri, body) in [
        (
            "unknown query parameter",
            "/_search?preference=local",
            serde_json::json!({"document": {"title": "x"}}),
        ),
        (
            "duplicate size",
            "/_search?size=1",
            serde_json::json!({"document": {"title": "x"}, "size": 2}),
        ),
        (
            "unitless timeout",
            "/_search?timeout=30",
            serde_json::json!({"document": {"title": "x"}}),
        ),
        (
            "overflowing timeout",
            "/_search",
            serde_json::json!({
                "document": {"title": "x"},
                "timeout": "18446744073709551615d"
            }),
        ),
        (
            "unknown body field",
            "/_search",
            serde_json::json!({"document": {"title": "x"}, "preference": "local"}),
        ),
        (
            "unknown document field",
            "/_search",
            serde_json::json!({"document": {"title": "x", "ignored": true}}),
        ),
        (
            "unknown rank field",
            "/_search",
            serde_json::json!({
                "document": {"title": "x"},
                "rank": {"sort": "_score"}
            }),
        ),
        (
            "source aliases together",
            "/_search",
            serde_json::json!({
                "document": {"title": "x"},
                "include_source": true,
                "_source": true
            }),
        ),
    ] {
        let (status, response) = routed_search(&state, "POST", uri, body).await;
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
async fn search_preserves_content_type_and_body_limit_statuses() {
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
    let body = serde_json::json!({"document": {"title": "topps chrome"}}).to_string();

    let app = Router::new()
        .route("/_search", post(search_route))
        .with_state(Arc::clone(&state));
    let response = app
        .oneshot(
            Request::post("/_search")
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
        .route("/_search", post(search_route))
        .layer(DefaultBodyLimit::max(16))
        .with_state(state);
    let response = app
        .oneshot(
            Request::post("/_search")
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
}

#[tokio::test]
async fn missing_documents_is_400() {
    let (eng, _) = corpus();
    let state = state_with(eng, false);
    let err = mpercolate(State(state), Json(body(None, None, false)))
        .await
        .err()
        .expect("missing documents must error");
    assert_eq!(err.0, axum::http::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn search_rejects_batch_over_max_percolate_batch() {
    // A multi-doc `/_search` must reject an oversized batch with 400 before
    // scheduling work, exactly like `/_mpercolate` (ADR-052) — otherwise it is
    // bounded only by the HTTP body size. A tiny cap keeps the test small.
    use reverse_rusty::config::EngineConfig;
    let cfg = EngineConfig {
        max_percolate_batch: 2,
        ..EngineConfig::default()
    };
    let mut eng = Engine::with_config(Normalizer::default_vocab().expect("vocab"), cfg);
    eng.build_from_queries(&[(1u64, "michael jordan".to_string())]);
    let state = state_with(eng, false);

    // 3 documents > cap of 2 ⇒ 400 before any matching runs.
    let over: SearchBody = serde_json::from_value(serde_json::json!({
        "documents": [{"title": "a"}, {"title": "b"}, {"title": "c"}],
        "include_source": false,
    }))
    .expect("valid SearchBody");
    let err = search(State(Arc::clone(&state)), Json(over))
        .await
        .err()
        .expect("a batch over max_percolate_batch must 400");
    assert_eq!(err.0, axum::http::StatusCode::BAD_REQUEST);

    // A batch AT the cap is accepted (the guard is strictly `>`).
    let at_cap: SearchBody = serde_json::from_value(serde_json::json!({
        "documents": [{"title": "a"}, {"title": "b"}],
        "include_source": false,
    }))
    .expect("valid SearchBody");
    assert!(
        search(State(state), Json(at_cap)).await.is_ok(),
        "a batch at the cap must be accepted"
    );
}

#[tokio::test]
async fn multi_document_explain_is_rejected_and_profile_is_aggregated() {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("vocab"));
    engine
        .try_insert_live("topps chrome", 7, 1)
        .expect("insert");
    let state = state_with(engine, false);

    let explain: SearchBody = serde_json::from_value(serde_json::json!({
        "documents": [{"title": "topps chrome"}, {"title": "topps chrome"}],
        "include_source": false,
        "explain": true
    }))
    .expect("body");
    let error = search(State(Arc::clone(&state)), Json(explain))
        .await
        .err()
        .expect("multi-document explain is ambiguous");
    assert_eq!(error.0, axum::http::StatusCode::BAD_REQUEST);

    let profile: SearchBody = serde_json::from_value(serde_json::json!({
        "documents": [{"title": "topps chrome"}, {"title": "topps chrome"}],
        "include_source": false,
        "profile": true
    }))
    .expect("body");
    let response = search(State(state), Json(profile)).await.expect("profile");
    let json = serde_json::to_value(response.0).expect("serialize response");
    assert_eq!(json["profile"]["matches"], 2, "{json}");
    assert_eq!(json["slots"][0]["stats"]["matches"], 1, "{json}");
    assert_eq!(json["slots"][1]["stats"]["matches"], 1, "{json}");
}

#[tokio::test]
async fn search_fails_loud_when_a_matched_source_is_unavailable() {
    use reverse_rusty::config::EngineConfig;

    let dir = std::env::temp_dir().join(format!(
        "reverse-rusty-search-source-guard-{}",
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
        engine
            .try_insert_live("topps chrome", 7, 1)
            .expect("insert");
        engine.flush();
    }
    let source_name = reverse_rusty::storage::read_manifest(&dir.join("manifest.bin"))
        .expect("manifest")
        .source_file_name;
    std::fs::remove_file(dir.join(source_name)).expect("remove source store");
    let engine = Engine::open(Normalizer::default_vocab().expect("vocab"), config).expect("reopen");
    assert!(engine.snapshot().has_live_query(7));
    let state = state_with(engine, false);
    let request: SearchBody = serde_json::from_value(serde_json::json!({
        "document": {"title": "topps chrome"}
    }))
    .expect("body");
    let error = search(State(state), Json(request))
        .await
        .err()
        .expect("missing source must fail");
    assert_eq!(error.0, axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    let json = serde_json::to_value(error.1 .0).expect("serialize error");
    assert_eq!(json["error"]["type"], "source_unavailable");

    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn empty_batch_is_noop() {
    let (eng, _) = corpus();
    let state = state_with(eng, true);
    let resp = mpercolate(State(state), Json(body(Some(Vec::new()), None, true)))
        .await
        .expect("empty batch is a valid no-op")
        .0;
    assert!(resp.responses.is_empty());
    assert!(resp.broad.is_none(), "no work => no broad summary");
}

// Reads the ES-convention `_id` field on hits (clippy::used_underscore_binding).
#[allow(clippy::used_underscore_binding)]
#[tokio::test]
async fn responses_are_byte_identical_to_per_title_search() {
    let (eng, titles) = corpus();
    // Capture a snapshot of the same state for the per-title baseline before
    // the engine moves into the AppState.
    let baseline = Arc::new(eng.snapshot());
    let state = state_with(eng, true);

    let batch: Vec<&str> = titles.iter().take(150).map(String::as_str).collect();
    // include_broad=true exercises the columnar broad lane through the endpoint.
    let resp = mpercolate(
        State(Arc::clone(&state)),
        Json(body(Some(batch.clone()), Some(true), true)),
    )
    .await
    .expect("ok")
    .0;

    assert_eq!(
        resp.responses.len(),
        batch.len(),
        "one response per document"
    );

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut summed = 0u32;
    for (i, title) in batch.iter().enumerate() {
        out.clear();
        baseline.match_title(title, &mut scratch, &mut out, true);
        let mut expected = out.clone();
        expected.sort_unstable();
        expected.dedup();

        let item = &resp.responses[i];
        let mut got: Vec<u64> = item.hits.hits.iter().map(|h| h._id).collect();
        got.sort_unstable();
        assert_eq!(
            got, expected,
            "document {i} ({title}) diverged from per-title search"
        );
        assert_eq!(item.hits.total, expected.len(), "total mismatch at {i}");
        summed += expected.len() as u32;
    }

    // Top-level broad summary present (profile=true) and internally consistent.
    let broad = resp.broad.expect("profile=true => broad summary");
    assert_eq!(broad.strategy, "columnar");
    assert_eq!(broad.batch_size, 256);
    assert_eq!(
        broad.total_matches, summed,
        "summary total must equal the per-document sum"
    );
}
