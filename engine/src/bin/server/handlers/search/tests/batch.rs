use super::*;

async fn routed_v2_mpercolate(
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
        .route(
            "/v2/_mpercolate",
            post(super::super::v2::v2_mpercolate_route),
        )
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
async fn v2_mpercolate_per_slot_equals_v2_search_and_shares_winner_sources() {
    let state = state_with(ranked_engine(), false);
    let titles = [
        "2020 acme chrome update",
        "no match at all",
        "2020 acme chrome update",
    ];
    let batch = v2_mpercolate(
        State(Arc::clone(&state)),
        Json(v2_batch_body(serde_json::json!({
            "documents": titles.iter().map(|t| serde_json::json!({"title": t})).collect::<Vec<_>>()
        }))),
    )
    .await
    .expect("batch response");
    let batch_json = serde_json::to_value(batch.0).expect("batch json");
    assert_eq!(batch_json["complete"], true);
    assert_eq!(batch_json["responses"].as_array().map(Vec::len), Some(3));
    for (i, title) in titles.iter().enumerate() {
        let single = v2_search(
            State(Arc::clone(&state)),
            Json(v2_body(serde_json::json!({"document": {"title": title}}))),
        )
        .await
        .expect("single response");
        let single_json = serde_json::to_value(single.0).expect("single json");
        assert_eq!(
            batch_json["responses"][i]["hits"], single_json["hits"],
            "slot {i} must equal its /v2/_search result"
        );
        assert_eq!(
            batch_json["responses"][i]["_shards"], single_json["_shards"],
            "slot {i} shard echo"
        );
    }
}

#[tokio::test]
async fn v2_mpercolate_named_unsupported_shapes_and_empty_batch() {
    let state = state_with(ranked_engine(), false);
    let Err(error) = v2_mpercolate(
        State(Arc::clone(&state)),
        Json(v2_batch_body(serde_json::json!({
            "documents": [{"title": "acme chrome"}],
            "explain": true
        }))),
    )
    .await
    else {
        panic!("explain must be a named 400");
    };
    assert_eq!(error.0, axum::http::StatusCode::BAD_REQUEST);

    let Err(error) = v2_mpercolate(
        State(Arc::clone(&state)),
        Json(v2_batch_body(serde_json::json!({
            "document": {"title": "acme chrome"}
        }))),
    )
    .await
    else {
        panic!("the singular document shape must be a named 400");
    };
    assert_eq!(error.0, axum::http::StatusCode::BAD_REQUEST);

    let Err(error) = v2_mpercolate(
        State(Arc::clone(&state)),
        Json(v2_batch_body(serde_json::json!({}))),
    )
    .await
    else {
        panic!("a MISSING documents field must be a named 400, not an empty 200");
    };
    assert_eq!(error.0, axum::http::StatusCode::BAD_REQUEST);

    let Err(error) = v2_mpercolate(
        State(Arc::clone(&state)),
        Json(v2_batch_body(serde_json::json!({
            "documents": [{"title": "acme chrome", "size": 1}]
        }))),
    )
    .await
    else {
        panic!("a per-document option must be a named 400, never silently discarded");
    };
    assert_eq!(error.0, axum::http::StatusCode::BAD_REQUEST);

    let empty = v2_mpercolate(
        State(Arc::clone(&state)),
        Json(v2_batch_body(serde_json::json!({"documents": []}))),
    )
    .await
    .expect("empty batch is a 200");
    let json = serde_json::to_value(empty.0).expect("empty json");
    assert_eq!(json["responses"], serde_json::json!([]));
    assert_eq!(json["complete"], true);
}

#[tokio::test]
async fn v2_mpercolate_route_supports_truthful_es_controls_and_strict_input() {
    let state = state_with(ranked_engine(), false);
    let (status, json) = routed_v2_mpercolate(
        &state,
        "/v2/_mpercolate",
        serde_json::json!({
            "documents": [{"title": "acme chrome"}],
            "_source": false,
            "timeout": "1s",
            "track_total_hits": 1,
            "explain": false,
            "allow_partial_search_results": false
        }),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{json}");
    assert!(json["took"].is_u64(), "{json}");
    assert!(json["took_ms"].is_f64(), "{json}");
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

    for (label, path, body) in [
        (
            "unknown query parameter",
            "/v2/_mpercolate?size=1",
            serde_json::json!({"documents": [{"title": "acme chrome"}]}),
        ),
        (
            "unknown top-level field",
            "/v2/_mpercolate",
            serde_json::json!({
                "documents": [{"title": "acme chrome"}],
                "preference": "local"
            }),
        ),
        (
            "unknown document field",
            "/v2/_mpercolate",
            serde_json::json!({
                "documents": [{"title": "acme chrome", "sku": "ABC-1"}]
            }),
        ),
        (
            "unknown rank field",
            "/v2/_mpercolate",
            serde_json::json!({
                "documents": [{"title": "acme chrome"}],
                "rank": {"priority_field": "priority", "mode": "sum"}
            }),
        ),
        (
            "unknown boost field",
            "/v2/_mpercolate",
            serde_json::json!({
                "documents": [{"title": "acme chrome"}],
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
            "duplicate source alias",
            "/v2/_mpercolate",
            serde_json::json!({
                "documents": [{"title": "acme chrome"}],
                "include_source": true,
                "_source": false
            }),
        ),
        (
            "duplicate total alias",
            "/v2/_mpercolate",
            serde_json::json!({
                "documents": [{"title": "acme chrome"}],
                "track_total_hits_up_to": 1,
                "track_total_hits": 1
            }),
        ),
        (
            "duplicate timeout alias",
            "/v2/_mpercolate",
            serde_json::json!({
                "documents": [{"title": "acme chrome"}],
                "timeout_ms": 1,
                "timeout": "1s"
            }),
        ),
        (
            "duplicate partial-result alias",
            "/v2/_mpercolate",
            serde_json::json!({
                "documents": [{"title": "acme chrome"}],
                "allow_partial_results": false,
                "allow_partial_search_results": false
            }),
        ),
        (
            "boolean total alias",
            "/v2/_mpercolate",
            serde_json::json!({
                "documents": [{"title": "acme chrome"}],
                "track_total_hits": true
            }),
        ),
        (
            "invalid timeout",
            "/v2/_mpercolate",
            serde_json::json!({
                "documents": [{"title": "acme chrome"}],
                "timeout": "1fortnight"
            }),
        ),
        (
            "partial results",
            "/v2/_mpercolate",
            serde_json::json!({
                "documents": [{"title": "acme chrome"}],
                "allow_partial_search_results": true
            }),
        ),
    ] {
        let (status, json) = routed_v2_mpercolate(&state, path, body).await;
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
async fn v2_mpercolate_route_preserves_content_type_and_body_limit_statuses() {
    use axum::body::Body;
    use axum::extract::DefaultBodyLimit;
    use axum::http::Request;
    use axum::routing::post;
    use axum::Router;
    use tower::ServiceExt;

    let state = state_with(ranked_engine(), false);
    let body = serde_json::json!({"documents": [{"title": "acme chrome"}]});
    let app = Router::new()
        .route(
            "/v2/_mpercolate",
            post(super::super::v2::v2_mpercolate_route),
        )
        .with_state(Arc::clone(&state));
    let response = app
        .clone()
        .oneshot(
            Request::post("/v2/_mpercolate")
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
            Request::post("/v2/_mpercolate")
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
            Request::post("/v2/_mpercolate")
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
