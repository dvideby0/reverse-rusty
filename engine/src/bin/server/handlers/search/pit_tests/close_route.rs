//! Strict `DELETE /v2/_pit` request/response compatibility and atomic
//! pre-validation proofs. Cursor behavior remains in the parent module.

use super::*;

async fn routed_close(
    state: &Arc<AppState>,
    request: axum::http::Request<axum::body::Body>,
) -> (StatusCode, serde_json::Value) {
    use axum::routing::delete;
    use axum::Router;
    use tower::ServiceExt;

    let response = Router::new()
        .route(
            "/v2/_pit",
            delete(close_pit_route).layer(axum::extract::DefaultBodyLimit::max(PIT_BODY_LIMIT)),
        )
        .with_state(Arc::clone(state))
        .oneshot(request)
        .await
        .expect("response");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let body = serde_json::from_slice(&bytes).expect("JSON response");
    (status, body)
}

fn request(body: &serde_json::Value) -> axum::http::Request<axum::body::Body> {
    axum::http::Request::delete("/v2/_pit")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(body.to_string()))
        .expect("request")
}

#[tokio::test]
async fn supports_es_os_native_shapes_and_truthful_results() {
    let state = state();
    let first = open(&state, None).expect("first PIT");
    let second = open(&state, None).expect("second PIT");
    let third = open(&state, None).expect("third PIT");

    let (status, es) =
        routed_close(&state, request(&serde_json::json!({"id": first.clone()}))).await;
    assert_eq!(status, StatusCode::OK, "{es}");
    assert_eq!(es["closed"], true, "{es}");
    assert_eq!(es["succeeded"], true, "{es}");
    assert_eq!(es["num_freed"], 1, "{es}");
    assert_eq!(
        es["pits"],
        serde_json::json!([{"successful": true, "pit_id": first}])
    );

    let (status, os) = routed_close(
        &state,
        request(&serde_json::json!({
            "pit_id": [second.clone(), third.clone()]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{os}");
    assert_eq!(os["closed"], true, "{os}");
    assert_eq!(os["succeeded"], true, "{os}");
    assert_eq!(os["num_freed"], 2, "{os}");
    assert_eq!(
        os["pits"],
        serde_json::json!([
            {"successful": true, "pit_id": second},
            {"successful": true, "pit_id": third}
        ])
    );
    assert!(state.pits.lock().is_empty());
    assert_eq!(state.prom.open_pits.get(), 0);

    let (status, gone) = routed_close(
        &state,
        request(&serde_json::json!({"pit_id": first.clone()})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{gone}");
    assert_eq!(gone["closed"], false, "{gone}");
    assert_eq!(gone["succeeded"], true, "{gone}");
    assert_eq!(gone["num_freed"], 0, "{gone}");
    assert_eq!(
        gone["pits"],
        serde_json::json!([{"successful": false, "pit_id": first}])
    );

    let fourth = open(&state, None).expect("fourth PIT");
    let (status, mixed) = routed_close(
        &state,
        request(&serde_json::json!({
            "pit_id": [first.clone(), fourth.clone()]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{mixed}");
    assert_eq!(mixed["closed"], false, "{mixed}");
    assert_eq!(mixed["succeeded"], true, "{mixed}");
    assert_eq!(mixed["num_freed"], 1, "{mixed}");
    assert_eq!(
        mixed["pits"],
        serde_json::json!([
            {"successful": false, "pit_id": first},
            {"successful": true, "pit_id": fourth}
        ])
    );
    assert!(state.pits.lock().is_empty());
    assert_eq!(state.prom.open_pits.get(), 0);
}

#[tokio::test]
async fn rejects_ambiguous_or_invalid_batches_before_any_close() {
    use axum::body::Body;
    use axum::http::Request;

    let state = state();
    let first = open(&state, None).expect("first PIT");
    let second = open(&state, None).expect("second PIT");
    let foreign = crate::pit::PitTokens::generate().mint_pit(reverse_rusty::PitId(999));
    let too_many = vec![first.clone(); state.pit_config.max_open + 1];
    let duplicate_id = format!(r#"{{"id":"{first}","id":"{first}"}}"#);

    let cases = [
        (
            "unknown query",
            Request::delete("/v2/_pit?refresh=true")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"id": first.clone()}).to_string(),
                ))
                .expect("request"),
            StatusCode::BAD_REQUEST,
        ),
        (
            "unknown body",
            request(&serde_json::json!({
                "id": first.clone(),
                "routing": "tenant-a"
            })),
            StatusCode::BAD_REQUEST,
        ),
        (
            "alias conflict",
            request(&serde_json::json!({
                "id": first.clone(),
                "pit_id": second.clone()
            })),
            StatusCode::BAD_REQUEST,
        ),
        (
            "missing id",
            request(&serde_json::json!({})),
            StatusCode::BAD_REQUEST,
        ),
        (
            "null id",
            request(&serde_json::json!({"id": null})),
            StatusCode::BAD_REQUEST,
        ),
        (
            "wrong-type id",
            request(&serde_json::json!({"id": 42})),
            StatusCode::BAD_REQUEST,
        ),
        (
            "duplicate id field",
            Request::delete("/v2/_pit")
                .header("content-type", "application/json")
                .body(Body::from(duplicate_id))
                .expect("request"),
            StatusCode::BAD_REQUEST,
        ),
        (
            "empty batch",
            request(&serde_json::json!({"pit_id": []})),
            StatusCode::BAD_REQUEST,
        ),
        (
            "oversized batch",
            request(&serde_json::json!({"pit_id": too_many})),
            StatusCode::BAD_REQUEST,
        ),
        (
            "foreign token after valid token",
            request(&serde_json::json!({
                "pit_id": [first.clone(), foreign]
            })),
            StatusCode::CONFLICT,
        ),
        (
            "malformed token after valid token",
            request(&serde_json::json!({
                "pit_id": [second.clone(), "garbage"]
            })),
            StatusCode::BAD_REQUEST,
        ),
    ];

    for (label, request, expected) in cases {
        let (status, json) = routed_close(&state, request).await;
        assert_eq!(status, expected, "{label}: {json}");
        assert_eq!(
            state.pits.lock().len(),
            2,
            "{label}: validation must precede every close"
        );
        assert_eq!(state.prom.open_pits.get(), 2, "{label}");
    }
}

#[tokio::test]
async fn preserves_structured_json_extractor_statuses() {
    use axum::body::Body;
    use axum::extract::DefaultBodyLimit;
    use axum::http::Request;
    use axum::routing::delete;
    use axum::Router;
    use tower::ServiceExt;

    let state = state();
    for (label, request, expected, error_type) in [
        (
            "missing content type",
            Request::delete("/v2/_pit")
                .body(Body::from("{}"))
                .expect("request"),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
        ),
        (
            "empty body",
            Request::delete("/v2/_pit")
                .header("content-type", "application/json")
                .body(Body::empty())
                .expect("request"),
            StatusCode::BAD_REQUEST,
            "validation_error",
        ),
        (
            "malformed JSON",
            Request::delete("/v2/_pit")
                .header("content-type", "application/json")
                .body(Body::from("{"))
                .expect("request"),
            StatusCode::BAD_REQUEST,
            "validation_error",
        ),
    ] {
        let (status, json) = routed_close(&state, request).await;
        assert_eq!(status, expected, "{label}: {json}");
        assert_eq!(json["status"], expected.as_u16(), "{label}: {json}");
        assert_eq!(json["error"]["type"], error_type, "{label}: {json}");
    }

    let empty_ids = vec![r#""""#; PIT_BODY_LIMIT / 3 + 16].join(",");
    let large_array = format!(r#"{{"pit_id":[{empty_ids}]}}"#);
    assert!(large_array.len() > PIT_BODY_LIMIT);
    let (status, json) = routed_close(
        &state,
        Request::delete("/v2/_pit")
            .header("content-type", "application/json")
            .body(Body::from(large_array))
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{json}");
    assert_eq!(json["status"], 413, "{json}");
    assert_eq!(json["error"]["type"], "payload_too_large", "{json}");

    let response = Router::new()
        .route("/v2/_pit", delete(close_pit_route))
        .with_state(Arc::clone(&state))
        .layer(DefaultBodyLimit::max(16))
        .oneshot(
            Request::delete("/v2/_pit")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"pit_id": "a-long-token"}).to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON response");
    assert_eq!(json["status"], 413, "{json}");
    assert_eq!(json["error"]["type"], "payload_too_large", "{json}");
}
