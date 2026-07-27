//! Strict `POST /v2/_pit` HTTP boundary and ES/OpenSearch request/response
//! compatibility proofs. Cursor paging behavior remains in the parent module.

use super::*;

async fn routed_open(
    state: &Arc<AppState>,
    request: axum::http::Request<axum::body::Body>,
) -> (StatusCode, serde_json::Value) {
    use axum::routing::post;
    use axum::Router;
    use tower::ServiceExt;

    let response = Router::new()
        .route("/v2/_pit", post(open_pit_route))
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

#[tokio::test]
async fn supports_es_os_controls_and_response_fields() {
    use axum::body::Body;
    use axum::http::Request;

    let state = state();
    let (status, json) = routed_open(
        &state,
        Request::post("/v2/_pit?keep_alive=1m&allow_partial_search_results=false")
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(json["id"], json["pit_id"], "{json}");
    assert!(json["id"].is_string(), "{json}");
    assert!(json["creation_time"].is_u64(), "{json}");
    assert_eq!(
        json["_shards"],
        serde_json::json!({"total": 1, "successful": 1, "skipped": 0, "failed": 0})
    );

    let (status, json) = routed_open(
        &state,
        Request::post("/v2/_pit")
            .header("content-type", "application/vnd.opensearch+json")
            .body(Body::from(
                serde_json::json!({
                    "keep_alive": "2m",
                    "allow_partial_pit_creation": false
                })
                .to_string(),
            ))
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(json["id"], json["pit_id"], "{json}");

    let (status, json) = routed_open(
        &state,
        Request::post("/v2/_pit?keep_alive_s=60&allow_partial_results=false")
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(json["id"], json["pit_id"], "{json}");
}

#[tokio::test]
async fn is_strict_and_preserves_extractor_statuses() {
    use axum::body::Body;
    use axum::extract::DefaultBodyLimit;
    use axum::http::Request;
    use axum::routing::post;
    use axum::Router;
    use tower::ServiceExt;

    let state = state();
    for (label, path, body) in [
        ("unknown query", "/v2/_pit?preference=local", None),
        (
            "unknown body",
            "/v2/_pit",
            Some(serde_json::json!({"routing": "tenant-a"})),
        ),
        (
            "duplicate body alias",
            "/v2/_pit",
            Some(serde_json::json!({"keep_alive_s": 60, "keep_alive": "1m"})),
        ),
        (
            "duplicate location",
            "/v2/_pit?keep_alive=1m",
            Some(serde_json::json!({"keep_alive_s": 60})),
        ),
        (
            "partial creation",
            "/v2/_pit",
            Some(serde_json::json!({"allow_partial_pit_creation": true})),
        ),
        (
            "duplicate partial aliases",
            "/v2/_pit",
            Some(serde_json::json!({
                "allow_partial_results": false,
                "allow_partial_search_results": false
            })),
        ),
        (
            "invalid time value",
            "/v2/_pit",
            Some(serde_json::json!({"keep_alive": "soon"})),
        ),
        (
            "null keep alive",
            "/v2/_pit",
            Some(serde_json::json!({"keep_alive": null})),
        ),
        (
            "null alias does not disappear",
            "/v2/_pit",
            Some(serde_json::json!({"keep_alive": "1m", "keep_alive_s": null})),
        ),
        (
            "null partial control",
            "/v2/_pit",
            Some(serde_json::json!({"allow_partial_pit_creation": null})),
        ),
    ] {
        let mut builder = Request::post(path);
        let body = match body {
            Some(body) => {
                builder = builder.header("content-type", "application/json");
                Body::from(body.to_string())
            }
            None => Body::empty(),
        };
        let (status, json) = routed_open(&state, builder.body(body).expect("request")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{label}: {json}");
        assert_eq!(json["status"], 400, "{label}: {json}");
        assert_eq!(json["error"]["type"], "validation_error", "{label}: {json}");
        if label == "invalid time value" {
            let reason = json["error"]["reason"].as_str().expect("validation reason");
            assert!(reason.contains("`keep_alive`"), "{json}");
            assert!(!reason.contains("`timeout`"), "{json}");
        }
    }
    assert!(
        state.pits.lock().is_empty(),
        "rejections must not pin a PIT"
    );
    assert_eq!(state.prom.open_pits.get(), 0);

    let (status, json) = routed_open(
        &state,
        Request::post("/v2/_pit")
            .body(Body::from("{}"))
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE, "{json}");
    assert_eq!(json["status"], 415, "{json}");
    assert_eq!(json["error"]["type"], "unsupported_media_type", "{json}");

    let (status, json) = routed_open(
        &state,
        Request::post("/v2/_pit")
            .header("content-type", "application/json")
            .body(Body::from("{"))
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
    assert_eq!(json["status"], 400, "{json}");

    let response = Router::new()
        .route("/v2/_pit", post(open_pit_route))
        .with_state(Arc::clone(&state))
        .layer(DefaultBodyLimit::max(16))
        .oneshot(
            Request::post("/v2/_pit")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"keep_alive": "123456789ms"}).to_string(),
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
