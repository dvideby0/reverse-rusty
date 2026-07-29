//! Production-boundary regressions for `POST /_percolate/jobs` (ADR-131).

use super::*;

use axum::extract::DefaultBodyLimit;
use axum::http::header::CONTENT_TYPE;
use axum::routing::post;

fn route(state: &Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/_percolate/jobs",
            post(create_job_route).layer(DefaultBodyLimit::max(EXHAUSTIVE_JOB_BODY_LIMIT)),
        )
        // Mirror the server-wide bulk allowance. The route-local limit must
        // remain the effective one nearest the JSON extractor.
        .layer(DefaultBodyLimit::max(100 * 1024 * 1024))
        .with_state(Arc::clone(state))
}

fn post_request(uri: &str, body: impl Into<Body>, content_type: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().method(Method::POST).uri(uri);
    if let Some(content_type) = content_type {
        builder = builder.header(CONTENT_TYPE, content_type);
    }
    builder.body(body.into()).expect("request")
}

async fn send(
    app: Router,
    uri: &str,
    body: impl Into<Body>,
    content_type: Option<&str>,
) -> (StatusCode, serde_json::Value) {
    let response = app
        .oneshot(post_request(uri, body, content_type))
        .await
        .expect("response");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 2 * 1024 * 1024)
        .await
        .expect("response body");
    let json = serde_json::from_slice(&bytes).expect("JSON response");
    (status, json)
}

#[tokio::test]
async fn minimal_body_generates_identity_and_async_compatibility_fields() {
    let state = state(0, 8);
    let (status, json) = send(
        route(&state),
        "/_percolate/jobs",
        r#"{"document":{"title":"deliveryneedle"}}"#,
        Some("application/json"),
    )
    .await;

    assert_eq!(status, StatusCode::ACCEPTED, "{json}");
    assert_eq!(json["id"], json["job_id"]);
    assert_eq!(json["state"], "running");
    assert_eq!(json["is_running"], true);
    assert_eq!(json["is_partial"], true);
    assert!(json["start_time_in_millis"].is_u64());
    assert_eq!(json["event_id"].as_str().map(str::len), Some(36));
    assert_eq!(
        json["status_url"],
        format!("/_percolate/jobs/{}", json["job_id"].as_str().unwrap())
    );
}

#[tokio::test]
async fn elastic_timeout_and_partial_controls_work_in_the_query_string() {
    let state = state(0, 8);
    let (status, json) = send(
        route(&state),
        "/_percolate/jobs?timeout=2s&allow_partial_search_results=false",
        r#"{"document":{"title":"deliveryneedle"}}"#,
        Some("application/vnd.reverse-rusty+json"),
    )
    .await;

    assert_eq!(status, StatusCode::ACCEPTED, "{json}");
}

#[tokio::test]
async fn the_explicit_native_request_remains_idempotent() {
    let state = state(0, 8);
    let body = serde_json::json!({
        "event_id": "native-retry-key",
        "document": {"title": "deliveryneedle"},
        "result_mode": "all",
        "query_scope": "standard",
        "sink": {"type": "ndjson_stream"},
        "timeout_ms": 2000,
        "allow_partial_results": false
    })
    .to_string();

    let (first_status, first) = send(
        route(&state),
        "/_percolate/jobs",
        body.clone(),
        Some("application/json"),
    )
    .await;
    let (second_status, second) = send(
        route(&state),
        "/_percolate/jobs",
        body,
        Some("application/json"),
    )
    .await;

    assert_eq!(first_status, StatusCode::ACCEPTED, "{first}");
    assert_eq!(second_status, StatusCode::ACCEPTED, "{second}");
    assert_eq!(second["reused"], true);
    assert_eq!(second["job_id"], first["job_id"]);
}

#[tokio::test]
async fn alias_conflicts_and_inexact_controls_fail_loud() {
    let cases = [
        (
            "/_percolate/jobs",
            r#"{"document":{"title":"x"},"timeout_ms":1,"timeout":"1ms"}"#,
            "aliases",
        ),
        (
            "/_percolate/jobs?timeout_ms=1&timeout=1ms",
            r#"{"document":{"title":"x"}}"#,
            "aliases",
        ),
        (
            "/_percolate/jobs?timeout=1ms",
            r#"{"document":{"title":"x"},"timeout_ms":1}"#,
            "either the request body or query string",
        ),
        (
            "/_percolate/jobs",
            r#"{"document":{"title":"x"},"allow_partial_results":false,"allow_partial_search_results":false}"#,
            "aliases",
        ),
        (
            "/_percolate/jobs?allow_partial_search_results=true",
            r#"{"document":{"title":"x"}}"#,
            "incompatible with exhaustive exact delivery",
        ),
        (
            "/_percolate/jobs?wait_for_completion_timeout=1s",
            r#"{"document":{"title":"x"}}"#,
            "not supported",
        ),
        (
            "/_percolate/jobs",
            r#"{"document":{"title":"x"},"keep_alive":"1m"}"#,
            "not supported",
        ),
        (
            "/_percolate/jobs",
            r#"{"document":{"title":"x"},"keep_on_completion":true}"#,
            "not supported",
        ),
    ];

    for (uri, body, reason) in cases {
        let state = state(0, 8);
        let (status, json) = send(route(&state), uri, body, Some("application/json")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {json}");
        assert_eq!(json["error"]["type"], "validation_error", "{uri}: {json}");
        assert!(
            json["error"]["reason"]
                .as_str()
                .is_some_and(|message| message.contains(reason)),
            "{uri}: {json}"
        );
    }
}

#[tokio::test]
async fn strict_json_and_query_shapes_return_structured_errors() {
    let cases = [
        (
            "/_percolate/jobs",
            r#"{"document":{"title":"x"},"unknown":1}"#,
        ),
        (
            "/_percolate/jobs",
            r#"{"document":{"title":"x","unknown":1}}"#,
        ),
        (
            "/_percolate/jobs",
            r#"{"event_id":null,"document":{"title":"x"}}"#,
        ),
        (
            "/_percolate/jobs?unknown=1",
            r#"{"document":{"title":"x"}}"#,
        ),
    ];

    for (uri, body) in cases {
        let state = state(0, 8);
        let (status, json) = send(route(&state), uri, body, Some("application/json")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {json}");
        assert_eq!(json["error"]["type"], "validation_error", "{uri}: {json}");
    }
}

#[tokio::test]
async fn malformed_media_type_and_body_limit_fail_before_admission() {
    let cases = [
        (
            r#"{"document":{"title":"x"}}"#.to_string(),
            None,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
        ),
        (
            r#"{"document":{"title":"x"}}"#.to_string(),
            Some("text/plain"),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
        ),
        (
            "{".to_string(),
            Some("application/json"),
            StatusCode::BAD_REQUEST,
            "validation_error",
        ),
    ];

    for (body, content_type, expected_status, expected_type) in cases {
        let state = state(0, 8);
        let (status, json) = send(route(&state), "/_percolate/jobs", body, content_type).await;
        assert_eq!(status, expected_status, "{json}");
        assert_eq!(json["error"]["type"], expected_type, "{json}");
    }

    let state = state(0, 8);
    let oversized = serde_json::json!({
        "document": {"title": "x".repeat(EXHAUSTIVE_JOB_BODY_LIMIT)}
    })
    .to_string();
    let (status, json) = send(
        route(&state),
        "/_percolate/jobs",
        oversized,
        Some("application/json"),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{json}");
    assert_eq!(json["error"]["type"], "payload_too_large", "{json}");
}

#[tokio::test]
async fn non_exhaustive_result_mode_has_a_named_error() {
    let state = state(0, 8);
    let (status, json) = send(
        route(&state),
        "/_percolate/jobs",
        r#"{"document":{"title":"x"},"result_mode":"top_k"}"#,
        Some("application/json"),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
    assert_eq!(json["error"]["type"], "unsupported_result_mode", "{json}");
}

#[tokio::test]
async fn unknown_rank_profile_has_the_shared_typed_error() {
    let state = state(0, 8);
    let (status, json) = send(
        route(&state),
        "/_percolate/jobs",
        r#"{"document":{"title":"x"},"rank":{"profile":"missing"}}"#,
        Some("application/json"),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
    assert_eq!(json["error"]["type"], "unknown_rank_profile", "{json}");
}
