use super::*;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn valid_update_names_the_cluster_alternative_and_is_observed() {
    let state = test_state(&seed());
    let (status, headers, bytes) = send_raw(
        &state,
        Request::builder()
            .method("PUT")
            .uri("/_settings?flat_settings=true&timeout=1s")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"max_segments":4}"#))
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
    assert_eq!(
        body["error"]["type"], "not_supported_in_cluster_mode",
        "{body}"
    );
    let reason = body["error"]["reason"].as_str().expect("reason");
    assert!(reason.contains("restart the coordinator"), "{reason}");
    assert!(reason.contains("shard nodes"), "{reason}");
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["settings_put", "501"])
            .get(),
        1
    );
    assert_eq!(
        state
            .prom
            .http_request_duration
            .with_label_values(&["settings_put"])
            .get_sample_count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_transport_and_patch_validation_precede_the_cluster_boundary() {
    let state = test_state(&seed());
    let requests = [
        Request::builder()
            .method("PUT")
            .uri("/_settings?unknown=true")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"max_segments":4}"#))
            .expect("query request"),
        Request::builder()
            .method("PUT")
            .uri("/_settings")
            .body(Body::from(r#"{"max_segments":4}"#))
            .expect("media request"),
        Request::builder()
            .method("PUT")
            .uri("/_settings")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from("{"))
            .expect("JSON request"),
        Request::builder()
            .method("PUT")
            .uri("/_settings")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"max_segments":4,"max_segments":5}"#))
            .expect("duplicate request"),
        Request::builder()
            .method("PUT")
            .uri("/_settings")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"max_segments":0}"#))
            .expect("semantic request"),
    ];
    let expected = [
        (StatusCode::BAD_REQUEST, "validation_error"),
        (StatusCode::UNSUPPORTED_MEDIA_TYPE, "unsupported_media_type"),
        (StatusCode::BAD_REQUEST, "validation_error"),
        (StatusCode::BAD_REQUEST, "validation_error"),
        (StatusCode::BAD_REQUEST, "settings_error"),
    ];

    for (request, (expected_status, expected_kind)) in requests.into_iter().zip(expected) {
        let (status, headers, bytes) = send_raw(&state, request).await;
        assert_eq!(status, expected_status);
        assert_eq!(
            headers.get(header::CACHE_CONTROL).expect("cache"),
            "no-store"
        );
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
        assert_eq!(body["error"]["type"], expected_kind, "{body}");
    }

    let oversized = vec![b'x'; crate::handlers::SETTINGS_WRITE_BODY_LIMIT + 1];
    let (status, headers, bytes) = send_raw(
        &state,
        Request::builder()
            .method("PUT")
            .uri("/_settings")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(oversized))
            .expect("oversized request"),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], "payload_too_large", "{body}");
}
