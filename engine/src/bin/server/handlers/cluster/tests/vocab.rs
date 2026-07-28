use super::*;

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vocabulary_read_is_complete_uncacheable_and_bodyless_for_head() {
    let state = test_state(&seed());
    let vocab = serde_json::json!({
        "synonyms": [
            {"token": "pkg", "canonical": "term:package", "kind": "generic"}
        ],
        "equivalences": [["ns", "north star"]],
        "number_context": ["model"]
    });
    let (status, put) = send(&state, req("PUT", "/_vocab", &vocab)).await;
    assert_eq!(status, StatusCode::OK, "{put}");

    let (status, headers, bytes) = send_raw(&state, req_empty("GET", "/_vocab")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CONTENT_TYPE).expect("content type"),
        "application/json"
    );
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert_eq!(
        headers
            .get(header::CONTENT_LENGTH)
            .expect("GET content length")
            .to_str()
            .expect("ASCII length"),
        bytes.len().to_string()
    );
    let get_content_length = headers
        .get(header::CONTENT_LENGTH)
        .expect("GET content length")
        .clone();
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON vocab");
    assert_eq!(
        body["equivalences"],
        serde_json::json!([["ns", "north star"]])
    );
    assert_eq!(body["number_context"], serde_json::json!(["model"]));

    let (status, headers, bytes) = send_raw(&state, req_empty("HEAD", "/_vocab")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CONTENT_TYPE).expect("content type"),
        "application/json"
    );
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert_eq!(
        headers.get(header::CONTENT_LENGTH),
        Some(&get_content_length),
        "HEAD must preserve the corresponding GET representation length"
    );
    assert!(bytes.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vocabulary_read_waits_asynchronously_for_shared_admission() {
    let state = test_state(&seed());
    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("permit");
    let request_state = Arc::clone(&state);
    let mut request =
        tokio::spawn(async move { send_raw(&request_state, req_empty("HEAD", "/_vocab")).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut request)
            .await
            .is_err(),
        "coordinator vocabulary read must wait without blocking an async worker"
    );
    drop(held);

    let (status, headers, bytes) = request.await.expect("request task");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert!(bytes.is_empty());

    let (status, headers, bytes) = send_raw(
        &state,
        Request::builder()
            .method("POST")
            .uri("/_vocab")
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "GET, HEAD, PUT");
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], "method_not_allowed");
}
