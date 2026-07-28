use super::*;

use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scrape_is_complete_prometheus_text_and_uncacheable() {
    let state = test_state(&seed());
    let (status, headers, bytes) = send_raw(&state, req_empty("GET", "/_metrics")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers
            .get(axum::http::header::CONTENT_TYPE)
            .expect("content type"),
        "text/plain; version=0.0.4; charset=utf-8"
    );
    assert_eq!(
        headers
            .get(axum::http::header::CACHE_CONTROL)
            .expect("cache"),
        "no-store"
    );
    let body = String::from_utf8(bytes.to_vec()).expect("UTF-8 metrics");
    assert!(body.contains("reverse_rusty_total_queries "));
    for shard in 0..3 {
        assert!(
            body.contains(&format!(
                "reverse_rusty_cluster_shard_queries{{shard=\"{shard}\"}} "
            )),
            "{body}"
        );
    }

    let (_, _, second) = send_raw(&state, req_empty("GET", "/_metrics")).await;
    let second = String::from_utf8(second.to_vec()).expect("UTF-8 metrics");
    assert!(
        second.contains("reverse_rusty_http_requests_total{endpoint=\"metrics\",status=\"200\"} 1")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn head_is_bodyless_and_collection_waits_asynchronously_for_stats_admission() {
    let state = test_state(&seed());
    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("stats permit");
    let request_state = Arc::clone(&state);
    let mut request =
        tokio::spawn(async move { send_raw(&request_state, req_empty("HEAD", "/_metrics")).await });

    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut request)
            .await
            .is_err(),
        "metrics collection must wait without blocking the runtime"
    );
    drop(held);

    let (status, headers, bytes) = request.await.expect("request task");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers
            .get(axum::http::header::CONTENT_TYPE)
            .expect("content type"),
        "text/plain; version=0.0.4; charset=utf-8"
    );
    assert!(bytes.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_transport_contract_is_strict_and_closed_admission_fails_loud() {
    let state = test_state(&seed());

    let (status, _, bytes) = send_raw(&state, req_empty("GET", "/_metrics?unknown=true")).await;
    assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");

    let request = Request::builder()
        .method("GET")
        .uri("/_metrics")
        .body(Body::from("not empty"))
        .expect("request");
    let (status, _, bytes) = send_raw(&state, request).await;
    assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");

    let (status, headers, bytes) = send_raw(&state, req_empty("POST", "/_metrics")).await;
    assert_eq!(
        headers.get(axum::http::header::ALLOW).expect("allow"),
        "GET, HEAD"
    );
    assert_error(
        status,
        &bytes,
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
    );

    state.stats_permits.close();
    let (status, _, bytes) = send_raw(&state, req_empty("GET", "/_metrics")).await;
    assert_error(
        status,
        &bytes,
        StatusCode::SERVICE_UNAVAILABLE,
        "metrics_unavailable",
    );
}

fn assert_error(status: StatusCode, bytes: &Bytes, expected: StatusCode, kind: &str) {
    assert_eq!(status, expected);
    let body: serde_json::Value = serde_json::from_slice(bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], kind, "{body}");
}
