use super::*;

#[tokio::test]
async fn cluster_exhaustive_creation_matches_the_local_http_contract() {
    let state = test_state(&seed());
    let (status, body) = send(
        &state,
        req(
            "POST",
            "/_percolate/jobs?timeout=2s&allow_partial_search_results=false",
            &serde_json::json!({
                "document": {"title": "1994 acme"}
            }),
        ),
    )
    .await;

    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(body["id"], body["job_id"]);
    assert_eq!(body["state"], "running");
    assert_eq!(body["is_running"], true);
    assert_eq!(body["is_partial"], true);
    assert!(body["start_time_in_millis"].is_u64());
    assert_eq!(body["event_id"].as_str().map(str::len), Some(36));

    let id = body["job_id"].as_str().expect("job id");
    let (status, status_body) = send(
        &state,
        req_empty(
            "GET",
            &format!("/_percolate/jobs/{id}?wait_for_completion_timeout=0s"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{status_body}");
    assert_eq!(status_body["id"], status_body["job_id"]);
    assert_eq!(status_body["state"], "running");
    assert_eq!(status_body["is_running"], true);
    assert_eq!(status_body["is_partial"], true);
    assert_eq!(
        status_body["start_time_in_millis"],
        status_body["created_unix_ms"]
    );

    let (status, delete_body) = send(
        &state,
        req_empty("DELETE", &format!("/_percolate/jobs/{id}")),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{delete_body}");
    assert_eq!(delete_body["acknowledged"], true);
    assert_eq!(delete_body["deleted"], false);
    assert_eq!(delete_body["id"], delete_body["job_id"]);
    assert_eq!(delete_body["job_id"], id);
    assert_eq!(delete_body["state"], "running");
}

#[tokio::test]
async fn cluster_exhaustive_stream_matches_the_local_http_contract() {
    let state = test_state(&seed());
    let (status, body) = send(
        &state,
        req(
            "POST",
            "/_percolate/jobs?timeout=2s&allow_partial_search_results=false",
            &serde_json::json!({
                "document": {"title": "1994 acme"}
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    let id = body["job_id"].as_str().expect("job id");

    let response = router(&state)
        .oneshot(req_empty("GET", &format!("/_percolate/jobs/{id}/stream")))
        .await
        .expect("router response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/x-ndjson")
    );
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );

    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("stream body");
    assert_eq!(bytes.last(), Some(&b'\n'));
    let completion = std::str::from_utf8(&bytes)
        .expect("UTF-8 stream")
        .lines()
        .last()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("JSON frame"))
        .expect("completion frame");
    assert_eq!(completion["type"], "completion");
    assert_eq!(completion["job_id"], id);
    assert_eq!(completion["exact_total"], 1);
}
