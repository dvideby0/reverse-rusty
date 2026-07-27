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
                "document": {"title": "1994 topps"}
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

    state
        .exhaustive_jobs
        .cancel(body["job_id"].as_str().expect("job id"));
}
