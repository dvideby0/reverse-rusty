//! Production-boundary regressions for `GET /_percolate/jobs/{id}`.

use super::*;

fn route(state: &Arc<AppState>) -> Router {
    Router::new()
        .route("/_percolate/jobs/{id}", get(get_job))
        .with_state(Arc::clone(state))
}

async fn create(state: &Arc<AppState>, event_id: &str) -> CreateJobResponse {
    let (_, Json(created)) = create_job(State(Arc::clone(state)), Json(request(event_id)))
        .await
        .expect("job accepted");
    created
}

async fn send(app: Router, uri: &str) -> (StatusCode, serde_json::Value, Option<String>) {
    let response = app
        .oneshot(Request::get(uri).body(Body::empty()).expect("request"))
        .await
        .expect("response");
    let status = response.status();
    let cache_control = response
        .headers()
        .get(axum::http::header::CACHE_CONTROL)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let bytes = to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("response body");
    let json = serde_json::from_slice(&bytes).expect("JSON response");
    (status, json, cache_control)
}

#[tokio::test]
async fn running_status_preserves_native_fields_and_adds_truthful_async_aliases() {
    let state = state(0, 8);
    let created = create(&state, "status-running").await;
    let uri = format!(
        "/_percolate/jobs/{}?wait_for_completion_timeout=0s",
        created.job_id
    );
    let (status, json, cache_control) = send(route(&state), &uri).await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(cache_control.as_deref(), Some("no-store"));
    assert_eq!(json["id"], json["job_id"]);
    assert_eq!(json["job_id"], created.job_id);
    assert_eq!(json["event_id"], "status-running");
    assert_eq!(json["state"], "running");
    assert_eq!(json["is_running"], true);
    assert_eq!(json["is_partial"], true);
    assert_eq!(json["start_time_in_millis"], json["created_unix_ms"]);
    assert_eq!(json["query_scope"], "standard");
    assert!(json.get("completion_time_in_millis").is_none());
    assert!(json.get("completed_unix_ms").is_none());
    assert!(json.get("exact_total").is_none());
    assert!(json.get("error").is_none());

    state.exhaustive_jobs.cancel(&created.job_id);
}

#[tokio::test]
async fn wait_for_completion_returns_the_terminally_attested_status() {
    let state = state(0, 8);
    let created = create(&state, "status-wait").await;
    let job_id = created.job_id.clone();
    let uri = format!("/_percolate/jobs/{job_id}?wait_for_completion_timeout=1s");

    let status_request = send(route(&state), &uri);
    let consume_completion = async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let response = get_job_stream(
            Method::GET,
            State(Arc::clone(&state)),
            Path(job_id),
            RawQuery(None),
        )
        .await
        .expect("stream");
        let bytes = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("stream body");
        assert!(!bytes.is_empty());
    };
    let ((status, json, _), ()) = tokio::join!(status_request, consume_completion);

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(json["state"], "completed");
    assert_eq!(json["is_running"], false);
    assert_eq!(json["is_partial"], false);
    assert_eq!(json["completion_time_in_millis"], json["completed_unix_ms"]);
    assert_eq!(json["exact_total"], 0);
    assert_eq!(json["chunk_count"], 0);
    assert!(json["checksum"].is_object());
    assert!(json.get("error").is_none());
}

#[tokio::test]
async fn cancelled_status_is_terminal_partial_and_structured() {
    let state = state(20, 1);
    let created = create(&state, "status-cancelled").await;
    state
        .exhaustive_jobs
        .cancel(&created.job_id)
        .expect("retained");
    let terminal = wait_terminal(&state, &created.job_id).await;
    assert_eq!(terminal.state, crate::jobs::JobPhase::Cancelled);

    let uri = format!("/_percolate/jobs/{}", created.job_id);
    let (status, json, _) = send(route(&state), &uri).await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(json["state"], "cancelled");
    assert_eq!(json["is_running"], false);
    assert_eq!(json["is_partial"], true);
    assert_eq!(json["error"]["type"], "exhaustive_job_cancelled");
    assert_eq!(json["error"]["reason"], json["failure"]);
    assert!(json.get("exact_total").is_none());
}

#[tokio::test]
async fn failed_status_preserves_the_diagnostic_and_adds_a_structured_error() {
    let state = state(20, 1);
    let created = create(&state, "status-failed").await;
    let mut receiver = state
        .exhaustive_jobs
        .take_stream(&created.job_id)
        .expect("claim stream");
    receiver.recv().await.expect("first chunk");
    drop(receiver);
    let terminal = wait_terminal(&state, &created.job_id).await;
    assert_eq!(terminal.state, crate::jobs::JobPhase::Failed);

    let uri = format!("/_percolate/jobs/{}", created.job_id);
    let (status, json, _) = send(route(&state), &uri).await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(json["state"], "failed");
    assert_eq!(json["is_running"], false);
    assert_eq!(json["is_partial"], true);
    assert_eq!(json["error"]["type"], "exhaustive_job_failed");
    assert_eq!(json["error"]["reason"], json["failure"]);
    assert!(json.get("exact_total").is_none());
}

#[tokio::test]
async fn failed_status_preserves_a_specific_execution_error_type() {
    let state = state(0, 8);
    let started = state
        .exhaustive_jobs
        .start(
            "status-profile-transport".into(),
            [0xA5; 32],
            reverse_rusty::QueryScope::Standard,
            Duration::from_secs(1),
            |_sink, _deadline| {
                Err(JobExecutionError::new(
                    "rank_profile_transport_unsupported",
                    "remote shard cannot execute ranking profile `linear_v1`",
                ))
            },
        )
        .expect("job admitted");
    let terminal = wait_terminal(&state, &started.job.job_id).await;
    assert_eq!(terminal.state, crate::jobs::JobPhase::Failed);

    let uri = format!("/_percolate/jobs/{}", started.job.job_id);
    let (status, json, _) = send(route(&state), &uri).await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(json["state"], "failed");
    assert_eq!(json["error"]["type"], "rank_profile_transport_unsupported");
    assert_eq!(json["error"]["reason"], json["failure"]);
}

#[tokio::test]
async fn query_contract_is_strict_and_retention_controls_fail_loud() {
    let state = state(0, 8);
    let created = create(&state, "status-controls").await;
    let base = format!("/_percolate/jobs/{}", created.job_id);
    let cases = [
        (
            format!("{base}?keep_alive=1m"),
            "`keep_alive` is not supported",
        ),
        (
            format!("{base}?wait_for_completion_timeout=soon"),
            "`wait_for_completion_timeout` must include a unit",
        ),
        (
            format!("{base}?wait_for_completion_timeout=6s"),
            "must not exceed the configured exhaustive-job maximum",
        ),
        (
            format!("{base}?wait_for_completion_timeout=0s&wait_for_completion_timeout=0s"),
            "duplicate field",
        ),
        (
            format!("{base}?return_intermediate_results=true"),
            "unknown field",
        ),
    ];

    for (uri, reason) in cases {
        let (status, json, _) = send(route(&state), &uri).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {json}");
        assert_eq!(json["error"]["type"], "validation_error", "{uri}: {json}");
        assert!(
            json["error"]["reason"]
                .as_str()
                .is_some_and(|message| message.contains(reason)),
            "{uri}: {json}"
        );
    }

    state.exhaustive_jobs.cancel(&created.job_id);
}

#[tokio::test]
async fn missing_status_uses_the_standard_error_envelope() {
    let (status, json, cache_control) = send(route(&state(0, 8)), "/_percolate/jobs/missing").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{json}");
    assert_eq!(json["error"]["type"], "job_not_found");
    assert_eq!(json["status"], 404);
    assert!(cache_control.is_none());
}
