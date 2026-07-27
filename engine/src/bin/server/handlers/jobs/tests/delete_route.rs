//! Production-boundary regressions for `DELETE /_percolate/jobs/{id}`.

use super::*;
use axum::routing::delete;

fn route(state: &Arc<AppState>) -> Router {
    Router::new()
        .route("/_percolate/jobs/{id}", delete(cancel_job))
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
        .oneshot(Request::delete(uri).body(Body::empty()).expect("request"))
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
async fn running_delete_acknowledges_cancellation_and_keeps_status_pollable() {
    let state = state(20, 1);
    let created = create(&state, "delete-running").await;
    let uri = format!("/_percolate/jobs/{}", created.job_id);
    let (status, json, cache_control) = send(route(&state), &uri).await;

    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(cache_control.as_deref(), Some("no-store"));
    assert_eq!(json["acknowledged"], true);
    assert_eq!(json["deleted"], false);
    assert_eq!(json["id"], json["job_id"]);
    assert_eq!(json["job_id"], created.job_id);
    assert_eq!(json["event_id"], "delete-running");
    assert_eq!(json["state"], "running");

    let terminal = wait_terminal(&state, &created.job_id).await;
    assert_eq!(terminal.state, crate::jobs::JobPhase::Cancelled);
}

#[tokio::test]
async fn terminal_delete_releases_the_record_and_event_id() {
    let state = state(0, 8);
    let created = create(&state, "delete-terminal").await;
    let response = get_job_stream(
        Method::GET,
        State(Arc::clone(&state)),
        Path(created.job_id.clone()),
    )
    .await
    .expect("stream");
    let bytes = to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("stream body");
    assert!(!bytes.is_empty());
    let terminal = wait_terminal(&state, &created.job_id).await;
    assert_eq!(terminal.state, crate::jobs::JobPhase::Completed);

    let uri = format!("/_percolate/jobs/{}", created.job_id);
    let (status, json, _) = send(route(&state), &uri).await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(json["acknowledged"], true);
    assert_eq!(json["deleted"], true);
    assert_eq!(json["state"], "completed");
    assert_eq!(json["exact_total"], 0);
    assert!(state.exhaustive_jobs.status(&created.job_id).is_none());
    let (status, json, _) = send(route(&state), &uri).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{json}");
    assert_eq!(json["error"]["type"], "job_not_found");

    let restarted = state
        .exhaustive_jobs
        .start(
            "delete-terminal".into(),
            [9; 32],
            reverse_rusty::QueryScope::Standard,
            Duration::from_secs(1),
            |_sink, _deadline| Ok(reverse_rusty::ExhaustiveSummary::default()),
        )
        .expect("deleted event id can be reused");
    state.exhaustive_jobs.cancel(&restarted.job.job_id);
}

#[tokio::test]
async fn unknown_delete_uses_the_standard_not_found_envelope() {
    let state = state(0, 8);
    let (status, json, cache_control) = send(route(&state), "/_percolate/jobs/missing").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{json}");
    assert_eq!(json["error"]["type"], "job_not_found");
    assert_eq!(json["status"], 404);
    assert!(cache_control.is_none());
}

#[tokio::test]
async fn delete_rejects_query_parameters() {
    let state = state(0, 8);
    let created = create(&state, "delete-query").await;
    let uri = format!("/_percolate/jobs/{}?force=true", created.job_id);
    let (status, json, _) = send(route(&state), &uri).await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
    assert_eq!(json["error"]["type"], "validation_error");
    assert!(json["error"]["reason"]
        .as_str()
        .is_some_and(|reason| reason.contains("unknown field")));
    assert!(state.exhaustive_jobs.status(&created.job_id).is_some());
    state.exhaustive_jobs.cancel(&created.job_id);
}
