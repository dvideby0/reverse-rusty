use super::*;

use std::sync::Arc;
use std::time::Duration;

use axum::http::{header, Method};
use reverse_rusty::cluster::{
    ClusterState, ClusterStateChange, ControlError, ControlPlane, NodeId, StateVersion,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn green_readiness_is_truthful_uncacheable_and_observed() {
    let state = test_state(&seed());
    let (status, headers, bytes) = send_raw(
        &state,
        req_empty(
            "GET",
            "/_health?wait_for_status=green&timeout=1s&level=cluster",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON health");
    assert_eq!(body["status"], "green");
    assert_eq!(body["mode"], "cluster");
    assert_eq!(body["timed_out"], false);
    assert_eq!(body["shards"], 3);
    assert_eq!(body["pending_repairs"], 0);
    assert!(body.get("reason").is_none());
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["health", "200"])
            .get(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn head_is_bodyless_and_collection_waits_asynchronously_for_stats_admission() {
    let state = test_state(&seed());
    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("permit");
    let request_state = Arc::clone(&state);
    let request =
        tokio::spawn(async move { send_raw(&request_state, req_empty("HEAD", "/_health")).await });
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(!request.is_finished(), "health must share stats admission");

    drop(held);
    let (status, headers, bytes) = tokio::time::timeout(Duration::from_secs(1), request)
        .await
        .expect("request completes after admission")
        .expect("request task");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert_eq!(
        headers.get(header::CONTENT_TYPE).expect("content type"),
        "application/json"
    );
    assert!(bytes.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_timeout_bounds_stats_admission() {
    let state = test_state(&seed());
    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("permit");
    let (status, _, bytes) = tokio::time::timeout(
        Duration::from_secs(1),
        send_raw(
            &state,
            req_empty("GET", "/_health?wait_for_status=green&timeout=0ms"),
        ),
    )
    .await
    .expect("health timeout must bound admission");
    drop(held);

    assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON health");
    assert_eq!(body["status"], "red");
    assert_eq!(body["timed_out"], true);
    assert_eq!(
        body["reason"],
        "requested health status was not reached before timeout"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_without_a_status_wait_still_reports_probe_expiry() {
    let state = test_state(&seed());
    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("permit");
    let (status, _, bytes) = send_raw(&state, req_empty("GET", "/_health?timeout=0ms")).await;
    drop(held);

    assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON health");
    assert_eq!(body["status"], "red");
    assert_eq!(body["timed_out"], true);
    assert_eq!(
        body["reason"],
        "health dependency probe did not complete before timeout"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_transport_contract_rejects_unknown_body_method_and_oversize() {
    let state = test_state(&seed());
    let (status, body) = send(&state, req_empty("GET", "/_health?unknown=true")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "validation_error");

    let (status, body) = send(
        &state,
        Request::builder()
            .method(Method::GET)
            .uri("/_health")
            .body(Body::from("not empty"))
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "validation_error");

    let (status, headers, bytes) = send_raw(&state, req_empty("POST", "/_health")).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "GET, HEAD");
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], "method_not_allowed");

    let oversized = vec![b'x'; crate::handlers::HEALTH_BODY_LIMIT + 1];
    let (status, _, bytes) = send_raw(
        &state,
        Request::builder()
            .method(Method::GET)
            .uri("/_health")
            .body(Body::from(oversized))
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], "payload_too_large");
}

struct BrokenControlPlane;

impl ControlPlane for BrokenControlPlane {
    fn cluster_state(&self) -> Result<Arc<ClusterState>, ControlError> {
        Err(ControlError::Backend("offline-secret-detail".to_string()))
    }

    fn version(&self) -> Result<StateVersion, ControlError> {
        Err(ControlError::Backend("offline-secret-detail".to_string()))
    }

    fn propose(&self, _: ClusterStateChange) -> Result<StateVersion, ControlError> {
        Err(ControlError::Backend("offline-secret-detail".to_string()))
    }

    fn change_membership(&self, _: Vec<NodeId>) -> Result<StateVersion, ControlError> {
        Err(ControlError::Backend("offline-secret-detail".to_string()))
    }

    fn leader(&self) -> Result<Option<NodeId>, ControlError> {
        Err(ControlError::Backend("offline-secret-detail".to_string()))
    }
}

fn broken_control_state() -> Arc<ClusterAppState> {
    let config = reverse_rusty::cluster::ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..Default::default()
    };
    let cluster = reverse_rusty::cluster::ClusterEngine::build(
        Normalizer::default_vocab().expect("vocab"),
        &config,
        &seed(),
    )
    .expect("cluster")
    .with_control_plane(Box::new(BrokenControlPlane));
    state_from_cluster(cluster)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_plane_failure_is_red_unavailable_and_sanitized() {
    let state = broken_control_state();
    let (status, headers, bytes) = send_raw(&state, req_empty("GET", "/_health")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON health");
    assert_eq!(body["status"], "red");
    assert_eq!(body["mode"], "cluster");
    assert_eq!(body["timed_out"], false);
    assert_eq!(
        body["reason"],
        "required shard or control-plane probe failed"
    );
    assert!(!String::from_utf8(bytes.to_vec())
        .expect("UTF-8 JSON")
        .contains("offline-secret-detail"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unmet_wait_for_status_returns_request_timeout() {
    let state = broken_control_state();
    let (status, headers, bytes) = send_raw(
        &state,
        req_empty("GET", "/_health?wait_for_status=green&timeout=0ms"),
    )
    .await;
    assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON health");
    assert_eq!(body["status"], "red");
    assert_eq!(body["timed_out"], true);
    assert_eq!(
        body["reason"],
        "requested health status was not reached before timeout"
    );
}
