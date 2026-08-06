use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::http::header;

use super::*;

fn reconcile_request(uri: &str, body: impl Into<Body>) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body.into())
        .expect("request")
}

#[cfg(feature = "distributed")]
fn state_with_topology(topology: crate::state::ClusterRebalanceTopology) -> Arc<ClusterAppState> {
    let config = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(
        Normalizer::default_vocab().expect("vocab"),
        &config,
        &seed(),
    )
    .expect("cluster");
    state_from_cluster_with_rebalance_topology(cluster, topology)
}

struct LateReadyBodyStream {
    delivered: bool,
}

impl tokio_stream::Stream for LateReadyBodyStream {
    type Item = Result<Bytes, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.delivered {
            return Poll::Ready(None);
        }
        self.delivered = true;
        std::thread::sleep(
            super::super::admin::CLUSTER_RECONCILE_BODY_TIMEOUT + Duration::from_millis(25),
        );
        Poll::Ready(Some(Ok(Bytes::from_static(br#"{"max_parallel":1}"#))))
    }
}

#[cfg(feature = "distributed")]
#[tokio::test]
async fn resolve_only_empty_pass_is_attested_and_observable() {
    let state = state_with_topology(crate::state::ClusterRebalanceTopology::ResolveOnlyRemote);
    let (status, headers, bytes) = send_raw(
        &state,
        req_empty("POST", "/_cluster/reconcile?master_timeout=0"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{bytes:?}");
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON");
    assert_eq!(body["acknowledged"], true, "{body}");
    assert_eq!(body["converged"], true, "{body}");
    assert!(body["version"].is_u64(), "{body}");
    assert!(body["took"].is_u64(), "{body}");
    assert!(body["took_ms"].is_number(), "{body}");
    assert_eq!(body["reconciled"], serde_json::json!([]), "{body}");
    assert_eq!(body["uncommitted"], serde_json::json!([]), "{body}");
    assert_eq!(body["failed"], serde_json::json!([]), "{body}");
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["cluster_reconcile", "200"])
            .get(),
        1
    );
    assert_eq!(
        state
            .prom
            .http_request_duration
            .with_label_values(&["cluster_reconcile"])
            .get_sample_count(),
        1
    );
}

#[cfg(not(feature = "distributed"))]
#[tokio::test]
async fn valid_request_is_an_explicit_feature_gated_501() {
    let state = test_state(&seed());
    let (status, headers, bytes) = send_raw(&state, req_empty("POST", "/_cluster/reconcile")).await;
    assert_error(
        status,
        &bytes,
        StatusCode::NOT_IMPLEMENTED,
        "not_supported_in_cluster_mode",
    );
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
}

#[cfg(feature = "distributed")]
#[tokio::test]
async fn unsafe_topologies_are_rejected_before_admission() {
    let cases = [
        (
            crate::state::ClusterRebalanceTopology::StaticRemote,
            StatusCode::CONFLICT,
            "reconcile_routing_not_authoritative",
        ),
        (
            crate::state::ClusterRebalanceTopology::CliSeededAssignmentRemote,
            StatusCode::CONFLICT,
            "reconcile_resolve_only_required",
        ),
        (
            crate::state::ClusterRebalanceTopology::InProcess,
            StatusCode::BAD_REQUEST,
            "reconcile_requires_remote_cluster",
        ),
    ];
    for (topology, expected_status, expected_type) in cases {
        let state = state_with_topology(topology);
        let _held = Arc::clone(&state.reconcile_permits)
            .acquire_owned()
            .await
            .expect("hold admission");
        let (status, _, bytes) = send_raw(
            &state,
            req_empty("POST", "/_cluster/reconcile?cluster_manager_timeout=0"),
        )
        .await;
        assert_error(status, &bytes, expected_status, expected_type);
    }
}

#[cfg(feature = "distributed")]
#[tokio::test]
async fn zero_manager_timeout_is_a_non_waiting_admission_probe() {
    let state = state_with_topology(crate::state::ClusterRebalanceTopology::ResolveOnlyRemote);
    let _held = Arc::clone(&state.reconcile_permits)
        .acquire_owned()
        .await
        .expect("hold admission");
    let (status, _, bytes) = send_raw(
        &state,
        req_empty("POST", "/_cluster/reconcile?cluster_manager_timeout=0"),
    )
    .await;
    assert_error(
        status,
        &bytes,
        StatusCode::REQUEST_TIMEOUT,
        "reconcile_timeout",
    );
}

#[cfg(feature = "distributed")]
#[tokio::test]
async fn closed_admission_is_explicitly_unavailable() {
    let state = state_with_topology(crate::state::ClusterRebalanceTopology::ResolveOnlyRemote);
    state.reconcile_permits.close();
    let (status, _, bytes) = send_raw(
        &state,
        req_empty("POST", "/_cluster/reconcile?cluster_manager_timeout=0"),
    )
    .await;
    assert_error(
        status,
        &bytes,
        StatusCode::SERVICE_UNAVAILABLE,
        "reconcile_unavailable",
    );
}

#[cfg(feature = "distributed")]
#[tokio::test]
async fn positive_manager_timeout_bounds_admission_without_late_start() {
    let state = state_with_topology(crate::state::ClusterRebalanceTopology::ResolveOnlyRemote);
    let held = Arc::clone(&state.reconcile_permits)
        .acquire_owned()
        .await
        .expect("hold admission");
    let (status, _, bytes) = send_raw(
        &state,
        req_empty("POST", "/_cluster/reconcile?cluster_manager_timeout=1ms"),
    )
    .await;
    assert_error(
        status,
        &bytes,
        StatusCode::REQUEST_TIMEOUT,
        "reconcile_timeout",
    );
    drop(held);
    assert_eq!(state.reconcile_permits.available_permits(), 1);
}

#[tokio::test]
async fn method_query_and_json_contract_is_strict() {
    let state = test_state(&seed());
    let requests = [
        req_empty("GET", "/_cluster/reconcile"),
        req_empty("POST", "/_cluster/reconcile?timeout=1s"),
        req_empty(
            "POST",
            "/_cluster/reconcile?master_timeout=1s&cluster_manager_timeout=1s",
        ),
        reconcile_request("/_cluster/reconcile", Body::from("[]")),
        reconcile_request("/_cluster/reconcile", Body::from(r#"{"max_parallel":0}"#)),
        reconcile_request(
            "/_cluster/reconcile",
            Body::from(r#"{"max_parallel":null}"#),
        ),
        reconcile_request(
            "/_cluster/reconcile",
            Body::from(r#"{"max_parallel":1,"max_parallel":2}"#),
        ),
        reconcile_request("/_cluster/reconcile", Body::from(r#"{"unknown":1}"#)),
    ];
    let expected = [
        (StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed"),
        (StatusCode::BAD_REQUEST, "validation_error"),
        (StatusCode::BAD_REQUEST, "validation_error"),
        (StatusCode::BAD_REQUEST, "validation_error"),
        (StatusCode::BAD_REQUEST, "validation_error"),
        (StatusCode::BAD_REQUEST, "validation_error"),
        (StatusCode::BAD_REQUEST, "validation_error"),
        (StatusCode::BAD_REQUEST, "validation_error"),
    ];

    for (request, (expected_status, expected_type)) in requests.into_iter().zip(expected) {
        let (status, headers, bytes) = send_raw(&state, request).await;
        assert_error(status, &bytes, expected_status, expected_type);
        assert_eq!(
            headers.get(header::CACHE_CONTROL).expect("cache"),
            "no-store"
        );
    }

    let (status, headers, _) = send_raw(&state, req_empty("GET", "/_cluster/reconcile")).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "POST");
}

#[tokio::test]
async fn nonempty_body_requires_json_but_empty_body_does_not() {
    let state = test_state(&seed());
    let request = Request::builder()
        .method("POST")
        .uri("/_cluster/reconcile")
        .body(Body::from(r#"{"max_parallel":1}"#))
        .expect("request");
    let (status, _, bytes) = send_raw(&state, request).await;
    assert_error(
        status,
        &bytes,
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "unsupported_media_type",
    );

    let request = Request::builder()
        .method("POST")
        .uri("/_cluster/reconcile")
        .header(header::CONTENT_TYPE, "application/vnd.reverse-rusty+json")
        .body(Body::from(r#"{"max_parallel":1}"#))
        .expect("request");
    let (status, _, bytes) = send_raw(&state, request).await;
    #[cfg(feature = "distributed")]
    assert_error(
        status,
        &bytes,
        StatusCode::BAD_REQUEST,
        "reconcile_requires_remote_cluster",
    );
    #[cfg(not(feature = "distributed"))]
    assert_error(
        status,
        &bytes,
        StatusCode::NOT_IMPLEMENTED,
        "not_supported_in_cluster_mode",
    );
}

#[tokio::test]
async fn route_enforces_the_dedicated_body_limit() {
    let state = test_state(&seed());
    let oversized = vec![b' '; CLUSTER_RECONCILE_BODY_LIMIT + 1];
    let (status, _, bytes) = send_raw(
        &state,
        reconcile_request("/_cluster/reconcile", Body::from(oversized)),
    )
    .await;
    assert_error(
        status,
        &bytes,
        StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
    );
}

#[tokio::test]
async fn body_read_has_a_wall_clock_deadline() {
    let state = test_state(&seed());
    let request = Request::builder()
        .method("POST")
        .uri("/_cluster/reconcile")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from_stream(LateReadyBodyStream { delivered: false }))
        .expect("request");
    let (status, _, bytes) = send_raw(&state, request).await;
    assert_error(
        status,
        &bytes,
        StatusCode::REQUEST_TIMEOUT,
        "request_timeout",
    );
}

fn assert_error(status: StatusCode, bytes: &Bytes, expected: StatusCode, kind: &str) {
    assert_eq!(status, expected, "{}", String::from_utf8_lossy(bytes));
    let body: serde_json::Value = serde_json::from_slice(bytes).expect("error JSON");
    assert_eq!(body["error"]["type"], kind, "{body}");
}
