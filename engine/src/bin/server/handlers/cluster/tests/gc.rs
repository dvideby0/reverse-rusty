use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::http::header;

use super::*;

struct LateReadyBodyStream {
    delivered: bool,
    bytes: Bytes,
}

impl tokio_stream::Stream for LateReadyBodyStream {
    type Item = Result<Bytes, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.delivered {
            return Poll::Ready(None);
        }
        self.delivered = true;
        std::thread::sleep(
            super::super::admin::CLUSTER_GC_BODY_TIMEOUT + Duration::from_millis(25),
        );
        Poll::Ready(Some(Ok(self.bytes.clone())))
    }
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

#[cfg(feature = "distributed")]
#[tokio::test]
async fn assignment_routed_noop_is_attested_and_observable() {
    for topology in [
        crate::state::ClusterRebalanceTopology::ResolveOnlyRemote,
        crate::state::ClusterRebalanceTopology::CliSeededAssignmentRemote,
    ] {
        let state = state_with_topology(topology);
        let (status, headers, bytes) = send_raw(
            &state,
            req_empty("POST", "/_cluster/gc?cluster_manager_timeout=0"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{bytes:?}");
        assert_eq!(
            headers.get(header::CACHE_CONTROL).expect("cache"),
            "no-store"
        );
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON");
        assert_eq!(body["acknowledged"], true, "{body}");
        assert_eq!(body["completed"], true, "{body}");
        assert!(body["version"].is_u64(), "{body}");
        assert!(body["took"].is_u64(), "{body}");
        assert!(body["took_ms"].is_number(), "{body}");
        for field in [
            "dropped",
            "pending_disk_cleanup",
            "kept_live_routed",
            "skipped_unassigned",
            "failed",
            "skipped_nodes",
        ] {
            assert_eq!(body[field], serde_json::json!([]), "{field}: {body}");
        }
        assert_eq!(
            state
                .prom
                .http_requests_total
                .with_label_values(&["cluster_gc", "200"])
                .get(),
            1
        );
        assert_eq!(
            state
                .prom
                .http_request_duration
                .with_label_values(&["cluster_gc"])
                .get_sample_count(),
            1
        );
    }
}

#[cfg(not(feature = "distributed"))]
#[tokio::test]
async fn valid_request_is_an_explicit_feature_gated_501() {
    let state = test_state(&seed());
    let (status, headers, bytes) = send_raw(&state, req_empty("POST", "/_cluster/gc")).await;
    assert_gc_error(
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
async fn non_authoritative_topologies_are_rejected_before_admission() {
    let cases = [
        (
            crate::state::ClusterRebalanceTopology::StaticRemote,
            StatusCode::CONFLICT,
            "gc_assignment_routing_required",
        ),
        (
            crate::state::ClusterRebalanceTopology::InProcess,
            StatusCode::BAD_REQUEST,
            "gc_requires_remote_cluster",
        ),
    ];
    for (topology, expected_status, expected_type) in cases {
        let state = state_with_topology(topology);
        let _held = Arc::clone(&state.reconcile_permits)
            .acquire_owned()
            .await
            .expect("hold shared maintenance admission");
        let (status, _, bytes) = send_raw(
            &state,
            req_empty("POST", "/_cluster/gc?cluster_manager_timeout=0"),
        )
        .await;
        assert_gc_error(status, &bytes, expected_status, expected_type);
    }
}

#[cfg(feature = "distributed")]
#[tokio::test]
async fn manager_timeout_bounds_shared_maintenance_admission() {
    let state = state_with_topology(crate::state::ClusterRebalanceTopology::ResolveOnlyRemote);
    let held = Arc::clone(&state.reconcile_permits)
        .acquire_owned()
        .await
        .expect("hold shared maintenance admission");
    for timeout in ["0", "1ms"] {
        let (status, _, bytes) = send_raw(
            &state,
            req_empty(
                "POST",
                &format!("/_cluster/gc?cluster_manager_timeout={timeout}"),
            ),
        )
        .await;
        assert_gc_error(status, &bytes, StatusCode::REQUEST_TIMEOUT, "gc_timeout");
    }
    drop(held);
    assert_eq!(state.reconcile_permits.available_permits(), 1);

    state.reconcile_permits.close();
    let (status, _, bytes) = send_raw(&state, req_empty("POST", "/_cluster/gc")).await;
    assert_gc_error(
        status,
        &bytes,
        StatusCode::SERVICE_UNAVAILABLE,
        "gc_unavailable",
    );
}

#[tokio::test]
async fn method_query_and_body_contract_is_strict() {
    let state = test_state(&seed());
    let requests = [
        req_empty("GET", "/_cluster/gc"),
        req_empty("POST", "/_cluster/gc?unknown=true"),
        req_empty("POST", "/_cluster/gc?accept_data_loss=true"),
        req_empty("POST", "/_cluster/gc?dry_run=true"),
        req_empty("POST", "/_cluster/gc?timeout=1s"),
        req_empty(
            "POST",
            "/_cluster/gc?master_timeout=1s&cluster_manager_timeout=1s",
        ),
        req_empty("POST", "/_cluster/gc?master_timeout=31s"),
        req_empty("POST", "/_cluster/gc?master_timeout=1s&master_timeout=1s"),
        Request::builder()
            .method("POST")
            .uri("/_cluster/gc")
            .body(Body::from("{}"))
            .expect("request"),
        Request::builder()
            .method("POST")
            .uri("/_cluster/gc")
            .body(Body::from(" "))
            .expect("request"),
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
        (StatusCode::BAD_REQUEST, "validation_error"),
        (StatusCode::BAD_REQUEST, "validation_error"),
    ];
    for (request, (expected_status, expected_type)) in requests.into_iter().zip(expected) {
        let (status, headers, bytes) = send_raw(&state, request).await;
        assert_gc_error(status, &bytes, expected_status, expected_type);
        assert_eq!(
            headers.get(header::CACHE_CONTROL).expect("cache"),
            "no-store"
        );
    }

    let (status, headers, _) = send_raw(&state, req_empty("GET", "/_cluster/gc")).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "POST");
}

#[tokio::test]
async fn body_transport_is_bounded_by_size_and_wall_clock() {
    let state = test_state(&seed());
    let oversized = vec![b' '; super::super::admin::CLUSTER_GC_BODY_LIMIT + 1];
    let request = Request::builder()
        .method("POST")
        .uri("/_cluster/gc")
        .body(Body::from(oversized))
        .expect("request");
    let (status, _, bytes) = send_raw(&state, request).await;
    assert_gc_error(
        status,
        &bytes,
        StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
    );

    let late = Request::builder()
        .method("POST")
        .uri("/_cluster/gc")
        .body(Body::from_stream(LateReadyBodyStream {
            delivered: false,
            bytes: Bytes::new(),
        }))
        .expect("request");
    let (status, _, bytes) = send_raw(&state, late).await;
    assert_gc_error(
        status,
        &bytes,
        StatusCode::REQUEST_TIMEOUT,
        "request_timeout",
    );
}

fn assert_gc_error(status: StatusCode, bytes: &Bytes, expected: StatusCode, kind: &str) {
    assert_eq!(status, expected, "{}", String::from_utf8_lossy(bytes));
    let body: serde_json::Value = serde_json::from_slice(bytes).expect("error JSON");
    assert_eq!(body["error"]["type"], kind, "{body}");
}
