use std::convert::Infallible;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::http::header;
use reverse_rusty::cluster::{
    ClusterState, ClusterStateChange, ControlError, ControlPlane, InMemoryControlPlane,
    NodeDescriptor, NodeId, NodeRole, StateVersion,
};

use super::*;

fn rebalance_request(uri: &str, body: impl Into<Body>) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body.into())
        .expect("request")
}

fn state_with_control(control: Box<dyn ControlPlane>) -> Arc<ClusterAppState> {
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
    .expect("cluster")
    .with_control_plane(control);
    state_from_cluster(cluster)
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
            super::super::admin::CLUSTER_REBALANCE_BODY_TIMEOUT + Duration::from_millis(25),
        );
        Poll::Ready(Some(Ok(Bytes::from_static(br#"{"move":false}"#))))
    }
}

#[tokio::test]
async fn map_only_rebalance_reports_the_attested_version_and_changed_count() {
    let state = test_state(&seed());
    let (status, registered) = send(
        &state,
        req(
            "POST",
            "/_cluster/nodes",
            &serde_json::json!({
                "id": 7,
                "addr": "http://127.0.0.1:50057",
                "role": "data",
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{registered}");
    let before = state.cluster.read().control_state().expect("state");

    let (status, headers, bytes) = send_raw(
        &state,
        req_empty("POST", "/_cluster/rebalance?master_timeout=0"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{bytes:?}");
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert_eq!(
        headers.get(header::CONTENT_TYPE).expect("content type"),
        "application/json"
    );
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON");
    assert_eq!(body["acknowledged"], true, "{body}");
    assert_eq!(body["moved_data"], false, "{body}");
    assert_eq!(body["moved"], serde_json::json!([]), "{body}");
    assert_eq!(body["failed"], serde_json::Value::Null, "{body}");
    assert_eq!(body["not_attempted"], serde_json::json!([]), "{body}");

    let after = state.cluster.read().control_state().expect("state");
    let reassigned = body["reassigned"].as_u64().expect("reassigned");
    assert!(
        reassigned > 0,
        "the new data node must receive work: {body}"
    );
    assert_eq!(after.epoch, body["version"].as_u64().expect("version"));
    assert_eq!(after.epoch - before.epoch, reassigned);
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["cluster_rebalance", "200"])
            .get(),
        1
    );
    assert_eq!(
        state
            .prom
            .http_request_duration
            .with_label_values(&["cluster_rebalance"])
            .get_sample_count(),
        1
    );
}

#[tokio::test]
async fn query_method_media_and_json_controls_are_strict() {
    let state = test_state(&seed());

    for query in [
        "unknown=true",
        "master_timeout=1s&master_timeout=2s",
        "master_timeout=soon",
        "master_timeout=31s",
        "master_timeout=1s&cluster_manager_timeout=1s",
        "timeout=1s",
    ] {
        let (status, _, bytes) = send_raw(
            &state,
            rebalance_request(&format!("/_cluster/rebalance?{query}"), Body::empty()),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "query `{query}` was accepted: {bytes:?}"
        );
        assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");
    }

    for raw in [
        "{",
        "[]",
        r#"{"unknown":true}"#,
        r#"{"move":null}"#,
        r#"{"max_parallel":null}"#,
        r#"{"max_parallel":0}"#,
        r#"{"move":true,"move":false}"#,
        r#"{"move":false,"max_parallel":2}"#,
    ] {
        let (status, _, bytes) = send_raw(
            &state,
            rebalance_request("/_cluster/rebalance", Body::from(raw)),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "body `{raw}` was accepted: {bytes:?}"
        );
        assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");
    }

    for content_type in [None, Some("text/plain")] {
        let mut request = Request::builder().method("POST").uri("/_cluster/rebalance");
        if let Some(content_type) = content_type {
            request = request.header(header::CONTENT_TYPE, content_type);
        }
        let (status, _, bytes) = send_raw(
            &state,
            request
                .body(Body::from(r#"{"move":false}"#))
                .expect("request"),
        )
        .await;
        assert_error(
            status,
            &bytes,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
        );
    }

    let (status, body) = send(
        &state,
        rebalance_request("/_cluster/rebalance", r#"{"max_parallel":2}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "validation_error");

    let (status, body) = send(
        &state,
        rebalance_request("/_cluster/rebalance", r#"{"move":true}"#),
    )
    .await;
    #[cfg(feature = "distributed")]
    {
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"]["type"], "validation_error");
    }
    #[cfg(not(feature = "distributed"))]
    {
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{body}");
        assert_eq!(body["error"]["type"], "not_supported_in_cluster_mode");
    }

    let oversized = vec![b'x'; CLUSTER_REBALANCE_BODY_LIMIT + 1];
    let (status, _, bytes) =
        send_raw(&state, rebalance_request("/_cluster/rebalance", oversized)).await;
    assert_error(
        status,
        &bytes,
        StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
    );

    let pending = Body::from_stream(tokio_stream::pending::<Result<Bytes, Infallible>>());
    let (status, _, bytes) = tokio::time::timeout(
        Duration::from_secs(1),
        send_raw(&state, rebalance_request("/_cluster/rebalance", pending)),
    )
    .await
    .expect("fixed body deadline");
    assert_error(
        status,
        &bytes,
        StatusCode::REQUEST_TIMEOUT,
        "request_timeout",
    );

    let (status, headers, bytes) = send_raw(&state, req_empty("GET", "/_cluster/rebalance")).await;
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "POST");
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert_error(
        status,
        &bytes,
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
    );
}

#[tokio::test]
async fn body_ready_after_the_absolute_deadline_is_rejected() {
    let state = test_state(&seed());
    let body = Body::from_stream(LateReadyBodyStream { delivered: false });
    let (status, _, bytes) = tokio::time::timeout(
        Duration::from_secs(1),
        send_raw(&state, rebalance_request("/_cluster/rebalance", body)),
    )
    .await
    .expect("late-ready body check remains bounded");
    assert_error(
        status,
        &bytes,
        StatusCode::REQUEST_TIMEOUT,
        "request_timeout",
    );
}

#[tokio::test]
async fn admission_and_topology_deadlines_do_not_start_a_rebalance() {
    let state = test_state(&seed());
    let before = state.cluster.read().control_state().expect("state");
    let held = Arc::clone(&state.rebalance_permits)
        .acquire_owned()
        .await
        .expect("rebalance permit");

    for timeout in ["0", "25ms"] {
        let (status, _, bytes) = send_raw(
            &state,
            rebalance_request(
                &format!("/_cluster/rebalance?master_timeout={timeout}"),
                Body::empty(),
            ),
        )
        .await;
        assert_error(
            status,
            &bytes,
            StatusCode::REQUEST_TIMEOUT,
            "rebalance_timeout",
        );
        assert!(
            String::from_utf8_lossy(&bytes).contains("no rebalance was started"),
            "{bytes:?}"
        );
    }
    drop(held);

    let topology_state = Arc::clone(&state);
    let (locked_sender, locked_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
    let topology_holder = std::thread::spawn(move || {
        let _topology = topology_state.topology_guard.write();
        locked_sender.send(()).expect("signal topology lock");
        release_receiver.recv().expect("release topology lock");
    });
    locked_receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("topology lock held");
    for timeout in ["0", "25ms"] {
        let (status, _, bytes) = send_raw(
            &state,
            rebalance_request(
                &format!("/_cluster/rebalance?cluster_manager_timeout={timeout}"),
                Body::empty(),
            ),
        )
        .await;
        assert_error(
            status,
            &bytes,
            StatusCode::REQUEST_TIMEOUT,
            "rebalance_timeout",
        );
    }
    release_sender.send(()).expect("release topology holder");
    topology_holder.join().expect("topology holder");
    assert_eq!(
        state.cluster.read().control_state().expect("state"),
        before,
        "admission and topology timeouts must not mutate state"
    );

    let closed = test_state(&seed());
    closed.rebalance_permits.close();
    let (status, _, bytes) = send_raw(
        &closed,
        rebalance_request("/_cluster/rebalance", Body::empty()),
    )
    .await;
    assert_error(
        status,
        &bytes,
        StatusCode::SERVICE_UNAVAILABLE,
        "rebalance_unavailable",
    );
}

#[test]
fn zero_timeout_dispatch_is_independent_of_the_shared_blocking_pool() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let state = test_state(&seed());
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let blocker = tokio::task::spawn_blocking(move || {
            started_tx.send(()).expect("signal blocker start");
            release_rx.recv().expect("release blocker");
        });
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("blocking pool is occupied");

        let (status, _, bytes) = tokio::time::timeout(
            Duration::from_secs(1),
            send_raw(
                &state,
                rebalance_request(
                    "/_cluster/rebalance?cluster_manager_timeout=0",
                    Body::empty(),
                ),
            ),
        )
        .await
        .expect("dedicated rebalance dispatch remains bounded");
        assert_eq!(status, StatusCode::OK, "{bytes:?}");

        release_tx.send(()).expect("release blocking pool");
        blocker.await.expect("blocking-pool blocker");
        assert_eq!(
            state.rebalance_permits.available_permits(),
            1,
            "completed dedicated worker released admission"
        );
    });
}

struct BlockingProposalControlPlane {
    inner: InMemoryControlPlane,
    started: Arc<AtomicBool>,
    off_request_thread: Arc<AtomicBool>,
    request_thread: std::thread::ThreadId,
    release: Arc<Barrier>,
}

impl ControlPlane for BlockingProposalControlPlane {
    fn cluster_state(&self) -> Result<Arc<ClusterState>, ControlError> {
        self.inner.cluster_state()
    }

    fn version(&self) -> Result<StateVersion, ControlError> {
        self.inner.version()
    }

    fn propose(&self, change: ClusterStateChange) -> Result<StateVersion, ControlError> {
        self.off_request_thread.store(
            std::thread::current().id() != self.request_thread,
            Ordering::SeqCst,
        );
        if !self.started.swap(true, Ordering::AcqRel) {
            self.release.wait();
        }
        self.inner.propose(change)
    }

    fn change_membership(&self, voters: Vec<NodeId>) -> Result<StateVersion, ControlError> {
        self.inner.change_membership(voters)
    }

    fn leader(&self) -> Result<Option<NodeId>, ControlError> {
        self.inner.leader()
    }
}

fn changed_initial_state() -> ClusterState {
    let base = test_state(&seed());
    let mut state = base.cluster.read().control_state().expect("state");
    state.nodes.push(NodeDescriptor {
        id: NodeId(7),
        addr: Some("http://127.0.0.1:50057".into()),
        role: NodeRole::Data,
    });
    // Guarantee at least one planner change without depending on the HRW
    // fixture's exact winner for this position.
    state.assignments[0].primary = NodeId(u64::MAX);
    state
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_timeout_only_bounds_start_and_disconnect_retains_admission() {
    let request_thread = std::thread::current().id();
    let started = Arc::new(AtomicBool::new(false));
    let off_request_thread = Arc::new(AtomicBool::new(false));
    let release = Arc::new(Barrier::new(2));
    let state = state_with_control(Box::new(BlockingProposalControlPlane {
        inner: InMemoryControlPlane::new(changed_initial_state()),
        started: Arc::clone(&started),
        off_request_thread: Arc::clone(&off_request_thread),
        request_thread,
        release: Arc::clone(&release),
    }));

    let task_state = Arc::clone(&state);
    let request = tokio::spawn(async move {
        send_raw(
            &task_state,
            rebalance_request(
                "/_cluster/rebalance?cluster_manager_timeout=25ms",
                Body::empty(),
            ),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("proposal started");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !request.is_finished(),
        "manager timeout must not claim to cancel an operation that already started"
    );
    assert!(off_request_thread.load(Ordering::SeqCst));
    assert_eq!(state.rebalance_permits.available_permits(), 0);

    request.abort();
    assert_eq!(
        state.rebalance_permits.available_permits(),
        0,
        "a disconnected request must not release the worker's admission"
    );
    release.wait();
    tokio::time::timeout(Duration::from_secs(1), async {
        while state.rebalance_permits.available_permits() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached worker released admission after completion");
    assert_ne!(
        state
            .cluster
            .read()
            .control_state()
            .expect("state")
            .assignments[0]
            .primary,
        NodeId(u64::MAX),
        "the detached, already-started rebalance must complete"
    );
}

struct FailingProposalControlPlane {
    state: Arc<ClusterState>,
}

impl ControlPlane for FailingProposalControlPlane {
    fn cluster_state(&self) -> Result<Arc<ClusterState>, ControlError> {
        Ok(Arc::clone(&self.state))
    }

    fn version(&self) -> Result<StateVersion, ControlError> {
        Ok(StateVersion(self.state.epoch))
    }

    fn propose(&self, _: ClusterStateChange) -> Result<StateVersion, ControlError> {
        Err(ControlError::Backend(
            "secret manager endpoint and transport detail".into(),
        ))
    }

    fn change_membership(&self, _: Vec<NodeId>) -> Result<StateVersion, ControlError> {
        Err(ControlError::Backend("not used".into()))
    }

    fn leader(&self) -> Result<Option<NodeId>, ControlError> {
        Ok(Some(NodeId(0)))
    }
}

#[tokio::test]
async fn control_failure_is_fail_loud_but_sanitized() {
    let state = state_with_control(Box::new(FailingProposalControlPlane {
        state: Arc::new(changed_initial_state()),
    }));
    let (status, headers, bytes) = send_raw(
        &state,
        rebalance_request("/_cluster/rebalance", Body::empty()),
    )
    .await;
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert_error(
        status,
        &bytes,
        StatusCode::SERVICE_UNAVAILABLE,
        "control_plane_error",
    );
    assert!(
        !String::from_utf8_lossy(&bytes).contains("secret manager"),
        "backend details must remain server-side"
    );
}

fn assert_error(status: StatusCode, bytes: &Bytes, expected: StatusCode, kind: &str) {
    assert_eq!(status, expected, "{bytes:?}");
    let body: serde_json::Value = serde_json::from_slice(bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], kind, "{body}");
}
