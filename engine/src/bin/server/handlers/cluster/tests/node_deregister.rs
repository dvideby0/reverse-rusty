use std::convert::Infallible;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::task::{Context, Poll};
use std::time::Duration;

use super::*;
use axum::http::header;
use reverse_rusty::cluster::{
    ClusterState, ClusterStateChange, ControlError, ControlPlane, InMemoryControlPlane,
    NodeDescriptor, NodeId, NodeRole, StateVersion,
};

fn node(id: u64, role: NodeRole) -> NodeDescriptor {
    NodeDescriptor {
        id: NodeId(id),
        addr: Some(format!("http://127.0.0.1:{}", 50050 + id)),
        role,
    }
}

fn deregister_request(uri: &str, body: impl Into<Body>) -> Request<Body> {
    Request::builder()
        .method("DELETE")
        .uri(uri)
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
            super::super::node_deregister::CLUSTER_NODE_DEREGISTER_BODY_TIMEOUT
                + Duration::from_millis(25),
        );
        Poll::Ready(Some(Ok(Bytes::new())))
    }
}

#[tokio::test]
async fn deregistration_commits_exact_version_without_changing_voters_or_assignments() {
    let state = test_state(&seed());
    state
        .cluster
        .read()
        .register_node(node(7, NodeRole::Data))
        .expect("register");
    let before = state.cluster.read().control_state().expect("state");

    let (status, headers, bytes) = send_raw(
        &state,
        deregister_request("/_cluster/nodes/7?master_timeout=0", Body::empty()),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
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
    assert_eq!(body["version"], before.epoch + 1, "{body}");
    assert_eq!(body["node_id"], 7, "{body}");

    let after = state.cluster.read().control_state().expect("state");
    assert_eq!(after.epoch, body["version"].as_u64().expect("version"));
    assert_eq!(after.voters, before.voters);
    assert_eq!(after.assignments, before.assignments);
    assert!(
        after
            .nodes
            .iter()
            .all(|registered| registered.id != NodeId(7)),
        "only the descriptor is removed"
    );

    let (status, second) = send(
        &state,
        deregister_request(
            "/_cluster/nodes/7?cluster_manager_timeout=1s",
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{second}");
    assert_eq!(
        second["version"].as_u64().expect("second version"),
        after.epoch + 1,
        "a repeated state-idempotent delete is still a committed transition"
    );
    let repeated = state.cluster.read().control_state().expect("state");
    assert_eq!(repeated.nodes, after.nodes);
    assert_eq!(repeated.voters, after.voters);
    assert_eq!(repeated.assignments, after.assignments);
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["cluster_node_deregister", "200"])
            .get(),
        2
    );
    assert_eq!(
        state
            .prom
            .http_request_duration
            .with_label_values(&["cluster_node_deregister"])
            .get_sample_count(),
        2
    );
}

#[tokio::test]
async fn deregistration_refuses_voters_and_assigned_nodes_without_proposing() {
    for (voter, assigned, expected_hint) in [
        (true, false, "control-plane voter"),
        (false, true, "1 shard assignment"),
        (true, true, "joint consensus and move its data"),
    ] {
        let base = test_state(&seed());
        base.cluster
            .read()
            .register_node(node(7, NodeRole::Data))
            .expect("register");
        let mut initial = base.cluster.read().control_state().expect("state");
        drop(base);
        if voter {
            initial.voters.push(NodeId(7));
            initial.voters.sort_unstable();
            initial.voters.dedup();
        }
        if assigned {
            initial.assignments[0].replicas.push(NodeId(7));
        }
        let state = state_with_control(Box::new(InMemoryControlPlane::new(initial.clone())));

        let (status, _, bytes) = send_raw(
            &state,
            deregister_request(
                "/_cluster/nodes/7?cluster_manager_timeout=1s",
                Body::empty(),
            ),
        )
        .await;
        assert_error(status, &bytes, StatusCode::CONFLICT, "node_in_use");
        assert!(
            String::from_utf8_lossy(&bytes).contains(expected_hint),
            "{bytes:?}"
        );
        assert_eq!(
            state.cluster.read().control_state().expect("state"),
            initial,
            "an in-use rejection must not propose or mutate cluster state"
        );
    }
}

#[tokio::test]
async fn path_query_method_and_body_transport_are_strict() {
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
            deregister_request(&format!("/_cluster/nodes/7?{query}"), Body::empty()),
        )
        .await;
        assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");
    }

    for id in ["0", "007", "-1", "node-7", "18446744073709551616", "%37"] {
        let (status, _, bytes) = send_raw(
            &state,
            deregister_request(&format!("/_cluster/nodes/{id}"), Body::empty()),
        )
        .await;
        assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");
    }

    let (status, _, bytes) = send_raw(
        &state,
        deregister_request("/_cluster/nodes/7", Body::from("{}")),
    )
    .await;
    assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");

    let oversized = vec![b'x'; CLUSTER_NODE_DEREGISTER_BODY_LIMIT + 1];
    let (status, _, bytes) =
        send_raw(&state, deregister_request("/_cluster/nodes/7", oversized)).await;
    assert_error(
        status,
        &bytes,
        StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
    );

    let pending = Body::from_stream(tokio_stream::pending::<Result<Bytes, Infallible>>());
    let (status, _, bytes) = tokio::time::timeout(
        Duration::from_secs(1),
        send_raw(&state, deregister_request("/_cluster/nodes/7", pending)),
    )
    .await
    .expect("fixed body deadline");
    assert_error(
        status,
        &bytes,
        StatusCode::REQUEST_TIMEOUT,
        "request_timeout",
    );

    let request = Request::builder()
        .method("POST")
        .uri("/_cluster/nodes/7")
        .body(Body::empty())
        .expect("request");
    let (status, headers, bytes) = send_raw(&state, request).await;
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "DELETE");
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
        send_raw(&state, deregister_request("/_cluster/nodes/7", body)),
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
async fn admission_deadlines_do_not_start_a_proposal() {
    let state = test_state(&seed());
    state
        .cluster
        .read()
        .register_node(node(7, NodeRole::Data))
        .expect("register");
    let before = state.cluster.read().control_state().expect("state");
    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("admin permit");

    for timeout in ["0", "25ms"] {
        let (status, _, bytes) = send_raw(
            &state,
            deregister_request(
                &format!("/_cluster/nodes/7?master_timeout={timeout}"),
                Body::empty(),
            ),
        )
        .await;
        assert_error(
            status,
            &bytes,
            StatusCode::REQUEST_TIMEOUT,
            "node_deregistration_timeout",
        );
        assert!(
            String::from_utf8_lossy(&bytes).contains("no proposal was started"),
            "{bytes:?}"
        );
    }
    drop(held);
    assert_eq!(
        state.cluster.read().control_state().expect("state"),
        before,
        "admission timeout must not mutate cluster state"
    );

    let closed = test_state(&seed());
    closed.stats_permits.close();
    let (status, _, bytes) = send_raw(
        &closed,
        deregister_request("/_cluster/nodes/7", Body::empty()),
    )
    .await;
    assert_error(
        status,
        &bytes,
        StatusCode::SERVICE_UNAVAILABLE,
        "node_deregistration_unavailable",
    );
}

#[test]
fn blocking_pool_queue_cannot_start_a_proposal_after_the_deadline() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let state = test_state(&seed());
        state
            .cluster
            .read()
            .register_node(node(7, NodeRole::Data))
            .expect("register");
        let before = state.cluster.read().control_state().expect("state");
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
                deregister_request(
                    "/_cluster/nodes/7?cluster_manager_timeout=25ms",
                    Body::empty(),
                ),
            ),
        )
        .await
        .expect("queued deregistration request remains bounded");
        assert_error(
            status,
            &bytes,
            StatusCode::REQUEST_TIMEOUT,
            "node_deregistration_timeout",
        );
        assert!(
            String::from_utf8_lossy(&bytes).contains("no proposal was started"),
            "{bytes:?}"
        );

        release_tx.send(()).expect("release blocking pool");
        blocker.await.expect("blocking-pool blocker");
        tokio::time::timeout(Duration::from_secs(1), async {
            while state.stats_permits.available_permits() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled queued worker released admission");
        assert_eq!(
            state.cluster.read().control_state().expect("state"),
            before,
            "a request-deadline cancellation must prevent the queued proposal"
        );
    });
}

struct BlockingProposalControlPlane {
    inner: InMemoryControlPlane,
    calls: Arc<AtomicUsize>,
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
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.off_request_thread.store(
            std::thread::current().id() != self.request_thread,
            Ordering::SeqCst,
        );
        self.started.store(true, Ordering::Release);
        self.release.wait();
        self.inner.propose(change)
    }

    fn change_membership(&self, voters: Vec<NodeId>) -> Result<StateVersion, ControlError> {
        self.inner.change_membership(voters)
    }

    fn leader(&self) -> Result<Option<NodeId>, ControlError> {
        self.inner.leader()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn timed_out_proposal_is_supervised_off_runtime_and_retains_admission() {
    let request_thread = std::thread::current().id();
    let base = test_state(&seed());
    base.cluster
        .read()
        .register_node(node(7, NodeRole::Data))
        .expect("register");
    let initial = base.cluster.read().control_state().expect("state");
    drop(base);
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(AtomicBool::new(false));
    let off_request_thread = Arc::new(AtomicBool::new(false));
    let release = Arc::new(Barrier::new(2));
    let state = state_with_control(Box::new(BlockingProposalControlPlane {
        inner: InMemoryControlPlane::new(initial),
        calls: Arc::clone(&calls),
        started: Arc::clone(&started),
        off_request_thread: Arc::clone(&off_request_thread),
        request_thread,
        release: Arc::clone(&release),
    }));

    let (status, _, bytes) = send_raw(
        &state,
        deregister_request(
            "/_cluster/nodes/7?cluster_manager_timeout=25ms",
            Body::empty(),
        ),
    )
    .await;
    assert_error(
        status,
        &bytes,
        StatusCode::REQUEST_TIMEOUT,
        "node_deregistration_timeout",
    );
    assert!(
        String::from_utf8_lossy(&bytes).contains("outcome is unknown"),
        "{bytes:?}"
    );
    assert!(started.load(Ordering::Acquire));
    assert!(off_request_thread.load(Ordering::SeqCst));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        state.stats_permits.available_permits(),
        0,
        "detached proposal must retain admission"
    );
    assert!(
        state.topology_guard.try_write().is_none(),
        "detached proposal must retain the exclusive topology guard"
    );
    assert!(
        state.cluster.try_read().is_some(),
        "a slow topology proposal must not exclude serving cluster reads"
    );

    release.wait();
    tokio::time::timeout(Duration::from_secs(1), async {
        while state.stats_permits.available_permits() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached proposal released admission");
    let committed = state.cluster.read().control_state().expect("state");
    assert!(
        committed
            .nodes
            .iter()
            .all(|registered| registered.id != NodeId(7)),
        "the timeout correctly reported an unknown outcome: the proposal committed later"
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
    let base = test_state(&seed());
    let initial = Arc::new(base.cluster.read().control_state().expect("state"));
    drop(base);
    let state = state_with_control(Box::new(FailingProposalControlPlane { state: initial }));
    let (status, headers, bytes) = send_raw(
        &state,
        deregister_request("/_cluster/nodes/7", Body::empty()),
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
