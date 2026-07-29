use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use super::*;
use axum::http::header;
use reverse_rusty::cluster::{
    ClusterState, ClusterStateChange, ControlError, ControlPlane, NodeDescriptor, NodeId, NodeRole,
    StateVersion,
};

struct BlockingControlPlane {
    state: Arc<ClusterState>,
    calls: Arc<AtomicUsize>,
    started: Arc<AtomicBool>,
    request_thread: std::thread::ThreadId,
    off_request_thread: Arc<AtomicBool>,
    release: Arc<Barrier>,
}

impl ControlPlane for BlockingControlPlane {
    fn cluster_state(&self) -> Result<Arc<ClusterState>, ControlError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.off_request_thread.store(
            std::thread::current().id() != self.request_thread,
            Ordering::SeqCst,
        );
        self.started.store(true, Ordering::SeqCst);
        self.release.wait();
        Ok(Arc::clone(&self.state))
    }

    fn version(&self) -> Result<StateVersion, ControlError> {
        Ok(StateVersion(self.state.epoch))
    }

    fn propose(&self, _: ClusterStateChange) -> Result<StateVersion, ControlError> {
        Err(ControlError::Backend("not used".into()))
    }

    fn change_membership(&self, _: Vec<NodeId>) -> Result<StateVersion, ControlError> {
        Err(ControlError::Backend("not used".into()))
    }

    fn leader(&self) -> Result<Option<NodeId>, ControlError> {
        Ok(Some(NodeId(0)))
    }
}

struct FixedControlPlane {
    result: Result<Arc<ClusterState>, ControlError>,
}

impl ControlPlane for FixedControlPlane {
    fn cluster_state(&self) -> Result<Arc<ClusterState>, ControlError> {
        self.result.clone()
    }

    fn version(&self) -> Result<StateVersion, ControlError> {
        self.result
            .as_ref()
            .map(|state| StateVersion(state.epoch))
            .map_err(Clone::clone)
    }

    fn propose(&self, _: ClusterStateChange) -> Result<StateVersion, ControlError> {
        Err(ControlError::Backend("not used".into()))
    }

    fn change_membership(&self, _: Vec<NodeId>) -> Result<StateVersion, ControlError> {
        Err(ControlError::Backend("not used".into()))
    }

    fn leader(&self) -> Result<Option<NodeId>, ControlError> {
        Ok(Some(NodeId(0)))
    }
}

struct CountingControlPlane {
    state: Arc<ClusterState>,
    calls: Arc<AtomicUsize>,
}

impl ControlPlane for CountingControlPlane {
    fn cluster_state(&self) -> Result<Arc<ClusterState>, ControlError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::clone(&self.state))
    }

    fn version(&self) -> Result<StateVersion, ControlError> {
        Ok(StateVersion(self.state.epoch))
    }

    fn propose(&self, _: ClusterStateChange) -> Result<StateVersion, ControlError> {
        Err(ControlError::Backend("not used".into()))
    }

    fn change_membership(&self, _: Vec<NodeId>) -> Result<StateVersion, ControlError> {
        Err(ControlError::Backend("not used".into()))
    }

    fn leader(&self) -> Result<Option<NodeId>, ControlError> {
        Ok(Some(NodeId(0)))
    }
}

struct VersionOnlyControlPlane {
    version: StateVersion,
}

impl ControlPlane for VersionOnlyControlPlane {
    fn cluster_state(&self) -> Result<Arc<ClusterState>, ControlError> {
        Err(ControlError::Backend(
            "full state is deliberately unavailable".into(),
        ))
    }

    fn version(&self) -> Result<StateVersion, ControlError> {
        Ok(self.version)
    }

    fn propose(&self, _: ClusterStateChange) -> Result<StateVersion, ControlError> {
        Err(ControlError::Backend("not used".into()))
    }

    fn change_membership(&self, _: Vec<NodeId>) -> Result<StateVersion, ControlError> {
        Err(ControlError::Backend("not used".into()))
    }

    fn leader(&self) -> Result<Option<NodeId>, ControlError> {
        Ok(Some(NodeId(0)))
    }
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

#[tokio::test]
async fn cluster_state_is_versioned_authoritative_uncacheable_and_observed() {
    let state = test_state(&seed());
    let (status, get_headers, bytes) = send_raw(&state, req_empty("GET", "/_cluster/state")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        get_headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert_eq!(
        get_headers.get(header::CONTENT_TYPE).expect("content type"),
        "application/json"
    );
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON");
    assert_eq!(body["version"], body["epoch"]);
    assert_eq!(body["num_shards"], 3);
    assert_eq!(
        body["assignments"].as_array().expect("assignments").len(),
        3
    );
    assert!(body["nodes"].is_array(), "{body}");

    let (status, head_headers, bytes) =
        send_raw(&state, req_empty("HEAD", "/_cluster/state")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        head_headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert!(bytes.is_empty());
    assert_eq!(
        head_headers.get(header::CONTENT_LENGTH),
        get_headers.get(header::CONTENT_LENGTH),
        "HEAD must preserve the corresponding GET representation length"
    );
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["cluster_state", "200"])
            .get(),
        2
    );
    assert_eq!(
        state
            .prom
            .http_request_duration
            .with_label_values(&["cluster_state"])
            .get_sample_count(),
        2
    );

    let (status, selected) = send(&state, req_empty("GET", "/_cluster/state/version")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        selected,
        serde_json::json!({"version": body["version"].clone()})
    );
    let (status, all) = send(&state, req_empty("GET", "/_cluster/state/_all")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(all["version"], body["version"]);
    assert_eq!(all["assignments"], body["assignments"]);

    let version_only = state_with_control(Box::new(VersionOnlyControlPlane {
        version: StateVersion(42),
    }));
    let (status, selected) = send(&version_only, req_empty("GET", "/_cluster/state/version")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(selected, serde_json::json!({"version": 42}));
    let (status, _, bytes) = send_raw(&version_only, req_empty("GET", "/_cluster/state")).await;
    assert_error(
        status,
        &bytes,
        StatusCode::SERVICE_UNAVAILABLE,
        "control_plane_error",
    );
}

#[tokio::test]
async fn cluster_state_transport_is_strict_bounded_and_keeps_exact_familiar_controls() {
    let state = test_state(&seed());

    for path in [
        "/_cluster/state?local=false",
        "/_cluster/state?flat_settings=true",
        "/_cluster/state?cluster_manager_timeout=1s",
        "/_cluster/state?master_timeout=1s",
        "/_cluster/state?cluster_manager_timeout=0ms",
        "/_cluster/state?master_timeout=0",
    ] {
        let (status, headers, _) = send_raw(&state, req_empty("GET", path)).await;
        assert_eq!(status, StatusCode::OK, "{path}");
        assert_eq!(
            headers.get(header::CACHE_CONTROL).expect("cache"),
            "no-store"
        );
    }

    for path in [
        "/_cluster/state?unknown=true",
        "/_cluster/state?local=false&local=false",
        "/_cluster/state?local=true",
        "/_cluster/state?wait_for_metadata_version=1",
        "/_cluster/state?cluster_manager_timeout=1s&master_timeout=1s",
        "/_cluster/state?cluster_manager_timeout=31s",
        "/_cluster/state/nodes",
        "/_cluster/state/version/catalog",
    ] {
        let (status, headers, bytes) = send_raw(&state, req_empty("GET", path)).await;
        assert_eq!(
            headers.get(header::CACHE_CONTROL).expect("cache"),
            "no-store"
        );
        assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");
    }

    let path = "/_cluster/state?local=true";
    let (get_status, get_headers, get_bytes) = send_raw(&state, req_empty("GET", path)).await;
    let (head_status, head_headers, head_bytes) = send_raw(&state, req_empty("HEAD", path)).await;
    assert_eq!(head_status, get_status);
    assert!(head_bytes.is_empty());
    assert_eq!(
        head_headers.get(header::CONTENT_LENGTH),
        get_headers.get(header::CONTENT_LENGTH),
        "error HEAD must preserve the corresponding GET representation length"
    );
    assert_error(
        get_status,
        &get_bytes,
        StatusCode::BAD_REQUEST,
        "validation_error",
    );

    let request = Request::get("/_cluster/state")
        .body(Body::from("{}"))
        .expect("request");
    let (status, _, bytes) = send_raw(&state, request).await;
    assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");

    let request = Request::get("/_cluster/state")
        .body(Body::from(vec![b'x'; CLUSTER_STATE_BODY_LIMIT + 1]))
        .expect("request");
    let (status, _, bytes) = send_raw(&state, request).await;
    assert_error(
        status,
        &bytes,
        StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
    );

    let pending = Body::from_stream(tokio_stream::pending::<Result<Bytes, Infallible>>());
    let request = Request::get("/_cluster/state")
        .body(pending)
        .expect("request");
    let (status, _, bytes) =
        tokio::time::timeout(Duration::from_secs(1), send_raw(&state, request))
            .await
            .expect("body deadline");
    assert_error(
        status,
        &bytes,
        StatusCode::REQUEST_TIMEOUT,
        "request_timeout",
    );

    let (status, headers, bytes) = send_raw(&state, req_empty("POST", "/_cluster/state")).await;
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "GET, HEAD");
    assert_error(
        status,
        &bytes,
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn cluster_state_waits_off_runtime_and_deadline_covers_shared_admission() {
    let request_thread = std::thread::current().id();
    let seed_cluster = test_state(&seed());
    let control_state = Arc::new(
        seed_cluster
            .cluster
            .read()
            .control_state()
            .expect("control state"),
    );
    drop(seed_cluster);
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(AtomicBool::new(false));
    let off_request_thread = Arc::new(AtomicBool::new(false));
    let release = Arc::new(Barrier::new(2));
    let state = state_with_control(Box::new(BlockingControlPlane {
        state: control_state,
        calls: Arc::clone(&calls),
        started: Arc::clone(&started),
        request_thread,
        off_request_thread: Arc::clone(&off_request_thread),
        release: Arc::clone(&release),
    }));

    let (status, headers, bytes) = tokio::time::timeout(
        Duration::from_secs(1),
        send_raw(
            &state,
            req_empty("GET", "/_cluster/state?cluster_manager_timeout=50ms"),
        ),
    )
    .await
    .expect("request deadline");
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert_error(
        status,
        &bytes,
        StatusCode::REQUEST_TIMEOUT,
        "cluster_state_timeout",
    );
    assert!(started.load(Ordering::SeqCst));
    assert!(off_request_thread.load(Ordering::SeqCst));
    assert_eq!(state.stats_permits.available_permits(), 0);
    release.wait();
    tokio::time::timeout(Duration::from_secs(1), async {
        while state.stats_permits.available_permits() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached read released admission");

    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("stats permit");
    let calls_before = calls.load(Ordering::SeqCst);
    let (status, _, bytes) =
        send_raw(&state, req_empty("GET", "/_cluster/state?master_timeout=0")).await;
    assert_error(
        status,
        &bytes,
        StatusCode::REQUEST_TIMEOUT,
        "cluster_state_timeout",
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        calls_before,
        "a zero timeout must not queue behind occupied admission"
    );

    let (status, _, bytes) = send_raw(
        &state,
        req_empty("GET", "/_cluster/state?cluster_manager_timeout=20ms"),
    )
    .await;
    assert_error(
        status,
        &bytes,
        StatusCode::REQUEST_TIMEOUT,
        "cluster_state_timeout",
    );
    assert_eq!(calls.load(Ordering::SeqCst), calls_before);
    drop(held);

    state.stats_permits.close();
    let (status, _, bytes) = send_raw(&state, req_empty("GET", "/_cluster/state")).await;
    assert_error(
        status,
        &bytes,
        StatusCode::SERVICE_UNAVAILABLE,
        "cluster_state_unavailable",
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cluster_state_does_not_start_work_when_admission_wakes_after_deadline() {
    let seed_cluster = test_state(&seed());
    let control_state = Arc::new(
        seed_cluster
            .cluster
            .read()
            .control_state()
            .expect("control state"),
    );
    drop(seed_cluster);
    let calls = Arc::new(AtomicUsize::new(0));
    let state = state_with_control(Box::new(CountingControlPlane {
        state: control_state,
        calls: Arc::clone(&calls),
    }));
    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("stats permit");
    let request_state = Arc::clone(&state);
    let request = tokio::spawn(async move {
        send_raw(
            &request_state,
            req_empty("GET", "/_cluster/state?cluster_manager_timeout=10ms"),
        )
        .await
    });
    tokio::task::yield_now().await;

    // Make admission ready only after the deadline, while starving this
    // current-thread runtime so both the semaphore and timeout are ready when
    // the request task resumes.
    let release_thread = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        drop(held);
    });
    std::thread::sleep(Duration::from_millis(30));

    let (status, _, bytes) = request.await.expect("cluster-state task");
    release_thread.join().expect("release thread");
    assert_error(
        status,
        &bytes,
        StatusCode::REQUEST_TIMEOUT,
        "cluster_state_timeout",
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "an expired admitted request must not start control-plane work"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cluster_state_rejects_a_worker_result_completed_after_deadline() {
    let request_thread = std::thread::current().id();
    let seed_cluster = test_state(&seed());
    let control_state = Arc::new(
        seed_cluster
            .cluster
            .read()
            .control_state()
            .expect("control state"),
    );
    drop(seed_cluster);
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(AtomicBool::new(false));
    let off_request_thread = Arc::new(AtomicBool::new(false));
    let release = Arc::new(Barrier::new(2));
    let state = state_with_control(Box::new(BlockingControlPlane {
        state: control_state,
        calls: Arc::clone(&calls),
        started: Arc::clone(&started),
        request_thread,
        off_request_thread,
        release: Arc::clone(&release),
    }));
    let request_state = Arc::clone(&state);
    let request = tokio::spawn(async move {
        send_raw(
            &request_state,
            req_empty("GET", "/_cluster/state?cluster_manager_timeout=10ms"),
        )
        .await
    });
    while !started.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }

    // Complete the worker after its deadline while starving the request
    // runtime. Tokio will see both the join and elapsed timer ready; the wall
    // clock check must make the deadline authoritative.
    let release_thread = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        release.wait();
    });
    std::thread::sleep(Duration::from_millis(30));

    let (status, _, bytes) = request.await.expect("cluster-state task");
    release_thread.join().expect("release thread");
    assert_error(
        status,
        &bytes,
        StatusCode::REQUEST_TIMEOUT,
        "cluster_state_timeout",
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cluster_state_fails_loud_sanitizes_backend_errors_and_bounds_output() {
    let failed = state_with_control(Box::new(FixedControlPlane {
        result: Err(ControlError::Backend(
            "secret manager endpoint and transport detail".into(),
        )),
    }));
    let (status, headers, bytes) = send_raw(&failed, req_empty("GET", "/_cluster/state")).await;
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

    let base = test_state(&seed());
    let mut oversized = base.cluster.read().control_state().expect("control state");
    drop(base);
    oversized.nodes.push(NodeDescriptor {
        id: NodeId(77),
        addr: Some("x".repeat(8 * 1024 * 1024)),
        role: NodeRole::Data,
    });
    let oversized = state_with_control(Box::new(FixedControlPlane {
        result: Ok(Arc::new(oversized)),
    }));
    let (status, _, bytes) = send_raw(&oversized, req_empty("GET", "/_cluster/state")).await;
    assert_error(
        status,
        &bytes,
        StatusCode::SERVICE_UNAVAILABLE,
        "cluster_state_too_large",
    );
}

fn assert_error(status: StatusCode, bytes: &Bytes, expected: StatusCode, kind: &str) {
    assert_eq!(status, expected);
    let body: serde_json::Value = serde_json::from_slice(bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], kind, "{body}");
}
