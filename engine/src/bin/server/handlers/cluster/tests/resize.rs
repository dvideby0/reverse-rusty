use std::convert::Infallible;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::http::header;
use reverse_rusty::cluster::{
    ClusterState, ClusterStateChange, ControlError, ControlPlane, InMemoryControlPlane, NodeId,
    StateVersion,
};

use super::*;

mod retry;

fn resize_request(uri: &str, body: impl Into<Body>) -> Request<Body> {
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
            super::super::admin::CLUSTER_RESIZE_BODY_TIMEOUT + Duration::from_millis(25),
        );
        Poll::Ready(Some(Ok(Bytes::from_static(br#"{"num_shards":4}"#))))
    }
}

#[tokio::test]
async fn resize_reports_final_state_and_preserves_matching() {
    let state = test_state(&seed());
    let before_version = state.cluster.read().control_version().expect("version").0;
    let (status, headers, bytes) = send_raw(
        &state,
        resize_request(
            "/_cluster/resize?cluster_manager_timeout=0",
            r#"{"num_shards":4}"#,
        ),
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
    assert_eq!(body["shards_acknowledged"], true, "{body}");
    assert_eq!(body["old_num_shards"], 3, "{body}");
    assert_eq!(body["num_shards"], 4, "{body}");
    assert_eq!(body["rebuilt"], 3, "{body}");
    assert!(
        body["version"].as_u64().expect("version") > before_version,
        "{body}"
    );
    assert_eq!(state.cluster.read().num_shards(), 4);

    let (status, search) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({"document": {"title": "1994 acme"}}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{search}");
    assert_eq!(search["hits"]["total"], 1, "{search}");
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["cluster_resize", "200"])
            .get(),
        1
    );
    assert_eq!(
        state
            .prom
            .http_request_duration
            .with_label_values(&["cluster_resize"])
            .get_sample_count(),
        1
    );
}

#[tokio::test]
async fn same_count_resize_is_an_acknowledged_noop() {
    let state = test_state(&seed());
    let (status, body) = send(
        &state,
        resize_request("/_cluster/resize", r#"{"num_shards":3}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["acknowledged"], true, "{body}");
    assert_eq!(body["shards_acknowledged"], true, "{body}");
    assert_eq!(body["old_num_shards"], 3, "{body}");
    assert_eq!(body["num_shards"], 3, "{body}");
    assert_eq!(body["rebuilt"], 0, "{body}");
}

#[tokio::test]
async fn resize_transport_is_strict_and_bounded() {
    let state = test_state(&seed());

    let (status, headers, bytes) = send_raw(
        &state,
        Request::builder()
            .method("GET")
            .uri("/_cluster/resize")
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert_error(
        status,
        &bytes,
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
    );
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "POST");

    for query in [
        "unknown=true",
        "timeout=1s",
        "master_timeout=1s&cluster_manager_timeout=1s",
        "master_timeout=31s",
        "master_timeout=bad",
        "master_timeout=1s&master_timeout=1s",
    ] {
        let (status, _, bytes) = send_raw(
            &state,
            resize_request(&format!("/_cluster/resize?{query}"), r#"{"num_shards":3}"#),
        )
        .await;
        assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");
    }

    let invalid_bodies = [
        "",
        "null",
        "[]",
        r#"{"num_shards":0}"#,
        r#"{"num_shards":1025}"#,
        r#"{"num_shards":3,"unknown":true}"#,
        r#"{"num_shards":3,"num_shards":4}"#,
        r#"{"num_shards":null}"#,
        r#"{"num_shards":"3"}"#,
    ];
    for raw in invalid_bodies {
        let (status, _, bytes) =
            send_raw(&state, resize_request("/_cluster/resize", Body::from(raw))).await;
        assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");
    }

    let (status, _, bytes) = send_raw(
        &state,
        Request::builder()
            .method("POST")
            .uri("/_cluster/resize")
            .body(Body::from(r#"{"num_shards":3}"#))
            .expect("request"),
    )
    .await;
    assert_error(
        status,
        &bytes,
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "unsupported_media_type",
    );

    let oversized = vec![b' '; super::super::admin::CLUSTER_RESIZE_BODY_LIMIT + 1];
    let (status, _, bytes) = send_raw(
        &state,
        resize_request("/_cluster/resize", Body::from(oversized)),
    )
    .await;
    assert_error(
        status,
        &bytes,
        StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
    );

    let pending = Body::from_stream(tokio_stream::pending::<Result<Bytes, Infallible>>());
    let (status, _, bytes) = send_raw(&state, resize_request("/_cluster/resize", pending)).await;
    assert_error(
        status,
        &bytes,
        StatusCode::REQUEST_TIMEOUT,
        "request_timeout",
    );

    let late = Body::from_stream(LateReadyBodyStream { delivered: false });
    let (status, _, bytes) = send_raw(&state, resize_request("/_cluster/resize", late)).await;
    assert_error(
        status,
        &bytes,
        StatusCode::REQUEST_TIMEOUT,
        "request_timeout",
    );

    let vendor = Request::builder()
        .method("POST")
        .uri("/_cluster/resize?master_timeout=0")
        .header(
            header::CONTENT_TYPE,
            "application/vnd.reverse-rusty+json; charset=utf-8",
        )
        .body(Body::from(r#"{"num_shards":3}"#))
        .expect("request");
    let (status, _, bytes) = send_raw(&state, vendor).await;
    assert_eq!(status, StatusCode::OK, "{bytes:?}");
}

#[tokio::test]
async fn manager_timeout_bounds_admission_and_exclusive_lock_waits() {
    let state = test_state(&seed());
    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("hold admin admission");
    for timeout in ["0", "25ms"] {
        let (status, _, bytes) = send_raw(
            &state,
            resize_request(
                &format!("/_cluster/resize?cluster_manager_timeout={timeout}"),
                r#"{"num_shards":4}"#,
            ),
        )
        .await;
        assert_error(
            status,
            &bytes,
            StatusCode::REQUEST_TIMEOUT,
            "resize_timeout",
        );
    }
    drop(held);
    assert_eq!(state.cluster.read().num_shards(), 3);

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
            resize_request(
                &format!("/_cluster/resize?master_timeout={timeout}"),
                r#"{"num_shards":4}"#,
            ),
        )
        .await;
        assert_error(
            status,
            &bytes,
            StatusCode::REQUEST_TIMEOUT,
            "resize_timeout",
        );
    }
    release_sender.send(()).expect("release topology holder");
    topology_holder.join().expect("topology holder");
    assert_eq!(
        state.cluster.read().num_shards(),
        3,
        "timed-out workers must not resize later"
    );

    let closed = test_state(&seed());
    closed.stats_permits.close();
    let (status, _, bytes) = send_raw(
        &closed,
        resize_request("/_cluster/resize", r#"{"num_shards":4}"#),
    )
    .await;
    assert_error(
        status,
        &bytes,
        StatusCode::SERVICE_UNAVAILABLE,
        "resize_unavailable",
    );
}

#[tokio::test]
async fn remote_topology_is_rejected_before_admission() {
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
    let state = state_from_cluster_with_rebalance_topology(
        cluster,
        crate::state::ClusterRebalanceTopology::StaticRemote,
    );
    let _held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("hold admission");
    let (status, _, bytes) = send_raw(
        &state,
        resize_request(
            "/_cluster/resize?cluster_manager_timeout=0",
            r#"{"num_shards":4}"#,
        ),
    )
    .await;
    assert_error(
        status,
        &bytes,
        StatusCode::NOT_IMPLEMENTED,
        "not_supported_in_cluster_mode",
    );
    assert!(
        String::from_utf8_lossy(&bytes).contains("separate cluster"),
        "{bytes:?}"
    );
    assert_eq!(state.cluster.read().num_shards(), 3);
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
                resize_request(
                    "/_cluster/resize?cluster_manager_timeout=0",
                    r#"{"num_shards":4}"#,
                ),
            ),
        )
        .await
        .expect("dedicated resize dispatch remains bounded");
        assert_eq!(status, StatusCode::OK, "{bytes:?}");
        assert_eq!(state.cluster.read().num_shards(), 4);

        release_tx.send(()).expect("release blocking pool");
        blocker.await.expect("blocking-pool blocker");
        assert_eq!(state.stats_permits.available_permits(), 1);
    });
}

struct BlockingResizeControlPlane {
    inner: InMemoryControlPlane,
    started: Arc<AtomicBool>,
    off_request_thread: Arc<AtomicBool>,
    request_thread: std::thread::ThreadId,
    release: Arc<Barrier>,
}

impl ControlPlane for BlockingResizeControlPlane {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_timeout_only_bounds_start_and_disconnect_keeps_resize_running() {
    let request_thread = std::thread::current().id();
    let base = test_state(&seed());
    let initial = base.cluster.read().control_state().expect("state");
    drop(base);
    let started = Arc::new(AtomicBool::new(false));
    let off_request_thread = Arc::new(AtomicBool::new(false));
    let release = Arc::new(Barrier::new(2));
    let state = state_with_control(Box::new(BlockingResizeControlPlane {
        inner: InMemoryControlPlane::new(initial),
        started: Arc::clone(&started),
        off_request_thread: Arc::clone(&off_request_thread),
        request_thread,
        release: Arc::clone(&release),
    }));

    let request_state = Arc::clone(&state);
    let request = tokio::spawn(async move {
        send_raw(
            &request_state,
            resize_request(
                "/_cluster/resize?cluster_manager_timeout=25ms",
                r#"{"num_shards":4}"#,
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
    .expect("resize started");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !request.is_finished(),
        "manager timeout must not claim to cancel an already-started rebuild"
    );
    assert!(off_request_thread.load(Ordering::SeqCst));
    assert_eq!(state.stats_permits.available_permits(), 0);

    request.abort();
    assert_eq!(
        state.stats_permits.available_permits(),
        0,
        "a disconnected request must not release resize admission"
    );
    release.wait();
    tokio::time::timeout(Duration::from_secs(1), async {
        while state.stats_permits.available_permits() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached resize released admission after completion");
    assert_eq!(state.cluster.read().num_shards(), 4);
    assert_eq!(
        state
            .cluster
            .read()
            .control_state()
            .expect("state")
            .num_shards,
        4
    );
}

fn assert_error(status: StatusCode, bytes: &Bytes, expected: StatusCode, kind: &str) {
    assert_eq!(status, expected, "{bytes:?}");
    let body: serde_json::Value = serde_json::from_slice(bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], kind, "{body}");
}
