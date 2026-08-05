use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
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
            super::super::admin::CLUSTER_RESYNC_BODY_TIMEOUT + Duration::from_millis(25),
        );
        Poll::Ready(Some(Ok(self.bytes.clone())))
    }
}

#[tokio::test]
async fn resync_noop_is_attested_and_observable() {
    let state = test_state(&seed());
    let (status, headers, bytes) = send_raw(
        &state,
        req_empty("POST", "/_cluster/resync?cluster_manager_timeout=0"),
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
    assert_eq!(body["repaired"], 0, "{body}");
    assert_eq!(body["still_pending"], 0, "{body}");
    assert!(body["took"].is_u64(), "{body}");
    assert!(body["took_ms"].is_f64(), "{body}");
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["cluster_resync", "200"])
            .get(),
        1
    );
    assert_eq!(
        state
            .prom
            .http_request_duration
            .with_label_values(&["cluster_resync"])
            .get_sample_count(),
        1
    );
}

#[tokio::test]
async fn resync_transport_is_strict_and_bounded() {
    let state = test_state(&seed());

    let (status, headers, bytes) = send_raw(
        &state,
        Request::builder()
            .method("GET")
            .uri("/_cluster/resync")
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
        "retry_failed=true",
        "timeout=1s",
        "master_timeout=1s&cluster_manager_timeout=1s",
        "master_timeout=31s",
        "master_timeout=bad",
        "master_timeout=1s&master_timeout=1s",
    ] {
        let (status, _, bytes) = send_raw(
            &state,
            req_empty("POST", &format!("/_cluster/resync?{query}")),
        )
        .await;
        assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");
    }

    for body in ["{}", " ", "null"] {
        let request = Request::builder()
            .method("POST")
            .uri("/_cluster/resync")
            .body(Body::from(body))
            .expect("request");
        let (status, _, bytes) = send_raw(&state, request).await;
        assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");
    }

    let oversized = vec![b' '; super::super::admin::CLUSTER_RESYNC_BODY_LIMIT + 1];
    let request = Request::builder()
        .method("POST")
        .uri("/_cluster/resync")
        .body(Body::from(oversized))
        .expect("request");
    let (status, _, bytes) = send_raw(&state, request).await;
    assert_error(
        status,
        &bytes,
        StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
    );

    let pending = Request::builder()
        .method("POST")
        .uri("/_cluster/resync")
        .body(Body::from_stream(tokio_stream::pending::<
            Result<Bytes, Infallible>,
        >()))
        .expect("request");
    let (status, _, bytes) = send_raw(&state, pending).await;
    assert_error(
        status,
        &bytes,
        StatusCode::REQUEST_TIMEOUT,
        "request_timeout",
    );

    let late = Request::builder()
        .method("POST")
        .uri("/_cluster/resync")
        .body(Body::from_stream(LateReadyBodyStream {
            delivered: false,
            bytes: Bytes::new(),
        }))
        .expect("request");
    let (status, _, bytes) = send_raw(&state, late).await;
    assert_error(
        status,
        &bytes,
        StatusCode::REQUEST_TIMEOUT,
        "request_timeout",
    );

    let late_oversized = Request::builder()
        .method("POST")
        .uri("/_cluster/resync")
        .body(Body::from_stream(LateReadyBodyStream {
            delivered: false,
            bytes: Bytes::from(vec![
                b' ';
                super::super::admin::CLUSTER_RESYNC_BODY_LIMIT + 1
            ]),
        }))
        .expect("request");
    let (status, _, bytes) = send_raw(&state, late_oversized).await;
    assert_error(
        status,
        &bytes,
        StatusCode::REQUEST_TIMEOUT,
        "request_timeout",
    );
}

#[tokio::test]
async fn manager_timeout_bounds_admission_and_exclusive_writer_waits() {
    let state = test_state(&seed());
    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("hold admin admission");
    for timeout in ["0", "25ms"] {
        let (status, _, bytes) = send_raw(
            &state,
            req_empty(
                "POST",
                &format!("/_cluster/resync?cluster_manager_timeout={timeout}"),
            ),
        )
        .await;
        assert_error(
            status,
            &bytes,
            StatusCode::REQUEST_TIMEOUT,
            "resync_timeout",
        );
    }
    drop(held);

    let writer_state = Arc::clone(&state);
    let (locked_sender, locked_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
    let writer_holder = std::thread::spawn(move || {
        let _writes = writer_state.write_serial.lock();
        locked_sender.send(()).expect("signal writer lock");
        release_receiver.recv().expect("release writer lock");
    });
    locked_receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("writer lock held");
    for timeout in ["0", "25ms"] {
        let (status, _, bytes) = send_raw(
            &state,
            req_empty(
                "POST",
                &format!("/_cluster/resync?master_timeout={timeout}"),
            ),
        )
        .await;
        assert_error(
            status,
            &bytes,
            StatusCode::REQUEST_TIMEOUT,
            "resync_timeout",
        );
    }
    release_sender.send(()).expect("release writer holder");
    writer_holder.join().expect("writer holder");

    let closed = test_state(&seed());
    closed.stats_permits.close();
    let (status, _, bytes) = send_raw(&closed, req_empty("POST", "/_cluster/resync")).await;
    assert_error(
        status,
        &bytes,
        StatusCode::SERVICE_UNAVAILABLE,
        "resync_unavailable",
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
                req_empty("POST", "/_cluster/resync?cluster_manager_timeout=0"),
            ),
        )
        .await
        .expect("dedicated resync dispatch remains bounded");
        assert_eq!(status, StatusCode::OK, "{bytes:?}");

        release_tx.send(()).expect("release blocking pool");
        blocker.await.expect("blocking-pool blocker");
    });
}

fn assert_error(status: StatusCode, bytes: &Bytes, expected: StatusCode, kind: &str) {
    assert_eq!(status, expected, "{bytes:?}");
    let body: serde_json::Value = serde_json::from_slice(bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], kind, "{body}");
}
