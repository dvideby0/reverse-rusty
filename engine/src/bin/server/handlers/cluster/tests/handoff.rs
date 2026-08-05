use std::convert::Infallible;
use std::pin::Pin;
#[cfg(feature = "distributed")]
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
            super::super::admin::CLUSTER_HANDOFF_BODY_TIMEOUT + Duration::from_millis(25),
        );
        Poll::Ready(Some(Ok(self.bytes.clone())))
    }
}

fn valid_body() -> serde_json::Value {
    serde_json::json!({
        "position": 0,
        "source": "https://source.example:50051",
        "target": "https://target.example:50051",
        "allow_uncommitted": true
    })
}

#[tokio::test]
async fn handoff_transport_is_strict_and_bounded() {
    let state = test_state(&seed());

    let (status, headers, bytes) = send_raw(
        &state,
        Request::builder()
            .method("GET")
            .uri("/_cluster/handoff")
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
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );

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
            req("POST", &format!("/_cluster/handoff?{query}"), &valid_body()),
        )
        .await;
        assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");
    }

    for body in [
        serde_json::json!({
            "position": 0,
            "source": "https://source.example:50051",
            "target": "https://target.example:50051"
        }),
        serde_json::json!({
            "position": 0,
            "source": "https://source.example:50051",
            "target": "https://target.example:50051",
            "allow_uncommitted": false
        }),
        serde_json::json!({
            "position": 0,
            "source": "source.example:50051",
            "target": "https://target.example:50051",
            "allow_uncommitted": true
        }),
        serde_json::json!({
            "position": 0,
            "source": "https://same.example:50051",
            "target": "HTTPS://SAME.EXAMPLE:50051/",
            "allow_uncommitted": true
        }),
        serde_json::json!({
            "position": 0,
            "source": "https://source.example:50051",
            "target": "https://target.example:50051",
            "allow_uncommitted": true,
            "unknown": true
        }),
    ] {
        let (status, _, bytes) = send_raw(&state, req("POST", "/_cluster/handoff", &body)).await;
        assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");
    }

    let duplicate = Request::builder()
        .method("POST")
        .uri("/_cluster/handoff")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"position":0,"shard":0,"source":"https://source.example:50051","target":"https://target.example:50051","allow_uncommitted":true}"#,
        ))
        .expect("request");
    let (status, _, bytes) = send_raw(&state, duplicate).await;
    assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");

    let missing_content_type = Request::builder()
        .method("POST")
        .uri("/_cluster/handoff")
        .body(Body::from(valid_body().to_string()))
        .expect("request");
    let (status, _, bytes) = send_raw(&state, missing_content_type).await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE, "{bytes:?}");

    let oversized = vec![b' '; super::super::admin::CLUSTER_HANDOFF_BODY_LIMIT + 1];
    let request = Request::builder()
        .method("POST")
        .uri("/_cluster/handoff")
        .header(header::CONTENT_TYPE, "application/json")
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
        .uri("/_cluster/handoff")
        .header(header::CONTENT_TYPE, "application/json")
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

    let late_oversized = Request::builder()
        .method("POST")
        .uri("/_cluster/handoff")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from_stream(LateReadyBodyStream {
            delivered: false,
            bytes: Bytes::from(vec![
                b' ';
                super::super::admin::CLUSTER_HANDOFF_BODY_LIMIT + 1
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
async fn shard_alias_is_accepted_but_raw_handoff_stays_native() {
    let state = test_state(&seed());
    let body = serde_json::json!({
        "shard": 0,
        "source": "https://source.example:50051",
        "target": "https://target.example:50051",
        "allow_uncommitted": true
    });
    let (status, headers, bytes) = send_raw(
        &state,
        req("POST", "/_cluster/handoff?cluster_manager_timeout=0", &body),
    )
    .await;
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    #[cfg(not(feature = "distributed"))]
    assert_error(
        status,
        &bytes,
        StatusCode::NOT_IMPLEMENTED,
        "not_supported_in_cluster_mode",
    );
    #[cfg(feature = "distributed")]
    assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");
}

#[cfg(feature = "distributed")]
#[tokio::test]
async fn manager_timeout_bounds_handoff_admission_and_topology_wait() {
    let state = test_state(&seed());
    let held = Arc::clone(&state.handoff_permits)
        .acquire_owned()
        .await
        .expect("hold handoff admission");
    for timeout in ["0", "25ms"] {
        let (status, _, bytes) = send_raw(
            &state,
            req(
                "POST",
                &format!("/_cluster/handoff?cluster_manager_timeout={timeout}"),
                &valid_body(),
            ),
        )
        .await;
        assert_error(
            status,
            &bytes,
            StatusCode::REQUEST_TIMEOUT,
            "handoff_timeout",
        );
    }
    drop(held);

    let lock_state = Arc::clone(&state);
    let (locked_sender, locked_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
    let holder = std::thread::spawn(move || {
        let _topology = lock_state.topology_guard.write();
        locked_sender.send(()).expect("signal topology lock");
        release_receiver.recv().expect("release topology lock");
    });
    locked_receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("topology lock acquired");
    for timeout in ["0", "25ms"] {
        let (status, _, bytes) = send_raw(
            &state,
            req(
                "POST",
                &format!("/_cluster/handoff?master_timeout={timeout}"),
                &valid_body(),
            ),
        )
        .await;
        assert_error(
            status,
            &bytes,
            StatusCode::REQUEST_TIMEOUT,
            "handoff_timeout",
        );
    }
    release_sender.send(()).expect("release topology lock");
    holder.join().expect("topology lock holder");

    let closed = test_state(&seed());
    closed.handoff_permits.close();
    let (status, _, bytes) =
        send_raw(&closed, req("POST", "/_cluster/handoff", &valid_body())).await;
    assert_error(
        status,
        &bytes,
        StatusCode::SERVICE_UNAVAILABLE,
        "handoff_unavailable",
    );
}

fn assert_error(status: StatusCode, bytes: &Bytes, expected: StatusCode, kind: &str) {
    assert_eq!(status, expected, "{bytes:?}");
    let body: serde_json::Value = serde_json::from_slice(bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], kind, "{body}");
}
