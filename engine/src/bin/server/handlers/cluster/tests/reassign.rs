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
            super::super::admin::CLUSTER_REASSIGN_BODY_TIMEOUT + Duration::from_millis(25),
        );
        Poll::Ready(Some(Ok(self.bytes.clone())))
    }
}

fn valid_body() -> serde_json::Value {
    serde_json::json!({"position": 0, "node": 2})
}

#[tokio::test]
async fn reassign_transport_is_strict_and_bounded() {
    let state = test_state(&seed());

    let (status, headers, bytes) = send_raw(
        &state,
        Request::builder()
            .method("GET")
            .uri("/_cluster/reassign")
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
            req(
                "POST",
                &format!("/_cluster/reassign?{query}"),
                &valid_body(),
            ),
        )
        .await;
        assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");
    }

    for body in [
        serde_json::json!({"position": 0}),
        serde_json::json!({"node": 2}),
        serde_json::json!({"position": 0, "node": null}),
        serde_json::json!({"position": -1, "node": 2}),
        serde_json::json!({"position": 0, "node": "node-two"}),
        serde_json::json!({"position": 0, "node": 2, "unknown": true}),
    ] {
        let (status, _, bytes) = send_raw(&state, req("POST", "/_cluster/reassign", &body)).await;
        assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");
    }

    let duplicate = Request::builder()
        .method("POST")
        .uri("/_cluster/reassign")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"position":0,"shard":0,"node":2,"to_node":"2"}"#,
        ))
        .expect("request");
    let (status, _, bytes) = send_raw(&state, duplicate).await;
    assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");

    let missing_content_type = Request::builder()
        .method("POST")
        .uri("/_cluster/reassign")
        .body(Body::from(valid_body().to_string()))
        .expect("request");
    let (status, _, bytes) = send_raw(&state, missing_content_type).await;
    assert_error(
        status,
        &bytes,
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "unsupported_media_type",
    );

    let oversized = vec![b' '; super::super::admin::CLUSTER_REASSIGN_BODY_LIMIT + 1];
    let request = Request::builder()
        .method("POST")
        .uri("/_cluster/reassign")
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
        .uri("/_cluster/reassign")
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
        .uri("/_cluster/reassign")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from_stream(LateReadyBodyStream {
            delivered: false,
            bytes: Bytes::from(vec![
                b' ';
                super::super::admin::CLUSTER_REASSIGN_BODY_LIMIT + 1
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
async fn shard_and_to_node_aliases_are_accepted_but_route_stays_native() {
    let state = test_state(&seed());
    let body = serde_json::json!({"shard": 0, "to_node": "2"});
    let (status, headers, bytes) = send_raw(
        &state,
        req(
            "POST",
            "/_cluster/reassign?cluster_manager_timeout=0",
            &body,
        ),
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
async fn unsafe_topologies_are_rejected_before_admission() {
    use crate::state::ClusterRebalanceTopology;

    for (topology, error_type) in [
        (
            ClusterRebalanceTopology::StaticRemote,
            "reassign_routing_not_authoritative",
        ),
        (
            ClusterRebalanceTopology::CliSeededAssignmentRemote,
            "reassign_resolve_only_required",
        ),
    ] {
        let base = test_state(&seed());
        let num_shards = { base.cluster.read().num_shards() };
        let replacement = reverse_rusty::cluster::ClusterEngine::build(
            reverse_rusty::Normalizer::default_vocab().expect("vocab"),
            &reverse_rusty::cluster::ClusterConfig {
                num_shards,
                ..Default::default()
            },
            &seed(),
        )
        .expect("cluster");
        let state = state_from_cluster_with_rebalance_topology(replacement, topology);
        let (status, _, bytes) =
            send_raw(&state, req("POST", "/_cluster/reassign", &valid_body())).await;
        assert_error(status, &bytes, StatusCode::CONFLICT, error_type);
        assert_eq!(
            state.reassign_permits.available_permits(),
            1,
            "deterministic topology rejection must happen before admission"
        );
    }
}

#[cfg(feature = "distributed")]
#[tokio::test]
async fn manager_timeout_bounds_reassign_admission_and_topology_wait() {
    use crate::state::ClusterRebalanceTopology;

    let cluster = reverse_rusty::cluster::ClusterEngine::build(
        reverse_rusty::Normalizer::default_vocab().expect("vocab"),
        &reverse_rusty::cluster::ClusterConfig {
            num_shards: 3,
            ..Default::default()
        },
        &seed(),
    )
    .expect("cluster");
    let state = state_from_cluster_with_rebalance_topology(
        cluster,
        ClusterRebalanceTopology::ResolveOnlyRemote,
    );
    let held = Arc::clone(&state.reassign_permits)
        .acquire_owned()
        .await
        .expect("hold reassign admission");
    for timeout in ["0", "25ms"] {
        let (status, _, bytes) = send_raw(
            &state,
            req(
                "POST",
                &format!("/_cluster/reassign?cluster_manager_timeout={timeout}"),
                &valid_body(),
            ),
        )
        .await;
        assert_error(
            status,
            &bytes,
            StatusCode::REQUEST_TIMEOUT,
            "reassign_timeout",
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
                &format!("/_cluster/reassign?master_timeout={timeout}"),
                &valid_body(),
            ),
        )
        .await;
        assert_error(
            status,
            &bytes,
            StatusCode::REQUEST_TIMEOUT,
            "reassign_timeout",
        );
    }
    release_sender.send(()).expect("release topology lock");
    holder.join().expect("topology lock holder");

    state.reassign_permits.close();
    let (status, _, bytes) =
        send_raw(&state, req("POST", "/_cluster/reassign", &valid_body())).await;
    assert_error(
        status,
        &bytes,
        StatusCode::SERVICE_UNAVAILABLE,
        "reassign_unavailable",
    );
}

fn assert_error(status: StatusCode, bytes: &Bytes, expected: StatusCode, kind: &str) {
    assert_eq!(status, expected, "{bytes:?}");
    let body: serde_json::Value = serde_json::from_slice(bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], kind, "{body}");
}
