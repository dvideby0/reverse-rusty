//! gRPC health endpoints (ADR-084): the deployable shard server exposes the standard
//! `grpc.health.v1.Health` service on a SEPARATE plaintext port for Kubernetes probes.
//! Liveness (`Check("")`) is SERVING once the gRPC server is up; readiness
//! (`Check("ready")`) tracks dict-adoption — a `--pending` shard is live-but-not-ready
//! until it adopts a dict. An unknown service name is `NOT_FOUND` (never a silent SERVING).
//!
//! These drive the REAL `ShardServer::serve()` two-port path (data + health) over the
//! wire via the generated `grpc.health.v1` client — the same surface a kubelet probes.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, ShardServer};
use reverse_rusty::config::EngineConfig;
use reverse_rusty_shard_proto as raw;

use raw::health::health_check_response::ServingStatus;
use raw::health::health_client::HealthClient;
use raw::health::HealthCheckRequest;

use crate::harness::*;

/// Two distinct free localhost ports (data + health), bound simultaneously so the OS
/// hands out different ports, then released for `serve()` to bind. The drop→re-bind window
/// is microseconds and harmless for a single focused server (unlike the K-server oracle,
/// which binds `TcpIncoming` race-free; `serve()` — the deployable path — binds by address).
fn free_addr_pair() -> (SocketAddr, SocketAddr) {
    let data = std::net::TcpListener::bind("127.0.0.1:0").expect("bind data port");
    let health = std::net::TcpListener::bind("127.0.0.1:0").expect("bind health port");
    let data_addr = data.local_addr().expect("data local_addr");
    let health_addr = health.local_addr().expect("health local_addr");
    (data_addr, health_addr) // both listeners drop here, freeing the ports
}

/// One `Check` against the health service, returning the `ServingStatus` or the gRPC error.
async fn check(
    client: &mut HealthClient<tonic::transport::Channel>,
    service: &str,
) -> Result<ServingStatus, tonic::Status> {
    client
        .check(HealthCheckRequest {
            service: service.to_string(),
        })
        .await
        .map(|r| r.into_inner().status())
}

/// A `--pending` shard with a health port: liveness SERVING, readiness NOT_SERVING (no
/// dict adopted), and an unknown service name `NOT_FOUND`.
#[test]
fn shard_health_liveness_readiness_and_unknown_over_grpc() {
    let norm = Arc::new(vocab());
    let (data_addr, health_addr) = free_addr_pair();

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    {
        let _enter = rt.enter();
        let server = ShardServer::pending(Arc::clone(&norm), EngineConfig::default())
            .with_health_addr(health_addr);
        rt.spawn(server.serve(data_addr));
    }
    wait_until_listening(data_addr);
    wait_until_listening(health_addr);

    rt.block_on(async {
        let mut hc = HealthClient::connect(format!("http://{health_addr}"))
            .await
            .expect("connect health client");

        // Liveness: the gRPC server is up.
        assert_eq!(
            check(&mut hc, "").await.expect("check overall"),
            ServingStatus::Serving,
            "overall (liveness) must be SERVING once the server is up"
        );
        // Readiness: pending (no dict) ⇒ NOT_SERVING.
        assert_eq!(
            check(&mut hc, "ready").await.expect("check ready"),
            ServingStatus::NotServing,
            "a pending shard is live but NOT ready"
        );
        // Unknown service ⇒ NOT_FOUND, not a silent SERVING.
        let err = check(&mut hc, "bogus")
            .await
            .expect_err("an unknown service name must be an error");
        assert_eq!(err.code(), tonic::Code::NotFound);
    });
}

/// Readiness flips to SERVING once the pending shard adopts a dict — the live transition a
/// Kubernetes readiness probe gates rollout on. `connect_remote` ships + adopts the
/// coordinator's frozen dict (ADR-034); the 250ms watcher then flips `Check("ready")`.
#[test]
fn shard_readiness_flips_to_serving_after_dict_adopt() {
    let norm = Arc::new(vocab());
    // A tiny authoritative frozen dict the coordinator ships to the pending shard.
    let queries = vec![
        (1u64, "1994 upper deck".to_string()),
        (2u64, "psa 10".to_string()),
    ];
    let dict = frozen_dict_over(&queries, &norm);

    let (data_addr, health_addr) = free_addr_pair();
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    {
        let _enter = rt.enter();
        let server = ShardServer::pending(Arc::clone(&norm), EngineConfig::default())
            .with_health_addr(health_addr);
        rt.spawn(server.serve(data_addr));
    }
    wait_until_listening(data_addr);
    wait_until_listening(health_addr);

    let mut hc = rt
        .block_on(HealthClient::connect(format!("http://{health_addr}")))
        .expect("connect health client");

    // Before adopt: not ready.
    assert_eq!(
        rt.block_on(check(&mut hc, "ready"))
            .expect("check ready before"),
        ServingStatus::NotServing,
        "pending shard must start not-ready"
    );

    // connect_remote SHIPS + adopts the dict on the pending server (outside the runtime
    // context: it block_on's internally — see dict_shipping.rs).
    let cfg = ClusterConfig {
        num_shards: 1,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &[format!("http://{data_addr}")],
        rt.handle(),
    )
    .expect("connect_remote ships + adopts the dict");
    cluster.ingest(&queries).expect("ingest corpus over gRPC");

    // After adopt: the watcher flips readiness to SERVING within a short window.
    let became_ready = rt.block_on(async {
        for _ in 0..60 {
            if check(&mut hc, "ready").await.expect("check ready after") == ServingStatus::Serving {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    });
    assert!(
        became_ready,
        "readiness must flip to SERVING after the shard adopts a dict"
    );

    // The adopted cluster still serves a real match (the readiness signal was truthful).
    let hits = cluster.percolate("1994 upper deck").expect("percolate");
    assert!(
        hits.contains(&1),
        "the now-ready shard serves the matching query"
    );
}
