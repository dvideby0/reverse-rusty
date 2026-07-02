//! ADR-085 transport hardening over REAL gRPC: the unified `call` seam records per-RPC
//! metrics on the happy path, and a percolate against a DOWNED shard fails loud (never
//! hangs) — with the read retried and the error counters reflecting it. The deterministic
//! timeout/retry/backoff logic itself is unit-tested in `src/cluster/remote.rs`; this is the
//! end-to-end backstop proving the seam is wired into the coordinator's fan-out and the
//! metrics flow through to `ClusterEngine::transport_metrics`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use reverse_rusty::cluster::{
    ClientSecurity, ClusterConfig, ClusterEngine, MeshTransport, ShardServer,
};
use reverse_rusty::config::EngineConfig;
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

#[test]
fn transport_metrics_recorded_and_downed_shard_fails_loud() {
    // Happy-path percolate workload size (an item before statements, not after — clippy).
    const N: usize = 40;
    let (queries, titles) = build_corpus();
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);

    // K = 1 so every title routes to the one shard — once we down it, every percolate hits
    // the failure deterministically (no need to reason about per-title fan-out).
    let cfg = ClusterConfig {
        num_shards: 1,
        include_broad: true,
        ..ClusterConfig::default()
    };

    // The server runs on its OWN runtime; the client/cluster runs on another. Downing the
    // shard is then a clean `drop(server_rt)`, which aborts the accept loop AND every
    // per-connection task (tonic spawns those independently, so merely aborting the `serve`
    // future would leave the client's existing connection alive — it would keep answering).
    let server_rt = tokio::runtime::Runtime::new().expect("server runtime");
    let client_rt = tokio::runtime::Runtime::new().expect("client runtime");

    let (addr, metrics_source) = {
        let _enter = server_rt.enter();
        let incoming =
            TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind ephemeral port");
        let addr: SocketAddr = incoming.local_addr().expect("local_addr");
        let server = ShardServer::new(
            Arc::clone(&norm),
            Arc::clone(&dict),
            EngineConfig::default(),
        );
        // Captured BEFORE `serve_with_incoming` consumes the server (ADR-100: the server-side
        // latency-histogram view of the same workload the client metrics record).
        let metrics_source = server.metrics_source();
        server_rt.spawn(server.serve_with_incoming(incoming));
        (addr, metrics_source)
    };
    wait_until_listening(addr);
    let endpoints = vec![format!("http://{addr}")];

    // Tight transport config so ANY failure fails fast in-test (the defaults are seconds);
    // `read_retries = 1` so we exercise — and bound — the retry path on the downed shard.
    let security = ClientSecurity {
        transport: MeshTransport {
            connect_timeout: Duration::from_millis(500),
            read_timeout: Duration::from_millis(500),
            write_timeout: Duration::from_secs(2),
            read_retries: 1,
            ..MeshTransport::default()
        },
        ..ClientSecurity::default()
    };

    let cluster = ClusterEngine::connect_remote_with_security(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &endpoints,
        client_rt.handle(),
        security,
    )
    .expect("connect remote cluster");
    cluster.ingest(&queries).expect("ingest corpus over gRPC");

    // Happy path: a percolate workload records calls with NO errors/timeouts.
    let mut any_match = false;
    for title in titles.iter().take(N) {
        let ids = cluster.percolate(title).expect("percolate over gRPC");
        any_match |= !ids.is_empty();
    }
    assert!(any_match, "degenerate corpus: no matches on the happy path");

    let snap = cluster.transport_metrics();
    let percolate = snap
        .methods
        .iter()
        .find(|m| m.method == "percolate")
        .expect("a percolate metric row");
    assert!(
        percolate.calls >= N as u64,
        "percolate calls recorded over the wire: {}",
        percolate.calls
    );
    assert_eq!(percolate.errors, 0, "no errors on the happy path");
    assert_eq!(percolate.timeouts, 0, "no timeouts on the happy path");
    let ingest = snap
        .methods
        .iter()
        .find(|m| m.method == "ingest")
        .expect("an ingest metric row");
    assert!(ingest.calls >= 1, "ingest recorded: {}", ingest.calls);
    assert_eq!(snap.total_timeouts(), 0, "the happy path has no timeouts");
    assert!(
        snap.total_calls() >= percolate.calls,
        "totals aggregate rows"
    );

    // ADR-100: the SERVER side of the same workload — the per-slot latency histograms recorded
    // at the gRPC handler boundary, rendered in the node's /_metrics exposition. Two-sided
    // consistency on the happy path (errors == 0 above): every client-recorded call completed
    // successfully server-side, so the shard's histogram count equals the client's call count
    // exactly, and `le="+Inf"` mirrors `_count` (the Prometheus histogram contract).
    let body = metrics_source.render();
    assert!(
        body.contains(&format!(
            "reverse_rusty_shard_rpc_duration_seconds_count{{shard=\"0\",method=\"percolate\"}} {}",
            percolate.calls
        )),
        "server-side percolate count must equal the client-side call count ({}); got:\n{body}",
        percolate.calls
    );
    assert!(body.contains(&format!(
        "reverse_rusty_shard_rpc_duration_seconds_bucket{{shard=\"0\",method=\"percolate\",le=\"+Inf\"}} {}",
        percolate.calls
    )));
    assert!(
        body.contains(&format!(
            "reverse_rusty_shard_rpc_duration_seconds_count{{shard=\"0\",method=\"ingest\"}} {}",
            ingest.calls
        )),
        "server-side ingest count must equal the client-side call count ({}); got:\n{body}",
        ingest.calls
    );

    // Now DOWN the shard (drop its whole runtime) and prove a percolate fails LOUD: the
    // broken connection is a transient error, so the read is retried, then fails loud.
    drop(server_rt);
    wait_until_not_listening(addr);

    let start = std::time::Instant::now();
    let res = cluster.percolate(&titles[0]);
    let elapsed = start.elapsed();
    assert!(
        res.is_err(),
        "percolate against a downed shard must FAIL loud, got {res:?}"
    );
    assert!(
        elapsed < Duration::from_secs(20),
        "must fail FAST via timeouts/bounded retry, not hang — took {elapsed:?}"
    );

    let snap2 = cluster.transport_metrics();
    let percolate2 = snap2
        .methods
        .iter()
        .find(|m| m.method == "percolate")
        .expect("a percolate metric row");
    assert!(
        percolate2.errors >= 1,
        "the downed-shard percolate recorded an error (fail-loud)"
    );
    assert!(
        percolate2.retries >= 1,
        "the read was retried (transient connection error) before failing loud"
    );
}
