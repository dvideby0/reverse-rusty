//! Guards on the `RemoteShard` sync→async `block_on` bridge: it must be drivable from a
//! rayon fan-out worker (the multi-shard parallel branch) AND from a tokio runtime worker
//! on the sequential single-target path — neither may trigger a nested-runtime panic.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, ShardServer};
use reverse_rusty::config::EngineConfig;
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

/// Guard: the `RemoteShard` sync→async `block_on` bridge ([`remote.rs`] lines 9-14) must be
/// drivable from a rayon fan-out without a nested-runtime panic. `percolate_inner`
/// parallelizes the per-shard probes with rayon `par_iter` when a title routes to >1 shard,
/// so each `RemoteShard::percolate` runs `Handle::block_on` on a *rayon worker* thread —
/// which is safe precisely because rayon workers are NOT tokio runtime threads. This is the
/// only place that arrangement is asserted to be load-bearing: a future refactor that drives
/// the fan-out from inside an async context (or onto tokio's own threads) would re-introduce
/// the nested-runtime panic and fail loudly HERE, even if the broader oracle were weakened.
#[test]
fn remote_fanout_block_on_does_not_panic_on_rayon_workers() {
    let (queries, titles) = build_corpus();
    let oracle = build_oracle(&queries, &titles);

    // ONE authoritative frozen feature space, shared into every server + the coordinator.
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);

    let k = 3usize;
    let cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        ..ClusterConfig::default()
    };

    // Stand up K real gRPC shard servers over the shared dict/norm (same pattern as the
    // oracle above: bind inside the runtime, connect — which `block_on`s — outside it).
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let mut addrs: Vec<SocketAddr> = Vec::with_capacity(k);
    {
        let _enter = rt.enter();
        for _ in 0..k {
            let incoming =
                TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind ephemeral port");
            addrs.push(incoming.local_addr().expect("local_addr"));
            let server = ShardServer::new(
                Arc::clone(&norm),
                Arc::clone(&dict),
                EngineConfig::default(),
            );
            rt.spawn(server.serve_with_incoming(incoming));
        }
    }
    for &addr in &addrs {
        wait_until_listening(addr);
    }
    let endpoints: Vec<String> = addrs.iter().map(|a| format!("http://{a}")).collect();

    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &endpoints,
        rt.handle(),
    )
    .expect("connect remote cluster");
    cluster.ingest(&queries).expect("ingest corpus over gRPC");

    // The guard only bites on titles that route to >1 shard (the rayon-parallel branch); a
    // single-target title takes the sequential branch and never parks `block_on` on a worker.
    // Drive every multi-shard title and assert (a) no panic and (b) the brute-oracle set.
    let mut covered = 0usize;
    for (i, title) in titles.iter().enumerate() {
        if cluster.shard_fanout(title).len() < 2 {
            continue;
        }
        covered += 1;
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("multi-shard percolate must not panic on the block_on bridge")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle[i],
            "rayon-fanout block_on bridge vs brute oracle on {title:?}"
        );
    }
    assert!(
        covered > 0,
        "guard needs >=1 title routing to >=2 shards (the rayon-parallel `par_iter` branch)"
    );
}

/// Guard (ADR-047): the `block_on` bridge must ALSO be safe on the SEQUENTIAL single-target
/// path — a title routing to only shard 0 (`fanout == 1`) skips the rayon `par_iter` branch and
/// runs `RemoteShard::percolate` directly on the CALLER's thread. If that caller is a tokio
/// runtime worker (a future async coordinator probing `percolate` from an axum/tonic handler),
/// a naive `Handle::block_on` panics with the nested-runtime error. `block_on_in_context`
/// detects the multi-thread worker and re-enters via `block_in_place` instead. This is exactly
/// the case the rayon guard above SKIPS (`fanout < 2`), so it is asserted here: a single-target
/// percolate is driven from INSIDE a spawned task on the runtime and must not panic + must equal
/// the brute-oracle set.
#[test]
fn remote_single_target_percolate_safe_from_tokio_worker() {
    let (queries, titles) = build_corpus();
    let oracle = build_oracle(&queries, &titles);

    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);

    let k = 3usize;
    let cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        ..ClusterConfig::default()
    };

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let mut addrs: Vec<SocketAddr> = Vec::with_capacity(k);
    {
        let _enter = rt.enter();
        for _ in 0..k {
            let incoming =
                TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind ephemeral port");
            addrs.push(incoming.local_addr().expect("local_addr"));
            let server = ShardServer::new(
                Arc::clone(&norm),
                Arc::clone(&dict),
                EngineConfig::default(),
            );
            rt.spawn(server.serve_with_incoming(incoming));
        }
    }
    for &addr in &addrs {
        wait_until_listening(addr);
    }
    let endpoints: Vec<String> = addrs.iter().map(|a| format!("http://{a}")).collect();

    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &endpoints,
        rt.handle(),
    )
    .expect("connect remote cluster");
    cluster.ingest(&queries).expect("ingest corpus over gRPC");

    // Single-target titles route to shard 0 only — the sequential (non-rayon) probe path.
    let single: Vec<usize> = titles
        .iter()
        .enumerate()
        .filter(|(_, t)| cluster.shard_fanout(t).len() == 1)
        .map(|(i, _)| i)
        .collect();
    assert!(
        !single.is_empty(),
        "guard needs >=1 single-target title (the sequential block_on path)"
    );

    // Drive each single-target percolate from INSIDE a tokio worker: `spawn` runs the closure
    // on a multi-thread worker, so `RemoteShard::percolate`'s `block_on` executes in a runtime
    // context — the exact arrangement that panics without `block_in_place`. A panic surfaces as
    // a `JoinError` (the first `expect`), so the guard bites even though the seam is sync.
    let cluster = Arc::new(cluster);
    for i in single {
        let title = titles[i].clone();
        let c = Arc::clone(&cluster);
        let got: HashSet<u64> = rt
            .block_on(async move { tokio::task::spawn(async move { c.percolate(&title) }).await })
            .expect("spawned percolate task must not panic on the block_on bridge")
            .expect("single-target percolate over gRPC")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle[i],
            "single-target block_on bridge (from tokio worker) vs brute oracle on title #{i}"
        );
    }
}
