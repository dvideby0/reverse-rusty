//! The hot tier over REAL gRPC (class H, ADR-105): a θ-on remote cluster matches
//! the single-node θ-on reference on both visibility modes; the 5-wide class
//! split crosses the wire via the ADDITIVE `hot` field (the `counts` list stays
//! at exactly 4 — the rolling-upgrade contract); the shard-side
//! `reverse_rusty_hot_*_total{shard}` counters move on hot work; and a
//! θ-DIVERGENT shard server (θ=0 while the coordinator runs θ-on) is COST-only:
//! results stay identical, the shard just stores the queries in its realtime
//! lane (`class_counts[4] == 0`) — the documented operator contract.

use std::net::SocketAddr;
use std::sync::Arc;

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, ShardServer};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::{Engine, MatchScratch};
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

const THETA: u32 = 32;

fn spawn_server(
    rt: &tokio::runtime::Runtime,
    norm: &Arc<reverse_rusty::normalize::Normalizer>,
    dict: &Arc<reverse_rusty::dict::Dict>,
    engine_cfg: EngineConfig,
) -> (SocketAddr, reverse_rusty::cluster::ShardMetricsSource) {
    let _enter = rt.enter();
    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind ephemeral port");
    let addr: SocketAddr = incoming.local_addr().expect("local_addr");
    let server = ShardServer::new(Arc::clone(norm), Arc::clone(dict), engine_cfg);
    let metrics_source = server.metrics_source();
    rt.spawn(server.serve_with_incoming(incoming));
    (addr, metrics_source)
}

#[test]
fn grpc_hot_tier_matches_reference_and_counters_cross_the_wire() {
    let (queries, titles) = build_corpus();
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);

    // The single-node θ-on reference (no cluster code, no wire).
    let mut reference = Engine::with_config(
        vocab(),
        EngineConfig {
            hot_anchor_threshold: THETA,
            ..EngineConfig::default()
        },
    );
    reference.build_from_queries(&queries);
    assert!(
        reference.class_counts()[4] > 0,
        "degenerate: the reference stored no class H at θ={THETA}"
    );

    let server_rt = tokio::runtime::Runtime::new().expect("server runtime");
    let client_rt = tokio::runtime::Runtime::new().expect("client runtime");
    let shard_cfg = EngineConfig {
        hot_anchor_threshold: THETA,
        ..EngineConfig::default()
    };
    let (addr0, metrics0) = spawn_server(&server_rt, &norm, &dict, shard_cfg.clone());
    let (addr1, _metrics1) = spawn_server(&server_rt, &norm, &dict, shard_cfg);
    wait_until_listening(addr0);
    wait_until_listening(addr1);

    let mut cfg = ClusterConfig {
        num_shards: 2,
        include_broad: true,
        ..ClusterConfig::default()
    };
    cfg.per_shard.hot_anchor_threshold = THETA;
    let endpoints = vec![format!("http://{addr0}"), format!("http://{addr1}")];
    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &endpoints,
        client_rt.handle(),
    )
    .expect("connect remote cluster");
    cluster.ingest(&queries).expect("ingest over gRPC");

    // The 5-wide split crosses the wire (the additive `hot` field): summed over
    // both shards it equals the reference's H population exactly (ring-placed,
    // never replicated).
    let cc = cluster.class_counts().expect("class counts over gRPC");
    assert_eq!(
        cc[4],
        reference.class_counts()[4],
        "class-H count must cross the wire and stay ring-placed (not ×K)"
    );

    // Percolates ≡ the reference on BOTH visibility modes.
    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    for title in titles.iter().take(120) {
        reference.match_title(title, &mut s, &mut out, true);
        out.sort_unstable();
        out.dedup();
        let got = cluster.percolate(title).expect("percolate over gRPC");
        assert_eq!(got, out, "broad-on gRPC vs reference on {title:?}");
        reference.match_title(title, &mut s, &mut out, false);
        out.sort_unstable();
        out.dedup();
        let got = cluster
            .percolate_with_broad(title, false)
            .expect("broad-off percolate over gRPC");
        assert_eq!(got, out, "broad-OFF gRPC vs reference on {title:?}");
    }

    // The shard-side hot counters rendered and moved (ADR-101's hot extension):
    // per-title percolates scan hot postings inline, so postings/candidates are
    // non-zero on at least the sampled shard; the columnar-only families stay 0
    // on the per-title Percolate wire (the ADR-101 caveat, name-symmetric).
    let body = metrics0.render();
    assert!(
        body.contains("reverse_rusty_hot_postings_scanned_total{shard=\"0\"}"),
        "hot counter family missing from the shard exposition:\n{body}"
    );
    assert!(
        !body.contains("reverse_rusty_hot_postings_scanned_total{shard=\"0\"} 0"),
        "hot postings counter never moved despite a hot-bearing workload"
    );
    assert!(body.contains("reverse_rusty_hot_batches_total{shard=\"0\"} 0"));
}

/// θ divergence between the coordinator and a shard server is COST-only (the
/// documented operator contract): a θ=0 server classifies the same placed
/// queries into its realtime lane — zero class H stored — and every percolate
/// still equals the θ-on reference on both visibility modes.
#[test]
fn grpc_theta_divergent_server_is_cost_only() {
    let (queries, titles) = build_corpus();
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);

    let mut reference = Engine::with_config(
        vocab(),
        EngineConfig {
            hot_anchor_threshold: THETA,
            ..EngineConfig::default()
        },
    );
    reference.build_from_queries(&queries);

    let server_rt = tokio::runtime::Runtime::new().expect("server runtime");
    let client_rt = tokio::runtime::Runtime::new().expect("client runtime");
    // The DIVERGENT server: default config (θ = 0).
    let (addr, _metrics) = spawn_server(&server_rt, &norm, &dict, EngineConfig::default());
    wait_until_listening(addr);

    let mut cfg = ClusterConfig {
        num_shards: 1,
        include_broad: true,
        ..ClusterConfig::default()
    };
    cfg.per_shard.hot_anchor_threshold = THETA; // the coordinator believes θ is on
    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &[format!("http://{addr}")],
        client_rt.handle(),
    )
    .expect("connect remote cluster");
    cluster.ingest(&queries).expect("ingest over gRPC");

    // The shard stored everything in its realtime lane (θ=0 server) — the
    // divergence is visible in the counts…
    assert_eq!(
        cluster.class_counts().expect("cc")[4],
        0,
        "a θ=0 server must classify the placed queries without class H"
    );
    // …and INVISIBLE in the results (both lanes are always probed).
    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    for title in titles.iter().take(120) {
        reference.match_title(title, &mut s, &mut out, true);
        out.sort_unstable();
        out.dedup();
        let got = cluster.percolate(title).expect("percolate over gRPC");
        assert_eq!(got, out, "divergent-θ broad-on vs reference on {title:?}");
        reference.match_title(title, &mut s, &mut out, false);
        out.sort_unstable();
        out.dedup();
        let got = cluster
            .percolate_with_broad(title, false)
            .expect("broad-off percolate over gRPC");
        assert_eq!(got, out, "divergent-θ broad-OFF vs reference on {title:?}");
    }
}
