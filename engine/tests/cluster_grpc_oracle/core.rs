//! Core differential oracle: a gRPC-backed cluster over K real `ShardServer`s must
//! return EXACTLY the brute oracle's and the single-node engine's sets (broad on/off),
//! its round-tripped `MatchStats` must equal a local cluster's, and the live add/find/
//! remove RPCs must work end-to-end.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, ShardServer};
use reverse_rusty::compile::extract;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::dict::Dict;
use reverse_rusty::segment::{Engine, MatchScratch};
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

#[test]
fn grpc_cluster_matches_single_node_and_oracle() {
    let (queries, titles) = build_corpus();

    // Independent expected sets: brute-force oracle + single-node engine, broad on/off.
    let brute = Brute::build(&queries);
    let mut reference = Engine::new(vocab());
    reference.build_from_queries(&queries);

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let mut ref_broad: Vec<HashSet<u64>> = Vec::with_capacity(titles.len());
    let mut ref_selective: Vec<HashSet<u64>> = Vec::with_capacity(titles.len());
    let mut oracle: Vec<HashSet<u64>> = Vec::with_capacity(titles.len());
    let mut total_truth = 0usize;
    for title in &titles {
        reference.match_title(title, &mut s, &mut out, true);
        ref_broad.push(out.iter().copied().collect());
        reference.match_title(title, &mut s, &mut out, false);
        ref_selective.push(out.iter().copied().collect());
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        total_truth += truth.len();
        oracle.push(truth);
    }
    assert!(total_truth > 0, "degenerate corpus: no matches at all");

    // ONE authoritative frozen feature space, shared into every server (this is how
    // the cross-process dict-identity requirement is met in-test) AND used by the
    // coordinator for placement/routing.
    let norm = Arc::new(vocab());
    let dict = {
        let mut d = Dict::new();
        let mut lc = String::new();
        for (_id, text) in &queries {
            if let Ok(ast) = reverse_rusty::dsl::parse(text) {
                let _ = extract(&ast, &norm, &mut d, &mut lc);
            }
        }
        d.finalize_mask();
        Arc::new(d)
    };

    let k = 3usize;
    let cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        ..ClusterConfig::default()
    };

    // Stand up K real gRPC shard servers over the SHARED frozen dict/norm. Each binds its
    // ephemeral port ONCE (via `TcpIncoming`) and serves on that same socket — no
    // bind→drop→rebind window for another process to steal the port (the old CI flake).
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let mut addrs: Vec<SocketAddr> = Vec::with_capacity(k);
    {
        // `TcpIncoming::bind` -> `TcpListener::from_std` registers with the reactor, so it
        // must run inside the runtime context; scope the guard so the later `connect_remote`
        // (which `block_on`s) still runs OUTSIDE it, as before.
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

    // Assemble the gRPC-backed cluster and load the corpus OVER THE WIRE.
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

    // Every placement branch is exercised (A, B, C all present), counted over gRPC.
    let cc = cluster.class_counts().expect("class_counts over gRPC");
    assert!(cc[0] > 0, "no class-A queries: {cc:?}");
    assert!(cc[1] > 0, "no class-B queries: {cc:?}");
    assert!(cc[2] > 0, "no class-C (broad) queries: {cc:?}");

    // A local (in-process) cluster over the SAME corpus + config: identical placement and
    // routing, so its merged `MatchStats` must equal the gRPC cluster's for every title. A
    // transposition in `cluster/proto.rs`'s wire map shows up as a stats mismatch here (the
    // proto.rs unit test catches it directly; this is the end-to-end backstop).
    let local = ClusterEngine::build(vocab(), &cfg, &queries).expect("build local cluster");

    // The differential contract, over gRPC, for every title — matched ids AND the
    // round-tripped MatchStats.
    for (i, title) in titles.iter().enumerate() {
        let (ids, mut grpc_stats) = cluster
            .percolate_with_stats(title)
            .expect("percolate over gRPC");
        let got: HashSet<u64> = ids.into_iter().collect();
        assert_eq!(
            got, oracle[i],
            "gRPC cluster vs brute-force oracle on {title:?}"
        );
        assert_eq!(
            got, ref_broad[i],
            "gRPC cluster vs single-node on {title:?}"
        );

        let (_, mut local_stats) = local
            .percolate_with_stats(title)
            .expect("percolate local cluster");
        // ADR-107 delivery telemetry is intentionally local-only until a later
        // protobuf revision. Compare the frozen wire compatibility profile.
        grpc_stats.logical_emissions = 0;
        grpc_stats.duplicate_emissions = 0;
        local_stats.logical_emissions = 0;
        local_stats.duplicate_emissions = 0;
        assert_eq!(
            grpc_stats, local_stats,
            "gRPC vs local-cluster MatchStats (wire round-trip) on {title:?}"
        );

        let got_sel: HashSet<u64> = cluster
            .percolate_with_broad(title, false)
            .expect("percolate (broad off) over gRPC")
            .into_iter()
            .collect();
        assert_eq!(
            got_sel, ref_selective[i],
            "gRPC cluster broad=off vs single-node selective on {title:?}"
        );
    }

    // Exercise the live-write RPCs end-to-end: add a class-A query, find it, remove it.
    let qid = 7_777_001u64;
    let placed = cluster
        .add_query(qid, "1994 upper deck rareplayer0")
        .expect("add_query over gRPC");
    assert!(
        matches!(placed, reverse_rusty::cluster::AddOutcome::Placed { .. }),
        "expected class-A Placed, got {placed:?}"
    );
    let live_title = "1994 upper deck rareplayer0 psa 10";
    assert!(
        cluster
            .percolate(live_title)
            .expect("percolate live")
            .contains(&qid),
        "a gRPC live-added query must match"
    );
    let removed = cluster.remove_query(qid).expect("remove_query over gRPC");
    assert!(
        removed >= 1,
        "remove should tombstone the holding shard, got {removed}"
    );
    assert!(
        !cluster
            .percolate(live_title)
            .expect("percolate after remove")
            .contains(&qid),
        "a removed query must no longer match over gRPC"
    );
}
