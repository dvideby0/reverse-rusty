//! Co-location oracle (ADR-093 Stage 2): K shard POSITIONS hosted on N < K `ShardServer`s (several
//! positions share one endpoint) must return EXACTLY the brute oracle's and the single-node engine's
//! sets — proving the per-endpoint adoption dedup + `AddShard` (the 2nd+ position on a node reuses the
//! node dict by `Arc`, no re-ship) preserve the zero-false-negative contract. `shard_query_counts`
//! confirms all K co-located slots are independently populated (a clobbered/collided slot would be
//! empty or `not_found`).

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, ShardServer};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::{Engine, MatchScratch};
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

#[test]
fn grpc_colocated_shards_match_single_node_and_oracle() {
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

    // ONE authoritative frozen feature space, shared into every server + the coordinator.
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);

    // K = 4 shard POSITIONS hosted on N = 2 servers: positions {0,2} on server A, {1,3} on server B.
    let k = 4usize;
    let n_servers = 2usize;
    let cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        ..ClusterConfig::default()
    };

    // Stand up N real gRPC shard servers over the SHARED frozen dict/norm (see `core.rs` for the
    // bind-once rationale).
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let mut addrs: Vec<SocketAddr> = Vec::with_capacity(n_servers);
    {
        let _enter = rt.enter();
        for _ in 0..n_servers {
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
    // CO-LOCATION: one endpoint entry PER POSITION (the `len == num_shards` contract holds), but only
    // N distinct endpoints — position `i` maps to server `i % N`, so several positions share a node.
    // The builder adopts the dict on the FIRST position of each endpoint and `AddShard`s the rest —
    // exercising the AddShard path for positions 2 and 3.
    let endpoints: Vec<String> = (0..k)
        .map(|position| format!("http://{}", addrs[position % n_servers]))
        .collect();

    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &endpoints,
        rt.handle(),
    )
    .expect("connect co-located remote cluster");
    cluster.ingest(&queries).expect("ingest corpus over gRPC");

    // Every co-located slot is independently populated — proving `AddShard` created K distinct slots
    // across N servers (a clobbered/collided slot would be empty, or `not_found` → a failing count).
    let counts = cluster
        .shard_query_counts()
        .expect("per-shard counts over gRPC");
    assert_eq!(
        counts.len(),
        k,
        "expected one count per position: {counts:?}"
    );
    assert!(
        counts.iter().all(|&c| c > 0),
        "every co-located slot must hold queries (a broken AddShard would leave one empty): {counts:?}"
    );

    // Placement branches present, over the co-located wire.
    let cc = cluster.class_counts().expect("class_counts over gRPC");
    assert!(
        cc[0] > 0 && cc[1] > 0 && cc[2] > 0,
        "missing a placement class: {cc:?}"
    );

    // A local in-process cluster over the SAME corpus + config: identical placement/routing, so its
    // merged `MatchStats` must equal the co-located gRPC cluster's for every title.
    let local = ClusterEngine::build(vocab(), &cfg, &queries).expect("build local cluster");

    // The differential contract, over the co-located gRPC cluster, for every title.
    for (i, title) in titles.iter().enumerate() {
        let (ids, mut grpc_stats) = cluster
            .percolate_with_stats(title)
            .expect("percolate over gRPC");
        let got: HashSet<u64> = ids.into_iter().collect();
        assert_eq!(
            got, oracle[i],
            "co-located cluster vs brute oracle on {title:?}"
        );
        assert_eq!(
            got, ref_broad[i],
            "co-located cluster vs single-node on {title:?}"
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
            "co-located gRPC vs local-cluster MatchStats (wire round-trip) on {title:?}"
        );

        let got_sel: HashSet<u64> = cluster
            .percolate_with_broad(title, false)
            .expect("percolate (broad off) over gRPC")
            .into_iter()
            .collect();
        assert_eq!(
            got_sel, ref_selective[i],
            "co-located cluster broad=off vs single-node selective on {title:?}"
        );
    }
}
