//! Filtered percolation over gRPC (ADR-049/055): the coordinator ships its frozen tag space
//! via `AdoptDict`, bulk-loads a TAGGED corpus over the wire, and filtered percolations must
//! agree with BOTH the single-node engine and the brute oracle — proving the resolved `TagId`
//! filter groups and the per-`AddItem` raw tags survive the round-trip.

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

/// Filtered percolation over gRPC (ADR-049/055): the coordinator ships its frozen tag space via
/// `AdoptDict` (atomic with the dict, fingerprint-checked), bulk-loads a TAGGED corpus over the wire,
/// then filtered percolations must agree with BOTH the single-node engine and the brute oracle —
/// proving the resolved `TagId` filter groups + the per-`AddItem` raw tags survive the round-trip and
/// the server resolves stored tags against the same shipped tag space.
#[test]
fn grpc_filtered_percolation_matches_single_node_and_oracle() {
    let (queries, titles) = build_corpus();
    let tags = tags_parallel(&queries);

    // Single-node tagged reference + brute oracle (the expected sets).
    let brute = Brute::build(&queries);
    let mut reference = Engine::new(vocab());
    reference
        .try_build_from_queries_with_tags(&queries, &tags)
        .expect("tagged single-node build");
    let ref_snap = reference.snapshot();
    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();

    // ONE frozen feature + tag space, shipped to every server (the cross-process identity).
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
    let tag_dict = frozen_tag_dict_over(&tags);

    let k = 3usize;
    let cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        ..ClusterConfig::default()
    };

    // Stand up K PENDING servers (dict-less); connect_remote ships the dict AND the tag dict.
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let mut addrs: Vec<SocketAddr> = Vec::with_capacity(k);
    {
        let _enter = rt.enter();
        for _ in 0..k {
            let incoming =
                TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind ephemeral port");
            addrs.push(incoming.local_addr().expect("local_addr"));
            let server = ShardServer::pending(Arc::clone(&norm), EngineConfig::default());
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
        Arc::clone(&tag_dict),
        &cfg,
        &endpoints,
        rt.handle(),
    )
    .expect("connect remote cluster (ships dict + tag dict)");
    cluster
        .ingest_with_tags(&queries, &tags)
        .expect("ingest tagged corpus over gRPC");

    let mut nonempty = 0usize;
    for (ti, title) in titles.iter().enumerate() {
        let unfiltered: HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate over gRPC")
            .into_iter()
            .collect();
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        for filter in filters_for(ti) {
            let got: HashSet<u64> = cluster
                .percolate_filtered(title, &filter)
                .expect("filtered percolate over gRPC")
                .into_iter()
                .collect();

            let pred = ref_snap.compile_tag_predicate(&filter);
            ref_snap.match_title_filtered(title, &mut s, &mut out, true, &pred);
            let ref_filtered: HashSet<u64> = out.iter().copied().collect();

            let brute_filtered: HashSet<u64> = truth
                .iter()
                .copied()
                .filter(|l| passes_filter(&tags_for(*l), &filter))
                .collect();

            assert_eq!(
                got, brute_filtered,
                "gRPC filtered vs brute oracle (title {ti}, filter {filter:?})"
            );
            assert_eq!(
                got, ref_filtered,
                "gRPC filtered vs single-node (title {ti}, filter {filter:?})"
            );
            assert!(
                got.is_subset(&unfiltered),
                "a filter added a match not in the unfiltered set, over gRPC"
            );
            if !got.is_empty() {
                nonempty += 1;
            }
        }
    }
    assert!(nonempty > 0, "degenerate: no filter ever matched over gRPC");

    // A tagged add over the gRPC insert RPC is filterable too (raw tags ride the wire).
    cluster
        .add_query_with_tags(
            8_800_001,
            "zzgrpclivetag",
            &[("category".to_string(), "cards".to_string())],
        )
        .expect("live tagged add over gRPC");
    let cards = vec![("category".to_string(), vec!["cards".to_string()])];
    let coins = vec![("category".to_string(), vec!["coins".to_string()])];
    assert!(
        cluster
            .percolate_filtered("zzgrpclivetag", &cards)
            .unwrap()
            .contains(&8_800_001),
        "the live tagged add must pass its own (cards) filter over gRPC"
    );
    assert!(
        !cluster
            .percolate_filtered("zzgrpclivetag", &coins)
            .unwrap()
            .contains(&8_800_001),
        "the live tagged add must NOT pass a different-category (coins) filter over gRPC"
    );
}

/// Cluster ranking over the WIRE (ADR-075): the compiled spec (resolved `TagId` boosts +
/// priority key) rides `PercolateRequest.rank`, each server scores its own matched ids
/// against the shipped tag space, the reply's parallel `scores` + `ranked` echo survive
/// the round-trip, and the merged scored set equals the single-node engine's `rank`.
#[test]
fn grpc_ranked_percolate_matches_single_node() {
    let (queries, titles) = build_corpus();
    let tags = tags_parallel(&queries);

    // Single-node tagged reference (the ranking ground truth).
    let mut reference = Engine::new(vocab());
    reference
        .try_build_from_queries_with_tags(&queries, &tags)
        .expect("tagged single-node build");
    let ref_snap = reference.snapshot();
    let raw_spec = reverse_rusty::RankSpec {
        priority_key: Some("priority".to_string()),
        boosts: vec![
            ("category".to_string(), "cards".to_string(), 1000),
            ("status".to_string(), "active".to_string(), 250),
        ],
    };
    let cspec = ref_snap.compile_rank_spec(&raw_spec);

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
    let tag_dict = frozen_tag_dict_over(&tags);

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
            let server = ShardServer::pending(Arc::clone(&norm), EngineConfig::default());
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
        Arc::clone(&tag_dict),
        &cfg,
        &endpoints,
        rt.handle(),
    )
    .expect("connect remote cluster");
    cluster
        .ingest_with_tags(&queries, &tags)
        .expect("ingest tagged corpus over gRPC");

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let mut scored_nonzero = 0usize;
    for (ti, title) in titles.iter().take(60).enumerate() {
        let (got, _stats) = cluster
            .percolate_filtered_ranked(title, &[], true, &raw_spec)
            .expect("ranked percolate over gRPC");
        ref_snap.match_title_filtered(
            title,
            &mut s,
            &mut out,
            true,
            &reverse_rusty::exact::TagPredicate::empty(),
        );
        let mut want = ref_snap.rank(&out, &cspec);
        want.sort_unstable_by_key(|&(id, _)| id);
        assert_eq!(
            got, want,
            "gRPC ranked percolate diverges from single-node (title {ti})"
        );
        if got.iter().any(|&(_, sc)| sc != 0) {
            scored_nonzero += 1;
        }
    }
    assert!(
        scored_nonzero > 0,
        "degenerate: no title ever produced a non-zero score over gRPC"
    );
}
