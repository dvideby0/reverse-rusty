//! Wave-parallel multi-position moves over gRPC (ADR-095): `reconcile_with` /
//! `rebalance_and_move_with` at `max_parallel_moves = 2` converge a diverged committed map by moving
//! several positions CONCURRENTLY (disjoint node footprints run in one wave; conflicting moves
//! serialize through the busy-endpoint ledger) — live under a concurrent writer, zero false
//! negatives, idempotent, and a resolve-only coordinator restart routes to the new owners.
//!
//! The topology is built so parallelism is REAL and deterministic: 4 nodes in two pairs
//! ({1,3} and {2,4}), every position seeded on the pair-SWAP of its HRW-desired owner. Every move's
//! footprint therefore stays inside one pair — cross-pair moves are disjoint (a wave runs one from
//! each pair concurrently) while same-pair moves conflict (the ledger serializes them). The wave
//! SHAPES are unit-proven in `coordinator/reassign/parallel.rs`; this proves the end-to-end
//! zero-FN contract at a parallel setting on real servers.
//!
//! Two proofs:
//!  - `grpc_reconcile_parallel_disjoint_pairs_converge_zero_fn_under_writes` — the unattended pass
//!    at `max_parallel = 2` under a firehose writer: every position moved + committed, no slot
//!    lost, ≡ brute, idempotent (epoch-invariant) second parallel pass, restart routes zero-FN.
//!  - `grpc_rebalance_and_move_parallel_converges_zero_fn` — the manual sweep at
//!    `max_parallel = 2`: converges to the HRW map, second call is a fixpoint, ≡ brute.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, NodeId};
use reverse_rusty::dict::Dict;
use reverse_rusty::normalize::Normalizer;

use crate::harness::*;
use crate::relocation::{owner, primary_endpoints, seed_map, spin_n_servers, Node};

/// Mirror `allocator::hrw_weight` (the stable rendezvous hash) so the test can compute each
/// position's HRW-desired owner and deterministically seed a DIFFERENT node — guaranteeing every
/// position genuinely moves. Coupled to the allocator on purpose (as in `reconcile.rs`): if its
/// placement changes, this asserts loudly rather than silently no-op'ing.
fn hrw_weight(position: u32, node: u64) -> u64 {
    let mut bytes = [0u8; 12];
    bytes[0..4].copy_from_slice(&position.to_le_bytes());
    bytes[4..12].copy_from_slice(&node.to_le_bytes());
    reverse_rusty::util::fnv1a64(&bytes)
}

/// The HRW-desired primary for `position` over data nodes 1..=4 (RF=1: argmax weight, tie → lower
/// id — exactly `allocator::plan_assignments`).
fn hrw_primary_over_1_to_4(position: u32) -> u64 {
    (1u64..=4)
        .max_by(|a, b| {
            hrw_weight(position, *a)
                .cmp(&hrw_weight(position, *b))
                .then(b.cmp(a)) // tie → LOWER id wins
        })
        .expect("non-empty node set")
}

/// The pair-swap: 1↔3, 2↔4 (the pairs are {1,3} and {2,4} — chosen because HRW over ids 1..=4
/// deterministically wants nodes 3 AND 4 among the first six positions, so both pairs are
/// exercised). Seeding each position on `swap(desired)` keeps every move's footprint inside one
/// pair, so cross-pair moves are provably disjoint (parallelizable) and same-pair moves provably
/// conflict (serialized) — deterministic wave structure without touching the planner.
fn pair_swap(node: u64) -> u64 {
    match node {
        1 => 3,
        3 => 1,
        2 => 4,
        _ => 2,
    }
}

struct PairDiverged {
    cluster: ClusterEngine,
    nodes: Vec<Node>,
    rt: tokio::runtime::Runtime,
    norm: Arc<Normalizer>,
    dict: Arc<Dict>,
    cfg: ClusterConfig,
    queries: Vec<(u64, String)>,
    titles: Vec<String>,
    k: usize,
    desired: Vec<u64>,
}

/// A K=6 cluster over 4 nodes whose committed map (and physical data) sits on the pair-swap of
/// every position's HRW-desired owner — every position diverges, and the divergence spans BOTH
/// pairs (asserted), so a `max_parallel = 2` pass has genuinely disjoint concurrent moves.
fn build_pair_diverged(tag: &str) -> PairDiverged {
    let (queries, titles) = build_corpus();
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let k = 6usize;
    let cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let nodes = spin_n_servers(&rt, &norm, tag, 4);

    let desired: Vec<u64> = (0..k as u32).map(hrw_primary_over_1_to_4).collect();
    let stale: Vec<u64> = desired.iter().map(|&d| pair_swap(d)).collect();
    // The parallelism precondition: desired owners span both pairs ({1,3} and {2,4}), so a wave
    // can run one move from each pair concurrently. Deterministic (fnv1a64); if the allocator's
    // hash ever changes and packs one pair, widen K rather than weakening the test.
    assert!(
        desired.iter().any(|&d| d % 2 == 1) && desired.iter().any(|&d| d % 2 == 0),
        "test setup: HRW must place positions in both node pairs: {desired:?}"
    );

    // Physically place each position's data on its STALE node, then commit that same map.
    let endpoints: Vec<String> = stale.iter().map(|&s| nodes[(s - 1) as usize].ep.clone()).collect();
    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &endpoints,
        rt.handle(),
    )
    .expect("connect cluster over the stale placement");
    cluster.ingest(&queries).expect("ingest corpus over gRPC");
    let plan: Vec<usize> = stale.iter().map(|&s| (s - 1) as usize).collect();
    seed_map(&cluster, &nodes, &plan);

    let counts = cluster.shard_query_counts().expect("per-shard counts");
    assert!(
        counts.iter().all(|&c| c > 0),
        "every position's slot is populated on its stale node: {counts:?}"
    );

    PairDiverged {
        cluster,
        nodes,
        rt,
        norm,
        dict,
        cfg,
        queries,
        titles,
        k,
        desired,
    }
}

/// Drain any fence-window writes queued for partial-apply repair (what an operator / a reopen does).
fn converge_repairs(cluster: &ClusterEngine) {
    for _ in 0..50 {
        if cluster.pending_repairs() == 0 {
            break;
        }
        let _ = cluster.resync();
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(cluster.pending_repairs(), 0, "fence-window writes converge");
}

/// Every position's committed owner is now its HRW-desired node, no slot was lost, and both the
/// live cluster and a fresh resolve-only coordinator match the brute oracle (zero FN).
fn assert_converged_and_zero_fn(d: &PairDiverged, oracle: &[HashSet<u64>]) {
    let state = d.cluster.control_state().expect("control state");
    for pos in 0..d.k {
        assert_eq!(
            owner(&state, pos),
            Some(NodeId(d.desired[pos])),
            "position {pos}'s committed owner is the HRW-desired node"
        );
    }
    let counts = d
        .cluster
        .shard_query_counts()
        .expect("per-shard counts after the sweep");
    assert!(
        counts.iter().all(|&c| c > 0),
        "no slot lost across the parallel sweep: {counts:?}"
    );
    for (i, title) in d.titles.iter().enumerate() {
        let got: HashSet<u64> = d
            .cluster
            .percolate(title)
            .expect("percolate live")
            .into_iter()
            .collect();
        assert_eq!(got, oracle[i], "live cluster vs brute on {title:?}");
    }
    let resolved = primary_endpoints(&state);
    let coord2 = ClusterEngine::connect_remote(
        Arc::clone(&d.norm),
        Arc::clone(&d.dict),
        empty_tag_dict(),
        &d.cfg,
        &resolved,
        d.rt.handle(),
    )
    .expect("fresh coordinator over the resolved (committed) endpoints");
    for (i, title) in d.titles.iter().enumerate() {
        let got: HashSet<u64> = coord2
            .percolate(title)
            .expect("percolate via restart coordinator")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle[i],
            "restart coordinator (routed by the committed map) vs brute on {title:?}"
        );
    }
}

#[test]
fn grpc_reconcile_parallel_disjoint_pairs_converge_zero_fn_under_writes() {
    let d = build_pair_diverged("parallel_reconcile");

    // A deterministic final live set: 20 adds of matching DSLs streamed concurrently with the
    // parallel reconcile (mirrors the sequential proof in `reconcile.rs`).
    let mut next_id = d.queries.iter().map(|(id, _)| *id).max().unwrap_or(0) + 1;
    let by_id: HashMap<u64, String> = d.queries.iter().map(|(id, x)| (*id, x.clone())).collect();
    let oracle_corpus = build_oracle(&d.queries, &d.titles);
    let matched: Vec<u64> = {
        let mut s: HashSet<u64> = HashSet::new();
        for set in &oracle_corpus {
            s.extend(set);
        }
        let mut v: Vec<u64> = s.into_iter().collect();
        v.sort_unstable();
        v
    };
    assert!(matched.len() >= 20, "need ≥20 matching queries");
    let additions: Vec<(u64, String)> = matched
        .iter()
        .take(20)
        .map(|id| {
            let nid = next_id;
            next_id += 1;
            (nid, by_id[id].clone())
        })
        .collect();
    let final_live: Vec<(u64, String)> = d
        .queries
        .iter()
        .cloned()
        .chain(additions.iter().cloned())
        .collect();
    let oracle_final = build_oracle(&final_live, &d.titles);
    assert!(
        oracle_corpus != oracle_final,
        "test setup: the concurrent adds must change some title results"
    );

    // RECONCILE at wave parallelism 2, concurrently with a writer streaming the additions.
    let report = std::thread::scope(|s| {
        let cluster_ref = &d.cluster;
        let adds = &additions;
        let writer = s.spawn(move || {
            for (id, dsl) in adds {
                stream_add(cluster_ref, *id, dsl);
                std::thread::sleep(Duration::from_millis(1));
            }
        });
        let report = d.cluster.reconcile_with(1, 2, d.rt.handle());
        writer.join().expect("writer thread");
        report
    })
    .expect("parallel reconcile");

    // Every position moved + committed (position-sorted report), fully converged.
    assert_eq!(
        report.reconciled,
        (0..d.k as u32).collect::<Vec<_>>(),
        "the parallel pass moved + committed every diverged position: {report:?}"
    );
    assert!(
        report.is_converged() && report.skipped.is_empty(),
        "a clean pass: no failed / uncommitted / concurrently-resolved positions: {report:?}"
    );
    assert_eq!(
        d.cluster.handoff_generations(),
        vec![1; d.k],
        "every position's routing flipped exactly once"
    );

    converge_repairs(&d.cluster);

    // IDEMPOTENCE at the same parallelism: a second parallel pass over the converged map moves
    // nothing, commits nothing (epoch invariant), re-flips nothing.
    let epoch_before = d.cluster.control_state().expect("state").epoch;
    let report2 = d
        .cluster
        .reconcile_with(1, 2, d.rt.handle())
        .expect("second parallel reconcile");
    assert!(
        report2.is_converged() && report2.moved_count() == 0 && report2.skipped.is_empty(),
        "a converged map reconciles to a no-op at any parallelism: {report2:?}"
    );
    assert_eq!(
        d.cluster.control_state().expect("state").epoch,
        epoch_before,
        "a no-op parallel reconcile commits nothing (epoch invariant)"
    );
    assert_eq!(
        d.cluster.handoff_generations(),
        vec![1; d.k],
        "a no-op parallel reconcile does not re-flip routing"
    );

    assert_converged_and_zero_fn(&d, &oracle_final);

    for n in &d.nodes {
        let _ = std::fs::remove_dir_all(&n.dir);
    }
}

#[test]
fn grpc_rebalance_and_move_parallel_converges_zero_fn() {
    let d = build_pair_diverged("parallel_rebalance");
    let oracle = build_oracle(&d.queries, &d.titles);

    // The MANUAL sweep at wave parallelism 2: every diverged position moves + commits in one call.
    let report = d
        .cluster
        .rebalance_and_move_with(1, 2, d.rt.handle())
        .expect("parallel rebalance_and_move");
    assert!(
        report.failed.is_none() && report.not_attempted.is_empty(),
        "a clean parallel sweep: {report:?}"
    );
    assert_eq!(
        report.moved,
        (0..d.k as u32).collect::<Vec<_>>(),
        "every diverged position moved + committed (position-sorted): {report:?}"
    );

    converge_repairs(&d.cluster);

    // FIXPOINT: a second parallel call finds nothing to move.
    let report2 = d
        .cluster
        .rebalance_and_move_with(1, 2, d.rt.handle())
        .expect("second parallel rebalance_and_move");
    assert!(
        report2.moved.is_empty() && report2.failed.is_none() && report2.not_attempted.is_empty(),
        "the converged map is a fixpoint: {report2:?}"
    );

    assert_converged_and_zero_fn(&d, &oracle);

    for n in &d.nodes {
        let _ = std::fs::remove_dir_all(&n.dir);
    }
}
