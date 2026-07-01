//! The unattended re-point reconciler over gRPC (ADR-092): `reconcile` (and the autoscaler's
//! membership-drift arm, now data-moving on a remote cluster) converges the committed shard→node map to
//! the desired HRW placement by MOVING data — live under a concurrent writer — and a resolve-only
//! coordinator restart routes to the new owner with zero false negatives. Where `reassign.rs` proves the
//! manual `reassign_and_move` primitive, this proves the UNATTENDED controller (and the autoscaler
//! safety fix) built on it.
//!
//! Three proofs:
//!  - `grpc_reconcile_moves_to_desired_under_writes_and_restart_routes_zero_fn` — the headline: a
//!    diverged committed map converges to the HRW-desired owner under a concurrent writer, a second pass
//!    is a no-op (idempotence / no-thrash), and a fresh coordinator routing by the committed map is
//!    zero-FN.
//!  - `grpc_reconcile_colocated_packing_converges_zero_fn` — the UNATTENDED controller on the packed
//!    K>N multi-shard topology (ADR-093): the exact scenario that PARKED the reconciler (HRW packs
//!    several positions onto one node; pre-Stage-1 a one-shard `RecoverFrom` clobbered the earlier
//!    move). `reconcile` now converges the whole packed map — no slot lost, ≥2 positions co-located on
//!    a destination, zero-FN, idempotent (epoch-invariant) second pass, restart routes zero-FN. The
//!    manual-sweep analogue is `rebalance.rs`; THIS is the proof the reconciler itself is collision-safe.
//!  - `grpc_autoscaler_tick_drives_data_moving_rebalance_zero_fn` — the ADR-086 safety fix: on a remote
//!    cluster the autoscaler's `tick` drives the DATA-MOVING rebalance (not the map-only one that would
//!    manufacture a false negative), so a membership change converges routing automatically, zero-FN.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use reverse_rusty::cluster::{
    AutoscaleConfig, ClusterConfig, ClusterEngine, NodeDescriptor, NodeId, NodeRole, ScalingAction,
    ShardAssignment,
};
use reverse_rusty::dict::Dict;
use reverse_rusty::normalize::Normalizer;

use crate::harness::*;
use crate::relocation::{owner, primary_endpoints, seed_map, spin_n_servers};

/// Mirror `allocator::hrw_weight` (the stable rendezvous hash) so the test can compute the HRW-desired
/// primary and deterministically seed the OPPOSITE node — guaranteeing a real move regardless of which
/// way the hash falls. Coupled to the allocator on purpose: if its placement changes, this asserts
/// loudly (the seeded node would no longer be the non-desired one) rather than silently no-op'ing.
fn hrw_weight(position: u32, node: u64) -> u64 {
    let mut bytes = [0u8; 12];
    bytes[0..4].copy_from_slice(&position.to_le_bytes());
    bytes[4..12].copy_from_slice(&node.to_le_bytes());
    reverse_rusty::util::fnv1a64(&bytes)
}

/// The HRW-desired primary for `position` over data nodes {1, 2} (RF=1: argmax weight, tie → lower id —
/// exactly `allocator::plan_assignments`).
fn hrw_primary_over_1_2(position: u32) -> u64 {
    if hrw_weight(position, 1) >= hrw_weight(position, 2) {
        1
    } else {
        2
    }
}

/// A 1-shard remote cluster whose committed map DIVERGES from the HRW-desired placement: the data and
/// the committed map both name the `stale` node, while HRW wants `desired`. The caller drives
/// convergence (`reconcile` or the autoscaler `tick`) and asserts the move + restart-zero-FN.
struct Diverged {
    cluster: ClusterEngine,
    nodes: TwoNode,
    rt: tokio::runtime::Runtime,
    norm: Arc<Normalizer>,
    dict: Arc<Dict>,
    cfg: ClusterConfig,
    queries: Vec<(u64, String)>,
    titles: Vec<String>,
    desired: u64,
    desired_ep: String,
}

fn build_diverged(tag: &str) -> Diverged {
    let (queries, titles) = build_corpus();
    let desired = hrw_primary_over_1_2(0);
    let stale = if desired == 1 { 2 } else { 1 };

    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let cfg = ClusterConfig {
        num_shards: 1,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let nodes = spin_two_servers(&rt, &norm, tag);
    let ep_of = |id: u64| {
        if id == 1 {
            nodes.src_ep.clone()
        } else {
            nodes.tgt_ep.clone()
        }
    };
    let stale_ep = ep_of(stale);
    let desired_ep = ep_of(desired);

    // The coordinator routes position 0 to the STALE node's endpoint (where the data will live).
    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        std::slice::from_ref(&stale_ep),
        rt.handle(),
    )
    .expect("connect cluster over the stale node");
    cluster.ingest(&queries).expect("ingest corpus");

    // Register both data nodes and COMMIT the diverged map: position 0 → the stale node (where the data
    // physically is). The HRW-desired is the OTHER node, so a convergence pass must move it.
    cluster
        .register_node(NodeDescriptor {
            id: NodeId(1),
            addr: Some(nodes.src_ep.clone()),
            role: NodeRole::Data,
        })
        .expect("register node 1");
    cluster
        .register_node(NodeDescriptor {
            id: NodeId(2),
            addr: Some(nodes.tgt_ep.clone()),
            role: NodeRole::Data,
        })
        .expect("register node 2");
    cluster
        .reassign_shard(ShardAssignment {
            position: 0,
            primary: NodeId(stale),
            replicas: Vec::new(),
        })
        .expect("seed the diverged committed map");
    assert_eq!(
        cluster.handoff_generations(),
        vec![0],
        "position 0 starts at generation 0 on the stale node"
    );

    Diverged {
        cluster,
        nodes,
        rt,
        norm,
        dict,
        cfg,
        queries,
        titles,
        desired,
        desired_ep,
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

/// Assert the live cluster AND a fresh resolve-only coordinator both match the brute oracle (zero FN).
fn assert_live_and_restart_match(d: &Diverged, oracle: &[HashSet<u64>]) {
    for (i, title) in d.titles.iter().enumerate() {
        let got: HashSet<u64> = d
            .cluster
            .percolate(title)
            .expect("percolate live")
            .into_iter()
            .collect();
        assert_eq!(got, oracle[i], "live cluster vs brute on {title:?}");
    }
    let resolved = primary_endpoints(&d.cluster.control_state().expect("state"));
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
fn grpc_reconcile_moves_to_desired_under_writes_and_restart_routes_zero_fn() {
    let d = build_diverged("reconcile");

    // A deterministic final live set: 20 adds of matching DSLs streamed concurrently with the reconcile.
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

    // RECONCILE concurrently with a writer streaming the additions through the cluster.
    let report = std::thread::scope(|s| {
        let cluster_ref = &d.cluster;
        let adds = &additions;
        let writer = s.spawn(move || {
            for (id, dsl) in adds {
                stream_add(cluster_ref, *id, dsl);
                std::thread::sleep(Duration::from_millis(1));
            }
        });
        let report = d.cluster.reconcile(1, d.rt.handle());
        writer.join().expect("writer thread");
        report
    })
    .expect("reconcile");

    // The pass moved + committed exactly position 0, and fully converged.
    assert_eq!(
        report.reconciled,
        vec![0],
        "reconcile moved + committed position 0: {report:?}"
    );
    assert!(
        report.is_converged() && report.skipped.is_empty(),
        "the pass converged with no pending work: {report:?}"
    );
    assert_eq!(
        d.cluster.handoff_generations(),
        vec![1],
        "the reconcile bumped position 0's generation"
    );

    // The committed map now names the HRW-desired node.
    let state = d.cluster.control_state().expect("control state");
    assert_eq!(
        state
            .assignments
            .iter()
            .find(|a| a.position == 0)
            .map(|a| a.primary),
        Some(NodeId(d.desired)),
        "the committed map names the HRW-desired node"
    );
    assert_eq!(
        primary_endpoints(&state),
        vec![d.desired_ep.clone()],
        "resolving the committed map yields the desired endpoint"
    );

    converge_repairs(&d.cluster);

    // IDEMPOTENCE / no-thrash: a SECOND reconcile on the converged map moves nothing and commits nothing
    // (the control-plane epoch is invariant, and routing is not re-flipped).
    let epoch_before = d.cluster.control_state().expect("state").epoch;
    let report2 = d
        .cluster
        .reconcile(1, d.rt.handle())
        .expect("second reconcile");
    assert!(
        report2.is_converged() && report2.moved_count() == 0,
        "a converged map reconciles to a no-op: {report2:?}"
    );
    assert_eq!(
        d.cluster.control_state().expect("state").epoch,
        epoch_before,
        "a no-op reconcile commits nothing (epoch invariant)"
    );
    assert_eq!(
        d.cluster.handoff_generations(),
        vec![1],
        "a no-op reconcile does not re-flip routing"
    );

    // Live + restart zero-FN over the final live set.
    assert_live_and_restart_match(&d, &oracle_final);

    let _ = std::fs::remove_dir_all(&d.nodes.src_dir);
    let _ = std::fs::remove_dir_all(&d.nodes.tgt_dir);
}

#[test]
fn grpc_autoscaler_tick_drives_data_moving_rebalance_zero_fn() {
    let d = build_diverged("autoscale_move");
    let oracle = build_oracle(&d.queries, &d.titles);

    // Membership drift (2 registered nodes, 1 placed) ⇒ the policy emits exactly one Rebalance (skew +
    // split disabled). On a REMOTE cluster (the coordinator carries a runtime handle) the ADR-092 fix
    // makes that Rebalance DATA-MOVING — `rebalance_and_move`, not the map-only `rebalance` that would
    // manufacture the ADR-086 false negative.
    let config = AutoscaleConfig {
        enabled: true,
        target_replication_factor: 1,
        max_node_load_skew: 0.0,
        split_corpus_threshold: 0,
    };
    let decision = d.cluster.tick(&config).expect("tick");
    assert!(
        decision
            .actions
            .iter()
            .any(|a| matches!(a, ScalingAction::Rebalance { .. })),
        "membership drift must fire a rebalance: {decision:?}"
    );
    assert_eq!(
        d.cluster.handoff_generations(),
        vec![1],
        "the tick MOVED position 0's data (generation bumped) — not a map-only rebalance"
    );
    assert_eq!(
        d.cluster
            .control_state()
            .expect("state")
            .assignments
            .iter()
            .find(|a| a.position == 0)
            .map(|a| a.primary),
        Some(NodeId(d.desired)),
        "the tick committed the HRW-desired owner"
    );

    converge_repairs(&d.cluster);
    assert_live_and_restart_match(&d, &oracle);

    let _ = std::fs::remove_dir_all(&d.nodes.src_dir);
    let _ = std::fs::remove_dir_all(&d.nodes.tgt_dir);
}

/// The UNATTENDED controller on the packed K>N multi-shard topology — the exact scenario that parked
/// the reconciler (codex P1): the HRW-desired map packs several positions onto shared destination
/// nodes, and pre-ADR-093 a one-shard `RecoverFrom` clobbered the earlier move (a shard-sized false
/// negative). The parked oracle was `num_shards: 1`, so it could never see this; here `reconcile`
/// drives the same packed convergence `rebalance.rs` proves for the MANUAL sweep — through the
/// reconciler's own report + idempotence contract.
#[test]
fn grpc_reconcile_colocated_packing_converges_zero_fn() {
    let (queries, titles) = build_corpus();
    let oracle = build_oracle(&queries, &titles);

    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let k = 6usize;
    let cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    // Three nodes; ALL six positions packed on node A (index 0) — a deliberately non-HRW committed
    // map. The unattended pass must spread them, co-locating moves on the destinations.
    let nodes = spin_n_servers(&rt, &norm, "reconcile_pack", 3);
    let endpoints = vec![nodes[0].ep.clone(); k];
    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &endpoints,
        rt.handle(),
    )
    .expect("connect packed cluster (all positions on node A)");
    cluster.ingest(&queries).expect("ingest corpus over gRPC");
    seed_map(&cluster, &nodes, &vec![0usize; k]); // committed: every position → node A (id 1)

    let counts0 = cluster.shard_query_counts().expect("per-shard counts");
    assert_eq!(counts0.len(), k, "one count per position: {counts0:?}");
    assert!(
        counts0.iter().all(|&c| c > 0),
        "all six co-located slots populated on the packed node: {counts0:?}"
    );

    // ONE unattended pass converges the whole packed map, sequentially, continuing past nothing
    // (every move must succeed here) — reported through the reconciler's own vocabulary.
    let report = cluster.reconcile(1, rt.handle()).expect("reconcile");
    assert!(
        report.reconciled.len() >= 2,
        "HRW must move ≥2 positions off the packed node (co-locating on destinations): {report:?}"
    );
    assert!(
        report.is_converged() && report.skipped.is_empty(),
        "a clean pass: no failed / uncommitted / concurrently-resolved positions: {report:?}"
    );

    converge_repairs(&cluster);

    // Convergence: NO slot lost + zero false negatives across the whole reconciled cluster. A clobber
    // (the pre-ADR-093 failure) would empty a co-located slot and drop its queries' matches.
    let counts1 = cluster
        .shard_query_counts()
        .expect("per-shard counts after reconcile");
    assert_eq!(counts1.len(), k);
    assert!(
        counts1.iter().all(|&c| c > 0),
        "no slot lost after the unattended pass (a clobber would empty one): {counts1:?}"
    );
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate after reconcile")
            .into_iter()
            .collect();
        assert_eq!(got, oracle[i], "reconciled cluster vs brute on {title:?}");
    }

    // Packing proof: at least one DESTINATION node (≠ the packed origin A = NodeId 1) now owns ≥2
    // co-located positions — a move landed a second shard on a node that already received one, the
    // exact former clobber case, now per-slot-isolated.
    let state = cluster.control_state().expect("control state");
    let mut per_node: HashMap<NodeId, usize> = HashMap::new();
    for pos in 0..k {
        if let Some(n) = owner(&state, pos) {
            *per_node.entry(n).or_default() += 1;
        }
    }
    assert!(
        per_node.len() >= 2,
        "positions must spread across multiple nodes after reconcile: {per_node:?}"
    );
    assert!(
        per_node.iter().any(|(&n, &c)| n != NodeId(1) && c >= 2),
        "a destination node (≠ the packed origin) must own ≥2 co-located positions: {per_node:?}"
    );

    // IDEMPOTENCE (the controller-level hysteresis the driver loop relies on): a second pass over the
    // now-HRW-optimal map moves nothing, commits nothing (epoch invariant), re-flips nothing.
    let epoch_before = state.epoch;
    let gens_before = cluster.handoff_generations();
    let report2 = cluster.reconcile(1, rt.handle()).expect("second reconcile");
    assert!(
        report2.is_converged() && report2.moved_count() == 0 && report2.skipped.is_empty(),
        "a converged packed map reconciles to a no-op: {report2:?}"
    );
    assert_eq!(
        cluster.control_state().expect("state").epoch,
        epoch_before,
        "a no-op reconcile commits nothing (epoch invariant)"
    );
    assert_eq!(
        cluster.handoff_generations(),
        gens_before,
        "a no-op reconcile does not re-flip any position's routing"
    );

    // Resolve-only restart: a fresh coordinator routed purely by the committed map is zero-FN.
    let resolved = primary_endpoints(&state);
    let coord2 = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &resolved,
        rt.handle(),
    )
    .expect("fresh coordinator over the resolved committed map");
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = coord2
            .percolate(title)
            .expect("percolate via restart coordinator")
            .into_iter()
            .collect();
        assert_eq!(got, oracle[i], "restart coordinator vs brute on {title:?}");
    }

    for n in &nodes {
        let _ = std::fs::remove_dir_all(&n.dir);
    }
}
