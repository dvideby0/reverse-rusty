//! RF>1 data-moving reconciliation (ADR-094): the unattended `reconcile` — and the group-move
//! primitive `reassign_group_and_move` under it — converge a REPLICATED cluster's committed map to
//! the HRW-desired GROUPS (primary + replicas) by moving data, zero-FN. Where `reconcile.rs` proves
//! the RF=1 controller and `replication_colocation.rs` proves RF=2 hosting/failover, THIS proves the
//! two compose: a replicated group physically moves, the moved-to group actually replicates (the
//! de-replication trap is dead), fence-window writes re-drive into the NEW group, and a coordinator
//! restart boots RF=2 from the committed map.
//!
//! Six proofs:
//!  - `grpc_reconcile_rf2_packed_converges_groups_failover_and_restart_zero_fn` — the headline: a
//!    packed RF=2 committed map (every primary on node A, every replica on B, C empty) converges to
//!    the HRW-desired groups (set-compare), zero-FN, idempotent second pass (epoch + generations
//!    invariant), a fresh coordinator boots RF=2 from the resolved committed map, and — the KILL
//!    SHOT — stopping a moved position's NEW primary node still serves every title from the new
//!    replicas, on the live AND the restarted coordinator.
//!  - `grpc_reconcile_rf2_under_concurrent_writer_zero_fn` — the same convergence under a firehose
//!    writer; fence-window writes converge via `resync` and are then served by the NEW group's
//!    replicas after the new primary dies (they re-drove through the swapped backing).
//!  - `grpc_group_move_replica_only_zero_fn` — {A;[B]} → {A;[C]}: the replica-only shape (no
//!    primary move, F = the fresh C, cp retained ⇒ fenced-then-unfenced); kill A ⇒ C serves.
//!  - `grpc_group_move_promotion_zero_fn` — {A;[B]} → {B;[A]}: the pure promotion (F = ∅, the
//!    freeze-probe is the only convergence witness; cp demoted to replica ⇒ MUST be unfenced after
//!    the swap); post-move writes land on B and fan to A — kill B ⇒ A serves them.
//!  - `grpc_group_move_abort_rolls_back` — `handoff_final_drain_cap = 0` forces the freeze-probe
//!    abort: `Err`, committed map + epoch + routing untouched, the source auto-unfenced (a
//!    subsequent write succeeds), zero-FN throughout.
//!  - `grpc_reconcile_rf2_continues_past_downed_target` — one desired node down ⇒ its positions
//!    land in `report.failed` while the others converge (the unattended continue-past-failure at
//!    RF>1); re-registering the node at a live endpoint lets the next pass converge fully.

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::time::Duration;

use reverse_rusty::cluster::{
    ClusterConfig, ClusterEngine, NodeDescriptor, NodeId, NodeRole, ShardAssignment, ShardGroup,
};
use reverse_rusty::normalize::Normalizer;

use crate::harness::*;

mod fixture;
use fixture::*;

/// The headline: a packed RF=2 map converges to the HRW-desired GROUPS with zero false negatives —
/// then the moved-to groups PROVE they replicate (kill a new primary's node; the new replicas
/// serve), live and across an RF=2 resolve-only coordinator restart. Also the controller
/// idempotence: a second pass moves nothing (epoch + handoff generations invariant).
#[test]
fn grpc_reconcile_rf2_packed_converges_groups_failover_and_restart_zero_fn() {
    let (queries, titles) = build_corpus();
    let oracle = build_oracle(&queries, &titles);
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let k = 4usize;
    let cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        replication_factor: 2,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let (cluster, servers) = build_packed_rf2(&rt, &norm, &dict, &cfg, &queries, "rf2_pack");

    let counts0 = cluster.shard_query_counts().expect("per-shard counts");
    assert!(
        counts0.len() == k && counts0.iter().all(|&c| c > 0),
        "all packed slots populated: {counts0:?}"
    );

    // ONE unattended pass converges every diverging GROUP (several positions must move off the
    // packed layout — assert the setup really diverges, coupled to the allocator on purpose).
    let node_ids = [1u64, 2, 3];
    let diverging: Vec<u32> = (0..k as u32)
        .filter(|&p| hrw_group(p, &node_ids, 2) != (1, vec![2]))
        .collect();
    assert!(
        diverging.len() >= 2,
        "setup: the packed map must diverge from HRW for ≥2 of {k} positions (allocator changed?)"
    );
    let report = cluster.reconcile(2, rt.handle()).expect("rf=2 reconcile");
    assert!(
        report.is_converged() && report.failed.is_empty() && report.uncommitted.is_empty(),
        "a clean pass: {report:?}"
    );
    assert_eq!(
        {
            let mut r = report.reconciled.clone();
            r.sort_unstable();
            r
        },
        diverging,
        "exactly the HRW-diverging positions moved + committed: {report:?}"
    );

    converge_repairs(&cluster);

    // The committed map now equals the HRW-desired GROUPS (primary identity + replica SET).
    let state = cluster.control_state().expect("control state");
    for p in 0..k as u32 {
        let (want_p, want_r) = hrw_group(p, &node_ids, 2);
        let (got_p, got_r) = group_of(&state, p);
        assert_eq!(got_p, want_p, "position {p}: committed primary = HRW desired");
        assert_eq!(
            got_r,
            want_r.into_iter().collect::<BTreeSet<u64>>(),
            "position {p}: committed replica SET = HRW desired"
        );
    }

    // No slot lost + zero-FN across the whole reconciled replicated cluster.
    let counts1 = cluster.shard_query_counts().expect("counts after");
    assert!(
        counts1.len() == k && counts1.iter().all(|&c| c > 0),
        "no position lost its data: {counts1:?}"
    );
    // Unmoved positions keep their exact count (sibling-intactness across co-located moves).
    for p in 0..k {
        if !report.reconciled.contains(&(p as u32)) {
            assert_eq!(counts1[p], counts0[p], "unmoved position {p} byte-identical");
        }
    }
    assert_matches_oracle(&cluster, &titles, &oracle, "reconciled live cluster");

    // IDEMPOTENCE: a second pass over the converged replicated map commits + re-flips nothing.
    let epoch_before = state.epoch;
    let gens_before = cluster.handoff_generations();
    let report2 = cluster.reconcile(2, rt.handle()).expect("second pass");
    assert!(
        report2.is_converged() && report2.moved_count() == 0 && report2.skipped.is_empty(),
        "converged RF=2 map reconciles to a no-op: {report2:?}"
    );
    assert_eq!(
        cluster.control_state().expect("state").epoch,
        epoch_before,
        "no-op pass commits nothing (epoch invariant)"
    );
    assert_eq!(
        cluster.handoff_generations(),
        gens_before,
        "no-op pass re-flips nothing"
    );

    // RF=2 RESTART: a fresh coordinator boots REPLICATED groups purely from the committed map.
    let coord2 = ClusterEngine::connect_replicated(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &resolved_groups(&state),
        rt.handle(),
    )
    .expect("fresh RF=2 coordinator over the resolved committed map");
    assert_matches_oracle(&coord2, &titles, &oracle, "restart coordinator");

    // THE KILL SHOT (the de-replication trap, dead): stop the node hosting a MOVED position's NEW
    // primary — its whole slot set goes down — and every title is still served from the new
    // replicas, on the live AND the restarted coordinator. Pre-ADR-094 this is exactly where a
    // "converged" map without real replica placements would go dark.
    let moved_pos = report.reconciled[0];
    let (new_primary, _) = group_of(&state, moved_pos);
    let victim = &servers[(new_primary - 1) as usize];
    victim.jh.abort();
    wait_until_not_listening(victim.addr);
    assert_matches_oracle(
        &cluster,
        &titles,
        &oracle,
        "live cluster after the moved position's new primary died",
    );
    assert_matches_oracle(
        &coord2,
        &titles,
        &oracle,
        "restart coordinator after the moved position's new primary died",
    );

    teardown(&servers);
}

/// The packed convergence under a firehose writer: fence-window writes queue as pending repairs and
/// re-drive into the NEW groups via `resync` — proven by killing a moved position's new primary and
/// still reading the FINAL live set (base + concurrent adds) from the new replicas.
#[test]
fn grpc_reconcile_rf2_under_concurrent_writer_zero_fn() {
    let (queries, titles) = build_corpus();
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let k = 4usize;
    let cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        replication_factor: 2,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let (cluster, servers) = build_packed_rf2(&rt, &norm, &dict, &cfg, &queries, "rf2_writer");

    // A deterministic final live set: 12 adds of KNOWN-MATCHING DSLs streamed during the pass.
    let oracle_base = build_oracle(&queries, &titles);
    let matched: Vec<u64> = {
        let mut s: HashSet<u64> = HashSet::new();
        for set in &oracle_base {
            s.extend(set);
        }
        let mut v: Vec<u64> = s.into_iter().collect();
        v.sort_unstable();
        v
    };
    assert!(matched.len() >= 12, "need ≥12 matching queries");
    let by_id: std::collections::HashMap<u64, String> =
        queries.iter().map(|(id, x)| (*id, x.clone())).collect();
    let mut next_id = queries.iter().map(|(id, _)| *id).max().unwrap_or(0) + 1;
    let additions: Vec<(u64, String)> = matched
        .iter()
        .take(12)
        .map(|id| {
            let nid = next_id;
            next_id += 1;
            (nid, by_id[id].clone())
        })
        .collect();
    let final_live: Vec<(u64, String)> = queries
        .iter()
        .cloned()
        .chain(additions.iter().cloned())
        .collect();
    let oracle_final = build_oracle(&final_live, &titles);

    let report = std::thread::scope(|s| {
        let cluster_ref = &cluster;
        let adds = &additions;
        let writer = s.spawn(move || {
            for (id, dsl) in adds {
                stream_add(cluster_ref, *id, dsl);
                std::thread::sleep(Duration::from_millis(1));
            }
        });
        let report = cluster.reconcile(2, rt.handle());
        writer.join().expect("writer thread");
        report
    })
    .expect("rf=2 reconcile under writer");
    assert!(
        report.is_converged() && !report.reconciled.is_empty(),
        "the pass converged and moved the diverging groups: {report:?}"
    );

    converge_repairs(&cluster);
    assert_matches_oracle(&cluster, &titles, &oracle_final, "post-writer live cluster");

    // Kill a moved position's NEW primary: the final live set — INCLUDING the fence-window adds —
    // must be served by the new replicas (the repairs re-drove through the swapped backing).
    let state = cluster.control_state().expect("state");
    let moved_pos = report.reconciled[0];
    let (new_primary, _) = group_of(&state, moved_pos);
    let victim = &servers[(new_primary - 1) as usize];
    victim.jh.abort();
    wait_until_not_listening(victim.addr);
    assert_matches_oracle(
        &cluster,
        &titles,
        &oracle_final,
        "final live set after the new primary died (fence-window writes on the new replicas)",
    );

    teardown(&servers);
}

/// Replica-only move {A;[B]} → {A;[C]}: no primary change (cp retained ⇒ fenced then unfenced), the
/// fresh C is bulk-established + drained. Kill A afterwards ⇒ the NEW replica C serves every title
/// (B is orphaned out of the composite; a stale composite still reading B would ALSO pass here, but
/// killing A proves C — the newly-placed member — actually holds the data and is in-sync).
#[test]
fn grpc_group_move_replica_only_zero_fn() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let (cluster, servers, titles, oracle, _q) = one_position_rf2("rf2_repl_only", &rt, false);

    let desired = ShardAssignment {
        position: 0,
        primary: NodeId(1),
        replicas: vec![NodeId(3)],
    };
    let outcome = cluster
        .reassign_group_and_move(0, desired, rt.handle())
        .expect("replica-only group move");
    assert!(
        matches!(
            outcome,
            reverse_rusty::cluster::ReassignOutcome::Moved { .. }
        ),
        "the replica-only change is a MOVE (data to C): {outcome:?}"
    );
    let state = cluster.control_state().expect("state");
    assert_eq!(
        group_of(&state, 0),
        (1, BTreeSet::from([3])),
        "committed group re-pointed to {{A;[C]}}"
    );
    converge_repairs(&cluster);
    assert_matches_oracle(&cluster, &titles, &oracle, "post replica-only move");

    // Writes still land on the (unfenced) retained primary A — the unfence-after-swap proof for
    // the replica-only shape.
    cluster
        .add_query(9_000_001, "+nike +shoe")
        .expect("post-move write lands on the retained, unfenced primary");

    // Kill A: reads fail over to C, the NEW replica — which must hold the whole corpus.
    servers[0].jh.abort();
    wait_until_not_listening(servers[0].addr);
    assert_matches_oracle(&cluster, &titles, &oracle, "C serves after A died");

    teardown(&servers);
}

/// Pure promotion {A;[B]} → {B;[A]}: F = ∅ (both members retained), so the freeze-probe is the only
/// convergence witness; the demoted cp (A) MUST be unfenced after the swap or its first fan-out
/// silently desyncs it. Proven end-to-end: a post-move write lands on the new primary B, fans to
/// the retained replica A — kill B and A serves it.
#[test]
fn grpc_group_move_promotion_zero_fn() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let (cluster, servers, titles, oracle, queries) = one_position_rf2("rf2_promote", &rt, false);

    let desired = ShardAssignment {
        position: 0,
        primary: NodeId(2),
        replicas: vec![NodeId(1)],
    };
    let outcome = cluster
        .reassign_group_and_move(0, desired, rt.handle())
        .expect("promotion group move");
    assert!(
        matches!(
            outcome,
            reverse_rusty::cluster::ReassignOutcome::Moved { .. }
        ),
        "the promotion is a MOVE (roles swap, map commits): {outcome:?}"
    );
    let state = cluster.control_state().expect("state");
    assert_eq!(
        group_of(&state, 0),
        (2, BTreeSet::from([1])),
        "committed group promoted to {{B;[A]}}"
    );
    converge_repairs(&cluster);
    assert_matches_oracle(&cluster, &titles, &oracle, "post promotion");

    // A post-move write: primary-first onto B, fanned to the retained (unfenced) replica A. Reuse a
    // known-matching DSL under a fresh id so the expected sets are deterministic.
    let (donor_id, donor_dsl) = {
        let matched = oracle
            .iter()
            .flat_map(|s| s.iter().copied())
            .next()
            .expect("some matching query");
        let dsl = queries
            .iter()
            .find(|(id, _)| *id == matched)
            .map(|(_, d)| d.clone())
            .expect("donor DSL");
        (matched, dsl)
    };
    let new_id = 9_000_002u64;
    cluster
        .add_query(new_id, &donor_dsl)
        .expect("post-promotion write lands on the new primary");
    let oracle_after: Vec<HashSet<u64>> = oracle
        .iter()
        .map(|s| {
            let mut s = s.clone();
            if s.contains(&donor_id) {
                s.insert(new_id);
            }
            s
        })
        .collect();
    assert_matches_oracle(&cluster, &titles, &oracle_after, "post-promotion write visible");

    // Kill the new primary B: the retained replica A must serve everything INCLUDING the post-move
    // write — proving A was re-established, unfenced, and receiving fan-out (a still-fenced A would
    // have desynced on the first fan-out and been excluded from failover).
    servers[1].jh.abort();
    wait_until_not_listening(servers[1].addr);
    assert_matches_oracle(
        &cluster,
        &titles,
        &oracle_after,
        "retained replica A serves the post-move write after B died",
    );

    teardown(&servers);
}

/// Fail-closed: `handoff_final_drain_cap = 0` forces the freeze-probe abort mid-move. The position
/// rolls back fully — committed map, epoch, and routing untouched; the fenced source auto-unfences
/// (a subsequent write succeeds) — and every read stays zero-FN throughout.
#[test]
fn grpc_group_move_abort_rolls_back() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let (cluster, servers, titles, oracle, _q) = one_position_rf2("rf2_abort", &rt, true);

    let epoch_before = cluster.control_state().expect("state").epoch;
    let gens_before = cluster.handoff_generations();
    let err = cluster
        .reassign_group_and_move(
            0,
            ShardAssignment {
                position: 0,
                primary: NodeId(1),
                replicas: vec![NodeId(3)],
            },
            rt.handle(),
        )
        .expect_err("cap=0 forces the freeze-probe abort");
    assert!(
        err.to_string().contains("did not converge"),
        "the abort names the convergence failure: {err}"
    );

    let state = cluster.control_state().expect("state");
    assert_eq!(
        group_of(&state, 0),
        (1, BTreeSet::from([2])),
        "committed map untouched by the aborted move"
    );
    assert_eq!(state.epoch, epoch_before, "epoch untouched");
    assert_eq!(
        cluster.handoff_generations(),
        gens_before,
        "routing untouched (no flip)"
    );
    // AUTO-UNFENCE: the source accepts writes again.
    cluster
        .add_query(9_000_003, "+nike +shoe")
        .expect("the auto-unfenced source accepts a subsequent write");
    assert_matches_oracle(&cluster, &titles, &oracle, "zero-FN after the aborted move");

    teardown(&servers);
}

/// The unattended continue-past-failure at RF>1: with one desired node DOWN, the positions whose
/// desired group includes it land in `report.failed` while every other diverging position converges
/// — then re-registering the node at a live endpoint lets the next pass converge fully (the
/// self-healing loop semantics). Four nodes (rf=2 over three would put the third node in EVERY
/// group — no C-free target could exist); the victim is picked DYNAMICALLY from the hash mirror so
/// the test self-adapts if the allocator's placement changes.
#[test]
fn grpc_reconcile_rf2_continues_past_downed_target() {
    let (queries, titles) = build_corpus();
    let oracle = build_oracle(&queries, &titles);
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let k = 6usize;
    let cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        replication_factor: 2,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let norm2 = Arc::clone(&norm);
    let (cluster, mut servers) = {
        // Four nodes: packed on A(1)+B(2) as usual, C(3)/D(4) fresh.
        let servers = spin_n_durable(&rt, &norm2, "rf2_downed", 4);
        let groups: Vec<ShardGroup> = (0..k)
            .map(|_| ShardGroup {
                primary: servers[0].ep.clone(),
                replicas: vec![servers[1].ep.clone()],
            })
            .collect();
        let cluster = ClusterEngine::connect_replicated(
            Arc::clone(&norm),
            Arc::clone(&dict),
            empty_tag_dict(),
            &cfg,
            &groups,
            rt.handle(),
        )
        .expect("connect packed RF=2 cluster over 4 nodes");
        cluster.ingest(&queries).expect("ingest corpus over gRPC");
        let plan: Vec<(usize, Vec<usize>)> = (0..k).map(|_| (0usize, vec![1usize])).collect();
        seed_group_map(&cluster, &servers, &plan);
        (cluster, servers)
    };

    // Pick the VICTIM: a non-source node (never 1 — it holds every move's data) that some diverging
    // desired groups include and others exclude. Assert one exists (coupled to the allocator).
    let node_ids = [1u64, 2, 3, 4];
    let in_group = |p: u32, n: u64| {
        let (pr, rs) = hrw_group(p, &node_ids, 2);
        pr == n || rs.contains(&n)
    };
    let diverging: Vec<u32> = (0..k as u32)
        .filter(|&p| hrw_group(p, &node_ids, 2) != (1, vec![2]))
        .collect();
    let victim = [2u64, 3, 4]
        .into_iter()
        .find(|&v| {
            diverging.iter().any(|&p| in_group(p, v))
                && diverging.iter().any(|&p| !in_group(p, v))
        })
        .expect("setup: some non-source node must split the diverging targets (allocator changed?)");
    let needs_victim: Vec<u32> = diverging.iter().copied().filter(|&p| in_group(p, victim)).collect();
    let victim_free: Vec<u32> = diverging
        .iter()
        .copied()
        .filter(|&p| !in_group(p, victim))
        .collect();
    let vi = (victim - 1) as usize;
    servers[vi].jh.abort();
    wait_until_not_listening(servers[vi].addr);

    let report = cluster
        .reconcile(2, rt.handle())
        .expect("pass with the victim down");
    let failed_positions: Vec<u32> = report.failed.iter().map(|(p, _)| *p).collect();
    for p in &needs_victim {
        assert!(
            failed_positions.contains(p),
            "position {p} needs downed node {victim} and must be recorded failed: {report:?}"
        );
    }
    for p in &victim_free {
        assert!(
            report.reconciled.contains(p),
            "position {p} is victim-free and must converge in the SAME pass \
             (continue-past-failure): {report:?}"
        );
    }
    converge_repairs(&cluster);
    assert_matches_oracle(&cluster, &titles, &oracle, "partial pass (victim down)");

    // "Restart" the victim at a fresh endpoint and RE-REGISTER its node id there — the next pass
    // self-heals: everything converges, nothing left pending.
    let fresh = spin_durable(&rt, &norm, "rf2_downed_fresh");
    cluster
        .register_node(NodeDescriptor {
            id: NodeId(victim),
            addr: Some(fresh.ep.clone()),
            role: NodeRole::Data,
        })
        .expect("re-register the victim node at the fresh endpoint");
    let report2 = cluster.reconcile(2, rt.handle()).expect("healing pass");
    assert!(
        report2.is_converged() && report2.failed.is_empty(),
        "the healing pass converges everything: {report2:?}"
    );
    let state = cluster.control_state().expect("state");
    for p in 0..k as u32 {
        let (want_p, want_r) = hrw_group(p, &node_ids, 2);
        let (got_p, got_r) = group_of(&state, p);
        assert_eq!(
            (got_p, got_r),
            (want_p, want_r.into_iter().collect::<BTreeSet<u64>>()),
            "position {p} fully converged after the heal"
        );
    }
    converge_repairs(&cluster);
    assert_matches_oracle(&cluster, &titles, &oracle, "after the healing pass");

    servers.push(fresh);
    teardown(&servers);
}
