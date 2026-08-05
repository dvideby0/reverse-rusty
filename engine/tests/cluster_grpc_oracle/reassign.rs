//! Data-moving live reassignment over gRPC (ADR-090): a committed shard→node assignment change that
//! MOVES the data and re-points routing — live under concurrent writes, and across a coordinator
//! restart. Where `handoff.rs` proves a bare `execute_handoff` is zero-FN, these prove the NEW
//! property: `reassign_and_move` commits the new owner WITH the move, so the committed map names the
//! target and a resolve-only restart routes there (the ADR-086 boot guard previously forbade this).
//!
//! Principal proofs:
//!  - `grpc_reassign_and_move_commits_map_and_restart_routes_zero_fn` — the primary proof: move under
//!    a concurrent writer, the committed map now names the target, and a FRESH coordinator resolving
//!    from that map lands on the new owner with zero false negatives.
//!  - `grpc_handoff_flip_without_commit_preserves_move_time_snapshot` — immediately after a flip
//!    without commit, both endpoints retain the complete move-time corpus. This deliberately makes
//!    no restart-safety claim after later writes advance only the live target.
//!  - `grpc_reassign_reconciles_an_existing_live_move_without_stale_recopy` — after an uncommitted
//!    raw flip and a newer write on the live target, reassign attests that target and commits it
//!    without copying again from the now-stale committed source.
//!  - `grpc_reassign_repairs_live_routing_without_recommitting_existing_target` — a committed
//!    target with divergent live routing is physically repaired without a redundant proposal.
//!  - `grpc_reassign_repair_clears_a_demoted_targets_stale_fence` — repairing back onto a previously
//!    demoted target clears its preserved slot fence before acknowledging a writable live primary.
//!  - `grpc_reassign_and_move_aborts_clean_and_does_not_commit` — fail-closed: a forced abort moves
//!    nothing, commits nothing, and auto-unfences the source.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use reverse_rusty::cluster::{
    ClusterConfig, ClusterEngine, NodeDescriptor, NodeId, NodeRole, ReassignOutcome, ShardError,
};

use crate::harness::*;
use crate::relocation::primary_endpoints;

/// Register source = node 1 (the src endpoint) and target = node 2 (the tgt endpoint), and commit a
/// position-preserving map: position 0 → node 1 (where the data physically lives after ingest). This
/// is the precondition `reassign_and_move` reads `from`/`to` endpoints from.
fn seed_committed_map(cluster: &ClusterEngine, src_ep: &str, tgt_ep: &str) {
    cluster
        .register_node(NodeDescriptor {
            id: NodeId(1),
            addr: Some(src_ep.to_string()),
            role: NodeRole::Data,
        })
        .expect("register source node");
    cluster
        .register_node(NodeDescriptor {
            id: NodeId(2),
            addr: Some(tgt_ep.to_string()),
            role: NodeRole::Data,
        })
        .expect("register target node");
    cluster
        .reassign_shard(reverse_rusty::cluster::ShardAssignment {
            position: 0,
            primary: NodeId(1),
            replicas: Vec::new(),
        })
        .expect("seed committed map: position 0 → source node");
}

/// The primary proof: a data-moving reassignment under a concurrent writer commits the new owner WITH
/// the move; the committed map names the target, and a fresh coordinator resolving from that map
/// routes to the new owner with zero false negatives (a simulated coordinator restart).
#[test]
fn grpc_reassign_and_move_commits_map_and_restart_routes_zero_fn() {
    let (queries, titles) = build_corpus();
    let mut next_id = queries.iter().map(|(id, _)| *id).max().unwrap_or(0) + 1;
    let by_id: HashMap<u64, String> = queries.iter().map(|(id, d)| (*id, d.clone())).collect();

    let oracle_corpus = build_oracle(&queries, &titles);
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

    // 20 adds of matching DSLs under fresh ids → a deterministic final live set.
    let additions: Vec<(u64, String)> = matched
        .iter()
        .take(20)
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
    assert!(
        oracle_corpus != oracle_final,
        "test setup: the concurrent adds must change some title results"
    );

    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let cfg = ClusterConfig {
        num_shards: 1,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let nodes = spin_two_servers(&rt, &norm, "reassign");

    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        std::slice::from_ref(&nodes.src_ep),
        rt.handle(),
    )
    .expect("connect source cluster");
    cluster.ingest(&queries).expect("ingest corpus");
    seed_committed_map(&cluster, &nodes.src_ep, &nodes.tgt_ep);
    assert_eq!(
        cluster.handoff_generations(),
        vec![0],
        "position 0 starts at generation 0 on the source"
    );

    // Reassign CONCURRENTLY with a writer streaming the additions through the cluster.
    let outcome = std::thread::scope(|s| {
        let cluster_ref = &cluster;
        let adds = &additions;
        let writer = s.spawn(move || {
            for (id, dsl) in adds {
                stream_add(cluster_ref, *id, dsl);
                std::thread::sleep(Duration::from_millis(1));
            }
        });
        let outcome = cluster.reassign_and_move(0, NodeId(2), rt.handle());
        writer.join().expect("writer thread");
        outcome
    })
    .expect("reassign_and_move");

    // The move committed the new owner AND flipped routing.
    assert!(
        matches!(
            outcome,
            ReassignOutcome::Moved {
                position: 0,
                to: NodeId(2),
                generation: 1,
                ..
            }
        ),
        "expected a committed Moved outcome, got {outcome:?}"
    );
    assert_eq!(
        cluster.handoff_generations(),
        vec![1],
        "the reassign bumped position 0's generation"
    );

    // (b) The committed map now names the TARGET.
    let state = cluster.control_state().expect("control state");
    assert_eq!(
        state
            .assignments
            .iter()
            .find(|a| a.position == 0)
            .map(|a| a.primary),
        Some(NodeId(2)),
        "the committed assignment for position 0 now names the target node"
    );
    assert_eq!(
        primary_endpoints(&state),
        vec![nodes.tgt_ep.clone()],
        "resolving the committed map yields the target endpoint"
    );

    // Converge any fence-window write queued for repair (what an operator / a reopen would do).
    for _ in 0..50 {
        if cluster.pending_repairs() == 0 {
            break;
        }
        let _ = cluster.resync();
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(cluster.pending_repairs(), 0, "fence-window writes converge");

    // (a) The live cluster (now serving from the target) matches brute over the final live set.
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate after reassign")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle_final[i],
            "live cluster vs brute(final) on {title:?}"
        );
    }

    // (d) THE RESTART PROOF: a fresh coordinator that resolves the committed map (resolve-only, no
    // re-ingest) lands on the new owner and matches the oracle — zero FN after a simulated restart.
    let resolved = primary_endpoints(&cluster.control_state().expect("state"));
    let coord2 = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &resolved,
        rt.handle(),
    )
    .expect("fresh coordinator over the resolved (committed) endpoints");
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = coord2
            .percolate(title)
            .expect("percolate via restart coordinator")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle_final[i],
            "restart coordinator (routed by the committed map) vs brute(final) on {title:?}"
        );
    }

    let _ = std::fs::remove_dir_all(&nodes.src_dir);
    let _ = std::fs::remove_dir_all(&nodes.tgt_dir);
}

/// Immediately after a direct live flip without commit, both the fenced source and live target
/// retain the complete move-time snapshot. This is the narrow property move-then-commit preserves;
/// after a later write reaches only the target, the stale durable map is not restart-safe.
#[test]
fn grpc_handoff_flip_without_commit_preserves_move_time_snapshot() {
    let (queries, titles) = build_corpus();
    let oracle = build_oracle(&queries, &titles);

    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let cfg = ClusterConfig {
        num_shards: 1,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let nodes = spin_two_servers(&rt, &norm, "crashwin");

    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        std::slice::from_ref(&nodes.src_ep),
        rt.handle(),
    )
    .expect("connect source cluster");
    cluster.ingest(&queries).expect("ingest corpus");
    seed_committed_map(&cluster, &nodes.src_ep, &nodes.tgt_ep);

    // FLIP without committing (the simulated crash in the move-then-commit gap): the lower-level
    // `execute_handoff` moves the data + flips THIS coordinator's routing, but never touches the map.
    cluster
        .execute_handoff(0, &nodes.src_ep, &nodes.tgt_ep, rt.handle())
        .expect("execute handoff (flip without commit)");
    assert_eq!(
        cluster.handoff_generations(),
        vec![1],
        "the flip happened on this coordinator"
    );

    // The committed map was NOT updated — it still names the source (the crash window).
    let state = cluster.control_state().expect("state");
    assert_eq!(
        state
            .assignments
            .iter()
            .find(|a| a.position == 0)
            .map(|a| a.primary),
        Some(NodeId(1)),
        "the committed map still names the source (the move-then-commit gap)"
    );
    assert_eq!(
        primary_endpoints(&state),
        vec![nodes.src_ep.clone()],
        "resolving the committed map still yields the SOURCE endpoint"
    );

    // A coordinator resolving the still-old committed map routes to the SOURCE. At this exact
    // move-time boundary, before any later write, the fenced source still serves the complete
    // snapshot. The next test covers the later-write divergence and safe reconciliation.
    let coord_src = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &primary_endpoints(&state),
        rt.handle(),
    )
    .expect("coordinator routed to the (fenced) source");
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = coord_src
            .percolate(title)
            .expect("percolate via source")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle[i],
            "the fenced source still serves reads (zero FN) on {title:?}"
        );
    }

    // The target also holds the moved data, so the OTHER side of the window is read-safe too.
    let coord_tgt = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        std::slice::from_ref(&nodes.tgt_ep),
        rt.handle(),
    )
    .expect("coordinator routed to the target");
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = coord_tgt
            .percolate(title)
            .expect("percolate via target")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle[i],
            "the target holds the moved data on {title:?}"
        );
    }

    let _ = std::fs::remove_dir_all(&nodes.src_dir);
    let _ = std::fs::remove_dir_all(&nodes.tgt_dir);
}

/// A retry after an uncommitted live flip must treat LIVE routing as the data authority. The old
/// committed owner is fenced and becomes stale as soon as a later write reaches the target; copying
/// from it again could erase or omit that write. Reassign therefore recognizes an already-live
/// target, commits it without another handoff, and a resolve-only restart preserves the late write.
#[test]
fn grpc_reassign_reconciles_an_existing_live_move_without_stale_recopy() {
    let (queries, titles) = build_corpus();
    let next_id = queries.iter().map(|(id, _)| *id).max().unwrap_or(0) + 1;
    let addition = (next_id, queries[0].1.clone());
    let final_live: Vec<(u64, String)> = queries
        .iter()
        .cloned()
        .chain(std::iter::once(addition.clone()))
        .collect();
    let oracle = build_oracle(&final_live, &titles);

    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let cfg = ClusterConfig {
        num_shards: 1,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let nodes = spin_two_servers(&rt, &norm, "reassign_reconcile_live");
    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        std::slice::from_ref(&nodes.src_ep),
        rt.handle(),
    )
    .expect("connect source cluster");
    cluster.ingest(&queries).expect("ingest corpus");
    seed_committed_map(&cluster, &nodes.src_ep, &nodes.tgt_ep);

    cluster
        .execute_handoff(0, &nodes.src_ep, &nodes.tgt_ep, rt.handle())
        .expect("raw live flip");
    cluster
        .add_query(addition.0, &addition.1)
        .expect("post-flip write reaches the live target");

    let outcome = cluster
        .reassign_and_move(0, NodeId(2), rt.handle())
        .expect("commit the already-live target");
    assert!(
        matches!(
            outcome,
            ReassignOutcome::Reconciled {
                position: 0,
                from: NodeId(1),
                to: NodeId(2),
                generation: 1,
            }
        ),
        "the retry must commit without a second physical move: {outcome:?}"
    );
    assert_eq!(
        cluster.handoff_generations(),
        vec![1],
        "commit-only reconciliation does not advance the handoff generation"
    );
    let state = cluster.control_state().expect("committed state");
    assert_eq!(
        state
            .assignments
            .iter()
            .find(|assignment| assignment.position == 0)
            .map(|assignment| assignment.primary),
        Some(NodeId(2)),
        "the durable assignment now names the attested live target"
    );

    let retry = cluster
        .reassign_and_move(0, NodeId(2), rt.handle())
        .expect("exact retry");
    assert!(
        matches!(
            retry,
            ReassignOutcome::NoChange {
                position: 0,
                generation: 1,
            }
        ),
        "the fully converged retry is an honest committed no-op: {retry:?}"
    );

    let resolved = primary_endpoints(&state);
    let restarted = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &resolved,
        rt.handle(),
    )
    .expect("restart over committed target");
    for (index, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = restarted
            .percolate(title)
            .expect("percolate after commit-only reconciliation")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle[index],
            "restart preserved late write on {title:?}"
        );
    }

    let _ = std::fs::remove_dir_all(&nodes.src_dir);
    let _ = std::fs::remove_dir_all(&nodes.tgt_dir);
}

/// A pre-existing committed target does not make live divergence a no-op. Reassignment repairs the
/// live backing, then recognizes that the durable owner already agrees instead of issuing a second
/// assignment proposal or reporting the result uncommitted if that redundant proposal failed.
#[test]
fn grpc_reassign_repairs_live_routing_without_recommitting_existing_target() {
    let (queries, titles) = build_corpus();
    let oracle = build_oracle(&queries, &titles);
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let cfg = ClusterConfig {
        num_shards: 1,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let nodes = spin_two_servers(&rt, &norm, "reassign_repair_live");
    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        std::slice::from_ref(&nodes.src_ep),
        rt.handle(),
    )
    .expect("connect source cluster");
    cluster.ingest(&queries).expect("ingest corpus");
    seed_committed_map(&cluster, &nodes.src_ep, &nodes.tgt_ep);
    cluster
        .reassign_shard(reverse_rusty::cluster::ShardAssignment {
            position: 0,
            primary: NodeId(2),
            replicas: Vec::new(),
        })
        .expect("craft committed-target/live-source divergence");
    let before = cluster.control_state().expect("state before repair").epoch;

    let outcome = cluster
        .reassign_and_move(0, NodeId(2), rt.handle())
        .expect("repair live routing to the already-committed target");
    assert!(
        matches!(
            outcome,
            ReassignOutcome::Moved {
                position: 0,
                from: NodeId(2),
                to: NodeId(2),
                generation: 1,
            }
        ),
        "live routing must move without a redundant durable proposal: {outcome:?}"
    );
    assert_eq!(
        cluster.control_state().expect("state after repair").epoch,
        before,
        "an already-committed target must not receive a redundant proposal"
    );
    for (index, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate after live repair")
            .into_iter()
            .collect();
        assert_eq!(got, oracle[index], "live repair stayed exact on {title:?}");
    }

    let _ = std::fs::remove_dir_all(&nodes.src_dir);
    let _ = std::fs::remove_dir_all(&nodes.tgt_dir);
}

/// A raw A -> B handoff leaves demoted A fenced. Reassigning back to the still-committed A recovers
/// its data from live B, but `RecoverFrom` preserves A's slot fence. The repair must clear and
/// attest that stale fence before swapping routing, or it would acknowledge a primary that rejects
/// every subsequent write.
#[test]
fn grpc_reassign_repair_clears_a_demoted_targets_stale_fence() {
    let (queries, titles) = build_corpus();
    let next_id = queries.iter().map(|(id, _)| *id).max().unwrap_or(0) + 1;
    let live_target_addition = (next_id, queries[0].1.clone());
    let repaired_target_addition = (next_id + 1, queries[1].1.clone());
    let final_live: Vec<(u64, String)> = queries
        .iter()
        .cloned()
        .chain([
            live_target_addition.clone(),
            repaired_target_addition.clone(),
        ])
        .collect();
    let oracle = build_oracle(&final_live, &titles);

    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let cfg = ClusterConfig {
        num_shards: 1,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let nodes = spin_two_servers(&rt, &norm, "reassign_clear_stale_target_fence");
    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        std::slice::from_ref(&nodes.src_ep),
        rt.handle(),
    )
    .expect("connect source cluster");
    cluster.ingest(&queries).expect("ingest corpus");
    seed_committed_map(&cluster, &nodes.src_ep, &nodes.tgt_ep);

    cluster
        .execute_handoff(0, &nodes.src_ep, &nodes.tgt_ep, rt.handle())
        .expect("raw A to B handoff");
    cluster
        .add_query(live_target_addition.0, &live_target_addition.1)
        .expect("write reaches live B after A was demoted and fenced");

    let outcome = cluster
        .reassign_and_move(0, NodeId(1), rt.handle())
        .expect("repair routing back to committed A");
    assert!(
        matches!(
            outcome,
            ReassignOutcome::Moved {
                position: 0,
                from: NodeId(1),
                to: NodeId(1),
                generation: 2,
            }
        ),
        "repair must recover and swap back without a redundant commit: {outcome:?}"
    );
    cluster
        .add_query(repaired_target_addition.0, &repaired_target_addition.1)
        .expect("the repaired target was unfenced before acknowledgement");

    let state = cluster
        .control_state()
        .expect("committed state after repair");
    assert_eq!(
        state
            .assignments
            .iter()
            .find(|assignment| assignment.position == 0)
            .map(|assignment| assignment.primary),
        Some(NodeId(1)),
        "the already-committed target remains durable authority"
    );
    for (index, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate after stale-fence repair")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle[index],
            "stale-fence repair preserved every write on {title:?}"
        );
    }

    let _ = std::fs::remove_dir_all(&nodes.src_dir);
    let _ = std::fs::remove_dir_all(&nodes.tgt_dir);
}

/// Fail-closed: a forced-abort move (`handoff_final_drain_cap = 0` ⇒ the post-fence drain never
/// converges) moves nothing, commits nothing, and auto-unfences the source — the committed map and
/// routing are both unchanged, and the cluster keeps matching.
#[test]
fn grpc_reassign_and_move_aborts_clean_and_does_not_commit() {
    let (queries, titles) = build_corpus();
    let next_id = queries.iter().map(|(id, _)| *id).max().unwrap_or(0) + 1;
    let by_id: HashMap<u64, String> = queries.iter().map(|(id, d)| (*id, d.clone())).collect();

    let oracle_corpus = build_oracle(&queries, &titles);
    let a_match = {
        let mut s: HashSet<u64> = HashSet::new();
        for set in &oracle_corpus {
            s.extend(set);
        }
        *s.iter().min().expect("need ≥1 matching query")
    };
    let addition = (next_id, by_id[&a_match].clone());
    let final_live: Vec<(u64, String)> = queries
        .iter()
        .cloned()
        .chain(std::iter::once(addition.clone()))
        .collect();
    let oracle_final = build_oracle(&final_live, &titles);

    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    // Force the post-fence drain to abort immediately.
    let cfg = ClusterConfig {
        num_shards: 1,
        include_broad: true,
        handoff_final_drain_cap: 0,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let nodes = spin_two_servers(&rt, &norm, "reassign_abort");

    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        std::slice::from_ref(&nodes.src_ep),
        rt.handle(),
    )
    .expect("connect source cluster");
    cluster.ingest(&queries).expect("ingest corpus");
    seed_committed_map(&cluster, &nodes.src_ep, &nodes.tgt_ep);

    // The move fences the source, then the 0-pass post-fence drain forces a fail-closed abort.
    let err = cluster
        .reassign_and_move(0, NodeId(2), rt.handle())
        .expect_err("reassign must abort with final_drain_cap = 0");
    assert!(
        matches!(err, ShardError::Remote(_)),
        "the abort surfaces as a remote error, got {err:?}"
    );
    // No flip, and — the crux for the new commit step — NO commit (the map still names the source).
    assert_eq!(
        cluster.handoff_generations(),
        vec![0],
        "an aborted reassign must NOT flip routing"
    );
    assert_eq!(
        cluster
            .control_state()
            .expect("state")
            .assignments
            .iter()
            .find(|a| a.position == 0)
            .map(|a| a.primary),
        Some(NodeId(1)),
        "an aborted move commits nothing — the map still names the source"
    );

    // The source AUTO-UNFENCED (ADR-048): a write lands again.
    cluster
        .add_query(addition.0, &addition.1)
        .expect("source must accept writes after the aborted reassign unfenced it");

    // And the cluster still matches the brute oracle over the final live set (zero FN).
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate after aborted reassign")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle_final[i],
            "post-abort cluster vs brute(final) on {title:?}"
        );
    }

    let _ = std::fs::remove_dir_all(&nodes.src_dir);
    let _ = std::fs::remove_dir_all(&nodes.tgt_dir);
}

/// Replicated-position reject (ADR-090, narrowed by ADR-094): a SINGLE-target `reassign_and_move`
/// of a position whose COMMITTED assignment carries replicas would de-replicate it — the move swaps
/// the position to one `RemoteShard` while the committed map still advertises the replicas. The
/// guard is per-position (the committed entry, not the cluster's replication factor) and points at
/// the group-aware `reassign_group_and_move`. Uses an in-process cluster with a seeded replicated
/// assignment (no servers needed — the guard fires before any move).
#[test]
fn grpc_reassign_and_move_rejects_replicated_position() {
    let queries = vec![(1u64, "1994 north star rareentity0".to_string())];
    let cfg = ClusterConfig {
        num_shards: 1,
        replication_factor: 2,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster =
        ClusterEngine::build(vocab(), &cfg, &queries).expect("build RF=2 in-process cluster");
    // Commit a REPLICATED assignment for position 0 — the per-position condition the guard reads.
    cluster
        .reassign_shard(reverse_rusty::cluster::ShardAssignment {
            position: 0,
            primary: NodeId(1),
            replicas: vec![NodeId(2)],
        })
        .expect("seed a replicated committed assignment");
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let err = cluster
        .reassign_and_move(0, NodeId(2), rt.handle())
        .expect_err("a single-target move of a replicated position must be rejected");
    match &err {
        ShardError::Config(msg) => assert!(
            msg.contains("committed replica") && msg.contains("reassign_group_and_move"),
            "the reject names the per-position reason and the group-aware alternative: {msg}"
        ),
        other => panic!("replicated-position reject surfaces as a config error, got {other:?}"),
    }
}
