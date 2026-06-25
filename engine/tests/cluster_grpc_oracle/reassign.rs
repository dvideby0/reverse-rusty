//! Data-moving live reassignment over gRPC (ADR-090): a committed shard→node assignment change that
//! MOVES the data and re-points routing — live under concurrent writes, and across a coordinator
//! restart. Where `handoff.rs` proves a bare `execute_handoff` is zero-FN, these prove the NEW
//! property: `reassign_and_move` commits the new owner WITH the move, so the committed map names the
//! target and a resolve-only restart routes there (the ADR-086 boot guard previously forbade this).
//!
//! Three proofs:
//!  - `grpc_reassign_and_move_commits_map_and_restart_routes_zero_fn` — the primary proof: move under
//!    a concurrent writer, the committed map now names the target, and a FRESH coordinator resolving
//!    from that map lands on the new owner with zero false negatives.
//!  - `grpc_handoff_flip_without_commit_still_serves_from_source_zero_fn` — the crash-window proof:
//!    flip WITHOUT committing (simulating a crash in the move-then-commit gap); a coordinator
//!    resolving the still-old map routes to the fenced source, which still SERVES READS — zero FN.
//!  - `grpc_reassign_and_move_aborts_clean_and_does_not_commit` — fail-closed: a forced abort moves
//!    nothing, commits nothing, and auto-unfences the source.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use reverse_rusty::cluster::{
    ClusterConfig, ClusterEngine, ClusterState, NodeDescriptor, NodeId, NodeRole, ReassignOutcome,
    ShardError, ShardServer,
};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::normalize::Normalizer;
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

/// A source + target durable shard server (A, B) over the SAME frozen dict/norm, so either can serve
/// position 0. Returns their endpoints + data dirs (for teardown).
struct TwoNode {
    src_ep: String,
    tgt_ep: String,
    src_dir: PathBuf,
    tgt_dir: PathBuf,
}

fn spin_two_servers(rt: &tokio::runtime::Runtime, norm: &Arc<Normalizer>, tag: &str) -> TwoNode {
    let src_dir = server_dir(&format!("{tag}_src"));
    let tgt_dir = server_dir(&format!("{tag}_tgt"));
    let (src_addr, tgt_addr) = {
        let _enter = rt.enter();
        let si = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind src");
        let sa = si.local_addr().expect("src addr");
        rt.spawn(
            ShardServer::pending_durable(
                Arc::clone(norm),
                EngineConfig::default(),
                src_dir.clone(),
            )
            .serve_with_incoming(si),
        );
        let ti = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind tgt");
        let ta = ti.local_addr().expect("tgt addr");
        rt.spawn(
            ShardServer::pending_durable(
                Arc::clone(norm),
                EngineConfig::default(),
                tgt_dir.clone(),
            )
            .serve_with_incoming(ti),
        );
        (sa, ta)
    };
    wait_until_listening(src_addr);
    wait_until_listening(tgt_addr);
    TwoNode {
        src_ep: format!("http://{src_addr}"),
        tgt_ep: format!("http://{tgt_addr}"),
        src_dir,
        tgt_dir,
    }
}

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

/// Resolve each position's primary endpoint from the committed document — what a resolve-only
/// coordinator restart (`--route-by-assignments`, no `--shard-endpoint`) does on boot.
fn primary_endpoints(state: &ClusterState) -> Vec<String> {
    (0..state.num_shards)
        .map(|pos| {
            let a = state
                .assignments
                .iter()
                .find(|a| a.position == pos)
                .expect("an assignment for every position");
            let node = state
                .nodes
                .iter()
                .find(|n| n.id == a.primary)
                .expect("a node for the primary");
            node.addr
                .clone()
                .expect("the primary node has a registered addr")
        })
        .collect()
}

/// The writer's per-add loop: an add routes to position 0's CURRENT backing (source pre-flip, target
/// post-flip) and is briefly REJECTED in the fence→flip window (durably logged + queued for repair).
/// Identical to `handoff.rs`'s firehose so the concurrency surface matches.
fn stream_add(cluster: &ClusterEngine, id: u64, dsl: &str) {
    loop {
        match cluster.add_query(id, dsl) {
            Ok(_) | Err(ShardError::PartiallyApplied { .. }) => break,
            Err(ShardError::Remote(_)) => std::thread::sleep(Duration::from_millis(2)),
            Err(e) => panic!("unexpected writer error: {e}"),
        }
    }
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

/// The crash-window proof: simulate a crash in the move-then-commit gap by FLIPPING (a direct
/// `execute_handoff`) WITHOUT committing. The committed map still names the source; a coordinator
/// resolving it routes to the source, which is fenced (writes only) but STILL SERVES READS and holds
/// the data — zero false negatives. And the target also holds the moved data, so neither side of the
/// window is a false negative.
#[test]
fn grpc_handoff_flip_without_commit_still_serves_from_source_zero_fn() {
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

    // A coordinator resolving the still-old committed map routes to the SOURCE. The source is fenced
    // (writes rejected) but still serves reads and holds the data → zero false negatives.
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
