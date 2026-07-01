//! Per-shard RELOCATION under co-location (ADR-093 Stage 3): a data-moving `reassign_and_move` moves
//! ONE co-located slot off a multi-shard node, leaving the node's OTHER slots untouched, with zero
//! false negatives. This is the load-bearing proof that HRW rebalance is collision-safe once a node
//! hosts many shards — the collision codex flagged (a one-shard `RecoverFrom` clobbering a sibling)
//! is structurally gone because every move targets a distinct per-shard slot / fence / `shard_<id>/`.
//!
//! Three proofs:
//!  - `grpc_relocate_colocated_shard_leaves_sibling_intact_zero_fn` — the primary proof: node A hosts
//!    co-located {0,1}; relocate position 0 onto a fresh node D; position 1's slot is byte-for-byte
//!    untouched (exact count), the committed map re-points only position 0, the whole cluster still ≡
//!    brute, and a resolve-only coordinator restart routes zero-FN.
//!  - `grpc_relocate_colocated_shard_under_concurrent_writer_zero_fn` — the same move under a firehose
//!    writer; the final live set ≡ brute (the concurrency surface reassign.rs exercises, now co-located).
//!  - `grpc_durable_colocated_node_restart_reattaches_all_slots` — an `open_durable` restart of a
//!    co-located node re-attaches BOTH `shard_<id>/` slots and serves them (ADR-093 `restore_durable_slots`).

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use reverse_rusty::cluster::{
    ClusterConfig, ClusterEngine, ClusterState, NodeDescriptor, NodeId, NodeRole, ReassignOutcome,
    ShardAssignment, ShardError, ShardServer,
};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::normalize::Normalizer;
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

/// A durable shard server + its data dir (for teardown / durable reopen). Shared with `rebalance.rs`.
pub(crate) struct Node {
    pub(crate) ep: String,
    pub(crate) dir: PathBuf,
}

/// Spin `n` durable *pending* servers over the shared norm; return their endpoints + data dirs. Each
/// can host MANY co-located slots (ADR-093) once the coordinator adopts/adds shards on it.
pub(crate) fn spin_n_servers(
    rt: &tokio::runtime::Runtime,
    norm: &Arc<Normalizer>,
    tag: &str,
    n: usize,
) -> Vec<Node> {
    let mut dirs = Vec::with_capacity(n);
    let mut addrs = Vec::with_capacity(n);
    {
        let _enter = rt.enter();
        for i in 0..n {
            let dir = server_dir(&format!("{tag}_{i}"));
            let inc =
                TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind ephemeral port");
            let addr = inc.local_addr().expect("local_addr");
            rt.spawn(
                ShardServer::pending_durable(
                    Arc::clone(norm),
                    EngineConfig::default(),
                    dir.clone(),
                )
                .serve_with_incoming(inc),
            );
            dirs.push(dir);
            addrs.push(addr);
        }
    }
    for &a in &addrs {
        wait_until_listening(a);
    }
    addrs
        .into_iter()
        .zip(dirs)
        .map(|(a, dir)| Node {
            ep: format!("http://{a}"),
            dir,
        })
        .collect()
}

/// Register every node (id = index + 1) and commit a position-preserving map: `plan[pos]` is the index
/// (into `nodes`) of the node that physically holds position `pos` after ingest. This is the committed
/// document `reassign_and_move` reads `from`/`to` endpoints from — the co-located generalization of
/// reassign.rs's `seed_committed_map`.
pub(crate) fn seed_map(cluster: &ClusterEngine, nodes: &[Node], plan: &[usize]) {
    for (i, node) in nodes.iter().enumerate() {
        cluster
            .register_node(NodeDescriptor {
                id: NodeId((i + 1) as u64),
                addr: Some(node.ep.clone()),
                role: NodeRole::Data,
            })
            .expect("register node");
    }
    for (pos, &ni) in plan.iter().enumerate() {
        cluster
            .reassign_shard(ShardAssignment {
                position: pos as u32,
                primary: NodeId((ni + 1) as u64),
                replicas: Vec::new(),
            })
            .expect("seed committed assignment");
    }
}

/// Resolve each position's primary endpoint from the committed document — what a resolve-only
/// coordinator restart (`--route-by-assignments`) does on boot. Copy of reassign.rs's helper.
pub(crate) fn primary_endpoints(state: &ClusterState) -> Vec<String> {
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

/// Which committed node owns `position`, as a `NodeId`.
pub(crate) fn owner(state: &ClusterState, position: usize) -> Option<NodeId> {
    state
        .assignments
        .iter()
        .find(|a| a.position as usize == position)
        .map(|a| a.primary)
}

/// A firehose add that routes to the position's CURRENT backing and is briefly rejected in the
/// fence→flip window (queued for repair). Copy of reassign.rs's writer.
fn stream_add(cluster: &ClusterEngine, id: u64, dsl: &str) {
    loop {
        match cluster.add_query(id, dsl) {
            Ok(_) | Err(ShardError::PartiallyApplied { .. }) => break,
            Err(ShardError::Remote(_)) => std::thread::sleep(Duration::from_millis(2)),
            Err(e) => panic!("unexpected writer error: {e}"),
        }
    }
}

fn teardown(nodes: &[Node]) {
    for n in nodes {
        let _ = std::fs::remove_dir_all(&n.dir);
    }
}

/// THE Stage-3 proof: a co-located sibling is untouched when its neighbour relocates, zero FN.
#[test]
fn grpc_relocate_colocated_shard_leaves_sibling_intact_zero_fn() {
    let (queries, titles) = build_corpus();
    let oracle = build_oracle(&queries, &titles);

    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let k = 4usize;
    let cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    // Nodes: A=0 (hosts co-located {0,1}), B=1 ({2}), C=2 ({3}), D=3 (fresh relocation target).
    let nodes = spin_n_servers(&rt, &norm, "relocate", 4);
    let endpoints = vec![
        nodes[0].ep.clone(), // position 0 → A
        nodes[0].ep.clone(), // position 1 → A (co-located)
        nodes[1].ep.clone(), // position 2 → B
        nodes[2].ep.clone(), // position 3 → C
    ];

    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &endpoints,
        rt.handle(),
    )
    .expect("connect co-located cluster");
    cluster.ingest(&queries).expect("ingest corpus over gRPC");
    seed_map(&cluster, &nodes, &[0, 0, 1, 2]);

    // Baseline: every position independently populated; capture the sibling (position 1) count.
    let counts0 = cluster.shard_query_counts().expect("per-shard counts");
    assert_eq!(counts0.len(), k, "one count per position: {counts0:?}");
    assert!(
        counts0.iter().all(|&c| c > 0),
        "every co-located slot must hold queries before the move: {counts0:?}"
    );
    let sibling_before = counts0[1];

    // Relocate position 0 off A onto the fresh node D (NodeId 4). Position 1 stays on A.
    let outcome = cluster
        .reassign_and_move(0, NodeId(4), rt.handle())
        .expect("reassign_and_move");
    assert!(
        matches!(
            outcome,
            ReassignOutcome::Moved {
                position: 0,
                to: NodeId(4),
                ..
            }
        ),
        "expected a committed Moved outcome, got {outcome:?}"
    );

    // The committed map re-points ONLY position 0; position 1 still names A (NodeId 1).
    let state = cluster.control_state().expect("control state");
    assert_eq!(owner(&state, 0), Some(NodeId(4)), "position 0 now on D");
    assert_eq!(
        owner(&state, 1),
        Some(NodeId(1)),
        "the co-located sibling (position 1) still names A — the move never touched it"
    );
    assert_eq!(owner(&state, 2), Some(NodeId(2)));
    assert_eq!(owner(&state, 3), Some(NodeId(3)));

    // Drain any fence-window write queued for repair (what an operator / reopen does).
    for _ in 0..50 {
        if cluster.pending_repairs() == 0 {
            break;
        }
        let _ = cluster.resync();
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(cluster.pending_repairs(), 0, "fence-window writes converge");

    // The sibling slot is byte-for-byte intact (exact count) and NO slot was lost.
    let counts1 = cluster
        .shard_query_counts()
        .expect("per-shard counts after move");
    assert_eq!(
        counts1[1], sibling_before,
        "the co-located sibling (position 1) is untouched by position 0's relocation \
         (a clobbering RecoverFrom would change this): {counts1:?} vs baseline {counts0:?}"
    );
    assert!(
        counts1.iter().all(|&c| c > 0),
        "no slot lost after relocation: {counts1:?}"
    );

    // Zero false negatives: the whole cluster still equals the independent brute oracle.
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate after relocation")
            .into_iter()
            .collect();
        assert_eq!(got, oracle[i], "co-located cluster vs brute on {title:?}");
    }

    // Resolve-only restart: a fresh coordinator routed purely by the committed map lands on the new
    // owner for position 0 and the untouched sibling for position 1 — zero FN.
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

    teardown(&nodes);
}

/// The concurrency surface: the same co-located relocation under a firehose writer converges to the
/// brute oracle over the FINAL live set (zero FN across the fence→flip window + repair).
#[test]
fn grpc_relocate_colocated_shard_under_concurrent_writer_zero_fn() {
    let (queries, titles) = build_corpus();
    let mut next_id = queries.iter().map(|(id, _)| *id).max().unwrap_or(0) + 1;
    let by_id: HashMap<u64, String> = queries.iter().map(|(id, d)| (*id, d.clone())).collect();

    // 20 adds of matching DSLs under fresh ids → a deterministic final live set.
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
    let k = 4usize;
    let cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let nodes = spin_n_servers(&rt, &norm, "relocate_cc", 4);
    let endpoints = vec![
        nodes[0].ep.clone(),
        nodes[0].ep.clone(),
        nodes[1].ep.clone(),
        nodes[2].ep.clone(),
    ];
    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &endpoints,
        rt.handle(),
    )
    .expect("connect co-located cluster");
    cluster.ingest(&queries).expect("ingest corpus");
    seed_map(&cluster, &nodes, &[0, 0, 1, 2]);

    // Relocate position 0 CONCURRENTLY with a writer streaming the additions through the cluster.
    let outcome = std::thread::scope(|s| {
        let cluster_ref = &cluster;
        let adds = &additions;
        let writer = s.spawn(move || {
            for (id, dsl) in adds {
                stream_add(cluster_ref, *id, dsl);
                std::thread::sleep(Duration::from_millis(1));
            }
        });
        let outcome = cluster.reassign_and_move(0, NodeId(4), rt.handle());
        writer.join().expect("writer thread");
        outcome
    })
    .expect("reassign_and_move under writer");
    assert!(
        matches!(
            outcome,
            ReassignOutcome::Moved {
                position: 0,
                to: NodeId(4),
                ..
            }
        ),
        "expected a committed Moved outcome, got {outcome:?}"
    );

    // The sibling still names A; the move re-pointed only position 0.
    let state = cluster.control_state().expect("control state");
    assert_eq!(owner(&state, 0), Some(NodeId(4)));
    assert_eq!(owner(&state, 1), Some(NodeId(1)), "sibling untouched");

    for _ in 0..50 {
        if cluster.pending_repairs() == 0 {
            break;
        }
        let _ = cluster.resync();
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(cluster.pending_repairs(), 0, "fence-window writes converge");

    // No slot lost, and the whole cluster equals brute over the FINAL live set (adds included).
    let counts = cluster.shard_query_counts().expect("counts");
    assert!(
        counts.iter().all(|&c| c > 0),
        "no slot lost after concurrent relocation: {counts:?}"
    );
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate after relocation")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle_final[i],
            "live cluster vs brute(final) on {title:?}"
        );
    }

    // Resolve-only restart still routes the final set with zero FN.
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
        assert_eq!(
            got, oracle_final[i],
            "restart coordinator vs brute(final) on {title:?}"
        );
    }

    teardown(&nodes);
}

/// A co-located durable node, restarted via `open_durable`, re-attaches BOTH `shard_<id>/` slots and
/// serves them (ADR-093 `restore_durable_slots`) — the multi-shard analogue of a single-shard reopen.
#[test]
fn grpc_durable_colocated_node_restart_reattaches_all_slots() {
    let (queries, titles) = build_corpus();
    let oracle = build_oracle(&queries, &titles);

    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let k = 2usize;
    let cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    // ONE node hosts BOTH positions (co-located {0,1}); its data dir is what we reopen.
    let node = &spin_n_servers(&rt, &norm, "reopen", 1)[0];
    let dir = node.dir.clone();
    let endpoints = vec![node.ep.clone(), node.ep.clone()];

    {
        let cluster = ClusterEngine::connect_remote(
            Arc::clone(&norm),
            Arc::clone(&dict),
            empty_tag_dict(),
            &cfg,
            &endpoints,
            rt.handle(),
        )
        .expect("connect co-located single-node cluster");
        cluster.ingest(&queries).expect("ingest corpus");
        let counts = cluster.shard_query_counts().expect("counts");
        assert_eq!(counts.len(), k);
        assert!(
            counts.iter().all(|&c| c > 0),
            "both slots populated: {counts:?}"
        );
        // Persist both slots to disk so an open_durable restart restores from segments (each slot
        // seals its memtable into a base segment under its `shard_<id>/`).
        cluster
            .flush()
            .expect("flush both co-located slots to segments");
    }

    // Simulate a container restart: a NEW ShardServer opened durably on the SAME dir must restore both
    // `shard_000/` and `shard_001/` slots. Bind a fresh port and re-point the cluster at it.
    let reopened_addr = {
        let _enter = rt.enter();
        let inc = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind reopen port");
        let addr = inc.local_addr().expect("local_addr");
        let srv =
            ShardServer::open_durable(Arc::clone(&norm), EngineConfig::default(), dir.clone())
                .expect("open_durable restores both co-located slots");
        rt.spawn(srv.serve_with_incoming(inc));
        addr
    };
    wait_until_listening(reopened_addr);

    // The reopened node is already dict-adopted (persisted), so route to it with a plain `connect`
    // cluster (no re-adopt) over both co-located positions.
    let reopened_ep = format!("http://{reopened_addr}");
    let cluster2 = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &[reopened_ep.clone(), reopened_ep],
        rt.handle(),
    )
    .expect("connect to the reopened durable node");
    let counts2 = cluster2
        .shard_query_counts()
        .expect("counts after durable reopen");
    assert!(
        counts2.iter().all(|&c| c > 0),
        "both co-located slots re-attached after open_durable: {counts2:?}"
    );
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster2
            .percolate(title)
            .expect("percolate after durable reopen")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle[i],
            "reopened co-located node vs brute on {title:?}"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}
