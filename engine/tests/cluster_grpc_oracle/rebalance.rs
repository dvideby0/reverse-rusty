//! HRW `rebalance_and_move` collision-safety across MULTI-SHARD nodes (ADR-093 Stage 3): the exact
//! scenario a code review flagged on the parked reconciler. The HRW-desired map PACKS several
//! positions onto shared destination nodes; pre-Stage-1 a one-shard `RecoverFrom` clobbered the
//! earlier move (a shard-sized false negative). Now each move targets a distinct per-shard slot /
//! fence / `shard_<id>/`, so the full SEQUENTIAL rebalance converges with no slot lost and zero false
//! negatives — and a second pass is a fixpoint (the map is HRW-optimal).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, NodeId};

use crate::harness::*;
use crate::relocation::{owner, primary_endpoints, seed_map, spin_n_servers};

#[test]
fn grpc_rebalance_and_move_colocated_packing_converges_zero_fn() {
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

    // Three nodes; start with ALL six positions PACKED on node A (index 0) — a deliberately non-HRW
    // committed map. `rebalance_and_move` will then want to spread them, forcing multi-position moves
    // that co-locate on the destinations.
    let nodes = spin_n_servers(&rt, &norm, "rebal", 3);
    let endpoints = vec![nodes[0].ep.clone(); k]; // every position co-located on node A
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

    // The full HRW rebalance: spread the 6 positions across the 3 nodes (~2 each), SEQUENTIALLY.
    let report = cluster
        .rebalance_and_move(1, rt.handle())
        .expect("rebalance_and_move");
    assert!(
        report.failed.is_none(),
        "no per-position move failed: {report:?}"
    );
    assert!(
        report.moved.len() >= 2,
        "HRW must move several positions off the packed node (co-located packing on destinations): \
         {report:?}"
    );

    // Drain any fence-window repair (no concurrent writer here, but be defensive).
    for _ in 0..50 {
        if cluster.pending_repairs() == 0 {
            break;
        }
        let _ = cluster.resync();
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(cluster.pending_repairs(), 0, "fence-window writes converge");

    // Convergence: NO slot lost + zero false negatives across the whole rebalanced cluster.
    let counts1 = cluster
        .shard_query_counts()
        .expect("per-shard counts after rebalance");
    assert_eq!(counts1.len(), k);
    assert!(
        counts1.iter().all(|&c| c > 0),
        "no slot lost after the rebalance (a clobber would empty one): {counts1:?}"
    );
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate after rebalance")
            .into_iter()
            .collect();
        assert_eq!(got, oracle[i], "rebalanced cluster vs brute on {title:?}");
    }

    // Packing proof: the committed map now spreads positions across multiple nodes, and at least one
    // DESTINATION node (not the originally-packed A = NodeId 1) owns ≥2 co-located positions — i.e. a
    // move landed a second shard on a node that already received one, the exact former clobber case.
    let state = cluster.control_state().expect("control state");
    let mut per_node: HashMap<NodeId, usize> = HashMap::new();
    for pos in 0..k {
        if let Some(n) = owner(&state, pos) {
            *per_node.entry(n).or_default() += 1;
        }
    }
    assert!(
        per_node.len() >= 2,
        "positions must spread across multiple nodes after rebalance: {per_node:?}"
    );
    assert!(
        per_node
            .iter()
            .any(|(&n, &c)| n != NodeId(1) && c >= 2),
        "a destination node (≠ the packed origin) must own ≥2 co-located positions — the move-packing \
         the fix makes safe: {per_node:?}"
    );

    // Fixpoint: the map is now HRW-optimal, so a second rebalance moves NOTHING.
    let report2 = cluster
        .rebalance_and_move(1, rt.handle())
        .expect("rebalance fixpoint");
    assert!(
        report2.moved.is_empty() && report2.failed.is_none(),
        "a second rebalance over an HRW-optimal map must be a no-op: {report2:?}"
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
