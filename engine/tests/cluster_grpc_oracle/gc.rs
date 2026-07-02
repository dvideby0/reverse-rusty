//! Orphan-slot GC over gRPC (ADR-096): `gc_orphan_slots` reclaims the fenced, unrouted slots a
//! data-moving reassignment strands on the old owner — slot map + `shard_<id>/` disk — with zero
//! false negatives, while every keep-set member survives: committed owners, the
//! flip-without-commit state (committed map still names the source, live routing the target),
//! and unassigned positions (fail-safe).
//!
//! Four proofs:
//!  - `grpc_gc_reclaims_orphan_slot_after_relocation_sibling_intact_zero_fn` — the primary: after
//!    a co-located relocation the sweep drops exactly the moved-away slot (dir gone), the sibling
//!    slot is byte-identical, the cluster stays ≡ brute, a second sweep is the idempotent empty
//!    report, and a durable restart of the swept node re-attaches ONLY the survivor.
//!  - `grpc_gc_keeps_flip_without_commit_source_and_target_zero_fn` — the keep-set kill shot: the
//!    raw-handoff flip (routing on the target, committed map on the source) loses NOTHING to the
//!    sweep — the source is committed-kept, the target live-routing-kept; committing then makes
//!    the source a true orphan a second sweep reclaims. Zero-FN throughout + restart coordinator.
//!  - `grpc_gc_reclaims_unfenced_orphan_after_source_restart_zero_fn` — fences are not durable:
//!    a restarted orphan comes back UNFENCED, and the sweep arms it (the `fence(0)` probe) before
//!    the guarded drop.
//!  - `grpc_gc_keeps_slot_committed_elsewhere_but_live_routed` — the second live-routed keep
//!    shape: a slot committed to a DIFFERENT node but actively routed here survives (a map-only
//!    classification would destroy the only routed copy). The truly-unassigned fail-safe branch
//!    is unit-proven in `coordinator/gc.rs` (it cannot be constructed via the public APIs).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use reverse_rusty::cluster::{
    ClusterConfig, ClusterEngine, NodeDescriptor, NodeId, NodeRole, ReassignOutcome, RemoteShard,
    ShardAssignment, ShardServer,
};
use reverse_rusty::config::EngineConfig;
use tonic::transport::server::TcpIncoming;

use crate::harness::*;
use crate::relocation::{owner, primary_endpoints, seed_map, spin_n_servers, Node};

fn teardown(nodes: &[Node]) {
    for n in nodes {
        let _ = std::fs::remove_dir_all(&n.dir);
    }
}

/// Drain any fence-window writes queued for partial-apply repair.
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

/// Assert the live cluster matches the brute oracle on every title.
fn assert_zero_fn(cluster: &ClusterEngine, titles: &[String], oracle: &[HashSet<u64>], ctx: &str) {
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate")
            .into_iter()
            .collect();
        assert_eq!(got, oracle[i], "{ctx}: cluster vs brute on {title:?}");
    }
}

#[test]
fn grpc_gc_reclaims_orphan_slot_after_relocation_sibling_intact_zero_fn() {
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
    // A=0 hosts co-located {0,1}; B=1 hosts {2}; C=2 hosts {3}; D=3 is the relocation target.
    let nodes = spin_n_servers(&rt, &norm, "gc_reclaim", 4);
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
    cluster.ingest(&queries).expect("ingest corpus over gRPC");
    seed_map(&cluster, &nodes, &[0, 0, 1, 2]);
    let counts0 = cluster.shard_query_counts().expect("counts");
    assert!(counts0.iter().all(|&c| c > 0), "all populated: {counts0:?}");
    let sibling_before = counts0[1];
    // Persist A's slots so the orphan has a `shard_000/` dir to reclaim.
    cluster.flush().expect("flush to segments");

    // Relocate position 0 off A onto D; A keeps the co-located sibling (position 1) and now
    // strands the moved-away slot 0 (fenced, on disk) — the orphan.
    let outcome = cluster
        .reassign_and_move(0, NodeId(4), rt.handle())
        .expect("reassign_and_move");
    assert!(
        matches!(outcome, ReassignOutcome::Moved { .. }),
        "{outcome:?}"
    );
    converge_repairs(&cluster);
    assert!(
        nodes[0].dir.join("shard_000").exists(),
        "the stranded orphan dir exists before the sweep"
    );

    // THE SWEEP: exactly A's slot 0 is dropped; the sibling and every committed owner survive.
    let report = cluster.gc_orphan_slots(rt.handle()).expect("gc sweep");
    assert_eq!(report.dropped.len(), 1, "exactly the orphan: {report:?}");
    assert_eq!(report.dropped[0].node, NodeId(1), "on node A");
    assert_eq!(report.dropped[0].shard_id, 0, "the moved-away slot");
    assert!(
        report.failed.is_empty()
            && report.kept_live_routed.is_empty()
            && report.skipped_unassigned.is_empty()
            && report.skipped_nodes.is_empty(),
        "a clean sweep: {report:?}"
    );
    assert!(
        !nodes[0].dir.join("shard_000").exists(),
        "the orphan's dir was reclaimed"
    );
    assert!(
        nodes[0].dir.join("shard_001").exists(),
        "the co-located sibling's dir is untouched"
    );

    // The sibling slot is byte-identical; the whole cluster still ≡ brute.
    let counts1 = cluster.shard_query_counts().expect("counts after gc");
    assert_eq!(counts1[1], sibling_before, "sibling count byte-identical");
    assert!(counts1.iter().all(|&c| c > 0), "no slot lost: {counts1:?}");
    assert_zero_fn(&cluster, &titles, &oracle, "after gc");

    // Idempotence: a second sweep finds nothing.
    let report2 = cluster.gc_orphan_slots(rt.handle()).expect("second sweep");
    assert!(
        report2.dropped.is_empty() && report2.is_clean(),
        "the second sweep is the idempotent no-op: {report2:?}"
    );

    // A durable restart of the swept node re-attaches ONLY the survivor (nothing resurrects).
    let reopened_addr = {
        let _enter = rt.enter();
        let inc = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind reopen port");
        let addr = inc.local_addr().expect("local_addr");
        let srv = ShardServer::open_durable(
            Arc::clone(&norm),
            EngineConfig::default(),
            nodes[0].dir.clone(),
        )
        .expect("open_durable over the swept dir");
        rt.spawn(srv.serve_with_incoming(inc));
        addr
    };
    wait_until_listening(reopened_addr);
    let reopened_ep = format!("http://{reopened_addr}");
    let tag_fp = empty_tag_dict().fingerprint();
    let reopened = RemoteShard::connect(
        &reopened_ep,
        rt.handle().clone(),
        dict.fingerprint(),
        tag_fp,
        1,
    )
    .expect("connect the reopened node");
    let listing = reopened.list_shards().expect("list the reopened node");
    assert_eq!(
        listing.shards.len(),
        1,
        "ONLY the survivor re-attached (the dropped slot did not resurrect): {:?}",
        listing.shards
    );
    assert_eq!(listing.shards[0].shard_id, 1, "the co-located sibling");
    assert_eq!(
        listing.shards[0].num_queries, sibling_before as u64,
        "the sibling slot re-attached byte-identical"
    );

    teardown(&nodes);
}

#[test]
fn grpc_gc_keeps_flip_without_commit_source_and_target_zero_fn() {
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
    let nodes = spin_n_servers(&rt, &norm, "gc_flip", 2);
    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        std::slice::from_ref(&nodes[0].ep),
        rt.handle(),
    )
    .expect("connect over A");
    cluster.ingest(&queries).expect("ingest corpus");
    seed_map(&cluster, &nodes, &[0]); // committed: position 0 → A (id 1)
    cluster.flush().expect("flush");

    // The RAW handoff flip (no commit): live routing now reaches B, the committed map still
    // names A — the exact crash-window state reassign.rs proves serves zero-FN.
    cluster
        .execute_handoff(0, &nodes[0].ep, &nodes[1].ep, rt.handle())
        .expect("raw execute_handoff");
    converge_repairs(&cluster);

    // THE KEEP-SET PROOF: the sweep drops NOTHING — A is committed-kept, B is live-routing-kept
    // (the committed map alone would have called B's slot an orphan and destroyed the live path).
    let report = cluster.gc_orphan_slots(rt.handle()).expect("gc sweep");
    assert!(report.dropped.is_empty(), "nothing dropped: {report:?}");
    assert_eq!(
        report.kept_live_routed.len(),
        1,
        "B's slot is kept BY LIVE ROUTING: {report:?}"
    );
    assert_eq!(report.kept_live_routed[0].node, NodeId(2));
    assert!(report.is_clean(), "{report:?}");
    assert!(nodes[0].dir.join("shard_000").exists(), "A's dir intact");
    assert!(nodes[1].dir.join("shard_000").exists(), "B's dir intact");
    assert_zero_fn(&cluster, &titles, &oracle, "flip-without-commit + gc");

    // COMMIT the new owner: A's slot becomes the true orphan; the second sweep reclaims it.
    cluster
        .reassign_shard(ShardAssignment {
            position: 0,
            primary: NodeId(2),
            replicas: Vec::new(),
        })
        .expect("commit the new owner");
    let report2 = cluster.gc_orphan_slots(rt.handle()).expect("second sweep");
    assert_eq!(report2.dropped.len(), 1, "now A is the orphan: {report2:?}");
    assert_eq!(report2.dropped[0].node, NodeId(1));
    assert!(
        !nodes[0].dir.join("shard_000").exists(),
        "A's dir reclaimed"
    );
    assert_zero_fn(&cluster, &titles, &oracle, "after commit + gc");

    // A resolve-only coordinator restart routes by the committed map — zero-FN.
    let state = cluster.control_state().expect("state");
    assert_eq!(owner(&state, 0), Some(NodeId(2)));
    let coord2 = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &primary_endpoints(&state),
        rt.handle(),
    )
    .expect("fresh coordinator over the committed map");
    assert_zero_fn(&coord2, &titles, &oracle, "restart coordinator");

    teardown(&nodes);
}

#[test]
fn grpc_gc_reclaims_unfenced_orphan_after_source_restart_zero_fn() {
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
    let nodes = spin_n_servers(&rt, &norm, "gc_unfenced", 2);
    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        std::slice::from_ref(&nodes[0].ep),
        rt.handle(),
    )
    .expect("connect over A");
    cluster.ingest(&queries).expect("ingest corpus");
    seed_map(&cluster, &nodes, &[0]);
    cluster.flush().expect("flush");

    // Move position 0 off A (fences A's slot) and commit.
    let outcome = cluster
        .reassign_and_move(0, NodeId(2), rt.handle())
        .expect("reassign_and_move");
    assert!(
        matches!(outcome, ReassignOutcome::Moved { .. }),
        "{outcome:?}"
    );
    converge_repairs(&cluster);

    // RESTART A over its dir: the orphan re-attaches UNFENCED (fences are not durable) — the
    // hazard class the fence-arm probe exists for. Re-register A's membership at the new port so
    // the sweep contacts the restarted process.
    let reopened_addr = {
        let _enter = rt.enter();
        let inc = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind reopen port");
        let addr = inc.local_addr().expect("local_addr");
        let srv = ShardServer::open_durable(
            Arc::clone(&norm),
            EngineConfig::default(),
            nodes[0].dir.clone(),
        )
        .expect("open_durable re-attaches the orphan (unfenced)");
        rt.spawn(srv.serve_with_incoming(inc));
        addr
    };
    wait_until_listening(reopened_addr);
    cluster
        .register_node(NodeDescriptor {
            id: NodeId(1),
            addr: Some(format!("http://{reopened_addr}")),
            role: NodeRole::Data,
        })
        .expect("re-register A at its restarted endpoint");

    // The sweep must ARM the unfenced orphan (fence(0) probe → fence(epoch)) then drop it.
    let report = cluster.gc_orphan_slots(rt.handle()).expect("gc sweep");
    assert_eq!(
        report.dropped.len(),
        1,
        "the restarted (unfenced) orphan is armed + dropped: {report:?}"
    );
    assert_eq!(report.dropped[0].node, NodeId(1));
    assert!(report.is_clean(), "{report:?}");
    assert!(
        !nodes[0].dir.join("shard_000").exists(),
        "the orphan's dir was reclaimed from the restarted node"
    );
    assert_zero_fn(&cluster, &titles, &oracle, "after restart + gc");

    teardown(&nodes);
}

/// The SECOND live-routed keep shape (the first is the flip-without-commit test): a slot whose
/// position the committed map assigns to a DIFFERENT node (here the seed default, the addr-less
/// manager `NodeId(0)`) — a map-only classification would call it an orphan and destroy the only
/// copy the coordinator is actively routing to. Live routing keeps it, zero-FN. (The truly
/// UNASSIGNED position — a map with a hole — cannot be constructed through the public APIs
/// (`connect_remote` seeds a default assignment per position), so that fail-safe branch is
/// unit-proven in `coordinator/gc.rs` instead.)
#[test]
fn grpc_gc_keeps_slot_committed_elsewhere_but_live_routed() {
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
    // ONE node hosts co-located {0,1}; the operator seeds ONLY position 0 — position 1 keeps the
    // build-time default assignment (the addr-less manager NodeId(0)), so the committed map says
    // "not this node" while live routing very much reaches it here.
    let nodes = spin_n_servers(&rt, &norm, "gc_routed_only", 1);
    let endpoints = vec![nodes[0].ep.clone(), nodes[0].ep.clone()];
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
    cluster
        .register_node(NodeDescriptor {
            id: NodeId(1),
            addr: Some(nodes[0].ep.clone()),
            role: NodeRole::Data,
        })
        .expect("register the node");
    cluster
        .reassign_shard(ShardAssignment {
            position: 0,
            primary: NodeId(1),
            replicas: Vec::new(),
        })
        .expect("seed ONLY position 0");
    cluster.flush().expect("flush");

    // The keep: position 1's slot is committed elsewhere but LIVE-ROUTED here — never dropped.
    let report = cluster.gc_orphan_slots(rt.handle()).expect("gc sweep");
    assert!(report.dropped.is_empty(), "nothing dropped: {report:?}");
    assert_eq!(
        report.kept_live_routed.len(),
        1,
        "the committed-elsewhere-but-routed slot is kept BY LIVE ROUTING: {report:?}"
    );
    assert_eq!(report.kept_live_routed[0].shard_id, 1);
    assert!(nodes[0].dir.join("shard_001").exists(), "its dir is intact");
    assert_zero_fn(&cluster, &titles, &oracle, "after the keep-set sweep");

    teardown(&nodes);
}
