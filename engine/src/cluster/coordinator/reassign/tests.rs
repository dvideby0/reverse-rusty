use super::*;
use crate::cluster::allocator;
use crate::cluster::control::{ClusterState, NodeDescriptor, NodeRole};

fn node(id: u64) -> NodeDescriptor {
    NodeDescriptor {
        id: NodeId(id),
        addr: Some(format!("http://127.0.0.1:{}", 50050 + id)),
        role: NodeRole::Data,
    }
}

fn state_with(
    nodes: Vec<NodeDescriptor>,
    num_shards: u32,
    assignments: Vec<ShardAssignment>,
) -> ClusterState {
    ClusterState {
        epoch: 0,
        nodes,
        voters: Vec::new(),
        assignments,
        num_shards,
        vnodes: 128,
        dict_fingerprint: 0,
        model_version: 0,
        placement_generation: crate::ownership::PlacementGeneration::INITIAL.get(),
    }
}

/// A map already equal to the HRW desired placement moves nothing (the idempotent re-run / the
/// single-node default ⇒ a no-op rebalance).
#[test]
fn no_targets_when_already_balanced() {
    let nodes = vec![node(1), node(2), node(3)];
    let node_ids: Vec<NodeId> = nodes.iter().map(|n| n.id).collect();
    let num_shards = 8u32;
    let desired = allocator::plan_assignments(&node_ids, num_shards, 1);
    let st = state_with(nodes, num_shards, desired);
    assert!(
        rebalance_group_targets(&st, 1).is_empty(),
        "an already-HRW-balanced map needs no moves"
    );
}

/// No members ⇒ nothing to place ⇒ no targets (the caller turns this into a fail-closed error).
#[test]
fn empty_membership_yields_no_targets() {
    let st = state_with(Vec::new(), 4, Vec::new());
    assert!(rebalance_group_targets(&st, 1).is_empty());
}

/// Targets are exactly the positions whose PRIMARY changes, named with the HRW desired owner,
/// sorted ascending and one per position; unmoved positions keep their current primary.
#[test]
fn targets_are_changed_primaries_sorted() {
    let nodes = vec![node(1), node(2), node(3)];
    let num_shards = 8u32;
    // Current: every position on node 1. HRW over {1,2,3} pulls ~2/3 of them off node 1.
    let current: Vec<ShardAssignment> = (0..num_shards)
        .map(|p| ShardAssignment {
            position: p,
            primary: NodeId(1),
            replicas: Vec::new(),
        })
        .collect();
    let st = state_with(nodes.clone(), num_shards, current);
    let targets: Vec<(u32, NodeId)> = rebalance_group_targets(&st, 1)
        .into_iter()
        .map(|(p, d)| (p, d.primary))
        .collect();
    assert!(
        !targets.is_empty(),
        "HRW over 3 nodes must move some positions off node 1"
    );

    // Sorted ascending, one per position.
    let positions: Vec<u32> = targets.iter().map(|(p, _)| *p).collect();
    let mut sorted = positions.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(positions, sorted, "targets sorted by position, no dups");

    // Each target names the HRW desired primary and is a genuine change off node 1.
    let node_ids: Vec<NodeId> = nodes.iter().map(|n| n.id).collect();
    let desired = allocator::plan_assignments(&node_ids, num_shards, 1);
    for (pos, to) in &targets {
        let d = desired.iter().find(|a| a.position == *pos).unwrap();
        assert_eq!(d.primary, *to, "target names the HRW desired primary");
        assert_ne!(*to, NodeId(1), "only changed primaries are targets");
    }
    // Positions absent from targets kept their current primary (node 1).
    for a in &desired {
        if !targets.iter().any(|(p, _)| *p == a.position) {
            assert_eq!(a.primary, NodeId(1), "unmoved positions stayed on node 1");
        }
    }
}

/// Targets are planned only over data nodes WITH an address: the addr-less control-plane manager
/// (`NodeId(0)`) and any addr-less data node are never picked as a move destination (HRW must not
/// produce a move-to-the-manager that then fails on the missing endpoint).
#[test]
fn excludes_manager_and_addrless_nodes() {
    let manager = NodeDescriptor {
        id: NodeId(0),
        addr: None,
        role: NodeRole::Manager,
    };
    let addrless_data = NodeDescriptor {
        id: NodeId(9),
        addr: None,
        role: NodeRole::Data,
    };
    let nodes = vec![manager, node(1), node(2), addrless_data];
    let num_shards = 8u32;
    let current: Vec<ShardAssignment> = (0..num_shards)
        .map(|p| ShardAssignment {
            position: p,
            primary: NodeId(1),
            replicas: Vec::new(),
        })
        .collect();
    let st = state_with(nodes, num_shards, current);
    let targets: Vec<(u32, NodeId)> = rebalance_group_targets(&st, 1)
        .into_iter()
        .map(|(p, d)| (p, d.primary))
        .collect();
    assert!(
        !targets.is_empty(),
        "HRW over the 2 eligible data nodes still moves some positions off node 1"
    );
    for (_pos, to) in &targets {
        assert!(
            *to == NodeId(1) || *to == NodeId(2),
            "only addr'd data nodes are targets, got {to:?}"
        );
    }
}

/// An `rf > 1` sweep is no longer rejected up front (ADR-094 replaces the ADR-092-landing
/// guard): `rebalance_and_move(2, ..)` computes GROUP targets and dispatches each replicated
/// placement to `reassign_group_and_move`. Here the first group move fails cleanly +
/// network-free (the committed primary is the addr-less manager) and the sweep stops-on-first
/// as documented — proving the rf=2 request flows down the group path rather than erroring
/// the whole call.
#[test]
fn rebalance_and_move_rf2_dispatches_group_moves() {
    use crate::cluster::coordinator::{ClusterConfig, ClusterEngine};
    use crate::normalize::Normalizer;

    let queries: Vec<(u64, String)> = vec![(1, "+nike +shoe".into()), (2, "+sony +tv".into())];
    let cluster = ClusterEngine::build(
        Normalizer::default_vocab().expect("vocab"),
        &ClusterConfig {
            num_shards: 2,
            ..ClusterConfig::default()
        },
        &queries,
    )
    .expect("in-process bare cluster");
    for id in [1u64, 2] {
        cluster
            .register_node(NodeDescriptor {
                id: NodeId(id),
                addr: Some(format!("http://127.0.0.1:{id}")),
                role: NodeRole::Data,
            })
            .expect("register node");
    }
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let report = cluster
        .rebalance_and_move(2, rt.handle())
        .expect("rf=2 sweep runs (per-position failures land in the report)");
    let (pos, msg) = report
        .failed
        .as_ref()
        .expect("the first group move fails loudly");
    assert!(
        msg.contains("reassign_group_and_move"),
        "an rf=2 target with replicas dispatches to the GROUP move: {msg}"
    );
    assert!(
        report.moved.is_empty(),
        "nothing moved (the manager primary has no endpoint): {report:?}"
    );
    assert!(
        report.not_attempted.iter().all(|p| p != pos),
        "stop-on-first: the failed position is not also listed as not-attempted"
    );
}

/// The public single and group APIs accept `usize` for historical callers but
/// the control and wire position is `u32`. Reject instead of truncating an
/// oversized value onto an unrelated real shard.
#[cfg(target_pointer_width = "64")]
#[test]
fn reassign_rejects_position_above_u32_without_narrowing() {
    use crate::cluster::coordinator::{ClusterConfig, ClusterEngine};
    use crate::normalize::Normalizer;

    let cluster = ClusterEngine::build(
        Normalizer::default_vocab().expect("vocab"),
        &ClusterConfig::default(),
        &[],
    )
    .expect("cluster");
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let position = usize::try_from(u64::from(u32::MAX) + 1).expect("64-bit usize");
    let error = cluster
        .reassign_and_move(position, NodeId(1), runtime.handle())
        .expect_err("oversized position must fail before endpoint or network access");
    assert!(error.to_string().contains("exceeds the u32"), "{error}");

    let error = cluster
        .reassign_group_and_move(
            position,
            &ShardAssignment {
                position: 0,
                primary: NodeId(1),
                replicas: vec![NodeId(2)],
            },
            runtime.handle(),
        )
        .expect_err("oversized group position must fail before endpoint or network access");
    assert!(error.to_string().contains("exceeds the u32"), "{error}");
}
