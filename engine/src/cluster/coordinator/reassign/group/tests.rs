//! Unit tests for the group-aware target computation + group-equality semantics (ADR-094) —
//! split from `group.rs` to keep it under the file-size goal.

use super::*;
use crate::cluster::control::NodeDescriptor;

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
    }
}

fn assign(position: u32, primary: u64, replicas: &[u64]) -> ShardAssignment {
    ShardAssignment {
        position,
        primary: NodeId(primary),
        replicas: replicas.iter().map(|&r| NodeId(r)).collect(),
    }
}

/// The committed groups exactly match the HRW plan ⇒ no targets — INCLUDING when the committed
/// replica list is in a different ORDER than the plan emits (seed order is CLI order, plan
/// order is HRW rank order): a Vec-compare here would flag every healthy cluster as diverged
/// and drive K spurious O(corpus) moves.
#[test]
fn converged_groups_yield_no_targets_regardless_of_replica_order() {
    let nodes = vec![node(1), node(2), node(3)];
    let ids: Vec<NodeId> = nodes.iter().map(|n| n.id).collect();
    let desired = allocator::plan_assignments(&ids, 6, 2);
    // Commit exactly the plan, but with each replica list REVERSED.
    let committed: Vec<ShardAssignment> = desired
        .iter()
        .map(|a| ShardAssignment {
            position: a.position,
            primary: a.primary,
            replicas: a.replicas.iter().rev().copied().collect(),
        })
        .collect();
    let st = state_with(nodes, 6, committed);
    assert!(
        rebalance_group_targets(&st, 2).is_empty(),
        "an HRW-converged map (replicas set-equal) has nothing to move"
    );
}

/// Primary-only, replica-only, and both-diverged positions are ALL targets at rf=2 — the
/// replica-only case is exactly what the primary-only `rebalance_targets` misses (at RF>1
/// remote, a replica diff IS a data move).
#[test]
fn targets_cover_primary_replica_and_both_divergence() {
    let nodes = vec![node(1), node(2), node(3)];
    let ids: Vec<NodeId> = nodes.iter().map(|n| n.id).collect();
    let desired = allocator::plan_assignments(&ids, 6, 2);

    // Start converged, then perturb: position 0 gets a wrong PRIMARY (swap with its replica),
    // position 1 a wrong REPLICA (rotate to the node the plan left out), position 2 both.
    let mut committed: Vec<ShardAssignment> = desired.clone();
    let other = |a: &ShardAssignment| -> NodeId {
        // The one node of {1,2,3} that is in neither the primary nor the replicas.
        ids.iter()
            .copied()
            .find(|n| *n != a.primary && !a.replicas.contains(n))
            .expect("3 nodes, rf=2 ⇒ exactly one left out")
    };
    committed[0] = ShardAssignment {
        position: desired[0].position,
        primary: desired[0].replicas[0],
        replicas: vec![desired[0].primary],
    };
    committed[1] = ShardAssignment {
        position: desired[1].position,
        primary: desired[1].primary,
        replicas: vec![other(&desired[1])],
    };
    committed[2] = ShardAssignment {
        position: desired[2].position,
        primary: other(&desired[2]),
        replicas: vec![desired[2].primary],
    };
    let st = state_with(nodes, 6, committed);
    let targets = rebalance_group_targets(&st, 2);
    let target_positions: Vec<u32> = targets.iter().map(|(p, _)| *p).collect();
    for expect in [
        desired[0].position,
        desired[1].position,
        desired[2].position,
    ] {
        assert!(
            target_positions.contains(&expect),
            "position {expect} diverges and must be a target: {target_positions:?}"
        );
    }
    // Every target carries the FULL desired assignment (the plan's group, not a bare primary).
    for (p, d) in &targets {
        let planned = desired.iter().find(|a| a.position == *p).unwrap();
        assert!(groups_equal(d, planned), "target {p} carries the HRW group");
    }
    // Untouched positions are not targets.
    for a in &desired[3..] {
        assert!(
            !target_positions.contains(&a.position),
            "converged position {} must not be a target",
            a.position
        );
    }
}

/// A missing committed entry counts as diverged (the move then fails loudly per position),
/// and only addr'd Data nodes are placement candidates (the addr-less manager is excluded).
#[test]
fn missing_assignment_is_diverged_and_manager_is_excluded() {
    let mut manager = node(0);
    manager.addr = None;
    manager.role = NodeRole::Manager;
    let nodes = vec![manager, node(1), node(2)];
    let st = state_with(nodes, 3, Vec::new());
    let targets = rebalance_group_targets(&st, 2);
    assert_eq!(targets.len(), 3, "every unassigned position is a target");
    for (_, d) in &targets {
        assert_ne!(d.primary, NodeId(0), "the manager is never a placement");
        assert!(
            !d.replicas.contains(&NodeId(0)),
            "the manager is never a replica placement"
        );
        assert_eq!(d.replicas.len(), 1, "rf=2 over 2 data nodes ⇒ 1 replica");
    }
}

/// rf clamps to the addr'd-node count: over ONE data node, an rf=3 request plans bare
/// primaries (a commanded de-replication when nodes were deregistered — never silent).
#[test]
fn rf_clamps_to_addrd_node_count() {
    let nodes = vec![node(1)];
    let st = state_with(nodes, 2, vec![assign(0, 1, &[]), assign(1, 1, &[])]);
    assert!(
        rebalance_group_targets(&st, 3).is_empty(),
        "one node, rf clamped to 1, both positions already there ⇒ converged"
    );
}

/// Group equality is primary-identity + replica-SET equality.
#[test]
fn groups_equal_semantics() {
    let a = assign(0, 1, &[2, 3]);
    assert!(
        groups_equal(&a, &assign(0, 1, &[3, 2])),
        "order-insensitive"
    );
    assert!(!groups_equal(&a, &assign(0, 2, &[1, 3])), "primary differs");
    assert!(
        !groups_equal(&a, &assign(0, 1, &[2])),
        "replica set differs"
    );
    assert!(groups_equal(&assign(0, 1, &[]), &assign(0, 1, &[])), "bare");
}
