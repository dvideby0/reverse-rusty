use super::*;

fn node(id: u64, role: NodeRole) -> NodeDescriptor {
    NodeDescriptor {
        id: NodeId(id),
        addr: Some(format!("http://127.0.0.1:{}", 50050 + id)),
        role,
    }
}

#[test]
fn single_node_is_one_manager_owning_every_position() {
    let cp = InMemoryControlPlane::single_node(4, 128, 0xABCD);
    let st = cp.cluster_state().unwrap();
    assert_eq!(st.epoch, 0);
    assert_eq!(st.num_shards, 4);
    assert_eq!(st.nodes.len(), 1);
    assert_eq!(st.voters, vec![NodeId(0)]);
    assert_eq!(st.assignments.len(), 4);
    assert!(st
        .assignments
        .iter()
        .all(|a| a.primary == NodeId(0) && a.replicas.is_empty()));
    assert_eq!(cp.leader().unwrap(), Some(NodeId(0)));
}

#[test]
fn add_node_is_idempotent_and_sorted_and_bumps_version() {
    let cp = InMemoryControlPlane::single_node(1, 64, 0);
    let v0 = cp.version().unwrap();
    cp.propose(ClusterStateChange::AddNode(node(2, NodeRole::Data)))
        .unwrap();
    let v1 = cp
        .propose(ClusterStateChange::AddNode(node(1, NodeRole::Data)))
        .unwrap();
    assert!(v1 > v0, "each commit advances the version");
    // Re-adding the same id replaces, never duplicates.
    cp.propose(ClusterStateChange::AddNode(node(1, NodeRole::Manager)))
        .unwrap();
    let st = cp.cluster_state().unwrap();
    let ids: Vec<u64> = st.nodes.iter().map(|n| n.id.0).collect();
    assert_eq!(ids, vec![0, 1, 2], "no dups, sorted by id");
    assert_eq!(
        st.nodes.iter().find(|n| n.id == NodeId(1)).unwrap().role,
        NodeRole::Manager,
        "the last add wins"
    );
}

#[test]
fn remove_node_is_idempotent() {
    let cp = InMemoryControlPlane::single_node(1, 64, 0);
    cp.propose(ClusterStateChange::AddNode(node(5, NodeRole::Data)))
        .unwrap();
    cp.propose(ClusterStateChange::RemoveNode(NodeId(5)))
        .unwrap();
    cp.propose(ClusterStateChange::RemoveNode(NodeId(5)))
        .unwrap(); // no-op, no panic
    assert!(cp
        .cluster_state()
        .unwrap()
        .nodes
        .iter()
        .all(|n| n.id != NodeId(5)));
}

#[test]
fn assign_shard_replaces_position_kept_sorted() {
    let cp = InMemoryControlPlane::single_node(3, 64, 0);
    cp.propose(ClusterStateChange::AssignShard(ShardAssignment {
        position: 2,
        primary: NodeId(7),
        replicas: vec![NodeId(8)],
    }))
    .unwrap();
    // Replace the same position rather than appending a second entry for it.
    cp.propose(ClusterStateChange::AssignShard(ShardAssignment {
        position: 2,
        primary: NodeId(9),
        replicas: vec![],
    }))
    .unwrap();
    let st = cp.cluster_state().unwrap();
    let positions: Vec<u32> = st.assignments.iter().map(|a| a.position).collect();
    assert_eq!(positions, vec![0, 1, 2], "one entry per position, sorted");
    let p2 = st.assignments.iter().find(|a| a.position == 2).unwrap();
    assert_eq!(p2.primary, NodeId(9));
    assert!(p2.replicas.is_empty(), "the last assignment wins");
}

#[test]
fn bump_model_version_advances_fingerprint_and_counter() {
    let cp = InMemoryControlPlane::single_node(1, 64, 0x1111);
    cp.propose(ClusterStateChange::BumpModelVersion {
        dict_fingerprint: 0x2222,
    })
    .unwrap();
    let st = cp.cluster_state().unwrap();
    assert_eq!(st.dict_fingerprint, 0x2222);
    assert_eq!(st.model_version, 1);
}

#[test]
fn change_membership_sorts_dedups_and_is_distinct_from_propose() {
    let cp = InMemoryControlPlane::single_node(1, 64, 0);
    cp.change_membership(vec![NodeId(3), NodeId(1), NodeId(3), NodeId(2)])
        .unwrap();
    assert_eq!(
        cp.cluster_state().unwrap().voters,
        vec![NodeId(1), NodeId(2), NodeId(3)]
    );
    // Leader is the first voter.
    assert_eq!(cp.leader().unwrap(), Some(NodeId(1)));
}

#[test]
fn proposals_are_deterministic_regardless_of_order() {
    // Two backends fed the same change SET in different orders converge to the same
    // canonical document — the property the two-backend differential relies on.
    let mk = || InMemoryControlPlane::single_node(2, 64, 0);
    let (a, b) = (mk(), mk());
    let changes = [
        ClusterStateChange::AddNode(node(3, NodeRole::Data)),
        ClusterStateChange::AddNode(node(1, NodeRole::Data)),
        ClusterStateChange::AssignShard(ShardAssignment {
            position: 1,
            primary: NodeId(3),
            replicas: vec![NodeId(1)],
        }),
    ];
    for c in &changes {
        a.propose(c.clone()).unwrap();
    }
    for c in changes.iter().rev() {
        b.propose(c.clone()).unwrap();
    }
    // Same membership + assignments (epoch differs only if order changed counts — here
    // both applied 3 changes, so epochs match too).
    let (sa, sb) = (a.cluster_state().unwrap(), b.cluster_state().unwrap());
    assert_eq!(sa.nodes, sb.nodes);
    assert_eq!(sa.assignments, sb.assignments);
    assert_eq!(sa.epoch, sb.epoch);
}

#[test]
fn broken_backend_fails_closed() {
    let cp = InMemoryControlPlane::single_node(1, 64, 0);
    let before = cp.cluster_state().unwrap();
    cp.break_proposals_for_test();
    assert!(matches!(
        cp.propose(ClusterStateChange::AddNode(node(1, NodeRole::Data))),
        Err(ControlError::Backend(_))
    ));
    // State is unchanged — fail-closed (no partial mutation).
    assert_eq!(*cp.cluster_state().unwrap(), *before);
}

#[test]
fn control_error_folds_into_shard_error() {
    let e: ShardError = ControlError::NoQuorum.into();
    assert!(matches!(e, ShardError::ControlPlane(_)));
}
