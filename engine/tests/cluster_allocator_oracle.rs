//! Allocator oracle — the acceptance gate for the shard→node allocator (ADR-042, step 5f).
//!
//! The allocator computes the cluster-state shard→node MAP via rendezvous (HRW) hashing. This
//! proves, over a real `ClusterEngine` (in-memory control plane, no `distributed` feature), that:
//!   * registering nodes + `rebalance` produces a balanced, fully-assigned map (every position has
//!     a primary; the replication factor is honored; primaries spread across nodes);
//!   * `rebalance` is idempotent (a second call with no membership change reassigns nothing);
//!   * a deregistered node holds NO position after the next rebalance; and — the load-bearing one —
//!   * the map change NEVER alters matching: an in-process cluster holds every shard locally, so the
//!     map is advisory and `percolate` is byte-identical before and after every rebalance (the
//!     allocator cannot introduce a false negative — the zero-FN contract is preserved).

use std::collections::HashSet;

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, NodeDescriptor, NodeId, NodeRole};
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;

const NUM_SHARDS: usize = 8;
const RF: usize = 2;

fn vocab() -> Normalizer {
    Normalizer::default_vocab().expect("built-in vocab")
}

fn data_node(id: u64) -> NodeDescriptor {
    NodeDescriptor {
        id: NodeId(id),
        addr: Some(format!("http://127.0.0.1:{}", 7000 + id)),
        role: NodeRole::Data,
    }
}

/// A small in-process cluster + the title set to probe.
fn build() -> (ClusterEngine, Vec<String>) {
    let cfg = GenConfig {
        num_queries: 1200,
        num_titles: 120,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x5F0C_A11E,
        num_players: 300,
        num_sets: 150,
    };
    let data = generate(&cfg);
    let ccfg = ClusterConfig {
        num_shards: NUM_SHARDS,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(vocab(), &ccfg, &data.queries).expect("build cluster");
    (cluster, data.titles)
}

/// The per-title match sets — the matching fingerprint we require unchanged across a rebalance.
fn snapshot(cluster: &ClusterEngine, titles: &[String]) -> Vec<HashSet<u64>> {
    titles
        .iter()
        .map(|t| {
            cluster
                .percolate(t)
                .expect("percolate")
                .into_iter()
                .collect()
        })
        .collect()
}

#[test]
fn allocator_balances_the_map_without_touching_matching() {
    let (cluster, titles) = build();
    let baseline = snapshot(&cluster, &titles);

    // The default document: one logical node owns every position.
    let st0 = cluster.control_state().expect("state");
    assert_eq!(st0.nodes.len(), 1, "genesis is a single logical node");
    assert!(
        st0.assignments
            .iter()
            .all(|a| a.primary == NodeId(0) && a.replicas.is_empty()),
        "genesis assigns every position to NodeId(0)"
    );

    // Register 3 data nodes (joining the genesis NodeId(0) ⇒ 4 placement candidates), rebalance.
    for id in 1..=3 {
        cluster.register_node(data_node(id)).expect("register");
    }
    let moved = cluster.rebalance(RF).expect("rebalance");
    assert!(
        moved > 0,
        "rebalancing onto freshly registered nodes must reassign some positions"
    );

    let st = cluster.control_state().expect("state");
    let members: HashSet<u64> = st.nodes.iter().map(|n| n.id.0).collect();
    assert_eq!(
        st.assignments.len(),
        NUM_SHARDS,
        "every position is assigned"
    );
    for a in &st.assignments {
        assert_eq!(
            a.replicas.len(),
            RF - 1,
            "rf honored at position {}",
            a.position
        );
        let mut here = vec![a.primary];
        here.extend(&a.replicas);
        for n in &here {
            assert!(members.contains(&n.0), "assigned a non-member node {n:?}");
        }
        let mut distinct = here.clone();
        distinct.sort_unstable();
        distinct.dedup();
        assert_eq!(
            distinct.len(),
            here.len(),
            "primary + replicas are distinct at position {}",
            a.position
        );
    }
    // Balanced: primaries are spread across more than one node (HRW over 8 positions / 4 nodes).
    let distinct_primaries: HashSet<u64> = st.assignments.iter().map(|a| a.primary.0).collect();
    assert!(
        distinct_primaries.len() >= 2,
        "primaries should spread across nodes: {distinct_primaries:?}"
    );

    // Idempotent: a second rebalance with no membership change moves nothing.
    assert_eq!(
        cluster.rebalance(RF).expect("rebalance again"),
        0,
        "rebalance with unchanged membership is a no-op"
    );

    // The load-bearing property: matching is byte-identical (the map is advisory in-process).
    assert_eq!(
        snapshot(&cluster, &titles),
        baseline,
        "rebalance must not change any title's match set"
    );
}

#[test]
fn deregistered_node_drops_out_of_the_map_and_matching_is_preserved() {
    let (cluster, titles) = build();
    let baseline = snapshot(&cluster, &titles);
    for id in 1..=3 {
        cluster.register_node(data_node(id)).expect("register");
    }
    cluster.rebalance(RF).expect("rebalance");

    // Remove node 2 + rebalance: it must hold no position (primary or replica) afterward.
    cluster.deregister_node(NodeId(2)).expect("deregister");
    cluster.rebalance(RF).expect("rebalance after removal");
    let st = cluster.control_state().expect("state");
    assert!(
        st.nodes.iter().all(|n| n.id != NodeId(2)),
        "node 2 is gone from membership"
    );
    for a in &st.assignments {
        assert_ne!(
            a.primary,
            NodeId(2),
            "removed node still primary at position {}",
            a.position
        );
        assert!(
            !a.replicas.contains(&NodeId(2)),
            "removed node still a replica at position {}",
            a.position
        );
    }
    assert_eq!(
        st.assignments.len(),
        NUM_SHARDS,
        "every position is still assigned after removal"
    );
    // Matching is still byte-identical after the membership churn.
    assert_eq!(
        snapshot(&cluster, &titles),
        baseline,
        "matching preserved across register + deregister + rebalance"
    );
}
