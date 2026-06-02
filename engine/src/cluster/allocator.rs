//! `allocator` — compute a balanced, minimal-movement **shard→node placement** for the
//! cluster-state document (clustering build-path step 5f / ADR-042).
//!
//! Design: docs/design/clustering-and-scaling.md §4.3 (control plane / allocation), §8 (auto-rebalance).
//!
//! The control plane (ADR-037/038) *holds* the shard→node map ([`ShardAssignment`]s in
//! [`ClusterState`](super::control::ClusterState)); this module is what *decides* it. Given the
//! current membership, the ring's shard count, and a replication factor, it produces one
//! [`ShardAssignment`] per position (a primary + RF−1 replicas) that is:
//!   - **balanced** — primaries (and replicas) spread roughly evenly across nodes;
//!   - **deterministic** — the same inputs always yield the same map (any manager computes the
//!     identical placement, so a proposal is idempotent);
//!   - **minimal-movement** — adding/removing a node reassigns only ≈1/N of positions, not a full
//!     reshuffle (the Elasticsearch/Cassandra rebalance property, §8).
//!
//! ## Why rendezvous (HRW) hashing, not `position % N`
//! `position % N` is balanced + deterministic but moves ≈*all* positions when N changes (every
//! modulus shifts). **Rendezvous / highest-random-weight (HRW)** hashing instead ranks the nodes
//! for each position by `hash(position, node)` and takes the top RF: adding a node only wins the
//! positions where it now out-weighs the previous top, ≈1/N of them; removing a node hands off only
//! *its* positions to each one's next-best node. Same balance, far less churn — and it reuses the
//! project's stable [`fnv1a64`](crate::util::fnv1a64) so the weights are identical across runs and
//! nodes. (The same family as the entity-anchor ring in [`HashRing`](super::ring::HashRing); here
//! the key is `(position, node)` rather than a feature id.)
//!
//! Dependency-free / lean core: pure computation over [`NodeId`] + [`ShardAssignment`], no openraft,
//! no gRPC. The coordinator drives it via `ClusterEngine::rebalance` (commit the diff through the
//! control plane); physically *relocating* a shard's segments on a reassignment reuses the existing
//! peer-recovery path (ADR-036/039) and is the deployment wiring on top of this decision layer.

use super::control::{NodeId, ShardAssignment};
use crate::util::fnv1a64;

/// The rendezvous weight of placing `position` on `node` — a stable hash of the pair. Higher wins.
/// `fnv1a64` keeps it identical across runs + nodes (the placement must be reproducible everywhere).
fn hrw_weight(position: u32, node: NodeId) -> u64 {
    let mut bytes = [0u8; 12];
    bytes[0..4].copy_from_slice(&position.to_le_bytes());
    bytes[4..12].copy_from_slice(&node.0.to_le_bytes());
    fnv1a64(&bytes)
}

/// Plan the desired assignment for every shard position over `nodes` at replication factor `rf`.
/// Each position's nodes are the top-`rf` by [`hrw_weight`] (highest = primary, rest = replicas),
/// so the result is balanced, deterministic, and minimal-movement under membership changes. `rf` is
/// clamped to `[1, nodes.len()]` (a position cannot have more distinct copies than there are nodes);
/// `nodes` must be non-empty (the caller checks). Replicas are distinct from the primary by
/// construction (the ranking is over distinct node ids).
pub(crate) fn plan_assignments(
    nodes: &[NodeId],
    num_shards: u32,
    rf: usize,
) -> Vec<ShardAssignment> {
    let rf = rf.clamp(1, nodes.len().max(1));
    (0..num_shards)
        .map(|position| {
            // Rank by weight DESC, tie-broken by node id ASC — fully deterministic.
            let mut ranked: Vec<(u64, NodeId)> = nodes
                .iter()
                .map(|&n| (hrw_weight(position, n), n))
                .collect();
            ranked.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1 .0.cmp(&b.1 .0)));
            let chosen: Vec<NodeId> = ranked.into_iter().take(rf).map(|(_, n)| n).collect();
            // `chosen` is non-empty (rf ≥ 1, nodes non-empty); split primary + replicas.
            let primary = chosen[0];
            let replicas = chosen[1..].to_vec();
            ShardAssignment {
                position,
                primary,
                replicas,
            }
        })
        .collect()
}

/// The subset of `desired` that differs from `current` (compared by position) — the minimal set of
/// `AssignShard` proposals a rebalance must commit. Order-independent on the `current` side.
pub(crate) fn changed_assignments(
    current: &[ShardAssignment],
    desired: Vec<ShardAssignment>,
) -> Vec<ShardAssignment> {
    desired
        .into_iter()
        .filter(|d| current.iter().find(|c| c.position == d.position) != Some(d))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nodes(ids: &[u64]) -> Vec<NodeId> {
        ids.iter().map(|&i| NodeId(i)).collect()
    }

    #[test]
    fn every_position_has_a_distinct_primary_and_replicas() {
        let ns = nodes(&[0, 1, 2, 3, 4]);
        let plan = plan_assignments(&ns, 32, 3);
        assert_eq!(plan.len(), 32);
        for (i, a) in plan.iter().enumerate() {
            assert_eq!(a.position as usize, i, "positions are dense + ordered");
            assert_eq!(a.replicas.len(), 2, "rf=3 ⇒ 1 primary + 2 replicas");
            // primary + replicas are all distinct nodes.
            let mut all = vec![a.primary];
            all.extend(&a.replicas);
            all.sort_unstable();
            let n = all.len();
            all.dedup();
            assert_eq!(all.len(), n, "no node appears twice in a position: {a:?}");
        }
    }

    #[test]
    fn rf_is_clamped_to_node_count() {
        let ns = nodes(&[0, 1]);
        let plan = plan_assignments(&ns, 4, 5); // rf=5 but only 2 nodes
        for a in &plan {
            assert_eq!(
                a.replicas.len(),
                1,
                "rf clamped to 2 ⇒ 1 primary + 1 replica"
            );
        }
        // A single node ⇒ primary only, no replicas.
        let solo = plan_assignments(&nodes(&[7]), 3, 3);
        assert!(solo
            .iter()
            .all(|a| a.primary == NodeId(7) && a.replicas.is_empty()));
    }

    #[test]
    fn placement_is_deterministic() {
        let ns = nodes(&[3, 1, 4, 1, 5]); // dup 1 is harmless for determinism check
        let a = plan_assignments(&ns, 16, 2);
        let b = plan_assignments(&ns, 16, 2);
        assert_eq!(a, b, "same inputs ⇒ identical plan");
    }

    #[test]
    fn primaries_are_roughly_balanced() {
        let ns = nodes(&[0, 1, 2, 3]);
        let num = 4000u32;
        let plan = plan_assignments(&ns, num, 1);
        let mut counts = [0usize; 4];
        for a in &plan {
            counts[a.primary.0 as usize] += 1;
        }
        let expected = num as usize / 4;
        for (node, &c) in counts.iter().enumerate() {
            // HRW spreads evenly; allow a generous ±25% band (it is a hash, not exact round-robin).
            assert!(
                c > expected * 3 / 4 && c < expected * 5 / 4,
                "node {node} got {c} primaries, expected ≈{expected}: {counts:?}"
            );
        }
    }

    #[test]
    fn adding_a_node_moves_about_one_over_n_of_primaries() {
        let num = 4000u32;
        let before = plan_assignments(&nodes(&[0, 1, 2]), num, 1);
        let after = plan_assignments(&nodes(&[0, 1, 2, 3]), num, 1);
        let moved = before
            .iter()
            .zip(&after)
            .filter(|(b, a)| b.primary != a.primary)
            .count();
        // Going 3→4 nodes, the newcomer should win ≈1/4 of positions; almost all churn is positions
        // moving TO node 3 (HRW's minimal-movement property — far below the ≈3/4 a modulus reshuffle
        // would cause).
        let frac = moved as f64 / f64::from(num);
        assert!(
            (0.15..0.35).contains(&frac),
            "expected ≈1/4 primaries to move (3→4 nodes), got {frac:.3} ({moved}/{num})"
        );
        // And the moved positions overwhelmingly landed on the new node.
        let to_new = before
            .iter()
            .zip(&after)
            .filter(|(b, a)| b.primary != a.primary && a.primary == NodeId(3))
            .count();
        assert!(
            to_new * 100 >= moved * 95,
            "≥95% of moves should go to the new node 3: {to_new}/{moved}"
        );
    }

    #[test]
    fn changed_assignments_returns_only_the_diff() {
        let ns = nodes(&[0, 1, 2]);
        let current = plan_assignments(&ns, 8, 1);
        // No change ⇒ empty diff.
        assert!(changed_assignments(&current, plan_assignments(&ns, 8, 1)).is_empty());
        // Add a node ⇒ only the positions that actually moved are returned.
        let desired = plan_assignments(&nodes(&[0, 1, 2, 3]), 8, 1);
        let diff = changed_assignments(&current, desired.clone());
        assert!(!diff.is_empty(), "adding a node changes some positions");
        for d in &diff {
            let c = current.iter().find(|c| c.position == d.position).unwrap();
            assert_ne!(c, d, "every returned entry is genuinely different");
        }
        assert!(diff.len() < desired.len(), "the diff is a strict subset");
    }
}
