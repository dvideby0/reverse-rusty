//! Deploy-topology ↔ control-document bridge (ADR-086): seed the committed shard→node map from a
//! coordinator's endpoint list, and resolve it back, so a coordinator can route by the durable
//! quorum's assignments instead of its static `--shard-endpoint` flags.
//!
//! Both functions are pure [`ControlPlane`] logic — no `ClusterEngine`, no gRPC, no `distributed`
//! feature — so they unit-test against the lean
//! [`InMemoryControlPlane`](crate::cluster::control::InMemoryControlPlane). They speak the
//! `ShardGroup`-free [`ShardEndpoints`] shape; the coordinator-mode binary maps it to/from
//! [`ShardGroup`](super::ShardGroup).
//!
//! SAFETY: resolution is only correct when the committed map is *position-preserving* — each
//! `position`'s primary node physically holds that position's data. The HRW
//! [`rebalance`](super::ClusterEngine::rebalance) permutes the map WITHOUT moving data, so routing a
//! post-rebalance map would send a position's titles to a node holding a different shard (a false
//! negative). The coordinator guards this by asserting the resolved topology equals its
//! `--shard-endpoint` list on boot; data-moving reassignment (live handoff) is a deferred follow-on.

use std::collections::HashMap;

use crate::cluster::control::{
    ClusterStateChange, ControlPlane, NodeDescriptor, NodeId, NodeRole, ShardAssignment,
};
use crate::cluster::shard::ShardError;

/// One shard position's resolved endpoints: `(primary, replicas)` — the lean, `ShardGroup`-free
/// shape [`resolve_topology`] / [`seed_position_preserving`] speak.
pub type ShardEndpoints = (String, Vec<String>);

/// Intern an endpoint URL to a logical `Data` [`NodeId`], reusing a committed node when its `addr`
/// already matches (idempotent) and otherwise proposing a fresh `AddNode`. New ids are allocated
/// from `next_id` upward in first-seen order, so re-seeding is deterministic.
fn intern_node(
    control: &dyn ControlPlane,
    url_to_id: &mut HashMap<String, NodeId>,
    next_id: &mut u64,
    url: &str,
) -> Result<NodeId, ShardError> {
    if let Some(id) = url_to_id.get(url) {
        return Ok(*id);
    }
    let id = NodeId(*next_id);
    *next_id += 1;
    url_to_id.insert(url.to_string(), id);
    control.propose(ClusterStateChange::AddNode(NodeDescriptor {
        id,
        addr: Some(url.to_string()),
        role: NodeRole::Data,
    }))?;
    Ok(id)
}

/// Seed the committed document with a **position-preserving** shard→node map derived from
/// `topology` (ADR-086): one logical `Data` node per distinct endpoint URL, `position i →` the node
/// for `topology[i].0` (primary) plus the nodes for its replicas. Idempotent — proposes only the
/// diff, so a clean coordinator restart is a no-op. Overwrites the genesis "every position →
/// `NodeId(0)`" map, closing the bootstrap gap so a subsequent [`resolve_topology`] reads back
/// `topology`. `NodeId(0)` is reserved for the genesis (addr-less) manager; data nodes get ids
/// above every committed id.
pub fn seed_position_preserving(
    control: &dyn ControlPlane,
    topology: &[ShardEndpoints],
) -> Result<(), ShardError> {
    let state = control.cluster_state()?;
    let mut url_to_id: HashMap<String, NodeId> = state
        .nodes
        .iter()
        .filter_map(|n| n.addr.clone().map(|a| (a, n.id)))
        .collect();
    let mut next_id = state.nodes.iter().map(|n| n.id.0).max().unwrap_or(0) + 1;
    for (position, (primary, replicas)) in topology.iter().enumerate() {
        let primary_id = intern_node(control, &mut url_to_id, &mut next_id, primary)?;
        let mut replica_ids = Vec::with_capacity(replicas.len());
        for r in replicas {
            replica_ids.push(intern_node(control, &mut url_to_id, &mut next_id, r)?);
        }
        let position = position as u32;
        let want = ShardAssignment {
            position,
            primary: primary_id,
            replicas: replica_ids,
        };
        // The `AddNode` proposals above do not touch the assignment map, so the pre-seed snapshot is
        // a valid basis for the `AssignShard` diff: skip when the committed entry already matches (so
        // a clean restart proposes nothing).
        let already = state
            .assignments
            .iter()
            .find(|a| a.position == position)
            .is_some_and(|a| a.primary == want.primary && a.replicas == want.replicas);
        if !already {
            control.propose(ClusterStateChange::AssignShard(want))?;
        }
    }
    Ok(())
}

/// Resolve the committed shard→node map back to a per-position `(primary, replicas)` endpoint list
/// (ADR-086): `position → assignments[position] → NodeId → nodes.addr`. **Fail-closed** — an
/// unassigned position or a node without a registered `addr` errors rather than yielding a
/// silently-unrouted shard (a false negative). `num_shards` bounds the positions resolved.
pub fn resolve_topology(
    control: &dyn ControlPlane,
    num_shards: u32,
) -> Result<Vec<ShardEndpoints>, ShardError> {
    let state = control.cluster_state()?;
    let addr_of = |id: NodeId| -> Result<String, ShardError> {
        state
            .nodes
            .iter()
            .find(|n| n.id == id)
            .and_then(|n| n.addr.clone())
            .ok_or_else(|| {
                ShardError::ControlPlane(format!(
                    "shard node {} has no registered endpoint (addr); cannot route by assignments",
                    id.0
                ))
            })
    };
    let mut topology = Vec::with_capacity(num_shards as usize);
    for position in 0..num_shards {
        let assignment = state
            .assignments
            .iter()
            .find(|a| a.position == position)
            .ok_or_else(|| {
                ShardError::ControlPlane(format!(
                    "no committed assignment for shard position {position}; cannot route by \
                     assignments (is the quorum seeded?)"
                ))
            })?;
        let primary = addr_of(assignment.primary)?;
        let replicas = assignment
            .replicas
            .iter()
            .map(|r| addr_of(*r))
            .collect::<Result<Vec<_>, _>>()?;
        topology.push((primary, replicas));
    }
    Ok(topology)
}

/// Decide the shard topology for a `--route-by-assignments` coordinator (ADR-086), reading the
/// committed map FIRST so a populated (possibly rebalanced) quorum is never silently overwritten:
///
/// - a *genesis* (unseeded) quorum is seeded position-preservingly from `cli` — or fails loud when
///   `cli` is empty (nothing to seed from, e.g. a resolve-only boot against an unseeded quorum);
/// - a *populated* quorum is authoritative; when `cli` is non-empty the resolved map MUST be
///   position-preserving (equal to `cli`), else this fails loud — a non-data-moving rebalance would
///   route a position's titles to a node holding different data (a false negative);
/// - with an empty `cli` (resolve-only boot) the committed map is trusted.
///
/// **Reading before any seed is load-bearing:** seeding first would overwrite a rebalanced map back to
/// the CLI order and silently defeat the guard. Lean (`&dyn ControlPlane`), so the decision is
/// unit-tested against `InMemoryControlPlane`.
pub fn route_topology(
    control: &dyn ControlPlane,
    num_shards: u32,
    cli: &[ShardEndpoints],
) -> Result<Vec<ShardEndpoints>, ShardError> {
    let state = control.cluster_state()?;
    // "Seeded" = at least one position is placed on a node WITH an address. A fresh bootstrap is
    // genesis (every position → the addr-less manager `NodeId(0)`), which is NOT seeded.
    let seeded = state.assignments.iter().any(|a| {
        state
            .nodes
            .iter()
            .any(|n| n.id == a.primary && n.addr.is_some())
    });
    if !seeded {
        if cli.is_empty() {
            return Err(ShardError::ControlPlane(
                "the control-plane quorum has no committed shard→node assignments (genesis) and no \
                 --shard-endpoint to seed them from"
                    .into(),
            ));
        }
        seed_position_preserving(control, cli)?;
    }
    let resolved = resolve_topology(control, num_shards)?;
    if !cli.is_empty() && resolved.as_slice() != cli {
        return Err(ShardError::ControlPlane(format!(
            "committed shard→node assignments differ from --shard-endpoint: the committed map is not \
             position-preserving (a non-data-moving rebalance?). Routing it would send a position's \
             titles to a node holding different data (a false negative). Re-seed the quorum or omit \
             --route-by-assignments. resolved={resolved:?} cli={cli:?}"
        )));
    }
    Ok(resolved)
}
