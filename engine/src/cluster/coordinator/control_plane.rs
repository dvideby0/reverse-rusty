//! `impl ClusterEngine` — control-plane operations (membership, assignment, rebalance,
//! runtime replica growth) and the durability-event observer plumbing.

use std::path::Path;
use std::sync::Arc;

use crate::cluster::allocator;
use crate::cluster::control::{
    ClusterState, ClusterStateChange, NodeDescriptor, NodeId, ShardAssignment,
};
use crate::cluster::shard::ShardError;
use crate::config::EngineConfig;
use crate::events::EngineEvent;

use super::{shard_dir, ClusterEngine, ClusterObserver};

impl ClusterEngine {
    /// A snapshot of the committed cluster-state document (membership + shard→node map +
    /// ring params + feature-model version + epoch — ADR-037), read from the control plane.
    /// An owned clone, so the caller holds a stable view. Distinct from [`Self::epoch`]
    /// (the local checkpoint generation): [`ClusterState::epoch`] is the control-plane term.
    pub fn control_state(&self) -> Result<ClusterState, ShardError> {
        Ok((*self.control.cluster_state()?).clone())
    }

    /// The node assignment for one shard POSITION (the ring's output index). Errors loudly
    /// if a live position is unassigned — a silently-unrouted shard would be a shard-sized
    /// false negative (the same fail-closed stance as a propagating shard probe). In-process
    /// every position is assigned to the one logical node.
    pub fn assignment_for(&self, position: usize) -> Result<ShardAssignment, ShardError> {
        let state = self.control.cluster_state()?;
        state
            .assignments
            .iter()
            .find(|a| a.position as usize == position)
            .cloned()
            .ok_or_else(|| {
                ShardError::ControlPlane(format!("no assignment for shard position {position}"))
            })
    }

    /// Commit a shard→node (re)assignment through the control plane — the rebalance /
    /// failover primitive. In-process this updates the committed map and bumps the
    /// control-plane epoch *without moving data* (physical movement on an assignment change
    /// — peer recovery — is a later increment). Errors propagate (fail-closed).
    pub fn reassign_shard(&self, assignment: ShardAssignment) -> Result<(), ShardError> {
        self.control
            .propose(ClusterStateChange::AssignShard(assignment))?;
        Ok(())
    }

    /// Register (or replace, by id) a cluster member in the control-plane document — the membership
    /// half of the allocator's inputs (ADR-042). Idempotent; errors fail-closed. A subsequent
    /// [`Self::rebalance`] folds the node into the shard→node map.
    pub fn register_node(&self, node: NodeDescriptor) -> Result<(), ShardError> {
        self.control.propose(ClusterStateChange::AddNode(node))?;
        Ok(())
    }

    /// Deregister a member by id (idempotent). Pruning it from the shard→node map is the separate
    /// [`Self::rebalance`] (exactly as removing a node is distinct from re-placing its shards).
    pub fn deregister_node(&self, id: NodeId) -> Result<(), ShardError> {
        self.control.propose(ClusterStateChange::RemoveNode(id))?;
        Ok(())
    }

    /// Recompute the desired **shard→node placement** from the current membership at replication
    /// factor `rf`, and commit only the positions that changed (ADR-042). Uses rendezvous (HRW)
    /// hashing so the map is balanced, deterministic, and minimal-movement: a node added/removed
    /// since the last rebalance reassigns ≈1/N of positions, not all of them. Returns the number of
    /// positions reassigned (0 ⇒ already balanced — e.g. the single-node default, a no-op). Errors
    /// fail-closed (a rejected proposal leaves the prior map intact).
    ///
    /// This is the **decision** layer: it commits the map the control plane holds. Physically
    /// relocating a shard's segments to a new owner on a reassignment reuses the existing
    /// peer-recovery path ([`Self::peer_recover_replica`], ADR-036/039) and is the deployment wiring
    /// on top — an in-process cluster holds every shard locally, so the map is advisory there and
    /// matching is unaffected (the local shards do not move).
    pub fn rebalance(&self, rf: usize) -> Result<usize, ShardError> {
        let state = self.control.cluster_state()?;
        let nodes: Vec<NodeId> = state.nodes.iter().map(|n| n.id).collect();
        if nodes.is_empty() {
            return Err(ShardError::ControlPlane(
                "rebalance: the cluster has no nodes to place shards on".into(),
            ));
        }
        let desired = allocator::plan_assignments(&nodes, state.num_shards, rf);
        let changed = allocator::changed_assignments(&state.assignments, desired);
        let count = changed.len();
        for assignment in changed {
            self.control
                .propose(ClusterStateChange::AssignShard(assignment))?;
        }
        Ok(count)
    }

    /// Runtime replica growth (ADR-040): bring up an additional in-process replica for shard
    /// `position` from its durable primary WITHOUT quiescing writes — peer-recover a snapshot +
    /// translog tail, loop the catch-up to shrink the residual, then promote it into the in-sync
    /// set under a brief quiesce (the finalize that closes ADR-036's whole-copy quiesce window). A
    /// retention lease pins the primary's tail across the copy so a concurrent seal cannot strand
    /// it. `replica_dir` roots the new copy's local segments + translog; `max_passes` bounds the
    /// convergence loop (the final quiesce covers whatever residual remains, so correctness never
    /// depends on convergence — only the window size does). Requires a durable cluster whose
    /// `position` is already replicated (a [`ReplicatedShard`](crate::cluster::replica::ReplicatedShard));
    /// errors fail-closed otherwise. Uses the default per-shard [`EngineConfig`] for the new copy.
    pub fn add_replica(
        &self,
        position: usize,
        replica_dir: &Path,
        max_passes: usize,
    ) -> Result<(), ShardError> {
        let Some(base) = self.data_dir.as_deref() else {
            return Err(ShardError::Config(
                "add_replica requires a durable cluster (no on-disk segments to copy)".into(),
            ));
        };
        let shard = self.shards.get(position).ok_or_else(|| {
            ShardError::Config(format!(
                "add_replica: shard position {position} out of range"
            ))
        })?;
        let primary_dir = shard_dir(base, position);
        shard.add_recovered_replica(
            &self.norm,
            &self.dict,
            EngineConfig::default(),
            &primary_dir,
            replica_dir,
            max_passes,
        )
    }

    /// Register an observer for durability events (recovery torn-tail, append failures).
    /// Any events buffered before this call are delivered immediately, mirroring the
    /// engine's `set_observer`.
    pub fn set_observer(&self, observer: ClusterObserver) {
        let pending: Vec<EngineEvent> = {
            let mut p = self
                .pending_events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut *p)
        };
        for ev in &pending {
            observer(ev);
        }
        // Fan the observer into each shard as an event sink, so a `ReplicatedShard` surfaces its
        // degraded-redundancy (`ReplicaDesync`) events through the same observer (ADR-035). A
        // plain shard's default `set_event_sink` is a no-op.
        for shard in &self.shards {
            shard.set_event_sink(Arc::clone(&observer));
        }
        *self
            .observer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(observer);
    }

    /// Emit a durability event: deliver to the observer if set, else buffer it for
    /// delivery on [`Self::set_observer`]. Library code never writes stderr (ADR-021).
    pub(in crate::cluster::coordinator) fn emit(&self, ev: EngineEvent) {
        let obs = self
            .observer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(obs) = obs {
            obs(&ev);
        } else {
            self.pending_events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(ev);
        }
    }
}
