//! `impl ClusterEngine` — the autoscaler driver: collect a [`LoadSnapshot`], run the pure
//! policy ([`evaluate`]), execute the executable subset (`rebalance`), and return the full
//! decision (incl. advisories). The policy itself lives in [`crate::cluster::autoscale`].

use crate::cluster::autoscale::{
    evaluate, AutoscaleConfig, AutoscaleDecision, LoadSnapshot, ScalingAction,
};
use crate::cluster::control::{NodeDescriptor, NodeId};
use crate::cluster::shard::ShardError;
#[cfg(feature = "distributed")]
use crate::events::{DurabilityOp, EngineEvent};

use super::ClusterEngine;

impl ClusterEngine {
    /// Collect the deterministic policy input: membership + the shard→node map from the
    /// control plane ([`Self::control_state`]) and per-shard corpus from the shards
    /// ([`Self::shard_query_counts`] — the only load signal that crosses the
    /// [`Shard`](crate::cluster::ClusterEngine) seam, so this works in-process AND across
    /// nodes). Fail-closed: a control-plane or shard error propagates rather than yielding a
    /// partial/blind snapshot.
    pub fn collect_load(&self, config: &AutoscaleConfig) -> Result<LoadSnapshot, ShardError> {
        let state = self.control_state()?;
        let shard_corpus = self.shard_query_counts()?;
        Ok(LoadSnapshot {
            nodes: state.nodes,
            assignments: state.assignments,
            shard_corpus,
            num_shards: state.num_shards,
            replication_factor: config.target_replication_factor,
        })
    }

    /// One autoscaler cycle: validate the config (fail-closed), collect the snapshot, run the
    /// policy, **execute the executable subset** (each [`Rebalance`](ScalingAction::Rebalance)
    /// dispatches to the idempotent [`Self::rebalance`] — a no-op when already balanced), and
    /// return the full [`AutoscaleDecision`] including the advisories
    /// ([`Handoff`](ScalingAction::Handoff)/[`RecommendSplit`](ScalingAction::RecommendSplit)/…)
    /// for the caller to log or act on. A disabled config yields an empty decision ⇒ a no-op
    /// tick, so a default-config caller is byte-identical to no autoscaler at all.
    pub fn tick(&self, config: &AutoscaleConfig) -> Result<AutoscaleDecision, ShardError> {
        let problems = config.validate();
        if !problems.is_empty() {
            return Err(ShardError::Config(format!(
                "invalid autoscale config: {}",
                problems.join("; ")
            )));
        }
        // Opportunistically converge any partial-apply divergence (ADR-047) each cycle — a cheap
        // no-op when nothing is queued (the default path). Repairing before snapshotting load
        // keeps the autoscaler's view consistent with the converged cluster.
        let _ = self.resync();
        let snapshot = self.collect_load(config)?;
        let decision = evaluate(&snapshot, config);
        // Execute the executable subset. A `Rebalance` reconciles placement (idempotent — a no-op
        // when already balanced).
        for action in &decision.actions {
            if let ScalingAction::Rebalance { rf } = action {
                self.rebalance(*rf)?;
            }
        }
        // An advisory `Handoff` (the policy, ADR-045) is now DRIVEN through `execute_handoff`
        // (ADR-048) — but only when NO rebalance ran this tick, because a rebalance moves placement
        // and would make the handoff's `from`/`to` (from the pre-rebalance snapshot) stale; the
        // skipped handoff is re-evaluated next tick. `RecommendSplit`/`RecommendScaleOut` stay
        // advisory (returned in the decision only). Gated: a `Handoff` can't arise in-process (skew
        // needs ≥2 loaded nodes) and `execute_handoff` is `distributed`-only, so the lean build
        // returns the recommendation without acting — byte-identical to before.
        #[cfg(feature = "distributed")]
        {
            let rebalanced = decision
                .actions
                .iter()
                .any(|a| matches!(a, ScalingAction::Rebalance { .. }));
            if !rebalanced {
                for action in &decision.actions {
                    if let ScalingAction::Handoff { position, from, to } = action {
                        self.drive_autoscaled_handoff(&snapshot, *position, *from, *to);
                    }
                }
            }
        }
        Ok(decision)
    }

    /// Drive an autoscaler-recommended [`Handoff`](ScalingAction::Handoff) through
    /// [`execute_handoff`](Self::execute_handoff) (ADR-048, closing ADR-045's advisory-only gap).
    /// Best-effort and side-effecting only: it never fails the enclosing `tick`. Skips silently when
    /// the cluster has no runtime handle (an in-process cluster can't hand off to a remote node) or
    /// when the recommendation is stale (a concurrent change moved `position` off `from`); surfaces
    /// a missing endpoint or a failed move as an event so the operator can see why a recommended
    /// move did or didn't happen. A failed move leaves routing unchanged and the source unfenced
    /// (ADR-048), so the next tick can retry.
    #[cfg(feature = "distributed")]
    fn drive_autoscaled_handoff(
        &self,
        snapshot: &LoadSnapshot,
        position: u32,
        from: NodeId,
        to: NodeId,
    ) {
        // Only a gRPC-built cluster carries a runtime handle (and only it has remote endpoints to
        // move between). An in-process cluster has neither, so there is nothing to do.
        let Some(handle) = self.handle.clone() else {
            return;
        };
        // Re-validate against the CURRENT assignment: skip a recommendation whose source no longer
        // owns the position rather than mis-targeting a move.
        let owns_position = snapshot
            .assignments
            .iter()
            .any(|a| a.position == position && a.primary.0 == from.0);
        if !owns_position {
            return;
        }
        // Resolve node ids → gRPC endpoints from the membership snapshot.
        let endpoint = |id: NodeId| -> Option<String> {
            snapshot
                .nodes
                .iter()
                .find(|n| n.id.0 == id.0)
                .and_then(|n| n.addr.clone())
        };
        let (Some(src_ep), Some(tgt_ep)) = (endpoint(from), endpoint(to)) else {
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::ReplicaDesync,
                detail: format!(
                    "autoscaler recommended moving shard {position} from node {} to node {}, but a \
                     node has no registered endpoint; skipping (the decision still reports it)",
                    from.0, to.0
                ),
                error: "missing node endpoint for an autoscaled handoff".into(),
            });
            return;
        };
        if let Err(e) = self.execute_handoff(position as usize, &src_ep, &tgt_ep, &handle) {
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::ReplicaDesync,
                detail: format!(
                    "autoscaler-driven handoff of shard {position} from node {} to node {} failed; \
                     the source auto-unfenced and routing is unchanged (retried next tick)",
                    from.0, to.0
                ),
                error: e.to_string(),
            });
        }
    }

    /// Event-driven entry: a node joined — register it, then run a [`Self::tick`]. The tick's
    /// membership-drift rule turns the new node into a rebalance that folds it into the map.
    pub fn on_node_joined(
        &self,
        node: NodeDescriptor,
        config: &AutoscaleConfig,
    ) -> Result<AutoscaleDecision, ShardError> {
        self.register_node(node)?;
        self.tick(config)
    }

    /// Event-driven entry: a node left — deregister it, then run a [`Self::tick`] (which
    /// rebalances its positions onto the survivors).
    pub fn on_node_left(
        &self,
        id: NodeId,
        config: &AutoscaleConfig,
    ) -> Result<AutoscaleDecision, ShardError> {
        self.deregister_node(id)?;
        self.tick(config)
    }
}
