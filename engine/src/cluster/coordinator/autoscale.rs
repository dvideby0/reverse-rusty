//! `impl ClusterEngine` — the autoscaler driver: collect a [`LoadSnapshot`], run the pure
//! policy ([`evaluate`]), execute the executable subset (`rebalance`), and return the full
//! decision (incl. advisories). The policy itself lives in [`crate::cluster::autoscale`].

use crate::cluster::autoscale::{
    evaluate, AutoscaleConfig, AutoscaleDecision, LoadSnapshot, ScalingAction,
};
use crate::cluster::control::{NodeDescriptor, NodeId};
use crate::cluster::shard::ShardError;

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
        let snapshot = self.collect_load(config)?;
        let decision = evaluate(&snapshot, config);
        for action in &decision.actions {
            if let ScalingAction::Rebalance { rf } = action {
                self.rebalance(*rf)?;
            }
        }
        Ok(decision)
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
