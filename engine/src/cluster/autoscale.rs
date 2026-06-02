//! `autoscale` — the cluster's elasticity POLICY/TRIGGER layer (clustering build-path
//! step 6c / ADR-045).
//!
//! Design: docs/design/clustering-and-scaling.md §6c (build path), §8 (auto-rebalance /
//! auto-split goals).
//!
//! The mechanisms are already built — `ClusterEngine::{register_node, deregister_node,
//! rebalance}` (the HRW allocator, ADR-042) and the live data-moving handoff
//! (`execute_handoff`, ADR-043/044). What was missing is the layer that *decides when* to
//! drive them. This module is that decision: a **pure, deterministic** policy
//! ([`evaluate`]) over a [`LoadSnapshot`] (membership + the shard→node map + per-shard
//! corpus) that emits [`ScalingAction`]s, plus the thin driver on `ClusterEngine`
//! (`tick`/`on_node_joined`/`on_node_left`, in `coordinator::autoscale`) that executes the
//! executable subset and surfaces the rest as advisories.
//!
//! ## What it consumes (and why only this)
//! Only signals that cross the [`Shard`](super::shard::Shard) seam — per-shard **corpus**
//! ([`num_queries`](super::shard::Shard::num_queries)) plus the control-plane membership +
//! assignments. Richer per-shard metrics (segment count, QPS, memtable depth) are
//! `LocalShard`-only [`EngineMetrics`](crate::events::EngineMetrics) and do NOT cross the
//! wire, so the policy deliberately ignores them: keying only off seam-available signals
//! means the autoscaler behaves identically in-process and across nodes.
//!
//! ## The three rules ([`evaluate`])
//! 1. **Membership drift → [`Rebalance`](ScalingAction::Rebalance) (executable).** When the
//!    registered node set differs from the node set the assignments actually reference (a
//!    join leaves a node owning nothing; a leave leaves a stale id owning a position — the
//!    dangerous case, routing to a dead owner), recommend a rebalance. The trigger is
//!    deliberately coarse: it never recomputes the HRW placement (that keeps `evaluate` a
//!    pure function of the snapshot, with no allocator coupling), and lets the idempotent
//!    [`rebalance`](crate::cluster::ClusterEngine::rebalance) compute the exact minimal diff.
//! 2. **Per-node skew → [`Handoff`](ScalingAction::Handoff) (advisory).** A node whose
//!    primary-corpus exceeds `max_node_load_skew ×` the mean earns a recommendation to move
//!    its largest primary shard to the least-loaded node. Advisory this increment — the
//!    move mechanism (`execute_handoff`) is gRPC-gated and not driven here.
//! 3. **Per-shard corpus over threshold → [`RecommendSplit`](ScalingAction::RecommendSplit)
//!    (advisory).** Advisory only: there is no split mechanism yet (the ring's `num_shards`
//!    is fixed at construction; splitting needs ring re-keying + a `recommended_shard_count`
//!    signal — a future increment).
//!
//! ## Determinism + the no-op default
//! [`evaluate`] uses no clock and no randomness, iterates in positional/sorted order, and
//! breaks every tie deterministically — the same [`LoadSnapshot`] always yields the same
//! [`AutoscaleDecision`] (the property the unit tests pin). [`AutoscaleConfig::default`] is
//! **disabled**, so a default-config cluster's `tick` is a no-op and every pre-existing
//! oracle stays byte-identical. There is no time-based hysteresis: `rebalance` is
//! idempotent and `evaluate` is a pure function of the snapshot, so back-to-back ticks on
//! unchanged membership cannot thrash — the idempotence *is* the hysteresis.
//!
//! Dependency-free / lean core: pure computation over the already-`serde` control types, no
//! tokio, no gRPC.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::control::{NodeDescriptor, NodeId, ShardAssignment};

/// Tunable knobs for the autoscaler policy. [`Default`] is **disabled** (every field off),
/// so a cluster that never opts in is byte-identical to one with no autoscaler at all.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AutoscaleConfig {
    /// Master switch. `false` (the default) ⇒ [`evaluate`] short-circuits to an empty
    /// decision and `tick` is a no-op.
    pub enabled: bool,
    /// The replication factor the driver passes to
    /// [`rebalance`](crate::cluster::ClusterEngine::rebalance) when it emits a
    /// [`Rebalance`](ScalingAction::Rebalance). The allocator clamps it to `[1, node_count]`,
    /// so an over-large value is safe.
    pub target_replication_factor: usize,
    /// A node whose primary-corpus exceeds `max_node_load_skew ×` the mean primary-corpus
    /// earns a [`Handoff`](ScalingAction::Handoff) advisory. `≤ 1.0` disables skew detection
    /// (every node is within `1.0×` the mean by definition). Typical: `1.5`–`2.0`.
    pub max_node_load_skew: f64,
    /// A shard whose corpus exceeds this earns a [`RecommendSplit`](ScalingAction::RecommendSplit)
    /// advisory. `0` (the default) disables split detection.
    pub split_corpus_threshold: usize,
}

impl Default for AutoscaleConfig {
    fn default() -> Self {
        AutoscaleConfig {
            enabled: false,
            target_replication_factor: 1,
            max_node_load_skew: 0.0,
            split_corpus_threshold: 0,
        }
    }
}

impl AutoscaleConfig {
    /// Validate the config, returning a list of problems (empty ⇒ valid) — mirrors
    /// [`EngineConfig::validate`](crate::config::EngineConfig::validate). The driver calls
    /// this at the top of `tick` and rejects an invalid config fail-closed.
    pub fn validate(&self) -> Vec<String> {
        let mut problems = Vec::new();
        if self.target_replication_factor == 0 {
            problems.push("target_replication_factor must be >= 1".into());
        }
        if self.max_node_load_skew < 0.0 {
            problems.push("max_node_load_skew must be >= 0".into());
        }
        problems
    }
}

/// The full, deterministic input to [`evaluate`] — a snapshot the driver collects from the
/// control plane (`nodes`/`assignments`/ring params) and the shards (`shard_corpus`).
///
/// `shard_corpus[i]` is the physical query count of shard position `i`, index-aligned with
/// the ring's `0..num_shards` and with [`ShardAssignment::position`] (the same position
/// space). `num_shards`/`replication_factor` are carried for a complete, self-describing
/// snapshot; the current rules key off membership + corpus (the rf the driver acts on comes
/// from [`AutoscaleConfig::target_replication_factor`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadSnapshot {
    /// Cluster membership (every registered node).
    pub nodes: Vec<NodeDescriptor>,
    /// The committed shard→node map, one entry per position.
    pub assignments: Vec<ShardAssignment>,
    /// Per-shard physical query count, index-aligned with position `0..num_shards`.
    pub shard_corpus: Vec<usize>,
    /// Ring shard count (`shard_corpus.len()` mirrors this).
    pub num_shards: u32,
    /// The cluster's configured replication factor (context).
    pub replication_factor: usize,
}

/// One action the policy recommends. Exactly one variant is **executable** this increment
/// ([`Rebalance`](Self::Rebalance)); the rest are **advisory** — surfaced for observability
/// and executed (or built) by a later increment. Kept as one enum + [`is_executable`]
/// (rather than two enums) so the driver's "execute the executable subset" is a one-line
/// filter and the decision stays a flat, serializable list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScalingAction {
    /// EXECUTABLE: recompute + commit the shard→node map at `rf`. Drives
    /// [`rebalance`](crate::cluster::ClusterEngine::rebalance).
    Rebalance { rf: usize },
    /// ADVISORY: move shard `position` off `from` onto `to` to relieve load skew. No move is
    /// performed this increment — `execute_handoff` (ADR-044) is gRPC-gated and not driven.
    Handoff {
        position: u32,
        from: NodeId,
        to: NodeId,
    },
    /// ADVISORY: shard `position` (corpus `corpus`) crossed the split threshold. No split
    /// mechanism exists yet (ring `num_shards` is fixed at construction).
    RecommendSplit { position: u32, corpus: usize },
    /// ADVISORY: a free-form "add capacity" signal — e.g. corpus growth outpacing the node
    /// count, leaving nowhere to place new shards.
    RecommendScaleOut { reason: String },
}

impl ScalingAction {
    /// Whether the driver executes this action now (vs. surfacing it as an advisory). Only
    /// [`Rebalance`](Self::Rebalance) is executable this increment.
    pub fn is_executable(&self) -> bool {
        matches!(self, ScalingAction::Rebalance { .. })
    }
}

/// The policy's verdict: the actions to take/surface plus human-readable rationale strings
/// (explain-style, for observability — matching the project's first-class explain ethos).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AutoscaleDecision {
    pub actions: Vec<ScalingAction>,
    pub rationale: Vec<String>,
}

/// The deterministic autoscaler policy: map a [`LoadSnapshot`] + [`AutoscaleConfig`] to an
/// [`AutoscaleDecision`]. Pure — no I/O, no clock, no randomness — so the same inputs always
/// yield the same decision (oracle-tested). A disabled config returns an empty decision.
pub fn evaluate(snapshot: &LoadSnapshot, config: &AutoscaleConfig) -> AutoscaleDecision {
    let mut decision = AutoscaleDecision::default();
    if !config.enabled {
        return decision;
    }
    membership_drift(snapshot, config, &mut decision);
    node_skew(snapshot, config, &mut decision);
    corpus_split(snapshot, config, &mut decision);
    scale_out_hint(snapshot, &mut decision);
    decision
}

/// Rule 1 (executable): if the registered node set differs from the node set the assignments
/// reference, recommend a rebalance. Symmetric (`!=`) so it fires on both a join (a node owns
/// nothing yet) and a leave (a stale id still owns a position). Coarse by design — the
/// idempotent `rebalance` computes the exact diff.
fn membership_drift(
    snapshot: &LoadSnapshot,
    config: &AutoscaleConfig,
    decision: &mut AutoscaleDecision,
) {
    let member_ids: BTreeSet<u64> = snapshot.nodes.iter().map(|n| n.id.0).collect();
    let assigned_ids: BTreeSet<u64> = snapshot
        .assignments
        .iter()
        .flat_map(|a| std::iter::once(a.primary.0).chain(a.replicas.iter().map(|r| r.0)))
        .collect();
    if member_ids != assigned_ids {
        decision.actions.push(ScalingAction::Rebalance {
            rf: config.target_replication_factor,
        });
        decision.rationale.push(format!(
            "registered nodes {member_ids:?} differ from placed nodes {assigned_ids:?}; \
             rebalancing to reconcile"
        ));
    }
}

/// Rule 2 (advisory): if a node's primary-corpus exceeds `max_node_load_skew ×` the mean,
/// recommend moving its largest primary shard to the least-loaded node. At most one handoff
/// per tick (the single worst offender), fully deterministic.
fn node_skew(snapshot: &LoadSnapshot, config: &AutoscaleConfig, decision: &mut AutoscaleDecision) {
    if config.max_node_load_skew <= 1.0 {
        return;
    }
    // Per-node primary load = sum of owned primary-shard corpus (replicas are copies, not
    // independent load). Index defensively — a malformed snapshot must never panic.
    let mut node_load: BTreeMap<u64, usize> = BTreeMap::new();
    for a in &snapshot.assignments {
        if let Some(&corpus) = snapshot.shard_corpus.get(a.position as usize) {
            *node_load.entry(a.primary.0).or_default() += corpus;
        }
    }
    if node_load.len() < 2 {
        return; // need ≥ 2 loaded nodes to move load between
    }
    let total: usize = node_load.values().sum();
    if total == 0 {
        return;
    }
    let mean = total as f64 / node_load.len() as f64;
    // Most-loaded node (tie-break lowest id) and least-loaded node (tie-break lowest id).
    // `node_load` iterates ascending by id, so `reduce` keeping the strictly-better element
    // retains the first (lowest-id) among ties.
    let hot = node_load
        .iter()
        .map(|(&id, &load)| (id, load))
        .reduce(|acc, x| if x.1 > acc.1 { x } else { acc });
    let cold = node_load
        .iter()
        .map(|(&id, &load)| (id, load))
        .reduce(|acc, x| if x.1 < acc.1 { x } else { acc });
    let (Some((hot_id, hot_load)), Some((cold_id, _))) = (hot, cold) else {
        return;
    };
    if (hot_load as f64) <= config.max_node_load_skew * mean || hot_id == cold_id {
        return;
    }
    // The hot node's largest-corpus primary shard (tie-break lowest position). Assignments are
    // kept sorted by position, so `reduce` retains the first (lowest position) among ties.
    let pick = snapshot
        .assignments
        .iter()
        .filter(|a| a.primary.0 == hot_id)
        .filter_map(|a| {
            snapshot
                .shard_corpus
                .get(a.position as usize)
                .map(|&c| (a.position, c))
        })
        .reduce(|acc, x| if x.1 > acc.1 { x } else { acc });
    if let Some((position, corpus)) = pick {
        decision.actions.push(ScalingAction::Handoff {
            position,
            from: NodeId(hot_id),
            to: NodeId(cold_id),
        });
        decision.rationale.push(format!(
            "node {hot_id} primary-corpus {hot_load} exceeds {:.2}x mean {mean:.0}; \
             recommend moving shard {position} (corpus {corpus}) to node {cold_id}",
            config.max_node_load_skew
        ));
    }
}

/// Rule 3 (advisory): every shard whose corpus exceeds the split threshold earns a
/// `RecommendSplit`. Iterated in position order (deterministic); splits don't
/// cascade-interfere, so all over-threshold shards are reported.
fn corpus_split(
    snapshot: &LoadSnapshot,
    config: &AutoscaleConfig,
    decision: &mut AutoscaleDecision,
) {
    if config.split_corpus_threshold == 0 {
        return;
    }
    for (pos, &corpus) in snapshot.shard_corpus.iter().enumerate() {
        if corpus > config.split_corpus_threshold {
            decision.actions.push(ScalingAction::RecommendSplit {
                position: pos as u32,
                corpus,
            });
            decision.rationale.push(format!(
                "shard {pos} corpus {corpus} exceeds split threshold {}; recommend split \
                 (advisory only — no split mechanism this increment)",
                config.split_corpus_threshold
            ));
        }
    }
}

/// Capstone advisory: if the policy recommended a split but the cluster has fewer than two
/// nodes, there is nowhere to place new shards — recommend scaling out.
fn scale_out_hint(snapshot: &LoadSnapshot, decision: &mut AutoscaleDecision) {
    let splits = decision
        .actions
        .iter()
        .filter(|a| matches!(a, ScalingAction::RecommendSplit { .. }))
        .count();
    if splits > 0 && snapshot.nodes.len() < 2 {
        decision.actions.push(ScalingAction::RecommendScaleOut {
            reason: format!(
                "{splits} shard(s) over the split threshold but only {} node(s); \
                 add a data node",
                snapshot.nodes.len()
            ),
        });
        decision
            .rationale
            .push("corpus growth outpacing node count; recommend scaling out".into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::control::NodeRole;

    fn node(id: u64) -> NodeDescriptor {
        NodeDescriptor {
            id: NodeId(id),
            addr: None,
            role: NodeRole::Data,
        }
    }

    fn assign(position: u32, primary: u64, replicas: &[u64]) -> ShardAssignment {
        ShardAssignment {
            position,
            primary: NodeId(primary),
            replicas: replicas.iter().map(|&r| NodeId(r)).collect(),
        }
    }

    /// A snapshot whose membership matches its assignments exactly (no drift), so only the
    /// skew/split rules can fire — the base for the targeted-rule tests.
    fn balanced_snapshot() -> LoadSnapshot {
        LoadSnapshot {
            nodes: vec![node(0), node(1)],
            assignments: vec![assign(0, 0, &[1]), assign(1, 1, &[0])],
            shard_corpus: vec![100, 100],
            num_shards: 2,
            replication_factor: 2,
        }
    }

    fn enabled() -> AutoscaleConfig {
        AutoscaleConfig {
            enabled: true,
            target_replication_factor: 2,
            max_node_load_skew: 0.0,
            split_corpus_threshold: 0,
        }
    }

    fn has_rebalance(d: &AutoscaleDecision) -> bool {
        d.actions
            .iter()
            .any(|a| matches!(a, ScalingAction::Rebalance { .. }))
    }

    #[test]
    fn disabled_config_yields_an_empty_decision() {
        let snap = LoadSnapshot {
            nodes: vec![node(0), node(1), node(2)],
            assignments: vec![assign(0, 0, &[])],
            shard_corpus: vec![100],
            num_shards: 1,
            replication_factor: 1,
        };
        let d = evaluate(&snap, &AutoscaleConfig::default());
        assert!(d.actions.is_empty(), "disabled ⇒ no actions: {d:?}");
    }

    #[test]
    fn membership_growth_triggers_rebalance() {
        // 3 nodes registered, but the map still references only node 0 (a fresh join).
        let snap = LoadSnapshot {
            nodes: vec![node(0), node(1), node(2)],
            assignments: vec![assign(0, 0, &[]), assign(1, 0, &[])],
            shard_corpus: vec![50, 50],
            num_shards: 2,
            replication_factor: 1,
        };
        let d = evaluate(&snap, &enabled());
        assert!(has_rebalance(&d), "a join must trigger a rebalance: {d:?}");
        assert!(!d.rationale.is_empty());
    }

    #[test]
    fn membership_shrink_triggers_rebalance() {
        // Only node 0 is registered, but the map still references a departed node 1.
        let snap = LoadSnapshot {
            nodes: vec![node(0)],
            assignments: vec![assign(0, 0, &[1]), assign(1, 1, &[0])],
            shard_corpus: vec![50, 50],
            num_shards: 2,
            replication_factor: 1,
        };
        let d = evaluate(&snap, &enabled());
        assert!(
            has_rebalance(&d),
            "a stale departed node must trigger a rebalance: {d:?}"
        );
    }

    #[test]
    fn balanced_membership_is_a_noop() {
        let d = evaluate(&balanced_snapshot(), &enabled());
        assert!(d.actions.is_empty(), "no drift, no skew, no split: {d:?}");
    }

    #[test]
    fn node_skew_recommends_a_handoff() {
        // node 0 owns two big primaries (pos 0,1); node 1 owns one tiny primary (pos 2). No
        // membership drift (both nodes are placed), so only the skew rule can fire.
        let snap = LoadSnapshot {
            nodes: vec![node(0), node(1)],
            assignments: vec![assign(0, 0, &[]), assign(1, 0, &[]), assign(2, 1, &[])],
            shard_corpus: vec![100, 100, 10],
            num_shards: 3,
            replication_factor: 1,
        };
        let cfg = AutoscaleConfig {
            max_node_load_skew: 1.5,
            ..enabled()
        };
        let d = evaluate(&snap, &cfg);
        assert!(
            !has_rebalance(&d),
            "membership matches, no rebalance: {d:?}"
        );
        let handoff = d.actions.iter().find_map(|a| match a {
            ScalingAction::Handoff { position, from, to } => Some((*position, *from, *to)),
            _ => None,
        });
        assert_eq!(
            handoff,
            Some((0, NodeId(0), NodeId(1))),
            "move node 0's largest (lowest-position) primary to node 1: {d:?}"
        );
    }

    #[test]
    fn corpus_over_threshold_recommends_split() {
        let mut snap = balanced_snapshot();
        snap.shard_corpus = vec![100, 5000];
        let cfg = AutoscaleConfig {
            split_corpus_threshold: 1000,
            ..enabled()
        };
        let d = evaluate(&snap, &cfg);
        let split = d.actions.iter().find_map(|a| match a {
            ScalingAction::RecommendSplit { position, corpus } => Some((*position, *corpus)),
            _ => None,
        });
        assert_eq!(split, Some((1, 5000)), "shard 1 over threshold: {d:?}");
    }

    #[test]
    fn split_on_a_single_node_also_recommends_scale_out() {
        let snap = LoadSnapshot {
            nodes: vec![node(0)],
            assignments: vec![assign(0, 0, &[])],
            shard_corpus: vec![9000],
            num_shards: 1,
            replication_factor: 1,
        };
        let cfg = AutoscaleConfig {
            split_corpus_threshold: 1000,
            ..enabled()
        };
        let d = evaluate(&snap, &cfg);
        assert!(
            d.actions
                .iter()
                .any(|a| matches!(a, ScalingAction::RecommendSplit { .. })),
            "split recommended: {d:?}"
        );
        assert!(
            d.actions
                .iter()
                .any(|a| matches!(a, ScalingAction::RecommendScaleOut { .. })),
            "single node + a split ⇒ scale-out hint: {d:?}"
        );
    }

    #[test]
    fn evaluate_is_deterministic() {
        let snap = LoadSnapshot {
            nodes: vec![node(0), node(1), node(2)],
            assignments: vec![assign(0, 0, &[]), assign(1, 0, &[]), assign(2, 1, &[])],
            shard_corpus: vec![100, 100, 10],
            num_shards: 3,
            replication_factor: 1,
        };
        let cfg = AutoscaleConfig {
            max_node_load_skew: 1.2,
            split_corpus_threshold: 50,
            ..enabled()
        };
        assert_eq!(
            evaluate(&snap, &cfg),
            evaluate(&snap, &cfg),
            "same inputs ⇒ identical decision"
        );
    }

    #[test]
    fn invalid_config_is_reported() {
        let bad = AutoscaleConfig {
            target_replication_factor: 0,
            max_node_load_skew: -1.0,
            ..AutoscaleConfig::default()
        };
        assert_eq!(bad.validate().len(), 2, "both invariants flagged");
        assert!(AutoscaleConfig::default().validate().is_empty());
    }
}
