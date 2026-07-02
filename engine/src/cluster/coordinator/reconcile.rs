//! `impl ClusterEngine` — the unattended re-point reconciler (ADR-092, `distributed` feature): an
//! idempotent, data-moving controller that converges the committed shard→node map to the desired HRW
//! placement WITHOUT operator action, preserving the move-then-commit zero-FN ordering.
//!
//! Design: docs/design/clustering-and-scaling.md §9. Builds on ADR-090 (the data-moving
//! [`reassign_and_move`](ClusterEngine::reassign_and_move) /
//! [`rebalance_and_move`](ClusterEngine::rebalance_and_move) primitives) and ADR-086 (route by the
//! committed map + the boot guard).
//!
//! ## What it closes
//! [`rebalance_and_move`](ClusterEngine::rebalance_and_move) is the *manual* data-moving sweep; the
//! autoscaler's membership-drift arm historically drove the MAP-ONLY
//! [`rebalance`](ClusterEngine::rebalance) — which on a `--route-by-assignments` cluster permutes the
//! committed map WITHOUT moving data, the exact ADR-086 false negative (the boot guard then refuses a
//! map that names a node holding different data). [`reconcile`](ClusterEngine::reconcile) is the
//! steady-state watcher's primitive: it drives the SAFE data-moving path, continues past per-position
//! failures (an unattended loop makes maximum safe progress each pass and retries the rest next pass),
//! and is purely idempotent — a converged map moves nothing and commits nothing (the epoch is
//! invariant).
//!
//! ## Hysteresis (two layers, cleanly separated)
//! - **Controller idempotence (here):** a converged map ⇒ no targets ⇒ no proposals ⇒ epoch invariant.
//! - **Wall-clock min-interval (the driver loop, the server):** the thrash guard against a
//!   membership-flap storm, since each move is `O(corpus)`. That wall-clock state lives ONLY in the
//!   loop — this engine method is clock-free and thread-free.
//!
//! `distributed`-gated; the in-process/default path never compiles it and is byte-identical (an
//! in-process cluster has no addr'd data nodes, so a hypothetical call is a clean no-op anyway).

use std::time::Duration;

use tokio::runtime::Handle;

use crate::cluster::control::NodeId;
use crate::cluster::shard::ShardError;

use super::reassign::{plan_waves, rebalance_group_targets, ReassignOutcome};
use super::ClusterEngine;

/// One [`ClusterEngine::reconcile`] pass's outcome (ADR-092). Every position is independent and
/// individually consistent (each move is move-then-commit + CAS + auto-unfence), so a PARTIAL pass is
/// always a valid, resumable state — never a false negative. The driver loop logs/meters these and
/// retries the `uncommitted` + `failed` positions on its next pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Positions whose primary moved AND committed this pass.
    pub reconciled: Vec<u32>,
    /// Positions already in place (resolved equal under us — the idempotent no-op).
    pub skipped: Vec<u32>,
    /// Positions whose data moved but whose commit did not land — **zero-FN: the source still serves
    /// reads** (the write-only fence — see ADR-090's crash-window table). Recorded for observability;
    /// re-driven next pass (re-running `reassign_and_move` re-converges the already-populated target and
    /// re-commits). `(position, from, to)`.
    pub uncommitted: Vec<(u32, NodeId, NodeId)>,
    /// Positions whose move failed and rolled back cleanly (routing + committed map unchanged);
    /// retried next pass. `(position, error-message)`.
    pub failed: Vec<(u32, String)>,
}

impl ReconcileReport {
    /// Did this pass leave the map converged (nothing pending)? `true` ⇒ a steady state: every target
    /// either moved+committed or was already in place.
    pub fn is_converged(&self) -> bool {
        self.uncommitted.is_empty() && self.failed.is_empty()
    }

    /// How many positions moved AND committed this pass.
    pub fn moved_count(&self) -> usize {
        self.reconciled.len()
    }
}

/// Driver config for the unattended reconciler (ADR-092). **Default is DISABLED** — a default-config
/// server never starts the loop, so the byte-identical default path is preserved. Lives at the driver
/// layer; [`ClusterEngine::reconcile`] itself is config-free + state-driven.
#[derive(Clone, Debug)]
pub struct ReconcileConfig {
    /// Master switch. `false` (the default) ⇒ no reconcile loop runs.
    pub enabled: bool,
    /// The replication factor passed to [`ClusterEngine::reconcile`] (→ HRW `plan_assignments`).
    /// The server wires the cluster's real `replication_factor`; at rf>1 each diverging position's
    /// whole GROUP is converged via the ADR-094 group move.
    pub rf: usize,
    /// Wall-clock minimum between reconcile passes — the THRASH GUARD. Each move is `O(corpus)`, so a
    /// membership-flap storm must not re-move on every edge; the loop sleeps at least this long between
    /// passes, coalescing a burst of changes into one pass. This wall-clock state lives ONLY in the
    /// driver loop — never in the engine.
    pub min_interval: Duration,
    /// Wave parallelism for each pass's moves (ADR-095): up to this many CONFLICT-FREE moves
    /// (disjoint node footprints — see [`ClusterEngine::reconcile_with`]) run concurrently.
    /// **Default `1` = the sequential pre-ADR-095 pass, byte-identical.** Each parallel move costs
    /// one OS thread plus its own connections for the duration of an `O(corpus)` copy — size to
    /// what the mesh and the nodes' disks can absorb.
    pub max_parallel_moves: usize,
    /// Run an orphan-slot GC sweep ([`ClusterEngine::gc_orphan_slots`], ADR-096) after a pass
    /// that left the map fully CONVERGED (never while positions are uncommitted/failed — belt on
    /// top of the sweep's own keep-set). **Default `false` — no sweep ever runs, byte-identical.**
    pub gc_orphans: bool,
}

impl Default for ReconcileConfig {
    fn default() -> Self {
        ReconcileConfig {
            enabled: false,
            rf: 1,
            min_interval: Duration::from_secs(30),
            max_parallel_moves: 1,
            gc_orphans: false,
        }
    }
}

impl ClusterEngine {
    /// Reconcile the committed shard→node map to the desired HRW placement by **moving data**
    /// (ADR-092/094 — the unattended controller's primitive). Reads the committed state +
    /// membership, computes the positions whose **group** (primary or replica set) diverges from
    /// the HRW-desired map ([`rebalance_group_targets`]), and drives the data-moving move for
    /// each — sequentially in position order (= [`reconcile_with`](Self::reconcile_with) at
    /// `max_parallel_moves = 1`, the byte-identical default). Dispatch is by SHAPE: a bare→bare
    /// change runs the proven single-shard [`reassign_and_move`](Self::reassign_and_move)
    /// byte-identically; any change touching replicas runs the group-aware
    /// [`reassign_group_and_move`](Self::reassign_group_and_move) (ADR-094), so an `rf > 1`
    /// reconcile creates and converges the replica placements it plans.
    ///
    /// ## Idempotent + unattended (differs from [`rebalance_and_move`](Self::rebalance_and_move))
    /// - **Idempotent / no-thrash:** a converged map yields an empty target set ⇒ zero moves ⇒ the
    ///   committed epoch is INVARIANT. Back-to-back passes on an unchanged map commit nothing — the
    ///   controller-level hysteresis the driver loop relies on.
    /// - **Continue past per-position failures:** unlike `rebalance_and_move` (which stops on the first
    ///   failure for a human to resume), `reconcile` records a failed/uncommitted position and
    ///   CONTINUES — an unattended loop should make maximum safe progress each pass and retry the rest
    ///   next pass. Each position is independent (the committed map is per-position), and every
    ///   individual move is still move-then-commit + CAS + auto-unfence, so continuing only runs more
    ///   safe moves.
    ///
    /// ## Zero-FN
    /// `reconcile` adds only sequencing + continue-past-failure over `reassign_and_move`; it holds NO
    /// lock across moves (each move reserves its own ledger footprint) and never touches the hot path. A
    /// failed move leaves that position's routing + committed map untouched (a clean rollback); an
    /// uncommitted move leaves the source serving reads (the write-only fence). Re-running absorbs both
    /// (the next pass re-targets the still-diverged position and re-drives the idempotent move).
    ///
    /// Returns an empty report (a clean no-op) for an in-process / genesis cluster — no addr'd data
    /// nodes to place on, so [`rebalance_group_targets`] is empty. Fails closed only on a
    /// control-plane READ failure (the driver logs + retries next pass); per-position move failures
    /// land in the report, not as an `Err`.
    pub fn reconcile(&self, rf: usize, handle: &Handle) -> Result<ReconcileReport, ShardError> {
        self.reconcile_with(rf, 1, handle)
    }

    /// [`reconcile`](Self::reconcile) with wave parallelism (ADR-095): the diverged positions are
    /// partitioned into conflict-free waves (moves sharing any node serialize — the
    /// chained-reshuffle constraint: position `p`: F→T while `q`: T→U would make T a handoff
    /// target and a fenced source at once) and up to `max_parallel_moves` disjoint moves run
    /// concurrently per wave. `max_parallel_moves <= 1` is the sequential pass, byte-identical to
    /// the pre-ADR-095 reconcile. Safety never rests on the wave planner: every move still plans,
    /// reserves its own endpoint footprint in the busy-endpoint ledger, and revalidates under its
    /// ticket. The continue-past-failure semantics are per POSITION and unchanged — every wave
    /// runs, every target is attempted exactly once.
    pub fn reconcile_with(
        &self,
        rf: usize,
        max_parallel_moves: usize,
        handle: &Handle,
    ) -> Result<ReconcileReport, ShardError> {
        let state = self.control_state()?;
        // Positions whose GROUP diverges from the HRW-desired placement (a data move), position
        // order, partitioned into conflict-free waves (singletons in target order at the default
        // parallelism). Empty for an in-process / genesis cluster (no addr'd data nodes) ⇒ a
        // clean no-op.
        let targets = rebalance_group_targets(&state, rf);
        let waves = plan_waves(&state, &targets, max_parallel_moves);

        let mut report = ReconcileReport::default();
        for wave in &waves {
            for (pos, outcome) in self.execute_move_wave(&state, &targets, wave, handle) {
                match outcome {
                    Ok(ReassignOutcome::Moved { .. }) => report.reconciled.push(pos),
                    // Resolved equal under us (a concurrent move already placed it) — not a
                    // failure.
                    Ok(ReassignOutcome::NoChange { .. }) => report.skipped.push(pos),
                    // Data moved, commit pending — zero-FN (source serves reads); retried next
                    // pass.
                    Ok(ReassignOutcome::MovedButNotCommitted { from, to, .. }) => {
                        report.uncommitted.push((pos, from, to));
                    }
                    // A clean move failure rolled this position fully back (routing + map
                    // unchanged). CONTINUE (do not abort the pass) — the next pass retries it.
                    Err(e) => report.failed.push((pos, e.to_string())),
                }
            }
        }
        // Position-sorted regardless of wave completion order (a no-op at the sequential default,
        // where waves are singletons in target order).
        report.reconciled.sort_unstable();
        report.skipped.sort_unstable();
        report.uncommitted.sort_unstable_by_key(|(p, _, _)| *p);
        report.failed.sort_by_key(|(p, _)| *p);
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_disabled() {
        let cfg = ReconcileConfig::default();
        assert!(
            !cfg.enabled,
            "the reconciler is opt-in: the default config must be disabled"
        );
        assert_eq!(cfg.min_interval, Duration::from_secs(30));
        assert_eq!(cfg.rf, 1);
        assert_eq!(
            cfg.max_parallel_moves, 1,
            "the sequential default (ADR-095)"
        );
        assert!(!cfg.gc_orphans, "no GC sweep by default (ADR-096)");
    }

    #[test]
    fn empty_report_is_converged() {
        let r = ReconcileReport::default();
        assert!(
            r.is_converged(),
            "an empty pass is a converged steady state"
        );
        assert_eq!(r.moved_count(), 0);
    }

    #[test]
    fn reconciled_and_skipped_are_converged_pending_is_not() {
        let mut r = ReconcileReport {
            reconciled: vec![0, 1],
            skipped: vec![2],
            ..Default::default()
        };
        assert!(
            r.is_converged(),
            "only reconciled/skipped positions ⇒ converged"
        );
        assert_eq!(r.moved_count(), 2);

        r.uncommitted.push((3, NodeId(1), NodeId(2)));
        assert!(!r.is_converged(), "an uncommitted position ⇒ not converged");

        let mut r2 = ReconcileReport::default();
        r2.failed.push((4, "boom".into()));
        assert!(!r2.is_converged(), "a failed position ⇒ not converged");
    }

    /// The UNATTENDED semantics: `reconcile` records EVERY per-position failure and keeps going —
    /// unlike `rebalance_and_move`, which stops at the first for a human to resume. Registering
    /// addr'd data nodes on an in-process cluster makes every position a target (the committed
    /// primary is the addr-less manager `NodeId(0)` ⇒ diverged from HRW-over-data-nodes), and each
    /// move fails INSTANTLY + network-free at the source-endpoint resolution (the manager has no
    /// addr) — so all K land in `failed`, proving the loop continued past the first error rather
    /// than aborting the pass.
    #[test]
    fn reconcile_continues_past_per_position_failures() {
        use crate::cluster::control::{NodeDescriptor, NodeRole};
        use crate::cluster::coordinator::{ClusterConfig, ClusterEngine};
        use crate::normalize::Normalizer;

        let k = 3usize;
        let cfg = ClusterConfig {
            num_shards: k,
            ..ClusterConfig::default()
        };
        let queries: Vec<(u64, String)> = vec![
            (1, "+nike +shoe".into()),
            (2, "+sony +tv".into()),
            (3, "+lego +set".into()),
        ];
        let cluster =
            ClusterEngine::build(Normalizer::default_vocab().expect("vocab"), &cfg, &queries)
                .expect("in-process cluster");
        // Two addr'd data nodes (never connected — the failures below fire before any network op).
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
        let report = cluster.reconcile(1, rt.handle()).expect("reconcile pass");
        assert_eq!(
            report.failed.len(),
            k,
            "every position's failure is recorded — the pass did NOT stop at the first: {report:?}"
        );
        assert!(
            report.reconciled.is_empty()
                && report.skipped.is_empty()
                && report.uncommitted.is_empty(),
            "nothing moved or committed: {report:?}"
        );
        assert!(!report.is_converged(), "failed positions ⇒ not converged");
        for (_, msg) in &report.failed {
            assert!(
                msg.contains("has no registered endpoint"),
                "each failure is the pre-network endpoint-resolution check: {msg}"
            );
        }
    }

    /// `reconcile_with` at wave parallelism ≥ 2 keeps the UNATTENDED semantics (ADR-095): every
    /// per-position failure is recorded and the pass continues — the parallel analogue of
    /// `reconcile_continues_past_per_position_failures` (each move fails instantly + network-free
    /// at the addr-less committed primary, whatever wave it ran in).
    #[test]
    fn reconcile_with_parallel_continues_past_per_position_failures() {
        use crate::cluster::control::{NodeDescriptor, NodeRole};
        use crate::cluster::coordinator::{ClusterConfig, ClusterEngine};
        use crate::normalize::Normalizer;

        let k = 3usize;
        let queries: Vec<(u64, String)> = vec![
            (1, "+nike +shoe".into()),
            (2, "+sony +tv".into()),
            (3, "+lego +set".into()),
        ];
        let cluster = ClusterEngine::build(
            Normalizer::default_vocab().expect("vocab"),
            &ClusterConfig {
                num_shards: k,
                ..ClusterConfig::default()
            },
            &queries,
        )
        .expect("in-process cluster");
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
            .reconcile_with(1, 2, rt.handle())
            .expect("parallel reconcile pass");
        assert_eq!(
            report.failed.len(),
            k,
            "every position's failure is recorded across the waves: {report:?}"
        );
        assert!(
            report.reconciled.is_empty()
                && report.skipped.is_empty()
                && report.uncommitted.is_empty(),
            "nothing moved or committed: {report:?}"
        );
        let positions: Vec<u32> = report.failed.iter().map(|(p, _)| *p).collect();
        assert_eq!(
            positions,
            vec![0, 1, 2],
            "the report is position-sorted regardless of wave completion order"
        );
    }

    /// `reconcile_with` on an in-process cluster (no addr'd data nodes ⇒ no targets) is a clean
    /// no-op at ANY parallelism — empty report, committed epoch invariant (the byte-identical
    /// default guard at the unit level; the full in-process proof is `cluster_reconcile_oracle`).
    #[test]
    fn reconcile_with_parallel_is_clean_no_op_in_process() {
        use crate::cluster::coordinator::{ClusterConfig, ClusterEngine};
        use crate::normalize::Normalizer;

        let cluster = ClusterEngine::build(
            Normalizer::default_vocab().expect("vocab"),
            &ClusterConfig {
                num_shards: 3,
                ..ClusterConfig::default()
            },
            &[(1u64, "+nike +shoe".to_string())],
        )
        .expect("in-process cluster");
        let epoch_before = cluster.control_state().expect("state").epoch;
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        let report = cluster
            .reconcile_with(1, 4, rt.handle())
            .expect("no-op pass");
        assert_eq!(report, ReconcileReport::default(), "clean empty report");
        assert_eq!(
            cluster.control_state().expect("state").epoch,
            epoch_before,
            "a no-op pass commits nothing — the epoch is invariant"
        );
    }

    /// An `rf > 1` reconcile is no longer rejected up front (ADR-094 replaces the ADR-092-landing
    /// guard): the pass computes GROUP targets and dispatches each replicated placement to
    /// `reassign_group_and_move`. Here every group move fails cleanly + network-free (the committed
    /// primary is the addr-less manager), proving (a) rf=2 requests flow down the group path
    /// rather than erroring the controller, and (b) the continue-past-failure semantics hold at
    /// RF>1 — all K failures recorded, the pass never aborts.
    #[test]
    fn reconcile_rf2_dispatches_group_moves_and_continues_past_failures() {
        use crate::cluster::control::{NodeDescriptor, NodeRole};
        use crate::cluster::coordinator::{ClusterConfig, ClusterEngine};
        use crate::normalize::Normalizer;

        let k = 3usize;
        let queries: Vec<(u64, String)> = vec![
            (1, "+nike +shoe".into()),
            (2, "+sony +tv".into()),
            (3, "+lego +set".into()),
        ];
        let cluster = ClusterEngine::build(
            Normalizer::default_vocab().expect("vocab"),
            &ClusterConfig {
                num_shards: k,
                ..ClusterConfig::default()
            },
            &queries,
        )
        .expect("in-process cluster");
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
        let report = cluster.reconcile(2, rt.handle()).expect("rf=2 pass runs");
        assert_eq!(
            report.failed.len(),
            k,
            "every rf=2 group move fails at the addr-less committed primary and is RECORDED — \
             the pass continued past each: {report:?}"
        );
        assert!(
            report.reconciled.is_empty()
                && report.skipped.is_empty()
                && report.uncommitted.is_empty(),
            "nothing moved or committed: {report:?}"
        );
        for (_, msg) in &report.failed {
            assert!(
                msg.contains("reassign_group_and_move"),
                "an rf=2 target with replicas dispatches to the GROUP move: {msg}"
            );
        }
    }
}
