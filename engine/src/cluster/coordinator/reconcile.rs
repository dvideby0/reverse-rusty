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

use super::reassign::{rebalance_targets, ReassignOutcome};
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
    /// The replication factor passed to [`ClusterEngine::reconcile`] (→ HRW `plan_assignments`). The
    /// RF=1 remote path is the only supported one today.
    pub rf: usize,
    /// Wall-clock minimum between reconcile passes — the THRASH GUARD. Each move is `O(corpus)`, so a
    /// membership-flap storm must not re-move on every edge; the loop sleeps at least this long between
    /// passes, coalescing a burst of changes into one pass. This wall-clock state lives ONLY in the
    /// driver loop — never in the engine.
    pub min_interval: Duration,
}

impl Default for ReconcileConfig {
    fn default() -> Self {
        ReconcileConfig {
            enabled: false,
            rf: 1,
            min_interval: Duration::from_secs(30),
        }
    }
}

impl ClusterEngine {
    /// Reconcile the committed shard→node map to the desired HRW placement by **moving data** (ADR-092
    /// — the unattended controller's primitive). Reads the committed state + membership, computes the
    /// positions whose **primary** diverges from the HRW-desired map ([`rebalance_targets`]), and drives
    /// the data-moving [`reassign_and_move`](Self::reassign_and_move) for each — SEQUENTIALLY in
    /// position order (the same chained-reshuffle constraint `rebalance_and_move` obeys: a node cannot be
    /// a fenced source and a recovery target at once).
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
    /// lock across moves (`reassign_serial` is taken inside each move) and never touches the hot path. A
    /// failed move leaves that position's routing + committed map untouched (a clean rollback); an
    /// uncommitted move leaves the source serving reads (the write-only fence). Re-running absorbs both
    /// (the next pass re-targets the still-diverged position and re-drives the idempotent move).
    ///
    /// Returns an empty report (a clean no-op) for an in-process / genesis cluster — no addr'd data
    /// nodes to place on, so `rebalance_targets` is empty. **RF>1 is rejected** (a replicated move would
    /// de-replicate — deferred, ADR-090). Fails closed only on a control-plane READ failure (the driver
    /// logs + retries next pass); per-position move failures land in the report, not as an `Err`.
    pub fn reconcile(&self, rf: usize, handle: &Handle) -> Result<ReconcileReport, ShardError> {
        // Data-moving reconciliation of a REPLICATED cluster is not supported (same reason as
        // reassign_and_move: a single-`RemoteShard` move would de-replicate the position). Fail fast
        // with a clear controller-level message rather than letting every per-position move reject.
        if self.replication_factor > 1 {
            return Err(ShardError::Config(format!(
                "reconcile: data-moving reconciliation of a replicated cluster \
                 (replication_factor = {}) is not yet supported (ADR-090/092); RF>1 needs the target \
                 replica group re-recovered first",
                self.replication_factor
            )));
        }

        let state = self.control_state()?;
        // Positions whose PRIMARY diverges from the HRW-desired placement (a data move), position
        // order. Empty for an in-process / genesis cluster (no addr'd data nodes) ⇒ a clean no-op.
        let targets = rebalance_targets(&state, rf);

        let mut report = ReconcileReport::default();
        for (pos, to) in targets {
            match self.reassign_and_move(pos as usize, to, handle) {
                Ok(ReassignOutcome::Moved { .. }) => report.reconciled.push(pos),
                // Resolved equal under us (a concurrent move already placed it) — not a failure.
                Ok(ReassignOutcome::NoChange { .. }) => report.skipped.push(pos),
                // Data moved, commit pending — zero-FN (source serves reads); retried next pass.
                Ok(ReassignOutcome::MovedButNotCommitted { from, .. }) => {
                    report.uncommitted.push((pos, from, to));
                }
                // A clean move failure rolled this position fully back (routing + map unchanged).
                // CONTINUE (do not abort the pass) — the next pass retries this position.
                Err(e) => report.failed.push((pos, e.to_string())),
            }
        }
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
}
