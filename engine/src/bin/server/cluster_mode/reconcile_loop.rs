//! The unattended re-point reconcile loop (ADR-092): an opt-in background task that periodically
//! drives [`ClusterEngine::reconcile`](reverse_rusty::cluster::ClusterEngine::reconcile) so the
//! committed shard→node map converges to the desired HRW placement WITHOUT operator action — the
//! steady-state watcher complementing the autoscaler's event-driven `tick`.
//!
//! `distributed`-gated and off by default (`--reconcile-interval-secs` unset ⇒ never spawned ⇒
//! byte-identical). It lives at the SERVER layer, not the engine, so `ClusterEngine` stays thread-free
//! and clock-free: this loop owns the wall-clock min-interval (the thrash guard, since each move is
//! `O(corpus)`) and the runtime, while the engine method is a pure, idempotent state transition.
//!
//! Lifecycle: spawned before the server starts serving, and `abort`ed by `cluster_mode::run` at the
//! start of the shutdown sequence (before the durability flush, so a pass never starts racing the
//! checkpoint). A pass already in flight on the blocking pool finishes its current move-then-commit
//! safely (the handoff is designed to run under concurrent flushes, ADR-044) and is not interrupted.

use std::sync::Arc;

use tracing::{info, warn};

use reverse_rusty::cluster::ReconcileConfig;

use crate::state::ClusterAppState;

/// Spawn the reconcile loop, returning its task handle (the caller `abort`s it at shutdown). The loop
/// is infinite by design — it runs until aborted. A disabled config returns immediately (defensive;
/// `run` only spawns it when `--reconcile-interval-secs` is set).
pub(crate) fn spawn_reconcile_loop(
    state: Arc<ClusterAppState>,
    cfg: &ReconcileConfig,
) -> tokio::task::JoinHandle<()> {
    // Copy the (all-Copy) config fields out so the spawned `'static` task captures plain values, not a
    // borrow — the loop never needs the struct itself.
    let enabled = cfg.enabled;
    let rf = cfg.rf;
    let min_interval = cfg.min_interval;
    tokio::spawn(async move {
        if !enabled {
            return;
        }
        info!(
            min_interval_secs = min_interval.as_secs(),
            rf, "reconcile loop started (ADR-092): watching the committed map for divergence"
        );

        // Epoch cursor: the committed version as of the last FULLY-CONVERGED pass. Polling it (a cheap
        // read, no document clone) lets us skip a pass when nothing changed since we last converged.
        // It is purely a cost optimization — the pass is idempotent, so correctness never depends on it
        // — and it is deliberately NOT advanced past a pass that left work pending (`uncommitted` /
        // `failed`), so a transient failure is retried on the very next interval rather than starved
        // until some other change bumps the epoch.
        let mut converged_epoch: Option<u64> = None;

        loop {
            tokio::time::sleep(min_interval).await;

            let epoch = {
                let cluster = state.cluster.read();
                match cluster.control_version() {
                    Ok(v) => v.0,
                    Err(e) => {
                        warn!(error = %e, "reconcile: control-plane version read failed; retrying next interval");
                        continue;
                    }
                }
            };
            if converged_epoch == Some(epoch) {
                continue; // nothing committed since the last fully-converged pass
            }

            // Run the pass OFF the async worker: `execute_handoff` does `block_on` internally, which
            // must not nest on a runtime worker thread. Holds the cluster READ guard for the pass
            // (excludes a concurrent vocab rebuild / resize `&mut self`, exactly like the manual
            // `/_cluster/reassign` handler); each move's own fence + the engine reassign guard provide
            // the rest of the concurrency safety.
            let handle = tokio::runtime::Handle::current();
            let st = Arc::clone(&state);
            let result = tokio::task::spawn_blocking(move || {
                let cluster = st.cluster.read();
                cluster.reconcile(rf, &handle)
            })
            .await;

            match result {
                Ok(Ok(report)) => {
                    converged_epoch = if report.is_converged() {
                        Some(epoch)
                    } else {
                        // Work remains (uncommitted / failed positions) — force a retry next interval.
                        None
                    };
                    if report.moved_count() > 0 || !report.is_converged() {
                        info!(
                            reconciled = report.moved_count(),
                            skipped = report.skipped.len(),
                            uncommitted = report.uncommitted.len(),
                            failed = report.failed.len(),
                            converged = report.is_converged(),
                            "reconcile pass"
                        );
                    }
                }
                Ok(Err(e)) => {
                    warn!(error = %e, "reconcile pass failed (will retry next interval)");
                    converged_epoch = None;
                }
                Err(e) => {
                    warn!(error = %e, "reconcile task panicked (will retry next interval)");
                    converged_epoch = None;
                }
            }
        }
    })
}
