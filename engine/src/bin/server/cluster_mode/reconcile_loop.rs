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
//! start of shutdown. Manual and unattended passes share one admission permit; an in-flight blocking
//! pass retains that permit after task abort, and shutdown acquires it before durability cleanup.

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
    let max_parallel_moves = cfg.max_parallel_moves.max(1);
    let gc_orphans = cfg.gc_orphans;
    tokio::spawn(async move {
        if !enabled {
            return;
        }
        info!(
            min_interval_secs = min_interval.as_secs(),
            rf,
            max_parallel_moves,
            gc_orphans,
            "reconcile loop started (ADR-092): watching the committed map for divergence"
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

            // Share one whole-cluster admission boundary with the manual REST
            // trigger. Wrapping the owned permit in an Arc lets an in-flight
            // blocking worker retain it if shutdown aborts this async loop.
            let admission = match Arc::clone(&state.reconcile_permits).acquire_owned().await {
                Ok(permit) => Arc::new(permit),
                Err(e) => {
                    warn!(error = %e, "reconcile admission closed; stopping loop");
                    return;
                }
            };

            // Run the pass OFF the async worker: `execute_handoff` does `block_on` internally, which
            // must not nest on a runtime worker thread. Holds the cluster READ guard for the pass
            // (excludes a concurrent vocab rebuild / resize `&mut self`, exactly like the manual
            // `/_cluster/reassign` handler); each move's own fence + the engine reassign guard provide
            // the rest of the concurrency safety.
            let handle = tokio::runtime::Handle::current();
            let st = Arc::clone(&state);
            let worker_admission = Arc::clone(&admission);
            let result = tokio::task::spawn_blocking(move || {
                let _admission = worker_admission;
                let _topology = st.topology_guard.read();
                let cluster = st.cluster.read();
                cluster.reconcile_with(rf, max_parallel_moves, &handle)
            })
            .await;

            match result {
                Ok(Ok(report)) => {
                    if report.reconciled_count() > 0 || !report.is_converged() {
                        info!(
                            reconciled = report.reconciled_count(),
                            skipped = report.skipped.len(),
                            uncommitted = report.uncommitted.len(),
                            failed = report.failed.len(),
                            converged = report.is_converged(),
                            "reconcile pass"
                        );
                    }
                    // Opt-in orphan-slot GC epilogue (ADR-096): only after a pass that left the
                    // map fully CONVERGED (never while positions are uncommitted/failed — belt on
                    // top of the sweep's own keep-set), on the blocking pool like the pass itself.
                    // A sweep that failed (or skipped a node) must be RETRIED next interval, so it
                    // holds the epoch cursor back exactly like an unconverged pass (codex P2: the
                    // cursor previously advanced before the epilogue, so a transient DropShard
                    // failure was never retried until an unrelated commit bumped the epoch).
                    let mut fully_done = report.is_converged();
                    if gc_orphans && report.is_converged() {
                        let handle = tokio::runtime::Handle::current();
                        let st = Arc::clone(&state);
                        let worker_admission = Arc::clone(&admission);
                        match tokio::task::spawn_blocking(move || {
                            let _admission = worker_admission;
                            let _topology = st.topology_guard.read();
                            let cluster = st.cluster.read();
                            cluster.gc_orphan_slots(&handle)
                        })
                        .await
                        {
                            Ok(Ok(gc)) => {
                                if !gc.dropped.is_empty() || !gc.is_complete() {
                                    info!(
                                        dropped = gc.dropped.len(),
                                        pending_disk_cleanup = gc.pending_disk_cleanup.len(),
                                        kept_live_routed = gc.kept_live_routed.len(),
                                        skipped_unassigned = gc.skipped_unassigned.len(),
                                        failed = gc.failed.len(),
                                        skipped_nodes = gc.skipped_nodes.len(),
                                        "orphan-slot GC sweep"
                                    );
                                }
                                if !gc.is_complete() {
                                    fully_done = false;
                                }
                            }
                            Ok(Err(e)) => {
                                warn!(error = %e, "orphan-slot GC sweep failed (retried next interval)");
                                fully_done = false;
                            }
                            Err(e) => {
                                warn!(error = %e, "orphan-slot GC task panicked (retried next interval)");
                                fully_done = false;
                            }
                        }
                    }
                    // Advance the cursor only past a pass whose FULL work (moves + the opt-in
                    // sweep) completed clean; anything pending forces a retry next interval.
                    converged_epoch = if fully_done { Some(epoch) } else { None };
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
