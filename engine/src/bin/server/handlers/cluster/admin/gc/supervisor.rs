//! Dedicated-thread dispatch and completion reporting for orphan-slot GC.

use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};

use tracing::{error, info, warn};

use super::{ClusterGcWorkerOutcome, ClusterGcWorkerResult};

#[derive(Debug)]
pub(super) struct ClusterGcWorkerFailure;

impl fmt::Display for ClusterGcWorkerFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("dedicated GC worker panicked")
    }
}

/// Dispatch the synchronous destructive sweep on an independent OS thread.
/// The worker owns shared maintenance admission until terminal completion, so
/// dropping the HTTP response receiver cannot cancel or orphan the sweep.
pub(super) fn supervise_cluster_gc_worker(
    worker: impl FnOnce() -> ClusterGcWorkerOutcome + Send + 'static,
) -> std::io::Result<tokio::sync::oneshot::Receiver<ClusterGcWorkerResult>> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let _worker = std::thread::Builder::new()
        .name("cluster-gc".to_string())
        .spawn(move || {
            let outcome =
                catch_unwind(AssertUnwindSafe(worker)).map_err(|_| ClusterGcWorkerFailure);
            let detached = sender.is_closed();
            match &outcome {
                Err(source) => error!(detached, error = %source, "GC worker failed"),
                Ok(ClusterGcWorkerOutcome::NotStarted) => {
                    info!(detached, "GC sweep was not started");
                }
                Ok(ClusterGcWorkerOutcome::Finished(Err(source))) => {
                    error!(detached, error = %source, "GC sweep failed");
                }
                Ok(ClusterGcWorkerOutcome::Finished(Ok(success))) => {
                    for (slot, reason) in &success.report.failed {
                        error!(
                            detached,
                            node = slot.node.0,
                            shard = slot.shard_id,
                            reason = %reason,
                            "orphan-slot GC drop failed"
                        );
                    }
                    for (node, reason) in &success.report.skipped_nodes {
                        error!(
                            detached,
                            node = node.0,
                            reason = %reason,
                            "orphan-slot GC node could not be classified"
                        );
                    }
                    for slot in &success.report.pending_disk_cleanup {
                        warn!(
                            detached,
                            node = slot.node.0,
                            shard = slot.shard_id,
                            "orphan-slot GC physical trash deletion remains pending"
                        );
                    }
                    info!(
                        detached,
                        version = success.version.0,
                        dropped = success.report.dropped.len(),
                        pending_disk_cleanup = success.report.pending_disk_cleanup.len(),
                        kept_live_routed = success.report.kept_live_routed.len(),
                        skipped_unassigned = success.report.skipped_unassigned.len(),
                        failed = success.report.failed.len(),
                        skipped_nodes = success.report.skipped_nodes.len(),
                        completed = success.report.is_complete(),
                        "orphan-slot GC sweep completed"
                    );
                }
            }
            let _ = sender.send(outcome);
        })?;
    Ok(receiver)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::time::Duration;

    use super::*;

    #[test]
    fn dropped_response_receiver_does_not_cancel_started_worker() {
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let (completed_tx, completed_rx) = std::sync::mpsc::channel();
        let worker_entered = Arc::clone(&entered);
        let worker_release = Arc::clone(&release);
        let receiver = supervise_cluster_gc_worker(move || {
            worker_entered.wait();
            worker_release.wait();
            let _ = completed_tx.send(());
            ClusterGcWorkerOutcome::NotStarted
        })
        .expect("dispatch");

        entered.wait();
        drop(receiver);
        release.wait();
        completed_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("dropping the HTTP-side receiver must not cancel GC execution");
    }
}
