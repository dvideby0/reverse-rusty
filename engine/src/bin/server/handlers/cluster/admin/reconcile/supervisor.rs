//! Dedicated-thread dispatch and completion reporting for reconcile workers.

use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};

use tracing::{error, info};

use super::{ClusterReconcileWorkerOutcome, ClusterReconcileWorkerResult};

#[derive(Debug)]
pub(super) struct ClusterReconcileWorkerFailure;

impl fmt::Display for ClusterReconcileWorkerFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("dedicated reconcile worker panicked")
    }
}

/// Dispatch the synchronous, corpus-wide pass on a dedicated OS thread. The
/// worker owns admission until completion, so an HTTP disconnect detaches only
/// the response and coordinator shutdown can still join the live operation.
pub(super) fn supervise_cluster_reconcile_worker(
    worker: impl FnOnce() -> ClusterReconcileWorkerOutcome + Send + 'static,
) -> std::io::Result<tokio::sync::oneshot::Receiver<ClusterReconcileWorkerResult>> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let _worker = std::thread::Builder::new()
        .name("cluster-reconcile".to_string())
        .spawn(move || {
            let outcome =
                catch_unwind(AssertUnwindSafe(worker)).map_err(|_| ClusterReconcileWorkerFailure);
            let detached = sender.is_closed();
            match &outcome {
                Err(source) => error!(detached, error = %source, "reconcile worker failed"),
                Ok(ClusterReconcileWorkerOutcome::NotStarted) => {
                    info!(detached, "reconcile was not started");
                }
                Ok(ClusterReconcileWorkerOutcome::Finished(Err(source))) => {
                    error!(detached, error = %source, "reconcile failed");
                }
                Ok(ClusterReconcileWorkerOutcome::Finished(Ok(success))) => {
                    for (position, reason) in &success.report.failed {
                        error!(
                            detached,
                            position,
                            reason = %reason,
                            "reconcile position failed"
                        );
                    }
                    info!(
                        detached,
                        version = success.version.0,
                        reconciled = success.report.reconciled.len(),
                        skipped = success.report.skipped.len(),
                        uncommitted = success.report.uncommitted.len(),
                        failed = success.report.failed.len(),
                        converged = success.report.is_converged(),
                        "reconcile pass completed"
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
        let receiver = supervise_cluster_reconcile_worker(move || {
            worker_entered.wait();
            worker_release.wait();
            let _ = completed_tx.send(());
            ClusterReconcileWorkerOutcome::NotStarted
        })
        .expect("dispatch");

        entered.wait();
        drop(receiver);
        release.wait();
        completed_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("dropping the HTTP-side receiver must not cancel worker execution");
    }
}
