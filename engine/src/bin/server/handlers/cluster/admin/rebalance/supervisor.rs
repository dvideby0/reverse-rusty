//! Dedicated-thread dispatch and independent completion reporting for
//! cluster-rebalance workers.

use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};

use tracing::{error, info};

use super::{ClusterRebalanceWorkerOutcome, ClusterRebalanceWorkerResult};

#[derive(Debug)]
pub(super) struct ClusterRebalanceWorkerFailure;

impl fmt::Display for ClusterRebalanceWorkerFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("dedicated rebalance worker panicked")
    }
}

/// Dispatch the long-running synchronous workflow on its own OS thread.
///
/// Rebalances are operator-triggered and separately single-admitted, so a
/// dedicated thread avoids both blocking Tokio and making a zero manager
/// timeout depend on the shared blocking pool's scheduler. Dropping the join
/// handle detaches the thread intentionally: the completion channel and logs
/// remain live after an HTTP disconnect. The worker owns the sole rebalance
/// permit until its safety-sensitive work finishes; coordinator shutdown
/// acquires and retains that permit before durability cleanup and process exit.
pub(super) fn supervise_cluster_rebalance_worker(
    worker: impl FnOnce() -> ClusterRebalanceWorkerOutcome + Send + 'static,
) -> std::io::Result<tokio::sync::oneshot::Receiver<ClusterRebalanceWorkerResult>> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let _worker = std::thread::Builder::new()
        .name("cluster-rebalance".to_string())
        .spawn(move || {
            let outcome =
                catch_unwind(AssertUnwindSafe(worker)).map_err(|_| ClusterRebalanceWorkerFailure);
            let detached = sender.is_closed();
            match &outcome {
                Err(source) => {
                    error!(detached, error = %source, "rebalance worker failed");
                }
                Ok(ClusterRebalanceWorkerOutcome::NotStarted) => {
                    info!(detached, "rebalance was not started");
                }
                Ok(ClusterRebalanceWorkerOutcome::Finished(Err(source))) => {
                    error!(detached, error = %source, "rebalance failed");
                }
                Ok(ClusterRebalanceWorkerOutcome::Finished(Ok(success))) => {
                    if let Some(failure) = &success.failed {
                        error!(
                            detached,
                            version = success.version.0,
                            position = failure.position,
                            reason = %failure.reason,
                            moved = success.moved.len(),
                            not_attempted = success.not_attempted.len(),
                            "data-moving rebalance stopped and is resumable"
                        );
                    } else {
                        info!(
                            detached,
                            version = success.version.0,
                            moved_data = success.mode.moves_data(),
                            reassigned = success.reassigned,
                            "rebalance completed"
                        );
                    }
                }
            }
            // A dropped receiver is expected after an HTTP disconnect. The
            // supervisor still observes and records the terminal worker outcome.
            let _ = sender.send(outcome);
        })?;
    Ok(receiver)
}
