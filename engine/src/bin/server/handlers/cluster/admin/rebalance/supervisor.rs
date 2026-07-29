//! Independent completion reporting for cluster-rebalance workers.

use tracing::{error, info};

use super::{
    ClusterRebalanceError, ClusterRebalanceWorkerOutcome, ClusterRebalanceWorkerResult,
    RebalanceMode,
};

pub(super) fn supervise_cluster_rebalance_worker(
    worker: tokio::task::JoinHandle<ClusterRebalanceWorkerOutcome>,
) -> tokio::sync::oneshot::Receiver<ClusterRebalanceWorkerResult> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let outcome = worker.await;
        let detached = sender.is_closed();
        match &outcome {
            Err(source) => {
                error!(detached, error = %source, "rebalance worker failed");
            }
            Ok(ClusterRebalanceWorkerOutcome::NotStarted) => {
                info!(detached, "rebalance was not started");
            }
            Ok(ClusterRebalanceWorkerOutcome::Finished(Err(ClusterRebalanceError::Request(
                source,
            )))) => {
                info!(detached, error = ?source, "rebalance request was rejected");
            }
            Ok(ClusterRebalanceWorkerOutcome::Finished(Err(ClusterRebalanceError::Backend(
                source,
            )))) => {
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
                        moved_data = matches!(success.mode, RebalanceMode::DataMoving { .. }),
                        reassigned = success.reassigned,
                        "rebalance completed"
                    );
                }
            }
        }
        // A dropped receiver is expected after an HTTP disconnect. The
        // supervisor still observes and records the terminal worker outcome.
        let _ = sender.send(outcome);
    });
    receiver
}
