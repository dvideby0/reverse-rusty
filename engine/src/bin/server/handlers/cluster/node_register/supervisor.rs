//! Independent completion reporting for node-registration workers.

use tracing::{error, info};

use super::{ClusterNodeRegisterWorkerOutcome, ClusterNodeRegisterWorkerResult};

pub(super) fn supervise_cluster_node_register_worker(
    node_id: u64,
    worker: tokio::task::JoinHandle<ClusterNodeRegisterWorkerOutcome>,
) -> tokio::sync::oneshot::Receiver<ClusterNodeRegisterWorkerResult> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let outcome = worker.await;
        let detached = sender.is_closed();
        match &outcome {
            Err(source) => {
                error!(node_id, detached, error = %source, "node-registration worker failed");
            }
            Ok(ClusterNodeRegisterWorkerOutcome::NotStarted) => {
                info!(
                    node_id,
                    detached, "node-registration proposal was not started"
                );
            }
            Ok(ClusterNodeRegisterWorkerOutcome::Finished(Err(source))) => {
                error!(node_id, detached, error = %source, "node registration failed");
            }
            Ok(ClusterNodeRegisterWorkerOutcome::Finished(Ok(version))) => {
                info!(
                    node_id,
                    detached,
                    version = version.0,
                    "node registration committed"
                );
            }
        }
        // A dropped receiver is expected after a timeout or disconnect. This
        // supervisor still observes and records the worker's final outcome.
        let _ = sender.send(outcome);
    });
    receiver
}
