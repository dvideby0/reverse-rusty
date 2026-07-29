//! Independent completion reporting for node-deregistration workers.

use tracing::{error, info};

use super::{
    ClusterNodeDeregisterError, ClusterNodeDeregisterWorkerOutcome,
    ClusterNodeDeregisterWorkerResult,
};

pub(super) fn supervise_cluster_node_deregister_worker(
    node_id: u64,
    worker: tokio::task::JoinHandle<ClusterNodeDeregisterWorkerOutcome>,
) -> tokio::sync::oneshot::Receiver<ClusterNodeDeregisterWorkerResult> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let outcome = worker.await;
        let detached = sender.is_closed();
        match &outcome {
            Err(source) => {
                error!(node_id, detached, error = %source, "node-deregistration worker failed");
            }
            Ok(ClusterNodeDeregisterWorkerOutcome::NotStarted) => {
                info!(
                    node_id,
                    detached, "node-deregistration proposal was not started"
                );
            }
            Ok(ClusterNodeDeregisterWorkerOutcome::Finished(Err(
                ClusterNodeDeregisterError::NodeInUse {
                    voter,
                    assignment_count,
                },
            ))) => {
                info!(
                    node_id,
                    detached,
                    voter,
                    assignment_count,
                    "node deregistration rejected because the node remains in use"
                );
            }
            Ok(ClusterNodeDeregisterWorkerOutcome::Finished(Err(
                ClusterNodeDeregisterError::Backend(source),
            ))) => {
                error!(node_id, detached, error = %source, "node deregistration failed");
            }
            Ok(ClusterNodeDeregisterWorkerOutcome::Finished(Ok(version))) => {
                info!(
                    node_id,
                    detached,
                    version = version.0,
                    "node deregistration committed"
                );
            }
        }
        // A dropped receiver is expected after a timeout or disconnect. This
        // supervisor still observes and records the worker's final outcome.
        let _ = sender.send(outcome);
    });
    receiver
}
