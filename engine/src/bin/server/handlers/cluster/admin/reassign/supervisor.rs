//! Dedicated-thread dispatch and terminal reporting for reassignment workers.

#[cfg(feature = "distributed")]
use std::fmt;
#[cfg(feature = "distributed")]
use std::panic::{catch_unwind, AssertUnwindSafe};

#[cfg(feature = "distributed")]
use tracing::{error, info};

#[cfg(feature = "distributed")]
use super::{ClusterReassignWorkerOutcome, ClusterReassignWorkerResult};

#[cfg(feature = "distributed")]
#[derive(Debug)]
pub(super) struct ClusterReassignWorkerFailure;

#[cfg(feature = "distributed")]
impl fmt::Display for ClusterReassignWorkerFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("dedicated reassign worker panicked")
    }
}

/// Run the synchronous move-and-commit workflow off the Tokio runtime. The
/// worker owns admission through its exact terminal result even when the HTTP
/// receiver disconnects; shutdown joins it by acquiring the same permit.
#[cfg(feature = "distributed")]
pub(super) fn supervise_cluster_reassign_worker(
    worker: impl FnOnce() -> ClusterReassignWorkerOutcome + Send + 'static,
) -> std::io::Result<tokio::sync::oneshot::Receiver<ClusterReassignWorkerResult>> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let _worker = std::thread::Builder::new()
        .name("cluster-reassign".to_string())
        .spawn(move || {
            let outcome =
                catch_unwind(AssertUnwindSafe(worker)).map_err(|_| ClusterReassignWorkerFailure);
            let detached = sender.is_closed();
            match &outcome {
                Err(source) => error!(detached, error = %source, "reassign worker failed"),
                Ok(ClusterReassignWorkerOutcome::NotStarted) => {
                    info!(detached, "reassign was not started");
                }
                Ok(ClusterReassignWorkerOutcome::Finished(Err(source))) => {
                    error!(detached, error = %source, "reassign failed");
                }
                Ok(ClusterReassignWorkerOutcome::Finished(Ok(success))) => {
                    info!(
                        detached,
                        position = success.position(),
                        moved = success.moved(),
                        committed = success.committed(),
                        took_ms = success.took_ms,
                        "reassign completed"
                    );
                }
            }
            let _ = sender.send(outcome);
        })?;
    Ok(receiver)
}

#[cfg(all(test, feature = "distributed"))]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use reverse_rusty::cluster::ReassignOutcome;

    use super::*;
    use crate::handlers::cluster::admin::reassign::ClusterReassignSuccess;

    #[test]
    fn detached_worker_still_runs_to_terminal_completion() {
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let (finished_sender, finished_receiver) = mpsc::sync_channel(1);
        let receiver = supervise_cluster_reassign_worker(move || {
            started_sender.send(()).expect("signal worker start");
            release_receiver.recv().expect("release worker");
            finished_sender.send(()).expect("signal completion");
            ClusterReassignWorkerOutcome::Finished(Ok(ClusterReassignSuccess {
                outcome: ReassignOutcome::NoChange {
                    position: 1,
                    generation: 2,
                },
                requested_node: 3,
                took_ms: 1.0,
            }))
        })
        .expect("dispatch worker");
        started_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("worker started");
        drop(receiver);
        release_sender.send(()).expect("release worker");
        finished_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("detached worker completed");
    }
}
