//! Dedicated-thread dispatch and independent completion reporting for raw handoff.

#[cfg(feature = "distributed")]
use std::fmt;
#[cfg(feature = "distributed")]
use std::panic::{catch_unwind, AssertUnwindSafe};

#[cfg(feature = "distributed")]
use tracing::{error, info};

#[cfg(feature = "distributed")]
use super::{ClusterHandoffWorkerOutcome, ClusterHandoffWorkerResult};

#[cfg(feature = "distributed")]
#[derive(Debug)]
pub(super) struct ClusterHandoffWorkerFailure;

#[cfg(feature = "distributed")]
impl fmt::Display for ClusterHandoffWorkerFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("dedicated handoff worker panicked")
    }
}

/// Dispatch the synchronous recovery/fence/flip workflow on an independently
/// supervised OS thread. The worker owns admission through terminal completion,
/// even if its HTTP receiver disconnects.
#[cfg(feature = "distributed")]
pub(super) fn supervise_cluster_handoff_worker(
    worker: impl FnOnce() -> ClusterHandoffWorkerOutcome + Send + 'static,
) -> std::io::Result<tokio::sync::oneshot::Receiver<ClusterHandoffWorkerResult>> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let _worker = std::thread::Builder::new()
        .name("cluster-handoff".to_string())
        .spawn(move || {
            let outcome =
                catch_unwind(AssertUnwindSafe(worker)).map_err(|_| ClusterHandoffWorkerFailure);
            let detached = sender.is_closed();
            match &outcome {
                Err(source) => error!(detached, error = %source, "handoff worker failed"),
                Ok(ClusterHandoffWorkerOutcome::NotStarted) => {
                    info!(detached, "handoff was not started");
                }
                Ok(ClusterHandoffWorkerOutcome::Finished(Err(source))) => {
                    error!(detached, error = %source, "handoff failed");
                }
                Ok(ClusterHandoffWorkerOutcome::Finished(Ok(success))) => {
                    info!(
                        detached,
                        position = success.position,
                        moved = success.outcome.moved(),
                        generation = success.outcome.generation(),
                        took_ms = success.took_ms,
                        "handoff completed"
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

    use reverse_rusty::cluster::HandoffOutcome;

    use super::*;
    use crate::handlers::cluster::admin::handoff::ClusterHandoffSuccess;

    #[test]
    fn detached_worker_still_runs_to_terminal_completion() {
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let (finished_sender, finished_receiver) = mpsc::sync_channel(1);
        let receiver = supervise_cluster_handoff_worker(move || {
            started_sender.send(()).expect("signal worker start");
            release_receiver.recv().expect("release worker");
            finished_sender.send(()).expect("signal completion");
            ClusterHandoffWorkerOutcome::Finished(Ok(ClusterHandoffSuccess {
                outcome: HandoffOutcome::Moved { generation: 2 },
                position: 1,
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
