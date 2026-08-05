//! Dedicated-thread dispatch and independent completion reporting for resync.

use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};

use tracing::{error, info};

use super::{ClusterResyncWorkerOutcome, ClusterResyncWorkerResult};

#[derive(Debug)]
pub(super) struct ClusterResyncWorkerFailure;

impl fmt::Display for ClusterResyncWorkerFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("dedicated resync worker panicked")
    }
}

/// Dispatch synchronous repair RPCs on an independently supervised OS thread.
/// The admitted worker owns the writer/cluster guards through its terminal pass,
/// including when the HTTP request disconnects.
pub(super) fn supervise_cluster_resync_worker(
    worker: impl FnOnce() -> ClusterResyncWorkerOutcome + Send + 'static,
) -> std::io::Result<tokio::sync::oneshot::Receiver<ClusterResyncWorkerResult>> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let _worker = std::thread::Builder::new()
        .name("cluster-resync".to_string())
        .spawn(move || {
            let outcome =
                catch_unwind(AssertUnwindSafe(worker)).map_err(|_| ClusterResyncWorkerFailure);
            let detached = sender.is_closed();
            match &outcome {
                Err(source) => {
                    error!(detached, error = %source, "resync worker failed");
                }
                Ok(ClusterResyncWorkerOutcome::NotStarted) => {
                    info!(detached, "resync was not started");
                }
                Ok(ClusterResyncWorkerOutcome::Finished(success)) => {
                    info!(
                        detached,
                        repaired = success.report.repaired,
                        still_pending = success.report.still_pending,
                        took_ms = success.took_ms,
                        "cluster resync pass completed"
                    );
                }
            }
            let _ = sender.send(outcome);
        })?;
    Ok(receiver)
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use reverse_rusty::cluster::ResyncReport;

    use super::*;
    use crate::handlers::cluster::admin::resync::ClusterResyncSuccess;

    #[test]
    fn detached_worker_still_runs_to_terminal_completion() {
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let (finished_sender, finished_receiver) = mpsc::sync_channel(1);
        let receiver = supervise_cluster_resync_worker(move || {
            started_sender.send(()).expect("signal worker start");
            release_receiver.recv().expect("release worker");
            finished_sender.send(()).expect("signal completion");
            ClusterResyncWorkerOutcome::Finished(ClusterResyncSuccess {
                report: ResyncReport::default(),
                took_ms: 1.0,
            })
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
