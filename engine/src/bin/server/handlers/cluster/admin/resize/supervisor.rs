//! Dedicated-thread dispatch and independent completion reporting for resize.

use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};

use tracing::{error, info};

use super::{ClusterResizeWorkerOutcome, ClusterResizeWorkerResult};

#[derive(Debug)]
pub(super) struct ClusterResizeWorkerFailure;

impl fmt::Display for ClusterResizeWorkerFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("dedicated resize worker panicked")
    }
}

/// Dispatch the synchronous `O(corpus)` rebuild on one independently supervised
/// OS thread. The admitted worker owns every safety-sensitive guard through its
/// terminal state even when the HTTP request disconnects.
pub(super) fn supervise_cluster_resize_worker(
    worker: impl FnOnce() -> ClusterResizeWorkerOutcome + Send + 'static,
) -> std::io::Result<tokio::sync::oneshot::Receiver<ClusterResizeWorkerResult>> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let _worker = std::thread::Builder::new()
        .name("cluster-resize".to_string())
        .spawn(move || {
            let outcome =
                catch_unwind(AssertUnwindSafe(worker)).map_err(|_| ClusterResizeWorkerFailure);
            let detached = sender.is_closed();
            match &outcome {
                Err(source) => {
                    error!(detached, error = %source, "resize worker failed");
                }
                Ok(ClusterResizeWorkerOutcome::NotStarted) => {
                    info!(detached, "resize was not started");
                }
                Ok(ClusterResizeWorkerOutcome::Finished(Err(source))) => {
                    error!(detached, error = %source, "resize failed");
                }
                Ok(ClusterResizeWorkerOutcome::Finished(Ok(success))) => {
                    info!(
                        detached,
                        old_num_shards = success.old_num_shards,
                        num_shards = success.num_shards,
                        rebuilt = success.rebuilt,
                        version = success.version,
                        "cluster resize completed"
                    );
                }
            }
            let _ = sender.send(outcome);
        })?;
    Ok(receiver)
}
