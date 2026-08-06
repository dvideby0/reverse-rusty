//! Strict native `POST /_cluster/gc` orphan-slot cleanup boundary.
//!
//! ES/OS dangling deletion targets one named index UUID after a data-loss acknowledgement. Reverse
//! Rusty instead proves each slot is outside durable and live routing before dropping it, so only
//! the manager-timeout spellings map to this native path.

#[cfg(feature = "distributed")]
use axum::Json;
use axum::{
    extract::State,
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
#[cfg(feature = "distributed")]
use parking_lot::Mutex;
#[cfg(feature = "distributed")]
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
#[cfg(feature = "distributed")]
use std::time::Instant;
#[cfg(feature = "distributed")]
use tokio::sync::TryAcquireError;
#[cfg(feature = "distributed")]
use tracing::{error, instrument, warn};

#[cfg(feature = "distributed")]
use reverse_rusty::cluster::{GcReport, OrphanSlot, ShardError, StateVersion};

use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::ClusterAppState;
#[cfg(feature = "distributed")]
use crate::state::ClusterRebalanceTopology;

#[cfg(not(feature = "distributed"))]
use super::super::not_in_cluster_mode;
#[cfg(feature = "distributed")]
use super::super::shard_error_status;

#[cfg(feature = "distributed")]
mod supervisor;
mod transport;

#[cfg(feature = "distributed")]
use supervisor::{supervise_cluster_gc_worker, ClusterGcWorkerFailure};
use transport::ClusterGcTransport;

pub(crate) const CLUSTER_GC_BODY_LIMIT: usize = 64 * 1024;
pub(crate) const CLUSTER_GC_BODY_TIMEOUT: Duration = Duration::from_millis(250);
const DEFAULT_CLUSTER_GC_MANAGER_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CLUSTER_GC_MANAGER_TIMEOUT: Duration = Duration::from_secs(30);
const CLUSTER_GC_ENDPOINT: &str = "cluster_gc";

#[cfg(feature = "distributed")]
#[derive(Clone, Copy)]
enum GcStart {
    Queued,
    Started,
    Cancelled,
}

#[cfg(feature = "distributed")]
fn begin_cluster_gc(gate: &Mutex<GcStart>, deadline: Instant, no_wait: bool) -> bool {
    let mut start = gate.lock();
    if matches!(*start, GcStart::Cancelled) || (!no_wait && Instant::now() >= deadline) {
        *start = GcStart::Cancelled;
        return false;
    }
    *start = GcStart::Started;
    true
}

#[cfg(feature = "distributed")]
fn cancel_queued_cluster_gc(gate: &Mutex<GcStart>) -> bool {
    let mut start = gate.lock();
    match *start {
        GcStart::Queued | GcStart::Cancelled => {
            *start = GcStart::Cancelled;
            true
        }
        GcStart::Started => false,
    }
}

#[cfg(feature = "distributed")]
struct CancelQueuedClusterGc(Arc<Mutex<GcStart>>);

#[cfg(feature = "distributed")]
impl Drop for CancelQueuedClusterGc {
    fn drop(&mut self) {
        // Disconnect cancels only pre-start work. A started destructive sweep
        // runs to its terminal report while retaining maintenance admission.
        let _ = cancel_queued_cluster_gc(&self.0);
    }
}

#[cfg(feature = "distributed")]
struct ClusterGcSuccess {
    version: StateVersion,
    report: GcReport,
}

#[cfg(feature = "distributed")]
enum ClusterGcWorkerOutcome {
    NotStarted,
    Finished(Result<ClusterGcSuccess, ShardError>),
}

#[cfg(feature = "distributed")]
type ClusterGcWorkerResult = Result<ClusterGcWorkerOutcome, ClusterGcWorkerFailure>;

#[cfg(feature = "distributed")]
#[derive(Serialize)]
struct GcSlotResponse {
    node: u64,
    shard: u32,
    num_queries: u64,
}

#[cfg(feature = "distributed")]
impl From<OrphanSlot> for GcSlotResponse {
    fn from(slot: OrphanSlot) -> Self {
        Self {
            node: slot.node.0,
            shard: slot.shard_id,
            num_queries: slot.num_queries,
        }
    }
}

#[cfg(feature = "distributed")]
#[derive(Serialize)]
struct GcPendingDiskCleanupResponse {
    #[serde(flatten)]
    slot: GcSlotResponse,
    warning: &'static str,
}

#[cfg(feature = "distributed")]
#[derive(Serialize)]
struct GcSkippedUnassignedResponse {
    #[serde(flatten)]
    slot: GcSlotResponse,
    warning: &'static str,
}

#[cfg(feature = "distributed")]
#[derive(Serialize)]
struct GcFailureResponse {
    #[serde(flatten)]
    slot: GcSlotResponse,
    reason: &'static str,
}

#[cfg(feature = "distributed")]
#[derive(Serialize)]
struct GcSkippedNodeResponse {
    node: u64,
    reason: &'static str,
}

#[cfg(feature = "distributed")]
#[derive(Serialize)]
struct ClusterGcResponse {
    acknowledged: bool,
    completed: bool,
    version: u64,
    took: u64,
    took_ms: f64,
    dropped: Vec<GcSlotResponse>,
    pending_disk_cleanup: Vec<GcPendingDiskCleanupResponse>,
    kept_live_routed: Vec<GcSlotResponse>,
    skipped_unassigned: Vec<GcSkippedUnassignedResponse>,
    failed: Vec<GcFailureResponse>,
    skipped_nodes: Vec<GcSkippedNodeResponse>,
}

/// Run one guarded orphan-slot sweep. Assignment-routed remote assembly is
/// required so the committed node directory is complete; unlike placement
/// reconciliation, a CLI-seeded topology is safe because GC never changes the
/// assignment map and the live-routing keep set protects its current backings.
#[cfg(feature = "distributed")]
#[instrument(skip_all)]
pub(crate) async fn cluster_gc(
    State(state): State<Arc<ClusterAppState>>,
    transport: ClusterGcTransport,
) -> Response {
    let (_duration, manager_timeout) = transport.into_parts();
    if let Some(response) = reject_gc_topology(&state.prom, state.rebalance_topology) {
        return response;
    }

    let started_at = Instant::now();
    let no_wait = manager_timeout.is_zero();
    let Some(deadline) = started_at.checked_add(manager_timeout) else {
        return cluster_gc_rejection(
            &state.prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            "GC manager timeout is too large for this platform",
        );
    };
    // Manual GC shares the reconcile slot. The unattended reconcile+GC path
    // already owns this permit through its epilogue, and shutdown joins it.
    let permit = if no_wait {
        match Arc::clone(&state.reconcile_permits).try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => return cluster_gc_not_started_timeout(&state.prom),
            Err(TryAcquireError::Closed) => {
                return cluster_gc_rejection(
                    &state.prom,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "gc_unavailable",
                    "GC admission is closed",
                )
            }
        }
    } else {
        let Some(admission_budget) = deadline.checked_duration_since(Instant::now()) else {
            return cluster_gc_not_started_timeout(&state.prom);
        };
        match tokio::time::timeout(
            admission_budget,
            Arc::clone(&state.reconcile_permits).acquire_owned(),
        )
        .await
        {
            Err(_) => return cluster_gc_not_started_timeout(&state.prom),
            Ok(Err(_)) => {
                return cluster_gc_rejection(
                    &state.prom,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "gc_unavailable",
                    "GC admission is closed",
                )
            }
            Ok(Ok(permit)) => permit,
        }
    };
    if !no_wait && Instant::now() >= deadline {
        return cluster_gc_not_started_timeout(&state.prom);
    }

    let worker_state = Arc::clone(&state);
    let gate = Arc::new(Mutex::new(GcStart::Queued));
    let _cancel_queued_on_drop = CancelQueuedClusterGc(Arc::clone(&gate));
    let worker_gate = Arc::clone(&gate);
    let (started_sender, mut started_receiver) = tokio::sync::oneshot::channel();
    let handle = tokio::runtime::Handle::current();
    let completion = match supervise_cluster_gc_worker(move || {
        let _permit = permit;
        let topology = if no_wait {
            worker_state.topology_guard.try_read()
        } else {
            deadline
                .checked_duration_since(Instant::now())
                .and_then(|budget| worker_state.topology_guard.try_read_for(budget))
        };
        let Some(_topology) = topology else {
            return ClusterGcWorkerOutcome::NotStarted;
        };
        let cluster = if no_wait {
            worker_state.cluster.try_read()
        } else {
            deadline
                .checked_duration_since(Instant::now())
                .and_then(|budget| worker_state.cluster.try_read_for(budget))
        };
        let Some(cluster) = cluster else {
            return ClusterGcWorkerOutcome::NotStarted;
        };
        if !begin_cluster_gc(&worker_gate, deadline, no_wait) {
            return ClusterGcWorkerOutcome::NotStarted;
        }
        let _ = started_sender.send(());
        ClusterGcWorkerOutcome::Finished(execute_cluster_gc(&cluster, &handle))
    }) {
        Ok(completion) => completion,
        Err(source) => {
            error!(error = %source, "failed to dispatch dedicated GC worker");
            return cluster_gc_rejection(
                &state.prom,
                StatusCode::SERVICE_UNAVAILABLE,
                "gc_unavailable",
                "GC worker could not be started",
            );
        }
    };
    let mut completion = completion;

    if no_wait {
        return match completion.await {
            Ok(outcome) => finish_cluster_gc_worker(&state.prom, started_at, outcome),
            Err(source) => cluster_gc_supervisor_failed(&state.prom, &source),
        };
    }

    let sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
    tokio::pin!(sleep);
    tokio::select! {
        outcome = &mut completion => match outcome {
            Ok(outcome) => finish_cluster_gc_worker(&state.prom, started_at, outcome),
            Err(source) => cluster_gc_supervisor_failed(&state.prom, &source),
        },
        started = &mut started_receiver => {
            if started.is_err() {
                warn!("GC worker ended without sending its start signal");
            }
            match completion.await {
                Ok(outcome) => finish_cluster_gc_worker(&state.prom, started_at, outcome),
                Err(source) => cluster_gc_supervisor_failed(&state.prom, &source),
            }
        },
        () = &mut sleep => {
            if cancel_queued_cluster_gc(&gate) {
                cluster_gc_not_started_timeout(&state.prom)
            } else {
                match completion.await {
                    Ok(outcome) => finish_cluster_gc_worker(&state.prom, started_at, outcome),
                    Err(source) => cluster_gc_supervisor_failed(&state.prom, &source),
                }
            }
        }
    }
}

#[cfg(feature = "distributed")]
fn execute_cluster_gc(
    cluster: &reverse_rusty::cluster::ClusterEngine,
    handle: &tokio::runtime::Handle,
) -> Result<ClusterGcSuccess, ShardError> {
    let report = cluster.gc_orphan_slots(handle)?;
    let version = cluster.control_version()?;
    Ok(ClusterGcSuccess { version, report })
}

#[cfg(feature = "distributed")]
fn finish_cluster_gc_worker(
    prom: &PrometheusMetrics,
    started_at: Instant,
    outcome: ClusterGcWorkerResult,
) -> Response {
    match outcome {
        Err(_) => cluster_gc_rejection(
            prom,
            StatusCode::INTERNAL_SERVER_ERROR,
            "gc_unavailable",
            "GC worker failed",
        ),
        Ok(ClusterGcWorkerOutcome::NotStarted) => cluster_gc_not_started_timeout(prom),
        Ok(ClusterGcWorkerOutcome::Finished(Err(source))) => {
            let status = shard_error_status(&source);
            let status = if status.is_success() {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                status
            };
            let (_, error_type) = source.write_http_class();
            cluster_gc_rejection(
                prom,
                status,
                error_type,
                "GC did not produce an attested terminal report; inspect server logs and \
                 /_cluster/state before retrying",
            )
        }
        Ok(ClusterGcWorkerOutcome::Finished(Ok(success))) => {
            let took_ms = started_at.elapsed().as_secs_f64() * 1_000.0;
            let completed = success.report.is_complete();
            let pending_disk_cleanup = success
                .report
                .pending_disk_cleanup
                .into_iter()
                .map(|slot| GcPendingDiskCleanupResponse {
                    slot: slot.into(),
                    warning: "the slot left the serving namespace but physical trash deletion is \
                              pending; a later sweep or node restart will retry it",
                })
                .collect();
            let skipped_unassigned = success
                .report
                .skipped_unassigned
                .into_iter()
                .map(|slot| GcSkippedUnassignedResponse {
                    slot: slot.into(),
                    warning: "the committed map has no assignment for this position, so the slot \
                              was kept fail-safe",
                })
                .collect();
            let failed = success
                .report
                .failed
                .into_iter()
                .map(|(slot, _)| GcFailureResponse {
                    slot: slot.into(),
                    reason: "this orphan slot was not reclaimed; inspect server logs and retry \
                             the idempotent GC sweep",
                })
                .collect();
            let skipped_nodes = success
                .report
                .skipped_nodes
                .into_iter()
                .map(|(node, _)| GcSkippedNodeResponse {
                    node: node.0,
                    reason: "this node could not be classified; inspect server logs and retry the \
                             idempotent GC sweep",
                })
                .collect();
            finish_cluster_gc_response(
                prom,
                Json(ClusterGcResponse {
                    acknowledged: completed,
                    completed,
                    version: success.version.0,
                    took: took_ms.floor() as u64,
                    took_ms,
                    dropped: success.report.dropped.into_iter().map(Into::into).collect(),
                    pending_disk_cleanup,
                    kept_live_routed: success
                        .report
                        .kept_live_routed
                        .into_iter()
                        .map(Into::into)
                        .collect(),
                    skipped_unassigned,
                    failed,
                    skipped_nodes,
                })
                .into_response(),
            )
        }
    }
}

#[cfg(feature = "distributed")]
fn reject_gc_topology(
    prom: &PrometheusMetrics,
    topology: ClusterRebalanceTopology,
) -> Option<Response> {
    match topology {
        ClusterRebalanceTopology::ResolveOnlyRemote
        | ClusterRebalanceTopology::CliSeededAssignmentRemote => None,
        ClusterRebalanceTopology::StaticRemote => Some(cluster_gc_rejection(
            prom,
            StatusCode::CONFLICT,
            "gc_assignment_routing_required",
            "a static remote coordinator has no authoritative committed node directory for a \
             whole-cluster GC sweep; restart with --route-by-assignments and --control-endpoint \
             before retrying",
        )),
        ClusterRebalanceTopology::InProcess => Some(cluster_gc_rejection(
            prom,
            StatusCode::BAD_REQUEST,
            "gc_requires_remote_cluster",
            "orphan-slot GC is a remote-node cleanup workflow and is not applicable to an \
             in-process cluster",
        )),
    }
}

#[cfg(feature = "distributed")]
fn cluster_gc_supervisor_failed(
    prom: &PrometheusMetrics,
    source: &tokio::sync::oneshot::error::RecvError,
) -> Response {
    error!(error = %source, "GC completion supervisor failed");
    cluster_gc_rejection(
        prom,
        StatusCode::INTERNAL_SERVER_ERROR,
        "gc_unavailable",
        "GC completion supervisor failed",
    )
}

#[cfg(feature = "distributed")]
fn cluster_gc_not_started_timeout(prom: &PrometheusMetrics) -> Response {
    cluster_gc_rejection(
        prom,
        StatusCode::REQUEST_TIMEOUT,
        "gc_timeout",
        "timed out waiting for GC admission or topology access; no GC sweep was started",
    )
}

fn cluster_gc_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    finish_cluster_gc_response(
        prom,
        ApiError::response(status, error_type, reason).into_response(),
    )
}

fn finish_cluster_gc_response(prom: &PrometheusMetrics, mut response: Response) -> Response {
    prom.http_requests_total
        .with_label_values(&[CLUSTER_GC_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

#[cfg(not(feature = "distributed"))]
pub(crate) async fn cluster_gc(
    State(state): State<Arc<ClusterAppState>>,
    transport: ClusterGcTransport,
) -> Response {
    let (_duration, _manager_timeout) = transport.into_parts();
    finish_cluster_gc_response(
        &state.prom,
        not_in_cluster_mode(
            "POST /_cluster/gc",
            "the orphan-slot GC sweep needs the gRPC transport — rebuild the server with \
             --features distributed",
        ),
    )
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "distributed")]
    use super::*;

    #[cfg(feature = "distributed")]
    #[test]
    fn assignment_routed_remote_topologies_are_accepted() {
        let prom = PrometheusMetrics::new();
        for topology in [
            ClusterRebalanceTopology::ResolveOnlyRemote,
            ClusterRebalanceTopology::CliSeededAssignmentRemote,
        ] {
            assert!(reject_gc_topology(&prom, topology).is_none());
        }
        for topology in [
            ClusterRebalanceTopology::InProcess,
            ClusterRebalanceTopology::StaticRemote,
        ] {
            assert!(reject_gc_topology(&prom, topology).is_some());
        }
    }

    #[cfg(feature = "distributed")]
    #[test]
    fn queued_cancellation_cannot_start_later_but_started_work_cannot_be_cancelled() {
        let queued = Mutex::new(GcStart::Queued);
        assert!(cancel_queued_cluster_gc(&queued));
        assert!(!begin_cluster_gc(&queued, Instant::now(), true));

        let started = Mutex::new(GcStart::Queued);
        assert!(begin_cluster_gc(&started, Instant::now(), true));
        assert!(!cancel_queued_cluster_gc(&started));
    }

    #[cfg(feature = "distributed")]
    #[tokio::test]
    async fn partial_response_sanitizes_internal_failure_details() {
        use reverse_rusty::cluster::NodeId;

        let prom = PrometheusMetrics::new();
        let slot = OrphanSlot {
            node: NodeId(7),
            shard_id: 3,
            num_queries: 12,
        };
        let report = GcReport {
            dropped: vec![],
            pending_disk_cleanup: vec![slot.clone()],
            kept_live_routed: vec![],
            skipped_unassigned: vec![],
            failed: vec![(slot, "https://secret.mesh:50051 leaked-token".to_string())],
            skipped_nodes: vec![(NodeId(9), "https://other.secret:50051".to_string())],
        };
        let response = finish_cluster_gc_worker(
            &prom,
            Instant::now(),
            Ok(ClusterGcWorkerOutcome::Finished(Ok(ClusterGcSuccess {
                version: StateVersion(11),
                report,
            }))),
        );
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let rendered = String::from_utf8(bytes.to_vec()).expect("UTF-8");
        assert!(!rendered.contains("secret.mesh"), "{rendered}");
        assert!(!rendered.contains("leaked-token"), "{rendered}");
        assert!(!rendered.contains("other.secret"), "{rendered}");
        let body: serde_json::Value = serde_json::from_str(&rendered).expect("JSON");
        assert_eq!(body["acknowledged"], false, "{body}");
        assert_eq!(body["completed"], false, "{body}");
        assert_eq!(body["version"], 11, "{body}");
    }
}
