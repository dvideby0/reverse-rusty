//! Strict native `POST /_cluster/reconcile` topology workflow.
//!
//! Elasticsearch and OpenSearch expose `/_cluster/reroute`; its command and
//! `retry_failed` semantics do not match Reverse Rusty's deterministic,
//! whole-cluster HRW convergence pass. Keep the native path honest while
//! adopting their manager-timeout spellings for the admission/start wait.

use std::sync::Arc;
use std::time::Duration;
#[cfg(feature = "distributed")]
use std::time::Instant;

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
#[cfg(feature = "distributed")]
use tokio::sync::TryAcquireError;
#[cfg(feature = "distributed")]
use tracing::{error, instrument, warn};

#[cfg(feature = "distributed")]
use reverse_rusty::cluster::{ReconcileReport, ShardError, StateVersion};

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
use supervisor::{supervise_cluster_reconcile_worker, ClusterReconcileWorkerFailure};
use transport::ClusterReconcileTransport;

pub(crate) const CLUSTER_RECONCILE_BODY_LIMIT: usize = 64 * 1024;
pub(crate) const CLUSTER_RECONCILE_BODY_TIMEOUT: Duration = Duration::from_millis(250);
const DEFAULT_CLUSTER_RECONCILE_MANAGER_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CLUSTER_RECONCILE_MANAGER_TIMEOUT: Duration = Duration::from_secs(30);
const CLUSTER_RECONCILE_ENDPOINT: &str = "cluster_reconcile";

#[cfg(feature = "distributed")]
#[derive(Clone, Copy)]
enum ReconcileStart {
    Queued,
    Started,
    Cancelled,
}

#[cfg(feature = "distributed")]
fn begin_cluster_reconcile(gate: &Mutex<ReconcileStart>, deadline: Instant, no_wait: bool) -> bool {
    let mut start = gate.lock();
    if matches!(*start, ReconcileStart::Cancelled) || (!no_wait && Instant::now() >= deadline) {
        *start = ReconcileStart::Cancelled;
        return false;
    }
    *start = ReconcileStart::Started;
    true
}

#[cfg(feature = "distributed")]
fn cancel_queued_cluster_reconcile(gate: &Mutex<ReconcileStart>) -> bool {
    let mut start = gate.lock();
    match *start {
        ReconcileStart::Queued | ReconcileStart::Cancelled => {
            *start = ReconcileStart::Cancelled;
            true
        }
        ReconcileStart::Started => false,
    }
}

#[cfg(feature = "distributed")]
struct CancelQueuedClusterReconcile(Arc<Mutex<ReconcileStart>>);

#[cfg(feature = "distributed")]
impl Drop for CancelQueuedClusterReconcile {
    fn drop(&mut self) {
        // A dropped request may cancel work that has not started. Once a pass
        // begins, cancellation is unsafe and the worker intentionally runs to
        // its terminal report while retaining admission.
        let _ = cancel_queued_cluster_reconcile(&self.0);
    }
}

#[cfg(feature = "distributed")]
struct ClusterReconcileSuccess {
    version: StateVersion,
    report: ReconcileReport,
}

#[cfg(feature = "distributed")]
enum ClusterReconcileWorkerOutcome {
    NotStarted,
    Finished(Result<ClusterReconcileSuccess, ShardError>),
}

#[cfg(feature = "distributed")]
type ClusterReconcileWorkerResult =
    Result<ClusterReconcileWorkerOutcome, ClusterReconcileWorkerFailure>;

#[cfg(feature = "distributed")]
#[derive(Serialize)]
struct ReconcileUncommittedResponse {
    position: u32,
    from: u64,
    to: u64,
    warning: &'static str,
}

#[cfg(feature = "distributed")]
#[derive(Serialize)]
struct ReconcileFailureResponse {
    position: u32,
    reason: &'static str,
}

#[cfg(feature = "distributed")]
#[derive(Serialize)]
struct ClusterReconcileResponse {
    acknowledged: bool,
    converged: bool,
    version: u64,
    took: u64,
    took_ms: f64,
    /// Desired positions committed by this pass. This includes a commit-only
    /// recovery when the desired target was already the attested live owner.
    reconciled: Vec<u32>,
    skipped: Vec<u32>,
    uncommitted: Vec<ReconcileUncommittedResponse>,
    failed: Vec<ReconcileFailureResponse>,
}

/// Run one idempotent desired-placement convergence pass. Only a resolve-only
/// assignment-routed remote coordinator has an authoritative, restart-safe
/// topology for this data-moving workflow.
#[cfg(feature = "distributed")]
#[instrument(skip_all)]
pub(crate) async fn cluster_reconcile(
    State(state): State<Arc<ClusterAppState>>,
    transport: ClusterReconcileTransport,
) -> Response {
    let (_duration, manager_timeout, body) = transport.into_parts();
    if let Some(response) = reject_reconcile_topology(&state.prom, state.rebalance_topology) {
        return response;
    }

    let started_at = Instant::now();
    let no_wait = manager_timeout.is_zero();
    let Some(deadline) = started_at.checked_add(manager_timeout) else {
        return cluster_reconcile_rejection(
            &state.prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            "reconcile manager timeout is too large for this platform",
        );
    };
    let permit = if no_wait {
        match Arc::clone(&state.reconcile_permits).try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => {
                return cluster_reconcile_not_started_timeout(&state.prom)
            }
            Err(TryAcquireError::Closed) => {
                return cluster_reconcile_rejection(
                    &state.prom,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "reconcile_unavailable",
                    "reconcile admission is closed",
                )
            }
        }
    } else {
        let Some(admission_budget) = deadline.checked_duration_since(Instant::now()) else {
            return cluster_reconcile_not_started_timeout(&state.prom);
        };
        match tokio::time::timeout(
            admission_budget,
            Arc::clone(&state.reconcile_permits).acquire_owned(),
        )
        .await
        {
            Err(_) => return cluster_reconcile_not_started_timeout(&state.prom),
            Ok(Err(_)) => {
                return cluster_reconcile_rejection(
                    &state.prom,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "reconcile_unavailable",
                    "reconcile admission is closed",
                )
            }
            Ok(Ok(permit)) => permit,
        }
    };
    if !no_wait && Instant::now() >= deadline {
        return cluster_reconcile_not_started_timeout(&state.prom);
    }

    let worker_state = Arc::clone(&state);
    let gate = Arc::new(Mutex::new(ReconcileStart::Queued));
    let _cancel_queued_on_drop = CancelQueuedClusterReconcile(Arc::clone(&gate));
    let worker_gate = Arc::clone(&gate);
    let (started_sender, mut started_receiver) = tokio::sync::oneshot::channel();
    let handle = tokio::runtime::Handle::current();
    let completion = match supervise_cluster_reconcile_worker(move || {
        let _permit = permit;
        let topology = if no_wait {
            worker_state.topology_guard.try_read()
        } else {
            deadline
                .checked_duration_since(Instant::now())
                .and_then(|budget| worker_state.topology_guard.try_read_for(budget))
        };
        let Some(_topology) = topology else {
            return ClusterReconcileWorkerOutcome::NotStarted;
        };
        let cluster = if no_wait {
            worker_state.cluster.try_read()
        } else {
            deadline
                .checked_duration_since(Instant::now())
                .and_then(|budget| worker_state.cluster.try_read_for(budget))
        };
        let Some(cluster) = cluster else {
            return ClusterReconcileWorkerOutcome::NotStarted;
        };
        if !begin_cluster_reconcile(&worker_gate, deadline, no_wait) {
            return ClusterReconcileWorkerOutcome::NotStarted;
        }
        let _ = started_sender.send(());
        let result = execute_cluster_reconcile(&cluster, body.max_parallel, &handle);
        ClusterReconcileWorkerOutcome::Finished(result)
    }) {
        Ok(completion) => completion,
        Err(source) => {
            error!(error = %source, "failed to dispatch dedicated reconcile worker");
            return cluster_reconcile_rejection(
                &state.prom,
                StatusCode::SERVICE_UNAVAILABLE,
                "reconcile_unavailable",
                "reconcile worker could not be started",
            );
        }
    };
    let mut completion = completion;

    if no_wait {
        return match completion.await {
            Ok(outcome) => finish_cluster_reconcile_worker(&state.prom, started_at, outcome),
            Err(source) => cluster_reconcile_supervisor_failed(&state.prom, &source),
        };
    }

    let sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
    tokio::pin!(sleep);
    tokio::select! {
        outcome = &mut completion => match outcome {
            Ok(outcome) => finish_cluster_reconcile_worker(&state.prom, started_at, outcome),
            Err(source) => cluster_reconcile_supervisor_failed(&state.prom, &source),
        },
        started = &mut started_receiver => {
            if started.is_err() {
                warn!("reconcile worker ended without sending its start signal");
            }
            match completion.await {
                Ok(outcome) => finish_cluster_reconcile_worker(&state.prom, started_at, outcome),
                Err(source) => cluster_reconcile_supervisor_failed(&state.prom, &source),
            }
        },
        () = &mut sleep => {
            if cancel_queued_cluster_reconcile(&gate) {
                cluster_reconcile_not_started_timeout(&state.prom)
            } else {
                match completion.await {
                    Ok(outcome) => finish_cluster_reconcile_worker(&state.prom, started_at, outcome),
                    Err(source) => cluster_reconcile_supervisor_failed(&state.prom, &source),
                }
            }
        }
    }
}

#[cfg(feature = "distributed")]
fn execute_cluster_reconcile(
    cluster: &reverse_rusty::cluster::ClusterEngine,
    max_parallel: usize,
    handle: &tokio::runtime::Handle,
) -> Result<ClusterReconcileSuccess, ShardError> {
    let report = cluster.reconcile_with(cluster.replication_factor(), max_parallel, handle)?;
    let version = cluster.control_version()?;
    Ok(ClusterReconcileSuccess { version, report })
}

#[cfg(feature = "distributed")]
fn finish_cluster_reconcile_worker(
    prom: &PrometheusMetrics,
    started_at: Instant,
    outcome: ClusterReconcileWorkerResult,
) -> Response {
    match outcome {
        Err(_) => cluster_reconcile_rejection(
            prom,
            StatusCode::INTERNAL_SERVER_ERROR,
            "reconcile_unavailable",
            "reconcile worker failed",
        ),
        Ok(ClusterReconcileWorkerOutcome::NotStarted) => {
            cluster_reconcile_not_started_timeout(prom)
        }
        Ok(ClusterReconcileWorkerOutcome::Finished(Err(source))) => {
            let status = shard_error_status(&source);
            let status = if status.is_success() {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                status
            };
            let (_, error_type) = source.write_http_class();
            cluster_reconcile_rejection(
                prom,
                status,
                error_type,
                "reconcile did not produce an attested terminal control state; inspect server \
                 logs and /_cluster/state before retrying",
            )
        }
        Ok(ClusterReconcileWorkerOutcome::Finished(Ok(success))) => {
            let took_ms = started_at.elapsed().as_secs_f64() * 1_000.0;
            let converged = success.report.is_converged();
            let uncommitted = success
                .report
                .uncommitted
                .into_iter()
                .map(|(position, from, to)| ReconcileUncommittedResponse {
                    position,
                    from: from.0,
                    to: to.0,
                    warning: "live routing reached the target but the durable assignment did not; \
                              retry promptly before coordinator restart",
                })
                .collect();
            let failed = success
                .report
                .failed
                .into_iter()
                .map(|(position, _)| ReconcileFailureResponse {
                    position,
                    reason: "this position did not converge; inspect server logs and retry the \
                             idempotent reconcile pass",
                })
                .collect();
            finish_cluster_reconcile_response(
                prom,
                Json(ClusterReconcileResponse {
                    acknowledged: converged,
                    converged,
                    version: success.version.0,
                    took: took_ms.floor() as u64,
                    took_ms,
                    reconciled: success.report.reconciled,
                    skipped: success.report.skipped,
                    uncommitted,
                    failed,
                })
                .into_response(),
            )
        }
    }
}

#[cfg(feature = "distributed")]
fn reject_reconcile_topology(
    prom: &PrometheusMetrics,
    topology: ClusterRebalanceTopology,
) -> Option<Response> {
    match topology {
        ClusterRebalanceTopology::ResolveOnlyRemote => None,
        ClusterRebalanceTopology::StaticRemote => Some(cluster_reconcile_rejection(
            prom,
            StatusCode::CONFLICT,
            "reconcile_routing_not_authoritative",
            "a static remote coordinator cannot reconcile safely because its live shard backings \
             do not follow the committed assignment map; restart resolve-only with \
             --route-by-assignments and --control-endpoint before retrying",
        )),
        ClusterRebalanceTopology::CliSeededAssignmentRemote => Some(cluster_reconcile_rejection(
            prom,
            StatusCode::CONFLICT,
            "reconcile_resolve_only_required",
            "this assignment-routed coordinator was started with --shard-endpoint, so a changed \
             map would make its next guarded restart fail; restart resolve-only with \
             --route-by-assignments, --control-endpoint, the committed --shards count, and no \
             --shard-endpoint before retrying",
        )),
        ClusterRebalanceTopology::InProcess => Some(cluster_reconcile_rejection(
            prom,
            StatusCode::BAD_REQUEST,
            "reconcile_requires_remote_cluster",
            "reconcile is a remote data-moving workflow; use POST /_cluster/rebalance for an \
             in-process cluster",
        )),
    }
}

#[cfg(feature = "distributed")]
fn cluster_reconcile_supervisor_failed(
    prom: &PrometheusMetrics,
    source: &tokio::sync::oneshot::error::RecvError,
) -> Response {
    error!(error = %source, "reconcile completion supervisor failed");
    cluster_reconcile_rejection(
        prom,
        StatusCode::INTERNAL_SERVER_ERROR,
        "reconcile_unavailable",
        "reconcile completion supervisor failed",
    )
}

#[cfg(feature = "distributed")]
fn cluster_reconcile_not_started_timeout(prom: &PrometheusMetrics) -> Response {
    cluster_reconcile_rejection(
        prom,
        StatusCode::REQUEST_TIMEOUT,
        "reconcile_timeout",
        "timed out waiting for reconcile admission or topology access; no reconcile was started",
    )
}

fn cluster_reconcile_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    finish_cluster_reconcile_response(
        prom,
        ApiError::response(status, error_type, reason).into_response(),
    )
}

fn finish_cluster_reconcile_response(prom: &PrometheusMetrics, mut response: Response) -> Response {
    prom.http_requests_total
        .with_label_values(&[CLUSTER_RECONCILE_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

#[cfg(not(feature = "distributed"))]
pub(crate) async fn cluster_reconcile(
    State(state): State<Arc<ClusterAppState>>,
    transport: ClusterReconcileTransport,
) -> Response {
    let (_duration, _manager_timeout, body) = transport.into_parts();
    let _ = body.max_parallel;
    finish_cluster_reconcile_response(
        &state.prom,
        not_in_cluster_mode(
            "POST /_cluster/reconcile",
            "the data-moving reconciler needs the gRPC transport — rebuild the server with \
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
    fn only_resolve_only_topology_is_accepted() {
        let prom = PrometheusMetrics::new();
        assert!(
            reject_reconcile_topology(&prom, ClusterRebalanceTopology::ResolveOnlyRemote).is_none()
        );
        for topology in [
            ClusterRebalanceTopology::InProcess,
            ClusterRebalanceTopology::StaticRemote,
            ClusterRebalanceTopology::CliSeededAssignmentRemote,
        ] {
            assert!(reject_reconcile_topology(&prom, topology).is_some());
        }
    }

    #[cfg(feature = "distributed")]
    #[test]
    fn queued_cancellation_cannot_start_later_but_started_work_cannot_be_cancelled() {
        let queued = Mutex::new(ReconcileStart::Queued);
        assert!(cancel_queued_cluster_reconcile(&queued));
        assert!(!begin_cluster_reconcile(&queued, Instant::now(), true));

        let started = Mutex::new(ReconcileStart::Queued);
        assert!(begin_cluster_reconcile(&started, Instant::now(), true));
        assert!(!cancel_queued_cluster_reconcile(&started));
    }

    #[cfg(feature = "distributed")]
    #[tokio::test]
    async fn partial_response_keeps_internal_failure_details_in_logs() {
        let prom = PrometheusMetrics::new();
        let report = ReconcileReport {
            reconciled: vec![1],
            skipped: vec![],
            uncommitted: vec![(
                2,
                reverse_rusty::cluster::NodeId(11),
                reverse_rusty::cluster::NodeId(14),
            )],
            failed: vec![(3, "https://secret.mesh:50051 leaked-token".to_string())],
        };
        let response = finish_cluster_reconcile_worker(
            &prom,
            Instant::now(),
            Ok(ClusterReconcileWorkerOutcome::Finished(Ok(
                ClusterReconcileSuccess {
                    version: StateVersion(9),
                    report,
                },
            ))),
        );
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let rendered = String::from_utf8(bytes.to_vec()).expect("UTF-8");
        assert!(!rendered.contains("secret.mesh"), "{rendered}");
        assert!(!rendered.contains("leaked-token"), "{rendered}");
        let body: serde_json::Value = serde_json::from_str(&rendered).expect("JSON");
        assert_eq!(body["acknowledged"], false, "{body}");
        assert_eq!(body["converged"], false, "{body}");
        assert_eq!(body["version"], 9, "{body}");
        assert_eq!(body["failed"][0]["position"], 3, "{body}");
        assert!(body["failed"][0]["reason"].is_string(), "{body}");
        assert!(body["uncommitted"][0]["warning"].is_string(), "{body}");
    }
}
