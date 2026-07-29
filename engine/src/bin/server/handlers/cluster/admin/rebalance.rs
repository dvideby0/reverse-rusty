//! Strict native `POST /_cluster/rebalance` topology workflow.
//!
//! Elasticsearch and OpenSearch expose explicit `/_cluster/reroute` commands,
//! not Reverse Rusty's whole-cluster HRW planner. Keep the native path honest
//! while adopting manager-timeout spellings for the admission/start wait that
//! maps exactly to this operation.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    extract::State,
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use parking_lot::Mutex;
use serde::Serialize;
use tokio::sync::TryAcquireError;
use tracing::{error, instrument, warn};

use reverse_rusty::cluster::{ShardError, StateVersion};

use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::ClusterAppState;

use super::super::shard_error_status;

mod supervisor;
mod transport;

use supervisor::supervise_cluster_rebalance_worker;
use transport::ClusterRebalanceTransport;

pub(crate) const CLUSTER_REBALANCE_BODY_LIMIT: usize = 64 * 1024;
pub(crate) const CLUSTER_REBALANCE_BODY_TIMEOUT: Duration = Duration::from_millis(250);
const DEFAULT_CLUSTER_REBALANCE_MANAGER_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CLUSTER_REBALANCE_MANAGER_TIMEOUT: Duration = Duration::from_secs(30);
const CLUSTER_REBALANCE_ENDPOINT: &str = "cluster_rebalance";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RebalanceMode {
    MapOnly,
    DataMoving { max_parallel: usize },
}

#[derive(Clone, Copy, Debug)]
enum RebalanceRequestError {
    UnsafeRemoteMapOnly,
    DataMovementRequiresRemote,
    ParallelismRequiresMovement,
}

fn resolve_rebalance_mode(
    remote: bool,
    requested_move: Option<bool>,
    max_parallel: Option<NonZeroUsize>,
) -> Result<RebalanceMode, RebalanceRequestError> {
    if remote {
        return match requested_move {
            Some(false) => Err(RebalanceRequestError::UnsafeRemoteMapOnly),
            None | Some(true) => Ok(RebalanceMode::DataMoving {
                max_parallel: max_parallel.map_or(1, NonZeroUsize::get),
            }),
        };
    }
    if requested_move == Some(true) {
        return Err(RebalanceRequestError::DataMovementRequiresRemote);
    }
    if max_parallel.is_some() {
        return Err(RebalanceRequestError::ParallelismRequiresMovement);
    }
    Ok(RebalanceMode::MapOnly)
}

#[derive(Clone, Copy)]
enum RebalanceStart {
    Queued,
    Started,
    Cancelled,
}

fn begin_cluster_rebalance(gate: &Mutex<RebalanceStart>, deadline: Instant, no_wait: bool) -> bool {
    let mut start = gate.lock();
    if matches!(*start, RebalanceStart::Cancelled) || (!no_wait && Instant::now() >= deadline) {
        *start = RebalanceStart::Cancelled;
        return false;
    }
    *start = RebalanceStart::Started;
    true
}

fn cancel_queued_cluster_rebalance(gate: &Mutex<RebalanceStart>) -> bool {
    let mut start = gate.lock();
    match *start {
        RebalanceStart::Queued | RebalanceStart::Cancelled => {
            *start = RebalanceStart::Cancelled;
            true
        }
        RebalanceStart::Started => false,
    }
}

struct CancelQueuedClusterRebalance(Arc<Mutex<RebalanceStart>>);

impl Drop for CancelQueuedClusterRebalance {
    fn drop(&mut self) {
        // Dropping an HTTP future must not leave a queued blocking worker able
        // to start later. Once `Started` wins the gate this is intentionally a
        // no-op: a live move cannot be cancelled safely.
        let _ = cancel_queued_cluster_rebalance(&self.0);
    }
}

#[derive(Debug)]
struct RebalanceFailure {
    position: u32,
    reason: String,
}

#[derive(Debug)]
struct ClusterRebalanceSuccess {
    mode: RebalanceMode,
    version: StateVersion,
    reassigned: usize,
    moved: Vec<u32>,
    failed: Option<RebalanceFailure>,
    not_attempted: Vec<u32>,
}

#[derive(Debug)]
enum ClusterRebalanceError {
    Request(RebalanceRequestError),
    Backend(ShardError),
}

enum ClusterRebalanceWorkerOutcome {
    NotStarted,
    Finished(Result<ClusterRebalanceSuccess, ClusterRebalanceError>),
}

type ClusterRebalanceWorkerResult = Result<ClusterRebalanceWorkerOutcome, tokio::task::JoinError>;

#[derive(Serialize)]
struct RebalanceFailureResponse {
    position: u32,
    reason: &'static str,
}

#[derive(Serialize)]
struct ClusterRebalanceResponse {
    acknowledged: bool,
    version: u64,
    moved_data: bool,
    reassigned: usize,
    moved: Vec<u32>,
    failed: Option<RebalanceFailureResponse>,
    not_attempted: Vec<u32>,
}

/// Recompute the desired HRW shard placement. In-process clusters commit the
/// advisory map. Remote clusters default to the data-moving move-then-commit
/// workflow; an explicit remote `move:false` is rejected before mutation.
#[instrument(skip_all)]
pub(crate) async fn cluster_rebalance(
    State(state): State<Arc<ClusterAppState>>,
    transport: ClusterRebalanceTransport,
) -> Response {
    let (_duration, manager_timeout, body) = transport.into_parts();
    let no_wait = manager_timeout.is_zero();
    let Some(deadline) = Instant::now().checked_add(manager_timeout) else {
        return cluster_rebalance_rejection(
            &state.prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            "rebalance manager timeout is too large for this platform",
        );
    };
    let permit = if no_wait {
        match Arc::clone(&state.rebalance_permits).try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => {
                return cluster_rebalance_not_started_timeout(&state.prom);
            }
            Err(TryAcquireError::Closed) => {
                return cluster_rebalance_rejection(
                    &state.prom,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "rebalance_unavailable",
                    "rebalance admission is closed",
                );
            }
        }
    } else {
        let Some(admission_budget) = deadline.checked_duration_since(Instant::now()) else {
            return cluster_rebalance_not_started_timeout(&state.prom);
        };
        match tokio::time::timeout(
            admission_budget,
            Arc::clone(&state.rebalance_permits).acquire_owned(),
        )
        .await
        {
            Err(_) => return cluster_rebalance_not_started_timeout(&state.prom),
            Ok(Err(_)) => {
                return cluster_rebalance_rejection(
                    &state.prom,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "rebalance_unavailable",
                    "rebalance admission is closed",
                );
            }
            Ok(Ok(permit)) => permit,
        }
    };
    if !no_wait && Instant::now() >= deadline {
        return cluster_rebalance_not_started_timeout(&state.prom);
    }

    let worker_state = Arc::clone(&state);
    let gate = Arc::new(Mutex::new(RebalanceStart::Queued));
    let _cancel_queued_on_drop = CancelQueuedClusterRebalance(Arc::clone(&gate));
    let worker_gate = Arc::clone(&gate);
    let (started_sender, mut started_receiver) = tokio::sync::oneshot::channel();
    #[cfg(feature = "distributed")]
    let handle = tokio::runtime::Handle::current();
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let topology = if no_wait {
            worker_state.topology_guard.try_read()
        } else {
            deadline
                .checked_duration_since(Instant::now())
                .and_then(|budget| worker_state.topology_guard.try_read_for(budget))
        };
        let Some(_topology) = topology else {
            return ClusterRebalanceWorkerOutcome::NotStarted;
        };
        let cluster = if no_wait {
            worker_state.cluster.try_read()
        } else {
            deadline
                .checked_duration_since(Instant::now())
                .and_then(|budget| worker_state.cluster.try_read_for(budget))
        };
        let Some(cluster) = cluster else {
            return ClusterRebalanceWorkerOutcome::NotStarted;
        };
        if !begin_cluster_rebalance(&worker_gate, deadline, no_wait) {
            return ClusterRebalanceWorkerOutcome::NotStarted;
        }
        let _ = started_sender.send(());
        let mode = match resolve_rebalance_mode(
            cluster.requires_data_moving_rebalance(),
            body.do_move,
            body.max_parallel,
        ) {
            Ok(mode) => mode,
            Err(source) => {
                return ClusterRebalanceWorkerOutcome::Finished(Err(
                    ClusterRebalanceError::Request(source),
                ));
            }
        };
        let result = execute_cluster_rebalance(
            &cluster,
            mode,
            #[cfg(feature = "distributed")]
            &handle,
        );
        ClusterRebalanceWorkerOutcome::Finished(result)
    });
    let mut completion = supervise_cluster_rebalance_worker(worker);

    if no_wait {
        // Give an immediately runnable blocking worker one scheduler turn to
        // win the start gate. If it is still queued, cancel it without waiting
        // for blocking-pool capacity; the worker will observe `Cancelled`
        // before any topology-dependent validation or mutation.
        tokio::task::yield_now().await;
        if cancel_queued_cluster_rebalance(&gate) {
            return cluster_rebalance_not_started_timeout(&state.prom);
        }
        return match completion.await {
            Ok(outcome) => finish_cluster_rebalance_worker(&state.prom, outcome),
            Err(source) => cluster_rebalance_supervisor_failed(&state.prom, &source),
        };
    }

    let sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
    tokio::pin!(sleep);
    tokio::select! {
        outcome = &mut completion => match outcome {
            Ok(outcome) => finish_cluster_rebalance_worker(&state.prom, outcome),
            Err(source) => cluster_rebalance_supervisor_failed(&state.prom, &source),
        },
        started = &mut started_receiver => {
            if started.is_err() {
                warn!("rebalance worker ended without sending its start signal");
            }
            match completion.await {
                Ok(outcome) => finish_cluster_rebalance_worker(&state.prom, outcome),
                Err(source) => cluster_rebalance_supervisor_failed(&state.prom, &source),
            }
        },
        () = &mut sleep => {
            if cancel_queued_cluster_rebalance(&gate) {
                cluster_rebalance_not_started_timeout(&state.prom)
            } else {
                // The worker won the start race at the manager deadline. This
                // timeout only governs admission/start; a live handoff is not
                // safely cancellable, so wait for its exact terminal report.
                match completion.await {
                    Ok(outcome) => finish_cluster_rebalance_worker(&state.prom, outcome),
                    Err(source) => cluster_rebalance_supervisor_failed(&state.prom, &source),
                }
            }
        }
    }
}

fn execute_cluster_rebalance(
    cluster: &reverse_rusty::cluster::ClusterEngine,
    mode: RebalanceMode,
    #[cfg(feature = "distributed")] handle: &tokio::runtime::Handle,
) -> Result<ClusterRebalanceSuccess, ClusterRebalanceError> {
    let rf = cluster.replication_factor();
    match mode {
        RebalanceMode::MapOnly => {
            let reassigned = cluster
                .rebalance(rf)
                .map_err(ClusterRebalanceError::Backend)?;
            let version = cluster
                .control_version()
                .map_err(ClusterRebalanceError::Backend)?;
            Ok(ClusterRebalanceSuccess {
                mode,
                version,
                reassigned,
                moved: Vec::new(),
                failed: None,
                not_attempted: Vec::new(),
            })
        }
        RebalanceMode::DataMoving { max_parallel } => {
            #[cfg(feature = "distributed")]
            {
                let report = cluster
                    .rebalance_and_move_with(rf, max_parallel, handle)
                    .map_err(ClusterRebalanceError::Backend)?;
                let version = cluster
                    .control_version()
                    .map_err(ClusterRebalanceError::Backend)?;
                let reassigned = report.moved.len();
                let failed = report
                    .failed
                    .map(|(position, reason)| RebalanceFailure { position, reason });
                Ok(ClusterRebalanceSuccess {
                    mode,
                    version,
                    reassigned,
                    moved: report.moved,
                    failed,
                    not_attempted: report.not_attempted,
                })
            }
            #[cfg(not(feature = "distributed"))]
            {
                let _ = max_parallel;
                Err(ClusterRebalanceError::Request(
                    RebalanceRequestError::DataMovementRequiresRemote,
                ))
            }
        }
    }
}

fn finish_cluster_rebalance_worker(
    prom: &PrometheusMetrics,
    outcome: ClusterRebalanceWorkerResult,
) -> Response {
    match outcome {
        Err(_) => cluster_rebalance_rejection(
            prom,
            StatusCode::INTERNAL_SERVER_ERROR,
            "rebalance_unavailable",
            "rebalance worker failed",
        ),
        Ok(ClusterRebalanceWorkerOutcome::NotStarted) => {
            cluster_rebalance_not_started_timeout(prom)
        }
        Ok(ClusterRebalanceWorkerOutcome::Finished(Err(ClusterRebalanceError::Request(
            source,
        )))) => cluster_rebalance_request_error(prom, source),
        Ok(ClusterRebalanceWorkerOutcome::Finished(Err(ClusterRebalanceError::Backend(
            source,
        )))) => {
            let status = shard_error_status(&source);
            let status = if status.is_success() {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                status
            };
            let (_, error_type) = source.write_http_class();
            cluster_rebalance_rejection(
                prom,
                status,
                error_type,
                "rebalance did not produce an attested terminal control state; inspect \
                 /_cluster/state before retrying",
            )
        }
        Ok(ClusterRebalanceWorkerOutcome::Finished(Ok(success))) => {
            let moved_data = matches!(success.mode, RebalanceMode::DataMoving { .. });
            let acknowledged = success.failed.is_none();
            let failed = success.failed.map(|failure| RebalanceFailureResponse {
                position: failure.position,
                reason: "data movement stopped before this position reached an attested commit; \
                         inspect server logs and retry the idempotent rebalance",
            });
            finish_cluster_rebalance_response(
                prom,
                Json(ClusterRebalanceResponse {
                    acknowledged,
                    version: success.version.0,
                    moved_data,
                    reassigned: success.reassigned,
                    moved: success.moved,
                    failed,
                    not_attempted: success.not_attempted,
                })
                .into_response(),
            )
        }
    }
}

fn cluster_rebalance_request_error(
    prom: &PrometheusMetrics,
    source: RebalanceRequestError,
) -> Response {
    match source {
        RebalanceRequestError::UnsafeRemoteMapOnly => cluster_rebalance_rejection(
            prom,
            StatusCode::CONFLICT,
            "unsafe_rebalance_mode",
            "a remote cluster cannot commit a map-only rebalance because routing could point at \
             data that was never moved; omit `move` or set it to true",
        ),
        RebalanceRequestError::ParallelismRequiresMovement => cluster_rebalance_rejection(
            prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            "`max_parallel` applies only to a remote data-moving rebalance",
        ),
        RebalanceRequestError::DataMovementRequiresRemote => {
            #[cfg(feature = "distributed")]
            {
                cluster_rebalance_rejection(
                    prom,
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    "`move:true` requires a remote cluster; omit `move` for an in-process cluster",
                )
            }
            #[cfg(not(feature = "distributed"))]
            {
                cluster_rebalance_rejection(
                    prom,
                    StatusCode::NOT_IMPLEMENTED,
                    "not_supported_in_cluster_mode",
                    "a data-moving rebalance requires a server built with the `distributed` \
                     feature and assembled over remote shard endpoints",
                )
            }
        }
    }
}

fn cluster_rebalance_supervisor_failed(
    prom: &PrometheusMetrics,
    source: &tokio::sync::oneshot::error::RecvError,
) -> Response {
    error!(error = %source, "rebalance completion supervisor failed");
    cluster_rebalance_rejection(
        prom,
        StatusCode::INTERNAL_SERVER_ERROR,
        "rebalance_unavailable",
        "rebalance completion supervisor failed",
    )
}

fn cluster_rebalance_not_started_timeout(prom: &PrometheusMetrics) -> Response {
    cluster_rebalance_rejection(
        prom,
        StatusCode::REQUEST_TIMEOUT,
        "rebalance_timeout",
        "timed out waiting for rebalance admission or topology access; no rebalance was started",
    )
}

fn cluster_rebalance_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    finish_cluster_rebalance_response(
        prom,
        ApiError::response(status, error_type, reason).into_response(),
    )
}

fn finish_cluster_rebalance_response(prom: &PrometheusMetrics, mut response: Response) -> Response {
    prom.http_requests_total
        .with_label_values(&[CLUSTER_REBALANCE_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_mode_is_safe_for_each_topology() {
        assert_eq!(
            resolve_rebalance_mode(false, None, None).expect("in-process default"),
            RebalanceMode::MapOnly
        );
        assert_eq!(
            resolve_rebalance_mode(false, Some(false), None).expect("explicit in-process map"),
            RebalanceMode::MapOnly
        );
        assert!(matches!(
            resolve_rebalance_mode(false, Some(true), None),
            Err(RebalanceRequestError::DataMovementRequiresRemote)
        ));
        assert!(matches!(
            resolve_rebalance_mode(false, None, NonZeroUsize::new(2)),
            Err(RebalanceRequestError::ParallelismRequiresMovement)
        ));

        assert_eq!(
            resolve_rebalance_mode(true, None, None).expect("remote safe default"),
            RebalanceMode::DataMoving { max_parallel: 1 }
        );
        assert_eq!(
            resolve_rebalance_mode(true, Some(true), NonZeroUsize::new(4))
                .expect("remote parallel move"),
            RebalanceMode::DataMoving { max_parallel: 4 }
        );
        assert!(matches!(
            resolve_rebalance_mode(true, Some(false), None),
            Err(RebalanceRequestError::UnsafeRemoteMapOnly)
        ));
    }

    #[test]
    fn dropping_a_handler_cancels_only_queued_work() {
        let queued = Arc::new(Mutex::new(RebalanceStart::Queued));
        drop(CancelQueuedClusterRebalance(Arc::clone(&queued)));
        assert!(
            !begin_cluster_rebalance(&queued, Instant::now(), true),
            "a dropped request must leave its queued worker unable to start"
        );

        let started = Arc::new(Mutex::new(RebalanceStart::Queued));
        let guard = CancelQueuedClusterRebalance(Arc::clone(&started));
        assert!(begin_cluster_rebalance(&started, Instant::now(), true));
        drop(guard);
        assert!(
            !cancel_queued_cluster_rebalance(&started),
            "a started move must survive HTTP cancellation"
        );
    }
}
