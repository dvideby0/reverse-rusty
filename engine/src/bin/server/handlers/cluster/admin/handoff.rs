//! Strict native `POST /_cluster/handoff` live-routing primitive.
//!
//! Elasticsearch/OpenSearch reroute commits logical allocation commands. A
//! Reverse Rusty raw handoff names physical endpoints and intentionally does
//! not commit the control-plane assignment, so it remains native and requires
//! an explicit `allow_uncommitted:true` acknowledgement.

use std::sync::Arc;
use std::time::Duration;
#[cfg(feature = "distributed")]
use std::time::Instant;

#[cfg(feature = "distributed")]
use axum::extract::Json;
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
use tracing::instrument;
#[cfg(feature = "distributed")]
use tracing::{error, warn};

#[cfg(feature = "distributed")]
use reverse_rusty::cluster::{HandoffOutcome, ShardError};

use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::ClusterAppState;

#[cfg(feature = "distributed")]
use super::super::shard_error_status;

mod supervisor;
mod transport;

#[cfg(feature = "distributed")]
use supervisor::{supervise_cluster_handoff_worker, ClusterHandoffWorkerFailure};
#[cfg(feature = "distributed")]
use transport::ClusterHandoffBody;
use transport::ClusterHandoffTransport;

pub(crate) const CLUSTER_HANDOFF_BODY_LIMIT: usize = 64 * 1024;
pub(crate) const CLUSTER_HANDOFF_BODY_TIMEOUT: Duration = Duration::from_millis(250);
const CLUSTER_HANDOFF_ENDPOINT: &str = "cluster_handoff";

#[cfg(feature = "distributed")]
#[derive(Clone, Copy)]
enum HandoffStart {
    Queued,
    Started,
    Cancelled,
}

#[cfg(feature = "distributed")]
fn begin_cluster_handoff(gate: &Mutex<HandoffStart>, deadline: Instant, no_wait: bool) -> bool {
    let mut start = gate.lock();
    if matches!(*start, HandoffStart::Cancelled) || (!no_wait && Instant::now() >= deadline) {
        *start = HandoffStart::Cancelled;
        return false;
    }
    *start = HandoffStart::Started;
    true
}

#[cfg(feature = "distributed")]
fn cancel_queued_cluster_handoff(gate: &Mutex<HandoffStart>) -> bool {
    let mut start = gate.lock();
    match *start {
        HandoffStart::Queued | HandoffStart::Cancelled => {
            *start = HandoffStart::Cancelled;
            true
        }
        HandoffStart::Started => false,
    }
}

#[cfg(feature = "distributed")]
struct CancelQueuedClusterHandoff(Arc<Mutex<HandoffStart>>);

#[cfg(feature = "distributed")]
impl Drop for CancelQueuedClusterHandoff {
    fn drop(&mut self) {
        let _ = cancel_queued_cluster_handoff(&self.0);
    }
}

#[cfg(feature = "distributed")]
#[derive(Debug)]
struct ClusterHandoffSuccess {
    outcome: HandoffOutcome,
    position: u32,
    took_ms: f64,
}

#[cfg(feature = "distributed")]
enum ClusterHandoffWorkerOutcome {
    NotStarted,
    Finished(Result<ClusterHandoffSuccess, ShardError>),
}

#[cfg(feature = "distributed")]
type ClusterHandoffWorkerResult = Result<ClusterHandoffWorkerOutcome, ClusterHandoffWorkerFailure>;

#[cfg(feature = "distributed")]
#[derive(Serialize)]
struct ClusterHandoffResponse {
    took: u64,
    took_ms: f64,
    acknowledged: bool,
    moved: bool,
    committed: bool,
    position: u32,
    generation: u64,
    warning: &'static str,
}

/// Run one raw live-routing handoff. Once movement starts, the request waits
/// for its exact terminal result; dropping the HTTP future detaches only the
/// response, not the safety-sensitive worker.
#[instrument(skip_all)]
pub(crate) async fn cluster_handoff(
    State(state): State<Arc<ClusterAppState>>,
    transport: ClusterHandoffTransport,
) -> Response {
    let (_duration, started, manager_timeout, body) = transport.into_parts();

    #[cfg(not(feature = "distributed"))]
    {
        let _ = (started, manager_timeout, body);
        return cluster_handoff_rejection(
            &state.prom,
            StatusCode::NOT_IMPLEMENTED,
            "not_supported_in_cluster_mode",
            "a live handoff needs the gRPC transport; rebuild the server with the `distributed` \
             feature",
        );
    }

    #[cfg(feature = "distributed")]
    {
        cluster_handoff_distributed(state, started, manager_timeout, body).await
    }
}

#[cfg(feature = "distributed")]
async fn cluster_handoff_distributed(
    state: Arc<ClusterAppState>,
    started: Instant,
    manager_timeout: Duration,
    body: ClusterHandoffBody,
) -> Response {
    let no_wait = manager_timeout.is_zero();
    let Some(deadline) = Instant::now().checked_add(manager_timeout) else {
        return cluster_handoff_rejection(
            &state.prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            "handoff manager timeout is too large for this platform",
        );
    };

    let permit = if no_wait {
        match Arc::clone(&state.handoff_permits).try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => {
                return cluster_handoff_not_started_timeout(&state.prom)
            }
            Err(TryAcquireError::Closed) => {
                return cluster_handoff_rejection(
                    &state.prom,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "handoff_unavailable",
                    "handoff admission is closed",
                );
            }
        }
    } else {
        let Some(admission_budget) = deadline.checked_duration_since(Instant::now()) else {
            return cluster_handoff_not_started_timeout(&state.prom);
        };
        match tokio::time::timeout(
            admission_budget,
            Arc::clone(&state.handoff_permits).acquire_owned(),
        )
        .await
        {
            Err(_) => return cluster_handoff_not_started_timeout(&state.prom),
            Ok(Err(_)) => {
                return cluster_handoff_rejection(
                    &state.prom,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "handoff_unavailable",
                    "handoff admission is closed",
                );
            }
            Ok(Ok(permit)) => permit,
        }
    };
    if !no_wait && Instant::now() >= deadline {
        return cluster_handoff_not_started_timeout(&state.prom);
    }

    let worker_state = Arc::clone(&state);
    let gate = Arc::new(Mutex::new(HandoffStart::Queued));
    let _cancel_queued_on_drop = CancelQueuedClusterHandoff(Arc::clone(&gate));
    let worker_gate = Arc::clone(&gate);
    let (started_sender, mut started_receiver) = tokio::sync::oneshot::channel();
    let handle = tokio::runtime::Handle::current();
    let completion = match supervise_cluster_handoff_worker(move || {
        let _permit = permit;
        let topology = if no_wait {
            worker_state.topology_guard.try_read()
        } else {
            deadline
                .checked_duration_since(Instant::now())
                .and_then(|budget| worker_state.topology_guard.try_read_for(budget))
        };
        let Some(_topology) = topology else {
            return ClusterHandoffWorkerOutcome::NotStarted;
        };
        let cluster = if no_wait {
            worker_state.cluster.try_read()
        } else {
            deadline
                .checked_duration_since(Instant::now())
                .and_then(|budget| worker_state.cluster.try_read_for(budget))
        };
        let Some(cluster) = cluster else {
            return ClusterHandoffWorkerOutcome::NotStarted;
        };
        let outcome = cluster.execute_handoff_until(
            body.position as usize,
            &body.source,
            &body.target,
            &handle,
            deadline,
            || {
                if !begin_cluster_handoff(&worker_gate, deadline, no_wait) {
                    return false;
                }
                let _ = started_sender.send(());
                true
            },
        );
        match outcome {
            Ok(None) => ClusterHandoffWorkerOutcome::NotStarted,
            Ok(Some(outcome)) => ClusterHandoffWorkerOutcome::Finished(Ok(ClusterHandoffSuccess {
                outcome,
                position: body.position,
                took_ms: started.elapsed().as_secs_f64() * 1_000.0,
            })),
            Err(source) => ClusterHandoffWorkerOutcome::Finished(Err(source)),
        }
    }) {
        Ok(completion) => completion,
        Err(source) => {
            error!(error = %source, "failed to dispatch dedicated handoff worker");
            return cluster_handoff_rejection(
                &state.prom,
                StatusCode::SERVICE_UNAVAILABLE,
                "handoff_unavailable",
                "handoff worker could not be started",
            );
        }
    };
    let mut completion = completion;

    if no_wait {
        return match completion.await {
            Ok(outcome) => finish_cluster_handoff_worker(&state.prom, outcome),
            Err(source) => cluster_handoff_supervisor_failed(&state.prom, &source),
        };
    }

    let sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
    tokio::pin!(sleep);
    tokio::select! {
        outcome = &mut completion => match outcome {
            Ok(outcome) => finish_cluster_handoff_worker(&state.prom, outcome),
            Err(source) => cluster_handoff_supervisor_failed(&state.prom, &source),
        },
        started = &mut started_receiver => {
            if started.is_err() {
                warn!("handoff worker ended without sending its start signal");
            }
            match completion.await {
                Ok(outcome) => finish_cluster_handoff_worker(&state.prom, outcome),
                Err(source) => cluster_handoff_supervisor_failed(&state.prom, &source),
            }
        },
        () = &mut sleep => {
            if cancel_queued_cluster_handoff(&gate) {
                cluster_handoff_not_started_timeout(&state.prom)
            } else {
                match completion.await {
                    Ok(outcome) => finish_cluster_handoff_worker(&state.prom, outcome),
                    Err(source) => cluster_handoff_supervisor_failed(&state.prom, &source),
                }
            }
        }
    }
}

#[cfg(feature = "distributed")]
fn finish_cluster_handoff_worker(
    prom: &PrometheusMetrics,
    outcome: ClusterHandoffWorkerResult,
) -> Response {
    match outcome {
        Err(_) => cluster_handoff_rejection(
            prom,
            StatusCode::INTERNAL_SERVER_ERROR,
            "handoff_unavailable",
            "handoff worker failed",
        ),
        Ok(ClusterHandoffWorkerOutcome::NotStarted) => cluster_handoff_not_started_timeout(prom),
        Ok(ClusterHandoffWorkerOutcome::Finished(Err(source))) => {
            let status = shard_error_status(&source);
            let status = if status.is_success() {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                status
            };
            let (_, error_type) = source.write_http_class();
            cluster_handoff_rejection(
                prom,
                status,
                error_type,
                format!("handoff failed: {source}"),
            )
        }
        Ok(ClusterHandoffWorkerOutcome::Finished(Ok(success))) => {
            let generation = success.outcome.generation();
            let moved = success.outcome.moved();
            finish_cluster_handoff_response(
                prom,
                Json(ClusterHandoffResponse {
                    took: success.took_ms.floor() as u64,
                    took_ms: success.took_ms,
                    acknowledged: true,
                    moved,
                    committed: false,
                    position: success.position,
                    generation,
                    warning:
                        "live routing changed without committing the control-plane assignment; \
                              use POST /_cluster/reassign for restart-stable placement",
                })
                .into_response(),
            )
        }
    }
}

#[cfg(feature = "distributed")]
fn cluster_handoff_supervisor_failed(
    prom: &PrometheusMetrics,
    source: &tokio::sync::oneshot::error::RecvError,
) -> Response {
    error!(error = %source, "handoff completion supervisor failed");
    cluster_handoff_rejection(
        prom,
        StatusCode::INTERNAL_SERVER_ERROR,
        "handoff_unavailable",
        "handoff completion supervisor failed",
    )
}

#[cfg(feature = "distributed")]
fn cluster_handoff_not_started_timeout(prom: &PrometheusMetrics) -> Response {
    cluster_handoff_rejection(
        prom,
        StatusCode::REQUEST_TIMEOUT,
        "handoff_timeout",
        "timed out waiting for handoff admission or topology access; no handoff was started",
    )
}

fn cluster_handoff_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    finish_cluster_handoff_response(
        prom,
        ApiError::response(status, error_type, reason).into_response(),
    )
}

fn finish_cluster_handoff_response(prom: &PrometheusMetrics, mut response: Response) -> Response {
    prom.http_requests_total
        .with_label_values(&[CLUSTER_HANDOFF_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
