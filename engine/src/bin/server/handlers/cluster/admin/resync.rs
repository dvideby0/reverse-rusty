//! Strict native `POST /_cluster/resync` partial-apply repair boundary.
//!
//! Elasticsearch and OpenSearch expose `/_cluster/reroute?retry_failed=true`
//! for failed shard *allocation*. Reverse Rusty's queue instead records query
//! mutations that were durably logged but reached only some target positions.
//! Keep the native path rather than presenting mutation delivery as allocation,
//! while adopting the familiar manager-timeout spellings for the admission and
//! exclusive-writer wait that precede a repair pass.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::Bytes,
    extract::{FromRequest, Query, Request, State},
    http::{header, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use parking_lot::Mutex;
use prometheus::HistogramTimer;
use serde::{Deserialize, Serialize};
use tokio::sync::TryAcquireError;
use tracing::{error, instrument, warn};

use reverse_rusty::cluster::ResyncReport;

use crate::dto::ApiError;
use crate::handlers::search::parse_named_time_value;
use crate::metrics::PrometheusMetrics;
use crate::state::ClusterAppState;

mod supervisor;

use supervisor::{supervise_cluster_resync_worker, ClusterResyncWorkerFailure};

pub(crate) const CLUSTER_RESYNC_BODY_LIMIT: usize = 64 * 1024;
pub(crate) const CLUSTER_RESYNC_BODY_TIMEOUT: Duration = Duration::from_millis(250);
const DEFAULT_CLUSTER_RESYNC_MANAGER_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CLUSTER_RESYNC_MANAGER_TIMEOUT: Duration = Duration::from_secs(30);
const CLUSTER_RESYNC_ENDPOINT: &str = "cluster_resync";

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterResyncParams {
    /// OpenSearch-inclusive spelling.
    cluster_manager_timeout: Option<String>,
    /// Elasticsearch and legacy OpenSearch spelling.
    master_timeout: Option<String>,
}

impl ClusterResyncParams {
    fn manager_timeout(self) -> Result<Duration, String> {
        if self.cluster_manager_timeout.is_some() && self.master_timeout.is_some() {
            return Err(
                "`cluster_manager_timeout` and `master_timeout` are aliases; specify exactly one"
                    .to_string(),
            );
        }
        let timeout = self
            .cluster_manager_timeout
            .or(self.master_timeout)
            .as_deref()
            .map(parse_cluster_resync_manager_timeout)
            .transpose()?
            .unwrap_or(DEFAULT_CLUSTER_RESYNC_MANAGER_TIMEOUT);
        if timeout > MAX_CLUSTER_RESYNC_MANAGER_TIMEOUT {
            return Err("resync manager timeout must not exceed 30s".to_string());
        }
        Ok(timeout)
    }
}

fn parse_cluster_resync_manager_timeout(raw: &str) -> Result<Duration, String> {
    if raw == "0" {
        return Ok(Duration::ZERO);
    }
    parse_named_time_value("cluster_manager_timeout/master_timeout", raw)
}

/// Method/query validation plus bounded extraction for the body-free native
/// resync request. The route timer begins before every transport check.
pub(crate) struct ClusterResyncTransport {
    duration: HistogramTimer,
    started: Instant,
    manager_timeout: Duration,
}

impl ClusterResyncTransport {
    fn into_parts(self) -> (HistogramTimer, Instant, Duration) {
        (self.duration, self.started, self.manager_timeout)
    }
}

impl FromRequest<Arc<ClusterAppState>> for ClusterResyncTransport {
    type Rejection = Response;

    async fn from_request(
        request: Request,
        state: &Arc<ClusterAppState>,
    ) -> Result<Self, Self::Rejection> {
        let started = Instant::now();
        let duration = state
            .prom
            .http_request_duration
            .with_label_values(&[CLUSTER_RESYNC_ENDPOINT])
            .start_timer();
        if request.method() != Method::POST {
            let mut response = cluster_resync_rejection(
                &state.prom,
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "POST is the only supported /_cluster/resync method",
            );
            response
                .headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static("POST"));
            return Err(response);
        }

        let Query(params) =
            Query::<ClusterResyncParams>::try_from_uri(request.uri()).map_err(|source| {
                cluster_resync_rejection(
                    &state.prom,
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    format!("invalid resync query parameters: {source}"),
                )
            })?;
        let manager_timeout = params.manager_timeout().map_err(|reason| {
            cluster_resync_rejection(
                &state.prom,
                StatusCode::BAD_REQUEST,
                "validation_error",
                reason,
            )
        })?;

        let body_deadline = Instant::now()
            .checked_add(CLUSTER_RESYNC_BODY_TIMEOUT)
            .unwrap_or_else(Instant::now);
        let body = tokio::time::timeout(
            CLUSTER_RESYNC_BODY_TIMEOUT,
            Bytes::from_request(request, state),
        )
        .await
        .map_err(|_| {
            cluster_resync_rejection(
                &state.prom,
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "resync request body did not complete within 250ms",
            )
        })?
        .map_err(|source| {
            let status = source.status();
            let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload_too_large"
            } else {
                "validation_error"
            };
            cluster_resync_rejection(
                &state.prom,
                status,
                error_type,
                format!("invalid resync body: {source}"),
            )
        })?;
        // Tokio polls a ready body before its timeout timer. Enforce the
        // absolute boundary too when this task was starved past the deadline.
        if Instant::now() >= body_deadline {
            return Err(cluster_resync_rejection(
                &state.prom,
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "resync request body did not complete within 250ms",
            ));
        }
        if !body.is_empty() {
            return Err(cluster_resync_rejection(
                &state.prom,
                StatusCode::BAD_REQUEST,
                "validation_error",
                "POST /_cluster/resync does not accept a request body",
            ));
        }

        Ok(Self {
            duration,
            started,
            manager_timeout,
        })
    }
}

#[derive(Clone, Copy)]
enum ResyncStart {
    Queued,
    Started,
    Cancelled,
}

fn begin_cluster_resync(gate: &Mutex<ResyncStart>, deadline: Instant, no_wait: bool) -> bool {
    let mut start = gate.lock();
    if matches!(*start, ResyncStart::Cancelled) || (!no_wait && Instant::now() >= deadline) {
        *start = ResyncStart::Cancelled;
        return false;
    }
    *start = ResyncStart::Started;
    true
}

fn cancel_queued_cluster_resync(gate: &Mutex<ResyncStart>) -> bool {
    let mut start = gate.lock();
    match *start {
        ResyncStart::Queued | ResyncStart::Cancelled => {
            *start = ResyncStart::Cancelled;
            true
        }
        ResyncStart::Started => false,
    }
}

struct CancelQueuedClusterResync(Arc<Mutex<ResyncStart>>);

impl Drop for CancelQueuedClusterResync {
    fn drop(&mut self) {
        let _ = cancel_queued_cluster_resync(&self.0);
    }
}

#[derive(Debug)]
struct ClusterResyncSuccess {
    report: ResyncReport,
    took_ms: f64,
}

enum ClusterResyncWorkerOutcome {
    NotStarted,
    Finished(ClusterResyncSuccess),
}

type ClusterResyncWorkerResult = Result<ClusterResyncWorkerOutcome, ClusterResyncWorkerFailure>;

#[derive(Serialize)]
struct ClusterResyncResponse {
    took: u64,
    took_ms: f64,
    /// The requested pass ran to completion. Individual unreachable targets
    /// remain visible through `still_pending` and can be retried safely.
    acknowledged: bool,
    repaired: usize,
    still_pending: usize,
}

/// Re-drive every mutation currently queued after a partial cross-position
/// apply. Admission, the exclusive REST-writer wait, and synchronous shard RPCs
/// all stay off Tokio. Once the pass starts it runs to an exact terminal report,
/// including after an HTTP disconnect.
#[instrument(skip_all)]
pub(crate) async fn cluster_resync(
    State(state): State<Arc<ClusterAppState>>,
    transport: ClusterResyncTransport,
) -> Response {
    let (_duration, started, manager_timeout) = transport.into_parts();
    let no_wait = manager_timeout.is_zero();
    let Some(deadline) = Instant::now().checked_add(manager_timeout) else {
        return cluster_resync_rejection(
            &state.prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            "resync manager timeout is too large for this platform",
        );
    };

    let permit = if no_wait {
        match Arc::clone(&state.stats_permits).try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => {
                return cluster_resync_not_started_timeout(&state.prom);
            }
            Err(TryAcquireError::Closed) => {
                return cluster_resync_rejection(
                    &state.prom,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "resync_unavailable",
                    "resync admission is closed",
                );
            }
        }
    } else {
        let Some(admission_budget) = deadline.checked_duration_since(Instant::now()) else {
            return cluster_resync_not_started_timeout(&state.prom);
        };
        match tokio::time::timeout(
            admission_budget,
            Arc::clone(&state.stats_permits).acquire_owned(),
        )
        .await
        {
            Err(_) => return cluster_resync_not_started_timeout(&state.prom),
            Ok(Err(_)) => {
                return cluster_resync_rejection(
                    &state.prom,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "resync_unavailable",
                    "resync admission is closed",
                );
            }
            Ok(Ok(permit)) => permit,
        }
    };
    if !no_wait && Instant::now() >= deadline {
        return cluster_resync_not_started_timeout(&state.prom);
    }

    let worker_state = Arc::clone(&state);
    let gate = Arc::new(Mutex::new(ResyncStart::Queued));
    let _cancel_queued_on_drop = CancelQueuedClusterResync(Arc::clone(&gate));
    let worker_gate = Arc::clone(&gate);
    let (started_sender, mut started_receiver) = tokio::sync::oneshot::channel();
    let completion = match supervise_cluster_resync_worker(move || {
        let _permit = permit;
        let writes = if no_wait {
            worker_state.write_serial.try_lock()
        } else {
            deadline
                .checked_duration_since(Instant::now())
                .and_then(|budget| worker_state.write_serial.try_lock_for(budget))
        };
        let Some(_writes) = writes else {
            return ClusterResyncWorkerOutcome::NotStarted;
        };
        let cluster = if no_wait {
            worker_state.cluster.try_read()
        } else {
            deadline
                .checked_duration_since(Instant::now())
                .and_then(|budget| worker_state.cluster.try_read_for(budget))
        };
        let Some(cluster) = cluster else {
            return ClusterResyncWorkerOutcome::NotStarted;
        };
        if !begin_cluster_resync(&worker_gate, deadline, no_wait) {
            return ClusterResyncWorkerOutcome::NotStarted;
        }
        let _ = started_sender.send(());
        let report = cluster.resync();
        ClusterResyncWorkerOutcome::Finished(ClusterResyncSuccess {
            report,
            took_ms: started.elapsed().as_secs_f64() * 1_000.0,
        })
    }) {
        Ok(completion) => completion,
        Err(source) => {
            error!(error = %source, "failed to dispatch dedicated resync worker");
            return cluster_resync_rejection(
                &state.prom,
                StatusCode::SERVICE_UNAVAILABLE,
                "resync_unavailable",
                "resync worker could not be started",
            );
        }
    };
    let mut completion = completion;

    if no_wait {
        return match completion.await {
            Ok(outcome) => finish_cluster_resync_worker(&state.prom, outcome),
            Err(source) => cluster_resync_supervisor_failed(&state.prom, &source),
        };
    }

    let sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
    tokio::pin!(sleep);
    tokio::select! {
        outcome = &mut completion => match outcome {
            Ok(outcome) => finish_cluster_resync_worker(&state.prom, outcome),
            Err(source) => cluster_resync_supervisor_failed(&state.prom, &source),
        },
        started = &mut started_receiver => {
            if started.is_err() {
                warn!("resync worker ended without sending its start signal");
            }
            match completion.await {
                Ok(outcome) => finish_cluster_resync_worker(&state.prom, outcome),
                Err(source) => cluster_resync_supervisor_failed(&state.prom, &source),
            }
        },
        () = &mut sleep => {
            if cancel_queued_cluster_resync(&gate) {
                cluster_resync_not_started_timeout(&state.prom)
            } else {
                // A pass can repair some positions before reaching a slow or
                // failing one. Once started, cancellation cannot truthfully
                // promise that the queue and shards stayed unchanged.
                match completion.await {
                    Ok(outcome) => finish_cluster_resync_worker(&state.prom, outcome),
                    Err(source) => cluster_resync_supervisor_failed(&state.prom, &source),
                }
            }
        }
    }
}

fn finish_cluster_resync_worker(
    prom: &PrometheusMetrics,
    outcome: ClusterResyncWorkerResult,
) -> Response {
    match outcome {
        Err(_) => cluster_resync_rejection(
            prom,
            StatusCode::INTERNAL_SERVER_ERROR,
            "resync_unavailable",
            "resync worker failed",
        ),
        Ok(ClusterResyncWorkerOutcome::NotStarted) => cluster_resync_not_started_timeout(prom),
        Ok(ClusterResyncWorkerOutcome::Finished(success)) => finish_cluster_resync_response(
            prom,
            Json(ClusterResyncResponse {
                took: success.took_ms.floor() as u64,
                took_ms: success.took_ms,
                acknowledged: true,
                repaired: success.report.repaired,
                still_pending: success.report.still_pending,
            })
            .into_response(),
        ),
    }
}

fn cluster_resync_supervisor_failed(
    prom: &PrometheusMetrics,
    source: &tokio::sync::oneshot::error::RecvError,
) -> Response {
    error!(error = %source, "resync completion supervisor failed");
    cluster_resync_rejection(
        prom,
        StatusCode::INTERNAL_SERVER_ERROR,
        "resync_unavailable",
        "resync completion supervisor failed",
    )
}

fn cluster_resync_not_started_timeout(prom: &PrometheusMetrics) -> Response {
    cluster_resync_rejection(
        prom,
        StatusCode::REQUEST_TIMEOUT,
        "resync_timeout",
        "timed out waiting for resync admission or exclusive cluster access; no resync pass was started",
    )
}

fn cluster_resync_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    finish_cluster_resync_response(
        prom,
        ApiError::response(status, error_type, reason).into_response(),
    )
}

fn finish_cluster_resync_response(prom: &PrometheusMetrics, mut response: Response) -> Response {
    prom.http_requests_total
        .with_label_values(&[CLUSTER_RESYNC_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
