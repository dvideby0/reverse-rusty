//! Strict native `POST /_cluster/resize` in-process blue/green rebuild.
//!
//! Elasticsearch and OpenSearch resize one named index into a distinct target
//! index through `_split` or `_shrink`. Reverse Rusty instead replaces one
//! in-process reverse-query ring in place after rebuilding the complete live
//! corpus. Keep that semantic boundary explicit while adopting the manager
//! timeout spellings that map exactly to waiting for administrative admission
//! and exclusive topology access before the rebuild starts.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::Bytes,
    extract::{FromRequest, Query, Request, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use parking_lot::Mutex;
use prometheus::HistogramTimer;
use serde::{Deserialize, Serialize};
use tokio::sync::TryAcquireError;
use tracing::{error, instrument, warn};

use reverse_rusty::cluster::ShardError;

use crate::dto::ApiError;
use crate::handlers::search::parse_named_time_value;
use crate::metrics::PrometheusMetrics;
use crate::state::{ClusterAppState, ClusterRebalanceTopology};

use super::super::shard_error_status;

mod supervisor;

use supervisor::{supervise_cluster_resize_worker, ClusterResizeWorkerFailure};

pub(crate) const CLUSTER_RESIZE_BODY_LIMIT: usize = 64 * 1024;
pub(crate) const CLUSTER_RESIZE_BODY_TIMEOUT: Duration = Duration::from_millis(250);
const DEFAULT_CLUSTER_RESIZE_MANAGER_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CLUSTER_RESIZE_MANAGER_TIMEOUT: Duration = Duration::from_secs(30);
/// One ring entry carries 128 virtual nodes by default. Bound the public API so
/// a tiny JSON request cannot allocate an effectively unbounded ring/shard set.
const MAX_CLUSTER_RESIZE_SHARDS: usize = 1_024;
const CLUSTER_RESIZE_ENDPOINT: &str = "cluster_resize";

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterResizeParams {
    /// OpenSearch-inclusive spelling.
    cluster_manager_timeout: Option<String>,
    /// Elasticsearch and legacy OpenSearch spelling.
    master_timeout: Option<String>,
}

impl ClusterResizeParams {
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
            .map(parse_cluster_resize_manager_timeout)
            .transpose()?
            .unwrap_or(DEFAULT_CLUSTER_RESIZE_MANAGER_TIMEOUT);
        if timeout > MAX_CLUSTER_RESIZE_MANAGER_TIMEOUT {
            return Err("resize manager timeout must not exceed 30s".to_string());
        }
        Ok(timeout)
    }
}

fn parse_cluster_resize_manager_timeout(raw: &str) -> Result<Duration, String> {
    if raw == "0" {
        return Ok(Duration::ZERO);
    }
    parse_named_time_value("cluster_manager_timeout/master_timeout", raw)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterResizeBody {
    num_shards: usize,
}

impl ClusterResizeBody {
    fn validate(self) -> Result<usize, String> {
        if self.num_shards == 0 {
            return Err("`num_shards` must be at least 1".to_string());
        }
        if self.num_shards > MAX_CLUSTER_RESIZE_SHARDS {
            return Err(format!(
                "`num_shards` must not exceed {MAX_CLUSTER_RESIZE_SHARDS}"
            ));
        }
        Ok(self.num_shards)
    }
}

pub(crate) struct ClusterResizeTransport {
    duration: HistogramTimer,
    manager_timeout: Duration,
    num_shards: usize,
}

impl ClusterResizeTransport {
    fn into_parts(self) -> (HistogramTimer, Duration, usize) {
        (self.duration, self.manager_timeout, self.num_shards)
    }
}

impl FromRequest<Arc<ClusterAppState>> for ClusterResizeTransport {
    type Rejection = Response;

    async fn from_request(
        request: Request,
        state: &Arc<ClusterAppState>,
    ) -> Result<Self, Self::Rejection> {
        let duration = state
            .prom
            .http_request_duration
            .with_label_values(&[CLUSTER_RESIZE_ENDPOINT])
            .start_timer();
        if request.method() != Method::POST {
            let mut response = cluster_resize_rejection(
                &state.prom,
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "POST is the only supported /_cluster/resize method",
            );
            response
                .headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static("POST"));
            return Err(response);
        }

        let Query(params) =
            Query::<ClusterResizeParams>::try_from_uri(request.uri()).map_err(|source| {
                cluster_resize_rejection(
                    &state.prom,
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    format!("invalid resize query parameters: {source}"),
                )
            })?;
        let manager_timeout = params.manager_timeout().map_err(|reason| {
            cluster_resize_rejection(
                &state.prom,
                StatusCode::BAD_REQUEST,
                "validation_error",
                reason,
            )
        })?;
        if !is_json_content_type(request.headers()) {
            return Err(cluster_resize_rejection(
                &state.prom,
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                "POST /_cluster/resize requires Content-Type: application/json",
            ));
        }

        let body_deadline = Instant::now()
            .checked_add(CLUSTER_RESIZE_BODY_TIMEOUT)
            .unwrap_or_else(Instant::now);
        let bytes = tokio::time::timeout(
            CLUSTER_RESIZE_BODY_TIMEOUT,
            Bytes::from_request(request, state),
        )
        .await
        .map_err(|_| {
            cluster_resize_rejection(
                &state.prom,
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "resize body did not complete within 250ms",
            )
        })?;
        // Tokio polls a ready body before its timeout timer. Enforce the
        // absolute boundary too when this task was starved past the deadline.
        if Instant::now() >= body_deadline {
            return Err(cluster_resize_rejection(
                &state.prom,
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "resize body did not complete within 250ms",
            ));
        }
        let bytes = bytes.map_err(|source| {
            let status = source.status();
            let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload_too_large"
            } else {
                "validation_error"
            };
            cluster_resize_rejection(
                &state.prom,
                status,
                error_type,
                format!("invalid resize body: {source}"),
            )
        })?;
        if bytes
            .iter()
            .find(|byte| !byte.is_ascii_whitespace())
            .copied()
            != Some(b'{')
        {
            return Err(cluster_resize_rejection(
                &state.prom,
                StatusCode::BAD_REQUEST,
                "validation_error",
                "the resize JSON body must be an object",
            ));
        }
        let num_shards = serde_json::from_slice::<ClusterResizeBody>(&bytes)
            .map_err(|source| {
                cluster_resize_rejection(
                    &state.prom,
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    format!("invalid resize JSON body: {source}"),
                )
            })?
            .validate()
            .map_err(|reason| {
                cluster_resize_rejection(
                    &state.prom,
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    reason,
                )
            })?;

        Ok(Self {
            duration,
            manager_timeout,
            num_shards,
        })
    }
}

fn is_json_content_type(headers: &HeaderMap) -> bool {
    let Some(value) = headers.get(header::CONTENT_TYPE) else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };
    let media_type = value
        .split_once(';')
        .map_or(value, |(media_type, _)| media_type)
        .trim()
        .to_ascii_lowercase();
    media_type == "application/json"
        || media_type
            .strip_prefix("application/")
            .is_some_and(|subtype| subtype.ends_with("+json"))
}

#[derive(Clone, Copy)]
enum ResizeStart {
    Queued,
    Started,
    Cancelled,
}

fn begin_cluster_resize(gate: &Mutex<ResizeStart>, deadline: Instant, no_wait: bool) -> bool {
    let mut start = gate.lock();
    if matches!(*start, ResizeStart::Cancelled) || (!no_wait && Instant::now() >= deadline) {
        *start = ResizeStart::Cancelled;
        return false;
    }
    *start = ResizeStart::Started;
    true
}

fn cancel_queued_cluster_resize(gate: &Mutex<ResizeStart>) -> bool {
    let mut start = gate.lock();
    match *start {
        ResizeStart::Queued | ResizeStart::Cancelled => {
            *start = ResizeStart::Cancelled;
            true
        }
        ResizeStart::Started => false,
    }
}

struct CancelQueuedClusterResize(Arc<Mutex<ResizeStart>>);

impl Drop for CancelQueuedClusterResize {
    fn drop(&mut self) {
        let _ = cancel_queued_cluster_resize(&self.0);
    }
}

#[derive(Debug)]
struct ClusterResizeSuccess {
    old_num_shards: usize,
    num_shards: usize,
    rebuilt: usize,
    version: u64,
}

enum ClusterResizeWorkerOutcome {
    NotStarted,
    Finished(Result<ClusterResizeSuccess, ShardError>),
}

type ClusterResizeWorkerResult = Result<ClusterResizeWorkerOutcome, ClusterResizeWorkerFailure>;

#[derive(Serialize)]
struct ClusterResizeResponse {
    acknowledged: bool,
    shards_acknowledged: bool,
    version: u64,
    old_num_shards: usize,
    num_shards: usize,
    rebuilt: usize,
}

/// Rebuild every live query under a fresh in-process ring and atomically swap
/// the serving cluster. Admission and all blocking locks remain off Tokio.
#[instrument(skip_all)]
pub(crate) async fn cluster_resize(
    State(state): State<Arc<ClusterAppState>>,
    transport: ClusterResizeTransport,
) -> Response {
    let (_duration, manager_timeout, num_shards) = transport.into_parts();
    if state.rebalance_topology != ClusterRebalanceTopology::InProcess {
        return cluster_resize_rejection(
            &state.prom,
            StatusCode::NOT_IMPLEMENTED,
            "not_supported_in_cluster_mode",
            "remote cluster resize is not implemented; build a separate cluster at the target \
             shard count, re-ingest and validate the corpus, then cut traffic over",
        );
    }

    let no_wait = manager_timeout.is_zero();
    let Some(deadline) = Instant::now().checked_add(manager_timeout) else {
        return cluster_resize_rejection(
            &state.prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            "resize manager timeout is too large for this platform",
        );
    };
    let permit = if no_wait {
        match Arc::clone(&state.stats_permits).try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => {
                return cluster_resize_not_started_timeout(&state.prom);
            }
            Err(TryAcquireError::Closed) => {
                return cluster_resize_rejection(
                    &state.prom,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "resize_unavailable",
                    "resize admission is closed",
                );
            }
        }
    } else {
        let Some(admission_budget) = deadline.checked_duration_since(Instant::now()) else {
            return cluster_resize_not_started_timeout(&state.prom);
        };
        match tokio::time::timeout(
            admission_budget,
            Arc::clone(&state.stats_permits).acquire_owned(),
        )
        .await
        {
            Err(_) => return cluster_resize_not_started_timeout(&state.prom),
            Ok(Err(_)) => {
                return cluster_resize_rejection(
                    &state.prom,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "resize_unavailable",
                    "resize admission is closed",
                );
            }
            Ok(Ok(permit)) => permit,
        }
    };
    if !no_wait && Instant::now() >= deadline {
        return cluster_resize_not_started_timeout(&state.prom);
    }

    let worker_state = Arc::clone(&state);
    let gate = Arc::new(Mutex::new(ResizeStart::Queued));
    let _cancel_queued_on_drop = CancelQueuedClusterResize(Arc::clone(&gate));
    let worker_gate = Arc::clone(&gate);
    let (started_sender, mut started_receiver) = tokio::sync::oneshot::channel();
    let completion = match supervise_cluster_resize_worker(move || {
        let _permit = permit;
        let topology = if no_wait {
            worker_state.topology_guard.try_write()
        } else {
            deadline
                .checked_duration_since(Instant::now())
                .and_then(|budget| worker_state.topology_guard.try_write_for(budget))
        };
        let Some(_topology) = topology else {
            return ClusterResizeWorkerOutcome::NotStarted;
        };
        let writes = if no_wait {
            worker_state.write_serial.try_lock()
        } else {
            deadline
                .checked_duration_since(Instant::now())
                .and_then(|budget| worker_state.write_serial.try_lock_for(budget))
        };
        let Some(_writes) = writes else {
            return ClusterResizeWorkerOutcome::NotStarted;
        };
        let cluster = if no_wait {
            worker_state.cluster.try_write()
        } else {
            deadline
                .checked_duration_since(Instant::now())
                .and_then(|budget| worker_state.cluster.try_write_for(budget))
        };
        let Some(mut cluster) = cluster else {
            return ClusterResizeWorkerOutcome::NotStarted;
        };
        if !begin_cluster_resize(&worker_gate, deadline, no_wait) {
            return ClusterResizeWorkerOutcome::NotStarted;
        }
        let _ = started_sender.send(());
        let old_num_shards = cluster.num_shards();
        let result = cluster.resize(num_shards).and_then(|rebuilt| {
            let control = cluster.control_state()?;
            let placement_generation = cluster.placement_generation().0;
            if control.num_shards as usize != num_shards
                || control.placement_generation != placement_generation
            {
                return Err(ShardError::ControlPlane(format!(
                    "resize terminal attestation failed: serving state is generation \
                     {placement_generation}/{num_shards} shards but committed control state is \
                     generation {}/{} shards",
                    control.placement_generation, control.num_shards
                )));
            }
            Ok(ClusterResizeSuccess {
                old_num_shards,
                num_shards,
                rebuilt,
                version: control.epoch,
            })
        });
        ClusterResizeWorkerOutcome::Finished(result)
    }) {
        Ok(completion) => completion,
        Err(source) => {
            error!(error = %source, "failed to dispatch dedicated resize worker");
            return cluster_resize_rejection(
                &state.prom,
                StatusCode::SERVICE_UNAVAILABLE,
                "resize_unavailable",
                "resize worker could not be started",
            );
        }
    };
    let mut completion = completion;

    if no_wait {
        return match completion.await {
            Ok(outcome) => finish_cluster_resize_worker(&state.prom, outcome),
            Err(source) => cluster_resize_supervisor_failed(&state.prom, &source),
        };
    }

    let sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
    tokio::pin!(sleep);
    tokio::select! {
        outcome = &mut completion => match outcome {
            Ok(outcome) => finish_cluster_resize_worker(&state.prom, outcome),
            Err(source) => cluster_resize_supervisor_failed(&state.prom, &source),
        },
        started = &mut started_receiver => {
            if started.is_err() {
                warn!("resize worker ended without sending its start signal");
            }
            match completion.await {
                Ok(outcome) => finish_cluster_resize_worker(&state.prom, outcome),
                Err(source) => cluster_resize_supervisor_failed(&state.prom, &source),
            }
        },
        () = &mut sleep => {
            if cancel_queued_cluster_resize(&gate) {
                cluster_resize_not_started_timeout(&state.prom)
            } else {
                // The worker acquired every exclusive guard and started before
                // the manager deadline. A blue/green swap cannot be cancelled
                // safely at an arbitrary HTTP deadline, so await its outcome.
                match completion.await {
                    Ok(outcome) => finish_cluster_resize_worker(&state.prom, outcome),
                    Err(source) => cluster_resize_supervisor_failed(&state.prom, &source),
                }
            }
        }
    }
}

fn finish_cluster_resize_worker(
    prom: &PrometheusMetrics,
    outcome: ClusterResizeWorkerResult,
) -> Response {
    match outcome {
        Err(_) => cluster_resize_rejection(
            prom,
            StatusCode::INTERNAL_SERVER_ERROR,
            "resize_unavailable",
            "resize worker failed",
        ),
        Ok(ClusterResizeWorkerOutcome::NotStarted) => cluster_resize_not_started_timeout(prom),
        Ok(ClusterResizeWorkerOutcome::Finished(Err(source))) => {
            let status = shard_error_status(&source);
            let status = if status.is_success() {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                status
            };
            let (_, error_type) = source.write_http_class();
            cluster_resize_rejection(
                prom,
                status,
                error_type,
                "resize did not produce an attested terminal cluster state; inspect \
                 /_health and /_cluster/state before retrying",
            )
        }
        Ok(ClusterResizeWorkerOutcome::Finished(Ok(success))) => finish_cluster_resize_response(
            prom,
            Json(ClusterResizeResponse {
                acknowledged: true,
                shards_acknowledged: true,
                version: success.version,
                old_num_shards: success.old_num_shards,
                num_shards: success.num_shards,
                rebuilt: success.rebuilt,
            })
            .into_response(),
        ),
    }
}

fn cluster_resize_supervisor_failed(
    prom: &PrometheusMetrics,
    source: &tokio::sync::oneshot::error::RecvError,
) -> Response {
    error!(error = %source, "resize completion supervisor failed");
    cluster_resize_rejection(
        prom,
        StatusCode::INTERNAL_SERVER_ERROR,
        "resize_unavailable",
        "resize completion supervisor failed",
    )
}

fn cluster_resize_not_started_timeout(prom: &PrometheusMetrics) -> Response {
    cluster_resize_rejection(
        prom,
        StatusCode::REQUEST_TIMEOUT,
        "resize_timeout",
        "timed out waiting for resize admission or exclusive cluster access; no resize was started",
    )
}

fn cluster_resize_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    finish_cluster_resize_response(
        prom,
        ApiError::response(status, error_type, reason).into_response(),
    )
}

fn finish_cluster_resize_response(prom: &PrometheusMetrics, mut response: Response) -> Response {
    prom.http_requests_total
        .with_label_values(&[CLUSTER_RESIZE_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
