//! Strict authoritative `GET`/`HEAD /_cluster/state` control-plane read.
//!
//! Elasticsearch and OpenSearch use the same path for a much larger internal
//! index/routing document. Reverse Rusty keeps its native state schema, while
//! adopting only the controls whose semantics map exactly.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::Bytes,
    extract::{FromRequest, Query, Request, State},
    http::{header, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
};
use prometheus::HistogramTimer;
use serde::{Deserialize, Serialize};
use tokio::sync::TryAcquireError;
use tracing::{error, warn};

use reverse_rusty::cluster::{ClusterState, ShardError};

use crate::dto::ApiError;
use crate::handlers::search::parse_named_time_value;
use crate::metrics::PrometheusMetrics;
use crate::state::ClusterAppState;

use super::shard_error_status;

pub(crate) const CLUSTER_STATE_BODY_LIMIT: usize = 64 * 1024;
pub(crate) const CLUSTER_STATE_BODY_TIMEOUT: Duration = Duration::from_millis(250);
const DEFAULT_CLUSTER_STATE_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CLUSTER_STATE_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CLUSTER_STATE_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const CLUSTER_STATE_ENDPOINT: &str = "cluster_state";

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterStateParams {
    /// Exact familiar behavior: this endpoint always obtains authoritative
    /// control state, so only the default `false` value is representable.
    #[serde(default)]
    local: bool,
    /// OpenSearch-inclusive spelling.
    cluster_manager_timeout: Option<String>,
    /// Elasticsearch and legacy OpenSearch spelling.
    master_timeout: Option<String>,
    /// Accepted honestly: this native state has no nested settings projection,
    /// so either value is representation-identical.
    #[serde(default, rename = "flat_settings")]
    _flat_settings: bool,
}

struct ClusterStateRequest {
    timeout: Duration,
    selection: ClusterStateSelection,
}

impl ClusterStateRequest {
    fn resolve(params: ClusterStateParams, path: &str) -> Result<Self, String> {
        if params.local {
            return Err(
                "`local=true` is unsupported: Reverse Rusty exposes only the authoritative \
                 linearizable control-plane document"
                    .to_string(),
            );
        }
        if params.cluster_manager_timeout.is_some() && params.master_timeout.is_some() {
            return Err(
                "`cluster_manager_timeout` and `master_timeout` are aliases; specify exactly one"
                    .to_string(),
            );
        }
        let timeout = params
            .cluster_manager_timeout
            .or(params.master_timeout)
            .as_deref()
            .map(parse_cluster_state_timeout)
            .transpose()?
            .unwrap_or(DEFAULT_CLUSTER_STATE_TIMEOUT);
        if timeout > MAX_CLUSTER_STATE_TIMEOUT {
            return Err("cluster-state timeout must not exceed 30s".to_string());
        }
        let selection = match path {
            "/_cluster/state" | "/_cluster/state/_all" => ClusterStateSelection::All,
            "/_cluster/state/version" => ClusterStateSelection::Version,
            path if path.starts_with("/_cluster/state/") => {
                return Err(
                    "only the `_all` and `version` cluster-state metrics are supported; \
                     Reverse Rusty has no index metadata or index routing-table target"
                        .to_string(),
                );
            }
            _ => return Err("invalid cluster-state path".to_string()),
        };
        Ok(Self { timeout, selection })
    }
}

#[derive(Clone, Copy)]
enum ClusterStateSelection {
    All,
    Version,
}

fn parse_cluster_state_timeout(raw: &str) -> Result<Duration, String> {
    if raw == "0" {
        return Ok(Duration::ZERO);
    }
    parse_named_time_value("cluster_manager_timeout/master_timeout", raw)
}

pub(crate) struct ClusterStateTransport {
    duration: HistogramTimer,
    head: bool,
    request: ClusterStateRequest,
}

impl ClusterStateTransport {
    fn into_parts(self) -> (HistogramTimer, bool, ClusterStateRequest) {
        (self.duration, self.head, self.request)
    }
}

impl FromRequest<Arc<ClusterAppState>> for ClusterStateTransport {
    type Rejection = Response;

    async fn from_request(
        request: Request,
        state: &Arc<ClusterAppState>,
    ) -> Result<Self, Self::Rejection> {
        let duration = state
            .prom
            .http_request_duration
            .with_label_values(&[CLUSTER_STATE_ENDPOINT])
            .start_timer();
        let head = request.method() == Method::HEAD;
        if request.method() != Method::GET && !head {
            let mut response = cluster_state_rejection(
                &state.prom,
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "GET and HEAD are the only supported /_cluster/state methods",
                false,
            );
            response
                .headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static("GET, HEAD"));
            return Err(response);
        }

        let Query(params) =
            Query::<ClusterStateParams>::try_from_uri(request.uri()).map_err(|source| {
                cluster_state_rejection(
                    &state.prom,
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    format!("invalid cluster-state query parameters: {source}"),
                    head,
                )
            })?;
        let resolved =
            ClusterStateRequest::resolve(params, request.uri().path()).map_err(|reason| {
                cluster_state_rejection(
                    &state.prom,
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    reason,
                    head,
                )
            })?;
        let body = tokio::time::timeout(
            CLUSTER_STATE_BODY_TIMEOUT,
            Bytes::from_request(request, state),
        )
        .await
        .map_err(|_| {
            cluster_state_rejection(
                &state.prom,
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "cluster-state request body did not complete within 250ms",
                head,
            )
        })?
        .map_err(|source| {
            let status = source.status();
            let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload_too_large"
            } else {
                "validation_error"
            };
            cluster_state_rejection(
                &state.prom,
                status,
                error_type,
                format!("invalid cluster-state body: {source}"),
                head,
            )
        })?;
        if !body.is_empty() {
            return Err(cluster_state_rejection(
                &state.prom,
                StatusCode::BAD_REQUEST,
                "validation_error",
                "GET/HEAD /_cluster/state does not accept a request body",
                head,
            ));
        }

        Ok(Self {
            duration,
            head,
            request: resolved,
        })
    }
}

#[derive(Serialize)]
struct ClusterStateResponse {
    /// Elasticsearch/OpenSearch-familiar exact alias for the application-level
    /// committed state version.
    version: u64,
    #[serde(flatten)]
    state: ClusterState,
}

enum ClusterStateWorkerError {
    Read(ShardError),
    Serialization(serde_json::Error),
    ResponseTooLarge(usize),
}

fn collect_cluster_state(
    state: &ClusterAppState,
    selection: ClusterStateSelection,
) -> Result<Vec<u8>, ClusterStateWorkerError> {
    let cluster = state.cluster.read();
    let encoded = match selection {
        ClusterStateSelection::All => {
            let doc = cluster
                .control_state()
                .map_err(ClusterStateWorkerError::Read)?;
            serde_json::to_vec(&ClusterStateResponse {
                version: doc.epoch,
                state: doc,
            })
        }
        ClusterStateSelection::Version => {
            let version = cluster
                .control_version()
                .map_err(ClusterStateWorkerError::Read)?;
            serde_json::to_vec(&serde_json::json!({"version": version.0}))
        }
    };
    let encoded = encoded.map_err(ClusterStateWorkerError::Serialization)?;
    if encoded.len() > MAX_CLUSTER_STATE_RESPONSE_BYTES {
        return Err(ClusterStateWorkerError::ResponseTooLarge(encoded.len()));
    }
    Ok(encoded)
}

/// Return one authoritative committed state document. Admission, cluster-lock
/// waiting, any remote linearizable RPC, and JSON serialization all run away
/// from Tokio request workers.
pub(crate) async fn cluster_state(
    State(state): State<Arc<ClusterAppState>>,
    transport: ClusterStateTransport,
) -> Response {
    let (_duration, head, request) = transport.into_parts();
    let no_wait = request.timeout.is_zero();
    let started = Instant::now();
    let Some(deadline) = started.checked_add(request.timeout) else {
        return cluster_state_rejection(
            &state.prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            "cluster-state timeout is too large for this platform",
            head,
        );
    };
    let permit = if no_wait {
        match Arc::clone(&state.stats_permits).try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => {
                return cluster_state_timeout(&state.prom, head);
            }
            Err(TryAcquireError::Closed) => {
                return cluster_state_rejection(
                    &state.prom,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "cluster_state_unavailable",
                    "cluster-state admission is closed",
                    head,
                );
            }
        }
    } else {
        let Some(admission_budget) = deadline.checked_duration_since(Instant::now()) else {
            return cluster_state_timeout(&state.prom, head);
        };
        match tokio::time::timeout(
            admission_budget,
            Arc::clone(&state.stats_permits).acquire_owned(),
        )
        .await
        {
            Err(_) => return cluster_state_timeout(&state.prom, head),
            Ok(Err(_)) => {
                return cluster_state_rejection(
                    &state.prom,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "cluster_state_unavailable",
                    "cluster-state admission is closed",
                    head,
                );
            }
            Ok(Ok(permit)) => permit,
        }
    };

    // Tokio polls the inner future before its elapsed timer. If this task was
    // starved while admission became available after the deadline, the
    // acquisition above can still report success. Do not launch expired
    // synchronous control-plane work in that case. Zero is intentionally a
    // non-queuing admission probe: once admitted, it executes one read.
    if !no_wait && Instant::now() >= deadline {
        return cluster_state_timeout(&state.prom, head);
    }

    let worker_state = Arc::clone(&state);
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        collect_cluster_state(&worker_state, request.selection)
    });
    if no_wait {
        return finish_cluster_state_worker(&state.prom, head, worker.await);
    }
    let Some(read_budget) = deadline.checked_duration_since(Instant::now()) else {
        return cluster_state_timeout(&state.prom, head);
    };
    let outcome = tokio::time::timeout(read_budget, worker).await;
    // The join handle is also polled before the timeout timer. A result that
    // became ready while this task was starved after the deadline must not turn
    // an expired request into a successful (or backend-error) response.
    if Instant::now() >= deadline {
        if outcome.is_err() {
            warn!("cluster-state read exceeded its request deadline; detached read will finish");
        }
        return cluster_state_timeout(&state.prom, head);
    }
    match outcome {
        Err(_) => {
            warn!("cluster-state read exceeded its request deadline; detached read will finish");
            cluster_state_timeout(&state.prom, head)
        }
        Ok(outcome) => finish_cluster_state_worker(&state.prom, head, outcome),
    }
}

fn finish_cluster_state_worker(
    prom: &PrometheusMetrics,
    head: bool,
    outcome: Result<Result<Vec<u8>, ClusterStateWorkerError>, tokio::task::JoinError>,
) -> Response {
    match outcome {
        Err(join_error) => {
            error!(error = %join_error, "cluster-state worker failed");
            cluster_state_rejection(
                prom,
                StatusCode::INTERNAL_SERVER_ERROR,
                "cluster_state_unavailable",
                "cluster-state worker failed",
                head,
            )
        }
        Ok(Err(ClusterStateWorkerError::Read(source))) => {
            let status = shard_error_status(&source);
            error!(error = %source, "authoritative cluster-state read failed");
            cluster_state_rejection(
                prom,
                status,
                "control_plane_error",
                "authoritative cluster state is unavailable",
                head,
            )
        }
        Ok(Err(ClusterStateWorkerError::Serialization(source))) => {
            error!(error = %source, "cluster-state serialization failed");
            cluster_state_rejection(
                prom,
                StatusCode::INTERNAL_SERVER_ERROR,
                "cluster_state_unavailable",
                "cluster-state serialization failed",
                head,
            )
        }
        Ok(Err(ClusterStateWorkerError::ResponseTooLarge(actual))) => {
            error!(
                actual,
                limit = MAX_CLUSTER_STATE_RESPONSE_BYTES,
                "cluster-state response exceeded its fixed serialization limit"
            );
            cluster_state_rejection(
                prom,
                StatusCode::SERVICE_UNAVAILABLE,
                "cluster_state_too_large",
                "cluster-state response exceeds the 8 MiB safety limit",
                head,
            )
        }
        Ok(Ok(encoded)) => finish_cluster_state_response(
            prom,
            (
                StatusCode::OK,
                [(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                )],
                encoded,
            )
                .into_response(),
            head,
        ),
    }
}

fn cluster_state_timeout(prom: &PrometheusMetrics, head: bool) -> Response {
    cluster_state_rejection(
        prom,
        StatusCode::REQUEST_TIMEOUT,
        "cluster_state_timeout",
        "timed out waiting for the authoritative cluster state",
        head,
    )
}

fn cluster_state_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
    head: bool,
) -> Response {
    finish_cluster_state_response(
        prom,
        ApiError::response(status, error_type, reason).into_response(),
        head,
    )
}

fn finish_cluster_state_response(
    prom: &PrometheusMetrics,
    mut response: Response,
    _head: bool,
) -> Response {
    prom.http_requests_total
        .with_label_values(&[CLUSTER_STATE_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    // Keep the representation body intact here. Axum's top-level route sets
    // its exact Content-Length before stripping it for HEAD.
    response
}
