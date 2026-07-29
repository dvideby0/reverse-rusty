//! Strict native `DELETE /_cluster/nodes/{id}` descriptor deregistration.
//!
//! Elasticsearch and OpenSearch use lifecycle/allocation procedures rather
//! than a REST membership delete. Keep the native path explicit while adopting
//! manager-timeout controls that map exactly to this consensus write.

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

use reverse_rusty::cluster::{ClusterState, NodeId, ShardError, StateVersion};

use crate::dto::ApiError;
use crate::handlers::search::parse_named_time_value;
use crate::metrics::PrometheusMetrics;
use crate::state::ClusterAppState;

use super::shard_error_status;

mod supervisor;

use supervisor::supervise_cluster_node_deregister_worker;

pub(crate) const CLUSTER_NODE_DEREGISTER_BODY_LIMIT: usize = 64 * 1024;
pub(crate) const CLUSTER_NODE_DEREGISTER_BODY_TIMEOUT: Duration = Duration::from_millis(250);
const DEFAULT_CLUSTER_NODE_DEREGISTER_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CLUSTER_NODE_DEREGISTER_TIMEOUT: Duration = Duration::from_secs(30);
const CLUSTER_NODE_DEREGISTER_ENDPOINT: &str = "cluster_node_deregister";
const CLUSTER_NODE_PATH_PREFIX: &str = "/_cluster/nodes/";

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterNodeDeregisterParams {
    /// OpenSearch-inclusive spelling.
    cluster_manager_timeout: Option<String>,
    /// Elasticsearch and legacy OpenSearch spelling.
    master_timeout: Option<String>,
}

impl ClusterNodeDeregisterParams {
    fn timeout(self) -> Result<Duration, String> {
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
            .map(parse_cluster_node_deregister_timeout)
            .transpose()?
            .unwrap_or(DEFAULT_CLUSTER_NODE_DEREGISTER_TIMEOUT);
        if timeout > MAX_CLUSTER_NODE_DEREGISTER_TIMEOUT {
            return Err("node-deregistration timeout must not exceed 30s".to_string());
        }
        Ok(timeout)
    }
}

fn parse_cluster_node_deregister_timeout(raw: &str) -> Result<Duration, String> {
    if raw == "0" {
        return Ok(Duration::ZERO);
    }
    parse_named_time_value("cluster_manager_timeout/master_timeout", raw)
}

fn parse_node_id(path: &str) -> Result<NodeId, String> {
    let raw = path
        .strip_prefix(CLUSTER_NODE_PATH_PREFIX)
        .ok_or_else(|| "invalid node-deregistration path".to_string())?;
    if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("node id must be a positive unsigned integer".to_string());
    }
    let id = raw
        .parse::<u64>()
        .map_err(|_| "node id is outside the unsigned 64-bit range".to_string())?;
    if id == 0 {
        return Err(
            "node id 0 is the reserved bootstrap in-process manager and cannot be deregistered"
                .to_string(),
        );
    }
    Ok(NodeId(id))
}

pub(crate) struct ClusterNodeDeregisterTransport {
    duration: HistogramTimer,
    timeout: Duration,
    node_id: NodeId,
}

impl ClusterNodeDeregisterTransport {
    fn into_parts(self) -> (HistogramTimer, Duration, NodeId) {
        (self.duration, self.timeout, self.node_id)
    }
}

impl FromRequest<Arc<ClusterAppState>> for ClusterNodeDeregisterTransport {
    type Rejection = Response;

    async fn from_request(
        request: Request,
        state: &Arc<ClusterAppState>,
    ) -> Result<Self, Self::Rejection> {
        let duration = state
            .prom
            .http_request_duration
            .with_label_values(&[CLUSTER_NODE_DEREGISTER_ENDPOINT])
            .start_timer();
        if request.method() != Method::DELETE {
            let mut response = cluster_node_deregister_rejection(
                &state.prom,
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "DELETE is the only supported /_cluster/nodes/{id} method",
            );
            response
                .headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static("DELETE"));
            return Err(response);
        }

        let Query(params) = Query::<ClusterNodeDeregisterParams>::try_from_uri(request.uri())
            .map_err(|source| {
                cluster_node_deregister_rejection(
                    &state.prom,
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    format!("invalid node-deregistration query parameters: {source}"),
                )
            })?;
        let timeout = params.timeout().map_err(|reason| {
            cluster_node_deregister_rejection(
                &state.prom,
                StatusCode::BAD_REQUEST,
                "validation_error",
                reason,
            )
        })?;
        let node_id = parse_node_id(request.uri().path()).map_err(|reason| {
            cluster_node_deregister_rejection(
                &state.prom,
                StatusCode::BAD_REQUEST,
                "validation_error",
                reason,
            )
        })?;

        let body_deadline = Instant::now()
            .checked_add(CLUSTER_NODE_DEREGISTER_BODY_TIMEOUT)
            .unwrap_or_else(Instant::now);
        let body = tokio::time::timeout(
            CLUSTER_NODE_DEREGISTER_BODY_TIMEOUT,
            Bytes::from_request(request, state),
        )
        .await
        .map_err(|_| {
            cluster_node_deregister_rejection(
                &state.prom,
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "node-deregistration body did not complete within 250ms",
            )
        })?;
        if Instant::now() >= body_deadline {
            return Err(cluster_node_deregister_rejection(
                &state.prom,
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "node-deregistration body did not complete within 250ms",
            ));
        }
        let body = body.map_err(|source| {
            let status = source.status();
            let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload_too_large"
            } else {
                "validation_error"
            };
            cluster_node_deregister_rejection(
                &state.prom,
                status,
                error_type,
                format!("invalid node-deregistration body: {source}"),
            )
        })?;
        if !body.is_empty() {
            return Err(cluster_node_deregister_rejection(
                &state.prom,
                StatusCode::BAD_REQUEST,
                "validation_error",
                "DELETE /_cluster/nodes/{id} does not accept a request body",
            ));
        }

        Ok(Self {
            duration,
            timeout,
            node_id,
        })
    }
}

#[derive(Serialize)]
struct ClusterNodeDeregisterResponse {
    acknowledged: bool,
    version: u64,
    node_id: u64,
}

#[derive(Clone, Copy)]
enum ProposalStart {
    Queued,
    Started,
    Cancelled,
}

enum ClusterNodeDeregisterWorkerOutcome {
    NotStarted,
    Finished(Result<StateVersion, ClusterNodeDeregisterError>),
}

type ClusterNodeDeregisterWorkerResult =
    Result<ClusterNodeDeregisterWorkerOutcome, tokio::task::JoinError>;

enum ClusterNodeDeregisterError {
    NodeInUse {
        voter: bool,
        assignment_count: usize,
    },
    Backend(ShardError),
}

fn validate_cluster_node_deregistration(
    state: &ClusterState,
    node_id: NodeId,
) -> Result<(), ClusterNodeDeregisterError> {
    let voter = state.voters.contains(&node_id);
    let assignment_count = state
        .assignments
        .iter()
        .filter(|assignment| {
            assignment.primary == node_id || assignment.replicas.contains(&node_id)
        })
        .count();
    if voter || assignment_count > 0 {
        return Err(ClusterNodeDeregisterError::NodeInUse {
            voter,
            assignment_count,
        });
    }
    Ok(())
}

fn begin_cluster_node_deregister_proposal(
    gate: &Mutex<ProposalStart>,
    deadline: Instant,
    no_wait: bool,
) -> bool {
    let mut start = gate.lock();
    if matches!(*start, ProposalStart::Cancelled) || (!no_wait && Instant::now() >= deadline) {
        *start = ProposalStart::Cancelled;
        return false;
    }
    *start = ProposalStart::Started;
    true
}

fn cancel_queued_cluster_node_deregister(gate: &Mutex<ProposalStart>) -> bool {
    let mut start = gate.lock();
    match *start {
        ProposalStart::Queued | ProposalStart::Cancelled => {
            *start = ProposalStart::Cancelled;
            true
        }
        ProposalStart::Started => false,
    }
}

/// Commit one descriptor removal off-runtime through the authoritative
/// control plane, including admission and topology/cluster-lock waiting.
#[instrument(skip_all)]
pub(crate) async fn cluster_deregister_node(
    State(state): State<Arc<ClusterAppState>>,
    transport: ClusterNodeDeregisterTransport,
) -> Response {
    let (_duration, timeout, node_id) = transport.into_parts();
    let no_wait = timeout.is_zero();
    let Some(deadline) = Instant::now().checked_add(timeout) else {
        return cluster_node_deregister_rejection(
            &state.prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            "node-deregistration timeout is too large for this platform",
        );
    };
    let permit = if no_wait {
        match Arc::clone(&state.stats_permits).try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => {
                return cluster_node_deregister_not_started_timeout(&state.prom);
            }
            Err(TryAcquireError::Closed) => {
                return cluster_node_deregister_rejection(
                    &state.prom,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "node_deregistration_unavailable",
                    "node-deregistration admission is closed",
                );
            }
        }
    } else {
        let Some(admission_budget) = deadline.checked_duration_since(Instant::now()) else {
            return cluster_node_deregister_not_started_timeout(&state.prom);
        };
        match tokio::time::timeout(
            admission_budget,
            Arc::clone(&state.stats_permits).acquire_owned(),
        )
        .await
        {
            Err(_) => return cluster_node_deregister_not_started_timeout(&state.prom),
            Ok(Err(_)) => {
                return cluster_node_deregister_rejection(
                    &state.prom,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "node_deregistration_unavailable",
                    "node-deregistration admission is closed",
                );
            }
            Ok(Ok(permit)) => permit,
        }
    };

    if !no_wait && Instant::now() >= deadline {
        return cluster_node_deregister_not_started_timeout(&state.prom);
    }

    let raw_node_id = node_id.0;
    let worker_state = Arc::clone(&state);
    let proposal_gate = Arc::new(Mutex::new(ProposalStart::Queued));
    let worker_gate = Arc::clone(&proposal_gate);
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        // Exclude descriptor mutation from registration and movement without
        // excluding serving READ guards. This closes the check/delete race
        // without changing the durable ClusterStateChange encoding.
        let _topology = if no_wait {
            worker_state.topology_guard.write()
        } else {
            let Some(lock_budget) = deadline.checked_duration_since(Instant::now()) else {
                return ClusterNodeDeregisterWorkerOutcome::NotStarted;
            };
            let Some(topology) = worker_state.topology_guard.try_write_for(lock_budget) else {
                return ClusterNodeDeregisterWorkerOutcome::NotStarted;
            };
            topology
        };
        let cluster = if no_wait {
            worker_state.cluster.read()
        } else {
            let Some(lock_budget) = deadline.checked_duration_since(Instant::now()) else {
                return ClusterNodeDeregisterWorkerOutcome::NotStarted;
            };
            let Some(cluster) = worker_state.cluster.try_read_for(lock_budget) else {
                return ClusterNodeDeregisterWorkerOutcome::NotStarted;
            };
            cluster
        };
        let control_state = match cluster.control_state() {
            Ok(state) => state,
            Err(source) => {
                return ClusterNodeDeregisterWorkerOutcome::Finished(Err(
                    ClusterNodeDeregisterError::Backend(source),
                ));
            }
        };
        if let Err(source) = validate_cluster_node_deregistration(&control_state, node_id) {
            return ClusterNodeDeregisterWorkerOutcome::Finished(Err(source));
        }
        if !begin_cluster_node_deregister_proposal(&worker_gate, deadline, no_wait) {
            return ClusterNodeDeregisterWorkerOutcome::NotStarted;
        }
        ClusterNodeDeregisterWorkerOutcome::Finished(
            cluster
                .deregister_node(node_id)
                .map_err(ClusterNodeDeregisterError::Backend),
        )
    });
    let mut completion = supervise_cluster_node_deregister_worker(raw_node_id, worker);

    if no_wait {
        return match completion.await {
            Ok(outcome) => finish_cluster_node_deregister_worker(&state.prom, raw_node_id, outcome),
            Err(source) => {
                cluster_node_deregister_supervisor_failed(&state.prom, raw_node_id, &source)
            }
        };
    }
    let Some(commit_budget) = deadline.checked_duration_since(Instant::now()) else {
        return if cancel_queued_cluster_node_deregister(&proposal_gate) {
            cluster_node_deregister_not_started_timeout(&state.prom)
        } else {
            cluster_node_deregister_unknown_timeout(&state.prom, raw_node_id)
        };
    };
    let outcome = tokio::time::timeout(commit_budget, &mut completion).await;
    if Instant::now() >= deadline {
        if matches!(
            outcome,
            Ok(Ok(Ok(ClusterNodeDeregisterWorkerOutcome::NotStarted)))
        ) || cancel_queued_cluster_node_deregister(&proposal_gate)
        {
            return cluster_node_deregister_not_started_timeout(&state.prom);
        }
        if outcome.is_err() {
            warn!(
                node_id = raw_node_id,
                "node deregistration exceeded its request deadline; detached proposal will finish"
            );
        } else {
            warn!(
                node_id = raw_node_id,
                "node deregistration completed after its request deadline"
            );
        }
        return cluster_node_deregister_unknown_timeout(&state.prom, raw_node_id);
    }
    match outcome {
        Err(_) => {
            if cancel_queued_cluster_node_deregister(&proposal_gate) {
                return cluster_node_deregister_not_started_timeout(&state.prom);
            }
            warn!(
                node_id = raw_node_id,
                "node deregistration exceeded its request deadline; detached proposal will finish"
            );
            cluster_node_deregister_unknown_timeout(&state.prom, raw_node_id)
        }
        Ok(Err(source)) => {
            cluster_node_deregister_supervisor_failed(&state.prom, raw_node_id, &source)
        }
        Ok(Ok(outcome)) => finish_cluster_node_deregister_worker(&state.prom, raw_node_id, outcome),
    }
}

fn finish_cluster_node_deregister_worker(
    prom: &PrometheusMetrics,
    node_id: u64,
    outcome: ClusterNodeDeregisterWorkerResult,
) -> Response {
    match outcome {
        Err(_) => cluster_node_deregister_rejection(
            prom,
            StatusCode::INTERNAL_SERVER_ERROR,
            "node_deregistration_unavailable",
            "node-deregistration worker failed",
        ),
        Ok(ClusterNodeDeregisterWorkerOutcome::NotStarted) => {
            cluster_node_deregister_not_started_timeout(prom)
        }
        Ok(ClusterNodeDeregisterWorkerOutcome::Finished(Err(
            ClusterNodeDeregisterError::NodeInUse {
                voter,
                assignment_count,
            },
        ))) => cluster_node_deregister_rejection(
            prom,
            StatusCode::CONFLICT,
            "node_in_use",
            cluster_node_deregister_in_use_reason(node_id, voter, assignment_count),
        ),
        Ok(ClusterNodeDeregisterWorkerOutcome::Finished(Err(
            ClusterNodeDeregisterError::Backend(source),
        ))) => {
            let status = shard_error_status(&source);
            let (_, error_type) = source.write_http_class();
            cluster_node_deregister_rejection(
                prom,
                status,
                error_type,
                "node deregistration was not acknowledged by the control plane",
            )
        }
        Ok(ClusterNodeDeregisterWorkerOutcome::Finished(Ok(version))) => {
            finish_cluster_node_deregister_response(
                prom,
                Json(ClusterNodeDeregisterResponse {
                    acknowledged: true,
                    version: version.0,
                    node_id,
                })
                .into_response(),
            )
        }
    }
}

fn cluster_node_deregister_in_use_reason(
    node_id: u64,
    voter: bool,
    assignment_count: usize,
) -> String {
    match (voter, assignment_count) {
        (true, 0) => format!(
            "node {node_id} is still a control-plane voter; remove it through joint consensus \
             before deregistering its descriptor"
        ),
        (false, 1) => format!(
            "node {node_id} is still referenced by 1 shard assignment; move its data and verify \
             cluster-state convergence before retrying"
        ),
        (false, count) => format!(
            "node {node_id} is still referenced by {count} shard assignments; move its data and \
             verify cluster-state convergence before retrying"
        ),
        (true, 1) => format!(
            "node {node_id} is still a control-plane voter and is referenced by 1 shard \
             assignment; remove its vote through joint consensus and move its data before retrying"
        ),
        (true, count) => format!(
            "node {node_id} is still a control-plane voter and is referenced by {count} shard \
             assignments; remove its vote through joint consensus and move its data before retrying"
        ),
    }
}

fn cluster_node_deregister_supervisor_failed(
    prom: &PrometheusMetrics,
    node_id: u64,
    source: &tokio::sync::oneshot::error::RecvError,
) -> Response {
    error!(node_id, error = %source, "node-deregistration completion supervisor failed");
    cluster_node_deregister_rejection(
        prom,
        StatusCode::INTERNAL_SERVER_ERROR,
        "node_deregistration_unavailable",
        "node-deregistration completion supervisor failed",
    )
}

fn cluster_node_deregister_not_started_timeout(prom: &PrometheusMetrics) -> Response {
    cluster_node_deregister_rejection(
        prom,
        StatusCode::REQUEST_TIMEOUT,
        "node_deregistration_timeout",
        "timed out waiting for node-deregistration admission; no proposal was started",
    )
}

fn cluster_node_deregister_unknown_timeout(prom: &PrometheusMetrics, node_id: u64) -> Response {
    cluster_node_deregister_rejection(
        prom,
        StatusCode::REQUEST_TIMEOUT,
        "node_deregistration_timeout",
        format!(
            "timed out waiting for node {node_id} deregistration to commit; its outcome is \
             unknown, so inspect /_cluster/state before retrying"
        ),
    )
}

fn cluster_node_deregister_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    finish_cluster_node_deregister_response(
        prom,
        ApiError::response(status, error_type, reason).into_response(),
    )
}

fn finish_cluster_node_deregister_response(
    prom: &PrometheusMetrics,
    mut response: Response,
) -> Response {
    prom.http_requests_total
        .with_label_values(&[CLUSTER_NODE_DEREGISTER_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
