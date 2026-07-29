//! Strict native `POST /_cluster/nodes` control-plane registration.
//!
//! Elasticsearch and OpenSearch expose node observation APIs, but no REST
//! membership insertion. Keep the native path and schema explicit while
//! adopting manager-timeout controls that map exactly to this consensus write.

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

use reverse_rusty::cluster::{NodeDescriptor, NodeId, NodeRole, ShardError, StateVersion};

use crate::dto::ApiError;
use crate::handlers::search::parse_named_time_value;
use crate::metrics::PrometheusMetrics;
use crate::state::ClusterAppState;

use super::shard_error_status;

mod endpoint;
mod supervisor;

use endpoint::validate_node_addr;
use supervisor::supervise_cluster_node_register_worker;

pub(crate) const CLUSTER_NODE_REGISTER_BODY_LIMIT: usize = 64 * 1024;
pub(crate) const CLUSTER_NODE_REGISTER_BODY_TIMEOUT: Duration = Duration::from_millis(250);
const DEFAULT_CLUSTER_NODE_REGISTER_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CLUSTER_NODE_REGISTER_TIMEOUT: Duration = Duration::from_secs(30);
const CLUSTER_NODE_REGISTER_ENDPOINT: &str = "cluster_node_register";

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterNodeRegisterParams {
    /// OpenSearch-inclusive spelling.
    cluster_manager_timeout: Option<String>,
    /// Elasticsearch and legacy OpenSearch spelling.
    master_timeout: Option<String>,
}

impl ClusterNodeRegisterParams {
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
            .map(parse_cluster_node_register_timeout)
            .transpose()?
            .unwrap_or(DEFAULT_CLUSTER_NODE_REGISTER_TIMEOUT);
        if timeout > MAX_CLUSTER_NODE_REGISTER_TIMEOUT {
            return Err("node-registration timeout must not exceed 30s".to_string());
        }
        Ok(timeout)
    }
}

fn parse_cluster_node_register_timeout(raw: &str) -> Result<Duration, String> {
    if raw == "0" {
        return Ok(Duration::ZERO);
    }
    parse_named_time_value("cluster_manager_timeout/master_timeout", raw)
}

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum RegisterNodeRole {
    #[default]
    Data,
    Manager,
}

impl RegisterNodeRole {
    const fn native(self) -> NodeRole {
        match self {
            Self::Data => NodeRole::Data,
            Self::Manager => NodeRole::Manager,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::Manager => "manager",
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RegisterNodeBody {
    id: u64,
    addr: String,
    #[serde(default)]
    role: RegisterNodeRole,
}

impl RegisterNodeBody {
    fn validate(self) -> Result<ValidatedRegisterNode, String> {
        if self.id == 0 {
            return Err(
                "node id 0 is reserved for the bootstrap in-process manager; use a positive id"
                    .to_string(),
            );
        }
        let addr = validate_node_addr(self.addr)?;
        Ok(ValidatedRegisterNode {
            descriptor: NodeDescriptor {
                id: NodeId(self.id),
                addr: Some(addr.clone()),
                role: self.role.native(),
            },
            addr,
            role: self.role,
        })
    }
}

struct ValidatedRegisterNode {
    descriptor: NodeDescriptor,
    addr: String,
    role: RegisterNodeRole,
}

pub(crate) struct ClusterNodeRegisterTransport {
    duration: HistogramTimer,
    timeout: Duration,
    node: ValidatedRegisterNode,
}

impl ClusterNodeRegisterTransport {
    fn into_parts(self) -> (HistogramTimer, Duration, ValidatedRegisterNode) {
        (self.duration, self.timeout, self.node)
    }
}

impl FromRequest<Arc<ClusterAppState>> for ClusterNodeRegisterTransport {
    type Rejection = Response;

    async fn from_request(
        request: Request,
        state: &Arc<ClusterAppState>,
    ) -> Result<Self, Self::Rejection> {
        let duration = state
            .prom
            .http_request_duration
            .with_label_values(&[CLUSTER_NODE_REGISTER_ENDPOINT])
            .start_timer();
        if request.method() != Method::POST {
            let mut response = cluster_node_register_rejection(
                &state.prom,
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "POST is the only supported /_cluster/nodes method",
            );
            response
                .headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static("POST"));
            return Err(response);
        }

        let Query(params) = Query::<ClusterNodeRegisterParams>::try_from_uri(request.uri())
            .map_err(|source| {
                cluster_node_register_rejection(
                    &state.prom,
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    format!("invalid node-registration query parameters: {source}"),
                )
            })?;
        let timeout = params.timeout().map_err(|reason| {
            cluster_node_register_rejection(
                &state.prom,
                StatusCode::BAD_REQUEST,
                "validation_error",
                reason,
            )
        })?;
        if !is_json_content_type(request.headers()) {
            return Err(cluster_node_register_rejection(
                &state.prom,
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                "POST /_cluster/nodes requires Content-Type: application/json",
            ));
        }

        let body_deadline = Instant::now()
            .checked_add(CLUSTER_NODE_REGISTER_BODY_TIMEOUT)
            .unwrap_or_else(Instant::now);
        let body = tokio::time::timeout(
            CLUSTER_NODE_REGISTER_BODY_TIMEOUT,
            Bytes::from_request(request, state),
        )
        .await
        .map_err(|_| {
            cluster_node_register_rejection(
                &state.prom,
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "node-registration body did not complete within 250ms",
            )
        })?;
        // Tokio polls the body future before its timeout timer. If this task was
        // starved while the body became ready, the timeout may therefore return
        // `Ok` after the fixed deadline; enforce the absolute boundary too.
        if Instant::now() >= body_deadline {
            return Err(cluster_node_register_rejection(
                &state.prom,
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "node-registration body did not complete within 250ms",
            ));
        }
        let body = body.map_err(|source| {
            let status = source.status();
            let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload_too_large"
            } else {
                "validation_error"
            };
            cluster_node_register_rejection(
                &state.prom,
                status,
                error_type,
                format!("invalid node-registration body: {source}"),
            )
        })?;
        let parsed: RegisterNodeBody = serde_json::from_slice(&body).map_err(|source| {
            cluster_node_register_rejection(
                &state.prom,
                StatusCode::BAD_REQUEST,
                "validation_error",
                format!("invalid node-registration JSON body: {source}"),
            )
        })?;
        let node = parsed.validate().map_err(|reason| {
            cluster_node_register_rejection(
                &state.prom,
                StatusCode::BAD_REQUEST,
                "validation_error",
                reason,
            )
        })?;

        Ok(Self {
            duration,
            timeout,
            node,
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

#[derive(Serialize)]
struct RegisteredNode {
    id: u64,
    addr: String,
    role: &'static str,
}

#[derive(Serialize)]
struct ClusterNodeRegisterResponse {
    acknowledged: bool,
    version: u64,
    node: RegisteredNode,
}

#[derive(Clone, Copy)]
enum ProposalStart {
    Queued,
    Started,
    Cancelled,
}

enum ClusterNodeRegisterWorkerOutcome {
    NotStarted,
    Finished(Result<StateVersion, ShardError>),
}

type ClusterNodeRegisterWorkerResult =
    Result<ClusterNodeRegisterWorkerOutcome, tokio::task::JoinError>;

fn begin_cluster_node_register_proposal(
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

/// Atomically cancel a proposal that has not started yet. The worker uses the
/// same gate immediately before touching the cluster/control plane, so a
/// blocking-pool queue cannot apply a descriptor after its request deadline.
fn cancel_queued_cluster_node_register(gate: &Mutex<ProposalStart>) -> bool {
    let mut start = gate.lock();
    match *start {
        ProposalStart::Queued | ProposalStart::Cancelled => {
            *start = ProposalStart::Cancelled;
            true
        }
        ProposalStart::Started => false,
    }
}

/// Commit one member descriptor through the authoritative control plane.
/// Admission, cluster-lock waiting, the synchronous consensus write, and
/// response construction all happen away from Tokio request workers.
#[instrument(skip_all)]
pub(crate) async fn cluster_register_node(
    State(state): State<Arc<ClusterAppState>>,
    transport: ClusterNodeRegisterTransport,
) -> Response {
    let (_duration, timeout, node) = transport.into_parts();
    let no_wait = timeout.is_zero();
    let Some(deadline) = Instant::now().checked_add(timeout) else {
        return cluster_node_register_rejection(
            &state.prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            "node-registration timeout is too large for this platform",
        );
    };
    let permit = if no_wait {
        match Arc::clone(&state.stats_permits).try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => {
                return cluster_node_register_not_started_timeout(&state.prom);
            }
            Err(TryAcquireError::Closed) => {
                return cluster_node_register_rejection(
                    &state.prom,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "node_registration_unavailable",
                    "node-registration admission is closed",
                );
            }
        }
    } else {
        let Some(admission_budget) = deadline.checked_duration_since(Instant::now()) else {
            return cluster_node_register_not_started_timeout(&state.prom);
        };
        match tokio::time::timeout(
            admission_budget,
            Arc::clone(&state.stats_permits).acquire_owned(),
        )
        .await
        {
            Err(_) => return cluster_node_register_not_started_timeout(&state.prom),
            Ok(Err(_)) => {
                return cluster_node_register_rejection(
                    &state.prom,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "node_registration_unavailable",
                    "node-registration admission is closed",
                );
            }
            Ok(Ok(permit)) => permit,
        }
    };

    // Zero means no admission wait. Once admitted it executes one synchronous
    // proposal to completion, matching the familiar manager-timeout convention.
    if !no_wait && Instant::now() >= deadline {
        return cluster_node_register_not_started_timeout(&state.prom);
    }

    let response_node = RegisteredNode {
        id: node.descriptor.id.0,
        addr: node.addr,
        role: node.role.as_str(),
    };
    let node_id = response_node.id;
    let worker_state = Arc::clone(&state);
    let proposal_gate = Arc::new(Mutex::new(ProposalStart::Queued));
    let worker_gate = Arc::clone(&proposal_gate);
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        // Descriptor replacement is exclusive with topology movement so a
        // move cannot resolve one endpoint/role and commit against another.
        // Serving uses the separate cluster READ lock and remains concurrent.
        let _topology = if no_wait {
            worker_state.topology_guard.write()
        } else {
            let Some(lock_budget) = deadline.checked_duration_since(Instant::now()) else {
                return ClusterNodeRegisterWorkerOutcome::NotStarted;
            };
            let Some(topology) = worker_state.topology_guard.try_write_for(lock_budget) else {
                return ClusterNodeRegisterWorkerOutcome::NotStarted;
            };
            topology
        };
        let cluster = if no_wait {
            worker_state.cluster.read()
        } else {
            let Some(lock_budget) = deadline.checked_duration_since(Instant::now()) else {
                return ClusterNodeRegisterWorkerOutcome::NotStarted;
            };
            let Some(cluster) = worker_state.cluster.try_read_for(lock_budget) else {
                return ClusterNodeRegisterWorkerOutcome::NotStarted;
            };
            cluster
        };
        if !begin_cluster_node_register_proposal(&worker_gate, deadline, no_wait) {
            return ClusterNodeRegisterWorkerOutcome::NotStarted;
        }
        ClusterNodeRegisterWorkerOutcome::Finished(cluster.register_node(node.descriptor))
    });
    let mut completion = supervise_cluster_node_register_worker(node_id, worker);

    if no_wait {
        return match completion.await {
            Ok(outcome) => finish_cluster_node_register_worker(&state.prom, response_node, outcome),
            Err(source) => cluster_node_register_supervisor_failed(&state.prom, node_id, &source),
        };
    }
    let Some(commit_budget) = deadline.checked_duration_since(Instant::now()) else {
        return if cancel_queued_cluster_node_register(&proposal_gate) {
            cluster_node_register_not_started_timeout(&state.prom)
        } else {
            cluster_node_register_unknown_timeout(&state.prom, node_id)
        };
    };
    let outcome = tokio::time::timeout(commit_budget, &mut completion).await;
    if Instant::now() >= deadline {
        if matches!(
            outcome,
            Ok(Ok(Ok(ClusterNodeRegisterWorkerOutcome::NotStarted)))
        ) || (outcome.is_err() && cancel_queued_cluster_node_register(&proposal_gate))
        {
            return cluster_node_register_not_started_timeout(&state.prom);
        }
        if outcome.is_err() {
            warn!(
                node_id,
                "node registration exceeded its request deadline; detached proposal will finish"
            );
        } else {
            warn!(
                node_id,
                "node registration completed after its request deadline"
            );
        }
        return cluster_node_register_unknown_timeout(&state.prom, node_id);
    }
    match outcome {
        Err(_) => {
            if cancel_queued_cluster_node_register(&proposal_gate) {
                return cluster_node_register_not_started_timeout(&state.prom);
            }
            warn!(
                node_id,
                "node registration exceeded its request deadline; detached proposal will finish"
            );
            cluster_node_register_unknown_timeout(&state.prom, node_id)
        }
        Ok(Err(source)) => cluster_node_register_supervisor_failed(&state.prom, node_id, &source),
        Ok(Ok(outcome)) => finish_cluster_node_register_worker(&state.prom, response_node, outcome),
    }
}

fn finish_cluster_node_register_worker(
    prom: &PrometheusMetrics,
    node: RegisteredNode,
    outcome: ClusterNodeRegisterWorkerResult,
) -> Response {
    match outcome {
        Err(_) => cluster_node_register_rejection(
            prom,
            StatusCode::INTERNAL_SERVER_ERROR,
            "node_registration_unavailable",
            "node-registration worker failed",
        ),
        Ok(ClusterNodeRegisterWorkerOutcome::NotStarted) => {
            cluster_node_register_not_started_timeout(prom)
        }
        Ok(ClusterNodeRegisterWorkerOutcome::Finished(Err(source))) => {
            let status = shard_error_status(&source);
            cluster_node_register_rejection(
                prom,
                status,
                "control_plane_error",
                "node registration was not acknowledged by the control plane",
            )
        }
        Ok(ClusterNodeRegisterWorkerOutcome::Finished(Ok(version))) => {
            finish_cluster_node_register_response(
                prom,
                Json(ClusterNodeRegisterResponse {
                    acknowledged: true,
                    version: version.0,
                    node,
                })
                .into_response(),
            )
        }
    }
}

fn cluster_node_register_supervisor_failed(
    prom: &PrometheusMetrics,
    node_id: u64,
    source: &tokio::sync::oneshot::error::RecvError,
) -> Response {
    error!(node_id, error = %source, "node-registration completion supervisor failed");
    cluster_node_register_rejection(
        prom,
        StatusCode::INTERNAL_SERVER_ERROR,
        "node_registration_unavailable",
        "node-registration completion supervisor failed",
    )
}

fn cluster_node_register_not_started_timeout(prom: &PrometheusMetrics) -> Response {
    cluster_node_register_rejection(
        prom,
        StatusCode::REQUEST_TIMEOUT,
        "node_registration_timeout",
        "timed out waiting for node-registration admission; no proposal was started",
    )
}

fn cluster_node_register_unknown_timeout(prom: &PrometheusMetrics, node_id: u64) -> Response {
    cluster_node_register_rejection(
        prom,
        StatusCode::REQUEST_TIMEOUT,
        "node_registration_timeout",
        format!(
            "timed out waiting for node {node_id} registration to commit; its outcome is unknown, \
             so inspect /_cluster/state before retrying"
        ),
    )
}

fn cluster_node_register_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    finish_cluster_node_register_response(
        prom,
        ApiError::response(status, error_type, reason).into_response(),
    )
}

fn finish_cluster_node_register_response(
    prom: &PrometheusMetrics,
    mut response: Response,
) -> Response {
    prom.http_requests_total
        .with_label_values(&[CLUSTER_NODE_REGISTER_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
