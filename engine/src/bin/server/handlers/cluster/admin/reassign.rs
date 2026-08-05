//! Strict native `POST /_cluster/reassign` move-and-commit workflow.
//!
//! Elasticsearch/OpenSearch `/_cluster/reroute` is a multi-command allocation
//! API with named indices, allocation deciders, simulation, and replica/primary
//! recovery commands. Reverse Rusty has one logical matcher and this endpoint
//! moves exactly one global position to a numeric membership id, so the native
//! path remains explicit while adopting compatible shard/target and manager-
//! timeout spellings where their meaning is exact.

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
use reverse_rusty::cluster::{NodeId, ReassignOutcome, ShardError};

use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::ClusterAppState;
#[cfg(feature = "distributed")]
use crate::state::ClusterRebalanceTopology;

#[cfg(feature = "distributed")]
use super::super::shard_error_status;

mod supervisor;
mod transport;

#[cfg(feature = "distributed")]
use supervisor::{supervise_cluster_reassign_worker, ClusterReassignWorkerFailure};
#[cfg(feature = "distributed")]
use transport::ClusterReassignBody;
use transport::ClusterReassignTransport;

pub(crate) const CLUSTER_REASSIGN_BODY_LIMIT: usize = 64 * 1024;
pub(crate) const CLUSTER_REASSIGN_BODY_TIMEOUT: Duration = Duration::from_millis(250);
const CLUSTER_REASSIGN_ENDPOINT: &str = "cluster_reassign";

#[cfg(feature = "distributed")]
#[derive(Clone, Copy)]
enum ReassignStart {
    Queued,
    Started,
    Cancelled,
}

#[cfg(feature = "distributed")]
fn begin_cluster_reassign(gate: &Mutex<ReassignStart>, deadline: Instant, no_wait: bool) -> bool {
    let mut start = gate.lock();
    if matches!(*start, ReassignStart::Cancelled) || (!no_wait && Instant::now() >= deadline) {
        *start = ReassignStart::Cancelled;
        return false;
    }
    *start = ReassignStart::Started;
    true
}

#[cfg(feature = "distributed")]
fn cancel_queued_cluster_reassign(gate: &Mutex<ReassignStart>) -> bool {
    let mut start = gate.lock();
    match *start {
        ReassignStart::Queued | ReassignStart::Cancelled => {
            *start = ReassignStart::Cancelled;
            true
        }
        ReassignStart::Started => false,
    }
}

#[cfg(feature = "distributed")]
struct CancelQueuedClusterReassign(Arc<Mutex<ReassignStart>>);

#[cfg(feature = "distributed")]
impl Drop for CancelQueuedClusterReassign {
    fn drop(&mut self) {
        let _ = cancel_queued_cluster_reassign(&self.0);
    }
}

#[cfg(feature = "distributed")]
#[derive(Debug)]
struct ClusterReassignSuccess {
    outcome: ReassignOutcome,
    requested_node: u64,
    took_ms: f64,
}

#[cfg(feature = "distributed")]
impl ClusterReassignSuccess {
    fn position(&self) -> u32 {
        match &self.outcome {
            ReassignOutcome::NoChange { position, .. }
            | ReassignOutcome::Moved { position, .. }
            | ReassignOutcome::Reconciled { position, .. }
            | ReassignOutcome::MovedButNotCommitted { position, .. } => *position,
        }
    }

    fn moved(&self) -> bool {
        match &self.outcome {
            ReassignOutcome::Moved { .. } => true,
            ReassignOutcome::MovedButNotCommitted { moved, .. } => *moved,
            ReassignOutcome::NoChange { .. } | ReassignOutcome::Reconciled { .. } => false,
        }
    }

    fn committed(&self) -> bool {
        !matches!(&self.outcome, ReassignOutcome::MovedButNotCommitted { .. })
    }
}

#[cfg(feature = "distributed")]
enum ClusterReassignWorkerOutcome {
    NotStarted,
    Finished(Result<ClusterReassignSuccess, ShardError>),
}

#[cfg(feature = "distributed")]
type ClusterReassignWorkerResult =
    Result<ClusterReassignWorkerOutcome, ClusterReassignWorkerFailure>;

#[cfg(feature = "distributed")]
#[derive(Serialize)]
// These booleans are the intentional wire-level terminal-state projection:
// clients must be able to distinguish physical movement, durable commit, and
// commit-only reconciliation without decoding an internal enum.
#[allow(clippy::struct_excessive_bools)]
struct ClusterReassignResponse {
    took: u64,
    took_ms: f64,
    acknowledged: bool,
    moved: bool,
    committed: bool,
    reconciled: bool,
    position: u32,
    node: u64,
    generation: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    warning: Option<&'static str>,
}

/// Move one global position to a registered node and commit that durable
/// owner. A manager timeout bounds only admission/start; once movement begins,
/// the request waits for its exact terminal result and a disconnect detaches
/// only the response.
#[instrument(skip_all)]
pub(crate) async fn cluster_reassign(
    State(state): State<Arc<ClusterAppState>>,
    transport: ClusterReassignTransport,
) -> Response {
    let (_duration, started, manager_timeout, body) = transport.into_parts();

    #[cfg(not(feature = "distributed"))]
    {
        let _ = (started, manager_timeout, body);
        return cluster_reassign_rejection(
            &state.prom,
            StatusCode::NOT_IMPLEMENTED,
            "not_supported_in_cluster_mode",
            "a data-moving reassignment needs the gRPC transport; rebuild the server with the \
             `distributed` feature",
        );
    }

    #[cfg(feature = "distributed")]
    {
        cluster_reassign_distributed(state, started, manager_timeout, body).await
    }
}

#[cfg(feature = "distributed")]
async fn cluster_reassign_distributed(
    state: Arc<ClusterAppState>,
    started: Instant,
    manager_timeout: Duration,
    body: ClusterReassignBody,
) -> Response {
    if let Err((status, error_type, reason)) = validate_reassign_topology(&state) {
        return cluster_reassign_rejection(&state.prom, status, error_type, reason);
    }
    let no_wait = manager_timeout.is_zero();
    let Some(deadline) = Instant::now().checked_add(manager_timeout) else {
        return cluster_reassign_rejection(
            &state.prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            "reassign manager timeout is too large for this platform",
        );
    };

    let permit = if no_wait {
        match Arc::clone(&state.reassign_permits).try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => {
                return cluster_reassign_not_started_timeout(&state.prom)
            }
            Err(TryAcquireError::Closed) => {
                return cluster_reassign_rejection(
                    &state.prom,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "reassign_unavailable",
                    "reassign admission is closed",
                );
            }
        }
    } else {
        let Some(admission_budget) = deadline.checked_duration_since(Instant::now()) else {
            return cluster_reassign_not_started_timeout(&state.prom);
        };
        match tokio::time::timeout(
            admission_budget,
            Arc::clone(&state.reassign_permits).acquire_owned(),
        )
        .await
        {
            Err(_) => return cluster_reassign_not_started_timeout(&state.prom),
            Ok(Err(_)) => {
                return cluster_reassign_rejection(
                    &state.prom,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "reassign_unavailable",
                    "reassign admission is closed",
                );
            }
            Ok(Ok(permit)) => permit,
        }
    };
    if !no_wait && Instant::now() >= deadline {
        return cluster_reassign_not_started_timeout(&state.prom);
    }

    let worker_state = Arc::clone(&state);
    let gate = Arc::new(Mutex::new(ReassignStart::Queued));
    let _cancel_queued_on_drop = CancelQueuedClusterReassign(Arc::clone(&gate));
    let worker_gate = Arc::clone(&gate);
    let (started_sender, mut started_receiver) = tokio::sync::oneshot::channel();
    let handle = tokio::runtime::Handle::current();
    let requested_node = body.node;
    let completion = match supervise_cluster_reassign_worker(move || {
        let _permit = permit;
        let topology = if no_wait {
            worker_state.topology_guard.try_read()
        } else {
            deadline
                .checked_duration_since(Instant::now())
                .and_then(|budget| worker_state.topology_guard.try_read_for(budget))
        };
        let Some(_topology) = topology else {
            return ClusterReassignWorkerOutcome::NotStarted;
        };
        let cluster = if no_wait {
            worker_state.cluster.try_read()
        } else {
            deadline
                .checked_duration_since(Instant::now())
                .and_then(|budget| worker_state.cluster.try_read_for(budget))
        };
        let Some(cluster) = cluster else {
            return ClusterReassignWorkerOutcome::NotStarted;
        };
        let outcome = cluster.reassign_and_move_until(
            body.position as usize,
            NodeId(body.node),
            &handle,
            deadline,
            || {
                if !begin_cluster_reassign(&worker_gate, deadline, no_wait) {
                    return false;
                }
                let _ = started_sender.send(());
                true
            },
        );
        match outcome {
            Ok(None) => ClusterReassignWorkerOutcome::NotStarted,
            Ok(Some(outcome)) => {
                ClusterReassignWorkerOutcome::Finished(Ok(ClusterReassignSuccess {
                    outcome,
                    requested_node,
                    took_ms: started.elapsed().as_secs_f64() * 1_000.0,
                }))
            }
            Err(source) => ClusterReassignWorkerOutcome::Finished(Err(source)),
        }
    }) {
        Ok(completion) => completion,
        Err(source) => {
            error!(error = %source, "failed to dispatch dedicated reassign worker");
            return cluster_reassign_rejection(
                &state.prom,
                StatusCode::SERVICE_UNAVAILABLE,
                "reassign_unavailable",
                "reassign worker could not be started",
            );
        }
    };
    let mut completion = completion;

    if no_wait {
        return match completion.await {
            Ok(outcome) => finish_cluster_reassign_worker(&state.prom, outcome),
            Err(source) => cluster_reassign_supervisor_failed(&state.prom, &source),
        };
    }

    let sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
    tokio::pin!(sleep);
    tokio::select! {
        outcome = &mut completion => match outcome {
            Ok(outcome) => finish_cluster_reassign_worker(&state.prom, outcome),
            Err(source) => cluster_reassign_supervisor_failed(&state.prom, &source),
        },
        started = &mut started_receiver => {
            if started.is_err() {
                warn!("reassign worker ended without sending its start signal");
            }
            match completion.await {
                Ok(outcome) => finish_cluster_reassign_worker(&state.prom, outcome),
                Err(source) => cluster_reassign_supervisor_failed(&state.prom, &source),
            }
        },
        () = &mut sleep => {
            if cancel_queued_cluster_reassign(&gate) {
                cluster_reassign_not_started_timeout(&state.prom)
            } else {
                match completion.await {
                    Ok(outcome) => finish_cluster_reassign_worker(&state.prom, outcome),
                    Err(source) => cluster_reassign_supervisor_failed(&state.prom, &source),
                }
            }
        }
    }
}

#[cfg(feature = "distributed")]
type ReassignTopologyRejection = (StatusCode, &'static str, &'static str);

#[cfg(feature = "distributed")]
fn validate_reassign_topology(state: &ClusterAppState) -> Result<(), ReassignTopologyRejection> {
    match state.rebalance_topology {
        ClusterRebalanceTopology::ResolveOnlyRemote => Ok(()),
        ClusterRebalanceTopology::StaticRemote => Err((
            StatusCode::CONFLICT,
            "reassign_routing_not_authoritative",
            "a static remote coordinator cannot reassign safely because its live shard backings \
             do not follow the committed assignment map; restart with --route-by-assignments and \
             --control-endpoint before retrying",
        )),
        ClusterRebalanceTopology::CliSeededAssignmentRemote => Err((
            StatusCode::CONFLICT,
            "reassign_resolve_only_required",
            "this assignment-routed coordinator was started with --shard-endpoint, so a changed \
             map would make its next guarded restart fail; restart resolve-only with \
             --route-by-assignments, --control-endpoint, the committed --shards count, and no \
             --shard-endpoint before retrying",
        )),
        ClusterRebalanceTopology::InProcess => Err((
            StatusCode::BAD_REQUEST,
            "validation_error",
            "reassign requires a resolve-only remote coordinator; use POST /_cluster/rebalance \
             for in-process placement",
        )),
    }
}

#[cfg(feature = "distributed")]
fn finish_cluster_reassign_worker(
    prom: &PrometheusMetrics,
    outcome: ClusterReassignWorkerResult,
) -> Response {
    match outcome {
        Err(_) => cluster_reassign_rejection(
            prom,
            StatusCode::INTERNAL_SERVER_ERROR,
            "reassign_unavailable",
            "reassign worker failed",
        ),
        Ok(ClusterReassignWorkerOutcome::NotStarted) => cluster_reassign_not_started_timeout(prom),
        Ok(ClusterReassignWorkerOutcome::Finished(Err(source))) => {
            error!(error = %source, "reassign failed before an attested terminal outcome");
            let status = shard_error_status(&source);
            let status = if status.is_success() {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                status
            };
            let (_, error_type) = source.write_http_class();
            cluster_reassign_rejection(
                prom,
                status,
                error_type,
                "reassign did not reach an attested terminal move-and-commit state; inspect server \
                 logs and /_cluster/state before retrying",
            )
        }
        Ok(ClusterReassignWorkerOutcome::Finished(Ok(success))) => {
            let (position, node, generation, moved, committed, reconciled, warning) =
                match success.outcome {
                    ReassignOutcome::NoChange {
                        position,
                        generation,
                    } => (
                        position,
                        success.requested_node,
                        generation,
                        false,
                        true,
                        false,
                        None,
                    ),
                    ReassignOutcome::Moved {
                        position,
                        to,
                        generation,
                        ..
                    } => (position, to.0, generation, true, true, false, None),
                    ReassignOutcome::Reconciled {
                        position,
                        to,
                        generation,
                        ..
                    } => (position, to.0, generation, false, true, true, None),
                    ReassignOutcome::MovedButNotCommitted {
                        position,
                        to,
                        generation,
                        moved,
                        ..
                    } => (
                        position,
                        to.0,
                        generation,
                        moved,
                        false,
                        false,
                        Some(
                            "live routing reaches the requested node, but committing the durable \
                             owner failed; re-run promptly before restarting the coordinator",
                        ),
                    ),
                };
            finish_cluster_reassign_response(
                prom,
                Json(ClusterReassignResponse {
                    took: success.took_ms.floor() as u64,
                    took_ms: success.took_ms,
                    acknowledged: committed,
                    moved,
                    committed,
                    reconciled,
                    position,
                    node,
                    generation,
                    warning,
                })
                .into_response(),
            )
        }
    }
}

#[cfg(feature = "distributed")]
fn cluster_reassign_supervisor_failed(
    prom: &PrometheusMetrics,
    source: &tokio::sync::oneshot::error::RecvError,
) -> Response {
    error!(error = %source, "reassign completion supervisor failed");
    cluster_reassign_rejection(
        prom,
        StatusCode::INTERNAL_SERVER_ERROR,
        "reassign_unavailable",
        "reassign completion supervisor failed",
    )
}

#[cfg(feature = "distributed")]
fn cluster_reassign_not_started_timeout(prom: &PrometheusMetrics) -> Response {
    cluster_reassign_rejection(
        prom,
        StatusCode::REQUEST_TIMEOUT,
        "reassign_timeout",
        "timed out waiting for reassign admission or topology access; no reassignment was started",
    )
}

fn cluster_reassign_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    finish_cluster_reassign_response(
        prom,
        ApiError::response(status, error_type, reason).into_response(),
    )
}

fn finish_cluster_reassign_response(prom: &PrometheusMetrics, mut response: Response) -> Response {
    prom.http_requests_total
        .with_label_values(&[CLUSTER_REASSIGN_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

#[cfg(all(test, feature = "distributed"))]
mod tests {
    use axum::body::to_bytes;

    use super::*;

    async fn response_json(outcome: ReassignOutcome) -> serde_json::Value {
        let response = finish_cluster_reassign_worker(
            &PrometheusMetrics::new(),
            Ok(ClusterReassignWorkerOutcome::Finished(Ok(
                ClusterReassignSuccess {
                    outcome,
                    requested_node: 2,
                    took_ms: 1.5,
                },
            ))),
        );
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response bytes");
        serde_json::from_slice(&bytes).expect("response JSON")
    }

    #[tokio::test]
    async fn backend_failures_preserve_class_without_exposing_mesh_details() {
        let response = finish_cluster_reassign_worker(
            &PrometheusMetrics::new(),
            Ok(ClusterReassignWorkerOutcome::Finished(Err(
                ShardError::Remote("secret-node.internal:50051 refused the request".into()),
            ))),
        );
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response bytes");
        let rendered = String::from_utf8(bytes.to_vec()).expect("UTF-8 response");
        assert!(rendered.contains("shard_unreachable"), "{rendered}");
        assert!(!rendered.contains("secret-node"), "{rendered}");
    }

    #[tokio::test]
    async fn terminal_flags_distinguish_noop_reconciliation_and_uncommitted_state() {
        let no_change = response_json(ReassignOutcome::NoChange {
            position: 0,
            generation: 7,
        })
        .await;
        assert_eq!(no_change["acknowledged"], true);
        assert_eq!(no_change["moved"], false);
        assert_eq!(no_change["committed"], true);
        assert_eq!(no_change["generation"], 7);

        let reconciled = response_json(ReassignOutcome::Reconciled {
            position: 0,
            from: NodeId(1),
            to: NodeId(2),
            generation: 7,
        })
        .await;
        assert_eq!(reconciled["acknowledged"], true);
        assert_eq!(reconciled["moved"], false);
        assert_eq!(reconciled["committed"], true);
        assert_eq!(reconciled["reconciled"], true);

        let uncommitted = response_json(ReassignOutcome::MovedButNotCommitted {
            position: 0,
            from: NodeId(1),
            to: NodeId(2),
            generation: 8,
            moved: true,
        })
        .await;
        assert_eq!(uncommitted["acknowledged"], false);
        assert_eq!(uncommitted["moved"], true);
        assert_eq!(uncommitted["committed"], false);
        assert!(uncommitted["warning"].is_string());
    }
}
