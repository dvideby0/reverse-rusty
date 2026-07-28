//! Coordinator implementation of the strict native health contract.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;

use axum::{
    body::Bytes,
    extract::{
        rejection::{BytesRejection, QueryRejection},
        Query, State,
    },
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use tracing::{error, instrument, warn};

use reverse_rusty::cluster::{ClusterEngine, ShardError};

use crate::handlers::admin::{
    finish_health_response, validate_health_method, validate_health_request, wait_delay,
    HealthParams, HealthStatus, HEALTH_ENDPOINT,
};
use crate::state::ClusterAppState;

#[derive(Clone)]
struct ClusterHealth {
    status: HealthStatus,
    deadline_expired: bool,
    shards: usize,
    pending_repairs: usize,
    reason: Option<&'static str>,
}

#[derive(Serialize)]
struct ClusterHealthResponse {
    status: &'static str,
    mode: &'static str,
    timed_out: bool,
    shards: usize,
    pending_repairs: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
}

/// `GET`/`HEAD /_health` — fail-loud serving and control-plane readiness.
///
/// The required control-state and per-position probes may cross the network.
/// They therefore share stats admission and execute on a blocking worker.
#[instrument(skip_all)]
pub(crate) async fn cluster_health(
    State(state): State<Arc<ClusterAppState>>,
    method: Method,
    params: Result<Query<HealthParams>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let _duration = state
        .prom
        .http_request_duration
        .with_label_values(&[HEALTH_ENDPOINT])
        .start_timer();
    let head = match validate_health_method(&state.prom, &method) {
        Ok(head) => head,
        Err(response) => return *response,
    };
    let request = match validate_health_request(&state.prom, params, body, head) {
        Ok(request) => request,
        Err(response) => return *response,
    };
    let deadline = match request.deadline() {
        Ok(deadline) => deadline,
        Err(reason) => {
            return crate::handlers::admin::health_rejection(
                &state.prom,
                StatusCode::BAD_REQUEST,
                "validation_error",
                reason,
                head,
            )
        }
    };

    loop {
        let current = collect_once(&state, deadline).await;
        if current.deadline_expired {
            return finish_health_response(&state.prom, cluster_response(&current, true), head);
        }
        if request.satisfied_by(current.status) {
            return finish_health_response(&state.prom, cluster_response(&current, false), head);
        }
        let Some(delay) = wait_delay(deadline) else {
            return finish_health_response(&state.prom, cluster_response(&current, true), head);
        };
        tokio::time::sleep(delay).await;
    }
}

async fn collect_once(state: &Arc<ClusterAppState>, deadline: Instant) -> ClusterHealth {
    let Some(admission_budget) = deadline.checked_duration_since(Instant::now()) else {
        return unavailable_fallback(state, "health deadline elapsed before admission", true);
    };
    let permit = match tokio::time::timeout(
        admission_budget,
        Arc::clone(&state.stats_permits).acquire_owned(),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(_)) => return unavailable_fallback(state, "health admission is closed", false),
        Err(_) => {
            return unavailable_fallback(state, "health admission deadline elapsed", true);
        }
    };
    let worker_state = Arc::clone(state);
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let cluster = worker_state.cluster.read();
        let shards = cluster.num_shards();
        let pending_repairs = cluster.pending_repairs();
        collect_cluster_health(&cluster).map_err(|source| (source, shards, pending_repairs))
    });
    let Some(probe_budget) = deadline.checked_duration_since(Instant::now()) else {
        return unavailable_fallback(
            state,
            "health deadline elapsed before dependency probe",
            true,
        );
    };
    match tokio::time::timeout(probe_budget, worker).await {
        Err(_) => unavailable_fallback(state, "health dependency probe deadline elapsed", true),
        Ok(Ok(Ok(health))) => health,
        Ok(Ok(Err((source, shards, pending_repairs)))) => {
            warn!(error = %source, "cluster health dependency probe failed");
            ClusterHealth {
                status: HealthStatus::Red,
                deadline_expired: false,
                shards,
                pending_repairs,
                reason: Some("required shard or control-plane probe failed"),
            }
        }
        Ok(Err(join_error)) => {
            error!(error = %join_error, "cluster health worker failed");
            unavailable_fallback(state, "health worker failed", false)
        }
    }
}

fn collect_cluster_health(cluster: &ClusterEngine) -> Result<ClusterHealth, ShardError> {
    let control = cluster.control_state()?;
    let counts = cluster.shard_query_counts()?;
    if control.num_shards as usize != counts.len() {
        return Err(ShardError::ControlPlane(format!(
            "committed shard count {} does not match the serving ring count {}",
            control.num_shards,
            counts.len()
        )));
    }

    let mut positions = BTreeSet::new();
    for assignment in &control.assignments {
        let position = assignment.position as usize;
        if position >= counts.len() {
            return Err(ShardError::ControlPlane(format!(
                "committed assignment names out-of-range shard position {position}"
            )));
        }
        if !positions.insert(position) {
            return Err(ShardError::ControlPlane(format!(
                "committed topology contains duplicate shard position {position}"
            )));
        }
    }
    if positions.len() != counts.len() {
        let missing = (0..counts.len())
            .find(|position| !positions.contains(position))
            .unwrap_or(positions.len());
        return Err(ShardError::ControlPlane(format!(
            "no committed node assignment for shard position {missing}"
        )));
    }

    let pending_repairs = cluster.pending_repairs();
    Ok(ClusterHealth {
        status: if pending_repairs > 0 {
            HealthStatus::Yellow
        } else {
            HealthStatus::Green
        },
        deadline_expired: false,
        shards: counts.len(),
        pending_repairs,
        reason: (pending_repairs > 0)
            .then_some("partial applies are queued; POST /_cluster/resync converges them"),
    })
}

fn unavailable_fallback(
    state: &ClusterAppState,
    log_message: &'static str,
    deadline_expired: bool,
) -> ClusterHealth {
    warn!(message = log_message, "cluster health unavailable");
    let (shards, pending_repairs) = state.cluster.try_read().map_or((0, 0), |cluster| {
        (cluster.num_shards(), cluster.pending_repairs())
    });
    ClusterHealth {
        status: HealthStatus::Red,
        deadline_expired,
        shards,
        pending_repairs,
        reason: Some("required shard or control-plane probe failed"),
    }
}

fn cluster_response(current: &ClusterHealth, timed_out: bool) -> Response {
    let status = if timed_out {
        StatusCode::REQUEST_TIMEOUT
    } else if current.status == HealthStatus::Red {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };
    (
        status,
        Json(ClusterHealthResponse {
            status: current.status.as_str(),
            mode: "cluster",
            timed_out,
            shards: current.shards,
            pending_repairs: current.pending_repairs,
            reason: timed_out
                .then_some("requested health status was not reached before timeout")
                .or(current.reason),
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use reverse_rusty::cluster::ClusterConfig;
    use reverse_rusty::Normalizer;

    #[test]
    fn healthy_in_process_topology_is_green() {
        let config = ClusterConfig {
            num_shards: 3,
            ..Default::default()
        };
        let cluster =
            ClusterEngine::build(Normalizer::default_vocab().expect("vocab"), &config, &[])
                .expect("cluster");
        let health = collect_cluster_health(&cluster).expect("health");
        assert_eq!(health.status, HealthStatus::Green);
        assert_eq!(health.shards, 3);
    }
}
