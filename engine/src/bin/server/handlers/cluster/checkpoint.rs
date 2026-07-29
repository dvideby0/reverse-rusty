//! Strict native `POST /_checkpoint` durability boundary.
//!
//! Elasticsearch/OpenSearch flush is already represented by `/_flush`.
//! Checkpoint additionally commits Reverse Rusty's coordinator manifest and
//! mutation-log cursor, so claiming a second flush spelling would be false.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::Bytes,
    extract::{FromRequest, Request, State},
    http::{header, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use prometheus::HistogramTimer;
use serde::Serialize;
use tracing::{error, info, instrument};

use reverse_rusty::cluster::ShardError;

use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::ClusterAppState;

use super::{shard_error_response, shard_error_status};

pub(crate) const CHECKPOINT_BODY_LIMIT: usize = 64 * 1024;
pub(crate) const CHECKPOINT_BODY_TIMEOUT: Duration = Duration::from_millis(250);
const CHECKPOINT_ENDPOINT: &str = "checkpoint";

/// Method/query validation plus bounded extraction for the body-free native
/// checkpoint request. The route timer begins before every transport check.
pub(crate) struct CheckpointTransport {
    duration: HistogramTimer,
    started: Instant,
}

impl CheckpointTransport {
    fn into_parts(self) -> (HistogramTimer, Instant) {
        (self.duration, self.started)
    }
}

impl FromRequest<Arc<ClusterAppState>> for CheckpointTransport {
    type Rejection = Response;

    async fn from_request(
        request: Request,
        state: &Arc<ClusterAppState>,
    ) -> Result<Self, Self::Rejection> {
        let started = Instant::now();
        let duration = state
            .prom
            .http_request_duration
            .with_label_values(&[CHECKPOINT_ENDPOINT])
            .start_timer();
        if request.method() != Method::POST {
            let mut response = checkpoint_rejection(
                &state.prom,
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "POST is the only supported /_checkpoint method",
            );
            response
                .headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static("POST"));
            return Err(response);
        }
        if request.uri().query().is_some_and(|query| !query.is_empty()) {
            return Err(checkpoint_rejection(
                &state.prom,
                StatusCode::BAD_REQUEST,
                "validation_error",
                "POST /_checkpoint does not accept query parameters",
            ));
        }

        let body =
            tokio::time::timeout(CHECKPOINT_BODY_TIMEOUT, Bytes::from_request(request, state))
                .await
                .map_err(|_| {
                    checkpoint_rejection(
                        &state.prom,
                        StatusCode::REQUEST_TIMEOUT,
                        "request_timeout",
                        "checkpoint request body did not complete within 250ms",
                    )
                })?
                .map_err(|source| {
                    let status = source.status();
                    let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
                        "payload_too_large"
                    } else {
                        "validation_error"
                    };
                    checkpoint_rejection(
                        &state.prom,
                        status,
                        error_type,
                        format!("invalid checkpoint body: {source}"),
                    )
                })?;
        if !body.is_empty() {
            return Err(checkpoint_rejection(
                &state.prom,
                StatusCode::BAD_REQUEST,
                "validation_error",
                "POST /_checkpoint does not accept a request body",
            ));
        }

        Ok(Self { duration, started })
    }
}

#[derive(Debug)]
struct CheckpointSuccess {
    durable: bool,
    epoch: u64,
    shards_checkpointed: usize,
}

#[derive(Serialize)]
struct CheckpointResponse {
    took: u64,
    took_ms: f64,
    acknowledged: bool,
    /// True only when this request committed a durable in-process coordinator
    /// manifest. A stateless or in-memory coordinator reports false.
    durable: bool,
    epoch: u64,
    /// Logical shard positions sealed into the durable checkpoint. This is zero
    /// when `durable` is false, including on a stateless remote coordinator.
    shards_checkpointed: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<&'static str>,
}

impl CheckpointResponse {
    fn new(took_ms: f64, success: &CheckpointSuccess) -> Self {
        Self {
            took: took_ms.floor() as u64,
            took_ms,
            acknowledged: true,
            durable: success.durable,
            epoch: success.epoch,
            shards_checkpointed: success.shards_checkpointed,
            message: (!success.durable).then_some(
                "no durable checkpoint was created because the coordinator has no data directory",
            ),
        }
    }
}

struct CheckpointCompletion {
    result: Result<CheckpointSuccess, ShardError>,
    took_ms: f64,
}

enum CheckpointExecutionError {
    AdmissionClosed,
    Reporter(tokio::task::JoinError),
    Worker(tokio::task::JoinError),
}

/// Run the durability commit outside Tokio's request workers. The owned
/// admission permit is shared with cluster backup and lives in the worker, so
/// disconnecting a client cannot admit overlapping durability work.
async fn execute_checkpoint(
    state: Arc<ClusterAppState>,
    started: Instant,
) -> Result<CheckpointCompletion, CheckpointExecutionError> {
    let permit = Arc::clone(&state.durability_permits)
        .acquire_owned()
        .await
        .map_err(|_| CheckpointExecutionError::AdmissionClosed)?;
    let work_state = Arc::clone(&state);
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let _writer = work_state.write_serial.lock();
        let cluster = work_state.cluster.read();
        let durable = cluster.is_durable();
        let shards_checkpointed = if durable { cluster.num_shards() } else { 0 };
        cluster.checkpoint().map(|()| CheckpointSuccess {
            durable,
            epoch: cluster.epoch(),
            shards_checkpointed,
        })
    });

    // Supervise completion independently from the request future. If the client
    // disconnects after admission, the durability result is still logged and
    // counted, just like the cluster backup path.
    let prom = state.prom.clone();
    let reporter = tokio::spawn(async move {
        let result = match worker.await {
            Ok(result) => result,
            Err(join_error) => {
                error!(error = %join_error, "cluster checkpoint worker failed");
                prom.http_requests_total
                    .with_label_values(&[CHECKPOINT_ENDPOINT, "500"])
                    .inc();
                return Err(join_error);
            }
        };
        let took_ms = started.elapsed().as_secs_f64() * 1_000.0;
        match &result {
            Ok(success) => {
                info!(
                    durable = success.durable,
                    epoch = success.epoch,
                    shards_checkpointed = success.shards_checkpointed,
                    took_ms,
                    "cluster checkpoint complete"
                );
                prom.http_requests_total
                    .with_label_values(&[CHECKPOINT_ENDPOINT, "200"])
                    .inc();
            }
            Err(source) => {
                let status = shard_error_status(source);
                error!(error = %source, "cluster checkpoint failed");
                prom.http_requests_total
                    .with_label_values(&[CHECKPOINT_ENDPOINT, status.as_str()])
                    .inc();
            }
        }
        Ok(CheckpointCompletion { result, took_ms })
    });

    reporter
        .await
        .map_err(CheckpointExecutionError::Reporter)?
        .map_err(CheckpointExecutionError::Worker)
}

/// `POST /_checkpoint` — seal every local shard position, atomically commit the
/// durable coordinator manifest, then truncate the captured mutation-log prefix.
/// Without a coordinator data directory it remains an acknowledged process-local
/// maintenance boundary, explicitly reported as `durable: false`.
#[instrument(skip_all)]
pub(crate) async fn cluster_checkpoint(
    State(state): State<Arc<ClusterAppState>>,
    transport: CheckpointTransport,
) -> Response {
    let (_duration, started) = transport.into_parts();
    let completion = match execute_checkpoint(Arc::clone(&state), started).await {
        Ok(completion) => completion,
        Err(CheckpointExecutionError::AdmissionClosed) => {
            return checkpoint_rejection(
                &state.prom,
                StatusCode::SERVICE_UNAVAILABLE,
                "checkpoint_unavailable",
                "checkpoint admission is closed",
            );
        }
        Err(CheckpointExecutionError::Reporter(join_error)) => {
            error!(error = %join_error, "cluster checkpoint completion reporter failed");
            state
                .prom
                .http_requests_total
                .with_label_values(&[CHECKPOINT_ENDPOINT, "500"])
                .inc();
            return finish_checkpoint_response(
                ApiError::response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "checkpoint_unavailable",
                    "checkpoint completion reporter failed",
                )
                .into_response(),
            );
        }
        Err(CheckpointExecutionError::Worker(join_error)) => {
            error!(error = %join_error, "cluster checkpoint worker failed");
            return finish_checkpoint_response(
                ApiError::response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "checkpoint_unavailable",
                    "checkpoint worker failed",
                )
                .into_response(),
            );
        }
    };

    match completion.result {
        Ok(success) => finish_checkpoint_response(
            Json(CheckpointResponse::new(completion.took_ms, &success)).into_response(),
        ),
        Err(source) => {
            finish_checkpoint_response(shard_error_response("checkpoint failed", &source))
        }
    }
}

fn checkpoint_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    prom.http_requests_total
        .with_label_values(&[CHECKPOINT_ENDPOINT, status.as_str()])
        .inc();
    finish_checkpoint_response(ApiError::response(status, error_type, reason).into_response())
}

fn finish_checkpoint_response(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
