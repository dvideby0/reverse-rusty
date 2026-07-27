//! Strict ES/OpenSearch-familiar `/_flush` request and response boundary.

use std::sync::Arc;
use std::time::Instant;

use axum::{
    body::Bytes,
    extract::{
        rejection::{BytesRejection, QueryRejection},
        Query, State,
    },
    http::{header, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};
use tracing::{error, info, instrument};

use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::AppState;

/// The ES/OpenSearch controls Reverse Rusty can honor without inventing index
/// selection semantics for its one implicit `queries` index.
#[derive(Clone, Copy, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FlushParams {
    force: Option<bool>,
    wait_if_ongoing: Option<bool>,
}

impl FlushParams {
    #[must_use]
    pub(crate) fn force_requested(self) -> bool {
        self.force.unwrap_or(false)
    }

    #[must_use]
    fn wait_if_ongoing(self) -> bool {
        self.wait_if_ongoing.unwrap_or(true)
    }
}

#[derive(Serialize)]
struct FlushShards {
    total: usize,
    successful: usize,
    failed: usize,
}

/// Shared success/degraded envelope. `_shards` is the familiar ES/OpenSearch
/// projection; the existing native fields remain additive compatibility data.
#[derive(Serialize)]
pub(crate) struct FlushResponse {
    took: u64,
    took_ms: f64,
    acknowledged: bool,
    #[serde(rename = "_shards")]
    shards: FlushShards,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_queries: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    base_segments: Option<usize>,
}

impl FlushResponse {
    #[must_use]
    pub(crate) fn new(
        took_ms: f64,
        acknowledged: bool,
        total_shards: usize,
        successful_shards: usize,
        total_queries: Option<usize>,
        base_segments: Option<usize>,
    ) -> Self {
        Self {
            took: took_ms.floor() as u64,
            took_ms,
            acknowledged,
            shards: FlushShards {
                total: total_shards,
                successful: successful_shards,
                failed: total_shards.saturating_sub(successful_shards),
            },
            total_queries,
            base_segments,
        }
    }
}

pub(crate) type FlushRejection = Box<Response>;

fn flush_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    prom.http_requests_total
        .with_label_values(&["flush", status.as_str()])
        .inc();
    ApiError::response(status, error_type, reason).into_response()
}

pub(crate) fn validate_flush_method(
    prom: &PrometheusMetrics,
    method: &Method,
) -> Result<(), FlushRejection> {
    if *method == Method::GET || *method == Method::POST {
        return Ok(());
    }
    let mut response = Box::new(flush_rejection(
        prom,
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        "GET and POST are the only supported /_flush methods",
    ));
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("GET, POST"));
    Err(response)
}

/// Decode the complete body-free transport contract before acquiring either
/// flush or writer admission.
pub(crate) fn validate_flush_request(
    prom: &PrometheusMetrics,
    params: Result<Query<FlushParams>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<FlushParams, FlushRejection> {
    let Query(params) = params.map_err(|error| {
        Box::new(flush_rejection(
            prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            format!("invalid flush query parameters: {error}"),
        ))
    })?;
    let body = body.map_err(|error| {
        let status = error.status();
        let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
            "payload_too_large"
        } else {
            "validation_error"
        };
        Box::new(flush_rejection(
            prom,
            status,
            error_type,
            format!("invalid flush body: {error}"),
        ))
    })?;
    if !body.is_empty() {
        return Err(Box::new(flush_rejection(
            prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            "GET and POST /_flush do not accept a request body",
        )));
    }
    Ok(params)
}

/// Acquire only explicit-flush admission. A non-waiting request rejects when
/// another explicit flush owns this mutex, but it does not misclassify an
/// unrelated document write as an ongoing flush.
pub(crate) fn acquire_flush<'a>(
    serial: &'a Mutex<()>,
    params: FlushParams,
    prom: &PrometheusMetrics,
) -> Result<MutexGuard<'a, ()>, FlushRejection> {
    if params.wait_if_ongoing() {
        Ok(serial.lock())
    } else {
        serial.try_lock().ok_or_else(|| {
            Box::new(flush_rejection(
                prom,
                StatusCode::CONFLICT,
                "flush_in_progress_exception",
                "another flush request is already in progress; retry or use \
                 wait_if_ongoing=true",
            ))
        })
    }
}

/// GET/POST `/_flush` — seal the standalone memtable and publish the resulting
/// immutable read snapshot.
#[instrument(skip_all)]
pub(crate) async fn flush_route(
    State(state): State<Arc<AppState>>,
    method: Method,
    params: Result<Query<FlushParams>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let _duration = state
        .prom
        .http_request_duration
        .with_label_values(&["flush"])
        .start_timer();
    let started = Instant::now();
    if let Err(response) = validate_flush_method(&state.prom, &method) {
        return *response;
    }
    let params = match validate_flush_request(&state.prom, params, body) {
        Ok(params) => params,
        Err(response) => return *response,
    };
    let force = params.force_requested();
    let _flush = match acquire_flush(&state.flush_serial, params, &state.prom) {
        Ok(guard) => guard,
        Err(response) => return *response,
    };

    let (metrics, persistence_healthy) = {
        let mut engine = state.engine.lock();
        engine.flush();
        (engine.metrics(), engine.persistence_healthy())
    };
    state.publish_snapshot();

    let (status, code, successful_shards) = if persistence_healthy {
        (StatusCode::OK, "200", 1)
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "503", 0)
    };
    if persistence_healthy {
        info!(
            force,
            total_queries = metrics.total_queries,
            base_segments = metrics.base_segments,
            "flush complete"
        );
    } else {
        error!(
            force,
            total_queries = metrics.total_queries,
            base_segments = metrics.base_segments,
            "flush could not be durably persisted; data retained in WAL, persistence degraded"
        );
    }
    state
        .prom
        .http_requests_total
        .with_label_values(&["flush", code])
        .inc();
    let took_ms = started.elapsed().as_secs_f64() * 1_000.0;
    (
        status,
        Json(FlushResponse::new(
            took_ms,
            persistence_healthy,
            1,
            successful_shards,
            Some(metrics.total_queries),
            Some(metrics.base_segments),
        )),
    )
        .into_response()
}
