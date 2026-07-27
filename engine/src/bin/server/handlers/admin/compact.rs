//! Strict native `/_compact` and ES/OpenSearch-familiar `/_forcemerge` boundaries.

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
use serde::{Deserialize, Serialize};
use tracing::{error, info, instrument};

use reverse_rusty::segment::{CompactionReport, Engine};

use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::AppState;

#[derive(Clone, Copy)]
enum CompactDialect {
    Native,
    ForceMerge,
}

#[derive(Clone, Copy)]
enum MergeMode {
    All,
    Policy,
}

#[derive(Clone, Copy)]
struct PreparedCompact {
    dialect: CompactDialect,
    merge_mode: MergeMode,
    flush: bool,
}

/// ES/OpenSearch force-merge controls that have a truthful single-index
/// projection. Index selection controls do not apply to Reverse Rusty's one
/// implicit `queries` index and therefore remain unknown fields.
#[derive(Clone, Copy, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CompactParams {
    max_num_segments: Option<usize>,
    only_expunge_deletes: Option<bool>,
    flush: Option<bool>,
    wait_for_completion: Option<bool>,
}

impl CompactParams {
    fn is_empty(self) -> bool {
        self.max_num_segments.is_none()
            && self.only_expunge_deletes.is_none()
            && self.flush.is_none()
            && self.wait_for_completion.is_none()
    }
}

#[derive(Serialize)]
struct CompactShards {
    total: usize,
    successful: usize,
    failed: usize,
}

/// The familiar shard projection is shared by both dialects. Existing native
/// report fields remain additive detail for operators.
#[derive(Serialize)]
struct CompactResponse {
    took: u64,
    took_ms: f64,
    acknowledged: bool,
    #[serde(rename = "_shards")]
    shards: CompactShards,
    #[serde(skip_serializing_if = "Option::is_none")]
    segments_merged: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    entries_before: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    entries_after: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tombstones_reclaimed: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reanchored: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hot_promoted: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hot_demoted: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<&'static str>,
}

impl CompactResponse {
    fn new(
        took_ms: f64,
        acknowledged: bool,
        report: Option<CompactionReport>,
        message: Option<&'static str>,
    ) -> Self {
        let successful = usize::from(acknowledged);
        Self {
            took: took_ms.floor() as u64,
            took_ms,
            acknowledged,
            shards: CompactShards {
                total: 1,
                successful,
                failed: 1 - successful,
            },
            segments_merged: report.map(|report| report.segments_merged),
            entries_before: report.map(|report| report.entries_before),
            entries_after: report.map(|report| report.entries_after),
            tombstones_reclaimed: report.map(|report| report.tombstones_reclaimed),
            reanchored: report.map(|report| report.reanchored),
            hot_promoted: report.map(|report| report.hot_promoted),
            hot_demoted: report.map(|report| report.hot_demoted),
            message,
        }
    }
}

fn compact_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    prom.http_requests_total
        .with_label_values(&["compact", status.as_str()])
        .inc();
    ApiError::response(status, error_type, reason).into_response()
}

fn validate_method(
    prom: &PrometheusMetrics,
    method: &Method,
    endpoint: &'static str,
) -> Result<(), Box<Response>> {
    if *method == Method::POST {
        return Ok(());
    }
    let mut response = Box::new(compact_rejection(
        prom,
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        format!("POST is the only supported {endpoint} method"),
    ));
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("POST"));
    Err(response)
}

fn validate_request(
    prom: &PrometheusMetrics,
    dialect: CompactDialect,
    params: Result<Query<CompactParams>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<PreparedCompact, Box<Response>> {
    let endpoint = match dialect {
        CompactDialect::Native => "/_compact",
        CompactDialect::ForceMerge => "/_forcemerge",
    };
    let Query(params) = params.map_err(|error| {
        Box::new(compact_rejection(
            prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            format!("invalid {endpoint} query parameters: {error}"),
        ))
    })?;
    let body = body.map_err(|error| {
        let status = error.status();
        let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
            "payload_too_large"
        } else {
            "validation_error"
        };
        Box::new(compact_rejection(
            prom,
            status,
            error_type,
            format!("invalid {endpoint} body: {error}"),
        ))
    })?;
    if !body.is_empty() {
        return Err(Box::new(compact_rejection(
            prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            format!("POST {endpoint} does not accept a request body"),
        )));
    }

    match dialect {
        CompactDialect::Native => {
            if !params.is_empty() {
                return Err(Box::new(compact_rejection(
                    prom,
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    "POST /_compact does not accept query parameters; use POST \
                     /_forcemerge for ES/OpenSearch controls",
                )));
            }
            Ok(PreparedCompact {
                dialect,
                merge_mode: MergeMode::All,
                flush: false,
            })
        }
        CompactDialect::ForceMerge => {
            if params.max_num_segments.is_some_and(|target| target != 1) {
                return Err(Box::new(compact_rejection(
                    prom,
                    StatusCode::BAD_REQUEST,
                    "illegal_argument_exception",
                    "Reverse Rusty supports max_num_segments=1 only",
                )));
            }
            if params.only_expunge_deletes.unwrap_or(false) {
                return Err(Box::new(compact_rejection(
                    prom,
                    StatusCode::BAD_REQUEST,
                    "illegal_argument_exception",
                    "only_expunge_deletes=true is not supported; omit it or set it to false",
                )));
            }
            if !params.wait_for_completion.unwrap_or(true) {
                return Err(Box::new(compact_rejection(
                    prom,
                    StatusCode::BAD_REQUEST,
                    "illegal_argument_exception",
                    "wait_for_completion=false requires a task API that Reverse Rusty does not expose",
                )));
            }
            Ok(PreparedCompact {
                dialect,
                merge_mode: if params.max_num_segments == Some(1) {
                    MergeMode::All
                } else {
                    MergeMode::Policy
                },
                flush: params.flush.unwrap_or(true),
            })
        }
    }
}

#[derive(Clone, Copy)]
struct CompactOutcome {
    report: Option<CompactionReport>,
    persistence_healthy: bool,
}

fn run_compact(engine: &mut Engine, prepared: PreparedCompact) -> CompactOutcome {
    if !engine.persistence_healthy() {
        return CompactOutcome {
            report: None,
            persistence_healthy: false,
        };
    }
    // The compatibility flush is performed under the same writer lock
    // before selection so a requested max_num_segments target covers the
    // formerly-mutable delta as well as the already-sealed base. Suppress
    // the ordinary post-flush policy pass for this one seal: the explicit
    // force-merge selection below owns the operation and its report.
    if prepared.flush {
        let original_config = engine.config().clone();
        let mut flush_config = original_config.clone();
        flush_config.auto_compact_on_flush = false;
        engine.set_config(flush_config);
        engine.flush();
        engine.set_config(original_config);
    }
    if !engine.persistence_healthy() {
        return CompactOutcome {
            report: None,
            persistence_healthy: false,
        };
    }
    let report = match prepared.merge_mode {
        MergeMode::All => engine.compact_all(),
        MergeMode::Policy => engine.maybe_compact(),
    };
    CompactOutcome {
        report,
        persistence_healthy: engine.persistence_healthy(),
    }
}

async fn execute_compact(
    state: Arc<AppState>,
    prepared: PreparedCompact,
) -> Result<CompactOutcome, ()> {
    let work_state = state;
    let outcome = tokio::task::spawn_blocking(move || {
        let outcome = {
            let mut engine = work_state.engine.lock();
            run_compact(&mut engine, prepared)
        };
        // Publication belongs to the maintenance task, not the HTTP future:
        // dropping a disconnected request must not leave completed physical
        // work invisible until some unrelated later write republishes.
        work_state.publish_snapshot();
        outcome
    })
    .await
    .map_err(|error| {
        error!(%error, "compaction worker failed");
    })?;
    Ok(outcome)
}

#[cfg(test)]
pub(super) async fn execute_native_for_test(state: Arc<AppState>) -> Result<(), ()> {
    execute_compact(
        state,
        PreparedCompact {
            dialect: CompactDialect::Native,
            merge_mode: MergeMode::All,
            flush: false,
        },
    )
    .await
    .map(|_| ())
}

async fn compact_handler(
    state: Arc<AppState>,
    method: Method,
    dialect: CompactDialect,
    params: Result<Query<CompactParams>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let _duration = state
        .prom
        .http_request_duration
        .with_label_values(&["compact"])
        .start_timer();
    let started = Instant::now();
    let endpoint = match dialect {
        CompactDialect::Native => "/_compact",
        CompactDialect::ForceMerge => "/_forcemerge",
    };
    if let Err(response) = validate_method(&state.prom, &method, endpoint) {
        return *response;
    }
    let prepared = match validate_request(&state.prom, dialect, params, body) {
        Ok(prepared) => prepared,
        Err(response) => return *response,
    };
    let Ok(outcome) = execute_compact(Arc::clone(&state), prepared).await else {
        return compact_rejection(
            &state.prom,
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "compaction worker failed",
        );
    };
    let took_ms = started.elapsed().as_secs_f64() * 1_000.0;

    let (status, message) = if !outcome.persistence_healthy {
        error!(
            endpoint,
            "compaction not durably acknowledged; source segments remain available"
        );
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Some("persistence degraded; compaction not durably acknowledged"),
        )
    } else if let Some(report) = outcome.report {
        info!(
            endpoint,
            segments_merged = report.segments_merged,
            entries_before = report.entries_before,
            entries_after = report.entries_after,
            tombstones_reclaimed = report.tombstones_reclaimed,
            reanchored = report.reanchored,
            hot_promoted = report.hot_promoted,
            hot_demoted = report.hot_demoted,
            "compaction complete"
        );
        (StatusCode::OK, None)
    } else {
        info!(endpoint, "no segment merge needed");
        let message = match prepared.dialect {
            CompactDialect::Native => "nothing to compact",
            CompactDialect::ForceMerge => "no segment merge needed",
        };
        (StatusCode::OK, Some(message))
    };
    state
        .prom
        .http_requests_total
        .with_label_values(&["compact", status.as_str()])
        .inc();
    (
        status,
        Json(CompactResponse::new(
            took_ms,
            status == StatusCode::OK,
            outcome.report.filter(|_| status == StatusCode::OK),
            message,
        )),
    )
        .into_response()
}

/// Native force-all compaction. The memtable remains a separate mutable delta.
#[instrument(skip_all)]
pub(crate) async fn compact_route(
    State(state): State<Arc<AppState>>,
    method: Method,
    params: Result<Query<CompactParams>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    compact_handler(state, method, CompactDialect::Native, params, body).await
}

/// ES/OpenSearch-familiar force merge over Reverse Rusty's implicit `queries` index.
#[instrument(skip_all)]
pub(crate) async fn force_merge_route(
    State(state): State<Arc<AppState>>,
    method: Method,
    params: Result<Query<CompactParams>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    compact_handler(state, method, CompactDialect::ForceMerge, params, body).await
}
