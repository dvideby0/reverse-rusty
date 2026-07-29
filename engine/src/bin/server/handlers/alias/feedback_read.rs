//! Strict native `GET`/`HEAD /_vocab/aliases/feedback` evidence report.

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
use tracing::error;

use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::{AppState, RequestCtx};

pub(crate) const ALIAS_FEEDBACK_READ_BODY_LIMIT: usize = 64 * 1024;
pub(crate) const ALIAS_FEEDBACK_READ_BODY_TIMEOUT: Duration = Duration::from_millis(250);
pub(crate) const ALIAS_FEEDBACK_READ_MAX_PAGE_SIZE: usize = 256;
const ALIAS_FEEDBACK_READ_RESPONSE_LIMIT: usize = 1024 * 1024;
const ALIAS_FEEDBACK_READ_ENDPOINT: &str = "vocab_aliases_feedback_get";

fn default_min_overlap() -> f64 {
    0.5
}

fn default_min_titles() -> u64 {
    50
}

fn default_min_queries() -> u64 {
    20
}

fn default_page_size() -> usize {
    ALIAS_FEEDBACK_READ_MAX_PAGE_SIZE
}

#[derive(Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
struct AliasFeedbackReadParams {
    #[serde(default = "default_min_overlap")]
    min_overlap: f64,
    #[serde(default = "default_min_titles")]
    min_titles: u64,
    #[serde(default = "default_min_queries")]
    min_queries: u64,
    /// Familiar Elasticsearch synonym-list offset.
    #[serde(default)]
    from: usize,
    /// Familiar Elasticsearch synonym-list page size.
    #[serde(default = "default_page_size")]
    size: usize,
}

impl Default for AliasFeedbackReadParams {
    fn default() -> Self {
        Self {
            min_overlap: default_min_overlap(),
            min_titles: default_min_titles(),
            min_queries: default_min_queries(),
            from: 0,
            size: default_page_size(),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct AliasFeedbackReadControls {
    min_overlap: f64,
    min_titles: u64,
    min_queries: u64,
    from: usize,
    size: usize,
}

impl AliasFeedbackReadParams {
    fn validate(self) -> Result<AliasFeedbackReadControls, String> {
        if !self.min_overlap.is_finite() || !(0.0..=1.0).contains(&self.min_overlap) {
            return Err("min_overlap must be finite and between 0 and 1".to_string());
        }
        if self.min_titles == 0 {
            return Err("min_titles must be at least 1".to_string());
        }
        if self.min_queries == 0 {
            return Err("min_queries must be at least 1".to_string());
        }
        if self.size > ALIAS_FEEDBACK_READ_MAX_PAGE_SIZE {
            return Err(format!(
                "size must not exceed {ALIAS_FEEDBACK_READ_MAX_PAGE_SIZE}"
            ));
        }
        Ok(AliasFeedbackReadControls {
            min_overlap: self.min_overlap,
            min_titles: self.min_titles,
            min_queries: self.min_queries,
            from: self.from,
            size: self.size,
        })
    }
}

/// Strict transport shared by standalone and coordinator feedback reads.
pub(crate) struct AliasFeedbackReadTransport {
    duration: HistogramTimer,
    started: Instant,
    controls: AliasFeedbackReadControls,
}

impl AliasFeedbackReadTransport {
    pub(crate) fn into_parts(self) -> (HistogramTimer, Instant, AliasFeedbackReadControls) {
        (self.duration, self.started, self.controls)
    }
}

impl<S> FromRequest<Arc<S>> for AliasFeedbackReadTransport
where
    S: RequestCtx,
{
    type Rejection = Response;

    async fn from_request(request: Request, state: &Arc<S>) -> Result<Self, Self::Rejection> {
        let started = Instant::now();
        let duration = state
            .prom()
            .http_request_duration
            .with_label_values(&[ALIAS_FEEDBACK_READ_ENDPOINT])
            .start_timer();
        match *request.method() {
            Method::GET | Method::HEAD => {}
            _ => {
                return Err(alias_feedback_read_rejection(
                    state.prom(),
                    StatusCode::METHOD_NOT_ALLOWED,
                    "method_not_allowed",
                    "GET and HEAD are the evidence methods supported by \
                     /_vocab/aliases/feedback",
                ));
            }
        }

        let Query(params) =
            Query::<AliasFeedbackReadParams>::try_from_uri(request.uri()).map_err(|source| {
                alias_feedback_read_rejection(
                    state.prom(),
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    format!("invalid alias-feedback query parameters: {source}"),
                )
            })?;
        let controls = params.validate().map_err(|reason| {
            alias_feedback_read_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "validation_error",
                reason,
            )
        })?;

        let body = tokio::time::timeout(
            ALIAS_FEEDBACK_READ_BODY_TIMEOUT,
            Bytes::from_request(request, state),
        )
        .await
        .map_err(|_| {
            alias_feedback_read_rejection(
                state.prom(),
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "alias-feedback read body did not complete within 250ms",
            )
        })?
        .map_err(|source| {
            let status = source.status();
            let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload_too_large"
            } else {
                "validation_error"
            };
            alias_feedback_read_rejection(
                state.prom(),
                status,
                error_type,
                format!("invalid alias-feedback read body: {source}"),
            )
        })?;
        if !body.is_empty() {
            return Err(alias_feedback_read_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "validation_error",
                "GET/HEAD /_vocab/aliases/feedback does not accept a request body",
            ));
        }

        Ok(Self {
            duration,
            started,
            controls,
        })
    }
}

#[derive(Serialize)]
struct AliasFeedbackReadResponse {
    took: u64,
    took_ms: f64,
    capture_enabled: bool,
    /// Total tracked pair count before paging.
    count: usize,
    /// Historical spelling retained for compatibility; equal to `count`.
    tracked_pairs: usize,
    min_overlap: f64,
    min_titles: u64,
    min_queries: u64,
    pairs: Vec<reverse_rusty::vocab::PairFeedback>,
}

enum AliasFeedbackReadWorkerError {
    Serialization(serde_json::Error),
    ResponseTooLarge(usize),
}

/// Snapshot one bounded evidence page under the feedback mutex, then release
/// it before source lookup, exclusion filtering, overlap calculation, and JSON
/// serialization.
pub(crate) async fn get_alias_feedback(
    State(state): State<Arc<AppState>>,
    transport: AliasFeedbackReadTransport,
) -> Response {
    let (_duration, started, controls) = transport.into_parts();
    let Ok(permit) = Arc::clone(&state.stats_permits).acquire_owned().await else {
        return alias_feedback_read_rejection(
            &state.prom,
            StatusCode::SERVICE_UNAVAILABLE,
            "aliases_unavailable",
            "alias-feedback read admission is closed",
        );
    };

    let worker_state = Arc::clone(&state);
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let (snapshot, count, feedback) = {
            let feedback = worker_state.feedback.lock();
            let snapshot = worker_state.snapshot.load_full();
            let (count, page) = feedback.snapshot_page(controls.from, controls.size);
            (snapshot, count, page)
        };
        let capture_enabled = snapshot.config().alias_feedback_capture;
        let pairs = feedback.report(
            controls.min_overlap,
            controls.min_titles,
            controls.min_queries,
            |id| snapshot.get_query_source(id),
        );
        let took_ms = started.elapsed().as_secs_f64() * 1_000.0;
        let encoded = serde_json::to_vec(&AliasFeedbackReadResponse {
            took: took_ms.floor() as u64,
            took_ms,
            capture_enabled,
            count,
            tracked_pairs: count,
            min_overlap: controls.min_overlap,
            min_titles: controls.min_titles,
            min_queries: controls.min_queries,
            pairs,
        })
        .map_err(AliasFeedbackReadWorkerError::Serialization)?;
        if encoded.len() > ALIAS_FEEDBACK_READ_RESPONSE_LIMIT {
            return Err(AliasFeedbackReadWorkerError::ResponseTooLarge(
                encoded.len(),
            ));
        }
        Ok(encoded)
    });

    finish_alias_feedback_read_worker(&state.prom, worker.await)
}

fn finish_alias_feedback_read_worker(
    prom: &PrometheusMetrics,
    result: Result<Result<Vec<u8>, AliasFeedbackReadWorkerError>, tokio::task::JoinError>,
) -> Response {
    match result {
        Ok(Ok(encoded)) => finish_alias_feedback_read_response(
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
        ),
        Ok(Err(AliasFeedbackReadWorkerError::ResponseTooLarge(bytes))) => {
            alias_feedback_read_rejection(
                prom,
                StatusCode::BAD_REQUEST,
                "validation_error",
                format!(
                    "alias-feedback page is {bytes} bytes, exceeding the \
                     {ALIAS_FEEDBACK_READ_RESPONSE_LIMIT}-byte response limit; lower size"
                ),
            )
        }
        Ok(Err(AliasFeedbackReadWorkerError::Serialization(source))) => {
            error!(error = %source, "failed to serialize alias-feedback report");
            alias_feedback_read_rejection(
                prom,
                StatusCode::INTERNAL_SERVER_ERROR,
                "aliases_unavailable",
                "alias-feedback report serialization failed",
            )
        }
        Err(join_error) => {
            error!(error = %join_error, "alias-feedback read worker failed");
            alias_feedback_read_rejection(
                prom,
                StatusCode::INTERNAL_SERVER_ERROR,
                "aliases_unavailable",
                "alias-feedback read worker failed",
            )
        }
    }
}

pub(crate) async fn alias_feedback_read_method_not_allowed<S: RequestCtx>(
    State(state): State<Arc<S>>,
    method: Method,
) -> Response {
    let _duration = state
        .prom()
        .http_request_duration
        .with_label_values(&[ALIAS_FEEDBACK_READ_ENDPOINT])
        .start_timer();
    let mut response = alias_feedback_read_rejection(
        state.prom(),
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        format!("{method} is not supported by /_vocab/aliases/feedback"),
    );
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("GET, HEAD"));
    response
}

fn alias_feedback_read_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    finish_alias_feedback_read_response(
        prom,
        ApiError::response(status, error_type, reason).into_response(),
    )
}

pub(crate) fn finish_alias_feedback_read_response(
    prom: &PrometheusMetrics,
    mut response: Response,
) -> Response {
    prom.http_requests_total
        .with_label_values(&[ALIAS_FEEDBACK_READ_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
