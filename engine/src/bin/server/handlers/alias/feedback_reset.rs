//! Strict native `POST /_vocab/aliases/feedback/reset` evidence-window boundary.

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
use tracing::error;

use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::{AppState, RequestCtx};

pub(crate) const ALIAS_FEEDBACK_RESET_BODY_LIMIT: usize = 64 * 1024;
pub(crate) const ALIAS_FEEDBACK_RESET_BODY_TIMEOUT: Duration = Duration::from_millis(250);
const ALIAS_FEEDBACK_RESET_ENDPOINT: &str = "vocab_aliases_feedback_reset_post";

/// Method, query, and body contract shared by standalone and coordinator mode.
/// The coordinator validates it before returning the capability boundary.
pub(crate) struct AliasFeedbackResetTransport {
    duration: HistogramTimer,
    started: Instant,
}

impl AliasFeedbackResetTransport {
    pub(crate) fn into_parts(self) -> (HistogramTimer, Instant) {
        (self.duration, self.started)
    }
}

impl<S> FromRequest<Arc<S>> for AliasFeedbackResetTransport
where
    S: RequestCtx,
{
    type Rejection = Response;

    async fn from_request(request: Request, state: &Arc<S>) -> Result<Self, Self::Rejection> {
        let started = Instant::now();
        let duration = state
            .prom()
            .http_request_duration
            .with_label_values(&[ALIAS_FEEDBACK_RESET_ENDPOINT])
            .start_timer();
        if request.method() != Method::POST {
            return Err(alias_feedback_reset_rejection(
                state.prom(),
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "POST is the evidence-window method supported by \
                 /_vocab/aliases/feedback/reset",
            ));
        }
        if request.uri().query().is_some_and(|query| !query.is_empty()) {
            return Err(alias_feedback_reset_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "validation_error",
                "POST /_vocab/aliases/feedback/reset does not accept query parameters",
            ));
        }

        let body = tokio::time::timeout(
            ALIAS_FEEDBACK_RESET_BODY_TIMEOUT,
            Bytes::from_request(request, state),
        )
        .await
        .map_err(|_| {
            alias_feedback_reset_rejection(
                state.prom(),
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "alias-feedback reset body did not complete within 250ms",
            )
        })?
        .map_err(|source| {
            let status = source.status();
            let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload_too_large"
            } else {
                "validation_error"
            };
            alias_feedback_reset_rejection(
                state.prom(),
                status,
                error_type,
                format!("invalid alias-feedback reset body: {source}"),
            )
        })?;
        if !body.is_empty() {
            return Err(alias_feedback_reset_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "validation_error",
                "POST /_vocab/aliases/feedback/reset does not accept a request body",
            ));
        }

        Ok(Self { duration, started })
    }
}

#[derive(Serialize)]
struct AliasFeedbackResetResponse {
    took: u64,
    took_ms: f64,
    acknowledged: bool,
    capture_enabled: bool,
    tracked_pairs: usize,
}

/// Clear every tracked pair's counters and sketches at one feedback-mutex
/// linearization point while preserving the pair universe and its tokenization.
pub(crate) async fn reset_alias_feedback(
    State(state): State<Arc<AppState>>,
    transport: AliasFeedbackResetTransport,
) -> Response {
    let (_duration, started) = transport.into_parts();
    let Ok(permit) = Arc::clone(&state.stats_permits).acquire_owned().await else {
        return alias_feedback_reset_rejection(
            &state.prom,
            StatusCode::SERVICE_UNAVAILABLE,
            "aliases_unavailable",
            "alias-feedback reset admission is closed",
        );
    };

    let worker_state = Arc::clone(&state);
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        worker_state.feedback.lock().clear_evidence()
    });
    let response = match worker.await {
        Ok(tracked_pairs) => {
            let took_ms = started.elapsed().as_secs_f64() * 1_000.0;
            Json(AliasFeedbackResetResponse {
                took: took_ms.floor() as u64,
                took_ms,
                acknowledged: true,
                capture_enabled: state.snapshot.load().config().alias_feedback_capture,
                tracked_pairs,
            })
            .into_response()
        }
        Err(join_error) => {
            error!(error = %join_error, "alias-feedback reset worker failed");
            ApiError::response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "aliases_unavailable",
                "alias-feedback reset worker failed",
            )
            .into_response()
        }
    };
    finish_alias_feedback_reset_response(&state.prom, response)
}

pub(crate) async fn alias_feedback_reset_method_not_allowed<S: RequestCtx>(
    State(state): State<Arc<S>>,
    method: Method,
) -> Response {
    let _duration = state
        .prom()
        .http_request_duration
        .with_label_values(&[ALIAS_FEEDBACK_RESET_ENDPOINT])
        .start_timer();
    let mut response = alias_feedback_reset_rejection(
        state.prom(),
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        format!("{method} is not supported by /_vocab/aliases/feedback/reset"),
    );
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("POST"));
    response
}

fn alias_feedback_reset_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    finish_alias_feedback_reset_response(
        prom,
        ApiError::response(status, error_type, reason).into_response(),
    )
}

pub(crate) fn finish_alias_feedback_reset_response(
    prom: &PrometheusMetrics,
    mut response: Response,
) -> Response {
    prom.http_requests_total
        .with_label_values(&[ALIAS_FEEDBACK_RESET_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
