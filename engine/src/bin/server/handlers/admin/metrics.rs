//! Strict native `GET`/`HEAD /_metrics` Prometheus exposition contract.

use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::{Body, Bytes},
    extract::{FromRequest, Request, State},
    http::{header, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
};
use prometheus::{Encoder, HistogramTimer, TextEncoder};
use tracing::error;

use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::{AppState, RequestCtx};

pub(crate) const METRICS_BODY_LIMIT: usize = 64 * 1024;
pub(crate) const METRICS_BODY_READ_TIMEOUT: Duration = Duration::from_millis(250);
const METRICS_ENDPOINT: &str = "metrics";

/// Method/query validation and bounded body extraction for the read-mostly
/// metrics surface. The duration timer starts before every transport check.
pub(crate) struct MetricsTransport {
    duration: HistogramTimer,
    head: bool,
}

impl MetricsTransport {
    pub(crate) fn into_parts(self) -> (HistogramTimer, bool) {
        (self.duration, self.head)
    }
}

impl<S> FromRequest<Arc<S>> for MetricsTransport
where
    S: RequestCtx,
{
    type Rejection = Response;

    async fn from_request(request: Request, state: &Arc<S>) -> Result<Self, Self::Rejection> {
        let duration = state
            .prom()
            .http_request_duration
            .with_label_values(&[METRICS_ENDPOINT])
            .start_timer();
        let head = validate_metrics_method(state.prom(), request.method())
            .map_err(|response| *response)?;
        if request.uri().query().is_some_and(|query| !query.is_empty()) {
            return Err(metrics_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "validation_error",
                "GET/HEAD /_metrics does not accept query parameters",
                head,
            ));
        }

        let body = tokio::time::timeout(
            METRICS_BODY_READ_TIMEOUT,
            Bytes::from_request(request, state),
        )
        .await
        .map_err(|_| {
            metrics_rejection(
                state.prom(),
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "metrics request body did not complete within 250ms",
                head,
            )
        })?
        .map_err(|error| {
            let status = error.status();
            let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload_too_large"
            } else {
                "validation_error"
            };
            metrics_rejection(
                state.prom(),
                status,
                error_type,
                format!("invalid metrics body: {error}"),
                head,
            )
        })?;
        if !body.is_empty() {
            return Err(metrics_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "validation_error",
                "GET/HEAD /_metrics does not accept a request body",
                head,
            ));
        }

        Ok(Self { duration, head })
    }
}

/// `GET`/`HEAD /_metrics` — Prometheus text 0.0.4 from a lock-free engine
/// snapshot plus the process-local event and HTTP registry.
pub(crate) async fn prometheus_metrics(
    State(state): State<Arc<AppState>>,
    transport: MetricsTransport,
) -> Response {
    let (_duration, head) = transport.into_parts();
    let metrics = {
        let snapshot = state.snapshot.load();
        snapshot.metrics()
    };
    state.prom.refresh_gauges(&metrics);
    encode_metrics(&state.prom, head)
}

fn validate_metrics_method(
    prom: &PrometheusMetrics,
    method: &Method,
) -> Result<bool, Box<Response>> {
    if method == Method::GET {
        return Ok(false);
    }
    if method == Method::HEAD {
        return Ok(true);
    }
    let mut response = metrics_rejection(
        prom,
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        "GET and HEAD are the only supported /_metrics methods",
        false,
    );
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("GET, HEAD"));
    Err(Box::new(response))
}

pub(crate) fn encode_metrics(prom: &PrometheusMetrics, head: bool) -> Response {
    let encoder = TextEncoder::new();
    let families = prom.registry.gather();
    let mut buffer = Vec::new();
    if let Err(encode_error) = encoder.encode(&families, &mut buffer) {
        error!(error = %encode_error, "failed to encode prometheus metrics");
        return metrics_rejection(
            prom,
            StatusCode::INTERNAL_SERVER_ERROR,
            "metrics_unavailable",
            "metrics encoding failed",
            head,
        );
    }
    finish_metrics_response(
        prom,
        (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )],
            buffer,
        )
            .into_response(),
        head,
    )
}

pub(crate) fn metrics_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
    head: bool,
) -> Response {
    finish_metrics_response(
        prom,
        ApiError::response(status, error_type, reason).into_response(),
        head,
    )
}

pub(crate) fn finish_metrics_response(
    prom: &PrometheusMetrics,
    mut response: Response,
    head: bool,
) -> Response {
    prom.http_requests_total
        .with_label_values(&[METRICS_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if head {
        *response.body_mut() = Body::empty();
    }
    response
}
