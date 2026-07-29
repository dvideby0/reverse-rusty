//! Strict native `POST /_vocab/aliases/discover_and_record` mutation.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::Bytes,
    extract::{FromRequest, Request, State},
    http::{header, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
};
use prometheus::HistogramTimer;
use serde::Serialize;
use tracing::{error, info};

use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::{AppState, RequestCtx};

use super::discover::{is_json_content_type, parse_alias_discover_record_config};

/// This endpoint accepts only discovery controls, never a caller corpus.
pub(crate) const ALIAS_DISCOVER_RECORD_BODY_LIMIT: usize = 64 * 1024;
pub(crate) const ALIAS_DISCOVER_RECORD_BODY_TIMEOUT: Duration = Duration::from_secs(5);
const ALIAS_DISCOVER_RECORD_RESPONSE_LIMIT: usize = 64 * 1024;
const ALIAS_DISCOVER_RECORD_ENDPOINT: &str = "vocab_aliases_discover_and_record";

/// Method/query/media/body validation shared by standalone and coordinator
/// routes. Coordinator mode validates the transport before returning its
/// explicit 501 boundary.
pub(crate) struct AliasDiscoverRecordTransport {
    duration: HistogramTimer,
    started: Instant,
    body: Bytes,
}

impl AliasDiscoverRecordTransport {
    pub(crate) fn into_parts(self) -> (HistogramTimer, Instant, Bytes) {
        (self.duration, self.started, self.body)
    }
}

impl<S> FromRequest<Arc<S>> for AliasDiscoverRecordTransport
where
    S: RequestCtx,
{
    type Rejection = Response;

    async fn from_request(request: Request, state: &Arc<S>) -> Result<Self, Self::Rejection> {
        let started = Instant::now();
        let duration = state
            .prom()
            .http_request_duration
            .with_label_values(&[ALIAS_DISCOVER_RECORD_ENDPOINT])
            .start_timer();
        if request.method() != Method::POST {
            return Err(alias_discover_record_rejection(
                state.prom(),
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "POST is the distributional alias recording method supported by \
                 /_vocab/aliases/discover_and_record",
            ));
        }
        if request.uri().query().is_some_and(|query| !query.is_empty()) {
            return Err(alias_discover_record_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "validation_error",
                "POST /_vocab/aliases/discover_and_record does not accept query parameters",
            ));
        }

        let json_content_type = is_json_content_type(request.headers());
        let body = tokio::time::timeout(
            ALIAS_DISCOVER_RECORD_BODY_TIMEOUT,
            Bytes::from_request(request, state),
        )
        .await
        .map_err(|_| {
            alias_discover_record_rejection(
                state.prom(),
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "alias discover-and-record body did not complete within 5s",
            )
        })?
        .map_err(|source| {
            let status = source.status();
            let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload_too_large"
            } else {
                "validation_error"
            };
            alias_discover_record_rejection(
                state.prom(),
                status,
                error_type,
                format!("invalid alias discover-and-record body: {source}"),
            )
        })?;
        if !body.is_empty() && !json_content_type {
            return Err(alias_discover_record_rejection(
                state.prom(),
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                "a non-empty POST /_vocab/aliases/discover_and_record body requires \
                 Content-Type: application/json",
            ));
        }

        Ok(Self {
            duration,
            started,
            body,
        })
    }
}

#[derive(Serialize)]
struct AliasDiscoverRecordResponse {
    took: u64,
    took_ms: f64,
    acknowledged: bool,
    /// Runtime vocabulary changes are not written back to the standalone
    /// operator's vocabulary file.
    persisted: bool,
    proposed: usize,
    new_candidates: usize,
    rediscovered: usize,
    rejected_sticky: usize,
    recompiled: usize,
    summary: reverse_rusty::vocab::AliasSummary,
}

enum AliasDiscoverRecordWorkerError {
    Invalid(String),
    Mutation(String),
    Serialization(serde_json::Error),
}

pub(crate) fn validate_alias_discover_record_body(
    body: &[u8],
) -> Result<reverse_rusty::vocab::DistributionalConfig, String> {
    parse_alias_discover_record_config(body)
}

/// Capture stored sources briefly, discover without the engine writer guard,
/// then reacquire the guard only to install review-only metadata. The blocking
/// worker owns admission and snapshot publication through terminal completion.
pub(crate) async fn discover_and_record_aliases(
    State(state): State<Arc<AppState>>,
    transport: AliasDiscoverRecordTransport,
) -> Response {
    let (_duration, started, body) = transport.into_parts();
    let Ok(permit) = Arc::clone(&state.stats_permits).acquire_owned().await else {
        return alias_discover_record_rejection(
            &state.prom,
            StatusCode::SERVICE_UNAVAILABLE,
            "aliases_unavailable",
            "alias discover-and-record admission is closed",
        );
    };

    let work_state = Arc::clone(&state);
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let config = validate_alias_discover_record_body(&body)
            .map_err(AliasDiscoverRecordWorkerError::Invalid)?;
        let queries = work_state.engine.lock().live_sources();
        let proposals = reverse_rusty::vocab::discover_pairs(&queries, &config);
        let report = {
            let mut engine = work_state.engine.lock();
            engine
                .record_discovered_aliases(&proposals)
                .map_err(|source| AliasDiscoverRecordWorkerError::Mutation(source.to_string()))?
        };
        work_state.publish_snapshot();

        let took_ms = started.elapsed().as_secs_f64() * 1_000.0;
        let encoded = serde_json::to_vec(&AliasDiscoverRecordResponse {
            took: took_ms.floor() as u64,
            took_ms,
            acknowledged: true,
            persisted: false,
            proposed: report.proposed,
            new_candidates: report.new_candidates,
            rediscovered: report.rediscovered,
            rejected_sticky: report.rejected_sticky,
            recompiled: 0,
            summary: report.summary,
        })
        .map_err(AliasDiscoverRecordWorkerError::Serialization)?;
        if encoded.len() > ALIAS_DISCOVER_RECORD_RESPONSE_LIMIT {
            return Err(AliasDiscoverRecordWorkerError::Mutation(
                "alias discover-and-record response exceeded its fixed serialization limit"
                    .to_string(),
            ));
        }
        Ok((encoded, report))
    });

    let response = match worker.await {
        Ok(Ok((encoded, report))) => {
            info!(
                proposed = report.proposed,
                new_candidates = report.new_candidates,
                rediscovered = report.rediscovered,
                rejected_sticky = report.rejected_sticky,
                "distributional alias candidates recorded for review"
            );
            (
                StatusCode::OK,
                [(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                )],
                encoded,
            )
                .into_response()
        }
        Ok(Err(AliasDiscoverRecordWorkerError::Invalid(reason))) => {
            alias_discover_record_error_response(
                StatusCode::BAD_REQUEST,
                "validation_error",
                reason,
            )
        }
        Ok(Err(AliasDiscoverRecordWorkerError::Mutation(reason))) => {
            error!(error = %reason, "alias discover-and-record mutation failed");
            alias_discover_record_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "aliases_unavailable",
                "alias discover-and-record mutation failed",
            )
        }
        Ok(Err(AliasDiscoverRecordWorkerError::Serialization(source))) => {
            error!(error = %source, "failed to serialize alias discover-and-record response");
            alias_discover_record_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "aliases_unavailable",
                "alias discover-and-record response serialization failed",
            )
        }
        Err(join_error) => {
            error!(error = %join_error, "alias discover-and-record worker failed");
            alias_discover_record_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "aliases_unavailable",
                "alias discover-and-record worker failed",
            )
        }
    };
    finish_alias_discover_record_response(&state.prom, response)
}

pub(crate) async fn alias_discover_record_method_not_allowed<S: RequestCtx>(
    State(state): State<Arc<S>>,
    method: Method,
) -> Response {
    let _duration = state
        .prom()
        .http_request_duration
        .with_label_values(&[ALIAS_DISCOVER_RECORD_ENDPOINT])
        .start_timer();
    let mut response = alias_discover_record_rejection(
        state.prom(),
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        format!("{method} is not supported by /_vocab/aliases/discover_and_record"),
    );
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("POST"));
    response
}

fn alias_discover_record_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    finish_alias_discover_record_response(
        prom,
        alias_discover_record_error_response(status, error_type, reason),
    )
}

pub(crate) fn alias_discover_record_error_response(
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    ApiError::response(status, error_type, reason).into_response()
}

pub(crate) fn finish_alias_discover_record_response(
    prom: &PrometheusMetrics,
    mut response: Response,
) -> Response {
    prom.http_requests_total
        .with_label_values(&[ALIAS_DISCOVER_RECORD_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
