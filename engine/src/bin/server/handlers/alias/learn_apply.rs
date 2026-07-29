//! Strict native `POST /_vocab/aliases/learn_and_apply` mutation.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::Bytes,
    extract::{FromRequest, Query, Request, State},
    http::{header, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use prometheus::HistogramTimer;
use serde::{Deserialize, Serialize};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{error, info};

use reverse_rusty::segment::{AliasApplyReport, Engine};

use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::{AppState, RequestCtx};

/// The operation is bodyless, so it must not inherit the bulk-ingest ceiling.
pub(crate) const ALIAS_LEARN_APPLY_BODY_LIMIT: usize = 64 * 1024;
pub(crate) const ALIAS_LEARN_APPLY_BODY_TIMEOUT: Duration = Duration::from_millis(250);
const ALIAS_LEARN_APPLY_ENDPOINT: &str = "vocab_aliases_learn_apply";

fn default_min_count() -> usize {
    2
}

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct AliasLearnApplyParams {
    /// Minimum distinct-query evidence for a learned alias group.
    #[serde(default = "default_min_count")]
    min_count: usize,
}

impl AliasLearnApplyParams {
    fn validate(self) -> Result<usize, String> {
        if self.min_count == 0 {
            return Err("`min_count` must be at least 1".to_string());
        }
        Ok(self.min_count)
    }
}

/// Method/query/body validation shared by standalone and coordinator modes.
pub(crate) struct AliasLearnApplyTransport {
    duration: HistogramTimer,
    started: Instant,
    min_count: usize,
}

impl AliasLearnApplyTransport {
    pub(crate) fn into_parts(self) -> (HistogramTimer, Instant, usize) {
        (self.duration, self.started, self.min_count)
    }
}

impl<S> FromRequest<Arc<S>> for AliasLearnApplyTransport
where
    S: RequestCtx,
{
    type Rejection = Response;

    async fn from_request(request: Request, state: &Arc<S>) -> Result<Self, Self::Rejection> {
        let started = Instant::now();
        let duration = state
            .prom()
            .http_request_duration
            .with_label_values(&[ALIAS_LEARN_APPLY_ENDPOINT])
            .start_timer();
        if request.method() != Method::POST {
            return Err(alias_learn_apply_rejection(
                state.prom(),
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "POST is the stored-corpus alias learning method supported by \
                 /_vocab/aliases/learn_and_apply",
            ));
        }

        let Query(params) =
            Query::<AliasLearnApplyParams>::try_from_uri(request.uri()).map_err(|source| {
                alias_learn_apply_rejection(
                    state.prom(),
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    format!("invalid alias learn-and-apply query parameters: {source}"),
                )
            })?;
        let min_count = params.validate().map_err(|reason| {
            alias_learn_apply_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "validation_error",
                reason,
            )
        })?;

        let body = tokio::time::timeout(
            ALIAS_LEARN_APPLY_BODY_TIMEOUT,
            Bytes::from_request(request, state),
        )
        .await
        .map_err(|_| {
            alias_learn_apply_rejection(
                state.prom(),
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "alias learn-and-apply body did not complete within 250ms",
            )
        })?
        .map_err(|source| {
            let status = source.status();
            let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload_too_large"
            } else {
                "validation_error"
            };
            alias_learn_apply_rejection(
                state.prom(),
                status,
                error_type,
                format!("invalid alias learn-and-apply body: {source}"),
            )
        })?;
        if !body.is_empty() {
            return Err(alias_learn_apply_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "validation_error",
                "POST /_vocab/aliases/learn_and_apply does not accept a request body",
            ));
        }

        Ok(Self {
            duration,
            started,
            min_count,
        })
    }
}

#[derive(Serialize)]
struct AliasLearnApplyResponse {
    took: u64,
    took_ms: f64,
    acknowledged: bool,
    activated: usize,
    recompiled: usize,
    summary: reverse_rusty::vocab::AliasSummary,
}

pub(crate) fn alias_learn_apply_success(started: Instant, report: AliasApplyReport) -> Response {
    let took_ms = started.elapsed().as_secs_f64() * 1_000.0;
    Json(AliasLearnApplyResponse {
        took: took_ms.floor() as u64,
        took_ms,
        acknowledged: true,
        activated: report.activated,
        recompiled: report.recompiled,
        summary: report.summary,
    })
    .into_response()
}

pub(crate) async fn acquire_alias_learn_apply_permit(
    permits: &Arc<Semaphore>,
    prom: &PrometheusMetrics,
) -> Result<OwnedSemaphorePermit, Response> {
    Arc::clone(permits).acquire_owned().await.map_err(|_| {
        alias_learn_apply_rejection(
            prom,
            StatusCode::SERVICE_UNAVAILABLE,
            "aliases_unavailable",
            "alias learn-and-apply admission is closed",
        )
    })
}

enum StandaloneAliasLearnApply {
    Applied(AliasApplyReport),
    Invalid(String),
    PersistenceUnavailable(String),
    NotDurable(AliasApplyReport),
    Incomplete { expected: usize, recompiled: usize },
}

fn apply_standalone_alias_learning(
    engine: &mut Engine,
    min_count: usize,
) -> StandaloneAliasLearnApply {
    let durable = engine.config().data_dir.is_some();
    if durable && !engine.persistence_healthy() {
        return StandaloneAliasLearnApply::PersistenceUnavailable(
            "cannot learn and apply aliases while persistence is unhealthy; repair or restart \
             from the last committed state first"
                .to_string(),
        );
    }

    let expected = engine.live_sources().len();
    let report = match engine.learn_aliases_and_apply(min_count) {
        Ok(report) => report,
        Err(source) => return StandaloneAliasLearnApply::Invalid(source.to_string()),
    };
    if report.recompiled != expected || engine.has_stale_segments() {
        return StandaloneAliasLearnApply::Incomplete {
            expected,
            recompiled: report.recompiled,
        };
    }
    if durable && !engine.persistence_healthy() {
        return StandaloneAliasLearnApply::NotDurable(report);
    }
    StandaloneAliasLearnApply::Applied(report)
}

/// Learn and apply aliases on one bounded blocking worker. The worker owns
/// admission and snapshot publication, so disconnecting the request cannot
/// leave a completed coherent feature-model change unpublished.
pub(crate) async fn learn_and_apply_aliases(
    State(state): State<Arc<AppState>>,
    transport: AliasLearnApplyTransport,
) -> Response {
    let (_duration, started, min_count) = transport.into_parts();
    let permit = match acquire_alias_learn_apply_permit(&state.stats_permits, &state.prom).await {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    let work_state = Arc::clone(&state);
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let outcome = {
            let mut engine = work_state.engine.lock();
            apply_standalone_alias_learning(&mut engine, min_count)
        };
        if matches!(
            outcome,
            StandaloneAliasLearnApply::Applied(_) | StandaloneAliasLearnApply::NotDurable(_)
        ) {
            work_state.publish_snapshot();
        }
        outcome
    });

    let response = match worker.await {
        Ok(StandaloneAliasLearnApply::Applied(report)) => {
            info!(
                activated = report.activated,
                recompiled = report.recompiled,
                "stored-corpus aliases learned and applied"
            );
            alias_learn_apply_success(started, report)
        }
        Ok(StandaloneAliasLearnApply::Invalid(reason)) => {
            alias_learn_apply_error_response(StatusCode::BAD_REQUEST, "vocab_error", reason)
        }
        Ok(StandaloneAliasLearnApply::PersistenceUnavailable(reason)) => {
            alias_learn_apply_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "persistence_unavailable",
                reason,
            )
        }
        Ok(StandaloneAliasLearnApply::NotDurable(report)) => {
            error!(
                activated = report.activated,
                recompiled = report.recompiled,
                "learned aliases are live but their query rebuild was not durably committed"
            );
            alias_learn_apply_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "persistence_unavailable",
                format!(
                    "learned aliases are live and {} queries were recompiled, but the rebuild was \
                     not durably committed; repair or restart from the last committed state",
                    report.recompiled
                ),
            )
        }
        Ok(StandaloneAliasLearnApply::Incomplete {
            expected,
            recompiled,
        }) => {
            error!(
                expected,
                recompiled, "alias learn-and-apply left stale or incomplete query state"
            );
            alias_learn_apply_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "aliases_unavailable",
                "alias learn-and-apply query rebuild did not complete",
            )
        }
        Err(join_error) => {
            error!(error = %join_error, "alias learn-and-apply worker failed");
            alias_learn_apply_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "aliases_unavailable",
                "alias learn-and-apply worker failed",
            )
        }
    };
    finish_alias_learn_apply_response(&state.prom, response)
}

pub(crate) async fn alias_learn_apply_method_not_allowed<S: RequestCtx>(
    State(state): State<Arc<S>>,
    method: Method,
) -> Response {
    let _duration = state
        .prom()
        .http_request_duration
        .with_label_values(&[ALIAS_LEARN_APPLY_ENDPOINT])
        .start_timer();
    let mut response = alias_learn_apply_rejection(
        state.prom(),
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        format!("{method} is not supported by /_vocab/aliases/learn_and_apply"),
    );
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("POST"));
    response
}

fn alias_learn_apply_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    finish_alias_learn_apply_response(
        prom,
        alias_learn_apply_error_response(status, error_type, reason),
    )
}

pub(crate) fn alias_learn_apply_error_response(
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    ApiError::response(status, error_type, reason).into_response()
}

pub(crate) fn finish_alias_learn_apply_response(
    prom: &PrometheusMetrics,
    mut response: Response,
) -> Response {
    prom.http_requests_total
        .with_label_values(&[ALIAS_LEARN_APPLY_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
