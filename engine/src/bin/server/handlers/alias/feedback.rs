//! Strict native `POST /_vocab/aliases/validate_and_apply` evidence mutation.

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
use tracing::{error, info};

use reverse_rusty::segment::{AliasFeedbackApplyReport, Engine};

use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::{AppState, RequestCtx};

pub(crate) const ALIAS_FEEDBACK_APPLY_BODY_LIMIT: usize = 64 * 1024;
pub(crate) const ALIAS_FEEDBACK_APPLY_BODY_TIMEOUT: Duration = Duration::from_millis(250);
const ALIAS_FEEDBACK_APPLY_RESPONSE_LIMIT: usize = 64 * 1024;
const ALIAS_FEEDBACK_APPLY_ENDPOINT: &str = "vocab_aliases_validate_and_apply";

fn default_min_overlap() -> f64 {
    0.5
}

fn default_min_titles() -> u64 {
    50
}

fn default_min_queries() -> u64 {
    20
}

#[derive(Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
struct AliasFeedbackApplyParams {
    #[serde(default = "default_min_overlap")]
    min_overlap: f64,
    #[serde(default = "default_min_titles")]
    min_titles: u64,
    #[serde(default = "default_min_queries")]
    min_queries: u64,
    #[serde(default)]
    activate: bool,
}

impl Default for AliasFeedbackApplyParams {
    fn default() -> Self {
        Self {
            min_overlap: default_min_overlap(),
            min_titles: default_min_titles(),
            min_queries: default_min_queries(),
            activate: false,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct AliasFeedbackApplyControls {
    min_overlap: f64,
    min_titles: u64,
    min_queries: u64,
    activate: bool,
}

impl AliasFeedbackApplyParams {
    fn validate(self) -> Result<AliasFeedbackApplyControls, String> {
        if !self.min_overlap.is_finite() || !(0.0..=1.0).contains(&self.min_overlap) {
            return Err("min_overlap must be finite and between 0 and 1".to_string());
        }
        if self.min_titles == 0 {
            return Err("min_titles must be at least 1".to_string());
        }
        if self.min_queries == 0 {
            return Err("min_queries must be at least 1".to_string());
        }
        Ok(AliasFeedbackApplyControls {
            min_overlap: self.min_overlap,
            min_titles: self.min_titles,
            min_queries: self.min_queries,
            activate: self.activate,
        })
    }
}

/// Method, query, and body contract shared by standalone and coordinator mode.
pub(crate) struct AliasFeedbackApplyTransport {
    duration: HistogramTimer,
    started: Instant,
    controls: AliasFeedbackApplyControls,
}

impl AliasFeedbackApplyTransport {
    pub(crate) fn into_parts(self) -> (HistogramTimer, Instant, AliasFeedbackApplyControls) {
        (self.duration, self.started, self.controls)
    }
}

impl<S> FromRequest<Arc<S>> for AliasFeedbackApplyTransport
where
    S: RequestCtx,
{
    type Rejection = Response;

    async fn from_request(request: Request, state: &Arc<S>) -> Result<Self, Self::Rejection> {
        let started = Instant::now();
        let duration = state
            .prom()
            .http_request_duration
            .with_label_values(&[ALIAS_FEEDBACK_APPLY_ENDPOINT])
            .start_timer();
        if request.method() != Method::POST {
            return Err(alias_feedback_apply_rejection(
                state.prom(),
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "POST is the validation method supported by \
                 /_vocab/aliases/validate_and_apply",
            ));
        }

        let Query(params) = Query::<AliasFeedbackApplyParams>::try_from_uri(request.uri())
            .map_err(|source| {
                alias_feedback_apply_rejection(
                    state.prom(),
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    format!("invalid alias-feedback validation parameters: {source}"),
                )
            })?;
        let controls = params.validate().map_err(|reason| {
            alias_feedback_apply_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "validation_error",
                reason,
            )
        })?;

        let body = tokio::time::timeout(
            ALIAS_FEEDBACK_APPLY_BODY_TIMEOUT,
            Bytes::from_request(request, state),
        )
        .await
        .map_err(|_| {
            alias_feedback_apply_rejection(
                state.prom(),
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "alias-feedback validation body did not complete within 250ms",
            )
        })?
        .map_err(|source| {
            let status = source.status();
            let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload_too_large"
            } else {
                "validation_error"
            };
            alias_feedback_apply_rejection(
                state.prom(),
                status,
                error_type,
                format!("invalid alias-feedback validation body: {source}"),
            )
        })?;
        if !body.is_empty() {
            return Err(alias_feedback_apply_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "validation_error",
                "POST /_vocab/aliases/validate_and_apply does not accept a request body",
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
struct AliasFeedbackApplyResponse {
    took: u64,
    took_ms: f64,
    acknowledged: bool,
    result: &'static str,
    /// Standalone runtime vocabulary changes are not written to the startup vocab file.
    persisted: bool,
    min_overlap: f64,
    min_titles: u64,
    min_queries: u64,
    activate: bool,
    validated: usize,
    stamped: usize,
    activated: usize,
    recompiled: usize,
    summary: reverse_rusty::vocab::AliasSummary,
}

enum StandaloneAliasFeedbackApply {
    Applied {
        validated: usize,
        report: AliasFeedbackApplyReport,
    },
    Noop {
        validated: usize,
        report: AliasFeedbackApplyReport,
    },
    Invalid(String),
    PersistenceUnavailable(String),
    NotDurable {
        validated: usize,
        report: AliasFeedbackApplyReport,
    },
    Incomplete {
        expected: usize,
        recompiled: usize,
    },
}

fn apply_standalone_alias_feedback(
    engine: &mut Engine,
    validated: &[(Vec<String>, reverse_rusty::vocab::FeedbackEvidence)],
    activate: bool,
) -> StandaloneAliasFeedbackApply {
    let durable = engine.config().data_dir.is_some();
    if activate && durable && !engine.persistence_healthy() {
        return StandaloneAliasFeedbackApply::PersistenceUnavailable(
            "cannot activate validated aliases while persistence is unhealthy; repair or restart \
             from the last committed state first"
                .to_string(),
        );
    }

    let expected = engine.live_sources().len();
    let report = match engine.apply_alias_feedback(validated, activate) {
        Ok(report) => report,
        Err(source) => return StandaloneAliasFeedbackApply::Invalid(source.to_string()),
    };
    if report.activated > 0 && (report.recompiled != expected || engine.has_stale_segments()) {
        return StandaloneAliasFeedbackApply::Incomplete {
            expected,
            recompiled: report.recompiled,
        };
    }
    if report.activated > 0 && durable && !engine.persistence_healthy() {
        return StandaloneAliasFeedbackApply::NotDurable {
            validated: validated.len(),
            report,
        };
    }
    if report.stamped > 0 || report.activated > 0 {
        StandaloneAliasFeedbackApply::Applied {
            validated: validated.len(),
            report,
        }
    } else {
        StandaloneAliasFeedbackApply::Noop {
            validated: validated.len(),
            report,
        }
    }
}

/// Snapshot operator-bounded evidence under its mutex, then perform source lookup,
/// registry mutation, any required O(corpus) recompile, and publication on one
/// admitted blocking worker.
pub(crate) async fn validate_and_apply_feedback(
    State(state): State<Arc<AppState>>,
    transport: AliasFeedbackApplyTransport,
) -> Response {
    let (_duration, started, controls) = transport.into_parts();
    let Ok(permit) = Arc::clone(&state.stats_permits).acquire_owned().await else {
        return alias_feedback_apply_rejection(
            &state.prom,
            StatusCode::SERVICE_UNAVAILABLE,
            "aliases_unavailable",
            "alias-feedback validation admission is closed",
        );
    };

    let work_state = Arc::clone(&state);
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let (snapshot, feedback) = {
            let feedback = work_state.feedback.lock();
            let snapshot = work_state.snapshot.load_full();
            let (_, evidence) = feedback.snapshot_page(0, usize::MAX);
            (snapshot, evidence)
        };
        let validated: Vec<(Vec<String>, reverse_rusty::vocab::FeedbackEvidence)> = feedback
            .report(
                controls.min_overlap,
                controls.min_titles,
                controls.min_queries,
                |id| snapshot.get_query_source(id),
            )
            .into_iter()
            .filter(|row| row.validated)
            .map(|row| {
                (
                    row.forms,
                    reverse_rusty::vocab::FeedbackEvidence {
                        overlap: row.overlap,
                        titles_a: row.titles_a,
                        titles_b: row.titles_b,
                        queries_sampled: row.sampled_a.min(row.sampled_b),
                    },
                )
            })
            .collect();

        let outcome = {
            let mut engine = work_state.engine.lock();
            apply_standalone_alias_feedback(&mut engine, &validated, controls.activate)
        };
        if matches!(
            outcome,
            StandaloneAliasFeedbackApply::Applied { .. }
                | StandaloneAliasFeedbackApply::NotDurable { .. }
        ) {
            work_state.publish_snapshot();
        }
        outcome
    });

    let response = match worker.await {
        Ok(StandaloneAliasFeedbackApply::Applied { validated, report }) => {
            info!(
                validated,
                stamped = report.stamped,
                activated = report.activated,
                recompiled = report.recompiled,
                "alias feedback validated and applied"
            );
            alias_feedback_apply_success(started, controls, validated, report, "updated")
        }
        Ok(StandaloneAliasFeedbackApply::Noop { validated, report }) => {
            alias_feedback_apply_success(started, controls, validated, report, "noop")
        }
        Ok(StandaloneAliasFeedbackApply::Invalid(reason)) => {
            alias_feedback_apply_error_response(StatusCode::BAD_REQUEST, "vocab_error", reason)
        }
        Ok(StandaloneAliasFeedbackApply::PersistenceUnavailable(reason)) => {
            alias_feedback_apply_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "persistence_unavailable",
                reason,
            )
        }
        Ok(StandaloneAliasFeedbackApply::NotDurable { validated, report }) => {
            error!(
                validated,
                activated = report.activated,
                recompiled = report.recompiled,
                "validated aliases are live but their query rebuild was not durably committed"
            );
            alias_feedback_apply_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "persistence_unavailable",
                format!(
                    "validated aliases are live and {} queries were recompiled, but the rebuild \
                     was not durably committed; repair or restart from the last committed state",
                    report.recompiled
                ),
            )
        }
        Ok(StandaloneAliasFeedbackApply::Incomplete {
            expected,
            recompiled,
        }) => {
            error!(
                expected,
                recompiled, "alias-feedback activation left stale or incomplete query state"
            );
            alias_feedback_apply_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "aliases_unavailable",
                "alias-feedback activation query rebuild did not complete",
            )
        }
        Err(join_error) => {
            error!(error = %join_error, "alias-feedback validation worker failed");
            alias_feedback_apply_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "aliases_unavailable",
                "alias-feedback validation worker failed",
            )
        }
    };
    finish_alias_feedback_apply_response(&state.prom, response)
}

fn alias_feedback_apply_success(
    started: Instant,
    controls: AliasFeedbackApplyControls,
    validated: usize,
    report: AliasFeedbackApplyReport,
    result: &'static str,
) -> Response {
    let took_ms = started.elapsed().as_secs_f64() * 1_000.0;
    let response = AliasFeedbackApplyResponse {
        took: took_ms.floor() as u64,
        took_ms,
        acknowledged: true,
        result,
        persisted: false,
        min_overlap: controls.min_overlap,
        min_titles: controls.min_titles,
        min_queries: controls.min_queries,
        activate: controls.activate,
        validated,
        stamped: report.stamped,
        activated: report.activated,
        recompiled: report.recompiled,
        summary: report.summary,
    };
    match serde_json::to_vec(&response) {
        Ok(encoded) if encoded.len() <= ALIAS_FEEDBACK_APPLY_RESPONSE_LIMIT => (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            encoded,
        )
            .into_response(),
        Ok(_) => alias_feedback_apply_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "aliases_unavailable",
            "alias-feedback validation response exceeded its fixed serialization limit",
        ),
        Err(source) => {
            error!(error = %source, "failed to serialize alias-feedback validation response");
            alias_feedback_apply_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "aliases_unavailable",
                "alias-feedback validation response serialization failed",
            )
        }
    }
}

pub(crate) async fn alias_feedback_apply_method_not_allowed<S: RequestCtx>(
    State(state): State<Arc<S>>,
    method: Method,
) -> Response {
    let _duration = state
        .prom()
        .http_request_duration
        .with_label_values(&[ALIAS_FEEDBACK_APPLY_ENDPOINT])
        .start_timer();
    let mut response = alias_feedback_apply_rejection(
        state.prom(),
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        format!("{method} is not supported by /_vocab/aliases/validate_and_apply"),
    );
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("POST"));
    response
}

fn alias_feedback_apply_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    finish_alias_feedback_apply_response(
        prom,
        alias_feedback_apply_error_response(status, error_type, reason),
    )
}

pub(crate) fn alias_feedback_apply_error_response(
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    ApiError::response(status, error_type, reason).into_response()
}

pub(crate) fn finish_alias_feedback_apply_response(
    prom: &PrometheusMetrics,
    mut response: Response,
) -> Response {
    prom.http_requests_total
        .with_label_values(&[ALIAS_FEEDBACK_APPLY_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
