//! Strict native `POST /_vocab/learn_and_apply` stored-corpus mutation.

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

use reverse_rusty::segment::Engine;
use reverse_rusty::vocab::CorpusLearnConfig;

use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::{AppState, RequestCtx};

use super::learn::validate_learn_controls;
use super::{build_corpus_config, default_min_count};

/// The operation is bodyless, so it must not inherit the bulk-ingest ceiling.
pub(crate) const VOCAB_LEARN_APPLY_BODY_LIMIT: usize = 64 * 1024;
pub(crate) const VOCAB_LEARN_APPLY_BODY_TIMEOUT: Duration = Duration::from_millis(250);
const VOCAB_LEARN_APPLY_ENDPOINT: &str = "vocab_learn_apply";

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct LearnApplyParams {
    /// Minimum distinct-query evidence for a learned relationship.
    #[serde(default = "default_min_count")]
    min_count: usize,
    /// Opt-in NPMI corpus phrase induction (ADR-053).
    #[serde(default)]
    corpus_phrases: bool,
    #[serde(default)]
    npmi_tau: Option<f64>,
    #[serde(default)]
    npmi_min_count: Option<usize>,
    #[serde(default)]
    npmi_iterations: Option<usize>,
    /// Learn any-of groups as widening equivalences instead of collapse synonyms.
    #[serde(default)]
    learn_equivalences: bool,
}

impl LearnApplyParams {
    fn into_config(self) -> Result<CorpusLearnConfig, String> {
        validate_learn_controls(
            self.min_count,
            self.corpus_phrases,
            self.npmi_tau,
            self.npmi_min_count,
            self.npmi_iterations,
        )?;
        Ok(build_corpus_config(
            self.min_count,
            self.corpus_phrases,
            self.npmi_tau,
            self.npmi_min_count,
            self.npmi_iterations,
            self.learn_equivalences,
        ))
    }
}

/// Method/query/body validation shared by standalone and coordinator modes.
///
/// The timer begins before transport validation. No engine or cluster lock is
/// held while the bounded, bodyless request is decoded.
pub(crate) struct VocabLearnApplyTransport {
    duration: HistogramTimer,
    started: Instant,
    config: CorpusLearnConfig,
}

impl VocabLearnApplyTransport {
    pub(crate) fn into_parts(self) -> (HistogramTimer, Instant, CorpusLearnConfig) {
        (self.duration, self.started, self.config)
    }
}

impl<S> FromRequest<Arc<S>> for VocabLearnApplyTransport
where
    S: RequestCtx,
{
    type Rejection = Response;

    async fn from_request(request: Request, state: &Arc<S>) -> Result<Self, Self::Rejection> {
        let started = Instant::now();
        let duration = state
            .prom()
            .http_request_duration
            .with_label_values(&[VOCAB_LEARN_APPLY_ENDPOINT])
            .start_timer();
        if request.method() != Method::POST {
            return Err(vocab_learn_apply_rejection(
                state.prom(),
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "POST is the stored-corpus vocabulary learning method supported by \
                 /_vocab/learn_and_apply",
            ));
        }

        let Query(params) =
            Query::<LearnApplyParams>::try_from_uri(request.uri()).map_err(|source| {
                vocab_learn_apply_rejection(
                    state.prom(),
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    format!("invalid vocabulary learn-and-apply query parameters: {source}"),
                )
            })?;
        let config = params.into_config().map_err(|reason| {
            vocab_learn_apply_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "validation_error",
                reason,
            )
        })?;

        let body = tokio::time::timeout(
            VOCAB_LEARN_APPLY_BODY_TIMEOUT,
            Bytes::from_request(request, state),
        )
        .await
        .map_err(|_| {
            vocab_learn_apply_rejection(
                state.prom(),
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "vocabulary learn-and-apply body did not complete within 250ms",
            )
        })?
        .map_err(|source| {
            let status = source.status();
            let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload_too_large"
            } else {
                "validation_error"
            };
            vocab_learn_apply_rejection(
                state.prom(),
                status,
                error_type,
                format!("invalid vocabulary learn-and-apply body: {source}"),
            )
        })?;
        if !body.is_empty() {
            return Err(vocab_learn_apply_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "validation_error",
                "POST /_vocab/learn_and_apply does not accept a request body",
            ));
        }

        Ok(Self {
            duration,
            started,
            config,
        })
    }
}

#[derive(Serialize)]
struct LearnApplyResponse {
    took: u64,
    took_ms: f64,
    acknowledged: bool,
    /// Stored queries rebuilt under the learned vocabulary.
    recompiled: usize,
}

pub(crate) fn vocab_learn_apply_success(started: Instant, recompiled: usize) -> Response {
    let took_ms = started.elapsed().as_secs_f64() * 1_000.0;
    Json(LearnApplyResponse {
        took: took_ms.floor() as u64,
        took_ms,
        acknowledged: true,
        recompiled,
    })
    .into_response()
}

pub(crate) async fn acquire_vocab_learn_apply_permit(
    permits: &Arc<Semaphore>,
    prom: &PrometheusMetrics,
) -> Result<OwnedSemaphorePermit, Response> {
    Arc::clone(permits).acquire_owned().await.map_err(|_| {
        vocab_learn_apply_rejection(
            prom,
            StatusCode::SERVICE_UNAVAILABLE,
            "vocab_unavailable",
            "vocabulary learn-and-apply admission is closed",
        )
    })
}

enum StandaloneLearnApply {
    Applied { recompiled: usize },
    Invalid(String),
    PersistenceUnavailable(String),
    NotDurable { recompiled: usize },
    Incomplete { expected: usize, recompiled: usize },
}

fn apply_standalone_learning(
    engine: &mut Engine,
    config: &CorpusLearnConfig,
) -> StandaloneLearnApply {
    let durable = engine.config().data_dir.is_some();
    if durable && !engine.persistence_healthy() {
        return StandaloneLearnApply::PersistenceUnavailable(
            "cannot learn and apply vocabulary while persistence is unhealthy; repair or restart \
             from the last committed state first"
                .to_string(),
        );
    }

    // The source store is the canonical one-row-per-logical-id corpus. A
    // successful rebuild must consume every one of those live documents and
    // leave no segment compiled under the prior vocabulary epoch.
    let expected = engine.live_sources().len();
    let recompiled = match engine.learn_and_apply_with(config) {
        Ok(recompiled) => recompiled,
        Err(source) => return StandaloneLearnApply::Invalid(source.to_string()),
    };
    if recompiled != expected || engine.has_stale_segments() {
        return StandaloneLearnApply::Incomplete {
            expected,
            recompiled,
        };
    }
    if durable && !engine.persistence_healthy() {
        return StandaloneLearnApply::NotDurable { recompiled };
    }
    StandaloneLearnApply::Applied { recompiled }
}

/// Learn from the standalone engine's live source corpus and apply on one
/// bounded blocking worker. The worker owns admission and snapshot publication,
/// so a disconnected request cannot release either responsibility early.
pub(crate) async fn learn_and_apply_vocab(
    State(state): State<Arc<AppState>>,
    transport: VocabLearnApplyTransport,
) -> Response {
    let (_duration, started, config) = transport.into_parts();
    let permit = match acquire_vocab_learn_apply_permit(&state.stats_permits, &state.prom).await {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    let work_state = Arc::clone(&state);
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let outcome = {
            let mut engine = work_state.engine.lock();
            apply_standalone_learning(&mut engine, &config)
        };
        if matches!(
            outcome,
            StandaloneLearnApply::Applied { .. } | StandaloneLearnApply::NotDurable { .. }
        ) {
            work_state.publish_snapshot();
        }
        outcome
    });

    let response = match worker.await {
        Ok(StandaloneLearnApply::Applied { recompiled }) => {
            info!(recompiled, "stored-corpus vocabulary learned and applied");
            vocab_learn_apply_success(started, recompiled)
        }
        Ok(StandaloneLearnApply::Invalid(reason)) => {
            vocab_learn_apply_error_response(StatusCode::BAD_REQUEST, "vocab_error", reason)
        }
        Ok(StandaloneLearnApply::PersistenceUnavailable(reason)) => {
            vocab_learn_apply_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "persistence_unavailable",
                reason,
            )
        }
        Ok(StandaloneLearnApply::NotDurable { recompiled }) => {
            error!(
                recompiled,
                "learned vocabulary is live but its query rebuild was not durably committed"
            );
            vocab_learn_apply_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "persistence_unavailable",
                format!(
                    "learned vocabulary is live and {recompiled} queries were recompiled, but the \
                     rebuild was not durably committed; repair or restart from the last committed \
                     state"
                ),
            )
        }
        Ok(StandaloneLearnApply::Incomplete {
            expected,
            recompiled,
        }) => {
            error!(
                expected,
                recompiled, "learn-and-apply left stale or incomplete query state"
            );
            vocab_learn_apply_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "vocab_unavailable",
                "vocabulary learn-and-apply query rebuild did not complete",
            )
        }
        Err(join_error) => {
            error!(error = %join_error, "vocabulary learn-and-apply worker failed");
            vocab_learn_apply_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "vocab_unavailable",
                "vocabulary learn-and-apply worker failed",
            )
        }
    };
    finish_vocab_learn_apply_response(&state.prom, response)
}

pub(crate) async fn vocab_learn_apply_method_not_allowed<S: RequestCtx>(
    State(state): State<Arc<S>>,
    method: Method,
) -> Response {
    let _duration = state
        .prom()
        .http_request_duration
        .with_label_values(&[VOCAB_LEARN_APPLY_ENDPOINT])
        .start_timer();
    let mut response = vocab_learn_apply_rejection(
        state.prom(),
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        format!("{method} is not supported by /_vocab/learn_and_apply"),
    );
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("POST"));
    response
}

fn vocab_learn_apply_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    finish_vocab_learn_apply_response(
        prom,
        vocab_learn_apply_error_response(status, error_type, reason),
    )
}

pub(crate) fn vocab_learn_apply_error_response(
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    ApiError::response(status, error_type, reason).into_response()
}

pub(crate) fn finish_vocab_learn_apply_response(
    prom: &PrometheusMetrics,
    mut response: Response,
) -> Response {
    prom.http_requests_total
        .with_label_values(&[VOCAB_LEARN_APPLY_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
