//! Strict native `PUT /_vocab` document replacement.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::Bytes,
    extract::{FromRequest, Request},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use prometheus::HistogramTimer;
use serde::Serialize;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{error, info};

use reverse_rusty::segment::Engine;
use reverse_rusty::vocab::Vocab;

use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::{AppState, RequestCtx};

/// A full vocabulary can be materially larger than the bodyless read request,
/// but it should not inherit the server's 100 MiB bulk-ingest ceiling.
pub(crate) const VOCAB_WRITE_BODY_LIMIT: usize = 16 * 1024 * 1024;
pub(crate) const VOCAB_WRITE_BODY_TIMEOUT: Duration = Duration::from_secs(5);
const VOCAB_WRITE_ENDPOINT: &str = "vocab_put";

/// Strict request transport shared by standalone and coordinator replacement.
///
/// The timer begins before method, query, content-type, body, and JSON checks.
/// The decoded document is held only until bounded administrative admission is
/// acquired; no engine or cluster lock is held while the request body arrives.
pub(crate) struct VocabWriteTransport {
    duration: HistogramTimer,
    started: Instant,
    vocab: Vocab,
}

impl VocabWriteTransport {
    pub(crate) fn into_parts(self) -> (HistogramTimer, Instant, Vocab) {
        (self.duration, self.started, self.vocab)
    }
}

impl<S> FromRequest<Arc<S>> for VocabWriteTransport
where
    S: RequestCtx,
{
    type Rejection = Response;

    async fn from_request(request: Request, state: &Arc<S>) -> Result<Self, Self::Rejection> {
        let started = Instant::now();
        let duration = state
            .prom()
            .http_request_duration
            .with_label_values(&[VOCAB_WRITE_ENDPOINT])
            .start_timer();
        if request.method() != Method::PUT {
            return Err(vocab_write_rejection(
                state.prom(),
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "PUT is the vocabulary replacement method supported by /_vocab",
            ));
        }
        if request.uri().query().is_some_and(|query| !query.is_empty()) {
            return Err(vocab_write_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "validation_error",
                "PUT /_vocab does not accept query parameters",
            ));
        }
        if !is_json_content_type(request.headers()) {
            return Err(vocab_write_rejection(
                state.prom(),
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                "PUT /_vocab requires Content-Type: application/json",
            ));
        }

        let body = tokio::time::timeout(
            VOCAB_WRITE_BODY_TIMEOUT,
            Bytes::from_request(request, state),
        )
        .await
        .map_err(|_| {
            vocab_write_rejection(
                state.prom(),
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "vocabulary write body did not complete within 5s",
            )
        })?
        .map_err(|error| {
            let status = error.status();
            let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload_too_large"
            } else {
                "validation_error"
            };
            vocab_write_rejection(
                state.prom(),
                status,
                error_type,
                format!("invalid vocabulary write body: {error}"),
            )
        })?;
        let vocab = serde_json::from_slice(&body).map_err(|error| {
            vocab_write_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "validation_error",
                format!("invalid vocabulary JSON body: {error}"),
            )
        })?;

        Ok(Self {
            duration,
            started,
            vocab,
        })
    }
}

fn is_json_content_type(headers: &HeaderMap) -> bool {
    let Some(value) = headers.get(header::CONTENT_TYPE) else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };
    let media_type = value
        .split_once(';')
        .map_or(value, |(media_type, _)| media_type)
        .trim()
        .to_ascii_lowercase();
    media_type == "application/json"
        || media_type
            .strip_prefix("application/")
            .is_some_and(|subtype| subtype.ends_with("+json"))
}

#[derive(Serialize)]
pub(crate) struct PutVocabResponse {
    took: u64,
    took_ms: f64,
    acknowledged: bool,
    /// Stored queries rebuilt under the new normalizer.
    recompiled: usize,
}

impl PutVocabResponse {
    pub(crate) fn new(took_ms: f64, recompiled: usize) -> Self {
        Self {
            took: took_ms.floor() as u64,
            took_ms,
            acknowledged: true,
            recompiled,
        }
    }
}

pub(crate) fn vocab_write_success(started: Instant, recompiled: usize) -> Response {
    let took_ms = started.elapsed().as_secs_f64() * 1_000.0;
    Json(PutVocabResponse::new(took_ms, recompiled)).into_response()
}

pub(crate) async fn acquire_vocab_write_permit(
    permits: &Arc<Semaphore>,
    prom: &PrometheusMetrics,
) -> Result<OwnedSemaphorePermit, Response> {
    Arc::clone(permits).acquire_owned().await.map_err(|_| {
        vocab_write_rejection(
            prom,
            StatusCode::SERVICE_UNAVAILABLE,
            "vocab_unavailable",
            "vocabulary write admission is closed",
        )
    })
}

enum StandaloneApply {
    Applied { recompiled: usize },
    Invalid(String),
    PersistenceUnavailable(String),
    NotDurable { recompiled: usize },
    Incomplete { expected: usize, recompiled: usize },
}

fn apply_standalone_vocab(engine: &mut Engine, vocab: Vocab) -> StandaloneApply {
    let durable = engine.config().data_dir.is_some();
    if durable && !engine.persistence_healthy() {
        return StandaloneApply::PersistenceUnavailable(
            "cannot change vocabulary while persistence is unhealthy; repair or restart from the \
             last committed state first"
                .to_string(),
        );
    }

    // Recompilation deliberately canonicalizes supported additive/legacy
    // histories to one row per logical id. `num_live_queries()` counts their
    // physical rows, while the source store has one current document per id.
    let expected = engine.live_sources().len();
    if let Err(source) = engine.set_vocab(vocab) {
        return StandaloneApply::Invalid(source.to_string());
    }
    let recompiled = engine.recompile_stale_segments();
    if recompiled != expected || engine.has_stale_segments() {
        return StandaloneApply::Incomplete {
            expected,
            recompiled,
        };
    }
    if durable && !engine.persistence_healthy() {
        return StandaloneApply::NotDurable { recompiled };
    }
    StandaloneApply::Applied { recompiled }
}

/// Replace the vocabulary and rebuild every stored query on a bounded blocking
/// worker. Publication belongs to the worker so disconnecting the HTTP request
/// cannot leave a completed coherent replacement invisible.
pub(crate) async fn put_vocab(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    transport: VocabWriteTransport,
) -> Response {
    let (_duration, started, vocab) = transport.into_parts();
    let permit = match acquire_vocab_write_permit(&state.stats_permits, &state.prom).await {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    let work_state = Arc::clone(&state);
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let outcome = {
            let mut engine = work_state.engine.lock();
            apply_standalone_vocab(&mut engine, vocab)
        };
        if matches!(
            outcome,
            StandaloneApply::Applied { .. } | StandaloneApply::NotDurable { .. }
        ) {
            work_state.publish_snapshot();
        }
        outcome
    });

    let response = match worker.await {
        Ok(StandaloneApply::Applied { recompiled }) => {
            info!(recompiled, "vocabulary replaced");
            vocab_write_success(started, recompiled)
        }
        Ok(StandaloneApply::Invalid(reason)) => {
            vocab_write_error_response(StatusCode::BAD_REQUEST, "vocab_error", reason)
        }
        Ok(StandaloneApply::PersistenceUnavailable(reason)) => vocab_write_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "persistence_unavailable",
            reason,
        ),
        Ok(StandaloneApply::NotDurable { recompiled }) => {
            error!(
                recompiled,
                "vocabulary is live but its query rebuild was not durably committed"
            );
            vocab_write_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "persistence_unavailable",
                format!(
                    "vocabulary is live and {recompiled} queries were recompiled, but the rebuild \
                     was not durably committed; repair or restart from the last committed state"
                ),
            )
        }
        Ok(StandaloneApply::Incomplete {
            expected,
            recompiled,
        }) => {
            error!(
                expected,
                recompiled, "vocabulary replacement left stale or incomplete query state"
            );
            vocab_write_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "vocab_unavailable",
                "vocabulary query rebuild did not complete",
            )
        }
        Err(join_error) => {
            error!(error = %join_error, "vocabulary write worker failed");
            vocab_write_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "vocab_unavailable",
                "vocabulary write worker failed",
            )
        }
    };
    finish_vocab_write_response(&state.prom, response)
}

pub(crate) fn vocab_write_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    finish_vocab_write_response(prom, vocab_write_error_response(status, error_type, reason))
}

pub(crate) fn vocab_write_error_response(
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    ApiError::response(status, error_type, reason).into_response()
}

pub(crate) fn finish_vocab_write_response(
    prom: &PrometheusMetrics,
    mut response: Response,
) -> Response {
    prom.http_requests_total
        .with_label_values(&[VOCAB_WRITE_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
