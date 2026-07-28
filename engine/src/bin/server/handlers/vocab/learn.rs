//! Strict native `POST /_vocab/learn` vocabulary dry run.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Bytes,
    extract::{FromRequest, Request, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
};
use prometheus::HistogramTimer;
use serde::Deserialize;
use tokio::sync::Semaphore;
use tracing::error;

use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::{AppState, RequestCtx};

use super::{build_corpus_config, default_min_count};

/// Corpus learning can return a complete vocabulary document, but it should not
/// inherit the server's 100 MiB bulk-ingest ceiling.
pub(crate) const VOCAB_LEARN_BODY_LIMIT: usize = 16 * 1024 * 1024;
pub(crate) const VOCAB_LEARN_BODY_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const VOCAB_LEARN_MAX_QUERIES: usize = 100_000;
pub(crate) const VOCAB_LEARN_MAX_NPMI_ITERATIONS: usize = 8;
pub(crate) const VOCAB_LEARN_MAX_RELATIONSHIP_OBSERVATIONS: usize = 100_000;
pub(crate) const VOCAB_LEARN_MAX_NPMI_TOKENS: usize = 100_000;
pub(crate) const VOCAB_LEARN_MAX_RESULT_ENTRIES: usize = 100_000;
const VOCAB_LEARN_ENDPOINT: &str = "vocab_learn";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LearnRequest {
    queries: Vec<(u64, String)>,
    #[serde(default = "default_min_count")]
    min_count: usize,
    /// Opt-in NPMI corpus phrase induction (ADR-053); off by default.
    #[serde(default)]
    corpus_phrases: bool,
    #[serde(default)]
    npmi_tau: Option<f64>,
    #[serde(default)]
    npmi_min_count: Option<usize>,
    #[serde(default)]
    npmi_iterations: Option<usize>,
    /// Opt-in: learn any-of groups as equivalences applied via expansion (ADR-054).
    #[serde(default)]
    learn_equivalences: bool,
}

impl LearnRequest {
    fn validate(&self) -> Result<(), String> {
        if self.queries.len() > VOCAB_LEARN_MAX_QUERIES {
            return Err(format!(
                "queries has {} entries; maximum is {VOCAB_LEARN_MAX_QUERIES}",
                self.queries.len()
            ));
        }
        validate_learn_controls(
            self.min_count,
            self.corpus_phrases,
            self.npmi_tau,
            self.npmi_min_count,
            self.npmi_iterations,
        )?;

        let mut ids = HashSet::with_capacity(self.queries.len());
        let mut relationship_observations = 0usize;
        let mut npmi_tokens = 0usize;
        for (position, (id, query)) in self.queries.iter().enumerate() {
            if !ids.insert(*id) {
                return Err(format!(
                    "queries[{position}] repeats query id {id}; ids must be unique"
                ));
            }
            let ast = reverse_rusty::dsl::parse(query).map_err(|source| {
                format!("queries[{position}] with id {id} is invalid: {source}")
            })?;
            for clause in ast.clauses {
                if clause.negated {
                    continue;
                }
                let reverse_rusty::dsl::Atom::AnyOf(members) = clause.atom else {
                    continue;
                };
                let members = members.len();
                let observations = if self.learn_equivalences {
                    members.saturating_mul(members.saturating_sub(1)) / 2
                } else {
                    members.saturating_sub(1)
                };
                relationship_observations = relationship_observations.saturating_add(observations);
                if relationship_observations > VOCAB_LEARN_MAX_RELATIONSHIP_OBSERVATIONS {
                    return Err(format!(
                        "queries expand to more than \
                         {VOCAB_LEARN_MAX_RELATIONSHIP_OBSERVATIONS} potential relationship \
                         observations"
                    ));
                }
            }
            if self.corpus_phrases {
                npmi_tokens =
                    npmi_tokens.saturating_add(reverse_rusty::corpus::tokenize(query).len());
                if npmi_tokens > VOCAB_LEARN_MAX_NPMI_TOKENS {
                    return Err(format!(
                        "queries contain more than {VOCAB_LEARN_MAX_NPMI_TOKENS} corpus tokens"
                    ));
                }
            }
        }
        Ok(())
    }

    fn into_work(self) -> (Vec<(u64, String)>, reverse_rusty::vocab::CorpusLearnConfig) {
        let config = build_corpus_config(
            self.min_count,
            self.corpus_phrases,
            self.npmi_tau,
            self.npmi_min_count,
            self.npmi_iterations,
            self.learn_equivalences,
        );
        (self.queries, config)
    }
}

pub(crate) fn validate_learn_controls(
    min_count: usize,
    corpus_phrases: bool,
    npmi_tau: Option<f64>,
    npmi_min_count: Option<usize>,
    npmi_iterations: Option<usize>,
) -> Result<(), String> {
    if min_count == 0 {
        return Err("min_count must be at least 1".to_string());
    }
    if !corpus_phrases
        && (npmi_tau.is_some() || npmi_min_count.is_some() || npmi_iterations.is_some())
    {
        return Err(
            "npmi_tau, npmi_min_count, and npmi_iterations require corpus_phrases=true".to_string(),
        );
    }
    if let Some(tau) = npmi_tau {
        if !tau.is_finite() || !(-1.0..=1.0).contains(&tau) {
            return Err("npmi_tau must be finite and between -1 and 1".to_string());
        }
    }
    if npmi_min_count == Some(0) {
        return Err("npmi_min_count must be at least 1".to_string());
    }
    if let Some(iterations) = npmi_iterations {
        if !(1..=VOCAB_LEARN_MAX_NPMI_ITERATIONS).contains(&iterations) {
            return Err(format!(
                "npmi_iterations must be between 1 and {VOCAB_LEARN_MAX_NPMI_ITERATIONS}"
            ));
        }
    }
    Ok(())
}

/// Strict request transport shared by standalone and coordinator dry runs.
pub(crate) struct VocabLearnTransport {
    duration: HistogramTimer,
    body: Bytes,
}

impl VocabLearnTransport {
    fn into_parts(self) -> (HistogramTimer, Bytes) {
        (self.duration, self.body)
    }
}

impl<S> FromRequest<Arc<S>> for VocabLearnTransport
where
    S: RequestCtx,
{
    type Rejection = Response;

    async fn from_request(request: Request, state: &Arc<S>) -> Result<Self, Self::Rejection> {
        let duration = state
            .prom()
            .http_request_duration
            .with_label_values(&[VOCAB_LEARN_ENDPOINT])
            .start_timer();
        if request.method() != Method::POST {
            return Err(vocab_learn_rejection(
                state.prom(),
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "POST is the vocabulary learning method supported by /_vocab/learn",
            ));
        }
        if request.uri().query().is_some_and(|query| !query.is_empty()) {
            return Err(vocab_learn_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "validation_error",
                "POST /_vocab/learn does not accept query parameters",
            ));
        }
        if !is_json_content_type(request.headers()) {
            return Err(vocab_learn_rejection(
                state.prom(),
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                "POST /_vocab/learn requires Content-Type: application/json",
            ));
        }

        let body = tokio::time::timeout(
            VOCAB_LEARN_BODY_TIMEOUT,
            Bytes::from_request(request, state),
        )
        .await
        .map_err(|_| {
            vocab_learn_rejection(
                state.prom(),
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "vocabulary learning body did not complete within 5s",
            )
        })?
        .map_err(|source| {
            let status = source.status();
            let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload_too_large"
            } else {
                "validation_error"
            };
            vocab_learn_rejection(
                state.prom(),
                status,
                error_type,
                format!("invalid vocabulary learning body: {source}"),
            )
        })?;
        Ok(Self { duration, body })
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

/// Learn and serialize the complete vocabulary on the shared administrative
/// blocking slot. The corpus is caller-supplied in both local modes.
pub(crate) async fn execute_vocab_learn(
    permits: &Arc<Semaphore>,
    prom: &PrometheusMetrics,
    transport: VocabLearnTransport,
) -> Response {
    let (_duration, body) = transport.into_parts();
    let Ok(permit) = Arc::clone(permits).acquire_owned().await else {
        return vocab_learn_rejection(
            prom,
            StatusCode::SERVICE_UNAVAILABLE,
            "vocab_unavailable",
            "vocabulary learning admission is closed",
        );
    };
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let request: LearnRequest = serde_json::from_slice(&body).map_err(|source| {
            LearnWorkerError::Invalid(format!("invalid vocabulary learning JSON body: {source}"))
        })?;
        request.validate().map_err(LearnWorkerError::Invalid)?;
        let (queries, config) = request.into_work();
        let vocab = reverse_rusty::vocab::learn_vocab_from_corpus(&queries, &config);
        if vocab.len() > VOCAB_LEARN_MAX_RESULT_ENTRIES {
            return Err(LearnWorkerError::Invalid(format!(
                "learned vocabulary has {} entries; maximum is \
                 {VOCAB_LEARN_MAX_RESULT_ENTRIES}; raise learning thresholds",
                vocab.len()
            )));
        }
        let encoded = serde_json::to_vec(&vocab).map_err(LearnWorkerError::Serialization)?;
        if encoded.len() > VOCAB_LEARN_BODY_LIMIT {
            return Err(LearnWorkerError::Invalid(format!(
                "learned vocabulary is larger than {VOCAB_LEARN_BODY_LIMIT} bytes and cannot be \
                 submitted to PUT /_vocab; raise learning thresholds"
            )));
        }
        Ok(encoded)
    });
    let response = match worker.await {
        Ok(Ok(encoded)) => (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            encoded,
        )
            .into_response(),
        Ok(Err(LearnWorkerError::Invalid(reason))) => {
            ApiError::response(StatusCode::BAD_REQUEST, "validation_error", reason).into_response()
        }
        Ok(Err(LearnWorkerError::Serialization(source))) => {
            error!(error = %source, "failed to serialize learned vocabulary");
            ApiError::response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "vocab_unavailable",
                "learned vocabulary serialization failed",
            )
            .into_response()
        }
        Err(join_error) => {
            error!(error = %join_error, "vocabulary learning worker failed");
            ApiError::response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "vocab_unavailable",
                "vocabulary learning worker failed",
            )
            .into_response()
        }
    };
    finish_vocab_learn_response(prom, response)
}

enum LearnWorkerError {
    Invalid(String),
    Serialization(serde_json::Error),
}

pub(crate) async fn learn_vocab(
    State(state): State<Arc<AppState>>,
    transport: VocabLearnTransport,
) -> Response {
    execute_vocab_learn(&state.stats_permits, &state.prom, transport).await
}

pub(crate) async fn vocab_learn_method_not_allowed<S: RequestCtx>(
    State(state): State<Arc<S>>,
    method: Method,
) -> Response {
    let _duration = state
        .prom()
        .http_request_duration
        .with_label_values(&[VOCAB_LEARN_ENDPOINT])
        .start_timer();
    let mut response = vocab_learn_rejection(
        state.prom(),
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        format!("{method} is not supported by /_vocab/learn"),
    );
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("POST"));
    response
}

fn vocab_learn_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    finish_vocab_learn_response(
        prom,
        ApiError::response(status, error_type, reason).into_response(),
    )
}

fn finish_vocab_learn_response(prom: &PrometheusMetrics, mut response: Response) -> Response {
    prom.http_requests_total
        .with_label_values(&[VOCAB_LEARN_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
