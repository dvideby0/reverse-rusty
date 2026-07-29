//! Strict native `POST /_vocab/aliases/discover` distributional dry run.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::Bytes,
    extract::{FromRequest, Request, State},
    http::{header, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
};
use prometheus::HistogramTimer;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tracing::error;

use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::{AppState, RequestCtx};

/// An explicit discovery corpus is a JSON admin document, not bulk ingest.
pub(crate) const ALIAS_DISCOVER_BODY_LIMIT: usize = 16 * 1024 * 1024;
pub(crate) const ALIAS_DISCOVER_BODY_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const ALIAS_DISCOVER_MAX_QUERIES: usize = 100_000;
pub(crate) const ALIAS_DISCOVER_MAX_CONTEXT_TOKENS: usize = 1_000_000;
pub(crate) const ALIAS_DISCOVER_MAX_PAIRS: usize = 100_000;
/// The algorithm's pair-key space is bounded by `N² / 2`; retain ADR-102's
/// shipped 4,096-token ceiling instead of accepting an unbounded override.
pub(crate) const ALIAS_DISCOVER_MAX_VOCAB: usize = 4_096;
pub(crate) const ALIAS_DISCOVER_RESPONSE_LIMIT: usize = 16 * 1024 * 1024;
const ALIAS_DISCOVER_ENDPOINT: &str = "vocab_aliases_discover";

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct AliasDiscoverRequest {
    /// Explicit `(id, dsl)` corpus. Absent means the standalone engine's live
    /// stored sources; coordinator mode requires this field.
    #[serde(default)]
    queries: Option<Vec<(u64, String)>>,
    #[serde(default)]
    min_token_freq: Option<usize>,
    #[serde(default)]
    min_similarity: Option<f64>,
    #[serde(default)]
    max_pairs: Option<usize>,
    #[serde(default)]
    max_vocab: Option<usize>,
    #[serde(default)]
    max_cooccurrence_rate: Option<f64>,
    #[serde(default)]
    glue_phrases: Option<bool>,
    #[serde(default)]
    include_numeric: Option<bool>,
}

struct AliasDiscoverWork {
    queries: Option<Vec<(u64, String)>>,
    config: reverse_rusty::vocab::DistributionalConfig,
}

impl AliasDiscoverRequest {
    fn into_work(self) -> Result<AliasDiscoverWork, String> {
        let defaults = reverse_rusty::vocab::DistributionalConfig::default();
        let config = reverse_rusty::vocab::DistributionalConfig {
            min_token_freq: self.min_token_freq.unwrap_or(defaults.min_token_freq),
            min_similarity: self.min_similarity.unwrap_or(defaults.min_similarity),
            max_pairs: self.max_pairs.unwrap_or(defaults.max_pairs),
            max_vocab: self.max_vocab.unwrap_or(defaults.max_vocab),
            max_cooccurrence_rate: self
                .max_cooccurrence_rate
                .unwrap_or(defaults.max_cooccurrence_rate),
            glue_phrases: self.glue_phrases.unwrap_or(defaults.glue_phrases),
            include_numeric: self.include_numeric.unwrap_or(defaults.include_numeric),
            ..defaults
        };
        validate_controls(&config)?;
        if let Some(queries) = self.queries.as_ref() {
            validate_explicit_corpus(queries)?;
        }
        Ok(AliasDiscoverWork {
            queries: self.queries,
            config,
        })
    }
}

fn validate_controls(config: &reverse_rusty::vocab::DistributionalConfig) -> Result<(), String> {
    if config.min_token_freq == 0 {
        return Err("min_token_freq must be at least 1".to_string());
    }
    if !config.min_similarity.is_finite() || !(0.0..=1.0).contains(&config.min_similarity) {
        return Err("min_similarity must be finite and between 0 and 1".to_string());
    }
    if config.max_pairs > ALIAS_DISCOVER_MAX_PAIRS {
        return Err(format!(
            "max_pairs must not exceed {ALIAS_DISCOVER_MAX_PAIRS}"
        ));
    }
    if !(1..=ALIAS_DISCOVER_MAX_VOCAB).contains(&config.max_vocab) {
        return Err(format!(
            "max_vocab must be between 1 and {ALIAS_DISCOVER_MAX_VOCAB}"
        ));
    }
    if !config.max_cooccurrence_rate.is_finite()
        || !(0.0..=1.0).contains(&config.max_cooccurrence_rate)
    {
        return Err("max_cooccurrence_rate must be finite and between 0 and 1".to_string());
    }
    Ok(())
}

fn validate_explicit_corpus(queries: &[(u64, String)]) -> Result<(), String> {
    if queries.len() > ALIAS_DISCOVER_MAX_QUERIES {
        return Err(format!(
            "queries has {} entries; maximum is {ALIAS_DISCOVER_MAX_QUERIES}",
            queries.len()
        ));
    }

    let mut ids = HashSet::with_capacity(queries.len());
    let mut context_tokens = 0usize;
    for (position, (id, query)) in queries.iter().enumerate() {
        if !ids.insert(*id) {
            return Err(format!(
                "queries[{position}] repeats query id {id}; ids must be unique"
            ));
        }
        let ast = reverse_rusty::dsl::parse(query)
            .map_err(|source| format!("queries[{position}] with id {id} is invalid: {source}"))?;
        for clause in ast.clauses {
            if clause.negated {
                continue;
            }
            let tokens = match clause.atom {
                reverse_rusty::dsl::Atom::Term(surface)
                | reverse_rusty::dsl::Atom::Phrase(surface) => {
                    reverse_rusty::corpus::tokenize(&surface).len()
                }
                reverse_rusty::dsl::Atom::AnyOf(members) => members
                    .iter()
                    .map(|surface| reverse_rusty::corpus::tokenize(surface).len())
                    .sum(),
            };
            context_tokens = context_tokens.saturating_add(tokens);
            if context_tokens > ALIAS_DISCOVER_MAX_CONTEXT_TOKENS {
                return Err(format!(
                    "queries contain more than {ALIAS_DISCOVER_MAX_CONTEXT_TOKENS} positive \
                     context tokens"
                ));
            }
        }
    }
    Ok(())
}

/// Method/query/media/body validation shared by standalone and coordinator
/// discovery. An empty body is meaningful in standalone mode.
pub(crate) struct AliasDiscoverTransport {
    duration: HistogramTimer,
    started: Instant,
    body: Bytes,
}

impl AliasDiscoverTransport {
    fn into_parts(self) -> (HistogramTimer, Instant, Bytes) {
        (self.duration, self.started, self.body)
    }
}

impl<S> FromRequest<Arc<S>> for AliasDiscoverTransport
where
    S: RequestCtx,
{
    type Rejection = Response;

    async fn from_request(request: Request, state: &Arc<S>) -> Result<Self, Self::Rejection> {
        let started = Instant::now();
        let duration = state
            .prom()
            .http_request_duration
            .with_label_values(&[ALIAS_DISCOVER_ENDPOINT])
            .start_timer();
        if request.method() != Method::POST {
            return Err(alias_discover_rejection(
                state.prom(),
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "POST is the distributional alias discovery method supported by \
                 /_vocab/aliases/discover",
            ));
        }
        if request.uri().query().is_some_and(|query| !query.is_empty()) {
            return Err(alias_discover_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "validation_error",
                "POST /_vocab/aliases/discover does not accept query parameters",
            ));
        }

        let json_content_type = is_json_content_type(request.headers());
        let body = tokio::time::timeout(
            ALIAS_DISCOVER_BODY_TIMEOUT,
            Bytes::from_request(request, state),
        )
        .await
        .map_err(|_| {
            alias_discover_rejection(
                state.prom(),
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "alias discovery body did not complete within 5s",
            )
        })?
        .map_err(|source| {
            let status = source.status();
            let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload_too_large"
            } else {
                "validation_error"
            };
            alias_discover_rejection(
                state.prom(),
                status,
                error_type,
                format!("invalid alias discovery body: {source}"),
            )
        })?;
        if !body.is_empty() && !json_content_type {
            return Err(alias_discover_rejection(
                state.prom(),
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                "a non-empty POST /_vocab/aliases/discover body requires Content-Type: \
                 application/json",
            ));
        }

        Ok(Self {
            duration,
            started,
            body,
        })
    }
}

fn is_json_content_type(headers: &axum::http::HeaderMap) -> bool {
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
struct AliasDiscoverResponse {
    took: u64,
    took_ms: f64,
    count: usize,
    proposals: Vec<reverse_rusty::vocab::DiscoveredPair>,
}

enum AliasDiscoverWorkerError {
    Invalid(String),
    Serialization(serde_json::Error),
}

/// Run parsing, validation, corpus capture, discovery, and serialization on the
/// shared one-slot blocking worker. `stored_corpus` is called only when the
/// request omits `queries`; coordinator mode supplies a fail-loud closure.
pub(crate) async fn execute_alias_discovery<F>(
    permits: &Arc<Semaphore>,
    prom: &PrometheusMetrics,
    transport: AliasDiscoverTransport,
    stored_corpus: F,
) -> Response
where
    F: FnOnce() -> Result<Vec<(u64, String)>, String> + Send + 'static,
{
    let (_duration, started, body) = transport.into_parts();
    let Ok(permit) = Arc::clone(permits).acquire_owned().await else {
        return alias_discover_rejection(
            prom,
            StatusCode::SERVICE_UNAVAILABLE,
            "aliases_unavailable",
            "alias discovery admission is closed",
        );
    };

    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let request = if body.is_empty() {
            AliasDiscoverRequest::default()
        } else {
            serde_json::from_slice(&body).map_err(|source| {
                AliasDiscoverWorkerError::Invalid(format!(
                    "invalid alias discovery JSON body: {source}"
                ))
            })?
        };
        let work = request
            .into_work()
            .map_err(AliasDiscoverWorkerError::Invalid)?;
        let queries = match work.queries {
            Some(queries) => queries,
            None => stored_corpus().map_err(AliasDiscoverWorkerError::Invalid)?,
        };
        let proposals = reverse_rusty::vocab::discover_pairs(&queries, &work.config);
        let took_ms = started.elapsed().as_secs_f64() * 1_000.0;
        let count = proposals.len();
        let encoded = serde_json::to_vec(&AliasDiscoverResponse {
            took: took_ms.floor() as u64,
            took_ms,
            count,
            proposals,
        })
        .map_err(AliasDiscoverWorkerError::Serialization)?;
        if encoded.len() > ALIAS_DISCOVER_RESPONSE_LIMIT {
            return Err(AliasDiscoverWorkerError::Invalid(format!(
                "alias discovery response exceeds {ALIAS_DISCOVER_RESPONSE_LIMIT} bytes; lower \
                 max_pairs or raise discovery thresholds"
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
        Ok(Err(AliasDiscoverWorkerError::Invalid(reason))) => {
            ApiError::response(StatusCode::BAD_REQUEST, "validation_error", reason).into_response()
        }
        Ok(Err(AliasDiscoverWorkerError::Serialization(source))) => {
            error!(error = %source, "failed to serialize alias discovery response");
            ApiError::response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "aliases_unavailable",
                "alias discovery response serialization failed",
            )
            .into_response()
        }
        Err(join_error) => {
            error!(error = %join_error, "alias-discovery worker failed");
            ApiError::response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "aliases_unavailable",
                "alias-discovery worker failed",
            )
            .into_response()
        }
    };
    finish_alias_discover_response(prom, response)
}

pub(crate) async fn discover_aliases(
    State(state): State<Arc<AppState>>,
    transport: AliasDiscoverTransport,
) -> Response {
    let worker_state = Arc::clone(&state);
    execute_alias_discovery(&state.stats_permits, &state.prom, transport, move || {
        let queries = worker_state.engine.lock().live_sources();
        Ok(queries)
    })
    .await
}

pub(crate) async fn alias_discover_method_not_allowed<S: RequestCtx>(
    State(state): State<Arc<S>>,
    method: Method,
) -> Response {
    let _duration = state
        .prom()
        .http_request_duration
        .with_label_values(&[ALIAS_DISCOVER_ENDPOINT])
        .start_timer();
    let mut response = alias_discover_rejection(
        state.prom(),
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        format!("{method} is not supported by /_vocab/aliases/discover"),
    );
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("POST"));
    response
}

fn alias_discover_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    finish_alias_discover_response(
        prom,
        ApiError::response(status, error_type, reason).into_response(),
    )
}

fn finish_alias_discover_response(prom: &PrometheusMetrics, mut response: Response) -> Response {
    prom.http_requests_total
        .with_label_values(&[ALIAS_DISCOVER_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
