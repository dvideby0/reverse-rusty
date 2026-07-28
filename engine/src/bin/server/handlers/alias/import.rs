//! Strict native `POST /_vocab/aliases/import` mutation.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::Bytes,
    extract::{FromRequest, Query, Request, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use prometheus::HistogramTimer;
use serde::{Deserialize, Serialize};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{error, info};

use reverse_rusty::segment::Engine;
use reverse_rusty::vocab::{validate_solr_aliases, AliasSummary, MAX_ALIAS_IMPORT_RULES};
use reverse_rusty::AliasApplyReport;

use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::{AppState, RequestCtx};

mod document;
use document::{SynonymRule, SynonymsSet};

/// Alias files are bounded independently from the server's bulk-ingest ceiling.
pub(crate) const ALIAS_IMPORT_BODY_LIMIT: usize = 16 * 1024 * 1024;
pub(crate) const ALIAS_IMPORT_BODY_TIMEOUT: Duration = Duration::from_secs(5);
const ALIAS_IMPORT_ENDPOINT: &str = "vocab_aliases_import";
const MAX_RULE_ID_BYTES: usize = 256;

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct AliasImportParams {
    /// Elasticsearch's synchronous analyzer-reload control. Reverse Rusty
    /// always applies synchronously, so only true/omission is honest.
    #[serde(default)]
    refresh: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AliasImportDocument {
    /// Native Solr/Lucene file text.
    #[serde(default)]
    synonyms: Option<String>,
    /// Elasticsearch-compatible rule object or array of rule objects.
    #[serde(default)]
    synonyms_set: Option<SynonymsSet>,
    /// OpenSearch synonym-filter spelling. Only `solr` is implemented.
    #[serde(default)]
    format: Option<String>,
    /// OpenSearch synonym-filter spelling. Reverse Rusty equivalences expand.
    #[serde(default)]
    expand: Option<bool>,
}

impl AliasImportDocument {
    fn into_payload(self) -> Result<AliasImportPayload, String> {
        if self.format.as_deref().unwrap_or("solr") != "solr" {
            return Err(
                "alias import supports only OpenSearch `format: \"solr\"`; WordNet is not \
                 implemented"
                    .to_string(),
            );
        }
        if self.expand == Some(false) {
            return Err(
                "alias import requires OpenSearch `expand: true`; directional `expand: false` \
                 semantics are not implemented"
                    .to_string(),
            );
        }

        match (self.synonyms, self.synonyms_set) {
            (Some(text), None) => Ok(AliasImportPayload::Native(text)),
            (None, Some(SynonymsSet::One(rule))) => {
                let text = validate_rule_metadata(rule, 0, &mut BTreeSet::new())?;
                Ok(AliasImportPayload::Elasticsearch(vec![text]))
            }
            (None, Some(SynonymsSet::Many(rules))) => {
                if rules.is_empty() {
                    return Err("`synonyms_set` must contain at least one rule object".to_string());
                }
                if rules.len() > MAX_ALIAS_IMPORT_RULES {
                    return Err(format!(
                        "`synonyms_set` accepts at most {MAX_ALIAS_IMPORT_RULES} rule objects"
                    ));
                }
                let mut ids = BTreeSet::new();
                let mut texts = Vec::with_capacity(rules.len());
                for (index, rule) in rules.into_iter().enumerate() {
                    texts.push(validate_rule_metadata(rule, index, &mut ids)?);
                }
                Ok(AliasImportPayload::Elasticsearch(texts))
            }
            (Some(_), Some(_)) => Err(
                "specify exactly one of native `synonyms` or Elasticsearch `synonyms_set`"
                    .to_string(),
            ),
            (None, None) => Err(
                "alias import requires native `synonyms` or Elasticsearch `synonyms_set`"
                    .to_string(),
            ),
        }
    }
}

fn validate_rule_metadata(
    rule: SynonymRule,
    index: usize,
    ids: &mut BTreeSet<String>,
) -> Result<String, String> {
    if let Some(id) = rule.id {
        if id.len() > MAX_RULE_ID_BYTES {
            return Err(format!(
                "`synonyms_set[{index}].id` may not exceed {MAX_RULE_ID_BYTES} bytes"
            ));
        }
        let id = id.trim();
        if id.is_empty() {
            return Err(format!("`synonyms_set[{index}].id` may not be empty"));
        }
        if !ids.insert(id.to_string()) {
            return Err(format!("duplicate synonym rule id `{id}`"));
        }
    }
    Ok(rule.synonyms)
}

/// Decoded input whose potentially large Solr parse is deferred to the bounded
/// blocking worker.
pub(crate) enum AliasImportPayload {
    Native(String),
    Elasticsearch(Vec<String>),
}

impl AliasImportPayload {
    pub(crate) fn validate(self) -> Result<(String, usize), String> {
        match self {
            Self::Native(text) => {
                let rules = validate_solr_aliases(&text).map_err(|error| error.to_string())?;
                Ok((text, rules))
            }
            Self::Elasticsearch(texts) => {
                let rule_count = texts.len();
                for (index, text) in texts.iter().enumerate() {
                    let count = validate_solr_aliases(text)
                        .map_err(|error| format!("invalid `synonyms_set[{index}]`: {error}"))?;
                    if count != 1 {
                        return Err(format!(
                            "`synonyms_set[{index}].synonyms` must contain exactly one rule"
                        ));
                    }
                }
                Ok((texts.join("\n"), rule_count))
            }
        }
    }
}

/// Strict request transport shared by standalone and coordinator imports.
pub(crate) struct AliasImportTransport {
    duration: HistogramTimer,
    started: Instant,
    payload: AliasImportPayload,
}

impl AliasImportTransport {
    pub(crate) fn into_parts(self) -> (HistogramTimer, Instant, AliasImportPayload) {
        (self.duration, self.started, self.payload)
    }
}

impl<S> FromRequest<Arc<S>> for AliasImportTransport
where
    S: RequestCtx,
{
    type Rejection = Response;

    async fn from_request(request: Request, state: &Arc<S>) -> Result<Self, Self::Rejection> {
        let started = Instant::now();
        let duration = state
            .prom()
            .http_request_duration
            .with_label_values(&[ALIAS_IMPORT_ENDPOINT])
            .start_timer();
        if request.method() != Method::POST {
            return Err(alias_import_rejection(
                state.prom(),
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "POST is the alias import method supported by /_vocab/aliases/import",
            ));
        }
        let Query(params) =
            Query::<AliasImportParams>::try_from_uri(request.uri()).map_err(|source| {
                alias_import_rejection(
                    state.prom(),
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    format!("invalid alias-import query parameters: {source}"),
                )
            })?;
        if params.refresh == Some(false) {
            return Err(alias_import_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "validation_error",
                "alias import applies synchronously; Elasticsearch `refresh=false` is not supported",
            ));
        }
        if !is_json_content_type(request.headers()) {
            return Err(alias_import_rejection(
                state.prom(),
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                "POST /_vocab/aliases/import requires Content-Type: application/json",
            ));
        }

        let body = tokio::time::timeout(
            ALIAS_IMPORT_BODY_TIMEOUT,
            Bytes::from_request(request, state),
        )
        .await
        .map_err(|_| {
            alias_import_rejection(
                state.prom(),
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "alias-import body did not complete within 5s",
            )
        })?
        .map_err(|source| {
            let status = source.status();
            let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload_too_large"
            } else {
                "validation_error"
            };
            alias_import_rejection(
                state.prom(),
                status,
                error_type,
                format!("invalid alias-import body: {source}"),
            )
        })?;
        let document: AliasImportDocument = serde_json::from_slice(&body).map_err(|source| {
            alias_import_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "validation_error",
                format!("invalid alias-import JSON body: {source}"),
            )
        })?;
        let payload = document.into_payload().map_err(|reason| {
            alias_import_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "validation_error",
                reason,
            )
        })?;

        Ok(Self {
            duration,
            started,
            payload,
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
struct AliasImportResponse {
    took: u64,
    took_ms: f64,
    acknowledged: bool,
    result: &'static str,
    rules: usize,
    activated: usize,
    recompiled: usize,
    summary: AliasSummary,
}

pub(crate) fn alias_import_success(
    started: Instant,
    rules: usize,
    report: AliasApplyReport,
) -> Response {
    let took_ms = started.elapsed().as_secs_f64() * 1_000.0;
    Json(AliasImportResponse {
        took: took_ms.floor() as u64,
        took_ms,
        acknowledged: true,
        result: if report.applied { "updated" } else { "noop" },
        rules,
        activated: report.activated,
        recompiled: report.recompiled,
        summary: report.summary,
    })
    .into_response()
}

pub(crate) async fn acquire_alias_import_permit(
    permits: &Arc<Semaphore>,
    prom: &PrometheusMetrics,
) -> Result<OwnedSemaphorePermit, Response> {
    Arc::clone(permits).acquire_owned().await.map_err(|_| {
        alias_import_rejection(
            prom,
            StatusCode::SERVICE_UNAVAILABLE,
            "aliases_unavailable",
            "alias-import admission is closed",
        )
    })
}

enum StandaloneAliasImport {
    Applied {
        report: AliasApplyReport,
        rules: usize,
    },
    InvalidRequest(String),
    InvalidVocab(String),
    PersistenceUnavailable(String),
    NotDurable(AliasApplyReport),
    Incomplete {
        expected: usize,
        recompiled: usize,
    },
}

fn apply_standalone_alias_import(
    engine: &mut Engine,
    synonyms: &str,
    rules: usize,
) -> StandaloneAliasImport {
    let durable = engine.config().data_dir.is_some();
    if durable && !engine.persistence_healthy() {
        return StandaloneAliasImport::PersistenceUnavailable(
            "cannot import aliases while persistence is unhealthy; repair or restart from the \
             last committed state first"
                .to_string(),
        );
    }

    let expected = engine.live_sources().len();
    let report = match engine.import_alias_synonyms(synonyms) {
        Ok(report) => report,
        Err(source) => return StandaloneAliasImport::InvalidVocab(source.to_string()),
    };
    if report.applied && (report.recompiled != expected || engine.has_stale_segments()) {
        return StandaloneAliasImport::Incomplete {
            expected,
            recompiled: report.recompiled,
        };
    }
    if report.applied && durable && !engine.persistence_healthy() {
        return StandaloneAliasImport::NotDurable(report);
    }
    StandaloneAliasImport::Applied { report, rules }
}

/// Import and apply aliases on one bounded blocking worker.
pub(crate) async fn import_aliases(
    State(state): State<Arc<AppState>>,
    transport: AliasImportTransport,
) -> Response {
    let (_duration, started, payload) = transport.into_parts();
    let permit = match acquire_alias_import_permit(&state.stats_permits, &state.prom).await {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    let work_state = Arc::clone(&state);
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let (synonyms, rules) = match payload.validate() {
            Ok(validated) => validated,
            Err(reason) => return StandaloneAliasImport::InvalidRequest(reason),
        };
        let outcome = {
            let mut engine = work_state.engine.lock();
            apply_standalone_alias_import(&mut engine, &synonyms, rules)
        };
        if matches!(
            outcome,
            StandaloneAliasImport::Applied {
                report: AliasApplyReport { applied: true, .. },
                ..
            } | StandaloneAliasImport::NotDurable(_)
        ) {
            work_state.publish_snapshot();
        }
        outcome
    });

    let response = match worker.await {
        Ok(StandaloneAliasImport::Applied { report, rules }) => {
            info!(
                result = if report.applied { "updated" } else { "noop" },
                activated = report.activated,
                recompiled = report.recompiled,
                "alias import complete"
            );
            alias_import_success(started, rules, report)
        }
        Ok(StandaloneAliasImport::InvalidRequest(reason)) => {
            alias_import_error_response(StatusCode::BAD_REQUEST, "validation_error", reason)
        }
        Ok(StandaloneAliasImport::InvalidVocab(reason)) => {
            alias_import_error_response(StatusCode::BAD_REQUEST, "vocab_error", reason)
        }
        Ok(StandaloneAliasImport::PersistenceUnavailable(reason)) => alias_import_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "persistence_unavailable",
            reason,
        ),
        Ok(StandaloneAliasImport::NotDurable(report)) => {
            error!(
                recompiled = report.recompiled,
                "alias import is live but was not durably committed"
            );
            alias_import_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "persistence_unavailable",
                format!(
                    "alias import is live and {} queries were recompiled, but the rebuild was not \
                     durably committed; repair or restart from the last committed state",
                    report.recompiled
                ),
            )
        }
        Ok(StandaloneAliasImport::Incomplete {
            expected,
            recompiled,
        }) => {
            error!(
                expected,
                recompiled, "alias import left stale or incomplete query state"
            );
            alias_import_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "aliases_unavailable",
                "alias-import query rebuild did not complete",
            )
        }
        Err(join_error) => {
            error!(error = %join_error, "alias-import worker failed");
            alias_import_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "aliases_unavailable",
                "alias-import worker failed",
            )
        }
    };
    finish_alias_import_response(&state.prom, response)
}

pub(crate) async fn alias_import_method_not_allowed<S: RequestCtx>(
    State(state): State<Arc<S>>,
    method: Method,
) -> Response {
    let _duration = state
        .prom()
        .http_request_duration
        .with_label_values(&[ALIAS_IMPORT_ENDPOINT])
        .start_timer();
    let mut response = alias_import_rejection(
        state.prom(),
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        format!("{method} is not supported by /_vocab/aliases/import"),
    );
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("POST"));
    response
}

fn alias_import_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    finish_alias_import_response(
        prom,
        alias_import_error_response(status, error_type, reason),
    )
}

pub(crate) fn alias_import_error_response(
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    ApiError::response(status, error_type, reason).into_response()
}

pub(crate) fn finish_alias_import_response(
    prom: &PrometheusMetrics,
    mut response: Response,
) -> Response {
    prom.http_requests_total
        .with_label_values(&[ALIAS_IMPORT_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
