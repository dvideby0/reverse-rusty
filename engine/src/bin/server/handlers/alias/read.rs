//! Strict native `GET`/`HEAD /_vocab/aliases` registry review.

use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Bytes,
    extract::{FromRequest, Query, Request, State},
    http::{header, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
};
use prometheus::HistogramTimer;
use serde::{Deserialize, Serialize};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::error;

use reverse_rusty::vocab::{AliasEntry, AliasRegistry, AliasSummary};

use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::{AppState, RequestCtx};

pub(crate) const ALIAS_READ_BODY_LIMIT: usize = 64 * 1024;
pub(crate) const ALIAS_READ_BODY_TIMEOUT: Duration = Duration::from_millis(250);
const ALIAS_READ_ENDPOINT: &str = "vocab_aliases_get";

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct AliasReadParams {
    /// Familiar Elasticsearch synonym-set offset.
    #[serde(default)]
    from: usize,
    /// Familiar Elasticsearch synonym-set page size. Absent preserves the
    /// historical complete-registry response.
    #[serde(default)]
    size: Option<usize>,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct AliasReadPage {
    from: usize,
    size: Option<usize>,
}

impl From<AliasReadParams> for AliasReadPage {
    fn from(params: AliasReadParams) -> Self {
        Self {
            from: params.from,
            size: params.size,
        }
    }
}

/// Strict shared transport for standalone and coordinator registry reads.
pub(crate) struct AliasReadTransport {
    duration: HistogramTimer,
    page: AliasReadPage,
}

impl AliasReadTransport {
    pub(crate) fn into_parts(self) -> (HistogramTimer, AliasReadPage) {
        (self.duration, self.page)
    }
}

impl<S> FromRequest<Arc<S>> for AliasReadTransport
where
    S: RequestCtx,
{
    type Rejection = Response;

    async fn from_request(request: Request, state: &Arc<S>) -> Result<Self, Self::Rejection> {
        let duration = state
            .prom()
            .http_request_duration
            .with_label_values(&[ALIAS_READ_ENDPOINT])
            .start_timer();
        match *request.method() {
            Method::GET | Method::HEAD => {}
            _ => {
                return Err(alias_read_rejection(
                    state.prom(),
                    StatusCode::METHOD_NOT_ALLOWED,
                    "method_not_allowed",
                    "GET and HEAD are the review methods supported by /_vocab/aliases",
                ));
            }
        }

        let Query(params) =
            Query::<AliasReadParams>::try_from_uri(request.uri()).map_err(|source| {
                alias_read_rejection(
                    state.prom(),
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    format!("invalid alias-registry query parameters: {source}"),
                )
            })?;

        let body =
            tokio::time::timeout(ALIAS_READ_BODY_TIMEOUT, Bytes::from_request(request, state))
                .await
                .map_err(|_| {
                    alias_read_rejection(
                        state.prom(),
                        StatusCode::REQUEST_TIMEOUT,
                        "request_timeout",
                        "alias-registry read body did not complete within 250ms",
                    )
                })?
                .map_err(|source| {
                    let status = source.status();
                    let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
                        "payload_too_large"
                    } else {
                        "validation_error"
                    };
                    alias_read_rejection(
                        state.prom(),
                        status,
                        error_type,
                        format!("invalid alias-registry read body: {source}"),
                    )
                })?;
        if !body.is_empty() {
            return Err(alias_read_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "validation_error",
                "GET/HEAD /_vocab/aliases does not accept a request body",
            ));
        }

        Ok(Self {
            duration,
            page: params.into(),
        })
    }
}

#[derive(Serialize)]
struct AliasEntries<'a> {
    entries: &'a [AliasEntry],
}

#[derive(Serialize)]
struct GetAliasesResponse<'a> {
    /// Total entries before paging, matching the Elasticsearch synonym-set
    /// meaning of `count`.
    count: usize,
    aliases: AliasEntries<'a>,
    /// Whole-registry lifecycle counts, not merely the selected page.
    summary: AliasSummary,
}

pub(crate) fn serialize_aliases(
    aliases: Option<&AliasRegistry>,
    page: AliasReadPage,
) -> Result<Vec<u8>, serde_json::Error> {
    let empty = AliasRegistry::default();
    let aliases = aliases.unwrap_or(&empty);
    let count = aliases.len();
    let start = page.from.min(count);
    let end = page
        .size
        .map_or(count, |size| start.saturating_add(size).min(count));
    serde_json::to_vec(&GetAliasesResponse {
        count,
        aliases: AliasEntries {
            entries: &aliases.entries()[start..end],
        },
        summary: aliases.summary(),
    })
}

/// Serialize one immutable standalone snapshot off the async executor.
pub(crate) async fn get_aliases(
    State(state): State<Arc<AppState>>,
    transport: AliasReadTransport,
) -> Response {
    let (_duration, page) = transport.into_parts();
    let permit = match acquire_alias_read_permit(&state.stats_permits, &state.prom).await {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    let snapshot = state.snapshot.load_full();
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        serialize_aliases(
            snapshot.vocab().map(reverse_rusty::vocab::Vocab::aliases),
            page,
        )
    });
    finish_alias_read_worker(&state.prom, worker.await)
}

pub(crate) async fn acquire_alias_read_permit(
    permits: &Arc<Semaphore>,
    prom: &PrometheusMetrics,
) -> Result<OwnedSemaphorePermit, Response> {
    Arc::clone(permits).acquire_owned().await.map_err(|_| {
        alias_read_rejection(
            prom,
            StatusCode::SERVICE_UNAVAILABLE,
            "aliases_unavailable",
            "alias-registry read admission is closed",
        )
    })
}

pub(crate) fn finish_alias_read_worker(
    prom: &PrometheusMetrics,
    result: Result<Result<Vec<u8>, serde_json::Error>, tokio::task::JoinError>,
) -> Response {
    match result {
        Ok(Ok(encoded)) => finish_alias_read_response(
            prom,
            (
                StatusCode::OK,
                [(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                )],
                encoded,
            )
                .into_response(),
        ),
        Ok(Err(source)) => {
            error!(error = %source, "failed to serialize alias registry");
            alias_read_rejection(
                prom,
                StatusCode::INTERNAL_SERVER_ERROR,
                "aliases_unavailable",
                "alias-registry serialization failed",
            )
        }
        Err(join_error) => {
            error!(error = %join_error, "alias-registry read worker failed");
            alias_read_rejection(
                prom,
                StatusCode::INTERNAL_SERVER_ERROR,
                "aliases_unavailable",
                "alias-registry read worker failed",
            )
        }
    }
}

pub(crate) async fn alias_read_method_not_allowed<S: RequestCtx>(
    State(state): State<Arc<S>>,
    method: Method,
) -> Response {
    let _duration = state
        .prom()
        .http_request_duration
        .with_label_values(&[ALIAS_READ_ENDPOINT])
        .start_timer();
    let mut response = alias_read_rejection(
        state.prom(),
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        format!("{method} is not supported by /_vocab/aliases"),
    );
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("GET, HEAD"));
    response
}

fn alias_read_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    finish_alias_read_response(
        prom,
        ApiError::response(status, error_type, reason).into_response(),
    )
}

fn finish_alias_read_response(prom: &PrometheusMetrics, mut response: Response) -> Response {
    prom.http_requests_total
        .with_label_values(&[ALIAS_READ_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
