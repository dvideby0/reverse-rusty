//! Strict native `GET`/`HEAD /_vocab` document read.

use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Bytes,
    extract::{FromRequest, Request, State},
    http::{header, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
};
use prometheus::HistogramTimer;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::error;

use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::{AppState, RequestCtx};

pub(crate) const VOCAB_READ_BODY_LIMIT: usize = 64 * 1024;
pub(crate) const VOCAB_READ_BODY_TIMEOUT: Duration = Duration::from_millis(250);
const VOCAB_READ_ENDPOINT: &str = "vocab_get";

/// Method/query validation and bounded body extraction for the vocabulary read.
///
/// The route-level body limit applies only to GET/HEAD; `PUT /_vocab` retains the
/// server's larger JSON-document allowance.
pub(crate) struct VocabReadTransport {
    duration: HistogramTimer,
}

impl VocabReadTransport {
    pub(crate) fn into_timer(self) -> HistogramTimer {
        self.duration
    }
}

impl<S> FromRequest<Arc<S>> for VocabReadTransport
where
    S: RequestCtx,
{
    type Rejection = Response;

    async fn from_request(request: Request, state: &Arc<S>) -> Result<Self, Self::Rejection> {
        let duration = state
            .prom()
            .http_request_duration
            .with_label_values(&[VOCAB_READ_ENDPOINT])
            .start_timer();
        match *request.method() {
            Method::GET | Method::HEAD => {}
            _ => {
                return Err(vocab_read_rejection(
                    state.prom(),
                    StatusCode::METHOD_NOT_ALLOWED,
                    "method_not_allowed",
                    "GET and HEAD are the read methods supported by /_vocab",
                ));
            }
        }
        if request.uri().query().is_some_and(|query| !query.is_empty()) {
            return Err(vocab_read_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "validation_error",
                "GET/HEAD /_vocab does not accept query parameters",
            ));
        }

        let body =
            tokio::time::timeout(VOCAB_READ_BODY_TIMEOUT, Bytes::from_request(request, state))
                .await
                .map_err(|_| {
                    vocab_read_rejection(
                        state.prom(),
                        StatusCode::REQUEST_TIMEOUT,
                        "request_timeout",
                        "vocabulary read body did not complete within 250ms",
                    )
                })?
                .map_err(|error| {
                    let status = error.status();
                    let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
                        "payload_too_large"
                    } else {
                        "validation_error"
                    };
                    vocab_read_rejection(
                        state.prom(),
                        status,
                        error_type,
                        format!("invalid vocabulary read body: {error}"),
                    )
                })?;
        if !body.is_empty() {
            return Err(vocab_read_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "validation_error",
                "GET/HEAD /_vocab does not accept a request body",
            ));
        }

        Ok(Self { duration })
    }
}

/// Return one complete, round-trippable vocabulary document from an immutable
/// engine snapshot. Serialization is bounded with the other administrative
/// snapshot reads and runs off the async executor.
pub(crate) async fn get_vocab(
    State(state): State<Arc<AppState>>,
    transport: VocabReadTransport,
) -> Response {
    let _duration = transport.into_timer();
    let permit = match acquire_vocab_read_permit(&state.stats_permits, &state.prom).await {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    let snapshot = state.snapshot.load_full();
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        serialize_vocab(snapshot.vocab())
    });
    finish_vocab_worker(&state.prom, worker.await)
}

pub(crate) async fn acquire_vocab_read_permit(
    permits: &Arc<Semaphore>,
    prom: &PrometheusMetrics,
) -> Result<OwnedSemaphorePermit, Response> {
    Arc::clone(permits).acquire_owned().await.map_err(|_| {
        vocab_read_rejection(
            prom,
            StatusCode::SERVICE_UNAVAILABLE,
            "vocab_unavailable",
            "vocabulary read admission is closed",
        )
    })
}

pub(crate) fn serialize_vocab(
    vocab: Option<&reverse_rusty::vocab::Vocab>,
) -> Result<Vec<u8>, serde_json::Error> {
    match vocab {
        Some(vocab) => serde_json::to_vec(vocab),
        None => serde_json::to_vec(&reverse_rusty::vocab::Vocab::default()),
    }
}

pub(crate) fn finish_vocab_worker(
    prom: &PrometheusMetrics,
    result: Result<Result<Vec<u8>, serde_json::Error>, tokio::task::JoinError>,
) -> Response {
    match result {
        Ok(Ok(encoded)) => finish_vocab_read_response(
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
            error!(error = %source, "failed to serialize vocabulary");
            vocab_read_rejection(
                prom,
                StatusCode::INTERNAL_SERVER_ERROR,
                "vocab_unavailable",
                "vocabulary serialization failed",
            )
        }
        Err(join_error) => {
            error!(error = %join_error, "vocabulary read worker failed");
            vocab_read_rejection(
                prom,
                StatusCode::INTERNAL_SERVER_ERROR,
                "vocab_unavailable",
                "vocabulary read worker failed",
            )
        }
    }
}

pub(crate) async fn vocab_method_not_allowed<S: RequestCtx>(
    State(state): State<Arc<S>>,
    method: Method,
) -> Response {
    let _duration = state
        .prom()
        .http_request_duration
        .with_label_values(&[VOCAB_READ_ENDPOINT])
        .start_timer();
    let mut response = vocab_read_rejection(
        state.prom(),
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        format!("{method} is not supported by /_vocab"),
    );
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("GET, HEAD, PUT"));
    response
}

pub(crate) fn vocab_read_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    finish_vocab_read_response(
        prom,
        ApiError::response(status, error_type, reason).into_response(),
    )
}

pub(crate) fn finish_vocab_read_response(
    prom: &PrometheusMetrics,
    mut response: Response,
) -> Response {
    prom.http_requests_total
        .with_label_values(&[VOCAB_READ_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    // Keep the representation body intact here. Axum's top-level method router
    // computes its exact Content-Length and then strips it for HEAD, preserving
    // the GET representation metadata required by RFC 9110.
    response
}
