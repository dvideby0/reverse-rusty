//! Strict native `GET`/`HEAD /_settings` live-configuration read.

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

use reverse_rusty::config::EngineConfig;

use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::{AppState, RequestCtx};

pub(crate) const SETTINGS_READ_BODY_LIMIT: usize = 64 * 1024;
pub(crate) const SETTINGS_READ_BODY_TIMEOUT: Duration = Duration::from_millis(250);
const SETTINGS_READ_RESPONSE_LIMIT: usize = 64 * 1024;
const SETTINGS_READ_ENDPOINT: &str = "settings_get";

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct SettingsReadParams {
    /// Familiar ES/OpenSearch cluster-settings control.
    #[serde(default)]
    include_defaults: bool,
    /// Accepted honestly: Reverse Rusty's setting keys are already flat, so
    /// either value has the same representation.
    #[serde(default, rename = "flat_settings")]
    _flat_settings: bool,
}

/// Method, query, and body contract shared by standalone and coordinator mode.
pub(crate) struct SettingsReadTransport {
    duration: HistogramTimer,
    include_defaults: bool,
}

impl SettingsReadTransport {
    pub(crate) fn into_parts(self) -> (HistogramTimer, bool) {
        (self.duration, self.include_defaults)
    }
}

impl<S> FromRequest<Arc<S>> for SettingsReadTransport
where
    S: RequestCtx,
{
    type Rejection = Response;

    async fn from_request(request: Request, state: &Arc<S>) -> Result<Self, Self::Rejection> {
        let duration = state
            .prom()
            .http_request_duration
            .with_label_values(&[SETTINGS_READ_ENDPOINT])
            .start_timer();
        match *request.method() {
            Method::GET | Method::HEAD => {}
            _ => {
                return Err(settings_read_rejection(
                    state.prom(),
                    StatusCode::METHOD_NOT_ALLOWED,
                    "method_not_allowed",
                    "GET and HEAD are the read methods supported by /_settings",
                ));
            }
        }

        let Query(params) =
            Query::<SettingsReadParams>::try_from_uri(request.uri()).map_err(|source| {
                settings_read_rejection(
                    state.prom(),
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    format!("invalid settings query parameters: {source}"),
                )
            })?;
        let body = tokio::time::timeout(
            SETTINGS_READ_BODY_TIMEOUT,
            Bytes::from_request(request, state),
        )
        .await
        .map_err(|_| {
            settings_read_rejection(
                state.prom(),
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "settings read body did not complete within 250ms",
            )
        })?
        .map_err(|source| {
            let status = source.status();
            let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload_too_large"
            } else {
                "validation_error"
            };
            settings_read_rejection(
                state.prom(),
                status,
                error_type,
                format!("invalid settings read body: {source}"),
            )
        })?;
        if !body.is_empty() {
            return Err(settings_read_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "validation_error",
                "GET/HEAD /_settings does not accept a request body",
            ));
        }

        Ok(Self {
            duration,
            include_defaults: params.include_defaults,
        })
    }
}

#[derive(Serialize)]
struct GetSettingsResponse<'a> {
    settings: &'a EngineConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    defaults: Option<&'a EngineConfig>,
}

#[derive(Debug)]
pub(crate) enum SettingsReadWorkerError {
    Serialization(serde_json::Error),
    ResponseTooLarge(usize),
}

pub(crate) fn serialize_settings_response<T: Serialize>(
    response: &T,
) -> Result<Vec<u8>, SettingsReadWorkerError> {
    let encoded = serde_json::to_vec(response).map_err(SettingsReadWorkerError::Serialization)?;
    if encoded.len() > SETTINGS_READ_RESPONSE_LIMIT {
        return Err(SettingsReadWorkerError::ResponseTooLarge(encoded.len()));
    }
    Ok(encoded)
}

/// Read one immutable standalone configuration and serialize it off the async
/// executor under the shared administrative admission bound.
pub(crate) async fn get_settings(
    State(state): State<Arc<AppState>>,
    transport: SettingsReadTransport,
) -> Response {
    let (_duration, include_defaults) = transport.into_parts();
    let permit = match acquire_settings_read_permit(&state.stats_permits, &state.prom).await {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    let snapshot = state.snapshot.load_full();
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let defaults = include_defaults.then(EngineConfig::default);
        serialize_settings_response(&GetSettingsResponse {
            settings: snapshot.config(),
            defaults: defaults.as_ref(),
        })
    });
    finish_settings_read_worker(&state.prom, worker.await)
}

pub(crate) async fn acquire_settings_read_permit(
    permits: &Arc<Semaphore>,
    prom: &PrometheusMetrics,
) -> Result<OwnedSemaphorePermit, Response> {
    Arc::clone(permits).acquire_owned().await.map_err(|_| {
        settings_read_rejection(
            prom,
            StatusCode::SERVICE_UNAVAILABLE,
            "settings_unavailable",
            "settings read admission is closed",
        )
    })
}

pub(crate) fn finish_settings_read_worker(
    prom: &PrometheusMetrics,
    result: Result<Result<Vec<u8>, SettingsReadWorkerError>, tokio::task::JoinError>,
) -> Response {
    match result {
        Ok(Ok(encoded)) => finish_settings_read_response(
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
        Ok(Err(SettingsReadWorkerError::Serialization(source))) => {
            error!(error = %source, "failed to serialize settings response");
            settings_read_rejection(
                prom,
                StatusCode::INTERNAL_SERVER_ERROR,
                "settings_unavailable",
                "settings response serialization failed",
            )
        }
        Ok(Err(SettingsReadWorkerError::ResponseTooLarge(actual))) => {
            error!(
                actual,
                limit = SETTINGS_READ_RESPONSE_LIMIT,
                "settings response exceeded its fixed serialization limit"
            );
            settings_read_rejection(
                prom,
                StatusCode::INTERNAL_SERVER_ERROR,
                "settings_unavailable",
                "settings response exceeded its fixed serialization limit",
            )
        }
        Err(join_error) => {
            error!(error = %join_error, "settings read worker failed");
            settings_read_rejection(
                prom,
                StatusCode::INTERNAL_SERVER_ERROR,
                "settings_unavailable",
                "settings read worker failed",
            )
        }
    }
}

pub(crate) async fn settings_method_not_allowed<S: RequestCtx>(
    State(state): State<Arc<S>>,
    method: Method,
) -> Response {
    let _duration = state
        .prom()
        .http_request_duration
        .with_label_values(&[SETTINGS_READ_ENDPOINT])
        .start_timer();
    let mut response = settings_read_rejection(
        state.prom(),
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        format!("{method} is not supported by /_settings"),
    );
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("GET, HEAD, PUT"));
    response
}

fn settings_read_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    finish_settings_read_response(
        prom,
        ApiError::response(status, error_type, reason).into_response(),
    )
}

pub(crate) fn finish_settings_read_response(
    prom: &PrometheusMetrics,
    mut response: Response,
) -> Response {
    prom.http_requests_total
        .with_label_values(&[SETTINGS_READ_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
