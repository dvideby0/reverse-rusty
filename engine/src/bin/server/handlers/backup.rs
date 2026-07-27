//! Strict native `POST /_backup` boundary.
//!
//! The storage operation is intentionally native: ES/OpenSearch snapshots
//! require a registered repository, named snapshot, and asynchronous status
//! surface that Reverse Rusty does not expose. Standalone and in-process
//! cluster modes share the transport contract here; the cluster executor lives
//! in [`super::cluster::admin`].

#[cfg(test)]
mod tests;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use axum::{
    body::Bytes,
    extract::{rejection::BytesRejection, RawQuery, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info, instrument};

use reverse_rusty::storage::BackupError;

use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::AppState;

/// A backup request contains one server-side path, so it must not inherit the
/// server's 100 MiB ingest ceiling.
pub(crate) const BACKUP_BODY_LIMIT: usize = 64 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BackupBody {
    /// Server-side destination directory for the snapshot. Must not already exist.
    dest: String,
}

pub(crate) struct PreparedBackup {
    pub(crate) dest: String,
    pub(crate) path: PathBuf,
}

#[derive(Serialize)]
pub(crate) struct BackupResponse {
    took: u64,
    took_ms: f64,
    acknowledged: bool,
    dest: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    epoch: Option<u64>,
}

impl BackupResponse {
    #[must_use]
    pub(crate) fn new(took_ms: f64, dest: String, epoch: Option<u64>) -> Self {
        Self {
            took: took_ms.floor() as u64,
            took_ms,
            acknowledged: true,
            dest,
            epoch,
        }
    }
}

pub(crate) fn backup_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    prom.http_requests_total
        .with_label_values(&["backup", status.as_str()])
        .inc();
    ApiError::response(status, error_type, reason).into_response()
}

pub(crate) fn validate_backup_method(
    prom: &PrometheusMetrics,
    method: &Method,
) -> Result<(), Box<Response>> {
    if *method == Method::POST {
        return Ok(());
    }
    let mut response = Box::new(backup_rejection(
        prom,
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        "POST is the only supported /_backup method",
    ));
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("POST"));
    Err(response)
}

fn is_json_content_type(headers: &HeaderMap) -> bool {
    let Some(content_type) = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    let media_type = content_type
        .split_once(';')
        .map_or(content_type, |(value, _)| value)
        .trim()
        .to_ascii_lowercase();
    media_type == "application/json"
        || media_type
            .strip_prefix("application/")
            .is_some_and(|subtype| subtype.ends_with("+json"))
}

/// Decode the full transport contract before acquiring writer admission.
pub(crate) fn validate_backup_request(
    prom: &PrometheusMetrics,
    RawQuery(raw_query): RawQuery,
    headers: &HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Result<PreparedBackup, Box<Response>> {
    if raw_query.as_deref().is_some_and(|query| !query.is_empty()) {
        return Err(Box::new(backup_rejection(
            prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            "POST /_backup does not accept query parameters",
        )));
    }
    let body = body.map_err(|error| {
        let status = error.status();
        let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
            "payload_too_large"
        } else {
            "validation_error"
        };
        Box::new(backup_rejection(
            prom,
            status,
            error_type,
            format!("invalid backup body: {error}"),
        ))
    })?;
    if !is_json_content_type(headers) {
        return Err(Box::new(backup_rejection(
            prom,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
            "POST /_backup requires Content-Type: application/json",
        )));
    }
    let decoded: BackupBody = serde_json::from_slice(&body).map_err(|error| {
        Box::new(backup_rejection(
            prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            format!("invalid backup JSON body: {error}"),
        ))
    })?;
    if decoded.dest.trim().is_empty() {
        return Err(Box::new(backup_rejection(
            prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            "backup `dest` must not be empty or whitespace-only",
        )));
    }
    if decoded.dest.contains('\0') {
        return Err(Box::new(backup_rejection(
            prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            "backup `dest` must not contain a NUL byte",
        )));
    }
    Ok(PreparedBackup {
        path: PathBuf::from(&decoded.dest),
        dest: decoded.dest,
    })
}

async fn execute_backup(
    state: Arc<AppState>,
    dest: PathBuf,
) -> Result<Result<(), BackupError>, tokio::task::JoinError> {
    tokio::task::spawn_blocking(move || {
        let mut engine = state.engine.lock();
        engine.backup_to(&dest)
    })
    .await
}

#[cfg(test)]
pub(super) async fn execute_backup_for_test(
    state: Arc<AppState>,
    dest: PathBuf,
) -> Result<Result<(), BackupError>, tokio::task::JoinError> {
    execute_backup(state, dest).await
}

/// Snapshot the standalone engine's durable state into a fresh server-side
/// directory. The response waits for copy, verification, and atomic promotion;
/// the blocking filesystem work runs outside Tokio's async workers.
#[instrument(skip_all)]
pub(crate) async fn backup_route(
    State(state): State<Arc<AppState>>,
    method: Method,
    raw_query: RawQuery,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let _duration = state
        .prom
        .http_request_duration
        .with_label_values(&["backup"])
        .start_timer();
    let started = Instant::now();
    if let Err(response) = validate_backup_method(&state.prom, &method) {
        return *response;
    }
    let prepared = match validate_backup_request(&state.prom, raw_query, &headers, body) {
        Ok(prepared) => prepared,
        Err(response) => return *response,
    };
    let result = match execute_backup(Arc::clone(&state), prepared.path).await {
        Ok(result) => result,
        Err(join_error) => {
            error!(error = %join_error, "backup worker failed");
            return backup_rejection(
                &state.prom,
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "backup worker failed",
            );
        }
    };
    let took_ms = started.elapsed().as_secs_f64() * 1_000.0;
    match result {
        Ok(()) => {
            info!(dest = %prepared.dest, took_ms, "backup complete");
            state
                .prom
                .http_requests_total
                .with_label_values(&["backup", "200"])
                .inc();
            Json(BackupResponse::new(took_ms, prepared.dest, None)).into_response()
        }
        Err(error) => {
            error!(dest = %prepared.dest, error = %error, "backup failed");
            let (status, error_type) = match &error {
                BackupError::NotDurable => (StatusCode::BAD_REQUEST, "not_durable"),
                BackupError::DestExists(_) => (StatusCode::BAD_REQUEST, "dest_exists"),
                BackupError::PersistenceDegraded => {
                    (StatusCode::SERVICE_UNAVAILABLE, "persistence_degraded")
                }
                _ => (StatusCode::INTERNAL_SERVER_ERROR, "backup_error"),
            };
            backup_rejection(&state.prom, status, error_type, error.to_string())
        }
    }
}
