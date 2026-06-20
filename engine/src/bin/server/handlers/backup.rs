//! `POST /_backup` — snapshot the engine's durable state into a server-side
//! directory (ADR-079). Restore is operator-driven: point a fresh server at the copy
//! via `--data-dir`. The cluster-mode analogue lives in [`super::cluster::admin`].

use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info, instrument};

use reverse_rusty::storage::BackupError;

use crate::dto::ApiError;
use crate::state::AppState;

#[derive(Deserialize)]
pub(crate) struct BackupBody {
    /// Server-side destination directory for the snapshot. Must not already exist.
    dest: String,
}

#[derive(Serialize)]
struct BackupResponse {
    took_ms: f64,
    acknowledged: bool,
    dest: String,
}

/// POST /_backup — snapshot the engine's durable state into `dest`, a server-side
/// path that must not already exist (ADR-079). Restore by pointing a fresh server at
/// the copy via `--data-dir`. Holds the engine write lock for the copy so no
/// concurrent flush/compaction deletes a segment mid-snapshot; reads keep flowing off
/// the lock-free snapshot. An in-memory engine (no `--data-dir`) or an existing `dest`
/// is a 400; a persistence-degraded engine is a 503 (its on-disk state is incomplete).
#[instrument(skip_all)]
pub(crate) async fn backup(
    State(state): State<Arc<AppState>>,
    Json(body): Json<BackupBody>,
) -> Response {
    let start = Instant::now();
    let dest = std::path::PathBuf::from(&body.dest);
    let result = {
        let mut engine = state.engine.lock();
        engine.backup_to(&dest)
    };
    let took_ms = start.elapsed().as_secs_f64() * 1000.0;
    let code = match &result {
        Ok(()) => "200",
        Err(BackupError::NotDurable | BackupError::DestExists(_)) => "400",
        Err(BackupError::PersistenceDegraded) => "503",
        Err(_) => "500",
    };
    state
        .prom
        .http_requests_total
        .with_label_values(&["backup", code])
        .inc();
    state
        .prom
        .http_request_duration
        .with_label_values(&["backup"])
        .observe(start.elapsed().as_secs_f64());
    match result {
        Ok(()) => {
            info!(dest = %body.dest, took_ms, "backup complete");
            (
                StatusCode::OK,
                Json(BackupResponse {
                    took_ms,
                    acknowledged: true,
                    dest: body.dest,
                }),
            )
                .into_response()
        }
        Err(e) => {
            error!(dest = %body.dest, error = %e, "backup failed");
            let (status, kind) = match &e {
                BackupError::NotDurable => (StatusCode::BAD_REQUEST, "not_durable"),
                BackupError::DestExists(_) => (StatusCode::BAD_REQUEST, "dest_exists"),
                BackupError::PersistenceDegraded => {
                    (StatusCode::SERVICE_UNAVAILABLE, "persistence_degraded")
                }
                _ => (StatusCode::INTERNAL_SERVER_ERROR, "backup_error"),
            };
            ApiError::response(status, kind, e.to_string()).into_response()
        }
    }
}
