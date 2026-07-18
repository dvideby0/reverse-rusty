//! PIT lifecycle endpoints (`POST /v2/_pit` open, `DELETE /v2/_pit` close;
//! ADR-113).
//!
//! A PIT pins the current engine snapshot under a bounded, renew-on-use
//! keep-alive so `/v2/_search` cursor pages traverse one frozen view. The
//! registry is in-memory by design (restart ⇒ every token is stale ⇒ 409).

use std::sync::Arc;
use std::time::Instant;

use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::dto::ApiError;
use crate::state::AppState;

use crate::pit::{keep_alive_from_secs, pit_error_response, token_failure_response};

#[derive(Deserialize, Default)]
pub(crate) struct OpenPitBody {
    keep_alive_s: Option<u64>,
}

#[derive(Serialize)]
pub(crate) struct OpenPitResponse {
    pit_id: String,
}

#[derive(Deserialize)]
pub(crate) struct ClosePitBody {
    pit_id: String,
}

#[derive(Serialize)]
pub(crate) struct ClosePitResponse {
    closed: bool,
}

/// Open a PIT over the current snapshot. An empty body takes the default
/// keep-alive; the registry cap rejects with 429 (never evicts a live PIT).
#[instrument(skip_all)]
pub(crate) async fn open_pit(
    State(state): State<Arc<AppState>>,
    body: Option<Json<OpenPitBody>>,
) -> Result<Json<OpenPitResponse>, (StatusCode, Json<ApiError>)> {
    let keep_alive = keep_alive_from_secs(body.and_then(|Json(b)| b.keep_alive_s));
    let snapshot = state.snapshot.load_full();
    let now = Instant::now();
    let opened = {
        let mut pits = state.pits.lock();
        // Dropping the reaped snapshot Arcs IS the local release.
        drop(pits.reap_expired(now));
        let opened = pits.open(snapshot, keep_alive, &state.pit_config, now);
        state.prom.open_pits.set(pits.len() as i64);
        opened
    };
    match opened {
        Ok(pit) => Ok(Json(OpenPitResponse {
            pit_id: state.pit_tokens.mint_pit(pit),
        })),
        Err(error) => Err(pit_error_response(error)),
    }
}

/// Close a PIT, releasing its pinned snapshot. Closing an already-gone PIT is
/// `closed: false`, not an error — the client's goal state is achieved.
#[instrument(skip_all)]
pub(crate) async fn close_pit(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ClosePitBody>,
) -> Result<Json<ClosePitResponse>, (StatusCode, Json<ApiError>)> {
    let pit = state
        .pit_tokens
        .verify_pit(&body.pit_id)
        .map_err(token_failure_response)?;
    let closed = {
        let mut pits = state.pits.lock();
        let closed = pits.close(pit).is_some();
        state.prom.open_pits.set(pits.len() as i64);
        closed
    };
    Ok(Json(ClosePitResponse { closed }))
}
