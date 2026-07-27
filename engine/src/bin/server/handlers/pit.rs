//! PIT lifecycle endpoints (`POST /v2/_pit` open, `DELETE /v2/_pit` close;
//! ADR-113).
//!
//! A PIT pins the current engine snapshot under a bounded, renew-on-use
//! keep-alive so `/v2/_search` cursor pages traverse one frozen view. The
//! registry is in-memory by design (restart ⇒ every token is stale ⇒ 409).

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::{
    body::Bytes,
    extract::{
        rejection::{BytesRejection, QueryRejection},
        Query, State,
    },
    http::{header::CONTENT_TYPE, HeaderMap, StatusCode},
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::dto::ApiError;
use crate::state::{AppState, ClusterAppState};

use crate::pit::{pit_error_response, token_failure_response};

use reverse_rusty::cluster::ClusterPitError;

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenPitControls {
    /// Native seconds control retained for backwards compatibility.
    #[serde(default, deserialize_with = "deserialize_non_null")]
    keep_alive_s: Option<u64>,
    /// ES/OpenSearch time-value spelling (`250ms`, `2m`, ...).
    #[serde(default, deserialize_with = "deserialize_non_null")]
    keep_alive: Option<String>,
    /// Native fail-loud partial-result control.
    #[serde(default, deserialize_with = "deserialize_non_null")]
    allow_partial_results: Option<bool>,
    /// Elasticsearch spelling for PIT shard admission.
    #[serde(default, deserialize_with = "deserialize_non_null")]
    allow_partial_search_results: Option<bool>,
    /// OpenSearch spelling for PIT shard admission.
    #[serde(default, deserialize_with = "deserialize_non_null")]
    allow_partial_pit_creation: Option<bool>,
}

pub(crate) type OpenPitBody = OpenPitControls;
type OpenPitParams = OpenPitControls;

#[derive(Serialize)]
struct PitShards {
    total: usize,
    successful: usize,
    skipped: usize,
    failed: usize,
}

#[derive(Serialize)]
pub(crate) struct OpenPitResponse {
    /// Elasticsearch response spelling.
    id: String,
    /// OpenSearch response spelling and Reverse Rusty's original field.
    pit_id: String,
    /// OpenSearch creation timestamp, in Unix epoch milliseconds.
    creation_time: u64,
    _shards: PitShards,
}

#[derive(Deserialize)]
pub(crate) struct ClosePitBody {
    pit_id: String,
}

#[derive(Serialize)]
pub(crate) struct ClosePitResponse {
    closed: bool,
}

type Reject = (StatusCode, Json<ApiError>);

/// `Option<T>` normally maps explicit JSON null to `None`, conflating a
/// wrong-type control with an omitted one. The field-level default handles
/// omission; a present value must deserialize as its concrete type.
fn deserialize_non_null<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

fn validation(reason: impl Into<String>) -> Reject {
    ApiError::response(StatusCode::BAD_REQUEST, "validation_error", reason)
}

fn one_alias<T>(left: Option<T>, right: Option<T>, names: &str) -> Result<Option<T>, String> {
    match (left, right) {
        (Some(_), Some(_)) => Err(format!(
            "`{names}` are aliases; specify exactly one of them"
        )),
        (left, right) => Ok(left.or(right)),
    }
}

fn one_location<T>(body: Option<T>, query: Option<T>, name: &str) -> Result<Option<T>, String> {
    match (body, query) {
        (Some(_), Some(_)) => Err(format!(
            "`{name}` must be specified in either the request body or query string, not both"
        )),
        (body, query) => Ok(body.or(query)),
    }
}

fn location_keep_alive(
    controls: OpenPitControls,
    location: &str,
) -> Result<(Option<Duration>, Option<bool>), String> {
    let keep_alive = match (controls.keep_alive_s, controls.keep_alive) {
        (Some(_), Some(_)) => {
            return Err(format!(
                "`keep_alive_s` and `keep_alive` are aliases; specify exactly one in the {location}"
            ));
        }
        (Some(seconds), None) => Some(Duration::from_secs(seconds)),
        (None, Some(value)) => Some(super::search::parse_named_time_value("keep_alive", &value)?),
        (None, None) => None,
    };
    let partial = one_alias(
        controls.allow_partial_results,
        controls.allow_partial_search_results,
        "allow_partial_results` and `allow_partial_search_results",
    )?;
    let partial = one_alias(
        partial,
        controls.allow_partial_pit_creation,
        "allow_partial_results/allow_partial_search_results` and `allow_partial_pit_creation",
    )?;
    Ok((keep_alive, partial))
}

fn resolve_open_pit(
    body: OpenPitControls,
    params: OpenPitParams,
) -> Result<Option<Duration>, String> {
    let (body_keep_alive, body_partial) = location_keep_alive(body, "request body")?;
    let (query_keep_alive, query_partial) = location_keep_alive(params, "query string")?;
    let keep_alive = one_location(body_keep_alive, query_keep_alive, "keep_alive")?;
    let partial = one_location(
        body_partial,
        query_partial,
        "allow_partial_results/allow_partial_search_results/allow_partial_pit_creation",
    )?;
    if partial == Some(true) {
        return Err(
            "partial PIT creation is not supported; set the partial-results control to false"
                .to_string(),
        );
    }
    Ok(keep_alive)
}

fn is_json_content_type(headers: &HeaderMap) -> bool {
    let Some(value) = headers.get(CONTENT_TYPE) else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };
    let media_type = value
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    media_type == "application/json"
        || (media_type.starts_with("application/") && media_type.ends_with("+json"))
}

fn parse_open_body(headers: &HeaderMap, bytes: &Bytes) -> Result<OpenPitBody, Reject> {
    if bytes.is_empty() {
        return Ok(OpenPitBody::default());
    }
    if !is_json_content_type(headers) {
        return Err(ApiError::response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
            "POST /v2/_pit requires Content-Type application/json when a body is present",
        ));
    }
    serde_json::from_slice(bytes)
        .map_err(|error| validation(format!("invalid v2 PIT body: {error}")))
}

fn body_rejection(error: &BytesRejection) -> Reject {
    let status = error.status();
    let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
        "payload_too_large"
    } else {
        "validation_error"
    };
    ApiError::response(status, error_type, format!("invalid v2 PIT body: {error}"))
}

fn query_rejection(error: &QueryRejection) -> Reject {
    validation(format!("invalid v2 PIT query parameters: {error}"))
}

fn creation_time_millis() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

fn open_response(token: String, creation_time: u64, shards: usize) -> Json<OpenPitResponse> {
    Json(OpenPitResponse {
        id: token.clone(),
        pit_id: token,
        creation_time,
        _shards: PitShards {
            total: shards,
            successful: shards,
            skipped: 0,
            failed: 0,
        },
    })
}

/// Strict HTTP boundary for local PIT creation. An absent body takes defaults;
/// a present body must be JSON, while ES/OpenSearch controls may use the query
/// string without requiring an artificial empty JSON document.
#[instrument(skip_all)]
#[allow(clippy::unused_async)] // Axum handlers are asynchronous entry points.
pub(crate) async fn open_pit_route(
    State(state): State<Arc<AppState>>,
    params: Result<Query<OpenPitParams>, QueryRejection>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<OpenPitResponse>, Reject> {
    let Query(params) = params.map_err(|error| query_rejection(&error))?;
    let bytes = body.map_err(|error| body_rejection(&error))?;
    let body = parse_open_body(&headers, &bytes)?;
    open_pit_inner(&state, body, params)
}

/// Open a PIT over the current snapshot. An empty body takes the default
/// keep-alive; the registry cap rejects with 429 (never evicts a live PIT).
#[cfg(test)]
#[instrument(skip_all)]
pub(crate) fn open_pit(
    State(state): State<Arc<AppState>>,
    body: Option<Json<OpenPitBody>>,
) -> Result<Json<OpenPitResponse>, Reject> {
    open_pit_inner(
        &state,
        body.map_or_else(OpenPitBody::default, |Json(body)| body),
        OpenPitParams::default(),
    )
}

fn open_pit_inner(
    state: &AppState,
    body: OpenPitBody,
    params: OpenPitParams,
) -> Result<Json<OpenPitResponse>, Reject> {
    let keep_alive = resolve_open_pit(body, params).map_err(validation)?;
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
        Ok(pit) => Ok(open_response(
            state.pit_tokens.mint_pit(pit),
            creation_time_millis(),
            1,
        )),
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
        let now = Instant::now();
        let mut pits = state.pits.lock();
        // Reap first: an expired target honestly reports `closed: false`, and
        // a DELETE-first client still frees every expired cap slot (dropping
        // the reaped Arcs IS the local release) — codex review.
        drop(pits.reap_expired(now));
        let closed = pits.close(pit).is_some();
        state.prom.open_pits.set(pits.len() as i64);
        closed
    };
    Ok(Json(ClosePitResponse { closed }))
}

fn cluster_pit_error_response(error: ClusterPitError) -> (StatusCode, Json<ApiError>) {
    match error {
        ClusterPitError::Unsupported(detail) => {
            ApiError::response(StatusCode::NOT_IMPLEMENTED, "pit_unsupported", detail)
        }
        ClusterPitError::Admission(error) => pit_error_response(error),
    }
}

fn join_failure() -> (StatusCode, Json<ApiError>) {
    ApiError::response(
        StatusCode::INTERNAL_SERVER_ERROR,
        "search_error",
        "pit task failed",
    )
}

/// Coordinator-mode open: pins EVERY position's current snapshot under one id
/// (index-wide, ES-style). The cluster lock is taken inside `spawn_blocking` —
/// a concurrent vocab/resize rebuild holds the write lock for a long time, and
/// an async-path read would park an executor thread behind it.
#[instrument(skip_all)]
pub(crate) async fn cluster_open_pit_route(
    State(state): State<Arc<ClusterAppState>>,
    params: Result<Query<OpenPitParams>, QueryRejection>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<OpenPitResponse>, Reject> {
    let Query(params) = params.map_err(|error| query_rejection(&error))?;
    let bytes = body.map_err(|error| body_rejection(&error))?;
    let body = parse_open_body(&headers, &bytes)?;
    let keep_alive = resolve_open_pit(body, params).map_err(validation)?;
    let worker = Arc::clone(&state);
    let opened = tokio::task::spawn_blocking(move || {
        let cluster = worker.cluster.read();
        let opened = cluster.open_pit(keep_alive, &worker.pit_config, Instant::now());
        let creation_time = creation_time_millis();
        worker.prom.open_pits.set(cluster.open_pit_count() as i64);
        (opened, cluster.num_shards(), creation_time)
    })
    .await
    .map_err(|_| join_failure())?;
    match opened.0 {
        Ok(pit) => Ok(open_response(
            state.pit_tokens.mint_pit(pit),
            opened.2,
            opened.1,
        )),
        Err(error) => Err(cluster_pit_error_response(error)),
    }
}

/// Coordinator-mode close: releases the registry entry and every shard pin.
#[instrument(skip_all)]
pub(crate) async fn cluster_close_pit(
    State(state): State<Arc<ClusterAppState>>,
    Json(body): Json<ClosePitBody>,
) -> Result<Json<ClosePitResponse>, (StatusCode, Json<ApiError>)> {
    let pit = state
        .pit_tokens
        .verify_pit(&body.pit_id)
        .map_err(token_failure_response)?;
    let worker = Arc::clone(&state);
    let closed = tokio::task::spawn_blocking(move || {
        let cluster = worker.cluster.read();
        let closed = cluster.close_pit(pit, Instant::now());
        worker.prom.open_pits.set(cluster.open_pit_count() as i64);
        closed
    })
    .await
    .map_err(|_| join_failure())?;
    Ok(Json(ClosePitResponse { closed }))
}
