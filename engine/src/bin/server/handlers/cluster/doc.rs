//! Cluster-mode `_doc` CRUD + `_bulk` (ADR-070). `PUT /_doc/{id}` is the
//! cluster-atomic upsert — ONE `ClusterMutation::Upsert` log frame replaces every
//! prior live copy and inserts the new version (ES `index` semantics, the ADR-067
//! contract at the coordinator). `_bulk` maps each index action onto the same upsert,
//! one per-item status each.

use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use tracing::{error, info, instrument, warn};

use reverse_rusty::cluster::{AddOutcome, ShardError};

use crate::dto::{ApiError, HitSource};
use crate::handlers::doc::{
    extract_bulk_id, extract_ranked_ingest, PutDocBody, CLASS_D_REJECT_MSG,
};
use crate::state::ClusterAppState;

use super::shard_error_response;

#[derive(Serialize)]
struct ClusterPutDocResponse {
    _id: u64,
    result: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct ClusterGetDocResponse {
    _id: u64,
    found: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    _source: Option<HitSource>,
}

#[derive(Serialize)]
struct ClusterDeleteDocResponse {
    _id: u64,
    result: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    deleted_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct ClusterBulkResponse {
    took_ms: f64,
    errors: bool,
    items: Vec<ClusterBulkItem>,
}

#[derive(Serialize)]
struct ClusterBulkItem {
    index: ClusterBulkItemInner,
}

#[derive(Serialize)]
struct ClusterBulkItemInner {
    _id: u64,
    status: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Render one upsert outcome as the PUT /_doc response. Shared with the per-item
/// bulk mapping so single and bulk writes can never drift.
fn upsert_status(
    removed: usize,
    outcome: &AddOutcome,
) -> (StatusCode, &'static str, Option<String>) {
    match outcome {
        AddOutcome::Placed { .. } | AddOutcome::Replicated => {
            if removed > 0 {
                (StatusCode::OK, "updated", None)
            } else {
                (StatusCode::CREATED, "created", None)
            }
        }
        AddOutcome::RejectedClassD => (
            StatusCode::BAD_REQUEST,
            "rejected",
            Some(format!(
                "{CLASS_D_REJECT_MSG}; in cluster mode class-D queries are rejected at \
                 placement (the cluster always-candidate lane is ADR-065 criterion 8)"
            )),
        ),
        AddOutcome::RejectedParse(e) => (
            StatusCode::BAD_REQUEST,
            "error",
            Some(format!("parse error: {e}")),
        ),
    }
}

/// PUT /_doc/{id} — cluster-atomic upsert (ADR-070): replace-by-id under ONE
/// coordinator log frame. 201 `created` for a fresh id, 200 `updated` for a
/// replacement; a rejected new version (parse / class D) leaves the prior version
/// live. A partial multi-shard apply (remote clusters only) answers 200 `partial`:
/// the mutation IS durably logged and queued for repair — re-PUTting would
/// double-log (`POST /_cluster/resync` converges it).
#[instrument(skip(state, body), fields(query_id = id))]
pub(crate) async fn cluster_put_doc(
    State(state): State<Arc<ClusterAppState>>,
    Path(id): Path<u64>,
    Json(body): Json<PutDocBody>,
) -> Response {
    let start = Instant::now();
    // A malformed tag value is a caller error: 400 before any coordinator work
    // (ADR-073 — never silently drop a tag the caller asked for).
    let tags = match extract_ranked_ingest(&body.rest) {
        Ok((tags, _rank)) => tags,
        Err((error_type, msg)) => {
            warn!(query_id = id, error = %msg, "invalid tag value");
            state
                .prom
                .http_requests_total
                .with_label_values(&["put_doc", "400"])
                .inc();
            // Keep the latency histogram complete (mirrors the single-node
            // handler — every other exit records a duration).
            state
                .prom
                .http_request_duration
                .with_label_values(&["put_doc"])
                .observe(start.elapsed().as_secs_f64());
            if error_type == "invalid_tag_value" {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ClusterPutDocResponse {
                        _id: id,
                        result: "error",
                        error: Some(msg),
                    }),
                )
                    .into_response();
            }
            return ApiError::response(StatusCode::BAD_REQUEST, error_type, msg).into_response();
        }
    };
    let result = {
        let _w = state.write_serial.lock();
        let cluster = state.cluster.read();
        cluster.upsert_query_with_tags(id, &body.query, body.version, &tags)
    };
    let response = match result {
        Ok((removed, outcome)) => {
            let (status, result, error) = upsert_status(removed, &outcome);
            match status {
                StatusCode::CREATED => info!(query_id = id, "query registered"),
                StatusCode::OK => info!(query_id = id, removed, "query replaced"),
                _ => warn!(query_id = id, result, "query rejected"),
            }
            state
                .prom
                .http_requests_total
                .with_label_values(&["put_doc", status.as_str()])
                .inc();
            (
                status,
                Json(ClusterPutDocResponse {
                    _id: id,
                    result,
                    error,
                }),
            )
                .into_response()
        }
        Err(ShardError::PartiallyApplied {
            ref applied,
            ref failed,
            ..
        }) => {
            // Durably logged + queued for repair: tell the caller precisely, with a
            // 200 (NOT a retry signal — a re-PUT would double-log; resync converges).
            warn!(
                query_id = id,
                ?applied,
                ?failed,
                "upsert partially applied; queued for repair"
            );
            state
                .prom
                .http_requests_total
                .with_label_values(&["put_doc", "200"])
                .inc();
            (
                StatusCode::OK,
                Json(ClusterPutDocResponse {
                    _id: id,
                    result: "partial",
                    error: Some(format!(
                        "applied on shards {applied:?}, pending on {failed:?}; durably \
                         logged — POST /_cluster/resync (or reopen) converges it"
                    )),
                }),
            )
                .into_response()
        }
        Err(e) => {
            error!(query_id = id, error = %e, "cluster upsert failed");
            state
                .prom
                .http_requests_total
                .with_label_values(&["put_doc", "503"])
                .inc();
            shard_error_response("upsert rejected", &e)
        }
    };
    state
        .prom
        .http_request_duration
        .with_label_values(&["put_doc"])
        .observe(start.elapsed().as_secs_f64());
    response
}

/// GET /_doc/{id} — retrieve a stored query's source, probing each shard's source
/// store (local clusters). On a remote cluster the lookup fails loud with 501
/// rather than reporting a false `found: false`.
#[instrument(skip(state), fields(query_id = id))]
pub(crate) async fn cluster_get_doc(
    State(state): State<Arc<ClusterAppState>>,
    Path(id): Path<u64>,
) -> Response {
    let result = {
        let cluster = state.cluster.read();
        cluster.get_source(id)
    };
    match result {
        Ok(Some(query)) => {
            state
                .prom
                .http_requests_total
                .with_label_values(&["get_doc", "200"])
                .inc();
            (
                StatusCode::OK,
                Json(ClusterGetDocResponse {
                    _id: id,
                    found: true,
                    _source: Some(HitSource { query }),
                }),
            )
                .into_response()
        }
        Ok(None) => {
            state
                .prom
                .http_requests_total
                .with_label_values(&["get_doc", "404"])
                .inc();
            (
                StatusCode::NOT_FOUND,
                Json(ClusterGetDocResponse {
                    _id: id,
                    found: false,
                    _source: None,
                }),
            )
                .into_response()
        }
        Err(e) => {
            state
                .prom
                .http_requests_total
                .with_label_values(&["get_doc", "501"])
                .inc();
            ApiError::response(
                StatusCode::NOT_IMPLEMENTED,
                "not_supported_in_cluster_mode",
                format!("source lookup is not available on this cluster: {e}"),
            )
            .into_response()
        }
    }
}

/// DELETE /_doc/{id} — remove a stored query everywhere (idempotent fan-out).
#[instrument(skip(state), fields(query_id = id))]
pub(crate) async fn cluster_delete_doc(
    State(state): State<Arc<ClusterAppState>>,
    Path(id): Path<u64>,
) -> Response {
    let result = {
        let _w = state.write_serial.lock();
        let cluster = state.cluster.read();
        cluster.remove_query(id)
    };
    match result {
        Ok(n) if n > 0 => {
            info!(query_id = id, deleted = n, "query deleted");
            state
                .prom
                .http_requests_total
                .with_label_values(&["delete_doc", "200"])
                .inc();
            (
                StatusCode::OK,
                Json(ClusterDeleteDocResponse {
                    _id: id,
                    result: "deleted",
                    deleted_count: Some(n as u64),
                    error: None,
                }),
            )
                .into_response()
        }
        Ok(_) => {
            state
                .prom
                .http_requests_total
                .with_label_values(&["delete_doc", "404"])
                .inc();
            (
                StatusCode::NOT_FOUND,
                Json(ClusterDeleteDocResponse {
                    _id: id,
                    result: "not_found",
                    deleted_count: None,
                    error: None,
                }),
            )
                .into_response()
        }
        Err(e) => {
            error!(query_id = id, error = %e, "cluster delete failed");
            state
                .prom
                .http_requests_total
                .with_label_values(&["delete_doc", "503"])
                .inc();
            shard_error_response("delete rejected", &e)
        }
    }
}

/// POST /_bulk — NDJSON bulk: each index action is one cluster upsert (the same
/// frame `PUT /_doc` writes), one per-item status each. Items after a durability
/// failure keep their own honest 503s (per-item upserts are independent — there is
/// no all-or-nothing batch at the coordinator).
#[instrument(skip_all)]
pub(crate) async fn cluster_bulk(
    State(state): State<Arc<ClusterAppState>>,
    headers: HeaderMap,
    body: String,
) -> Response {
    let start = Instant::now();

    if let Some(ct) = headers.get("content-type") {
        if let Ok(ct_str) = ct.to_str() {
            let ct_lower = ct_str.to_ascii_lowercase();
            if !ct_lower.starts_with("application/json")
                && !ct_lower.starts_with("application/x-ndjson")
            {
                state
                    .prom
                    .http_requests_total
                    .with_label_values(&["bulk", "415"])
                    .inc();
                return ApiError::response(
                    StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    "unsupported_media_type",
                    "Content-Type must be application/json or application/x-ndjson",
                )
                .into_response();
            }
        }
    }

    let lines: Vec<&str> = body.lines().filter(|l| !l.trim().is_empty()).collect();
    let mut items: Vec<ClusterBulkItem> = Vec::new();
    let mut has_errors = false;
    let mut accepted = 0usize;

    // One writer guard across the batch (the Mutex<Engine> analogue), so two
    // concurrent bulks don't interleave their per-item apply order.
    let _w = state.write_serial.lock();
    let cluster = state.cluster.read();

    let mut i = 0;
    while i < lines.len() {
        let action_line = lines[i];
        i += 1;

        let action: serde_json::Value = match serde_json::from_str(action_line) {
            Ok(v) => v,
            Err(e) => {
                has_errors = true;
                items.push(ClusterBulkItem {
                    index: ClusterBulkItemInner {
                        _id: 0,
                        status: 400,
                        error: Some(format!("invalid action JSON: {e}")),
                    },
                });
                if i < lines.len() {
                    i += 1;
                }
                continue;
            }
        };
        let id = extract_bulk_id(&action);

        if i >= lines.len() {
            has_errors = true;
            items.push(ClusterBulkItem {
                index: ClusterBulkItemInner {
                    _id: id.unwrap_or(0),
                    status: 400,
                    error: Some("missing source line after action".into()),
                },
            });
            break;
        }
        let source_line = lines[i];
        i += 1;

        let Some(id) = id else {
            has_errors = true;
            items.push(ClusterBulkItem {
                index: ClusterBulkItemInner {
                    _id: 0,
                    status: 400,
                    error: Some("could not extract _id from action".into()),
                },
            });
            continue;
        };

        let source: serde_json::Value = match serde_json::from_str(source_line) {
            Ok(v) => v,
            Err(e) => {
                has_errors = true;
                items.push(ClusterBulkItem {
                    index: ClusterBulkItemInner {
                        _id: id,
                        status: 400,
                        error: Some(format!("invalid source JSON: {e}")),
                    },
                });
                continue;
            }
        };
        let Some(query) = source.get("query").and_then(|v| v.as_str()) else {
            has_errors = true;
            items.push(ClusterBulkItem {
                index: ClusterBulkItemInner {
                    _id: id,
                    status: 400,
                    error: Some("missing or non-string 'query' field".into()),
                },
            });
            continue;
        };
        // A malformed tag value fails the ITEM loud (ADR-073), mirroring the
        // parse-error per-item contract — never ingest with silently fewer tags.
        let tags = match source.as_object().map(extract_ranked_ingest).transpose() {
            Ok(value) => value.unwrap_or_default().0,
            Err((error_type, msg)) => {
                has_errors = true;
                items.push(ClusterBulkItem {
                    index: ClusterBulkItemInner {
                        _id: id,
                        status: 400,
                        error: Some(format!("{error_type}: {msg}")),
                    },
                });
                continue;
            }
        };

        // Bulk carries no per-item version (parity with the single-node `_bulk` path,
        // which ingests at the default version 1); `PUT /_doc/{id}` is the versioned write.
        let (status, error) = match cluster.upsert_query_with_tags(id, query, 1, &tags) {
            Ok((removed, outcome)) => {
                let (status, _, error) = upsert_status(removed, &outcome);
                if status.is_success() {
                    accepted += 1;
                }
                (status.as_u16(), error)
            }
            Err(ShardError::PartiallyApplied {
                applied, failed, ..
            }) => {
                accepted += 1;
                (
                    200,
                    Some(format!(
                        "partial: applied on {applied:?}, pending on {failed:?}; \
                         POST /_cluster/resync converges it"
                    )),
                )
            }
            Err(e) => (503, Some(format!("write rejected: {e}"))),
        };
        // Any item carrying an error detail flips the top-level flag — including a
        // 200 "partial" (durably logged, repair queued): a client checking only
        // `errors` must see the degraded state (review finding), even though the
        // right reaction is a resync, not a retry.
        if !(200..300).contains(&status) || error.is_some() {
            has_errors = true;
        }
        items.push(ClusterBulkItem {
            index: ClusterBulkItemInner {
                _id: id,
                status,
                error,
            },
        });
    }
    drop(cluster);

    info!(accepted, items = items.len(), "cluster bulk complete");
    let took_ms = start.elapsed().as_secs_f64() * 1000.0;
    state
        .prom
        .http_requests_total
        .with_label_values(&["bulk", "200"])
        .inc();
    state
        .prom
        .http_request_duration
        .with_label_values(&["bulk"])
        .observe(start.elapsed().as_secs_f64());
    Json(ClusterBulkResponse {
        took_ms,
        errors: has_errors,
        items,
    })
    .into_response()
}
