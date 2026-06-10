//! `_doc` CRUD and `_bulk` ingest handlers: register, fetch, delete, and bulk-load
//! stored queries, plus the per-query metadata-tag extraction shared by the single
//! and bulk write paths (ADR-049).

use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info, instrument, warn};

use reverse_rusty::segment::IngestItemStatus;

use crate::dto::{ApiError, HitSource};
use crate::state::AppState;

/// The class-D rejection body, shared by the single-doc and bulk paths. Names the
/// opt-in lane (ADR-068) so an operator hitting the reject knows the way out.
pub(crate) const CLASS_D_REJECT_MSG: &str = "query has no anchorable feature (cost class D); \
     negation-only queries are stored as always-candidates when the accept_class_d \
     setting is enabled";

#[derive(Deserialize)]
pub(crate) struct PutDocBody {
    pub(crate) query: String,
    #[serde(default = "default_version")]
    pub(crate) version: u32,
    /// Per-query metadata tags (ADR-049): a canonical `tags` object plus any ES-style
    /// sibling fields (everything not named `query`/`version`/`tags`). See
    /// [`extract_ingest_tags`].
    #[serde(flatten)]
    pub(crate) rest: serde_json::Map<String, serde_json::Value>,
}
fn default_version() -> u32 {
    1
}

#[derive(Serialize)]
struct PutDocResponse {
    _id: u64,
    result: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

// -- GET /_doc/{id}
#[derive(Serialize)]
struct GetDocResponse {
    _id: u64,
    found: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    _source: Option<HitSource>,
}

// -- DELETE /_doc/{id}
#[derive(Serialize)]
struct DeleteDocResponse {
    _id: u64,
    result: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    deleted_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

// -- POST /_bulk
#[derive(Serialize)]
struct BulkResponse {
    took_ms: f64,
    errors: bool,
    items: Vec<BulkItem>,
}

#[derive(Serialize)]
struct BulkItem {
    index: BulkItemInner,
}

#[derive(Serialize)]
struct BulkItemInner {
    _id: u64,
    status: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[cfg(test)]
mod tests;

/// Reserved top-level fields on an ingest body that are NOT metadata tags.
const RESERVED_INGEST_FIELDS: [&str; 3] = ["query", "version", "tags"];

/// Extract per-query metadata tags from an ingest body's top-level fields (`PUT /_doc` or a
/// `/_bulk` source line), ES-style (ADR-049). Tags come from a canonical `tags` object
/// **and** any other non-reserved top-level scalar/array field (ES stores percolator
/// metadata as siblings of `query`). A value that is neither a string nor an array of
/// strings is ignored.
pub(crate) fn extract_ingest_tags(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    let mut push_kv = |key: &str, v: &serde_json::Value| match v {
        serde_json::Value::String(s) => out.push((key.to_string(), s.clone())),
        serde_json::Value::Array(arr) => {
            for e in arr {
                if let serde_json::Value::String(s) = e {
                    out.push((key.to_string(), s.clone()));
                }
            }
        }
        _ => {}
    };
    // canonical `tags` object
    if let Some(serde_json::Value::Object(tags)) = obj.get("tags") {
        for (k, v) in tags {
            push_kv(k, v);
        }
    }
    // ES-style sibling fields
    for (k, v) in obj {
        if !RESERVED_INGEST_FIELDS.contains(&k.as_str()) {
            push_kv(k, v);
        }
    }
    out
}

/// PUT /_doc/{id} — register or replace a single query. ES `index` semantics
/// (ADR-067): an atomic upsert — the new version is inserted and every prior
/// live copy of the id is tombstoned in ONE writer critical section, ONE WAL
/// frame, and ONE snapshot publish. A fresh id answers 201 `created`; a
/// replacement answers 200 `updated` (the ES status split). A rejected new
/// version (parse error or class D) leaves the prior version live and matchable.
#[instrument(skip(state, body), fields(query_id = id))]
pub(crate) async fn put_doc(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u64>,
    Json(body): Json<PutDocBody>,
) -> impl IntoResponse {
    let start = Instant::now();
    let tags = extract_ingest_tags(&body.rest);
    let result = {
        let mut engine = state.engine.lock();
        match engine.try_upsert_live_with_tags(&body.query, id, body.version, &tags) {
            Ok(reverse_rusty::segment::UpsertOutcome::Created(_)) => {
                info!(query_id = id, "query registered");
                state
                    .prom
                    .http_requests_total
                    .with_label_values(&["put_doc", "201"])
                    .inc();
                (
                    StatusCode::CREATED,
                    Json(PutDocResponse {
                        _id: id,
                        result: "created",
                        error: None,
                    }),
                )
            }
            Ok(reverse_rusty::segment::UpsertOutcome::Updated { replaced, .. }) => {
                info!(query_id = id, replaced, "query replaced");
                state
                    .prom
                    .http_requests_total
                    .with_label_values(&["put_doc", "200"])
                    .inc();
                (
                    StatusCode::OK,
                    Json(PutDocResponse {
                        _id: id,
                        result: "updated",
                        error: None,
                    }),
                )
            }
            Ok(reverse_rusty::segment::UpsertOutcome::RejectedClassD) => {
                warn!(query_id = id, "query rejected: cost class D");
                state
                    .prom
                    .http_requests_total
                    .with_label_values(&["put_doc", "400"])
                    .inc();
                (
                    StatusCode::BAD_REQUEST,
                    Json(PutDocResponse {
                        _id: id,
                        result: "rejected",
                        error: Some(CLASS_D_REJECT_MSG.into()),
                    }),
                )
            }
            Err(reverse_rusty::WriteError::Parse(e)) => {
                warn!(query_id = id, error = %e, "query parse error");
                state
                    .prom
                    .http_requests_total
                    .with_label_values(&["put_doc", "400"])
                    .inc();
                (
                    StatusCode::BAD_REQUEST,
                    Json(PutDocResponse {
                        _id: id,
                        result: "error",
                        error: Some(format!("parse error: {e}")),
                    }),
                )
            }
            Err(reverse_rusty::WriteError::Wal(e)) => {
                // Durability failure: the mutation was NOT applied. Never
                // acknowledge a write we couldn't log (see ADR-013). 503 tells
                // the client to retry — the engine state is unchanged.
                error!(query_id = id, error = %e, "WAL write failed, mutation rejected");
                state
                    .prom
                    .http_requests_total
                    .with_label_values(&["put_doc", "503"])
                    .inc();
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(PutDocResponse {
                        _id: id,
                        result: "error",
                        error: Some(format!("write-ahead log error: {e}")),
                    }),
                )
            }
        }
    };
    state.publish_snapshot();
    state
        .prom
        .http_request_duration
        .with_label_values(&["put_doc"])
        .observe(start.elapsed().as_secs_f64());
    result
}

/// GET /_doc/{id} — retrieve a stored query by logical ID.
#[instrument(skip(state), fields(query_id = id))]
pub(crate) async fn get_doc(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u64>,
) -> impl IntoResponse {
    let start = Instant::now();
    let snap = state.snapshot.load();
    let result = if let Some(query_text) = snap.get_query_source(id) {
        state
            .prom
            .http_requests_total
            .with_label_values(&["get_doc", "200"])
            .inc();
        (
            StatusCode::OK,
            Json(GetDocResponse {
                _id: id,
                found: true,
                _source: Some(HitSource { query: query_text }),
            }),
        )
    } else {
        state
            .prom
            .http_requests_total
            .with_label_values(&["get_doc", "404"])
            .inc();
        (
            StatusCode::NOT_FOUND,
            Json(GetDocResponse {
                _id: id,
                found: false,
                _source: None,
            }),
        )
    };
    state
        .prom
        .http_request_duration
        .with_label_values(&["get_doc"])
        .observe(start.elapsed().as_secs_f64());
    result
}

/// DELETE /_doc/{id} — remove a stored query by logical ID.
#[instrument(skip(state), fields(query_id = id))]
pub(crate) async fn delete_doc(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u64>,
) -> impl IntoResponse {
    let start = Instant::now();
    let deleted = {
        let mut engine = state.engine.lock();
        engine.delete_by_logical_id(id)
    };
    state.publish_snapshot();
    state
        .prom
        .http_request_duration
        .with_label_values(&["delete_doc"])
        .observe(start.elapsed().as_secs_f64());
    match deleted {
        Ok(n) if n > 0 => {
            info!(query_id = id, deleted = n, "query deleted");
            state
                .prom
                .http_requests_total
                .with_label_values(&["delete_doc", "200"])
                .inc();
            (
                StatusCode::OK,
                Json(DeleteDocResponse {
                    _id: id,
                    result: "deleted",
                    deleted_count: Some(n as u64),
                    error: None,
                }),
            )
        }
        Ok(_) => {
            state
                .prom
                .http_requests_total
                .with_label_values(&["delete_doc", "404"])
                .inc();
            (
                StatusCode::NOT_FOUND,
                Json(DeleteDocResponse {
                    _id: id,
                    result: "not_found",
                    deleted_count: None,
                    error: None,
                }),
            )
        }
        Err(e) => {
            // Tombstone WAL append failed: the delete was NOT applied. Reject
            // rather than acknowledge a delete we couldn't log (see ADR-013).
            error!(query_id = id, error = %e, "WAL write failed, delete rejected");
            state
                .prom
                .http_requests_total
                .with_label_values(&["delete_doc", "503"])
                .inc();
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(DeleteDocResponse {
                    _id: id,
                    result: "error",
                    deleted_count: None,
                    error: Some(format!("write-ahead log error: {e}")),
                }),
            )
        }
    }
}

/// POST /_bulk — NDJSON bulk ingest.
///
/// Format (ES-compatible):
///   {"index": {"_id": 123}}
///   {"query": "pokemon base set"}
///   {"index": {"_id": 456}}
///   {"query": "charizard holo"}
#[instrument(skip_all)]
pub(crate) async fn bulk_ingest(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: String,
) -> impl IntoResponse {
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

    // Parse NDJSON action/source pairs.
    let lines: Vec<&str> = body.lines().filter(|l| !l.trim().is_empty()).collect();
    let mut pairs: Vec<(u64, String)> = Vec::new();
    // Per-query metadata tags (ADR-049), parallel to `pairs`.
    let mut tags_per_pair: Vec<Vec<(String, String)>> = Vec::new();
    // For each entry in `pairs`, the index of its provisional item in `items`,
    // so the engine's per-item outcome can be mapped back to the right slot.
    let mut pair_item_idx: Vec<usize> = Vec::new();
    let mut items: Vec<BulkItem> = Vec::new();
    let mut has_errors = false;

    let mut i = 0;
    while i < lines.len() {
        let action_line = lines[i];
        i += 1;

        // Parse action: {"index": {"_id": N}} or just {"_id": N, ...}
        let action: serde_json::Value = match serde_json::from_str(action_line) {
            Ok(v) => v,
            Err(e) => {
                has_errors = true;
                items.push(BulkItem {
                    index: BulkItemInner {
                        _id: 0,
                        status: 400,
                        error: Some(format!("invalid action JSON: {e}")),
                    },
                });
                // Try to skip the source line too.
                if i < lines.len() {
                    i += 1;
                }
                continue;
            }
        };

        let id = extract_bulk_id(&action);

        // Next line is the source document.
        if i >= lines.len() {
            has_errors = true;
            items.push(BulkItem {
                index: BulkItemInner {
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
            items.push(BulkItem {
                index: BulkItemInner {
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
                items.push(BulkItem {
                    index: BulkItemInner {
                        _id: id,
                        status: 400,
                        error: Some(format!("invalid source JSON: {e}")),
                    },
                });
                continue;
            }
        };

        let query = if let Some(q) = source.get("query").and_then(|v| v.as_str()) {
            q.to_string()
        } else {
            has_errors = true;
            items.push(BulkItem {
                index: BulkItemInner {
                    _id: id,
                    status: 400,
                    error: Some("missing or non-string 'query' field".into()),
                },
            });
            continue;
        };

        pairs.push((id, query));
        tags_per_pair.push(
            source
                .as_object()
                .map(extract_ingest_tags)
                .unwrap_or_default(),
        );
        // Provisional success; the engine outcome (below) may downgrade this
        // item to a 400 once the batch is compiled.
        pair_item_idx.push(items.len());
        items.push(BulkItem {
            index: BulkItemInner {
                _id: id,
                status: 201,
                error: None,
            },
        });
    }

    // Ingest the valid pairs.
    if !pairs.is_empty() {
        let result = {
            let mut engine = state.engine.lock();
            engine.try_bulk_ingest_detailed_with_tags(&pairs, &tags_per_pair)
        };

        let (report, item_status) = match result {
            Ok(outcome) => {
                state.publish_snapshot();
                outcome
            }
            Err(e) => {
                // Durability failure: the batch was NOT committed (all-or-nothing,
                // ADR-017). 503 tells the client to retry — engine state is
                // unchanged, so no snapshot republish is needed.
                error!(error = %e, "bulk ingest persistence failed, batch rolled back");
                state
                    .prom
                    .http_requests_total
                    .with_label_values(&["bulk", "503"])
                    .inc();
                return ApiError::response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "persistence_unavailable",
                    format!("bulk ingest could not be durably persisted: {e}"),
                )
                .into_response();
            }
        };

        // Map each engine outcome back onto its provisional item. `item_status[k]`
        // describes `pairs[k]`, whose response slot is `pair_item_idx[k]`. Parse
        // and class-D rejections become per-item 400s (mirroring PUT /_doc), so a
        // caller can see exactly which queries were dropped and why.
        for (status, &slot) in item_status.iter().zip(pair_item_idx.iter()) {
            match status {
                IngestItemStatus::Ingested => {}
                IngestItemStatus::RejectedParse(e) => {
                    items[slot].index.status = 400;
                    items[slot].index.error = Some(format!("parse error: {e}"));
                    has_errors = true;
                }
                IngestItemStatus::RejectedClassD => {
                    items[slot].index.status = 400;
                    items[slot].index.error = Some(CLASS_D_REJECT_MSG.into());
                    has_errors = true;
                }
            }
        }

        info!(
            ingested = report.ingested,
            rejected_parse = report.rejected_parse,
            rejected_class_d = report.rejected_class_d,
            "bulk ingest complete"
        );
    }

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
    Json(BulkResponse {
        took_ms,
        errors: has_errors,
        items,
    })
    .into_response()
}

/// Extract _id from ES-style action line.
/// Accepts: {"index": {"_id": 123}} or {"_id": 123}
pub(crate) fn extract_bulk_id(action: &serde_json::Value) -> Option<u64> {
    // ES style: {"index": {"_id": N}}
    if let Some(inner) = action.get("index") {
        if let Some(id) = inner.get("_id").and_then(serde_json::Value::as_u64) {
            return Some(id);
        }
    }
    // Flat style: {"_id": N}
    if let Some(id) = action.get("_id").and_then(serde_json::Value::as_u64) {
        return Some(id);
    }
    // Also try "id" without underscore.
    action.get("id").and_then(serde_json::Value::as_u64)
}
