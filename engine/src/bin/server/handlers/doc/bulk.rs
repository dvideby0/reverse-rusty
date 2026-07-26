use super::{
    error, extract_ranked_ingest, info, instrument, ApiError, AppState, Arc, BulkItem,
    BulkItemInner, BulkResponse, HeaderMap, IngestItemStatus, Instant, IntoResponse, Json, State,
    StatusCode, CLASS_D_REJECT_MSG,
};

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
    let mut ranks_per_pair: Vec<Option<reverse_rusty::RankValues>> = Vec::new();
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

        // A malformed tag value fails the ITEM loud (ADR-073), mirroring the
        // parse-error per-item contract — never ingest with silently fewer tags.
        let (tags, rank) = match source.as_object().map(extract_ranked_ingest).transpose() {
            Ok(value) => value.unwrap_or_default(),
            Err((error_type, msg)) => {
                has_errors = true;
                items.push(BulkItem {
                    index: BulkItemInner {
                        _id: id,
                        status: 400,
                        error: Some(format!("{error_type}: {msg}")),
                    },
                });
                continue;
            }
        };

        pairs.push((id, query));
        tags_per_pair.push(tags);
        ranks_per_pair.push(rank);
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
            engine.try_bulk_ingest_detailed_with_tags_and_ranks(
                &pairs,
                &tags_per_pair,
                &ranks_per_pair,
            )
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
