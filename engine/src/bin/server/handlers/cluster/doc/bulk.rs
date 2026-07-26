use super::{
    extract_bulk_id, extract_ranked_ingest, info, instrument, upsert_status, ApiError, Arc,
    ClusterAppState, ClusterBulkItem, ClusterBulkItemInner, ClusterBulkResponse, HeaderMap,
    Instant, IntoResponse, Json, Response, ShardError, State, StatusCode,
};

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
