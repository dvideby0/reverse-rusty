use super::{
    bulk_body_rejection, bulk_query_rejection, bulk_rejection, error_item, fail_item, info,
    instrument, item_inner_mut, parse_bulk_request, pending_item, shard_error_status, succeed_item,
    Arc, BulkActionKind, BulkItem, BulkItemError, BulkParams, BulkResponse, Bytes, BytesRejection,
    ClusterAppState, HeaderMap, Instant, IntoResponse, Json, ParsedBulkItem, Query, QueryRejection,
    Response, ShardError, State, StatusCode,
};

/// Strict coordinator HTTP boundary for `POST /_bulk`.
#[instrument(skip_all)]
pub(crate) async fn cluster_bulk_route(
    State(state): State<Arc<ClusterAppState>>,
    params: Result<Query<BulkParams>, QueryRejection>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let _duration = state
        .prom
        .http_request_duration
        .with_label_values(&["bulk"])
        .start_timer();
    let Query(params) = match params {
        Ok(params) => params,
        Err(error) => return bulk_query_rejection(&state.prom, &error),
    };
    let body = match body {
        Ok(body) => body,
        Err(error) => return bulk_body_rejection(&state.prom, &error),
    };
    let items = match parse_bulk_request(&headers, &body, params) {
        Ok(items) => items,
        Err(error) => {
            return bulk_rejection(&state.prom, error.status, error.error_type, error.reason);
        }
    };
    cluster_bulk_inner(&state, items)
}

fn cluster_bulk_inner(state: &Arc<ClusterAppState>, items: Vec<ParsedBulkItem>) -> Response {
    let start = Instant::now();
    let mut responses: Vec<BulkItem> = Vec::with_capacity(items.len());
    let mut accepted = 0usize;

    // One writer guard across the batch (the Mutex<Engine> analogue), so two
    // concurrent bulks don't interleave their per-item apply order.
    let _write = state.write_serial.lock();
    let cluster = state.cluster.read();
    for item in items {
        let source = match item.source {
            Ok(source) => source,
            Err(error) => {
                responses.push(error_item(
                    item.action,
                    item.id,
                    StatusCode::BAD_REQUEST,
                    error.error_type,
                    error.reason,
                ));
                continue;
            }
        };
        let mut response = pending_item(item.action, item.id);
        let result = match item.action {
            BulkActionKind::Index => {
                cluster.upsert_query_with_tags(item.id, &source.query, source.version, &source.tags)
            }
            BulkActionKind::Create => cluster
                .create_query_with_tags(item.id, &source.query, source.version, &source.tags)
                .map(|outcome| (0, outcome)),
        };
        match result {
            Ok((removed, outcome)) => {
                let (status, result, error) = super::upsert_status(removed, &outcome);
                if status.is_success() {
                    accepted += 1;
                    succeed_item(&mut response, status, source.version, result);
                } else {
                    fail_item(
                        &mut response,
                        status,
                        if matches!(
                            outcome,
                            reverse_rusty::cluster::AddOutcome::RejectedParse(_)
                        ) {
                            "parse_exception"
                        } else {
                            "illegal_argument_exception"
                        },
                        error.unwrap_or_else(|| "bulk item was rejected".to_string()),
                    );
                }
            }
            Err(ShardError::DuplicateLogicalId(_)) if item.action == BulkActionKind::Create => {
                fail_item(
                    &mut response,
                    StatusCode::CONFLICT,
                    "version_conflict_engine_exception",
                    format!(
                        "document {} already exists; `create` requires a missing id",
                        item.id
                    ),
                );
            }
            Err(ShardError::PartiallyApplied {
                applied, failed, ..
            }) => {
                accepted += 1;
                succeed_item(&mut response, StatusCode::OK, source.version, "partial");
                let inner = item_inner_mut(&mut response);
                inner.error = Some(BulkItemError {
                    error_type: "partial_write",
                    reason: format!(
                        "applied on {applied:?}, pending on {failed:?}; durably logged — \
                         POST /_cluster/resync converges it"
                    ),
                });
            }
            Err(error) => {
                let status = shard_error_status(&error);
                fail_item(
                    &mut response,
                    status,
                    "cluster_write_error",
                    format!("write rejected: {error}"),
                );
            }
        }
        responses.push(response);
    }
    drop(cluster);

    let errors = responses.iter_mut().any(|item| {
        let inner = item_inner_mut(item);
        inner.error.is_some()
    });
    let took_ms = start.elapsed().as_secs_f64() * 1000.0;
    info!(
        accepted,
        items = responses.len(),
        errors,
        "cluster bulk complete"
    );
    state
        .prom
        .http_requests_total
        .with_label_values(&["bulk", "200"])
        .inc();
    Json(BulkResponse {
        took: took_ms.floor() as u64,
        took_ms,
        errors,
        items: responses,
    })
    .into_response()
}
