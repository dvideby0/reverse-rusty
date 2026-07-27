use std::collections::HashSet;

use axum::extract::{Query, State};

use super::{
    error, info, instrument, ApiError, AppState, Arc, BulkItem, BulkItemError, BulkItemInner,
    BulkResponse, Bytes, BytesRejection, HeaderMap, IngestItemStatus, Instant, IntoResponse, Json,
    QueryRejection, Response, StatusCode, CLASS_D_REJECT_MSG, QUERY_INDEX,
};

mod request;

pub(crate) use request::{
    parse_bulk_request, BulkActionKind, BulkParams, BulkRequestError, BulkSource, ParsedBulkItem,
};

type PreparedItem = (usize, BulkActionKind, u64, BulkSource);

fn item_inner(id: u64) -> BulkItemInner {
    BulkItemInner {
        index: QUERY_INDEX,
        id,
        version: None,
        result: None,
        status: 0,
        error: None,
    }
}

pub(crate) fn pending_item(action: BulkActionKind, id: u64) -> BulkItem {
    let inner = item_inner(id);
    match action {
        BulkActionKind::Index => BulkItem::Index(inner),
        BulkActionKind::Create => BulkItem::Create(inner),
    }
}

pub(crate) fn error_item(
    action: BulkActionKind,
    id: u64,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> BulkItem {
    let mut inner = item_inner(id);
    inner.status = status.as_u16();
    inner.error = Some(BulkItemError {
        error_type,
        reason: reason.into(),
    });
    match action {
        BulkActionKind::Index => BulkItem::Index(inner),
        BulkActionKind::Create => BulkItem::Create(inner),
    }
}

pub(crate) fn item_inner_mut(item: &mut BulkItem) -> &mut BulkItemInner {
    match item {
        BulkItem::Index(inner) | BulkItem::Create(inner) => inner,
    }
}

pub(crate) fn succeed_item(
    item: &mut BulkItem,
    status: StatusCode,
    version: u32,
    result: &'static str,
) {
    let inner = item_inner_mut(item);
    inner.status = status.as_u16();
    inner.version = Some(version);
    inner.result = Some(result);
    inner.error = None;
}

pub(crate) fn fail_item(
    item: &mut BulkItem,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) {
    let inner = item_inner_mut(item);
    inner.status = status.as_u16();
    inner.version = None;
    inner.result = None;
    inner.error = Some(BulkItemError {
        error_type,
        reason: reason.into(),
    });
}

pub(crate) fn bulk_rejection(
    prom: &crate::metrics::PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    prom.http_requests_total
        .with_label_values(&["bulk", status.as_str()])
        .inc();
    ApiError::response(status, error_type, reason).into_response()
}

pub(crate) fn bulk_query_rejection(
    prom: &crate::metrics::PrometheusMetrics,
    error: &QueryRejection,
) -> Response {
    bulk_rejection(
        prom,
        StatusCode::BAD_REQUEST,
        "validation_error",
        format!("invalid bulk query parameters: {error}"),
    )
}

pub(crate) fn bulk_body_rejection(
    prom: &crate::metrics::PrometheusMetrics,
    error: &BytesRejection,
) -> Response {
    let status = error.status();
    let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
        "payload_too_large"
    } else {
        "validation_error"
    };
    bulk_rejection(
        prom,
        status,
        error_type,
        format!("invalid bulk body: {error}"),
    )
}

fn request_rejection(
    prom: &crate::metrics::PrometheusMetrics,
    error: BulkRequestError,
) -> Response {
    bulk_rejection(prom, error.status, error.error_type, error.reason)
}

/// Strict HTTP boundary for `POST /_bulk`.
#[instrument(skip_all)]
pub(crate) async fn bulk_route(
    State(state): State<Arc<AppState>>,
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
        Err(error) => return request_rejection(&state.prom, error),
    };
    bulk_ingest_inner(&state, items)
}

fn classify_items(items: Vec<ParsedBulkItem>) -> (Vec<BulkItem>, Vec<PreparedItem>) {
    let mut responses = Vec::with_capacity(items.len());
    let mut prepared = Vec::new();
    for item in items {
        let slot = responses.len();
        match item.source {
            Ok(source) => {
                responses.push(pending_item(item.action, item.id));
                prepared.push((slot, item.action, item.id, source));
            }
            Err(error) => responses.push(error_item(
                item.action,
                item.id,
                StatusCode::BAD_REQUEST,
                error.error_type,
                error.reason,
            )),
        }
    }
    (responses, prepared)
}

fn can_use_fresh_batch(engine: &reverse_rusty::Engine, prepared: &[PreparedItem]) -> bool {
    let mut ids = HashSet::with_capacity(prepared.len());
    let snapshot = engine.snapshot();
    prepared.iter().all(|(_, _, id, source)| {
        source.version == 1 && ids.insert(*id) && !snapshot.has_live_query(*id)
    })
}

fn apply_fresh_batch(
    engine: &mut reverse_rusty::Engine,
    responses: &mut [BulkItem],
    prepared: &[PreparedItem],
) -> std::io::Result<bool> {
    let pairs: Vec<(u64, String)> = prepared
        .iter()
        .map(|(_, _, id, source)| (*id, source.query.clone()))
        .collect();
    let tags: Vec<Vec<(String, String)>> = prepared
        .iter()
        .map(|(_, _, _, source)| source.tags.clone())
        .collect();
    let ranks: Vec<Option<reverse_rusty::RankValues>> = prepared
        .iter()
        .map(|(_, _, _, source)| source.rank)
        .collect();
    let (report, outcomes) =
        engine.try_bulk_ingest_detailed_with_tags_and_ranks(&pairs, &tags, &ranks)?;
    for ((slot, _, _, source), outcome) in prepared.iter().zip(outcomes) {
        match outcome {
            IngestItemStatus::Ingested => {
                succeed_item(
                    &mut responses[*slot],
                    StatusCode::CREATED,
                    source.version,
                    "created",
                );
            }
            IngestItemStatus::RejectedParse(error) => {
                fail_item(
                    &mut responses[*slot],
                    StatusCode::BAD_REQUEST,
                    "parse_exception",
                    error.to_string(),
                );
            }
            IngestItemStatus::RejectedClassD => {
                fail_item(
                    &mut responses[*slot],
                    StatusCode::BAD_REQUEST,
                    "illegal_argument_exception",
                    CLASS_D_REJECT_MSG,
                );
            }
        }
    }
    info!(
        ingested = report.ingested,
        rejected_parse = report.rejected_parse,
        rejected_class_d = report.rejected_class_d,
        "bulk ingest complete through fresh-segment fast path"
    );
    // Even an all-rejected compiled batch can finalize or grow the feature
    // dictionary and commits an empty segment, so publish every successful
    // direct-batch transaction just as the previous handler did.
    Ok(true)
}

fn apply_one(
    engine: &mut reverse_rusty::Engine,
    response: &mut BulkItem,
    action: BulkActionKind,
    id: u64,
    source: &BulkSource,
) {
    if action == BulkActionKind::Create && engine.snapshot().has_live_query(id) {
        fail_item(
            response,
            StatusCode::CONFLICT,
            "version_conflict_engine_exception",
            format!("document {id} already exists; `create` requires a missing id"),
        );
        return;
    }

    let write = if action == BulkActionKind::Create {
        engine
            .try_insert_live_ranked(&source.query, id, source.version, &source.tags, source.rank)
            .map(|outcome| match outcome {
                reverse_rusty::segment::InsertOutcome::Inserted(_) => {
                    reverse_rusty::segment::UpsertOutcome::Created(0)
                }
                reverse_rusty::segment::InsertOutcome::RejectedClassD => {
                    reverse_rusty::segment::UpsertOutcome::RejectedClassD
                }
            })
    } else {
        engine.try_upsert_live_ranked(&source.query, id, source.version, &source.tags, source.rank)
    };

    match write {
        Ok(reverse_rusty::segment::UpsertOutcome::Created(_)) => {
            succeed_item(response, StatusCode::CREATED, source.version, "created");
        }
        Ok(reverse_rusty::segment::UpsertOutcome::Updated { .. }) => {
            succeed_item(response, StatusCode::OK, source.version, "updated");
        }
        Ok(reverse_rusty::segment::UpsertOutcome::RejectedClassD) => {
            fail_item(
                response,
                StatusCode::BAD_REQUEST,
                "illegal_argument_exception",
                CLASS_D_REJECT_MSG,
            );
        }
        Err(reverse_rusty::WriteError::Parse(error)) => {
            fail_item(
                response,
                StatusCode::BAD_REQUEST,
                "parse_exception",
                error.to_string(),
            );
        }
        Err(reverse_rusty::WriteError::Wal(error)) => {
            error!(query_id = id, error = %error, "bulk item WAL append failed");
            fail_item(
                response,
                StatusCode::SERVICE_UNAVAILABLE,
                "persistence_unavailable",
                format!("bulk item could not be durably recorded: {error}"),
            );
        }
    }
}

fn bulk_ingest_inner(state: &Arc<AppState>, items: Vec<ParsedBulkItem>) -> Response {
    let start = Instant::now();
    let (mut responses, prepared) = classify_items(items);
    let mut published = false;
    if !prepared.is_empty() {
        let result = {
            let mut engine = state.engine.lock();
            if can_use_fresh_batch(&engine, &prepared) {
                apply_fresh_batch(&mut engine, &mut responses, &prepared)
            } else {
                for (slot, action, id, source) in &prepared {
                    apply_one(&mut engine, &mut responses[*slot], *action, *id, source);
                }
                // Rejected class-D compiles and WAL failures can still update
                // diagnostic/dictionary health in the engine. Publish once
                // after every completed ordered pass, matching PUT /_doc.
                Ok(true)
            }
        };
        match result {
            Ok(changed) => {
                if changed {
                    state.publish_snapshot();
                    published = true;
                }
            }
            Err(error) => {
                error!(error = %error, "bulk ingest persistence failed, batch rolled back");
                return bulk_rejection(
                    &state.prom,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "persistence_unavailable",
                    format!("bulk ingest could not be durably persisted: {error}"),
                );
            }
        }
    }

    let errors = responses
        .iter_mut()
        .any(|item| item_inner_mut(item).error.is_some());
    let took_ms = start.elapsed().as_secs_f64() * 1000.0;
    info!(
        items = responses.len(),
        errors, published, "bulk request complete"
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
