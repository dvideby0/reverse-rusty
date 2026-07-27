use super::super::super::controls::parse_time_value;
use super::super::one_alias;
use super::{
    record_outcome, resolve_percolate, validation, ApiError, DeliveryError, Duration, HitSource,
    Instant, Json, PrepareFailure, PreparedBatch, RankProgramBody, RankedHitBody, SlotDelivered,
    SlotRows, StatusCode, V2MPercolateBody,
};

pub(super) fn prepare_batch(body: V2MPercolateBody) -> Result<PreparedBatch, PrepareFailure> {
    if body.explain == Some(true) {
        return Err(PrepareFailure::Validation(validation(
            "explain=true is not supported on /v2/_mpercolate; use /v2/_search per document",
        )));
    }
    let allow_partial_results = one_alias(
        body.allow_partial_results,
        body.allow_partial_search_results,
        "allow_partial_results` and `allow_partial_search_results",
    )
    .map_err(|reason| PrepareFailure::Validation(validation(reason)))?;
    if allow_partial_results == Some(true) {
        return Err(PrepareFailure::Validation(validation(
            "partial results are not supported for exact top_k",
        )));
    }
    if body.page_from.is_some()
        || body.cursor.is_some()
        || body.pit.is_some()
        || body.document.is_some()
        || body.query.is_some()
    {
        return Err(PrepareFailure::Validation(validation(
            "v2 batch percolate accepts `documents`; from, cursor, pit, document and query are not \
             supported — page per title via /v2/_search",
        )));
    }
    if body.result_mode.unwrap_or_default() != reverse_rusty::ResultMode::TopK {
        return Err(PrepareFailure::Validation(ApiError::response(
            StatusCode::BAD_REQUEST,
            "unsupported_result_mode",
            "v2 batch percolate supports result_mode=top_k only",
        )));
    }
    // A MISSING field must 400 (a misspelled request must not look like a
    // successful empty batch); an explicit `documents: []` stays the 200 no-op.
    let Some(documents) = body.documents else {
        return Err(PrepareFailure::Validation(validation(
            "request must include 'documents'",
        )));
    };
    for (index, document) in documents.iter().enumerate() {
        if let Some(key) = document.extra.keys().next() {
            return Err(PrepareFailure::Validation(validation(format!(
                "documents[{index}] carries unsupported per-document option `{key}`;                  options are batch-wide on /v2/_mpercolate"
            ))));
        }
    }
    let documents: Vec<super::super::DocBody> = documents
        .into_iter()
        .map(|document| super::super::DocBody {
            title: document.title,
        })
        .collect();
    let (titles, _, filter) = resolve_percolate(None, Some(documents), body.filter, None)
        .map_err(|reason| PrepareFailure::Validation(validation(reason)))?;
    let track_total_hits_up_to = one_alias(
        body.track_total_hits_up_to,
        body.track_total_hits,
        "track_total_hits_up_to` and `track_total_hits",
    )
    .map_err(|reason| PrepareFailure::Validation(validation(reason)))?;
    let include_source = one_alias(
        body.include_source,
        body.source,
        "include_source` and `_source",
    )
    .map_err(|reason| PrepareFailure::Validation(validation(reason)))?
    .unwrap_or(true);
    let timeout = one_alias(
        body.timeout_ms.map(Duration::from_millis),
        body.timeout
            .as_deref()
            .map(parse_time_value)
            .transpose()
            .map_err(|reason| PrepareFailure::Validation(validation(reason)))?,
        "timeout_ms` and `timeout",
    )
    .map_err(|reason| PrepareFailure::Validation(validation(reason)))?
    .unwrap_or(Duration::from_secs(30));
    let options = reverse_rusty::TopKOptions {
        search_after: None,
        size: body.size.unwrap_or(reverse_rusty::DEFAULT_TOP_K),
        track_total_hits_up_to: track_total_hits_up_to
            .unwrap_or(reverse_rusty::DEFAULT_TRACK_TOTAL_HITS_UP_TO),
        query_scope: body.query_scope.unwrap_or_default(),
    };
    if options.size > reverse_rusty::MAX_TOP_K {
        return Err(PrepareFailure::Admission(
            "size",
            ApiError::response(
                StatusCode::BAD_REQUEST,
                "rank_admission_rejected",
                format!(
                    "size {} exceeds maximum {}",
                    options.size,
                    reverse_rusty::MAX_TOP_K
                ),
            ),
        ));
    }
    if options.track_total_hits_up_to > reverse_rusty::DEFAULT_TRACK_TOTAL_HITS_UP_TO {
        return Err(PrepareFailure::Admission(
            "total_threshold",
            ApiError::response(
                StatusCode::BAD_REQUEST,
                "rank_admission_rejected",
                format!(
                    "track_total_hits_up_to {} exceeds maximum {}",
                    options.track_total_hits_up_to,
                    reverse_rusty::DEFAULT_TRACK_TOTAL_HITS_UP_TO
                ),
            ),
        ));
    }
    Ok(PreparedBatch {
        titles,
        filter,
        options,
        rank: body
            .rank
            .map(RankProgramBody::into_spec)
            .unwrap_or_default(),
        include_source,
        timeout,
    })
}

/// HTTP-layer batch-size admission: the operator-facing dynamic knob (the v1
/// `/_mpercolate` bound) composed with the ADR-112 lean-core ceiling. The
/// aggregate `size × titles` heap budget is enforced by the core entry points.
pub(super) fn admit_batch_len(
    prom: &crate::metrics::PrometheusMetrics,
    scope: reverse_rusty::QueryScope,
    titles: usize,
    max_percolate_batch: usize,
) -> Result<(), (StatusCode, Json<ApiError>)> {
    let max = max_percolate_batch.min(reverse_rusty::MAX_RANKED_BATCH_TITLES);
    if titles > max {
        prom.rank_admission_rejections_total
            .with_label_values(&["batch_titles"])
            .inc();
        record_outcome(prom, "admission", scope);
        return Err(ApiError::response(
            StatusCode::BAD_REQUEST,
            "rank_admission_rejected",
            format!("batch of {titles} documents exceeds maximum {max}"),
        ));
    }
    Ok(())
}

/// Build the per-slot hit bodies from bounded rows + the deduped source map.
pub(super) fn assemble_slots<E>(
    slots: Vec<SlotRows>,
    include_source: bool,
    sources: &std::collections::HashMap<u64, String>,
    deadline: Instant,
) -> Result<Vec<SlotDelivered>, DeliveryError<E>> {
    let mut out = Vec::with_capacity(slots.len());
    for (rows, total_hits, routed_shards) in slots {
        if Instant::now() >= deadline {
            return Err(DeliveryError::Deadline);
        }
        let mut hits = Vec::with_capacity(rows.len());
        for (logical_id, score) in rows {
            let source = if include_source {
                Some(
                    sources
                        .get(&logical_id)
                        .cloned()
                        .map(|query| HitSource { query })
                        .ok_or(DeliveryError::SourceUnavailable(logical_id))?,
                )
            } else {
                None
            };
            hits.push(RankedHitBody {
                _id: logical_id,
                _score: score,
                _source: source,
                _explanation: None,
            });
        }
        out.push(SlotDelivered {
            hits,
            total_hits,
            routed_shards,
        });
    }
    Ok(out)
}
