//! `POST /v2/_mpercolate` (ADR-112): the bounded ranked batch surface, in both
//! serving modes, over the shared v2 delivery seams.
//!
//! Mirrors the v1 `/_mpercolate` shape — ONE shared parameter set +
//! `documents[]` — with v2 slot semantics: per-slot exact top-K winners,
//! honest totals, optional winner `_source` under the ONE 16 MiB enrichment
//! credit (a cross-slot duplicate winner is fetched once, charged per
//! delivered occurrence), one permit, one absolute deadline, whole-batch 408.
//! `explain` stays on `/v2/_search` (per-(title, winner) explanation compile
//! is antithetical to the throughput path) and is a named 400 here.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::dto::{ApiError, HitSource};
use crate::state::{AppState, ClusterAppState};

use super::super::resolve::resolve_percolate;
use super::delivery::{
    failure_response, run_bounded, DeliveryError, DeliveryFailure, RankedSearchCtx,
};
use super::{
    prepare_failure, record_outcome, validation, PrepareFailure, RankProgramBody, RankedHitBody,
    RankedHitsBody, Shards,
};

/// Batch document DTO: unlike the permissive shared `DocBody`, unknown fields
/// are captured and rejected as a named 400 — the contract says per-document
/// options are unsupported, and silently discarding `{"title":x,"size":1}`
/// would apply the batch-wide K while looking honored (codex review).
#[derive(Deserialize)]
pub(crate) struct V2BatchDoc {
    title: String,
    #[serde(flatten)]
    extra: std::collections::HashMap<String, serde_json::Value>,
}

#[derive(Deserialize)]
pub(crate) struct V2MPercolateBody {
    documents: Option<Vec<V2BatchDoc>>,
    filter: Option<serde_json::Value>,
    result_mode: Option<reverse_rusty::ResultMode>,
    query_scope: Option<reverse_rusty::QueryScope>,
    size: Option<usize>,
    track_total_hits_up_to: Option<u64>,
    rank: Option<RankProgramBody>,
    include_source: Option<bool>,
    timeout_ms: Option<u64>,
    // Named unsupported shapes produce a stable 400 rather than being ignored.
    explain: Option<serde_json::Value>,
    #[serde(rename = "from")]
    page_from: Option<serde_json::Value>,
    cursor: Option<serde_json::Value>,
    allow_partial_results: Option<serde_json::Value>,
    document: Option<serde_json::Value>,
    query: Option<serde_json::Value>,
}

#[derive(Serialize)]
struct V2SlotResponse {
    _shards: Shards,
    hits: RankedHitsBody,
}

#[derive(Serialize)]
pub(crate) struct V2MPercolateResponse {
    took_ms: f64,
    complete: bool,
    query_scope: reverse_rusty::QueryScope,
    responses: Vec<V2SlotResponse>,
}

struct PreparedBatch {
    titles: Vec<String>,
    filter: Vec<(String, Vec<String>)>,
    options: reverse_rusty::TopKOptions,
    rank: reverse_rusty::RankProgramSpec,
    include_source: bool,
    timeout: Duration,
}

/// One slot's delivered rows before response assembly.
struct SlotDelivered {
    hits: Vec<RankedHitBody>,
    total_hits: reverse_rusty::TotalHits,
    routed_shards: usize,
}

struct BatchDelivered {
    slots: Vec<SlotDelivered>,
    rank_stats: reverse_rusty::RankStats,
    source_bytes: usize,
    shard_rows_received: usize,
    shard_result_bytes: u64,
}

fn prepare_batch(body: V2MPercolateBody) -> Result<PreparedBatch, PrepareFailure> {
    if body.explain.is_some() {
        return Err(PrepareFailure::Validation(validation(
            "explain is not supported on /v2/_mpercolate; use /v2/_search per document",
        )));
    }
    if body.page_from.is_some()
        || body.cursor.is_some()
        || body.allow_partial_results.is_some()
        || body.document.is_some()
        || body.query.is_some()
    {
        return Err(PrepareFailure::Validation(validation(
            "v2 batch percolate accepts `documents`; from, cursor, allow_partial_results, \
             document and query are not supported",
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
    let options = reverse_rusty::TopKOptions {
        search_after: None,
        size: body.size.unwrap_or(reverse_rusty::DEFAULT_TOP_K),
        track_total_hits_up_to: body
            .track_total_hits_up_to
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
        include_source: body.include_source.unwrap_or(true),
        timeout: Duration::from_millis(body.timeout_ms.unwrap_or(30_000)),
    })
}

/// HTTP-layer batch-size admission: the operator-facing dynamic knob (the v1
/// `/_mpercolate` bound) composed with the ADR-112 lean-core ceiling. The
/// aggregate `size × titles` heap budget is enforced by the core entry points.
fn admit_batch_len(
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

/// One slot's bounded rows before enrichment: `(rows, total, routed_shards)`.
type SlotRows = (Vec<(u64, i64)>, reverse_rusty::TotalHits, usize);

/// The per-request batch parameters both kernels share.
struct BatchSpec<'a> {
    titles: &'a [String],
    options: reverse_rusty::TopKOptions,
    include_source: bool,
    enrichment_limit: usize,
    deadline: Instant,
}

/// Build the per-slot hit bodies from bounded rows + the deduped source map.
fn assemble_slots<E>(
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

/// Single-node kernel: the columnar batch entry + distinct-winner enrichment
/// under the fail-closed budget, charged per DELIVERED occurrence (the same
/// rule as the cluster batch fetch).
fn local_batch_delivery(
    snap: &reverse_rusty::EngineSnapshot,
    program: &reverse_rusty::CompiledRankProgram,
    predicate: &reverse_rusty::exact::TagPredicate,
    spec: &BatchSpec<'_>,
) -> Result<BatchDelivered, DeliveryError<reverse_rusty::RankedMatchError>> {
    let BatchSpec {
        titles,
        options,
        include_source,
        enrichment_limit,
        deadline,
    } = *spec;
    let cfg = snap.config();
    let batch_opts = reverse_rusty::segment::BatchMatchOptions {
        include_broad: options.query_scope == reverse_rusty::QueryScope::WithBroad,
        broad_batch_size: cfg.broad_batch_size,
        broad_strategy: if cfg.broad_columnar {
            reverse_rusty::segment::BroadStrategy::Columnar
        } else {
            reverse_rusty::segment::BroadStrategy::Inline
        },
        broad_materialize: cfg.broad_materialize,
        broad_prefilter: cfg.broad_prefilter,
    };
    let ranked = snap
        .try_match_titles_batch_top_k(
            titles,
            batch_opts,
            options,
            program,
            predicate,
            Some(deadline),
        )
        .map_err(DeliveryError::Backend)?;
    let mut rank_stats = reverse_rusty::RankStats::default();
    let mut slots = Vec::with_capacity(ranked.titles.len());
    for title in &ranked.titles {
        rank_stats.evaluations = rank_stats
            .evaluations
            .saturating_add(title.rank_stats.evaluations);
        rank_stats.heap_replacements = rank_stats
            .heap_replacements
            .saturating_add(title.rank_stats.heap_replacements);
        slots.push((
            title
                .hits
                .iter()
                .map(|hit| (hit.logical_id, hit.score))
                .collect::<Vec<_>>(),
            title.total_hits,
            1usize,
        ));
    }
    let mut sources = std::collections::HashMap::new();
    let mut delivered = 0usize;
    if include_source {
        // The cluster batch-fetch rule, mirrored locally: each DISTINCT winner
        // is fetched once under the running credit, and every delivered
        // occurrence is then charged against the same limit (a source shared
        // by three slots spends three times its bytes).
        let mut fetch_remaining = enrichment_limit;
        for (rows, _, _) in &slots {
            for &(logical_id, _) in rows {
                if Instant::now() >= deadline {
                    return Err(DeliveryError::Deadline);
                }
                if sources.contains_key(&logical_id) {
                    continue;
                }
                let source = match snap.get_query_source_bounded(logical_id, fetch_remaining) {
                    Ok(Some(source)) => source,
                    Ok(None) => return Err(DeliveryError::SourceUnavailable(logical_id)),
                    Err(_over_credit) => return Err(DeliveryError::EnrichmentLimit),
                };
                fetch_remaining = fetch_remaining.saturating_sub(source.len());
                sources.insert(logical_id, source);
            }
        }
        for (rows, _, _) in &slots {
            for &(logical_id, _) in rows {
                let bytes = sources
                    .get(&logical_id)
                    .map(String::len)
                    .ok_or(DeliveryError::SourceUnavailable(logical_id))?;
                delivered = delivered.saturating_add(bytes);
                if delivered > enrichment_limit {
                    return Err(DeliveryError::EnrichmentLimit);
                }
            }
        }
    }
    let source_bytes = delivered;
    let slots = assemble_slots(slots, include_source, &sources, deadline)?;
    if Instant::now() >= deadline {
        return Err(DeliveryError::Deadline);
    }
    Ok(BatchDelivered {
        slots,
        rank_stats,
        source_bytes,
        shard_rows_received: 0,
        shard_result_bytes: 0,
    })
}

/// Coordinator kernel: the one-call-per-shard batch fan + the union winner
/// fetch under the same ONE credit.
fn cluster_batch_delivery(
    cluster: &reverse_rusty::cluster::ClusterEngine,
    program: &reverse_rusty::CompiledRankProgram,
    filter: &[(String, Vec<String>)],
    spec: &BatchSpec<'_>,
) -> Result<BatchDelivered, DeliveryError<reverse_rusty::cluster::ClusterRankedError>> {
    let BatchSpec {
        titles,
        options,
        include_source,
        enrichment_limit,
        deadline,
    } = *spec;
    let ranked = cluster
        .try_percolate_filtered_top_k_batch(titles, filter, options, program, Some(deadline))
        .map_err(DeliveryError::Backend)?;
    let per_slot_sources = if include_source {
        cluster
            .fetch_ranked_sources_batch_bounded(&ranked, enrichment_limit, Some(deadline))
            .map_err(|error| match error {
                reverse_rusty::cluster::ClusterRankedError::EnrichmentLimit { .. } => {
                    DeliveryError::EnrichmentLimit
                }
                other => DeliveryError::Backend(other),
            })?
    } else {
        Vec::new()
    };
    let source_bytes = per_slot_sources
        .iter()
        .flatten()
        .map(String::len)
        .fold(0usize, usize::saturating_add);
    let mut slots = Vec::with_capacity(ranked.titles.len());
    for (index, title) in ranked.titles.iter().enumerate() {
        if Instant::now() >= deadline {
            return Err(DeliveryError::Deadline);
        }
        let mut hits = Vec::with_capacity(title.hits.len());
        for (hit_index, hit) in title.hits.iter().enumerate() {
            let source = if include_source {
                Some(HitSource {
                    query: per_slot_sources
                        .get(index)
                        .and_then(|sources| sources.get(hit_index))
                        .cloned()
                        .ok_or(DeliveryError::SourceUnavailable(hit.logical_id))?,
                })
            } else {
                None
            };
            hits.push(RankedHitBody {
                _id: hit.logical_id,
                _score: hit.score,
                _source: source,
                _explanation: None,
            });
        }
        slots.push(SlotDelivered {
            hits,
            total_hits: title.total_hits,
            routed_shards: title.routed_shards,
        });
    }
    Ok(BatchDelivered {
        slots,
        rank_stats: ranked.rank_stats,
        source_bytes,
        shard_rows_received: ranked.shard_rows_received,
        shard_result_bytes: ranked.shard_result_bytes,
    })
}

/// The batch driver: shared bounded run + shared failure classification + the
/// batch success epilogue.
async fn drive_batch<S, E, F>(
    state: Arc<S>,
    started: Instant,
    options: reverse_rusty::TopKOptions,
    timeout: Duration,
    deadline: Instant,
    work: F,
) -> Result<Json<V2MPercolateResponse>, (StatusCode, Json<ApiError>)>
where
    S: RankedSearchCtx + Send + Sync + 'static,
    E: super::delivery::RankedBackendError + std::fmt::Display + Send + 'static,
    F: FnOnce() -> Result<BatchDelivered, DeliveryError<E>> + Send + 'static,
{
    let delivered = match run_bounded(&state, deadline, work).await {
        Ok(Ok(Ok(result))) => result,
        Ok(Ok(Err(error))) => {
            return Err(failure_response(
                &*state,
                started,
                options,
                timeout,
                "v2_mpercolate",
                DeliveryFailure::Error(error),
            ));
        }
        Ok(Err(join)) => {
            return Err(failure_response::<S, E>(
                &*state,
                started,
                options,
                timeout,
                "v2_mpercolate",
                DeliveryFailure::Join(join.to_string()),
            ));
        }
        Err(_) => {
            return Err(failure_response::<S, E>(
                &*state,
                started,
                options,
                timeout,
                "v2_mpercolate",
                DeliveryFailure::Elapsed,
            ));
        }
    };

    let took_ms = started.elapsed().as_secs_f64() * 1_000.0;
    let prom = state.prom();
    prom.http_requests_total
        .with_label_values(&["v2_mpercolate", "200"])
        .inc();
    record_outcome(prom, "success", options.query_scope);
    prom.rank_evaluations_total
        .inc_by(delivered.rank_stats.evaluations);
    prom.rank_heap_replacements_total
        .inc_by(delivered.rank_stats.heap_replacements);
    let mut total_values = 0u64;
    for slot in &delivered.slots {
        prom.rank_total_relation_total
            .with_label_values(&[match slot.total_hits.relation {
                reverse_rusty::TotalHitsRelation::Eq => "eq",
                reverse_rusty::TotalHitsRelation::Gte => "gte",
            }])
            .inc();
        total_values = total_values.saturating_add(slot.total_hits.value);
    }
    prom.rank_true_match_lower_bound_total.inc_by(total_values);
    prom.rank_source_bytes_total
        .inc_by(u64::try_from(delivered.source_bytes).unwrap_or(u64::MAX));
    prom.rank_shard_rows_received_total
        .inc_by(u64::try_from(delivered.shard_rows_received).unwrap_or(u64::MAX));
    prom.rank_shard_result_bytes_total
        .inc_by(delivered.shard_result_bytes);
    prom.http_request_duration
        .with_label_values(&["v2_mpercolate"])
        .observe(started.elapsed().as_secs_f64());
    if took_ms >= state.slow_query_threshold_ms() as f64 {
        prom.slow_queries_total.inc();
    }
    Ok(Json(V2MPercolateResponse {
        took_ms,
        complete: true,
        query_scope: options.query_scope,
        responses: delivered
            .slots
            .into_iter()
            .map(|slot| V2SlotResponse {
                _shards: Shards {
                    total: slot.routed_shards,
                    successful: slot.routed_shards,
                    failed: 0,
                },
                hits: RankedHitsBody {
                    total: slot.total_hits,
                    hits: slot.hits,
                },
            })
            .collect(),
    }))
}

/// Multi-document, local-only, exact bounded top-K per slot.
#[instrument(skip_all)]
pub(crate) async fn v2_mpercolate(
    State(state): State<Arc<AppState>>,
    Json(body): Json<V2MPercolateBody>,
) -> Result<Json<V2MPercolateResponse>, (StatusCode, Json<ApiError>)> {
    let started = Instant::now();
    let requested_scope = body.query_scope.unwrap_or_default();
    let prepared = match prepare_batch(body) {
        Ok(prepared) => prepared,
        Err(failure) => return Err(prepare_failure(&state.prom, failure, requested_scope)),
    };
    let PreparedBatch {
        titles,
        filter,
        options,
        rank: raw_program,
        include_source,
        timeout,
    } = prepared;

    let snap = Arc::clone(&state.snapshot.load());
    admit_batch_len(
        &state.prom,
        options.query_scope,
        titles.len(),
        snap.config().max_percolate_batch,
    )?;
    let program = match snap.compile_rank_program(&raw_program) {
        Ok(program) => program,
        Err(error) => {
            record_outcome(&state.prom, "validation", options.query_scope);
            return Err(ApiError::response(
                StatusCode::BAD_REQUEST,
                "unsupported_rank_field",
                error.to_string(),
            ));
        }
    };
    let predicate = snap.compile_tag_predicate(&filter);
    let Some(deadline) = Instant::now().checked_add(timeout) else {
        record_outcome(&state.prom, "validation", options.query_scope);
        return Err(validation("timeout_ms is too large"));
    };
    let enrichment_limit = state.max_ranked_enrichment_bytes;
    let work = move || {
        local_batch_delivery(
            &snap,
            &program,
            &predicate,
            &BatchSpec {
                titles: &titles,
                options,
                include_source,
                enrichment_limit,
                deadline,
            },
        )
    };
    drive_batch(state, started, options, timeout, deadline, work).await
}

/// Coordinator-mode exact bounded batch: one call per involved shard, union
/// winner fetch, no partial response.
#[instrument(skip_all)]
pub(crate) async fn cluster_v2_mpercolate(
    State(state): State<Arc<ClusterAppState>>,
    Json(body): Json<V2MPercolateBody>,
) -> Result<Json<V2MPercolateResponse>, (StatusCode, Json<ApiError>)> {
    let started = Instant::now();
    let requested_scope = body.query_scope.unwrap_or_default();
    let prepared = match prepare_batch(body) {
        Ok(prepared) => prepared,
        Err(failure) => return Err(prepare_failure(&state.prom, failure, requested_scope)),
    };
    let PreparedBatch {
        titles,
        filter,
        options,
        rank,
        include_source,
        timeout,
    } = prepared;
    let (program, max_batch) = {
        let cluster = state.cluster.read();
        let max_batch = cluster.per_shard_config().max_percolate_batch;
        match cluster.compile_rank_program(&rank) {
            Ok(program) => (program, max_batch),
            Err(error) => {
                record_outcome(&state.prom, "validation", options.query_scope);
                return Err(ApiError::response(
                    StatusCode::BAD_REQUEST,
                    "unsupported_rank_field",
                    error.to_string(),
                ));
            }
        }
    };
    admit_batch_len(&state.prom, options.query_scope, titles.len(), max_batch)?;
    let Some(deadline) = Instant::now().checked_add(timeout) else {
        record_outcome(&state.prom, "validation", options.query_scope);
        return Err(validation("timeout_ms is too large"));
    };
    let enrichment_limit = state.max_ranked_enrichment_bytes;
    let cluster_state = Arc::clone(&state);
    let work = move || {
        // The cluster read lock is taken INSIDE the blocking closure (never
        // across an await) and held for fan + fetch + assembly.
        let cluster = cluster_state.cluster.read();
        cluster_batch_delivery(
            &cluster,
            &program,
            &filter,
            &BatchSpec {
                titles: &titles,
                options,
                include_source,
                enrichment_limit,
                deadline,
            },
        )
    };
    drive_batch(state, started, options, timeout, deadline, work).await
}
