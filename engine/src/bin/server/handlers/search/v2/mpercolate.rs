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

use axum::{
    extract::{
        rejection::{JsonRejection, QueryRejection},
        Query, State,
    },
    http::StatusCode,
    Json,
};
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
    extra: std::collections::BTreeMap<String, serde_json::Value>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V2MPercolateParams {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V2MPercolateBody {
    documents: Option<Vec<V2BatchDoc>>,
    filter: Option<serde_json::Value>,
    result_mode: Option<reverse_rusty::ResultMode>,
    query_scope: Option<reverse_rusty::QueryScope>,
    size: Option<usize>,
    track_total_hits_up_to: Option<u64>,
    /// ES/OS numeric threshold alias for `track_total_hits_up_to`.
    track_total_hits: Option<u64>,
    rank: Option<RankProgramBody>,
    include_source: Option<bool>,
    /// ES/OS spelling for `include_source`.
    #[serde(rename = "_source")]
    source: Option<bool>,
    timeout_ms: Option<u64>,
    /// ES/OS time value (`250ms`, `2s`, ...), equivalent to `timeout_ms`.
    timeout: Option<String>,
    // Named unsupported shapes produce a stable 400 rather than being ignored.
    explain: Option<bool>,
    #[serde(rename = "from")]
    page_from: Option<serde_json::Value>,
    cursor: Option<serde_json::Value>,
    /// ADR-113: PIT paging is single-title; the batch surface names the reject
    /// (previously an unknown `pit` key was silently ignored).
    pit: Option<serde_json::Value>,
    allow_partial_results: Option<bool>,
    /// ES/OS multi-search spelling for the same fail-closed control.
    allow_partial_search_results: Option<bool>,
    document: Option<serde_json::Value>,
    query: Option<serde_json::Value>,
}

#[derive(Serialize)]
struct V2SlotResponse {
    timed_out: bool,
    status: u16,
    _shards: Shards,
    hits: RankedHitsBody,
}

#[derive(Serialize)]
pub(crate) struct V2MPercolateResponse {
    /// ES/OS-compatible whole-millisecond batch duration.
    took: u64,
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

type SlotRows = (Vec<(u64, i64)>, reverse_rusty::TotalHits, usize);

/// The per-request batch parameters both kernels share.
struct BatchSpec<'a> {
    titles: &'a [String],
    options: reverse_rusty::TopKOptions,
    include_source: bool,
    enrichment_limit: usize,
    deadline: Instant,
}

/// The batch driver: shared bounded run + shared failure classification + the
/// batch success epilogue.
async fn drive_batch<S, E, F>(
    state: Arc<S>,
    started: Instant,
    options: reverse_rusty::TopKOptions,
    timeout: Duration,
    deadline: Instant,
    install_pool: bool,
    work: F,
) -> Result<Json<V2MPercolateResponse>, (StatusCode, Json<ApiError>)>
where
    S: RankedSearchCtx + Send + Sync + 'static,
    E: super::delivery::RankedBackendError + std::fmt::Display + Send + 'static,
    F: FnOnce() -> Result<BatchDelivered, DeliveryError<E>> + Send + 'static,
{
    let delivered = match run_bounded(&state, deadline, install_pool, work).await {
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
        took: took_ms.floor() as u64,
        took_ms,
        complete: true,
        query_scope: options.query_scope,
        responses: delivered
            .slots
            .into_iter()
            .map(|slot| V2SlotResponse {
                timed_out: false,
                status: StatusCode::OK.as_u16(),
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

type Reject = (StatusCode, Json<ApiError>);

fn request_rejection<S: RankedSearchCtx>(
    state: &S,
    status: StatusCode,
    error_type: &'static str,
    reason: String,
) -> Reject {
    state
        .prom()
        .http_requests_total
        .with_label_values(&["v2_mpercolate", status.as_str()])
        .inc();
    record_outcome(
        state.prom(),
        "validation",
        reverse_rusty::QueryScope::default(),
    );
    ApiError::response(status, error_type, reason)
}

fn body_rejection<S: RankedSearchCtx>(state: &S, error: &JsonRejection) -> Reject {
    let status = error.status();
    if status == StatusCode::PAYLOAD_TOO_LARGE || status == StatusCode::UNSUPPORTED_MEDIA_TYPE {
        let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
            "payload_too_large"
        } else {
            "unsupported_media_type"
        };
        return request_rejection(
            state,
            status,
            error_type,
            format!("invalid v2 mpercolate body: {error}"),
        );
    }
    request_rejection(
        state,
        StatusCode::BAD_REQUEST,
        "validation_error",
        format!("invalid v2 mpercolate body: {error}"),
    )
}

fn query_rejection<S: RankedSearchCtx>(state: &S, error: &QueryRejection) -> Reject {
    request_rejection(
        state,
        StatusCode::BAD_REQUEST,
        "validation_error",
        format!("invalid v2 mpercolate query parameters: {error}"),
    )
}

/// Strict HTTP extractor boundary for local `POST /v2/_mpercolate`.
#[instrument(skip_all)]
pub(crate) async fn v2_mpercolate_route(
    State(state): State<Arc<AppState>>,
    params: Result<Query<V2MPercolateParams>, QueryRejection>,
    body: Result<Json<V2MPercolateBody>, JsonRejection>,
) -> Result<Json<V2MPercolateResponse>, Reject> {
    let Query(_) = params.map_err(|error| query_rejection(&*state, &error))?;
    let Json(body) = body.map_err(|error| body_rejection(&*state, &error))?;
    v2_mpercolate_inner(state, body).await
}

/// Multi-document, local-only, exact bounded top-K per slot.
#[cfg(test)]
#[instrument(skip_all)]
pub(crate) async fn v2_mpercolate(
    State(state): State<Arc<AppState>>,
    Json(body): Json<V2MPercolateBody>,
) -> Result<Json<V2MPercolateResponse>, (StatusCode, Json<ApiError>)> {
    v2_mpercolate_inner(state, body).await
}

async fn v2_mpercolate_inner(
    state: Arc<AppState>,
    body: V2MPercolateBody,
) -> Result<Json<V2MPercolateResponse>, Reject> {
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
        return Err(validation("timeout is too large"));
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
    drive_batch(state, started, options, timeout, deadline, true, work).await
}

/// Strict HTTP extractor boundary for coordinator `POST /v2/_mpercolate`.
#[instrument(skip_all)]
pub(crate) async fn cluster_v2_mpercolate_route(
    State(state): State<Arc<ClusterAppState>>,
    params: Result<Query<V2MPercolateParams>, QueryRejection>,
    body: Result<Json<V2MPercolateBody>, JsonRejection>,
) -> Result<Json<V2MPercolateResponse>, Reject> {
    let Query(_) = params.map_err(|error| query_rejection(&*state, &error))?;
    let Json(body) = body.map_err(|error| body_rejection(&*state, &error))?;
    cluster_v2_mpercolate_inner(state, body).await
}

/// Coordinator-mode exact bounded batch: one call per involved shard, union
/// winner fetch, no partial response.
async fn cluster_v2_mpercolate_inner(
    state: Arc<ClusterAppState>,
    body: V2MPercolateBody,
) -> Result<Json<V2MPercolateResponse>, Reject> {
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
        return Err(validation("timeout is too large"));
    };
    let enrichment_limit = state.max_ranked_enrichment_bytes;
    let cluster_state = Arc::clone(&state);
    let mutation_fenced = include_source;
    let work = move || {
        let spec = BatchSpec {
            titles: &titles,
            options,
            include_source,
            enrichment_limit,
            deadline,
        };
        if mutation_fenced {
            // As on `/v2/_search`, source-enriched requests acquire both
            // mutation fences before entering Rayon so matching and the union
            // winner fetch cannot observe different same-ID versions.
            let _write_guard = cluster_state.write_serial.lock();
            let cluster = cluster_state.cluster.read();
            let stable_view = cluster.consistent_read_view();
            cluster_state
                .pool
                .install(|| cluster_batch_delivery(&stable_view, &program, &filter, &spec))
        } else {
            // Source-free bounded batches retain the fully concurrent path.
            let cluster = cluster_state.cluster.read();
            cluster_batch_delivery(&*cluster, &program, &filter, &spec)
        }
    };
    drive_batch(
        state,
        started,
        options,
        timeout,
        deadline,
        !mutation_fenced,
        work,
    )
    .await
}

mod delivery;
mod prepare;

use delivery::{cluster_batch_delivery, local_batch_delivery};
use prepare::{admit_batch_len, assemble_slots, prepare_batch};
