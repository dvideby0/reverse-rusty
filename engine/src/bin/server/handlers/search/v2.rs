//! Local bounded ranked percolation (`POST /v2/_search`, ADR-107/108).

use std::cell::RefCell;
use std::sync::Arc;
use std::time::Instant;

use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use tracing::{info, instrument, warn};

use crate::dto::{ApiError, HitSource};
use crate::state::AppState;

use super::resolve::resolve_percolate;
use super::DocBody;

thread_local! {
    static RANKED_SCRATCH: RefCell<reverse_rusty::segment::MatchScratch> =
        RefCell::new(reverse_rusty::segment::MatchScratch::new());
}

#[derive(Deserialize)]
struct BoostBody {
    key: String,
    value: String,
    boost: i64,
}

#[derive(Deserialize)]
struct RankProgramBody {
    priority_field: Option<String>,
    #[serde(default)]
    boosts: Vec<BoostBody>,
}

impl RankProgramBody {
    fn into_spec(self) -> reverse_rusty::RankProgramSpec {
        reverse_rusty::RankProgramSpec {
            priority_field: Some(
                self.priority_field
                    .unwrap_or_else(|| "priority".to_string()),
            ),
            boosts: self
                .boosts
                .into_iter()
                .map(|boost| (boost.key, boost.value, boost.boost))
                .collect(),
        }
    }
}

#[derive(Deserialize)]
pub(crate) struct V2SearchBody {
    document: Option<DocBody>,
    filter: Option<serde_json::Value>,
    result_mode: Option<reverse_rusty::ResultMode>,
    query_scope: Option<reverse_rusty::QueryScope>,
    size: Option<usize>,
    track_total_hits_up_to: Option<u64>,
    rank: Option<RankProgramBody>,
    include_source: Option<bool>,
    explain: Option<bool>,
    allow_partial_results: Option<bool>,
    timeout_ms: Option<u64>,
    // Named unsupported shapes produce a stable 400 rather than being ignored.
    #[serde(rename = "from")]
    page_from: Option<serde_json::Value>,
    cursor: Option<serde_json::Value>,
    documents: Option<serde_json::Value>,
    query: Option<serde_json::Value>,
}

#[derive(Serialize)]
struct Shards {
    total: u8,
    successful: u8,
    failed: u8,
}

#[derive(Serialize)]
struct RankedHitBody {
    _id: u64,
    _score: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    _source: Option<HitSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    _explanation: Option<reverse_rusty::ExplainDetail>,
}

#[derive(Serialize)]
struct RankedHitsBody {
    total: reverse_rusty::TotalHits,
    hits: Vec<RankedHitBody>,
}

#[derive(Serialize)]
pub(crate) struct V2SearchResponse {
    took_ms: f64,
    complete: bool,
    query_scope: reverse_rusty::QueryScope,
    _shards: Shards,
    hits: RankedHitsBody,
}

fn validation(reason: impl Into<String>) -> (StatusCode, Json<ApiError>) {
    ApiError::response(StatusCode::BAD_REQUEST, "validation_error", reason)
}

fn scope_label(scope: reverse_rusty::QueryScope) -> &'static str {
    match scope {
        reverse_rusty::QueryScope::Standard => "standard",
        reverse_rusty::QueryScope::WithBroad => "with_broad",
    }
}

fn record_outcome(state: &AppState, outcome: &'static str, scope: reverse_rusty::QueryScope) {
    state
        .prom
        .ranked_requests_total
        .with_label_values(&[outcome, scope_label(scope)])
        .inc();
}

/// One-document, local-only, exact bounded top-K search.
#[instrument(skip_all)]
pub(crate) async fn v2_search(
    State(state): State<Arc<AppState>>,
    Json(body): Json<V2SearchBody>,
) -> Result<Json<V2SearchResponse>, (StatusCode, Json<ApiError>)> {
    let started = Instant::now();
    let requested_scope = body.query_scope.unwrap_or_default();
    if body.page_from.is_some()
        || body.cursor.is_some()
        || body.documents.is_some()
        || body.query.is_some()
    {
        record_outcome(&state, "validation", requested_scope);
        return Err(validation(
            "v2 ranked search accepts one `document`; from, cursor, documents and query are not supported",
        ));
    }
    if body.allow_partial_results == Some(true) {
        record_outcome(&state, "validation", requested_scope);
        return Err(validation(
            "allow_partial_results=true is not supported for exact local top_k",
        ));
    }
    let mode = body.result_mode.unwrap_or_default();
    if mode != reverse_rusty::ResultMode::TopK {
        record_outcome(&state, "validation", requested_scope);
        return Err(ApiError::response(
            StatusCode::BAD_REQUEST,
            "unsupported_result_mode",
            "Increment 2 supports result_mode=top_k only",
        ));
    }
    let Some(document) = body.document else {
        record_outcome(&state, "validation", requested_scope);
        return Err(validation("request must include one `document`"));
    };
    let title = document.title.clone();
    let (_, _, filter_spec) = match resolve_percolate(Some(document), None, body.filter, None) {
        Ok(value) => value,
        Err(reason) => {
            record_outcome(&state, "validation", requested_scope);
            return Err(validation(reason));
        }
    };

    let options = reverse_rusty::TopKOptions {
        size: body.size.unwrap_or(reverse_rusty::DEFAULT_TOP_K),
        track_total_hits_up_to: body
            .track_total_hits_up_to
            .unwrap_or(reverse_rusty::DEFAULT_TRACK_TOTAL_HITS_UP_TO),
        query_scope: requested_scope,
    };
    if options.size > reverse_rusty::MAX_TOP_K {
        state
            .prom
            .rank_admission_rejections_total
            .with_label_values(&["size"])
            .inc();
        record_outcome(&state, "admission", options.query_scope);
        return Err(ApiError::response(
            StatusCode::BAD_REQUEST,
            "rank_admission_rejected",
            format!(
                "size {} exceeds maximum {}",
                options.size,
                reverse_rusty::MAX_TOP_K
            ),
        ));
    }
    if options.track_total_hits_up_to > reverse_rusty::DEFAULT_TRACK_TOTAL_HITS_UP_TO {
        state
            .prom
            .rank_admission_rejections_total
            .with_label_values(&["total_threshold"])
            .inc();
        record_outcome(&state, "admission", options.query_scope);
        return Err(ApiError::response(
            StatusCode::BAD_REQUEST,
            "rank_admission_rejected",
            format!(
                "track_total_hits_up_to {} exceeds maximum {}",
                options.track_total_hits_up_to,
                reverse_rusty::DEFAULT_TRACK_TOTAL_HITS_UP_TO
            ),
        ));
    }

    let snap = Arc::clone(&state.snapshot.load());
    let raw_program = body
        .rank
        .map(RankProgramBody::into_spec)
        .unwrap_or_default();
    let program = match snap.compile_rank_program(&raw_program) {
        Ok(program) => program,
        Err(error) => {
            record_outcome(&state, "validation", options.query_scope);
            return Err(ApiError::response(
                StatusCode::BAD_REQUEST,
                "unsupported_rank_field",
                error.to_string(),
            ));
        }
    };
    let predicate = snap.compile_tag_predicate(&filter_spec);
    let enrichment_snapshot = Arc::clone(&snap);
    let include_source = body.include_source.unwrap_or(true);
    let include_explain = body.explain.unwrap_or(false);
    let timeout = std::time::Duration::from_millis(body.timeout_ms.unwrap_or(5_000));
    // Arm only after decode/validation and include both permit queuing and compute.
    let deadline = Instant::now() + timeout;
    let permit_sem = Arc::clone(&state.ranked_search_permits);
    let state_inner = Arc::clone(&state);
    let title_for_match = title.clone();

    let work = async move {
        let permit = crate::state::acquire_search_permit(
            Some(&permit_sem),
            &state_inner.prom.ranked_search_permits_in_use,
        )
        .await;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            state_inner.pool.install(|| {
                RANKED_SCRATCH.with(|cell| {
                    snap.try_match_title_top_k(
                        &title_for_match,
                        options,
                        &program,
                        &predicate,
                        &mut cell.borrow_mut(),
                        Some(deadline),
                    )
                })
            })
        })
        .await
    };

    let ranked = match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), work).await
    {
        Ok(Ok(Ok(result))) => result,
        Ok(Ok(Err(reverse_rusty::RankedMatchError::Cancelled(_)))) | Err(_) => {
            state
                .prom
                .http_requests_total
                .with_label_values(&["v2_search", "408"])
                .inc();
            record_outcome(&state, "timeout", options.query_scope);
            let elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0;
            if elapsed_ms >= state.slow_query_threshold_ms as f64 {
                state.prom.slow_queries_total.inc();
            }
            warn!(
                k = options.size,
                scope = ?options.query_scope,
                relation = "unknown",
                candidates = "unknown",
                rank_time_ms = elapsed_ms,
                cancellation = "deadline",
                "v2 ranked search timed out"
            );
            return Err(ApiError::response(
                StatusCode::REQUEST_TIMEOUT,
                "timeout",
                format!("ranked search timed out after {}ms", timeout.as_millis()),
            ));
        }
        Ok(Ok(Err(reverse_rusty::RankedMatchError::Admission(error)))) => {
            state
                .prom
                .rank_admission_rejections_total
                .with_label_values(&["core"])
                .inc();
            record_outcome(&state, "admission", options.query_scope);
            return Err(ApiError::response(
                StatusCode::BAD_REQUEST,
                "rank_admission_rejected",
                error.to_string(),
            ));
        }
        Ok(Err(error)) => {
            record_outcome(&state, "error", options.query_scope);
            return Err(ApiError::response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "search_error",
                format!("ranked search task failed: {error}"),
            ));
        }
    };

    // Enrichment is winner-only and fail-closed: a requested missing source or
    // explanation invalidates the exact response rather than returning a
    // partially enriched winner list.
    let snap = enrichment_snapshot;
    let mut hits = Vec::with_capacity(ranked.hits.len());
    for hit in ranked.hits {
        let source = if include_source {
            let query = snap.get_query_source(hit.logical_id).ok_or_else(|| {
                record_outcome(&state, "error", options.query_scope);
                ApiError::response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "source_unavailable",
                    format!("source unavailable for ranked winner {}", hit.logical_id),
                )
            })?;
            state
                .prom
                .rank_source_bytes_total
                .inc_by(u64::try_from(query.len()).unwrap_or(u64::MAX));
            Some(HitSource { query })
        } else {
            None
        };
        let explanation = if include_explain {
            Some(snap.explain_hit(hit.logical_id, &title).ok_or_else(|| {
                record_outcome(&state, "error", options.query_scope);
                ApiError::response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "explanation_unavailable",
                    format!(
                        "explanation unavailable for ranked winner {}",
                        hit.logical_id
                    ),
                )
            })?)
        } else {
            None
        };
        hits.push(RankedHitBody {
            _id: hit.logical_id,
            _score: hit.score,
            _source: source,
            _explanation: explanation,
        });
    }

    let took_ms = started.elapsed().as_secs_f64() * 1_000.0;
    state
        .prom
        .http_requests_total
        .with_label_values(&["v2_search", "200"])
        .inc();
    record_outcome(&state, "success", options.query_scope);
    state
        .prom
        .rank_evaluations_total
        .inc_by(ranked.rank_stats.evaluations);
    state
        .prom
        .rank_heap_replacements_total
        .inc_by(ranked.rank_stats.heap_replacements);
    state
        .prom
        .rank_total_relation_total
        .with_label_values(&[match ranked.total_hits.relation {
            reverse_rusty::TotalHitsRelation::Eq => "eq",
            reverse_rusty::TotalHitsRelation::Gte => "gte",
        }])
        .inc();
    state
        .prom
        .rank_true_match_lower_bound_total
        .inc_by(ranked.total_hits.value);
    state
        .prom
        .http_request_duration
        .with_label_values(&["v2_search"])
        .observe(started.elapsed().as_secs_f64());
    if took_ms >= state.slow_query_threshold_ms as f64 {
        state.prom.slow_queries_total.inc();
        info!(
            k = options.size,
            scope = ?options.query_scope,
            relation = ?ranked.total_hits.relation,
            candidates = ranked.stats.unique_candidates,
            rank_time_ms = took_ms,
            cancellation = "none",
            "slow v2 ranked search"
        );
    }
    Ok(Json(V2SearchResponse {
        took_ms,
        complete: true,
        query_scope: options.query_scope,
        _shards: Shards {
            total: 1,
            successful: 1,
            failed: 0,
        },
        hits: RankedHitsBody {
            total: ranked.total_hits,
            hits,
        },
    }))
}
