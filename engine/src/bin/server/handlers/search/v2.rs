//! Local bounded ranked percolation (`POST /v2/_search`, ADR-107/108).

use std::cell::RefCell;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use tracing::{info, instrument, warn};

use crate::dto::{ApiError, HitSource};
use crate::metrics::PrometheusMetrics;
use crate::state::{AppState, ClusterAppState};

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
    total: usize,
    successful: usize,
    failed: usize,
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

fn record_outcome(
    prom: &PrometheusMetrics,
    outcome: &'static str,
    scope: reverse_rusty::QueryScope,
) {
    prom.ranked_requests_total
        .with_label_values(&[outcome, scope_label(scope)])
        .inc();
}

struct PreparedSearch {
    title: String,
    filter: Vec<(String, Vec<String>)>,
    options: reverse_rusty::TopKOptions,
    rank: reverse_rusty::RankProgramSpec,
    include_source: bool,
    include_explain: bool,
    timeout: Duration,
}

enum PrepareFailure {
    Validation((StatusCode, Json<ApiError>)),
    Admission(&'static str, (StatusCode, Json<ApiError>)),
}

impl PrepareFailure {
    fn response(self) -> (StatusCode, Json<ApiError>) {
        match self {
            Self::Validation(response) | Self::Admission(_, response) => response,
        }
    }

    fn admission_reason(&self) -> Option<&'static str> {
        match self {
            Self::Admission(reason, _) => Some(reason),
            Self::Validation(_) => None,
        }
    }
}

/// Parse and validate the public v2 contract once for both local and cluster
/// serving. Keeping this lowering shared is what makes their defaults and 400s
/// identical as the delivery implementations evolve independently.
fn prepare(body: V2SearchBody) -> Result<PreparedSearch, PrepareFailure> {
    if body.page_from.is_some()
        || body.cursor.is_some()
        || body.documents.is_some()
        || body.query.is_some()
    {
        return Err(PrepareFailure::Validation(validation(
            "v2 ranked search accepts one `document`; from, cursor, documents and query are not supported",
        )));
    }
    if body.allow_partial_results == Some(true) {
        return Err(PrepareFailure::Validation(validation(
            "allow_partial_results=true is not supported for exact top_k",
        )));
    }
    if body.result_mode.unwrap_or_default() != reverse_rusty::ResultMode::TopK {
        return Err(PrepareFailure::Validation(ApiError::response(
            StatusCode::BAD_REQUEST,
            "unsupported_result_mode",
            "Increment 4 supports result_mode=top_k only",
        )));
    }
    let document = body.document.ok_or_else(|| {
        PrepareFailure::Validation(validation("request must include one `document`"))
    })?;
    let title = document.title.clone();
    let (_, _, filter) = resolve_percolate(Some(document), None, body.filter, None)
        .map_err(|reason| PrepareFailure::Validation(validation(reason)))?;
    let options = reverse_rusty::TopKOptions {
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
    Ok(PreparedSearch {
        title,
        filter,
        options,
        rank: body
            .rank
            .map(RankProgramBody::into_spec)
            .unwrap_or_default(),
        include_source: body.include_source.unwrap_or(true),
        include_explain: body.explain.unwrap_or(false),
        timeout: Duration::from_millis(body.timeout_ms.unwrap_or(5_000)),
    })
}

/// Fail-closed accounting for source text fetched during winner enrichment.
/// A source used by both `_source` and explain is charged once.
struct EnrichmentBudget {
    used: usize,
    limit: usize,
}

impl EnrichmentBudget {
    fn new(limit: usize) -> Self {
        Self { used: 0, limit }
    }

    fn charge(&mut self, bytes: usize) -> Result<(), ()> {
        let next = self.used.checked_add(bytes).ok_or(())?;
        if next > self.limit {
            return Err(());
        }
        self.used = next;
        Ok(())
    }

    fn remaining(&self) -> usize {
        self.limit.saturating_sub(self.used)
    }
}

struct DeliveryResult {
    hits: Vec<RankedHitBody>,
    total_hits: reverse_rusty::TotalHits,
    stats: reverse_rusty::segment::MatchStats,
    rank_stats: reverse_rusty::RankStats,
    routed_shards: usize,
    shard_rows_received: usize,
    shard_result_bytes: u64,
    source_bytes: usize,
}

enum DeliveryError {
    Local(reverse_rusty::RankedMatchError),
    Cluster(reverse_rusty::cluster::ClusterRankedError),
    SourceUnavailable(u64),
    ExplanationUnavailable(u64),
    EnrichmentLimit,
    Deadline,
}

fn deadline_expired(deadline: Instant) -> bool {
    Instant::now() >= deadline
}

/// One-document, local-only, exact bounded top-K search.
#[instrument(skip_all)]
pub(crate) async fn v2_search(
    State(state): State<Arc<AppState>>,
    Json(body): Json<V2SearchBody>,
) -> Result<Json<V2SearchResponse>, (StatusCode, Json<ApiError>)> {
    let started = Instant::now();
    let requested_scope = body.query_scope.unwrap_or_default();
    let prepared = match prepare(body) {
        Ok(prepared) => prepared,
        Err(failure) => {
            if let Some(reason) = failure.admission_reason() {
                state
                    .prom
                    .rank_admission_rejections_total
                    .with_label_values(&[reason])
                    .inc();
                record_outcome(&state.prom, "admission", requested_scope);
            } else {
                record_outcome(&state.prom, "validation", requested_scope);
            }
            return Err(failure.response());
        }
    };
    let PreparedSearch {
        title,
        filter,
        options,
        rank: raw_program,
        include_source,
        include_explain,
        timeout,
    } = prepared;

    let snap = Arc::clone(&state.snapshot.load());
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
    // Arm only after decode/validation and include both permit queuing and compute.
    let Some(deadline) = Instant::now().checked_add(timeout) else {
        record_outcome(&state.prom, "validation", options.query_scope);
        return Err(validation("timeout_ms is too large"));
    };
    let permit_sem = Arc::clone(&state.ranked_search_permits);
    let state_inner = Arc::clone(&state);
    let title_for_match = title.clone();
    let enrichment_limit = state.max_ranked_enrichment_bytes;

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
                    let ranked = snap
                        .try_match_title_top_k(
                            &title_for_match,
                            options,
                            &program,
                            &predicate,
                            &mut cell.borrow_mut(),
                            Some(deadline),
                        )
                        .map_err(DeliveryError::Local)?;
                    let mut budget = EnrichmentBudget::new(enrichment_limit);
                    let mut hits = Vec::with_capacity(ranked.hits.len());
                    for hit in ranked.hits {
                        if deadline_expired(deadline) {
                            return Err(DeliveryError::Deadline);
                        }
                        let source = if include_source || include_explain {
                            // Bounded lookup: the store checks its borrowed value
                            // against the REMAINING credit before cloning, so an
                            // over-limit winner 413s without ever allocating the
                            // full source `String` (codex review — peak-memory
                            // bound).
                            let source = match snap
                                .get_query_source_bounded(hit.logical_id, budget.remaining())
                            {
                                Ok(Some(source)) => source,
                                Ok(None) => {
                                    return Err(if include_explain && !include_source {
                                        DeliveryError::ExplanationUnavailable(hit.logical_id)
                                    } else {
                                        DeliveryError::SourceUnavailable(hit.logical_id)
                                    });
                                }
                                Err(_over_credit) => return Err(DeliveryError::EnrichmentLimit),
                            };
                            budget
                                .charge(source.len())
                                .map_err(|()| DeliveryError::EnrichmentLimit)?;
                            Some(source)
                        } else {
                            None
                        };
                        let explanation = if include_explain {
                            let source = source
                                .as_deref()
                                .ok_or(DeliveryError::ExplanationUnavailable(hit.logical_id))?;
                            Some(
                                snap.explain_source(hit.logical_id, source, &title_for_match)
                                    .ok_or(DeliveryError::ExplanationUnavailable(hit.logical_id))?,
                            )
                        } else {
                            None
                        };
                        hits.push(RankedHitBody {
                            _id: hit.logical_id,
                            _score: hit.score,
                            _source: if include_source {
                                source.map(|query| HitSource { query })
                            } else {
                                None
                            },
                            _explanation: explanation,
                        });
                    }
                    if deadline_expired(deadline) {
                        return Err(DeliveryError::Deadline);
                    }
                    Ok(DeliveryResult {
                        hits,
                        total_hits: ranked.total_hits,
                        stats: ranked.stats,
                        rank_stats: ranked.rank_stats,
                        routed_shards: 1,
                        shard_rows_received: 0,
                        shard_result_bytes: 0,
                        source_bytes: budget.used,
                    })
                })
            })
        })
        .await
    };

    let delivered = match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), work)
        .await
    {
        Ok(Ok(Ok(result))) => result,
        Ok(Ok(Err(
            DeliveryError::Local(reverse_rusty::RankedMatchError::Cancelled(_))
            | DeliveryError::Deadline,
        )))
        | Err(_) => {
            state
                .prom
                .http_requests_total
                .with_label_values(&["v2_search", "408"])
                .inc();
            record_outcome(&state.prom, "timeout", options.query_scope);
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
        Ok(Ok(Err(DeliveryError::Local(reverse_rusty::RankedMatchError::Admission(error))))) => {
            state
                .prom
                .rank_admission_rejections_total
                .with_label_values(&["core"])
                .inc();
            record_outcome(&state.prom, "admission", options.query_scope);
            return Err(ApiError::response(
                StatusCode::BAD_REQUEST,
                "rank_admission_rejected",
                error.to_string(),
            ));
        }
        Ok(Ok(Err(DeliveryError::SourceUnavailable(logical_id)))) => {
            record_outcome(&state.prom, "error", options.query_scope);
            return Err(ApiError::response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "source_unavailable",
                format!("source unavailable for ranked winner {logical_id}"),
            ));
        }
        Ok(Ok(Err(DeliveryError::ExplanationUnavailable(logical_id)))) => {
            record_outcome(&state.prom, "error", options.query_scope);
            return Err(ApiError::response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "explanation_unavailable",
                format!("explanation unavailable for ranked winner {logical_id}"),
            ));
        }
        Ok(Ok(Err(DeliveryError::EnrichmentLimit))) => {
            state.prom.rank_enrichment_rejections_total.inc();
            record_outcome(&state.prom, "enrichment_limit", options.query_scope);
            return Err(ApiError::response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "rank_enrichment_limit",
                format!(
                    "ranked winner enrichment exceeds {} bytes",
                    state.max_ranked_enrichment_bytes
                ),
            ));
        }
        Ok(Ok(Err(DeliveryError::Cluster(_)))) => {
            record_outcome(&state.prom, "error", options.query_scope);
            return Err(ApiError::response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "search_error",
                "unexpected local ranked delivery error",
            ));
        }
        Ok(Err(error)) => {
            record_outcome(&state.prom, "error", options.query_scope);
            return Err(ApiError::response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "search_error",
                format!("ranked search task failed: {error}"),
            ));
        }
    };

    let took_ms = started.elapsed().as_secs_f64() * 1_000.0;
    state
        .prom
        .http_requests_total
        .with_label_values(&["v2_search", "200"])
        .inc();
    record_outcome(&state.prom, "success", options.query_scope);
    state
        .prom
        .rank_evaluations_total
        .inc_by(delivered.rank_stats.evaluations);
    state
        .prom
        .rank_heap_replacements_total
        .inc_by(delivered.rank_stats.heap_replacements);
    state
        .prom
        .rank_total_relation_total
        .with_label_values(&[match delivered.total_hits.relation {
            reverse_rusty::TotalHitsRelation::Eq => "eq",
            reverse_rusty::TotalHitsRelation::Gte => "gte",
        }])
        .inc();
    state
        .prom
        .rank_true_match_lower_bound_total
        .inc_by(delivered.total_hits.value);
    state
        .prom
        .rank_source_bytes_total
        .inc_by(u64::try_from(delivered.source_bytes).unwrap_or(u64::MAX));
    state
        .prom
        .rank_shard_rows_received_total
        .inc_by(u64::try_from(delivered.shard_rows_received).unwrap_or(u64::MAX));
    state
        .prom
        .rank_shard_result_bytes_total
        .inc_by(delivered.shard_result_bytes);
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
            relation = ?delivered.total_hits.relation,
            candidates = delivered.stats.unique_candidates,
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
            total: delivered.routed_shards,
            successful: delivered.routed_shards,
            failed: 0,
        },
        hits: RankedHitsBody {
            total: delivered.total_hits,
            hits: delivered.hits,
        },
    }))
}

/// Coordinator-mode exact bounded top-K plus current-view winner fetch. No
/// partial response is possible: every routed position, every final source, and
/// every requested explanation must succeed before the response is built.
#[instrument(skip_all)]
pub(crate) async fn cluster_v2_search(
    State(state): State<Arc<ClusterAppState>>,
    Json(body): Json<V2SearchBody>,
) -> Result<Json<V2SearchResponse>, (StatusCode, Json<ApiError>)> {
    let started = Instant::now();
    let requested_scope = body.query_scope.unwrap_or_default();
    let prepared = match prepare(body) {
        Ok(prepared) => prepared,
        Err(failure) => {
            if let Some(reason) = failure.admission_reason() {
                state
                    .prom
                    .rank_admission_rejections_total
                    .with_label_values(&[reason])
                    .inc();
                record_outcome(&state.prom, "admission", requested_scope);
            } else {
                record_outcome(&state.prom, "validation", requested_scope);
            }
            return Err(failure.response());
        }
    };
    let PreparedSearch {
        title,
        filter,
        options,
        rank,
        include_source,
        include_explain,
        timeout,
    } = prepared;
    let program = match state.cluster.read().compile_rank_program(&rank) {
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
    let Some(deadline) = Instant::now().checked_add(timeout) else {
        record_outcome(&state.prom, "validation", options.query_scope);
        return Err(validation("timeout_ms is too large"));
    };
    let permit_sem = Arc::clone(&state.ranked_search_permits);
    let state_inner = Arc::clone(&state);
    let title_for_work = title.clone();
    let enrichment_limit = state.max_ranked_enrichment_bytes;
    let work = async move {
        let permit = crate::state::acquire_search_permit(
            Some(&permit_sem),
            &state_inner.prom.ranked_search_permits_in_use,
        )
        .await;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            state_inner.pool.install(|| {
                let cluster = state_inner.cluster.read();
                let ranked = cluster
                    .try_percolate_filtered_top_k(
                        &title_for_work,
                        &filter,
                        options,
                        &program,
                        Some(deadline),
                    )
                    .map_err(DeliveryError::Cluster)?;
                let sources = if include_source || include_explain {
                    cluster
                        .fetch_ranked_sources_bounded(&ranked, enrichment_limit, Some(deadline))
                        .map_err(|error| match error {
                            reverse_rusty::cluster::ClusterRankedError::EnrichmentLimit {
                                ..
                            } => DeliveryError::EnrichmentLimit,
                            other => DeliveryError::Cluster(other),
                        })?
                } else {
                    Vec::new()
                };
                let source_bytes = sources.iter().map(String::len).sum();
                let mut sources = sources.into_iter();
                let mut hits = Vec::with_capacity(ranked.hits.len());
                for hit in &ranked.hits {
                    if deadline_expired(deadline) {
                        return Err(DeliveryError::Deadline);
                    }
                    let source = if include_source || include_explain {
                        let source = sources
                            .next()
                            .ok_or(DeliveryError::SourceUnavailable(hit.logical_id))?;
                        Some(source)
                    } else {
                        None
                    };
                    let explanation = if include_explain {
                        Some(
                            cluster
                                .explain_ranked_source(
                                    hit.logical_id,
                                    source.as_deref().ok_or(
                                        DeliveryError::ExplanationUnavailable(hit.logical_id),
                                    )?,
                                    &title_for_work,
                                )
                                .ok_or(DeliveryError::ExplanationUnavailable(hit.logical_id))?,
                        )
                    } else {
                        None
                    };
                    hits.push(RankedHitBody {
                        _id: hit.logical_id,
                        _score: hit.score,
                        _source: if include_source {
                            source.map(|query| HitSource { query })
                        } else {
                            None
                        },
                        _explanation: explanation,
                    });
                }
                if deadline_expired(deadline) {
                    return Err(DeliveryError::Deadline);
                }
                Ok(DeliveryResult {
                    hits,
                    total_hits: ranked.total_hits,
                    stats: ranked.stats,
                    rank_stats: ranked.rank_stats,
                    routed_shards: ranked.routed_shards,
                    shard_rows_received: ranked.shard_rows_received,
                    shard_result_bytes: ranked.shard_result_bytes,
                    source_bytes,
                })
            })
        })
        .await
    };

    let delivered = match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), work)
        .await
    {
        Ok(Ok(Ok(result))) => result,
        Ok(Ok(Err(
            DeliveryError::Deadline
            | DeliveryError::Cluster(reverse_rusty::cluster::ClusterRankedError::DeadlineExceeded),
        )))
        | Err(_) => {
            state
                .prom
                .http_requests_total
                .with_label_values(&["v2_search", "408"])
                .inc();
            record_outcome(&state.prom, "timeout", options.query_scope);
            // Mirror the single-node timeout arm's slow-query accounting: a
            // cluster timeout must not be invisible to the slow-query metric
            // and structured log (review finding — the two handlers drifted).
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
                "distributed v2 ranked search timed out"
            );
            return Err(ApiError::response(
                StatusCode::REQUEST_TIMEOUT,
                "timeout",
                format!("ranked search timed out after {}ms", timeout.as_millis()),
            ));
        }
        Ok(Ok(Err(DeliveryError::Cluster(error)))) => {
            let (status, kind, outcome) = error.v2_http_class();
            let status = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            if matches!(
                error,
                reverse_rusty::cluster::ClusterRankedError::Admission(_)
            ) {
                state
                    .prom
                    .rank_admission_rejections_total
                    .with_label_values(&["core"])
                    .inc();
            }
            record_outcome(&state.prom, outcome, options.query_scope);
            return Err(ApiError::response(status, kind, error.to_string()));
        }
        Ok(Ok(Err(DeliveryError::ExplanationUnavailable(logical_id)))) => {
            record_outcome(&state.prom, "error", options.query_scope);
            return Err(ApiError::response(
                StatusCode::BAD_GATEWAY,
                "explanation_unavailable",
                format!("explanation unavailable for ranked winner {logical_id}"),
            ));
        }
        Ok(Ok(Err(DeliveryError::SourceUnavailable(logical_id)))) => {
            record_outcome(&state.prom, "error", options.query_scope);
            return Err(ApiError::response(
                StatusCode::BAD_GATEWAY,
                "source_unavailable",
                format!("source unavailable for ranked winner {logical_id}"),
            ));
        }
        Ok(Ok(Err(DeliveryError::EnrichmentLimit))) => {
            state.prom.rank_enrichment_rejections_total.inc();
            record_outcome(&state.prom, "enrichment_limit", options.query_scope);
            return Err(ApiError::response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "rank_enrichment_limit",
                format!(
                    "ranked winner enrichment exceeds {} bytes",
                    state.max_ranked_enrichment_bytes
                ),
            ));
        }
        Ok(Ok(Err(DeliveryError::Local(_)))) => {
            record_outcome(&state.prom, "error", options.query_scope);
            return Err(ApiError::response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "search_error",
                "unexpected cluster ranked delivery error",
            ));
        }
        Ok(Err(error)) => {
            record_outcome(&state.prom, "error", options.query_scope);
            return Err(ApiError::response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "search_error",
                format!("ranked search task failed: {error}"),
            ));
        }
    };

    let took_ms = started.elapsed().as_secs_f64() * 1_000.0;
    state
        .prom
        .http_requests_total
        .with_label_values(&["v2_search", "200"])
        .inc();
    record_outcome(&state.prom, "success", options.query_scope);
    state
        .prom
        .rank_evaluations_total
        .inc_by(delivered.rank_stats.evaluations);
    state
        .prom
        .rank_heap_replacements_total
        .inc_by(delivered.rank_stats.heap_replacements);
    state
        .prom
        .rank_total_relation_total
        .with_label_values(&[match delivered.total_hits.relation {
            reverse_rusty::TotalHitsRelation::Eq => "eq",
            reverse_rusty::TotalHitsRelation::Gte => "gte",
        }])
        .inc();
    state
        .prom
        .rank_true_match_lower_bound_total
        .inc_by(delivered.total_hits.value);
    state
        .prom
        .rank_source_bytes_total
        .inc_by(u64::try_from(delivered.source_bytes).unwrap_or(u64::MAX));
    state
        .prom
        .rank_shard_rows_received_total
        .inc_by(u64::try_from(delivered.shard_rows_received).unwrap_or(u64::MAX));
    state
        .prom
        .rank_shard_result_bytes_total
        .inc_by(delivered.shard_result_bytes);
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
            relation = ?delivered.total_hits.relation,
            candidates = delivered.stats.unique_candidates,
            shard_rows = delivered.shard_rows_received,
            rank_time_ms = took_ms,
            cancellation = "none",
            "slow distributed v2 ranked search"
        );
    }
    Ok(Json(V2SearchResponse {
        took_ms,
        complete: true,
        query_scope: options.query_scope,
        _shards: Shards {
            total: delivered.routed_shards,
            successful: delivered.routed_shards,
            failed: 0,
        },
        hits: RankedHitsBody {
            total: delivered.total_hits,
            hits: delivered.hits,
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    // The full v2 classification table (including the deliberate write-409 vs
    // read-503 ownership divergence) is owned + pinned by the library in
    // `cluster/http_status.rs`; this pin holds the handler to that seam.
    #[test]
    fn stale_cluster_ownership_maps_to_no_partial_503() {
        let error = reverse_rusty::cluster::ClusterRankedError::Shard(
            reverse_rusty::cluster::ShardError::OwnershipMismatch(
                reverse_rusty::ownership::OwnershipError::PlacementDecisionMismatch,
            ),
        );
        let (status, kind, outcome) = error.v2_http_class();
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE.as_u16());
        assert_eq!(kind, "placement_generation_mismatch");
        assert_eq!(outcome, "error");
    }
}
