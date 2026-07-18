//! The one v2 ranked-search delivery pipeline shared by single-node and
//! coordinator serving (the ADR-107..110 review-residue unification).
//!
//! Three layers: the handlers in the parent module lower the request
//! (`prepare`) and build a mode-specific blocking kernel over
//! [`local_delivery`] / [`cluster_delivery`]; [`drive`] owns everything the two
//! handlers used to duplicate — the ADR-099 permit-inside-deadline race, the
//! blocking dispatch, the failure classification, and the success
//! metrics/log/response epilogue. The per-title kernels are synchronous and
//! axum-free on purpose: a future batch endpoint calls them once per slot
//! under one permit and one deadline.
//!
//! The two deliberate per-mode differences are codified here instead of being
//! re-derived in each handler: the missing-source/explanation status
//! ([`RankedBackendError::UNAVAILABLE_STATUS`] — 500 locally, where our own
//! store is inconsistent; 502 in cluster mode, where an upstream shard failed
//! us) and the backend error classification (the library-owned
//! `v2_http_class` table for the cluster).

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{http::StatusCode, Json};
use tracing::{info, warn};

use crate::dto::{ApiError, HitSource};
use crate::metrics::PrometheusMetrics;
use crate::state::{AppState, ClusterAppState};

use reverse_rusty::cluster::{ClusterEngine, ClusterRankedError};
use reverse_rusty::exact::TagPredicate;
use reverse_rusty::segment::MatchScratch;
use reverse_rusty::{CompiledRankProgram, EngineSnapshot, RankedMatchError, TopKOptions};

use super::{record_outcome, RankedHitBody, RankedHitsBody, Shards, V2SearchResponse};

/// Fail-closed accounting for source text fetched during winner enrichment.
/// A source used by both `_source` and explain is charged once.
pub(super) struct EnrichmentBudget {
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

pub(super) struct DeliveryResult {
    pub(super) hits: Vec<RankedHitBody>,
    pub(super) total_hits: reverse_rusty::TotalHits,
    pub(super) stats: reverse_rusty::segment::MatchStats,
    pub(super) rank_stats: reverse_rusty::RankStats,
    pub(super) routed_shards: usize,
    pub(super) shard_rows_received: usize,
    pub(super) shard_result_bytes: u64,
    pub(super) source_bytes: usize,
}

/// Delivery failure, generic over the mode's backend error — a local handler
/// can no longer receive a cluster error (or vice versa) by construction,
/// which is what deleted the two "unexpected … delivery error" 500 arms.
pub(super) enum DeliveryError<E> {
    Backend(E),
    SourceUnavailable(u64),
    ExplanationUnavailable(u64),
    EnrichmentLimit,
    Deadline,
}

/// What [`drive`] needs to know about a mode's backend error. The constants
/// pin the two sanctioned per-mode divergences at one site.
pub(super) trait RankedBackendError {
    /// Log discriminator for the shared warn/slow-query sites.
    const MODE: &'static str;
    /// Status for a winner whose source/explanation cannot be produced after a
    /// successful match: local 500 (our own store is inconsistent) vs cluster
    /// 502 (an upstream shard failed us).
    const UNAVAILABLE_STATUS: StatusCode;
    /// Deadline-shaped failures join the timeout arm (408 + timeout metrics)
    /// instead of generic classification.
    fn is_deadline(&self) -> bool;
    /// Core (post-`prepare`) admission failures additionally count toward
    /// `rank_admission_rejections_total{core}`.
    fn is_core_admission(&self) -> bool;
    /// `(status, error kind, metrics outcome)` for everything that is neither
    /// a deadline nor handled by a dedicated `DeliveryError` variant.
    fn http_class(&self) -> (StatusCode, &'static str, &'static str);
}

impl RankedBackendError for RankedMatchError {
    const MODE: &'static str = "local";
    const UNAVAILABLE_STATUS: StatusCode = StatusCode::INTERNAL_SERVER_ERROR;

    fn is_deadline(&self) -> bool {
        matches!(self, Self::Cancelled(_))
    }

    fn is_core_admission(&self) -> bool {
        matches!(self, Self::Admission(_))
    }

    fn http_class(&self) -> (StatusCode, &'static str, &'static str) {
        match self {
            Self::Admission(_) => (
                StatusCode::BAD_REQUEST,
                "rank_admission_rejected",
                "admission",
            ),
            // Consumed by the timeout arm before classification; kept total.
            Self::Cancelled(_) => (StatusCode::REQUEST_TIMEOUT, "timeout", "timeout"),
        }
    }
}

impl RankedBackendError for ClusterRankedError {
    const MODE: &'static str = "distributed";
    const UNAVAILABLE_STATUS: StatusCode = StatusCode::BAD_GATEWAY;

    fn is_deadline(&self) -> bool {
        matches!(self, Self::DeadlineExceeded)
    }

    fn is_core_admission(&self) -> bool {
        matches!(self, Self::Admission(_))
    }

    fn http_class(&self) -> (StatusCode, &'static str, &'static str) {
        let (status, kind, outcome) = self.v2_http_class();
        (
            StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            kind,
            outcome,
        )
    }
}

/// The state either mode hands [`drive`] — the two `AppState`s carry these
/// identically-named fields already; the trait is what lets one driver own the
/// permit race, metrics, and epilogue for both.
pub(super) trait RankedSearchCtx {
    fn prom(&self) -> &PrometheusMetrics;
    fn pool(&self) -> &rayon::ThreadPool;
    fn ranked_search_permits(&self) -> &Arc<tokio::sync::Semaphore>;
    fn max_ranked_enrichment_bytes(&self) -> usize;
    fn slow_query_threshold_ms(&self) -> u64;
}

impl RankedSearchCtx for AppState {
    fn prom(&self) -> &PrometheusMetrics {
        &self.prom
    }
    fn pool(&self) -> &rayon::ThreadPool {
        &self.pool
    }
    fn ranked_search_permits(&self) -> &Arc<tokio::sync::Semaphore> {
        &self.ranked_search_permits
    }
    fn max_ranked_enrichment_bytes(&self) -> usize {
        self.max_ranked_enrichment_bytes
    }
    fn slow_query_threshold_ms(&self) -> u64 {
        self.slow_query_threshold_ms
    }
}

impl RankedSearchCtx for ClusterAppState {
    fn prom(&self) -> &PrometheusMetrics {
        &self.prom
    }
    fn pool(&self) -> &rayon::ThreadPool {
        &self.pool
    }
    fn ranked_search_permits(&self) -> &Arc<tokio::sync::Semaphore> {
        &self.ranked_search_permits
    }
    fn max_ranked_enrichment_bytes(&self) -> usize {
        self.max_ranked_enrichment_bytes
    }
    fn slow_query_threshold_ms(&self) -> u64 {
        self.slow_query_threshold_ms
    }
}

/// The per-request delivery parameters both kernels share.
pub(super) struct DeliverySpec<'a> {
    pub(super) title: &'a str,
    pub(super) options: TopKOptions,
    pub(super) include_source: bool,
    pub(super) include_explain: bool,
    pub(super) enrichment_limit: usize,
    pub(super) deadline: Instant,
}

fn deadline_expired(deadline: Instant) -> bool {
    Instant::now() >= deadline
}

/// Build the response hit bodies for finalized winners. `fetch_source` maps a
/// winner id to its source (each mode owns its missing-source error choice);
/// `explain` produces the structured explanation for an already-fetched
/// source. Shared so the source/explanation/deadline shape cannot drift.
fn assemble_hits<E>(
    spec: &DeliverySpec<'_>,
    winners: impl ExactSizeIterator<Item = (u64, i64)>,
    mut fetch_source: impl FnMut(u64) -> Result<String, DeliveryError<E>>,
    mut explain: impl FnMut(u64, &str) -> Option<reverse_rusty::ExplainDetail>,
) -> Result<Vec<RankedHitBody>, DeliveryError<E>> {
    let mut hits = Vec::with_capacity(winners.len());
    for (logical_id, score) in winners {
        if deadline_expired(spec.deadline) {
            return Err(DeliveryError::Deadline);
        }
        let source = if spec.include_source || spec.include_explain {
            Some(fetch_source(logical_id)?)
        } else {
            None
        };
        let explanation = if spec.include_explain {
            let text = source
                .as_deref()
                .ok_or(DeliveryError::ExplanationUnavailable(logical_id))?;
            Some(
                explain(logical_id, text)
                    .ok_or(DeliveryError::ExplanationUnavailable(logical_id))?,
            )
        } else {
            None
        };
        hits.push(RankedHitBody {
            _id: logical_id,
            _score: score,
            _source: if spec.include_source {
                source.map(|query| HitSource { query })
            } else {
                None
            },
            _explanation: explanation,
        });
    }
    Ok(hits)
}

/// Single-node kernel: bounded snapshot match, then inline per-winner
/// enrichment under the fail-closed byte budget.
pub(super) fn local_delivery(
    snap: &EngineSnapshot,
    program: &CompiledRankProgram,
    predicate: &TagPredicate,
    scratch: &mut MatchScratch,
    spec: &DeliverySpec<'_>,
) -> Result<DeliveryResult, DeliveryError<RankedMatchError>> {
    let ranked = snap
        .try_match_title_top_k(
            spec.title,
            spec.options,
            program,
            predicate,
            scratch,
            Some(spec.deadline),
        )
        .map_err(DeliveryError::Backend)?;
    let mut budget = EnrichmentBudget::new(spec.enrichment_limit);
    let hits = assemble_hits(
        spec,
        ranked.hits.iter().map(|hit| (hit.logical_id, hit.score)),
        |logical_id| {
            // Bounded lookup: the store checks its borrowed value against the
            // REMAINING credit before cloning, so an over-limit winner 413s
            // without ever allocating the full source `String` (codex review —
            // peak-memory bound).
            let source = match snap.get_query_source_bounded(logical_id, budget.remaining()) {
                Ok(Some(source)) => source,
                Ok(None) => {
                    return Err(if spec.include_explain && !spec.include_source {
                        DeliveryError::ExplanationUnavailable(logical_id)
                    } else {
                        DeliveryError::SourceUnavailable(logical_id)
                    });
                }
                Err(_over_credit) => return Err(DeliveryError::EnrichmentLimit),
            };
            budget
                .charge(source.len())
                .map_err(|()| DeliveryError::EnrichmentLimit)?;
            Ok(source)
        },
        |logical_id, source| snap.explain_source(logical_id, source, spec.title),
    )?;
    if deadline_expired(spec.deadline) {
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
}

/// Coordinator kernel: exact bounded distributed top-K, then the phase-two
/// winner fetch under one global credit, then assembly.
pub(super) fn cluster_delivery(
    cluster: &ClusterEngine,
    program: &CompiledRankProgram,
    filter: &[(String, Vec<String>)],
    spec: &DeliverySpec<'_>,
) -> Result<DeliveryResult, DeliveryError<ClusterRankedError>> {
    let ranked = cluster
        .try_percolate_filtered_top_k(
            spec.title,
            filter,
            spec.options,
            program,
            Some(spec.deadline),
        )
        .map_err(DeliveryError::Backend)?;
    let sources = if spec.include_source || spec.include_explain {
        cluster
            .fetch_ranked_sources_bounded(&ranked, spec.enrichment_limit, Some(spec.deadline))
            .map_err(|error| match error {
                ClusterRankedError::EnrichmentLimit { .. } => DeliveryError::EnrichmentLimit,
                other => DeliveryError::Backend(other),
            })?
    } else {
        Vec::new()
    };
    let source_bytes = sources.iter().map(String::len).sum();
    let mut sources = sources.into_iter();
    let hits = assemble_hits(
        spec,
        ranked.hits.iter().map(|hit| (hit.logical_id, hit.score)),
        |logical_id| {
            sources
                .next()
                .ok_or(DeliveryError::SourceUnavailable(logical_id))
        },
        |logical_id, source| cluster.explain_ranked_source(logical_id, source, spec.title),
    )?;
    if deadline_expired(spec.deadline) {
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
}

fn timeout_response<S: RankedSearchCtx>(
    state: &S,
    started: Instant,
    options: TopKOptions,
    timeout: Duration,
    mode: &'static str,
) -> (StatusCode, Json<ApiError>) {
    state
        .prom()
        .http_requests_total
        .with_label_values(&["v2_search", "408"])
        .inc();
    record_outcome(state.prom(), "timeout", options.query_scope);
    let elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0;
    if elapsed_ms >= state.slow_query_threshold_ms() as f64 {
        state.prom().slow_queries_total.inc();
    }
    warn!(
        k = options.size,
        scope = ?options.query_scope,
        relation = "unknown",
        candidates = "unknown",
        rank_time_ms = elapsed_ms,
        cancellation = "deadline",
        mode,
        "v2 ranked search timed out"
    );
    ApiError::response(
        StatusCode::REQUEST_TIMEOUT,
        "timeout",
        format!("ranked search timed out after {}ms", timeout.as_millis()),
    )
}

/// The shared async driver: permit race, blocking dispatch, failure
/// classification, and the success epilogue.
pub(super) async fn drive<S, E, F>(
    state: Arc<S>,
    started: Instant,
    options: TopKOptions,
    timeout: Duration,
    deadline: Instant,
    work: F,
) -> Result<Json<V2SearchResponse>, (StatusCode, Json<ApiError>)>
where
    S: RankedSearchCtx + Send + Sync + 'static,
    E: RankedBackendError + std::fmt::Display + Send + 'static,
    F: FnOnce() -> Result<DeliveryResult, DeliveryError<E>> + Send + 'static,
{
    let permit_sem = Arc::clone(state.ranked_search_permits());
    let state_inner = Arc::clone(&state);
    // ADR-099 semantics: the permit is acquired INSIDE the deadline race (queue
    // wait counts against the timeout) and moved into the blocking closure
    // (released when the work actually ends, not when an abandoned join handle
    // drops at timeout).
    let work = async move {
        let permit = crate::state::acquire_search_permit(
            Some(&permit_sem),
            &state_inner.prom().ranked_search_permits_in_use,
        )
        .await;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            state_inner.pool().install(work)
        })
        .await
    };

    let delivered =
        match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), work).await {
            Ok(Ok(Ok(result))) => result,
            Ok(Ok(Err(DeliveryError::Deadline))) | Err(_) => {
                return Err(timeout_response(
                    &*state,
                    started,
                    options,
                    timeout,
                    E::MODE,
                ));
            }
            Ok(Ok(Err(DeliveryError::Backend(error)))) => {
                if error.is_deadline() {
                    return Err(timeout_response(
                        &*state,
                        started,
                        options,
                        timeout,
                        E::MODE,
                    ));
                }
                let (status, kind, outcome) = error.http_class();
                if error.is_core_admission() {
                    state
                        .prom()
                        .rank_admission_rejections_total
                        .with_label_values(&["core"])
                        .inc();
                }
                record_outcome(state.prom(), outcome, options.query_scope);
                return Err(ApiError::response(status, kind, error.to_string()));
            }
            Ok(Ok(Err(DeliveryError::SourceUnavailable(logical_id)))) => {
                record_outcome(state.prom(), "error", options.query_scope);
                return Err(ApiError::response(
                    E::UNAVAILABLE_STATUS,
                    "source_unavailable",
                    format!("source unavailable for ranked winner {logical_id}"),
                ));
            }
            Ok(Ok(Err(DeliveryError::ExplanationUnavailable(logical_id)))) => {
                record_outcome(state.prom(), "error", options.query_scope);
                return Err(ApiError::response(
                    E::UNAVAILABLE_STATUS,
                    "explanation_unavailable",
                    format!("explanation unavailable for ranked winner {logical_id}"),
                ));
            }
            Ok(Ok(Err(DeliveryError::EnrichmentLimit))) => {
                state.prom().rank_enrichment_rejections_total.inc();
                record_outcome(state.prom(), "enrichment_limit", options.query_scope);
                return Err(ApiError::response(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "rank_enrichment_limit",
                    format!(
                        "ranked winner enrichment exceeds {} bytes",
                        state.max_ranked_enrichment_bytes()
                    ),
                ));
            }
            Ok(Err(error)) => {
                record_outcome(state.prom(), "error", options.query_scope);
                return Err(ApiError::response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "search_error",
                    format!("ranked search task failed: {error}"),
                ));
            }
        };

    let took_ms = started.elapsed().as_secs_f64() * 1_000.0;
    let prom = state.prom();
    prom.http_requests_total
        .with_label_values(&["v2_search", "200"])
        .inc();
    record_outcome(prom, "success", options.query_scope);
    prom.rank_evaluations_total
        .inc_by(delivered.rank_stats.evaluations);
    prom.rank_heap_replacements_total
        .inc_by(delivered.rank_stats.heap_replacements);
    prom.rank_total_relation_total
        .with_label_values(&[match delivered.total_hits.relation {
            reverse_rusty::TotalHitsRelation::Eq => "eq",
            reverse_rusty::TotalHitsRelation::Gte => "gte",
        }])
        .inc();
    prom.rank_true_match_lower_bound_total
        .inc_by(delivered.total_hits.value);
    prom.rank_source_bytes_total
        .inc_by(u64::try_from(delivered.source_bytes).unwrap_or(u64::MAX));
    prom.rank_shard_rows_received_total
        .inc_by(u64::try_from(delivered.shard_rows_received).unwrap_or(u64::MAX));
    prom.rank_shard_result_bytes_total
        .inc_by(delivered.shard_result_bytes);
    prom.http_request_duration
        .with_label_values(&["v2_search"])
        .observe(started.elapsed().as_secs_f64());
    if took_ms >= state.slow_query_threshold_ms() as f64 {
        prom.slow_queries_total.inc();
        info!(
            k = options.size,
            scope = ?options.query_scope,
            relation = ?delivered.total_hits.relation,
            candidates = delivered.stats.unique_candidates,
            shard_rows = delivered.shard_rows_received,
            rank_time_ms = took_ms,
            cancellation = "none",
            mode = E::MODE,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The sanctioned per-mode divergence: a missing winner source is our own
    /// inconsistency locally (500) but an upstream failure in cluster mode
    /// (502). Pinned here because both handlers now share every other arm.
    #[test]
    fn unavailable_status_divergence_is_pinned() {
        assert_eq!(
            <RankedMatchError as RankedBackendError>::UNAVAILABLE_STATUS,
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            <ClusterRankedError as RankedBackendError>::UNAVAILABLE_STATUS,
            StatusCode::BAD_GATEWAY
        );
    }
}
