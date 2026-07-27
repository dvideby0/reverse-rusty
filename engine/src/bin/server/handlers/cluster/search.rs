//! Cluster-mode percolate handlers (ADR-070): `GET|POST /_search` + `POST /_mpercolate`
//! over [`ClusterEngine::percolate_filtered_with_stats`] — the routing + merge the
//! cluster oracles prove ≡ single-node ≡ brute. Resolves the same native + ES
//! envelopes (shared [`resolve_percolate`]) and the same `rank` block (shared
//! [`RankBody`], ADR-075: the coordinator compiles the spec against the shared
//! frozen tag space and each shard scores its own matched ids — same
//! `(score desc, _id asc)` order + `from`/`size` as single-node). Both endpoints
//! take a per-request `include_broad` (the coordinator owns broad routing, so the
//! per-shard toggle is free here; single-node `/_search` parity is ADR-064 item 6).
//! A request feature the cluster cannot honor yet (`explain`) is a 400, never
//! silently ignored.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{
        rejection::{JsonRejection, QueryRejection},
        Query, State,
    },
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::{info, instrument, warn};

use reverse_rusty::cluster::{ClusterReadView, ShardError};
use reverse_rusty::segment::MatchStats;

use crate::dto::{ApiError, HitSource};
use crate::handlers::doc::QUERY_INDEX;
use crate::handlers::search::{
    resolve_percolate, resolve_percolate_strict, resolve_search_controls, to_rank_spec,
    CompatibilityDocBody, DocBody, RankBody, SearchControlInput, SearchParams,
};
use crate::state::ClusterAppState;

/// A request filter resolved for the cluster percolate calls.
type FilterSpec = Vec<(String, Vec<String>)>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ClusterSearchBody {
    document: Option<CompatibilityDocBody>,
    documents: Option<Vec<CompatibilityDocBody>>,
    filter: Option<serde_json::Value>,
    query: Option<serde_json::Value>,
    /// Per-request broad-lane override (falls back to the server default).
    include_broad: Option<bool>,
    timeout_ms: Option<u64>,
    timeout: Option<String>,
    size: Option<usize>,
    from: Option<usize>,
    /// Include each hit's stored query source (default false in cluster mode — it
    /// costs a per-hit source probe; explicit `true` on a remote cluster is a 501).
    include_source: Option<bool>,
    #[serde(rename = "_source")]
    source: Option<bool>,
    /// Optional ranking (ADR-059/075): order hits by a numeric priority tag and/or
    /// additive request boosts, scored at the shards against the shared tag space.
    /// Absent ⇒ hits keep merged engine order — byte-identical to the pre-rank path.
    rank: Option<RankBody>,
    /// Not supported in cluster mode — present so a request using it is REJECTED
    /// loudly rather than silently un-explained.
    explain: Option<bool>,
    profile: Option<bool>,
}

#[derive(Serialize)]
pub(crate) struct ClusterSearchResponse {
    took: u64,
    timed_out: bool,
    took_ms: f64,
    hits: ClusterHits,
    #[serde(skip_serializing_if = "Option::is_none")]
    slots: Option<Vec<ClusterSlotHit>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    profile: Option<StatsResponse>,
}

#[derive(Serialize)]
struct ClusterHits {
    total: usize,
    hits: Vec<ClusterHitItem>,
}

#[derive(Serialize)]
struct ClusterHitItem {
    _index: &'static str,
    _id: u64,
    /// Ranking score (ADR-075) — present only when the request supplied a `rank`
    /// block; omitted (so the response is byte-identical) on the unranked path.
    #[serde(skip_serializing_if = "Option::is_none")]
    _score: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    _source: Option<HitSource>,
}

#[derive(Serialize)]
struct ClusterSlotHit {
    slot: usize,
    total: usize,
    hits: Vec<ClusterHitItem>,
    stats: StatsResponse,
}

#[derive(Serialize, Clone, Default)]
struct StatsResponse {
    unique_candidates: u64,
    broad_candidates: u64,
    postings_scanned: u64,
    matches: u64,
    probes_attempted: u64,
    probes_skipped: u64,
}

impl From<MatchStats> for StatsResponse {
    fn from(s: MatchStats) -> Self {
        Self {
            unique_candidates: u64::from(s.unique_candidates),
            broad_candidates: u64::from(s.broad_candidates),
            postings_scanned: u64::from(s.postings_scanned),
            matches: u64::from(s.matches),
            probes_attempted: u64::from(s.probes_attempted),
            probes_skipped: u64::from(s.probes_skipped),
        }
    }
}

impl StatsResponse {
    fn merge(&mut self, stats: MatchStats) {
        self.unique_candidates += u64::from(stats.unique_candidates);
        self.broad_candidates += u64::from(stats.broad_candidates);
        self.postings_scanned += u64::from(stats.postings_scanned);
        self.matches += u64::from(stats.matches);
        self.probes_attempted += u64::from(stats.probes_attempted);
        self.probes_skipped += u64::from(stats.probes_skipped);
    }
}

type Reject = (StatusCode, Json<ApiError>);

/// One title's percolate result rows: matched id + its ranking score (`None` on the
/// unranked path, so the response stays byte-identical). Rows are kept sorted by id
/// (the merge order) until presentation ordering.
type ScoredIds = Vec<(u64, Option<i64>)>;

/// Order one matched set for presentation + slice the page — the cluster analogue
/// of the single-node `order_and_page` (ADR-059/075). Ranked rows sort by
/// `(score desc, _id asc)` (a total order, so pagination is byte-stable); unranked
/// rows keep the merged ascending-id order. Then `from`/`size`.
///
/// The canonical statement of this order is `reverse_rusty::ranked_order`; this
/// sort stays hand-written only because its rows carry `Option<i64>` scores (the
/// ranked branch is all-`Some`, so the `Option` ordering never actually decides).
fn order_and_page(rows: &ScoredIds, ranked: bool, from: usize, size: usize) -> ScoredIds {
    if ranked {
        let mut sorted = rows.clone();
        sorted.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        sorted.into_iter().skip(from).take(size).collect()
    } else {
        rows.iter().copied().skip(from).take(size).collect()
    }
}

/// Materialize hit items for already-ordered, already-paged rows, optionally
/// attaching `_source` cloned under the same mutation fence as matching.
fn attach_hits(
    rows: &[(u64, Option<i64>)],
    include_source: bool,
    sources: Option<&HashMap<u64, String>>,
) -> Result<Vec<ClusterHitItem>, ShardError> {
    rows.iter()
        .map(|&(id, score)| {
            let source = if include_source {
                Some(HitSource {
                    query: sources
                        .and_then(|source| source.get(&id))
                        .cloned()
                        .ok_or(ShardError::SourceUnavailable(id))?,
                })
            } else {
                None
            };
            Ok(ClusterHitItem {
                _index: QUERY_INDEX,
                _id: id,
                _score: score,
                _source: source,
            })
        })
        .collect()
}

/// Reject the request features the cluster cannot honor yet — loudly, per the
/// no-silent-degrade rule.
fn reject_unsupported(
    state: &ClusterAppState,
    endpoint: &'static str,
    explain: bool,
) -> Result<(), Reject> {
    if !explain {
        return Ok(());
    }
    state
        .prom
        .http_requests_total
        .with_label_values(&[endpoint, "400"])
        .inc();
    Err(ApiError::response(
        StatusCode::BAD_REQUEST,
        "validation_error",
        "per-hit explain is not supported in cluster mode yet; remove `explain`",
    ))
}

fn validation(state: &ClusterAppState, message: impl Into<String>) -> Reject {
    state
        .prom
        .http_requests_total
        .with_label_values(&["search", "400"])
        .inc();
    ApiError::response(StatusCode::BAD_REQUEST, "validation_error", message)
}

fn body_rejection(state: &ClusterAppState, error: &JsonRejection) -> Reject {
    let status = error.status();
    if status == StatusCode::PAYLOAD_TOO_LARGE || status == StatusCode::UNSUPPORTED_MEDIA_TYPE {
        state
            .prom
            .http_requests_total
            .with_label_values(&["search", status.as_str()])
            .inc();
        let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
            "payload_too_large"
        } else {
            "unsupported_media_type"
        };
        return ApiError::response(status, error_type, format!("invalid search body: {error}"));
    }
    validation(state, format!("invalid search body: {error}"))
}

/// Route extractor for `GET|POST /_search`.
#[instrument(skip_all)]
pub(crate) async fn cluster_search_route(
    State(state): State<Arc<ClusterAppState>>,
    params: Result<Query<SearchParams>, QueryRejection>,
    body: Result<Json<ClusterSearchBody>, JsonRejection>,
) -> Result<Json<ClusterSearchResponse>, Reject> {
    let _duration = state
        .prom
        .http_request_duration
        .with_label_values(&["search"])
        .start_timer();
    let Query(params) = params
        .map_err(|error| validation(&state, format!("invalid search query parameters: {error}")))?;
    let Json(body) = body.map_err(|error| body_rejection(&state, &error))?;
    cluster_search_inner(state, body, params).await
}

/// GET|POST /_search — percolate one or more titles against the cluster.
async fn cluster_search_inner(
    state: Arc<ClusterAppState>,
    body: ClusterSearchBody,
    params: SearchParams,
) -> Result<Json<ClusterSearchResponse>, Reject> {
    let start = Instant::now();
    let controls = resolve_search_controls(
        SearchControlInput {
            from: body.from,
            size: body.size,
            explain: body.explain,
            profile: body.profile,
            source: body.source,
            include_source: body.include_source,
            timeout: body.timeout,
            timeout_ms: body.timeout_ms,
        },
        params,
        false,
    )
    .map_err(|message| validation(&state, message))?;
    reject_unsupported(&state, "search", controls.features.explain)?;

    let include_broad = body.include_broad.unwrap_or(state.include_broad);
    let include_source = controls.features.include_source;
    let include_profile = controls.features.profile;
    let timeout = controls.timeout;
    let page_size = controls.size;
    let page_from = controls.from;
    let rank_spec = to_rank_spec(body.rank);
    let ranked = rank_spec.is_some();

    let document = body.document.map(Into::into);
    let documents = body
        .documents
        .map(|documents| documents.into_iter().map(Into::into).collect());
    let (titles, single, filter_spec) =
        match resolve_percolate_strict(document, documents, body.filter, body.query) {
            Ok(t) => t,
            Err(msg) => return Err(validation(&state, msg)),
        };

    // ADR-099: arm cooperative (per-title) cancellation only for an EXPLICIT
    // timeout/timeout_ms. Lock-free here — the dynamic kill-switch is resolved INSIDE the
    // blocking task (under the timeout race), so a held cluster write lock (e.g. a
    // vocab rebuild) can never stall this async handler past its own deadline (codex).
    let deadline = if controls.explicit_timeout {
        Some(
            start
                .checked_add(timeout)
                .ok_or_else(|| validation(&state, "`timeout` is too large"))?,
        )
    } else {
        None
    };
    let results = percolate_blocking(
        &state,
        titles,
        filter_spec,
        include_broad,
        rank_spec,
        timeout,
        deadline,
        "search",
        include_source.then_some(SourceFetch {
            shape: SourceShape::Search { single },
            ranked,
            from: page_from,
            size: page_size,
        }),
    )
    .await?;
    let sources = results.sources;
    let results = results.results;

    let attach = |rows: &ScoredIds| {
        attach_hits(
            &order_and_page(rows, ranked, page_from, page_size),
            include_source,
            sources.as_ref(),
        )
    };
    let response = if single {
        let (rows, stats) = &results[0];
        let hits = attach(rows).map_err(|e| source_unavailable(&state, "search", &e))?;
        ClusterSearchResponse {
            took: 0,
            timed_out: false,
            took_ms: 0.0,
            hits: ClusterHits {
                total: rows.len(),
                hits,
            },
            slots: None,
            profile: include_profile.then(|| StatsResponse::from(*stats)),
        }
    } else {
        let mut slots = Vec::with_capacity(results.len());
        let mut merged = StatsResponse::default();
        let mut all: ScoredIds = Vec::new();
        for (slot, (rows, stats)) in results.iter().enumerate() {
            let hits = attach(rows).map_err(|e| source_unavailable(&state, "search", &e))?;
            merged.merge(*stats);
            all.extend_from_slice(rows);
            slots.push(ClusterSlotHit {
                slot,
                total: rows.len(),
                hits,
                stats: StatsResponse::from(*stats),
            });
        }
        // Dedup the cross-document union by id: a query matching several documents
        // carries ONE score (scores are per-query, not per-document), so any copy wins.
        all.sort_unstable_by_key(|&(id, _)| id);
        all.dedup_by_key(|&mut (id, _)| id);
        let hits = attach(&all).map_err(|e| source_unavailable(&state, "search", &e))?;
        ClusterSearchResponse {
            took: 0,
            timed_out: false,
            took_ms: 0.0,
            hits: ClusterHits {
                total: all.len(),
                hits,
            },
            slots: Some(slots),
            profile: include_profile.then_some(merged),
        }
    };

    let mut response = response;
    let took = start.elapsed();
    response.took = took.as_millis() as u64;
    response.took_ms = took.as_secs_f64() * 1000.0;
    let slow_ms = state.slow_query_threshold_ms;
    if slow_ms > 0 && took.as_millis() as u64 >= slow_ms {
        warn!(
            took_ms = took.as_millis() as u64,
            titles = results.len(),
            "slow cluster search"
        );
    }

    state
        .prom
        .http_requests_total
        .with_label_values(&["search", "200"])
        .inc();
    Ok(Json(response))
}

#[derive(Deserialize)]
pub(crate) struct ClusterMPercolateBody {
    documents: Option<Vec<DocBody>>,
    filter: Option<serde_json::Value>,
    query: Option<serde_json::Value>,
    include_broad: Option<bool>,
    include_source: Option<bool>,
    size: Option<usize>,
    from: Option<usize>,
    timeout_ms: Option<u64>,
    /// Optional ranking (ADR-059/075): order each document's hits by a numeric
    /// priority tag and/or additive request boosts. Absent ⇒ engine order.
    rank: Option<RankBody>,
}

#[derive(Serialize)]
pub(crate) struct ClusterMPercolateResponse {
    took_ms: f64,
    responses: Vec<ClusterPercolateItem>,
}

#[derive(Serialize)]
struct ClusterPercolateItem {
    hits: ClusterHits,
}

/// POST /_mpercolate — batch percolation against the cluster (ES `_msearch`-shaped
/// `responses[]`, one per input document in submission order).
#[instrument(skip_all)]
pub(crate) async fn cluster_mpercolate(
    State(state): State<Arc<ClusterAppState>>,
    Json(body): Json<ClusterMPercolateBody>,
) -> Result<Json<ClusterMPercolateResponse>, Reject> {
    let start = Instant::now();

    let include_broad = body.include_broad.unwrap_or(state.include_broad);
    let include_source = body.include_source.unwrap_or(false);
    let timeout = tokio::time::Duration::from_millis(body.timeout_ms.unwrap_or(30_000));
    let page_size = body.size.unwrap_or(1000);
    let page_from = body.from.unwrap_or(0);
    let rank_spec = to_rank_spec(body.rank);
    let ranked = rank_spec.is_some();

    let (titles, _single, filter_spec) =
        match resolve_percolate(None, body.documents, body.filter, body.query) {
            Ok(t) => t,
            Err(msg) => {
                state
                    .prom
                    .http_requests_total
                    .with_label_values(&["mpercolate", "400"])
                    .inc();
                return Err(ApiError::response(
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    msg,
                ));
            }
        };

    // ADR-099: see cluster_search above (lock-free; the kill-switch resolves in the
    // blocking task).
    let deadline = body.timeout_ms.is_some().then(|| start + timeout);
    let results = percolate_blocking(
        &state,
        titles,
        filter_spec,
        include_broad,
        rank_spec,
        timeout,
        deadline,
        "mpercolate",
        include_source.then_some(SourceFetch {
            shape: SourceShape::PerSlot,
            ranked,
            from: page_from,
            size: page_size,
        }),
    )
    .await?;
    let sources = results.sources;
    let results = results.results;

    let mut responses = Vec::with_capacity(results.len());
    for (rows, _stats) in &results {
        // Per-slot rank + `from`/`size`, the single-node `/_mpercolate` semantics.
        let hits = attach_hits(
            &order_and_page(rows, ranked, page_from, page_size),
            include_source,
            sources.as_ref(),
        )
        .map_err(|e| source_unavailable(&state, "mpercolate", &e))?;
        responses.push(ClusterPercolateItem {
            hits: ClusterHits {
                total: rows.len(),
                hits,
            },
        });
    }

    state
        .prom
        .http_requests_total
        .with_label_values(&["mpercolate", "200"])
        .inc();
    state
        .prom
        .http_request_duration
        .with_label_values(&["mpercolate"])
        .observe(start.elapsed().as_secs_f64());
    Ok(Json(ClusterMPercolateResponse {
        took_ms: start.elapsed().as_secs_f64() * 1000.0,
        responses,
    }))
}

/// Run the per-title percolates on the rayon pool under a timeout — the cluster
/// analogue of the single-node spawn_blocking pattern. Titles evaluate in parallel
/// (each percolate additionally fans across its target shards); results keep
/// submission order. With a `rank` spec each row carries its shard-computed score
/// (ADR-075); without one, scores are `None` and the rows are byte-identical to the
/// pre-rank path.
/// How a `percolate_blocking` title evaluation failed: a shard probe failure (the
/// fail-loud 502 — never a silently shrunken union) or a cooperative-deadline
/// cancellation (ADR-099 — the same 408 the response deadline produces; a shard
/// failure is never masked by a concurrent cancellation because each title maps to
/// its own variant and `Shard` short-circuits identically either way).
enum PercFail {
    Validation(String),
    Shard(ShardError),
    Source(ShardError),
    Cancelled,
}

#[derive(Clone, Copy)]
enum SourceShape {
    Search { single: bool },
    PerSlot,
}

#[derive(Clone, Copy)]
struct SourceFetch {
    shape: SourceShape,
    ranked: bool,
    from: usize,
    size: usize,
}

struct PercolateRun {
    results: Vec<(ScoredIds, MatchStats)>,
    sources: Option<HashMap<u64, String>>,
}

fn source_ids(results: &[(ScoredIds, MatchStats)], fetch: SourceFetch) -> Vec<u64> {
    let mut ids = Vec::new();
    for (rows, _) in results {
        ids.extend(
            order_and_page(rows, fetch.ranked, fetch.from, fetch.size)
                .into_iter()
                .map(|(id, _)| id),
        );
    }
    if matches!(fetch.shape, SourceShape::Search { single: false }) {
        let mut union: ScoredIds = results
            .iter()
            .flat_map(|(rows, _)| rows.iter().copied())
            .collect();
        union.sort_unstable_by_key(|&(id, _)| id);
        union.dedup_by_key(|&mut (id, _)| id);
        ids.extend(
            order_and_page(&union, fetch.ranked, fetch.from, fetch.size)
                .into_iter()
                .map(|(id, _)| id),
        );
    }
    ids.sort_unstable();
    ids.dedup();
    ids
}

#[allow(clippy::too_many_arguments)] // the request knobs of two endpoints funnel here
async fn percolate_blocking(
    state: &Arc<ClusterAppState>,
    titles: Vec<String>,
    filter: FilterSpec,
    include_broad: bool,
    rank: Option<reverse_rusty::RankSpec>,
    timeout: tokio::time::Duration,
    requested_deadline: Option<Instant>,
    endpoint: &'static str,
    source_fetch: Option<SourceFetch>,
) -> Result<PercolateRun, Reject> {
    let state_inner = Arc::clone(state);
    let fut = async {
        // ADR-099: the permit wait sits inside the timeout race; the permit rides the
        // blocking closure so it is released when the match work actually ends.
        let permit = crate::state::acquire_search_permit(
            state.search_permits.as_ref(),
            &state.prom.search_permits_in_use,
        )
        .await;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            // Admission and the dynamic cancellation knob are read on this blocking
            // thread so a queued cluster writer remains inside the request timeout.
            let (max_batch, cooperative_cancel) = {
                let cluster = state_inner.cluster.read();
                let config = cluster.per_shard_config();
                (config.max_percolate_batch, config.cooperative_cancel)
            };
            if titles.len() > max_batch {
                return Err(PercFail::Validation(format!(
                    "batch of {} documents exceeds max_percolate_batch ({max_batch})",
                    titles.len()
                )));
            }

            let run_pool = |stable_view: Option<&ClusterReadView<'_>>| {
                state_inner.pool.install(|| {
                    use rayon::prelude::*;
                    let deadline = requested_deadline.filter(|_| cooperative_cancel);
                    // Without source enrichment the read guard is taken PER TITLE: the
                    // RwLock is fair, so a queued vocabulary writer cannot stall every
                    // subsequent read for the whole batch. Source requests use the
                    // core mutation-frozen view through match and source cloning.
                    let one = |t: &str| -> Result<(ScoredIds, MatchStats), ShardError> {
                        match (stable_view, rank.as_ref()) {
                            (Some(view), Some(spec)) => {
                                let (rows, stats) = view.percolate_filtered_ranked(
                                    t,
                                    &filter,
                                    include_broad,
                                    spec,
                                )?;
                                Ok((
                                    rows.into_iter()
                                        .map(|(id, score)| (id, Some(score)))
                                        .collect(),
                                    stats,
                                ))
                            }
                            (Some(view), None) => {
                                let (ids, stats) =
                                    view.percolate_filtered_with_stats(t, &filter, include_broad)?;
                                Ok((ids.into_iter().map(|id| (id, None)).collect(), stats))
                            }
                            (None, Some(spec)) => {
                                let cluster = state_inner.cluster.read();
                                let (rows, stats) = cluster.percolate_filtered_ranked(
                                    t,
                                    &filter,
                                    include_broad,
                                    spec,
                                )?;
                                Ok((
                                    rows.into_iter()
                                        .map(|(id, score)| (id, Some(score)))
                                        .collect(),
                                    stats,
                                ))
                            }
                            (None, None) => {
                                let cluster = state_inner.cluster.read();
                                let (ids, stats) = cluster.percolate_filtered_with_stats(
                                    t,
                                    &filter,
                                    include_broad,
                                )?;
                                Ok((ids.into_iter().map(|id| (id, None)).collect(), stats))
                            }
                        }
                    };
                    let run = (|| {
                        let results = titles
                            .par_iter()
                            .map(|t| {
                                // Cooperative TITLE boundary (ADR-099): expired work stops
                                // between titles instead of running the batch to completion.
                                if deadline.is_some_and(|d| Instant::now() >= d) {
                                    return Err(PercFail::Cancelled);
                                }
                                one(t).map_err(PercFail::Shard)
                            })
                            .collect::<Result<Vec<_>, PercFail>>()?;
                        let sources = match (source_fetch, stable_view) {
                            (Some(fetch), Some(view)) => {
                                let mut sources = HashMap::new();
                                for id in source_ids(&results, fetch) {
                                    if deadline.is_some_and(|d| Instant::now() >= d) {
                                        return Err(PercFail::Cancelled);
                                    }
                                    let source =
                                        view.get_source(id).map_err(PercFail::Source)?.ok_or(
                                            PercFail::Source(ShardError::SourceUnavailable(id)),
                                        )?;
                                    sources.insert(id, source);
                                }
                                Some(sources)
                            }
                            _ => None,
                        };
                        Ok(PercolateRun { results, sources })
                    })();
                    if matches!(run, Err(PercFail::Cancelled)) {
                        state_inner
                            .prom
                            .match_cancellations_total
                            .with_label_values(&[endpoint])
                            .inc();
                    }
                    run
                })
            };

            if source_fetch.is_some() {
                // Source waiters must not occupy the shared Rayon pool. Acquire
                // both the HTTP write funnel and the core mutation-frozen view on
                // this blocking thread before entering the pool. The core fence
                // also covers direct `ClusterEngine` mutations.
                let _write_guard = state_inner.write_serial.lock();
                let cluster = state_inner.cluster.read();
                let stable_view = cluster.consistent_read_view();
                run_pool(Some(&stable_view))
            } else {
                run_pool(None)
            }
        })
        .await
    };
    match tokio::time::timeout(timeout, fut).await {
        Ok(Ok(Ok(results))) => Ok(results),
        Ok(Ok(Err(PercFail::Cancelled))) => {
            // The cooperative deadline fired before the tokio timer: same contract as
            // the response deadline — 408, results discarded, never an empty 200.
            state
                .prom
                .http_requests_total
                .with_label_values(&[endpoint, "408"])
                .inc();
            Err(ApiError::response(
                StatusCode::REQUEST_TIMEOUT,
                "timeout",
                format!("percolate timed out after {}ms", timeout.as_millis()),
            ))
        }
        Ok(Ok(Err(PercFail::Validation(message)))) => {
            state
                .prom
                .http_requests_total
                .with_label_values(&[endpoint, "400"])
                .inc();
            Err(ApiError::response(
                StatusCode::BAD_REQUEST,
                "validation_error",
                message,
            ))
        }
        Ok(Ok(Err(PercFail::Shard(e)))) => {
            // A failed shard probe fails the percolate rather than shrinking the
            // union (the zero-false-negative posture) — surface it.
            state
                .prom
                .http_requests_total
                .with_label_values(&[endpoint, "502"])
                .inc();
            Err(ApiError::response(
                StatusCode::BAD_GATEWAY,
                "shard_unreachable",
                format!("a shard probe failed; result withheld rather than truncated: {e}"),
            ))
        }
        Ok(Ok(Err(PercFail::Source(e)))) => Err(source_unavailable(state, endpoint, &e)),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "cluster percolate task panicked");
            state
                .prom
                .http_requests_total
                .with_label_values(&[endpoint, "500"])
                .inc();
            Err(ApiError::response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "search_error",
                "internal percolate task failed",
            ))
        }
        Err(_) => {
            state
                .prom
                .http_requests_total
                .with_label_values(&[endpoint, "408"])
                .inc();
            Err(ApiError::response(
                StatusCode::REQUEST_TIMEOUT,
                "timeout",
                format!("percolate timed out after {}ms", timeout.as_millis()),
            ))
        }
    }
}

/// Classify requested-source failures: a remote cluster without source transport
/// is 501; a confirmed in-process hit whose source is absent is a fail-closed 502.
fn source_unavailable(state: &ClusterAppState, endpoint: &'static str, e: &ShardError) -> Reject {
    if matches!(e, ShardError::SourceUnavailable(_)) {
        state
            .prom
            .http_requests_total
            .with_label_values(&[endpoint, "502"])
            .inc();
        return ApiError::response(StatusCode::BAD_GATEWAY, "source_unavailable", e.to_string());
    }
    state
        .prom
        .http_requests_total
        .with_label_values(&[endpoint, "501"])
        .inc();
    info!(error = %e, "include_source unavailable on this cluster");
    ApiError::response(
        StatusCode::NOT_IMPLEMENTED,
        "not_supported_in_cluster_mode",
        format!("include_source is not available on this cluster: {e}"),
    )
}
