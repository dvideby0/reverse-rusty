//! Cluster-mode percolate handlers (ADR-070): `POST /_search` + `POST /_mpercolate`
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

use std::sync::Arc;
use std::time::Instant;

use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use tracing::{info, instrument, warn};

use reverse_rusty::cluster::{ClusterEngine, ShardError};
use reverse_rusty::segment::MatchStats;

use crate::dto::{ApiError, HitSource};
use crate::handlers::search::{resolve_percolate, to_rank_spec, DocBody, RankBody};
use crate::state::ClusterAppState;

/// A request filter resolved for the cluster percolate calls.
type FilterSpec = Vec<(String, Vec<String>)>;

#[derive(Deserialize)]
pub(crate) struct ClusterSearchBody {
    document: Option<DocBody>,
    documents: Option<Vec<DocBody>>,
    filter: Option<serde_json::Value>,
    query: Option<serde_json::Value>,
    /// Per-request broad-lane override (falls back to the server default).
    include_broad: Option<bool>,
    timeout_ms: Option<u64>,
    size: Option<usize>,
    from: Option<usize>,
    /// Include each hit's stored query source (default false in cluster mode — it
    /// costs a per-hit source probe; explicit `true` on a remote cluster is a 501).
    include_source: Option<bool>,
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

#[derive(Serialize, Clone)]
struct StatsResponse {
    unique_candidates: u32,
    broad_candidates: u32,
    postings_scanned: u32,
    matches: u32,
    probes_attempted: u32,
    probes_skipped: u32,
}

impl From<MatchStats> for StatsResponse {
    fn from(s: MatchStats) -> Self {
        Self {
            unique_candidates: s.unique_candidates,
            broad_candidates: s.broad_candidates,
            postings_scanned: s.postings_scanned,
            matches: s.matches,
            probes_attempted: s.probes_attempted,
            probes_skipped: s.probes_skipped,
        }
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
/// attaching `_source` via the cluster's source probe. `Err` only when sources were
/// explicitly requested but this cluster cannot serve them (remote shards, v1).
fn attach_hits(
    state: &ClusterAppState,
    rows: &[(u64, Option<i64>)],
    include_source: bool,
) -> Result<Vec<ClusterHitItem>, ShardError> {
    let cluster = state.cluster.read();
    rows.iter()
        .map(|&(id, score)| {
            let source = if include_source {
                cluster.get_source(id)?.map(|query| HitSource { query })
            } else {
                None
            };
            Ok(ClusterHitItem {
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

/// POST /_search — percolate one or more titles against the cluster.
#[instrument(skip_all)]
pub(crate) async fn cluster_search(
    State(state): State<Arc<ClusterAppState>>,
    Json(body): Json<ClusterSearchBody>,
) -> Result<Json<ClusterSearchResponse>, Reject> {
    let start = Instant::now();
    reject_unsupported(&state, "search", body.explain.unwrap_or(false))?;

    let include_broad = body.include_broad.unwrap_or(state.include_broad);
    let include_source = body.include_source.unwrap_or(false);
    let include_profile = body.profile.unwrap_or(false);
    let timeout = tokio::time::Duration::from_millis(body.timeout_ms.unwrap_or(30_000));
    let page_size = body.size.unwrap_or(1000);
    let page_from = body.from.unwrap_or(0);
    let rank_spec = to_rank_spec(body.rank);
    let ranked = rank_spec.is_some();

    let (titles, single, filter_spec) =
        match resolve_percolate(body.document, body.documents, body.filter, body.query) {
            Ok(t) => t,
            Err(msg) => {
                state
                    .prom
                    .http_requests_total
                    .with_label_values(&["search", "400"])
                    .inc();
                return Err(ApiError::response(
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    msg,
                ));
            }
        };

    // ADR-099: arm cooperative (per-title) cancellation only for an EXPLICIT
    // timeout_ms. Lock-free here — the dynamic kill-switch is resolved INSIDE the
    // blocking task (under the timeout race), so a held cluster write lock (e.g. a
    // vocab rebuild) can never stall this async handler past its own deadline (codex).
    let deadline = body.timeout_ms.is_some().then(|| start + timeout);
    let results = percolate_blocking(
        &state,
        titles,
        filter_spec,
        include_broad,
        rank_spec,
        timeout,
        deadline,
        "search",
    )
    .await?;

    let slow_ms = state.slow_query_threshold_ms;
    let took = start.elapsed();
    if slow_ms > 0 && took.as_millis() as u64 >= slow_ms {
        warn!(
            took_ms = took.as_millis() as u64,
            titles = results.len(),
            "slow cluster search"
        );
    }

    let attach = |rows: &ScoredIds| {
        attach_hits(
            &state,
            &order_and_page(rows, ranked, page_from, page_size),
            include_source,
        )
    };
    let response = if single {
        let (rows, stats) = &results[0];
        let hits = attach(rows).map_err(|e| source_unavailable(&state, "search", &e))?;
        ClusterSearchResponse {
            took_ms: took.as_secs_f64() * 1000.0,
            hits: ClusterHits {
                total: rows.len(),
                hits,
            },
            slots: None,
            profile: include_profile.then(|| StatsResponse::from(*stats)),
        }
    } else {
        let mut slots = Vec::with_capacity(results.len());
        let mut merged = MatchStats::default();
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
            took_ms: took.as_secs_f64() * 1000.0,
            hits: ClusterHits {
                total: all.len(),
                hits,
            },
            slots: Some(slots),
            profile: include_profile.then(|| StatsResponse::from(merged)),
        }
    };

    state
        .prom
        .http_requests_total
        .with_label_values(&["search", "200"])
        .inc();
    state
        .prom
        .http_request_duration
        .with_label_values(&["search"])
        .observe(start.elapsed().as_secs_f64());
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

    let max_batch = {
        let cluster = state.cluster.read();
        cluster.per_shard_config().max_percolate_batch
    };
    if titles.len() > max_batch {
        state
            .prom
            .http_requests_total
            .with_label_values(&["mpercolate", "400"])
            .inc();
        return Err(ApiError::response(
            StatusCode::BAD_REQUEST,
            "validation_error",
            format!(
                "batch of {} exceeds max_percolate_batch {max_batch}",
                titles.len()
            ),
        ));
    }

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
    )
    .await?;

    let mut responses = Vec::with_capacity(results.len());
    for (rows, _stats) in &results {
        // Per-slot rank + `from`/`size`, the single-node `/_mpercolate` semantics.
        let hits = attach_hits(
            &state,
            &order_and_page(rows, ranked, page_from, page_size),
            include_source,
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
    Shard(ShardError),
    Cancelled,
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
) -> Result<Vec<(ScoredIds, MatchStats)>, Reject> {
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
            state_inner.pool.install(|| {
                use rayon::prelude::*;
                // Resolve the dynamic kill-switch HERE — on a rayon thread, inside the
                // timeout race — so a held cluster write lock stalls only this blocking
                // task (which the client can time out on), never the async handler.
                let deadline = requested_deadline.filter(|_| {
                    state_inner
                        .cluster
                        .read()
                        .per_shard_config()
                        .cooperative_cancel
                });
                // The read guard is taken PER TITLE, not hoisted over the batch: the
                // RwLock is fair, so one long-held batch guard would let a queued vocab
                // writer stall every subsequent read for the whole batch duration
                // (review finding). Each title still evaluates under one consistent
                // engine view; a concurrent vocab rebuild may split a batch across
                // vocab epochs — the same visibility a single-node client gets when a
                // PUT /_vocab lands between two requests.
                let one = |cluster: &ClusterEngine,
                           t: &str|
                 -> Result<(ScoredIds, MatchStats), ShardError> {
                    if let Some(spec) = &rank {
                        let (rows, st) =
                            cluster.percolate_filtered_ranked(t, &filter, include_broad, spec)?;
                        Ok((rows.into_iter().map(|(id, s)| (id, Some(s))).collect(), st))
                    } else {
                        let (ids, st) =
                            cluster.percolate_filtered_with_stats(t, &filter, include_broad)?;
                        Ok((ids.into_iter().map(|id| (id, None)).collect(), st))
                    }
                };
                let r = titles
                    .par_iter()
                    .map(|t| {
                        // Cooperative TITLE boundary (ADR-099): expired work stops
                        // between titles instead of running the batch to completion.
                        // (Within one title, the in-shard match is bounded by the
                        // per-RPC deadline on a remote cluster — the stated ADR-099
                        // deferral for shard-side cancellation.)
                        if deadline.is_some_and(|d| Instant::now() >= d) {
                            return Err(PercFail::Cancelled);
                        }
                        one(&state_inner.cluster.read(), t).map_err(PercFail::Shard)
                    })
                    .collect::<Result<Vec<_>, PercFail>>();
                if matches!(r, Err(PercFail::Cancelled)) {
                    state_inner
                        .prom
                        .match_cancellations_total
                        .with_label_values(&[endpoint])
                        .inc();
                }
                r
            })
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

/// The explicit-`include_source`-on-a-remote-cluster rejection (a clear 501, never a
/// silently source-less hit).
fn source_unavailable(state: &ClusterAppState, endpoint: &'static str, e: &ShardError) -> Reject {
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
