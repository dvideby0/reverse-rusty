//! Cluster-mode percolate handlers (ADR-070): `POST /_search` + `POST /_mpercolate`
//! over [`ClusterEngine::percolate_filtered_with_stats`] — the routing + merge the
//! cluster oracles prove ≡ single-node ≡ brute. Resolves the same native + ES
//! envelopes (shared [`resolve_percolate`]). Both endpoints take a per-request
//! `include_broad` (the coordinator owns broad routing, so the per-shard toggle is
//! free here; single-node `/_search` parity is ADR-064 item 6). A request feature
//! the cluster cannot honor yet (`rank` — ADR-065 criterion 5, `explain`) is a 400,
//! never silently ignored.

use std::sync::Arc;
use std::time::Instant;

use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use tracing::{info, instrument, warn};

use reverse_rusty::cluster::ShardError;
use reverse_rusty::segment::MatchStats;

use crate::dto::{ApiError, HitSource};
use crate::handlers::search::{resolve_percolate, DocBody};
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
    /// Not supported in cluster mode (criterion 5 / explain) — present so a request
    /// using them is REJECTED loudly rather than silently un-ranked/un-explained.
    rank: Option<serde_json::Value>,
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

/// Resolve hits for one page window, optionally attaching `_source` via the
/// cluster's source probe. `Err` only when sources were explicitly requested but
/// this cluster cannot serve them (remote shards, v1).
fn page_hits(
    state: &ClusterAppState,
    ids: &[u64],
    from: usize,
    size: usize,
    include_source: bool,
) -> Result<Vec<ClusterHitItem>, ShardError> {
    let cluster = state.cluster.read();
    ids.iter()
        .skip(from)
        .take(size)
        .map(|&id| {
            let source = if include_source {
                cluster.get_source(id)?.map(|query| HitSource { query })
            } else {
                None
            };
            Ok(ClusterHitItem {
                _id: id,
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
    rank: Option<&serde_json::Value>,
    explain: bool,
) -> Result<(), Reject> {
    let msg = if rank.is_some() {
        "ranking is not supported in cluster mode yet (ADR-065 criterion 5); remove the `rank` block"
    } else if explain {
        "per-hit explain is not supported in cluster mode yet; remove `explain`"
    } else {
        return Ok(());
    };
    state
        .prom
        .http_requests_total
        .with_label_values(&[endpoint, "400"])
        .inc();
    Err(ApiError::response(
        StatusCode::BAD_REQUEST,
        "validation_error",
        msg,
    ))
}

/// POST /_search — percolate one or more titles against the cluster.
#[instrument(skip_all)]
pub(crate) async fn cluster_search(
    State(state): State<Arc<ClusterAppState>>,
    Json(body): Json<ClusterSearchBody>,
) -> Result<Json<ClusterSearchResponse>, Reject> {
    let start = Instant::now();
    reject_unsupported(
        &state,
        "search",
        body.rank.as_ref(),
        body.explain.unwrap_or(false),
    )?;

    let include_broad = body.include_broad.unwrap_or(state.include_broad);
    let include_source = body.include_source.unwrap_or(false);
    let include_profile = body.profile.unwrap_or(false);
    let timeout = tokio::time::Duration::from_millis(body.timeout_ms.unwrap_or(30_000));
    let page_size = body.size.unwrap_or(1000);
    let page_from = body.from.unwrap_or(0);

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

    let results = percolate_blocking(
        &state,
        titles,
        filter_spec,
        include_broad,
        timeout,
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

    let attach = |ids: &Vec<u64>| page_hits(&state, ids, page_from, page_size, include_source);
    let response = if single {
        let (ids, stats) = &results[0];
        let hits = attach(ids).map_err(|e| source_unavailable(&state, "search", &e))?;
        ClusterSearchResponse {
            took_ms: took.as_secs_f64() * 1000.0,
            hits: ClusterHits {
                total: ids.len(),
                hits,
            },
            slots: None,
            profile: include_profile.then(|| StatsResponse::from(*stats)),
        }
    } else {
        let mut slots = Vec::with_capacity(results.len());
        let mut merged = MatchStats::default();
        let mut all: Vec<u64> = Vec::new();
        for (slot, (ids, stats)) in results.iter().enumerate() {
            let hits = attach(ids).map_err(|e| source_unavailable(&state, "search", &e))?;
            merged.merge(*stats);
            all.extend_from_slice(ids);
            slots.push(ClusterSlotHit {
                slot,
                total: ids.len(),
                hits,
                stats: StatsResponse::from(*stats),
            });
        }
        all.sort_unstable();
        all.dedup();
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
    /// Not supported in cluster mode — rejected loudly (see `reject_unsupported`).
    rank: Option<serde_json::Value>,
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
    reject_unsupported(&state, "mpercolate", body.rank.as_ref(), false)?;

    let include_broad = body.include_broad.unwrap_or(state.include_broad);
    let include_source = body.include_source.unwrap_or(false);
    let timeout = tokio::time::Duration::from_millis(body.timeout_ms.unwrap_or(30_000));
    let page_size = body.size.unwrap_or(1000);
    let page_from = body.from.unwrap_or(0);

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

    let results = percolate_blocking(
        &state,
        titles,
        filter_spec,
        include_broad,
        timeout,
        "mpercolate",
    )
    .await?;

    let mut responses = Vec::with_capacity(results.len());
    for (ids, _stats) in &results {
        let hits = page_hits(&state, ids, page_from, page_size, include_source)
            .map_err(|e| source_unavailable(&state, "mpercolate", &e))?;
        responses.push(ClusterPercolateItem {
            hits: ClusterHits {
                total: ids.len(),
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
/// submission order.
async fn percolate_blocking(
    state: &Arc<ClusterAppState>,
    titles: Vec<String>,
    filter: FilterSpec,
    include_broad: bool,
    timeout: tokio::time::Duration,
    endpoint: &'static str,
) -> Result<Vec<(Vec<u64>, MatchStats)>, Reject> {
    let state_inner = Arc::clone(state);
    let fut = tokio::task::spawn_blocking(move || {
        state_inner.pool.install(|| {
            use rayon::prelude::*;
            // The read guard is taken PER TITLE, not hoisted over the batch: the
            // RwLock is fair, so one long-held batch guard would let a queued vocab
            // writer stall every subsequent read for the whole batch duration
            // (review finding). Each title still evaluates under one consistent
            // engine view; a concurrent vocab rebuild may split a batch across
            // vocab epochs — the same visibility a single-node client gets when a
            // PUT /_vocab lands between two requests.
            titles
                .par_iter()
                .map(|t| {
                    state_inner.cluster.read().percolate_filtered_with_stats(
                        t,
                        &filter,
                        include_broad,
                    )
                })
                .collect::<Result<Vec<_>, ShardError>>()
        })
    });
    match tokio::time::timeout(timeout, fut).await {
        Ok(Ok(Ok(results))) => Ok(results),
        Ok(Ok(Err(e))) => {
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
