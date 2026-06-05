//! `POST /_mpercolate` — the batch throughput path (ES `_msearch`-shaped). Percolates
//! a batch of documents in one request, evaluating the columnar broad lane ONCE per
//! title-batch (ADR-026) so the broad-posting scan amortizes across the batch. Owns the
//! `/_mpercolate` request/response DTOs; the rich per-title path lives in
//! [`super::percolate`].

use std::sync::Arc;
use std::time::Instant;

use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};

use reverse_rusty::segment::{BatchMatchOptions, BroadStrategy};

use crate::dto::{ApiError, HitSource};
use crate::state::AppState;

use super::rank::{order_and_page, to_rank_spec, RankBody};
use super::resolve::resolve_percolate;
use super::{DocBody, SearchHitItem, SearchHits};

// -- POST /_mpercolate (batch percolation; ES `_msearch`-shaped responses[])
#[derive(Deserialize)]
pub(crate) struct MPercolateBody {
    /// The batch of documents to percolate. Each entry is matched independently;
    /// `responses[i]` corresponds to `documents[i]`.
    pub(super) documents: Option<Vec<DocBody>>,
    /// Native tag filter (ADR-049): an object `{key: value|[values]}` applied to every
    /// document in the batch.
    pub(super) filter: Option<serde_json::Value>,
    /// ES-compatible percolate envelope (see [`super::percolate::SearchBody::query`]); when
    /// present the batch documents and filter are taken from here.
    pub(super) query: Option<serde_json::Value>,
    /// Per-request override of the server's broad-lane default. When set, controls
    /// whether class-C (broad) queries are evaluated for this batch.
    pub(super) include_broad: Option<bool>,
    /// Include original query text in each hit (default: true).
    pub(super) include_source: Option<bool>,
    /// Maximum hits to return per document (default: 1000).
    pub(super) size: Option<usize>,
    /// Per-document offset into each document's hits for pagination (default: 0).
    pub(super) from: Option<usize>,
    /// Optional ranking (ADR-059): order each document's hits by a numeric priority
    /// tag and/or request boosts before applying `from`/`size`. Absent (or empty) ⇒
    /// hits keep engine order — byte-identical to the pre-ranking response.
    pub(super) rank: Option<RankBody>,
    /// Per-request timeout in milliseconds (default: 30000).
    pub(super) timeout_ms: Option<u64>,
    /// Include the top-level broad-lane summary in the response (default: false).
    pub(super) profile: Option<bool>,
}

#[derive(Serialize)]
pub(crate) struct MPercolateResponse {
    took_ms: f64,
    /// One entry per input document, in submission order.
    pub(super) responses: Vec<PercolateItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) broad: Option<BroadSummary>,
}

#[derive(Serialize)]
pub(super) struct PercolateItem {
    pub(super) hits: SearchHits,
}

/// Top-level broad-lane summary for a `/_mpercolate` batch — surfaces the columnar
/// evaluator's amortization (see `MatchStats` / ADR-026). `broad_postings_scanned`
/// rising far slower than `broad_candidates` as `batch_size` grows IS the win.
#[derive(Serialize)]
pub(super) struct BroadSummary {
    pub(super) strategy: &'static str,
    pub(super) batch_size: usize,
    broad_batches: u32,
    broad_postings_scanned: u32,
    broad_queries_evaluated: u32,
    broad_candidates: u32,
    pub(super) total_matches: u32,
}

/// POST /_mpercolate — batch percolation (ES `_msearch`-shaped).
///
/// Percolates a batch of documents in one request, evaluating the broad lane
/// ONCE per title-batch (columnar; ADR-026) instead of once per document, so the
/// broad-posting scan amortizes across the batch. Returns a `responses[]`
/// envelope, one entry per input document in submission order. The broad lane is
/// opt-in per request (`include_broad`, falling back to the server default).
///
/// This is the throughput path; `/_search` remains the rich path. Because the
/// broad lane is amortized per batch, `/_mpercolate` does not produce per-document
/// candidate/posting stats — only an optional top-level broad summary (`profile`).
#[instrument(skip_all)]
pub(crate) async fn mpercolate(
    State(state): State<Arc<AppState>>,
    Json(body): Json<MPercolateBody>,
) -> Result<Json<MPercolateResponse>, (StatusCode, Json<ApiError>)> {
    let start = Instant::now();

    // Resolve the batch + tag filter from the native (`documents` + `filter`) or ES
    // (`query`) shape; an unsupported request is a 400.
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

    let include_broad = body.include_broad.unwrap_or(state.include_broad);
    let include_source = body.include_source.unwrap_or(true);
    let page_size = body.size.unwrap_or(1000);
    let page_from = body.from.unwrap_or(0);
    let rank_raw = to_rank_spec(body.rank);
    let include_profile = body.profile.unwrap_or(false);
    let timeout = tokio::time::Duration::from_millis(body.timeout_ms.unwrap_or(30_000));

    // Empty batch: a valid no-op — return an empty responses[] without scheduling
    // any work.
    if titles.is_empty() {
        state
            .prom
            .http_requests_total
            .with_label_values(&["mpercolate", "200"])
            .inc();
        return Ok(Json(MPercolateResponse {
            took_ms: start.elapsed().as_secs_f64() * 1000.0,
            responses: Vec::new(),
            broad: None,
        }));
    }

    let num_docs = titles.len();

    // Read the live broad-lane config from the snapshot (ADR-026 dynamic knobs):
    // batch size, columnar-vs-inline kill-switch, pure-anchor materialization, and
    // the max batch size that bounds per-request work.
    let snap = Arc::clone(&state.snapshot.load());
    let cfg = snap.config();
    if num_docs > cfg.max_percolate_batch {
        state
            .prom
            .http_requests_total
            .with_label_values(&["mpercolate", "400"])
            .inc();
        return Err(ApiError::response(
            StatusCode::BAD_REQUEST,
            "validation_error",
            format!(
                "batch of {num_docs} documents exceeds max_percolate_batch ({})",
                cfg.max_percolate_batch
            ),
        ));
    }
    let opts = BatchMatchOptions {
        include_broad,
        broad_batch_size: cfg.broad_batch_size,
        broad_strategy: if cfg.broad_columnar {
            BroadStrategy::Columnar
        } else {
            BroadStrategy::Inline
        },
        broad_materialize: cfg.broad_materialize,
    };

    let pred = snap.compile_tag_predicate(&filter_spec);
    let state_inner = Arc::clone(&state);
    let search_fut = tokio::task::spawn_blocking(move || {
        state_inner
            .pool
            .install(|| snap.match_titles_batch_with_stats_filtered(&titles, opts, &pred))
    });

    let (results, stats) = match tokio::time::timeout(timeout, search_fut).await {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => {
            eprintln!("mpercolate task panicked: {e}");
            state
                .prom
                .http_requests_total
                .with_label_values(&["mpercolate", "500"])
                .inc();
            return Err(ApiError::response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "search_error",
                "internal percolate task failed",
            ));
        }
        Err(_) => {
            state
                .prom
                .http_requests_total
                .with_label_values(&["mpercolate", "408"])
                .inc();
            return Err(ApiError::response(
                StatusCode::REQUEST_TIMEOUT,
                "timeout",
                format!("mpercolate timed out after {}ms", timeout.as_millis()),
            ));
        }
    };

    // Broad-lane meters (cumulative across requests).
    state
        .prom
        .broad_batches_total
        .inc_by(u64::from(stats.broad_batches));
    state
        .prom
        .broad_postings_scanned_total
        .inc_by(u64::from(stats.broad_postings_scanned));
    state
        .prom
        .broad_queries_evaluated_total
        .inc_by(u64::from(stats.broad_queries_evaluated));
    state
        .prom
        .broad_candidates_total
        .inc_by(u64::from(stats.broad_candidates));

    // Reassemble per-document results in submission order (`results` is
    // (global_index, ids) with index in 0..num_docs).
    let mut per_doc: Vec<Vec<u64>> = vec![Vec::new(); num_docs];
    for (idx, ids) in results {
        if let Some(slot) = per_doc.get_mut(idx) {
            *slot = ids;
        }
    }

    let snap = state.snapshot.load();
    let cspec = rank_raw
        .as_ref()
        .map(|r| snap.compile_rank_spec(r))
        .filter(|c| !c.is_noop());
    let responses: Vec<PercolateItem> = per_doc
        .into_iter()
        .map(|ids| {
            let total = ids.len();
            let hits = order_and_page(&snap, &ids, cspec.as_ref(), page_from, page_size)
                .into_iter()
                .map(|(id, score)| {
                    let source = if include_source {
                        snap.get_query_source(id).map(|q| HitSource { query: q })
                    } else {
                        None
                    };
                    SearchHitItem {
                        _id: id,
                        _score: score,
                        _source: source,
                        _explanation: None,
                    }
                })
                .collect();
            PercolateItem {
                hits: SearchHits { total, hits },
            }
        })
        .collect();

    let took_ms = start.elapsed().as_secs_f64() * 1000.0;
    // Build the summary lazily (only when requested) — `then_some` would build it
    // even when `profile` is false.
    let broad = if include_profile {
        Some(BroadSummary {
            strategy: if matches!(opts.broad_strategy, BroadStrategy::Columnar) {
                "columnar"
            } else {
                "inline"
            },
            batch_size: opts.broad_batch_size,
            broad_batches: stats.broad_batches,
            broad_postings_scanned: stats.broad_postings_scanned,
            broad_queries_evaluated: stats.broad_queries_evaluated,
            broad_candidates: stats.broad_candidates,
            total_matches: stats.matches,
        })
    } else {
        None
    };

    info!(
        titles = num_docs,
        matches = stats.matches,
        include_broad,
        took_ms = format!("{:.2}", took_ms),
        "mpercolate complete"
    );

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

    Ok(Json(MPercolateResponse {
        took_ms,
        responses,
        broad,
    }))
}
