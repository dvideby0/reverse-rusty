//! `POST /_mpercolate` — the strict native batch throughput path. Percolates
//! a batch of documents in one request, evaluating the columnar broad lane ONCE per
//! title-batch (ADR-026) so the broad-posting scan amortizes across the batch. Owns the
//! `/_mpercolate` request/response DTOs; the rich per-title path lives in
//! [`super::percolate`].

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
use tracing::{error, info, instrument};

use reverse_rusty::segment::{BatchMatchOptions, BroadStrategy};

use crate::dto::{ApiError, HitSource};
use crate::handlers::doc::QUERY_INDEX;
use crate::metrics::PrometheusMetrics;
use crate::state::AppState;

use super::parse_named_time_value;
use super::rank::{order_and_page, to_rank_spec, RankBody};
use super::resolve::{resolve_percolate_strict, FilterSpec};
use super::{DocBody, SearchHitItem, SearchHits};

// -- POST /_mpercolate (batch percolation; ordered responses[])
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MPercolateDoc {
    pub(crate) title: String,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MPercolateParams {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MPercolateBody {
    /// The batch of documents to percolate. Each entry is matched independently;
    /// `responses[i]` corresponds to `documents[i]`.
    pub(super) documents: Option<Vec<MPercolateDoc>>,
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
    /// ES/OS spelling for `include_source`.
    #[serde(rename = "_source")]
    pub(super) source: Option<bool>,
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
    /// ES/OS time value (`250ms`, `2s`, ...), equivalent to `timeout_ms`.
    pub(super) timeout: Option<String>,
    /// Include the top-level broad-lane summary in the response (default: false).
    pub(super) profile: Option<bool>,
    /// Accepted only when false. Per-hit explanations belong to `/_search`.
    pub(super) explain: Option<bool>,
    /// ES/OS fail-closed spelling. Partial slot success is unsupported.
    pub(super) allow_partial_search_results: Option<bool>,
}

pub(crate) struct PreparedMPercolate {
    pub(crate) titles: Vec<String>,
    pub(crate) filter: FilterSpec,
    pub(crate) include_broad: Option<bool>,
    pub(crate) include_source: bool,
    pub(crate) size: usize,
    pub(crate) page_from: usize,
    pub(crate) rank: Option<reverse_rusty::RankSpec>,
    pub(crate) timeout: Duration,
    pub(crate) explicit_timeout: bool,
    pub(crate) profile: bool,
}

pub(crate) fn prepare_mpercolate(
    body: MPercolateBody,
    default_include_source: bool,
) -> Result<PreparedMPercolate, String> {
    if body.explain.unwrap_or(false) {
        return Err(
            "`explain=true` is not supported on `/_mpercolate`; use `/_search` per document"
                .to_string(),
        );
    }
    if body.allow_partial_search_results.unwrap_or(false) {
        return Err(
            "`allow_partial_search_results=true` is unsupported; `/_mpercolate` fails the whole batch"
                .to_string(),
        );
    }
    if body.include_source.is_some() && body.source.is_some() {
        return Err(
            "`include_source` and `_source` are aliases; specify exactly one of them".to_string(),
        );
    }
    if body.timeout_ms.is_some() && body.timeout.is_some() {
        return Err(
            "`timeout_ms` and `timeout` are aliases; specify exactly one of them".to_string(),
        );
    }

    let explicit_timeout = body.timeout_ms.is_some() || body.timeout.is_some();
    let timeout = match (body.timeout_ms, body.timeout) {
        (Some(ms), None) => Duration::from_millis(ms),
        (None, Some(raw)) => parse_named_time_value("timeout", &raw)?,
        (None, None) => Duration::from_secs(30),
        (Some(_), Some(_)) => unreachable!("timeout aliases rejected above"),
    };
    let documents = body.documents.map(|documents| {
        documents
            .into_iter()
            .map(|document| DocBody {
                title: document.title,
            })
            .collect()
    });
    let (titles, _single, filter) =
        resolve_percolate_strict(None, documents, body.filter, body.query)?;

    Ok(PreparedMPercolate {
        titles,
        filter,
        include_broad: body.include_broad,
        include_source: body
            .include_source
            .or(body.source)
            .unwrap_or(default_include_source),
        size: body.size.unwrap_or(1000),
        page_from: body.from.unwrap_or(0),
        rank: to_rank_spec(body.rank),
        timeout,
        explicit_timeout,
        profile: body.profile.unwrap_or(false),
    })
}

#[derive(Serialize)]
pub(crate) struct MPercolateResponse {
    /// ES/OS-compatible whole-millisecond batch duration.
    took: u64,
    took_ms: f64,
    /// One entry per input document, in submission order.
    pub(super) responses: Vec<PercolateItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) broad: Option<BroadSummary>,
}

#[derive(Serialize)]
pub(super) struct PercolateItem {
    timed_out: bool,
    status: u16,
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

type Reject = (StatusCode, Json<ApiError>);

pub(crate) fn mpercolate_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Reject {
    prom.http_requests_total
        .with_label_values(&["mpercolate", status.as_str()])
        .inc();
    ApiError::response(status, error_type, reason)
}

pub(crate) fn mpercolate_body_rejection(prom: &PrometheusMetrics, error: &JsonRejection) -> Reject {
    let status = error.status();
    if status == StatusCode::PAYLOAD_TOO_LARGE || status == StatusCode::UNSUPPORTED_MEDIA_TYPE {
        let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
            "payload_too_large"
        } else {
            "unsupported_media_type"
        };
        return mpercolate_rejection(
            prom,
            status,
            error_type,
            format!("invalid mpercolate body: {error}"),
        );
    }
    mpercolate_rejection(
        prom,
        StatusCode::BAD_REQUEST,
        "validation_error",
        format!("invalid mpercolate body: {error}"),
    )
}

pub(crate) fn mpercolate_query_rejection(
    prom: &PrometheusMetrics,
    error: &QueryRejection,
) -> Reject {
    mpercolate_rejection(
        prom,
        StatusCode::BAD_REQUEST,
        "validation_error",
        format!("invalid mpercolate query parameters: {error}"),
    )
}

/// Strict HTTP extractor boundary for local `POST /_mpercolate`.
#[instrument(skip_all)]
pub(crate) async fn mpercolate_route(
    State(state): State<Arc<AppState>>,
    params: Result<Query<MPercolateParams>, QueryRejection>,
    body: Result<Json<MPercolateBody>, JsonRejection>,
) -> Result<Json<MPercolateResponse>, Reject> {
    let _duration = state
        .prom
        .http_request_duration
        .with_label_values(&["mpercolate"])
        .start_timer();
    let Query(_) = params.map_err(|error| mpercolate_query_rejection(&state.prom, &error))?;
    let Json(body) = body.map_err(|error| mpercolate_body_rejection(&state.prom, &error))?;
    mpercolate_inner(state, body).await
}

/// POST /_mpercolate — strict native batch percolation.
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
#[cfg(test)]
pub(crate) async fn mpercolate(
    State(state): State<Arc<AppState>>,
    Json(body): Json<MPercolateBody>,
) -> Result<Json<MPercolateResponse>, Reject> {
    let _duration = state
        .prom
        .http_request_duration
        .with_label_values(&["mpercolate"])
        .start_timer();
    mpercolate_inner(state, body).await
}

async fn mpercolate_inner(
    state: Arc<AppState>,
    body: MPercolateBody,
) -> Result<Json<MPercolateResponse>, Reject> {
    let start = Instant::now();

    let prepared = prepare_mpercolate(body, true).map_err(|message| {
        mpercolate_rejection(
            &state.prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            message,
        )
    })?;
    let PreparedMPercolate {
        titles,
        filter: filter_spec,
        include_broad,
        include_source,
        size: page_size,
        page_from,
        rank: rank_raw,
        timeout,
        explicit_timeout,
        profile: include_profile,
    } = prepared;
    let include_broad = include_broad.unwrap_or(state.include_broad);

    // Empty batch: a valid no-op — return an empty responses[] without scheduling
    // any work.
    if titles.is_empty() {
        let took_ms = start.elapsed().as_secs_f64() * 1000.0;
        state
            .prom
            .http_requests_total
            .with_label_values(&["mpercolate", "200"])
            .inc();
        return Ok(Json(MPercolateResponse {
            took: took_ms.floor() as u64,
            took_ms,
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
        broad_prefilter: cfg.broad_prefilter,
    };

    let pred = snap.compile_tag_predicate(&filter_spec);
    let match_snap = Arc::clone(&snap);
    let state_inner = Arc::clone(&state);
    // ADR-099: arm cooperative cancellation only for an EXPLICIT timeout control, gated by
    // the dynamic kill-switch. On expiry the WHOLE batch 408s — never a partially
    // filled responses[] (a missing slot is indistinguishable from an empty match set).
    let deadline = (explicit_timeout && cfg.cooperative_cancel).then(|| start + timeout);
    let search_fut = async {
        // Permit wait inside the timeout race; the permit rides the closure (ADR-099).
        let permit = crate::state::acquire_search_permit(
            state.search_permits.as_ref(),
            &state.prom.search_permits_in_use,
        )
        .await;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            state_inner.pool.install(|| {
                let r = match_snap
                    .try_match_titles_batch_with_stats_filtered(&titles, opts, &pred, deadline);
                if r.is_err() {
                    state_inner
                        .prom
                        .match_cancellations_total
                        .with_label_values(&["mpercolate"])
                        .inc();
                }
                // Match-feedback capture (ADR-103): opt-in, post-match.
                if match_snap.config().alias_feedback_capture {
                    if let Ok((results, _)) = &r {
                        let mut fb = state_inner.feedback.lock();
                        for (idx, ids) in results {
                            let toks = reverse_rusty::corpus::tokenize(&titles[*idx]);
                            fb.observe(&toks, ids);
                        }
                    }
                }
                r
            })
        })
        .await
    };

    let (results, stats) = match tokio::time::timeout(timeout, search_fut).await {
        Ok(Ok(Ok(r))) => r,
        Ok(Ok(Err(_cancelled))) => {
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
        Ok(Err(e)) => {
            error!(error = %e, "mpercolate task panicked");
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
    // Hot-tier meters (class H, ADR-105) — all-zero while θ is off.
    state
        .prom
        .hot_batches_total
        .inc_by(u64::from(stats.hot_batches));
    state
        .prom
        .hot_postings_scanned_total
        .inc_by(u64::from(stats.hot_postings_scanned));
    state
        .prom
        .hot_queries_evaluated_total
        .inc_by(u64::from(stats.hot_queries_evaluated));
    state
        .prom
        .hot_candidates_total
        .inc_by(u64::from(stats.hot_candidates));

    // Reassemble per-document results in submission order (`results` is
    // (global_index, ids) with index in 0..num_docs).
    let mut per_doc: Vec<Vec<u64>> = vec![Vec::new(); num_docs];
    for (idx, ids) in results {
        if let Some(slot) = per_doc.get_mut(idx) {
            *slot = ids;
        }
    }

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
                .map(|(id, score)| -> Result<SearchHitItem, Reject> {
                    let source = if include_source {
                        let query = snap.get_query_source(id).ok_or_else(|| {
                            mpercolate_rejection(
                                &state.prom,
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "source_unavailable",
                                format!(
                                    "query {id} matched but its stored source is unavailable; \
                                     repair or restore sources.dat"
                                ),
                            )
                        })?;
                        Some(HitSource { query })
                    } else {
                        None
                    };
                    Ok(SearchHitItem {
                        _index: QUERY_INDEX,
                        _id: id,
                        _score: score,
                        _source: source,
                        _explanation: None,
                    })
                })
                .collect::<Result<Vec<_>, Reject>>()?;
            Ok(PercolateItem {
                timed_out: false,
                status: StatusCode::OK.as_u16(),
                hits: SearchHits { total, hits },
            })
        })
        .collect::<Result<Vec<_>, Reject>>()?;

    let took_ms = start.elapsed().as_secs_f64() * 1000.0;
    // Build the summary lazily (only when requested) — `then_some` would build it
    // even when `profile` is false.
    let broad = if include_profile {
        Some(BroadSummary {
            // Report the EFFECTIVE strategy: a Columnar request runs inline while multi-word
            // aliases are active (the columnar kernel is single-view, ADR-061), so the profile
            // must say `inline` to match what actually ran — not the requested option (codex R9).
            strategy: if matches!(opts.broad_strategy, BroadStrategy::Columnar)
                && !snap.normalizer().has_multiword_aliases()
            {
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
    Ok(Json(MPercolateResponse {
        took: took_ms.floor() as u64,
        took_ms,
        responses,
        broad,
    }))
}
