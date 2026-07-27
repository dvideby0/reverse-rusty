//! `GET|POST /_search` — the rich, per-title percolate path: single- or multi-document,
//! with optional explain, per-slot stats, ranking (ADR-059) and `from`/`size`
//! pagination. Owns the `/_search` request/response DTOs; the batch throughput path
//! lives in [`super::mpercolate`].

use std::cell::RefCell;
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

use reverse_rusty::segment::{MatchScratch, MatchStats};

use crate::dto::{ApiError, HitSource};
use crate::handlers::doc::QUERY_INDEX;
use crate::state::AppState;

use super::controls::{resolve_search_controls, SearchControlInput, SearchParams};
use super::rank::{order_and_page, to_rank_spec, RankBody};
use super::resolve::resolve_percolate_strict;
use super::{CompatibilityDocBody, DocBody, SearchHitItem, SearchHits};

thread_local! {
    static SCRATCH: RefCell<MatchScratch> = RefCell::new(MatchScratch::new());
}

// -- GET|POST /_search
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SearchBody {
    document: Option<CompatibilityDocBody>,
    documents: Option<Vec<CompatibilityDocBody>>,
    /// Native tag filter (ADR-049): an object `{key: value|[values]}` narrowing the
    /// percolated candidates. Conjunction across keys, OR within a key's values.
    filter: Option<serde_json::Value>,
    /// ES-compatible percolate envelope: `{bool:{must:{percolate:{document(s)}}, filter:[…]}}`
    /// or a bare `{percolate:{document(s)}}`. When present, the documents and tag filter are
    /// taken from here instead of the native fields.
    query: Option<serde_json::Value>,
    /// Per-request broad-lane (class C) override, falling back to the server-wide
    /// `--include-broad` default when absent (ADR-073, closing ADR-064 item 6 —
    /// `/_mpercolate` and the cluster handlers already had it; here the field was
    /// silently ignored, so class-C hits read as missing data).
    include_broad: Option<bool>,
    /// Optional per-request timeout in milliseconds (default: 30000).
    timeout_ms: Option<u64>,
    /// ES/OS time-value timeout (`250ms`, `2s`, ...), equivalent to `timeout_ms`.
    timeout: Option<String>,
    /// Maximum number of hits to return (default: 1000).
    size: Option<usize>,
    /// Offset into the result set for pagination (default: 0).
    from: Option<usize>,
    /// Optional ranking (ADR-059): order hits by a numeric priority tag and/or
    /// request-supplied boosts before applying `from`/`size`. Absent (or empty) ⇒
    /// hits keep engine order — byte-identical to the pre-ranking response.
    rank: Option<RankBody>,
    /// Include original query text in each hit (default: true).
    include_source: Option<bool>,
    /// ES/OS spelling for `include_source`.
    #[serde(rename = "_source")]
    source: Option<bool>,
    /// Include per-hit explain detail showing why each query matched (default: false).
    explain: Option<bool>,
    /// Include match profile (candidate/posting stats) in the response (default: false).
    profile: Option<bool>,
}

#[derive(Serialize)]
pub(crate) struct SearchResponse {
    /// ES/OS-compatible whole-millisecond duration.
    took: u64,
    timed_out: bool,
    /// Reverse Rusty's higher-precision duration extension.
    took_ms: f64,
    pub(super) hits: SearchHits,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) slots: Option<Vec<SlotHit>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    profile: Option<StatsResponse>,
}

type Reject = (StatusCode, Json<ApiError>);

fn validation(state: &AppState, message: impl Into<String>) -> Reject {
    state
        .prom
        .http_requests_total
        .with_label_values(&["search", "400"])
        .inc();
    ApiError::response(StatusCode::BAD_REQUEST, "validation_error", message)
}

fn body_rejection(state: &AppState, error: &JsonRejection) -> Reject {
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

fn materialize_hit(
    snap: &reverse_rusty::EngineSnapshot,
    id: u64,
    score: Option<i64>,
    include_source: bool,
    explain_title: Option<&str>,
) -> Result<SearchHitItem, (&'static str, String)> {
    let source_text = if include_source || explain_title.is_some() {
        Some(snap.get_query_source(id).ok_or_else(|| {
            (
                "source_unavailable",
                format!(
                    "query {id} matched but its stored source is unavailable; repair or \
                     restore sources.dat"
                ),
            )
        })?)
    } else {
        None
    };
    let explanation = match explain_title {
        Some(title) => {
            let source = source_text.as_deref().ok_or_else(|| {
                (
                    "source_unavailable",
                    format!("query {id} matched but its stored source is unavailable"),
                )
            })?;
            Some(snap.explain_source(id, source, title).ok_or_else(|| {
                (
                    "explanation_unavailable",
                    format!("query {id} matched but its explanation is unavailable"),
                )
            })?)
        }
        None => None,
    };
    let source = if include_source {
        source_text.map(|query| HitSource { query })
    } else {
        None
    };
    Ok(SearchHitItem {
        _index: QUERY_INDEX,
        _id: id,
        _score: score,
        _source: source,
        _explanation: explanation,
    })
}

fn enrichment_error(state: &AppState, error: (&'static str, String)) -> Reject {
    state
        .prom
        .http_requests_total
        .with_label_values(&["search", "500"])
        .inc();
    ApiError::response(StatusCode::INTERNAL_SERVER_ERROR, error.0, error.1)
}

#[derive(Serialize)]
pub(super) struct SlotHit {
    slot: usize,
    pub(super) total: usize,
    pub(super) hits: Vec<SearchHitItem>,
    stats: StatsResponse,
}

#[derive(Serialize, Clone, Default)]
struct StatsResponse {
    unique_candidates: u64,
    /// Broad-lane subset of `unique_candidates` — how much of the work came from
    /// quarantined broad (class-C) queries (0 unless `include_broad`).
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

/// Route extractor for `GET|POST /_search`. Capturing extractor rejections makes
/// syntax/data failures the documented JSON 400 while preserving transport-level
/// 413/415 statuses instead of returning Axum's plain-text defaults.
#[instrument(skip_all)]
pub(crate) async fn search_route(
    State(state): State<Arc<AppState>>,
    params: Result<Query<SearchParams>, QueryRejection>,
    body: Result<Json<SearchBody>, JsonRejection>,
) -> Result<Json<SearchResponse>, Reject> {
    let _duration = state
        .prom
        .http_request_duration
        .with_label_values(&["search"])
        .start_timer();
    let Query(params) = params
        .map_err(|error| validation(&state, format!("invalid search query parameters: {error}")))?;
    let Json(body) = body.map_err(|error| body_rejection(&state, &error))?;
    search_inner(state, body, params).await
}

/// Direct entry used by handler tests.
#[cfg(test)]
pub(crate) async fn search(
    State(state): State<Arc<AppState>>,
    Json(body): Json<SearchBody>,
) -> Result<Json<SearchResponse>, Reject> {
    let _duration = state
        .prom
        .http_request_duration
        .with_label_values(&["search"])
        .start_timer();
    search_inner(state, body, SearchParams::default()).await
}

/// GET|POST /_search — percolate one or more titles.
async fn search_inner(
    state: Arc<AppState>,
    body: SearchBody,
    params: SearchParams,
) -> Result<Json<SearchResponse>, Reject> {
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
        true,
    )
    .map_err(|message| validation(&state, message))?;
    let include_broad = body.include_broad.unwrap_or(state.include_broad);
    let include_source = controls.features.include_source;
    let include_explain = controls.features.explain;
    let include_profile = controls.features.profile;
    let timeout = controls.timeout;
    let page_size = controls.size;
    let page_from = controls.from;
    let requested_deadline = if controls.explicit_timeout {
        Some(
            start
                .checked_add(timeout)
                .ok_or_else(|| validation(&state, "`timeout` is too large"))?,
        )
    } else {
        None
    };
    let rank_raw = to_rank_spec(body.rank);

    // Resolve documents + tag filter from EITHER the native shape (document/documents +
    // filter) or the ES bool/terms percolate envelope (query). A malformed/unsupported
    // request is a 400 (an unsupported query node never silently widens the result set).
    let document = body.document.map(Into::into);
    let documents = body
        .documents
        .map(|documents| documents.into_iter().map(Into::into).collect());
    let (titles, single, filter_spec) =
        match resolve_percolate_strict(document, documents, body.filter, body.query) {
            Ok(t) => t,
            Err(msg) => return Err(validation(&state, msg)),
        };
    if include_explain && !single {
        return Err(validation(
            &state,
            "`explain:true` is supported only with a single `document`; use one request per title",
        ));
    }
    let (eff_document, eff_documents) = if single {
        let title = titles.into_iter().next().unwrap_or_default();
        (Some(DocBody { title }), None)
    } else {
        let docs: Vec<DocBody> = titles.into_iter().map(|title| DocBody { title }).collect();
        (None, Some(docs))
    };

    let response = match (eff_document, eff_documents) {
        // Single document percolation.
        (Some(doc), _) => {
            let title = doc.title;
            let title_for_explain = if include_explain {
                Some(title.clone())
            } else {
                None
            };
            let prom = state.prom.clone();
            let snap = Arc::clone(&state.snapshot.load());
            let pred = snap.compile_tag_predicate(&filter_spec);
            let worker_snap = Arc::clone(&snap);
            let state_inner = Arc::clone(&state);
            // ADR-099: arm cooperative cancellation only for an explicit timeout
            // (the implicit 30s default stays a response deadline — zero deadline
            // reads on the unarmed hot path), gated by the dynamic kill-switch.
            let deadline = requested_deadline.filter(|_| snap.config().cooperative_cancel);

            let search_fut = async {
                // The permit wait sits INSIDE the timeout race below, and the permit
                // moves into the closure — released when the blocking work ends.
                let permit = crate::state::acquire_search_permit(
                    state.search_permits.as_ref(),
                    &state.prom.search_permits_in_use,
                )
                .await;
                tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    state_inner.pool.install(|| {
                        SCRATCH.with(|cell| {
                            let mut scratch = cell.borrow_mut();
                            let mut out = Vec::new();
                            let r = worker_snap
                                .try_match_title_filtered(
                                    &title,
                                    &mut scratch,
                                    &mut out,
                                    include_broad,
                                    &pred,
                                    deadline,
                                )
                                .map(|stats| (out, stats));
                            if r.is_err() {
                                // Counted in the closure so an already-408'd request
                                // still records that its work actually stopped.
                                state_inner
                                    .prom
                                    .match_cancellations_total
                                    .with_label_values(&["search"])
                                    .inc();
                            }
                            // Match-feedback capture (ADR-103): opt-in, post-match, off the
                            // engine's match path (this is the handler's blocking thread).
                            if worker_snap.config().alias_feedback_capture {
                                if let Ok((ids, _)) = &r {
                                    let toks = reverse_rusty::corpus::tokenize(&title);
                                    state_inner.feedback.lock().observe(&toks, ids);
                                }
                            }
                            r
                        })
                    })
                })
                .await
            };

            let (ids, stats) = match tokio::time::timeout(timeout, search_fut).await {
                Ok(Ok(Ok(result))) => result,
                // Cooperative cancellation racing ahead of the tokio timer is the SAME
                // outcome as the response deadline: the existing 408, results discarded
                // — never an empty 200 (ADR-099).
                Ok(Ok(Err(_cancelled))) => {
                    state
                        .prom
                        .http_requests_total
                        .with_label_values(&["search", "408"])
                        .inc();
                    return Err(ApiError::response(
                        StatusCode::REQUEST_TIMEOUT,
                        "timeout",
                        format!("search timed out after {}ms", timeout.as_millis()),
                    ));
                }
                Ok(Err(e)) => {
                    tracing::error!(error = %e, "search task panicked");
                    state
                        .prom
                        .http_requests_total
                        .with_label_values(&["search", "500"])
                        .inc();
                    return Err(ApiError::response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "search_error",
                        "internal search task failed",
                    ));
                }
                Err(_) => {
                    state
                        .prom
                        .http_requests_total
                        .with_label_values(&["search", "408"])
                        .inc();
                    return Err(ApiError::response(
                        StatusCode::REQUEST_TIMEOUT,
                        "timeout",
                        format!("search timed out after {}ms", timeout.as_millis()),
                    ));
                }
            };

            prom.match_candidates_per_title
                .observe(f64::from(stats.unique_candidates));
            prom.match_results_per_title.observe(ids.len() as f64);

            let total = ids.len();
            let cspec = rank_raw
                .as_ref()
                .map(|r| snap.compile_rank_spec(r))
                .filter(|c| !c.is_noop());
            let hits = order_and_page(&snap, &ids, cspec.as_ref(), page_from, page_size)
                .into_iter()
                .map(|(id, score)| {
                    materialize_hit(
                        &snap,
                        id,
                        score,
                        include_source,
                        title_for_explain.as_deref(),
                    )
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| enrichment_error(&state, error))?;
            let took_ms = start.elapsed().as_secs_f64() * 1000.0;
            info!(
                titles = 1,
                matches = total,
                took_ms = format!("{:.2}", took_ms),
                "search complete"
            );
            SearchResponse {
                took: took_ms.floor() as u64,
                timed_out: false,
                took_ms,
                hits: SearchHits { total, hits },
                slots: None,
                profile: if include_profile {
                    Some(stats.into())
                } else {
                    None
                },
            }
        }

        // Multi-document percolation.
        (None, Some(docs)) => {
            let num_docs = docs.len();
            let prom = state.prom.clone();
            let snap = Arc::clone(&state.snapshot.load());
            let worker_snap = Arc::clone(&snap);
            // Bound per-request fan-out exactly as `/_mpercolate` does (ADR-052): a
            // multi-doc `/_search` is otherwise limited only by the HTTP body-size cap,
            // so one large body could schedule millions of parallel matches. Reject an
            // oversized batch with 400 before building titles or scheduling any work.
            let max_batch = snap.config().max_percolate_batch;
            if num_docs > max_batch {
                state
                    .prom
                    .http_requests_total
                    .with_label_values(&["search", "400"])
                    .inc();
                return Err(ApiError::response(
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    format!(
                        "batch of {num_docs} documents exceeds max_percolate_batch ({max_batch})"
                    ),
                ));
            }
            let titles: Vec<String> = docs.into_iter().map(|d| d.title).collect();
            let pred = snap.compile_tag_predicate(&filter_spec);
            let state_inner = Arc::clone(&state);
            // ADR-099: see the single-document arm.
            let deadline = requested_deadline.filter(|_| snap.config().cooperative_cancel);

            let search_fut = async {
                let permit = crate::state::acquire_search_permit(
                    state.search_permits.as_ref(),
                    &state.prom.search_permits_in_use,
                )
                .await;
                tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    state_inner.pool.install(|| {
                        let r = worker_snap.try_match_titles_par_filtered(
                            &titles,
                            include_broad,
                            &pred,
                            deadline,
                        );
                        if r.is_err() {
                            state_inner
                                .prom
                                .match_cancellations_total
                                .with_label_values(&["search"])
                                .inc();
                        }
                        // Match-feedback capture (ADR-103): opt-in, post-match.
                        if worker_snap.config().alias_feedback_capture {
                            if let Ok(results) = &r {
                                let mut fb = state_inner.feedback.lock();
                                for (idx, ids, _stats) in results {
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

            let results = match tokio::time::timeout(timeout, search_fut).await {
                Ok(Ok(Ok(result))) => result,
                Ok(Ok(Err(_cancelled))) => {
                    state
                        .prom
                        .http_requests_total
                        .with_label_values(&["search", "408"])
                        .inc();
                    return Err(ApiError::response(
                        StatusCode::REQUEST_TIMEOUT,
                        "timeout",
                        format!("search timed out after {}ms", timeout.as_millis()),
                    ));
                }
                Ok(Err(e)) => {
                    tracing::error!(error = %e, "search task panicked");
                    state
                        .prom
                        .http_requests_total
                        .with_label_values(&["search", "500"])
                        .inc();
                    return Err(ApiError::response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "search_error",
                        "internal search task failed",
                    ));
                }
                Err(_) => {
                    state
                        .prom
                        .http_requests_total
                        .with_label_values(&["search", "408"])
                        .inc();
                    return Err(ApiError::response(
                        StatusCode::REQUEST_TIMEOUT,
                        "timeout",
                        format!("search timed out after {}ms", timeout.as_millis()),
                    ));
                }
            };

            let mut all_ids = Vec::new();
            let mut slot_data: Vec<(usize, Vec<u64>, MatchStats)> = Vec::new();
            let mut merged_stats = StatsResponse::default();
            for (slot, ids, stats) in results {
                prom.match_candidates_per_title
                    .observe(f64::from(stats.unique_candidates));
                prom.match_results_per_title.observe(ids.len() as f64);

                all_ids.extend_from_slice(&ids);
                merged_stats.merge(stats);
                slot_data.push((slot, ids, stats));
            }
            all_ids.sort_unstable();
            all_ids.dedup();

            let total = all_ids.len();
            let cspec = rank_raw
                .as_ref()
                .map(|r| snap.compile_rank_spec(r))
                .filter(|c| !c.is_noop());
            let hits: Vec<_> =
                order_and_page(&snap, &all_ids, cspec.as_ref(), page_from, page_size)
                    .into_iter()
                    .map(|(id, score)| materialize_hit(&snap, id, score, include_source, None))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| enrichment_error(&state, error))?;
            // Per-slot hits get the same rank + `from`/`size` treatment (ADR-059 closes
            // the ADR-052 #3 tail): `total` still reports the untruncated per-slot count.
            let slots: Vec<_> = slot_data
                .into_iter()
                .map(|(slot, ids, stats)| {
                    let slot_total = ids.len();
                    let slot_hits =
                        order_and_page(&snap, &ids, cspec.as_ref(), page_from, page_size)
                            .into_iter()
                            .map(|(id, score)| {
                                materialize_hit(&snap, id, score, include_source, None)
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                    Ok(SlotHit {
                        slot,
                        total: slot_total,
                        hits: slot_hits,
                        stats: stats.into(),
                    })
                })
                .collect::<Result<Vec<_>, (&'static str, String)>>()
                .map_err(|error| enrichment_error(&state, error))?;
            let took_ms = start.elapsed().as_secs_f64() * 1000.0;

            info!(
                titles = num_docs,
                matches = total,
                took_ms = format!("{:.2}", took_ms),
                "search complete"
            );
            SearchResponse {
                took: took_ms.floor() as u64,
                timed_out: false,
                took_ms,
                hits: SearchHits { total, hits },
                slots: Some(slots),
                profile: include_profile.then_some(merged_stats),
            }
        }

        (None, None) => {
            state
                .prom
                .http_requests_total
                .with_label_values(&["search", "400"])
                .inc();
            return Err(ApiError::response(
                StatusCode::BAD_REQUEST,
                "validation_error",
                "request must include 'document' or 'documents' field",
            ));
        }
    };

    let threshold = state.slow_query_threshold_ms;
    if threshold > 0 && response.took_ms >= threshold as f64 {
        state.prom.slow_queries_total.inc();
        warn!(
            took_ms = format!("{:.2}", response.took_ms),
            threshold_ms = threshold,
            matches = response.hits.total,
            titles = response.slots.as_ref().map_or(1, std::vec::Vec::len),
            "slow query"
        );
    }

    state
        .prom
        .http_requests_total
        .with_label_values(&["search", "200"])
        .inc();
    Ok(Json(response))
}

#[cfg(test)]
mod response_tests {
    use super::*;

    #[test]
    fn profile_totals_accumulate_beyond_u32() {
        let one = MatchStats {
            unique_candidates: u32::MAX,
            postings_scanned: u32::MAX,
            matches: u32::MAX,
            ..MatchStats::default()
        };
        let mut total = StatsResponse::default();
        total.merge(one);
        total.merge(one);

        assert_eq!(total.unique_candidates, u64::from(u32::MAX) * 2);
        assert_eq!(total.postings_scanned, u64::from(u32::MAX) * 2);
        assert_eq!(total.matches, u64::from(u32::MAX) * 2);
    }
}
