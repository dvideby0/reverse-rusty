//! `POST /_search` — the rich, per-title percolate path: single- or multi-document,
//! with optional explain, per-slot stats, ranking (ADR-059) and `from`/`size`
//! pagination. Owns the `/_search` request/response DTOs; the batch throughput path
//! lives in [`super::mpercolate`].

use std::cell::RefCell;
use std::sync::Arc;
use std::time::Instant;

use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use tracing::{info, instrument, warn};

use reverse_rusty::segment::{MatchScratch, MatchStats};

use crate::dto::{ApiError, HitSource};
use crate::state::AppState;

use super::rank::{order_and_page, to_rank_spec, RankBody};
use super::resolve::resolve_percolate;
use super::{DocBody, SearchHitItem, SearchHits};

thread_local! {
    static SCRATCH: RefCell<MatchScratch> = RefCell::new(MatchScratch::new());
}

// -- POST /_search
#[derive(Deserialize)]
pub(crate) struct SearchBody {
    document: Option<DocBody>,
    documents: Option<Vec<DocBody>>,
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
    /// Include per-hit explain detail showing why each query matched (default: false).
    explain: Option<bool>,
    /// Include match profile (candidate/posting stats) in the response (default: false).
    profile: Option<bool>,
}

#[derive(Serialize)]
pub(crate) struct SearchResponse {
    took_ms: f64,
    pub(super) hits: SearchHits,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) slots: Option<Vec<SlotHit>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    profile: Option<StatsResponse>,
}

#[derive(Serialize)]
pub(super) struct SlotHit {
    slot: usize,
    pub(super) total: usize,
    pub(super) hits: Vec<SearchHitItem>,
    stats: StatsResponse,
}

#[derive(Serialize, Clone)]
struct StatsResponse {
    unique_candidates: u32,
    /// Broad-lane subset of `unique_candidates` — how much of the work came from
    /// quarantined broad (class-C) queries (0 unless `include_broad`).
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

/// POST /_search — percolate one or more titles.
#[instrument(skip_all)]
pub(crate) async fn search(
    State(state): State<Arc<AppState>>,
    Json(body): Json<SearchBody>,
) -> Result<Json<SearchResponse>, (StatusCode, Json<ApiError>)> {
    let start = Instant::now();
    let include_broad = body.include_broad.unwrap_or(state.include_broad);
    let include_source = body.include_source.unwrap_or(true);
    let include_explain = body.explain.unwrap_or(false);
    let include_profile = body.profile.unwrap_or(false);
    let timeout = tokio::time::Duration::from_millis(body.timeout_ms.unwrap_or(30_000));
    let page_size = body.size.unwrap_or(1000);
    let page_from = body.from.unwrap_or(0);
    let rank_raw = to_rank_spec(body.rank);

    // Resolve documents + tag filter from EITHER the native shape (document/documents +
    // filter) or the ES bool/terms percolate envelope (query). A malformed/unsupported
    // request is a 400 (an unsupported query node never silently widens the result set).
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
            let state_inner = Arc::clone(&state);
            // ADR-099: arm cooperative cancellation only for an EXPLICIT timeout_ms
            // (the implicit 30s default stays a response deadline — zero deadline
            // reads on the unarmed hot path), gated by the dynamic kill-switch.
            let deadline = (body.timeout_ms.is_some() && snap.config().cooperative_cancel)
                .then(|| start + timeout);

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
                            let r = snap
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
                    eprintln!("search task panicked: {e}");
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

            let took_ms = start.elapsed().as_secs_f64() * 1000.0;
            let total = ids.len();
            let snap = state.snapshot.load();
            let cspec = rank_raw
                .as_ref()
                .map(|r| snap.compile_rank_spec(r))
                .filter(|c| !c.is_noop());
            let hits = order_and_page(&snap, &ids, cspec.as_ref(), page_from, page_size)
                .into_iter()
                .map(|(id, score)| {
                    let source = if include_source {
                        snap.get_query_source(id).map(|q| HitSource { query: q })
                    } else {
                        None
                    };
                    let explanation = title_for_explain
                        .as_deref()
                        .and_then(|t| snap.explain_hit(id, t));
                    SearchHitItem {
                        _id: id,
                        _score: score,
                        _source: source,
                        _explanation: explanation,
                    }
                })
                .collect();
            info!(
                titles = 1,
                matches = total,
                took_ms = format!("{:.2}", took_ms),
                "search complete"
            );
            SearchResponse {
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
            let deadline = (body.timeout_ms.is_some() && snap.config().cooperative_cancel)
                .then(|| start + timeout);

            let search_fut = async {
                let permit = crate::state::acquire_search_permit(
                    state.search_permits.as_ref(),
                    &state.prom.search_permits_in_use,
                )
                .await;
                tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    state_inner.pool.install(|| {
                        let r = snap.try_match_titles_par_filtered(
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
                    eprintln!("search task panicked: {e}");
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

            let took_ms = start.elapsed().as_secs_f64() * 1000.0;
            let mut all_ids = Vec::new();
            let mut slot_data: Vec<(usize, Vec<u64>, StatsResponse)> = Vec::new();
            for (slot, ids, stats) in results {
                prom.match_candidates_per_title
                    .observe(f64::from(stats.unique_candidates));
                prom.match_results_per_title.observe(ids.len() as f64);

                all_ids.extend_from_slice(&ids);
                slot_data.push((slot, ids, stats.into()));
            }
            all_ids.sort_unstable();
            all_ids.dedup();

            let total = all_ids.len();
            let snap = state.snapshot.load();
            let cspec = rank_raw
                .as_ref()
                .map(|r| snap.compile_rank_spec(r))
                .filter(|c| !c.is_noop());
            let make_hit = |id: u64, score: Option<i64>| {
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
            };
            let hits: Vec<_> =
                order_and_page(&snap, &all_ids, cspec.as_ref(), page_from, page_size)
                    .into_iter()
                    .map(|(id, score)| make_hit(id, score))
                    .collect();
            // Per-slot hits get the same rank + `from`/`size` treatment (ADR-059 closes
            // the ADR-052 #3 tail): `total` still reports the untruncated per-slot count.
            let slots: Vec<_> = slot_data
                .into_iter()
                .map(|(slot, ids, stats)| {
                    let slot_total = ids.len();
                    let slot_hits =
                        order_and_page(&snap, &ids, cspec.as_ref(), page_from, page_size)
                            .into_iter()
                            .map(|(id, score)| make_hit(id, score))
                            .collect();
                    SlotHit {
                        slot,
                        total: slot_total,
                        hits: slot_hits,
                        stats,
                    }
                })
                .collect();

            info!(
                titles = num_docs,
                matches = total,
                took_ms = format!("{:.2}", took_ms),
                "search complete"
            );
            SearchResponse {
                took_ms,
                hits: SearchHits { total, hits },
                slots: Some(slots),
                profile: None,
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
    state
        .prom
        .http_request_duration
        .with_label_values(&["search"])
        .observe(start.elapsed().as_secs_f64());
    Ok(Json(response))
}
