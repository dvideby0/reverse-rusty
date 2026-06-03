//! Percolate read handlers: `POST /_search` (the rich, per-title path with explain
//! and per-slot stats) and `POST /_mpercolate` (the batch throughput path, columnar
//! broad lane amortized per title-batch — ADR-026). Owns the request-resolution
//! helpers that normalize both the native and ES percolate envelopes (ADR-049 filters).

use std::cell::RefCell;
use std::sync::Arc;
use std::time::Instant;

use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use tracing::{info, instrument, warn};

use reverse_rusty::segment::{BatchMatchOptions, BroadStrategy, MatchScratch, MatchStats};

use crate::dto::{ApiError, HitSource};
use crate::state::AppState;

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
    /// Optional per-request timeout in milliseconds (default: 30000).
    timeout_ms: Option<u64>,
    /// Maximum number of hits to return (default: 1000).
    size: Option<usize>,
    /// Offset into the result set for pagination (default: 0).
    from: Option<usize>,
    /// Include original query text in each hit (default: true).
    include_source: Option<bool>,
    /// Include per-hit explain detail showing why each query matched (default: false).
    explain: Option<bool>,
    /// Include match profile (candidate/posting stats) in the response (default: false).
    profile: Option<bool>,
}

#[derive(Deserialize)]
struct DocBody {
    title: String,
}

/// A request filter: a conjunction of `(key, [values])` groups (ADR-049).
type FilterSpec = Vec<(String, Vec<String>)>;

/// Parse the ES `bool.filter` clause list into a [`FilterSpec`]. Each clause is a
/// `{"terms": {key: [values]}}` or `{"term": {key: value}}`; any other clause type is a
/// hard error (so an unsupported filter never silently widens the result set). Accepts a
/// single clause object or an array of them.
fn parse_es_filter(filter: &serde_json::Value) -> Result<FilterSpec, String> {
    let clauses: Vec<&serde_json::Value> = match filter {
        serde_json::Value::Array(a) => a.iter().collect(),
        other => vec![other],
    };
    let mut spec = FilterSpec::new();
    for clause in clauses {
        let obj = clause
            .as_object()
            .ok_or_else(|| "filter clause must be an object".to_string())?;
        if let Some(terms) = obj.get("terms").and_then(|t| t.as_object()) {
            for (k, v) in terms {
                let vals = match v {
                    serde_json::Value::Array(a) => a
                        .iter()
                        .filter_map(|e| e.as_str().map(str::to_string))
                        .collect(),
                    serde_json::Value::String(s) => vec![s.clone()],
                    _ => return Err(format!("terms[{k}] must be a string or array of strings")),
                };
                spec.push((k.clone(), vals));
            }
        } else if let Some(term) = obj.get("term").and_then(|t| t.as_object()) {
            for (k, v) in term {
                let val = v
                    .as_str()
                    .ok_or_else(|| format!("term[{k}] must be a string"))?;
                spec.push((k.clone(), vec![val.to_string()]));
            }
        } else {
            return Err(
                "unsupported filter clause: only `terms` and `term` are supported".to_string(),
            );
        }
    }
    Ok(spec)
}

/// Parse a native filter block — an object `{key: value|[values], ...}` — into a
/// [`FilterSpec`].
fn parse_native_filter(filter: &serde_json::Value) -> Result<FilterSpec, String> {
    let obj = filter
        .as_object()
        .ok_or_else(|| "`filter` must be an object of key → value(s)".to_string())?;
    let mut spec = FilterSpec::new();
    for (k, v) in obj {
        let vals = match v {
            serde_json::Value::String(s) => vec![s.clone()],
            serde_json::Value::Array(a) => a
                .iter()
                .filter_map(|e| e.as_str().map(str::to_string))
                .collect(),
            _ => return Err(format!("filter[{k}] must be a string or array of strings")),
        };
        spec.push((k.clone(), vals));
    }
    Ok(spec)
}

/// The percolate documents + tag filter resolved from a request, normalizing BOTH the
/// native RR shape (`document`/`documents` + `filter`) and the ES `bool`/`terms`/`percolate`
/// envelope (`query.bool.must.percolate` + `query.bool.filter`). Returns the titles, whether
/// the request was single-document (drives the response shape), and the filter spec. Any
/// unsupported ES query node is a hard error (never silently ignored).
fn resolve_percolate(
    document: Option<DocBody>,
    documents: Option<Vec<DocBody>>,
    native_filter: Option<serde_json::Value>,
    es_query: Option<serde_json::Value>,
) -> Result<(Vec<String>, bool, FilterSpec), String> {
    if let Some(q) = es_query {
        return resolve_es_query(&q);
    }
    let mut filter = FilterSpec::new();
    if let Some(f) = native_filter {
        filter = parse_native_filter(&f)?;
    }
    match (document, documents) {
        (Some(d), _) => Ok((vec![d.title], true, filter)),
        (None, Some(ds)) => Ok((ds.into_iter().map(|d| d.title).collect(), false, filter)),
        (None, None) => Err("request must include 'document' or 'documents'".to_string()),
    }
}

/// Resolve the ES percolate envelope: `{query:{bool:{must:{percolate:{document(s)}}, filter:[…]}}}`
/// or the bare `{query:{percolate:{document(s)}}}`. Only the percolate + bool.filter(terms/term)
/// subset is supported.
fn resolve_es_query(query: &serde_json::Value) -> Result<(Vec<String>, bool, FilterSpec), String> {
    let obj = query
        .as_object()
        .ok_or_else(|| "`query` must be an object".to_string())?;
    let (percolate, filter) = if let Some(b) = obj.get("bool") {
        let b = b
            .as_object()
            .ok_or_else(|| "`query.bool` must be an object".to_string())?;
        // must → the percolate clause (single object or a one-element array)
        let must = b
            .get("must")
            .ok_or_else(|| "`query.bool` must contain a `must` percolate clause".to_string())?;
        let must_clause = match must {
            serde_json::Value::Array(a) if a.len() == 1 => &a[0],
            serde_json::Value::Array(_) => {
                return Err("only a single `percolate` clause is supported in `must`".to_string())
            }
            obj => obj,
        };
        let percolate = must_clause
            .get("percolate")
            .ok_or_else(|| "`query.bool.must` must be a `percolate` clause".to_string())?;
        let filter = match b.get("filter") {
            Some(f) => parse_es_filter(f)?,
            None => FilterSpec::new(),
        };
        (percolate, filter)
    } else if let Some(p) = obj.get("percolate") {
        (p, FilterSpec::new())
    } else {
        return Err("`query` must be a `percolate` or `bool` percolate clause".to_string());
    };
    let (titles, single) = extract_percolate_docs(percolate)?;
    Ok((titles, single, filter))
}

/// Pull the document(s) out of an ES `percolate` clause (`{field, document}` or
/// `{field, documents}`); `field` is accepted but ignored (RR has one query field).
fn extract_percolate_docs(percolate: &serde_json::Value) -> Result<(Vec<String>, bool), String> {
    let p = percolate
        .as_object()
        .ok_or_else(|| "`percolate` must be an object".to_string())?;
    let title_of = |doc: &serde_json::Value| -> Result<String, String> {
        doc.get("title")
            .and_then(|t| t.as_str())
            .map(str::to_string)
            .ok_or_else(|| "percolate document must have a string `title`".to_string())
    };
    if let Some(doc) = p.get("document") {
        Ok((vec![title_of(doc)?], true))
    } else if let Some(docs) = p.get("documents").and_then(|d| d.as_array()) {
        Ok((docs.iter().map(title_of).collect::<Result<_, _>>()?, false))
    } else {
        Err("`percolate` must contain `document` or `documents`".to_string())
    }
}

#[derive(Serialize)]
pub(crate) struct SearchResponse {
    took_ms: f64,
    hits: SearchHits,
    #[serde(skip_serializing_if = "Option::is_none")]
    slots: Option<Vec<SlotHit>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    profile: Option<StatsResponse>,
}

#[derive(Serialize)]
struct SearchHits {
    total: usize,
    hits: Vec<SearchHitItem>,
}

#[derive(Serialize)]
struct SearchHitItem {
    _id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    _source: Option<HitSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    _explanation: Option<reverse_rusty::ExplainDetail>,
}

#[derive(Serialize)]
struct SlotHit {
    slot: usize,
    total: usize,
    hits: Vec<SearchHitItem>,
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

// -- POST /_mpercolate (batch percolation; ES `_msearch`-shaped responses[])
#[derive(Deserialize)]
pub(crate) struct MPercolateBody {
    /// The batch of documents to percolate. Each entry is matched independently;
    /// `responses[i]` corresponds to `documents[i]`.
    documents: Option<Vec<DocBody>>,
    /// Native tag filter (ADR-049): an object `{key: value|[values]}` applied to every
    /// document in the batch.
    filter: Option<serde_json::Value>,
    /// ES-compatible percolate envelope (see [`SearchBody::query`]); when present the
    /// batch documents and filter are taken from here.
    query: Option<serde_json::Value>,
    /// Per-request override of the server's broad-lane default. When set, controls
    /// whether class-C (broad) queries are evaluated for this batch.
    include_broad: Option<bool>,
    /// Include original query text in each hit (default: true).
    include_source: Option<bool>,
    /// Maximum hits to return per document (default: 1000).
    size: Option<usize>,
    /// Per-request timeout in milliseconds (default: 30000).
    timeout_ms: Option<u64>,
    /// Include the top-level broad-lane summary in the response (default: false).
    profile: Option<bool>,
}

#[derive(Serialize)]
pub(crate) struct MPercolateResponse {
    took_ms: f64,
    /// One entry per input document, in submission order.
    responses: Vec<PercolateItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    broad: Option<BroadSummary>,
}

#[derive(Serialize)]
struct PercolateItem {
    hits: SearchHits,
}

/// Top-level broad-lane summary for a `/_mpercolate` batch — surfaces the columnar
/// evaluator's amortization (see `MatchStats` / ADR-026). `broad_postings_scanned`
/// rising far slower than `broad_candidates` as `batch_size` grows IS the win.
#[derive(Serialize)]
struct BroadSummary {
    strategy: &'static str,
    batch_size: usize,
    broad_batches: u32,
    broad_postings_scanned: u32,
    broad_queries_evaluated: u32,
    broad_candidates: u32,
    total_matches: u32,
}

/// POST /_search — percolate one or more titles.
#[instrument(skip_all)]
pub(crate) async fn search(
    State(state): State<Arc<AppState>>,
    Json(body): Json<SearchBody>,
) -> Result<Json<SearchResponse>, (StatusCode, Json<ApiError>)> {
    let start = Instant::now();
    let include_broad = state.include_broad;
    let include_source = body.include_source.unwrap_or(true);
    let include_explain = body.explain.unwrap_or(false);
    let include_profile = body.profile.unwrap_or(false);
    let timeout = tokio::time::Duration::from_millis(body.timeout_ms.unwrap_or(30_000));
    let page_size = body.size.unwrap_or(1000);
    let page_from = body.from.unwrap_or(0);

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

            let search_fut = tokio::task::spawn_blocking(move || {
                state_inner.pool.install(|| {
                    SCRATCH.with(|cell| {
                        let mut scratch = cell.borrow_mut();
                        let mut out = Vec::new();
                        let stats = snap.match_title_filtered(
                            &title,
                            &mut scratch,
                            &mut out,
                            include_broad,
                            &pred,
                        );
                        (out, stats)
                    })
                })
            });

            let (ids, stats) = match tokio::time::timeout(timeout, search_fut).await {
                Ok(Ok(result)) => result,
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
            let paged_ids: Vec<u64> = ids.into_iter().skip(page_from).take(page_size).collect();
            let snap = state.snapshot.load();
            let hits = paged_ids
                .iter()
                .map(|&id| {
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
            let titles: Vec<String> = docs.into_iter().map(|d| d.title).collect();
            let prom = state.prom.clone();
            let snap = Arc::clone(&state.snapshot.load());
            let pred = snap.compile_tag_predicate(&filter_spec);
            let state_inner = Arc::clone(&state);

            let search_fut = tokio::task::spawn_blocking(move || {
                state_inner
                    .pool
                    .install(|| snap.match_titles_par_filtered(&titles, include_broad, &pred))
            });

            let results = match tokio::time::timeout(timeout, search_fut).await {
                Ok(Ok(result)) => result,
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
            let paged_ids: Vec<u64> = all_ids
                .into_iter()
                .skip(page_from)
                .take(page_size)
                .collect();

            let snap = state.snapshot.load();
            let make_hit = |id: u64| {
                let source = if include_source {
                    snap.get_query_source(id).map(|q| HitSource { query: q })
                } else {
                    None
                };
                SearchHitItem {
                    _id: id,
                    _source: source,
                    _explanation: None,
                }
            };
            let hits: Vec<_> = paged_ids.iter().map(|&id| make_hit(id)).collect();
            let slots: Vec<_> = slot_data
                .into_iter()
                .map(|(slot, ids, stats)| {
                    let slot_hits = ids.iter().map(|&id| make_hit(id)).collect();
                    SlotHit {
                        slot,
                        total: ids.len(),
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
    let responses: Vec<PercolateItem> = per_doc
        .into_iter()
        .map(|ids| {
            let total = ids.len();
            let hits = ids
                .into_iter()
                .take(page_size)
                .map(|id| {
                    let source = if include_source {
                        snap.get_query_source(id).map(|q| HitSource { query: q })
                    } else {
                        None
                    };
                    SearchHitItem {
                        _id: id,
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

#[cfg(test)]
mod mpercolate_tests {
    //! Handler-level tests for POST /_mpercolate: request validation, the empty
    //! batch no-op, the responses[] envelope shape, and — the load-bearing one —
    //! that each per-document response is identical to the per-title path
    //! (`match_title`), so the batch endpoint can't silently diverge from
    //! `/_search`. The library already proves batch == scalar (tests/broad_batch);
    //! this proves the HTTP layer threads results through in order and unchanged.
    use super::{mpercolate, AppState, DocBody, MPercolateBody, State};
    use crate::metrics::PrometheusMetrics;
    use axum::Json;
    use reverse_rusty::gen::{generate, GenConfig};
    use reverse_rusty::segment::{Engine, MatchScratch};
    use reverse_rusty::Normalizer;
    use std::sync::Arc;

    fn corpus() -> (Engine, Vec<String>) {
        let data = generate(&GenConfig {
            num_queries: 5_000,
            num_titles: 300,
            broad_query_frac: 0.1,
            hot_skew: 2.0,
            family_size: 8,
            seed: 0x0BA7_C0DE,
            num_players: 2_000,
            num_sets: 1_000,
        });
        let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
        eng.build_from_queries(&data.queries);
        (eng, data.titles)
    }

    fn state_with(eng: Engine, include_broad: bool) -> Arc<AppState> {
        let snap = Arc::new(eng.snapshot());
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .expect("pool");
        Arc::new(AppState {
            engine: parking_lot::Mutex::new(eng),
            snapshot: arc_swap::ArcSwap::new(snap),
            pool,
            include_broad,
            prom: PrometheusMetrics::new(),
            slow_query_threshold_ms: 0,
        })
    }

    fn body(docs: Option<Vec<&str>>, include_broad: Option<bool>, profile: bool) -> MPercolateBody {
        MPercolateBody {
            documents: docs.map(|v| {
                v.into_iter()
                    .map(|t| DocBody {
                        title: t.to_string(),
                    })
                    .collect()
            }),
            filter: None,
            query: None,
            include_broad,
            include_source: Some(false),
            // Large cap so no per-document truncation can mask a result mismatch.
            size: Some(1_000_000),
            timeout_ms: None,
            profile: Some(profile),
        }
    }

    #[tokio::test]
    async fn missing_documents_is_400() {
        let (eng, _) = corpus();
        let state = state_with(eng, false);
        let err = mpercolate(State(state), Json(body(None, None, false)))
            .await
            .err()
            .expect("missing documents must error");
        assert_eq!(err.0, axum::http::StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn empty_batch_is_noop() {
        let (eng, _) = corpus();
        let state = state_with(eng, true);
        let resp = mpercolate(State(state), Json(body(Some(Vec::new()), None, true)))
            .await
            .expect("empty batch is a valid no-op")
            .0;
        assert!(resp.responses.is_empty());
        assert!(resp.broad.is_none(), "no work => no broad summary");
    }

    // Reads the ES-convention `_id` field on hits (clippy::used_underscore_binding).
    #[allow(clippy::used_underscore_binding)]
    #[tokio::test]
    async fn responses_are_byte_identical_to_per_title_search() {
        let (eng, titles) = corpus();
        // Capture a snapshot of the same state for the per-title baseline before
        // the engine moves into the AppState.
        let baseline = Arc::new(eng.snapshot());
        let state = state_with(eng, true);

        let batch: Vec<&str> = titles.iter().take(150).map(String::as_str).collect();
        // include_broad=true exercises the columnar broad lane through the endpoint.
        let resp = mpercolate(
            State(Arc::clone(&state)),
            Json(body(Some(batch.clone()), Some(true), true)),
        )
        .await
        .expect("ok")
        .0;

        assert_eq!(
            resp.responses.len(),
            batch.len(),
            "one response per document"
        );

        let mut scratch = MatchScratch::new();
        let mut out = Vec::new();
        let mut summed = 0u32;
        for (i, title) in batch.iter().enumerate() {
            out.clear();
            baseline.match_title(title, &mut scratch, &mut out, true);
            let mut expected = out.clone();
            expected.sort_unstable();
            expected.dedup();

            let item = &resp.responses[i];
            let mut got: Vec<u64> = item.hits.hits.iter().map(|h| h._id).collect();
            got.sort_unstable();
            assert_eq!(
                got, expected,
                "document {i} ({title}) diverged from per-title search"
            );
            assert_eq!(item.hits.total, expected.len(), "total mismatch at {i}");
            summed += expected.len() as u32;
        }

        // Top-level broad summary present (profile=true) and internally consistent.
        let broad = resp.broad.expect("profile=true => broad summary");
        assert_eq!(broad.strategy, "columnar");
        assert_eq!(broad.batch_size, 256);
        assert_eq!(
            broad.total_matches, summed,
            "summary total must equal the per-document sum"
        );
    }
}
