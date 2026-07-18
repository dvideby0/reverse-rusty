//! Bounded ranked percolation (`POST /v2/_search`, ADR-107/108/110): the
//! request contract (DTOs + `prepare`) and the two thin mode handlers over the
//! shared delivery pipeline in [`delivery`].

use std::cell::RefCell;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::dto::{ApiError, HitSource};
use crate::metrics::PrometheusMetrics;
use crate::state::{AppState, ClusterAppState};

use super::resolve::resolve_percolate;
use super::DocBody;

mod delivery;
mod mpercolate;
mod page;

#[cfg(test)]
pub(crate) use mpercolate::V2MPercolateBody;
pub(crate) use mpercolate::{cluster_v2_mpercolate, v2_mpercolate};

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

/// A PIT reference on a page-one cursor request (ADR-113).
#[derive(Deserialize)]
struct PitRefBody {
    id: String,
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
    /// ADR-113 page one: bind this search to an open PIT and return
    /// `next_cursor`.
    pit: Option<PitRefBody>,
    /// ADR-113 page N: continue a cursor (the same document/rank/filter/scope
    /// must be resent; the cursor names its PIT internally).
    cursor: Option<serde_json::Value>,
    // Named unsupported shapes produce a stable 400 rather than being ignored.
    #[serde(rename = "from")]
    page_from: Option<serde_json::Value>,
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
    /// ADR-113: present only on a full pit-bound page — the opaque token that
    /// continues the ranked stream. `null`/absent means the stream is done.
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
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
    /// ADR-113 page shape (tokens still unverified — verification is
    /// mode-side, where the registry lives).
    page: page::PageRequest,
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
    if body.page_from.is_some() || body.documents.is_some() || body.query.is_some() {
        return Err(PrepareFailure::Validation(validation(
            "v2 ranked search accepts one `document`; from, documents and query are not supported",
        )));
    }
    let page = match (body.pit, body.cursor) {
        (None, None) => page::PageRequest::None,
        (Some(_), Some(_)) => {
            return Err(PrepareFailure::Validation(validation(
                "`pit` and `cursor` are mutually exclusive; the cursor already names its PIT",
            )));
        }
        (Some(pit), None) => page::PageRequest::Pit(pit.id),
        (None, Some(cursor)) => match cursor.as_str() {
            Some(token) => page::PageRequest::Cursor(token.to_string()),
            None => {
                return Err(PrepareFailure::Validation(validation(
                    "`cursor` must be the opaque string returned as next_cursor",
                )));
            }
        },
    };
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
        search_after: None,
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
        page,
    })
}

/// Shared prepare-failure accounting: an admission rejection counts its
/// reason label, everything else records a validation outcome.
fn prepare_failure(
    prom: &PrometheusMetrics,
    failure: PrepareFailure,
    scope: reverse_rusty::QueryScope,
) -> (StatusCode, Json<ApiError>) {
    if let Some(reason) = failure.admission_reason() {
        prom.rank_admission_rejections_total
            .with_label_values(&[reason])
            .inc();
        record_outcome(prom, "admission", scope);
    } else {
        record_outcome(prom, "validation", scope);
    }
    failure.response()
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
        Err(failure) => return Err(prepare_failure(&state.prom, failure, requested_scope)),
    };
    let PreparedSearch {
        title,
        filter,
        options,
        rank: raw_program,
        include_source,
        include_explain,
        timeout,
        page,
    } = prepared;

    // Resolve the page BEFORE compiling: a pit/cursor page must compile its
    // rank program and predicate against the PINNED snapshot, not the live one.
    let plan = match page::resolve_local(
        &state,
        &page,
        &title,
        options.query_scope,
        &raw_program,
        &filter,
    ) {
        Ok(plan) => plan,
        Err(response) => return Err(response),
    };
    let snap = plan.snapshot;
    let options = reverse_rusty::TopKOptions {
        search_after: plan.search_after,
        ..options
    };
    let mint = plan.mint;
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
    let enrichment_limit = state.max_ranked_enrichment_bytes;
    let mint_state = Arc::clone(&state);
    let work = move || {
        RANKED_SCRATCH.with(|cell| {
            delivery::local_delivery(
                &snap,
                &program,
                &predicate,
                &mut cell.borrow_mut(),
                &delivery::DeliverySpec {
                    title: &title,
                    options,
                    include_source,
                    include_explain,
                    enrichment_limit,
                    deadline,
                },
            )
            .map(|mut result| {
                if let Some(mint) = &mint {
                    page::attach_next_cursor(&mint_state.pit_tokens, mint, options, &mut result);
                }
                result
            })
        })
    };
    delivery::drive(state, started, options, timeout, deadline, work).await
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
        Err(failure) => return Err(prepare_failure(&state.prom, failure, requested_scope)),
    };
    let PreparedSearch {
        title,
        filter,
        options,
        rank,
        include_source,
        include_explain,
        timeout,
        page,
    } = prepared;
    // Verify page tokens (pure — no lock). Garbled ⇒ 400; foreign key ⇒ 409.
    let (pit, search_after, expected_fingerprint) = match &page {
        page::PageRequest::None => (None, None, None),
        page::PageRequest::Pit(token) => match state.pit_tokens.verify_pit(token) {
            Ok(pit) => (Some(pit), None, None),
            Err(error) => {
                record_outcome(&state.prom, "validation", options.query_scope);
                return Err(crate::pit::token_failure_response(error));
            }
        },
        page::PageRequest::Cursor(token) => match state.pit_tokens.verify_cursor(token) {
            Ok(payload) => (
                Some(payload.pit),
                Some(payload.after),
                Some(payload.fingerprint),
            ),
            Err(error) => {
                record_outcome(&state.prom, "validation", options.query_scope);
                return Err(crate::pit::token_failure_response(error));
            }
        },
    };
    // One brief read-lock scope (the existing compile pattern — sync, never
    // across an await): compile + PIT pre-check + fingerprint. The stale gate
    // runs BEFORE the fingerprint so a rebuilt normalizer cannot mis-classify
    // a dead cursor as a client mismatch; the kernel re-gates inside the
    // blocking closure, so the gap between here and there stays fail-closed.
    let (program, mint) = {
        let cluster = state.cluster.read();
        let program = match cluster.compile_rank_program(&rank) {
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
        let mint = match pit {
            None => None,
            Some(pit) => {
                if let Err(error) = cluster.check_pit(pit, Instant::now()) {
                    let (status, kind, outcome) = error.v2_http_class();
                    record_outcome(&state.prom, outcome, options.query_scope);
                    return Err(ApiError::response(
                        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                        kind,
                        error.to_string(),
                    ));
                }
                let fingerprint = crate::pit::request_fingerprint(
                    cluster.normalizer(),
                    &title,
                    options.query_scope,
                    &rank,
                    &filter,
                );
                if let Some(expected) = expected_fingerprint {
                    if fingerprint != expected {
                        record_outcome(&state.prom, "cursor_mismatch", options.query_scope);
                        return Err(crate::pit::cursor_mismatch_response());
                    }
                }
                Some(page::MintCtx { pit, fingerprint })
            }
        };
        (program, mint)
    };
    let options = reverse_rusty::TopKOptions {
        search_after,
        ..options
    };
    let Some(deadline) = Instant::now().checked_add(timeout) else {
        record_outcome(&state.prom, "validation", options.query_scope);
        return Err(validation("timeout_ms is too large"));
    };
    let enrichment_limit = state.max_ranked_enrichment_bytes;
    let cluster_state = Arc::clone(&state);
    let work = move || {
        // The cluster read lock is taken INSIDE the blocking closure (never
        // across an await) and held for match + fetch + assembly, exactly as
        // before the unification.
        let cluster = cluster_state.cluster.read();
        delivery::cluster_delivery(
            &cluster,
            mint.as_ref().map(|mint| mint.pit),
            &program,
            &filter,
            &delivery::DeliverySpec {
                title: &title,
                options,
                include_source,
                include_explain,
                enrichment_limit,
                deadline,
            },
        )
        .map(|mut result| {
            if let Some(mint) = &mint {
                page::attach_next_cursor(&cluster_state.pit_tokens, mint, options, &mut result);
            }
            result
        })
    };
    delivery::drive(state, started, options, timeout, deadline, work).await
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
