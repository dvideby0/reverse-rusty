//! Learned-alias governance endpoints (ADR-060): `/_vocab/aliases*`.
//!
//! A thin HTTP layer over the engine's alias registry: review the governed candidates, import a
//! Solr/Lucene synonym file, or learn candidates from the engine's own stored queries — each
//! reusing the engine's `set_vocab` + `recompile_stale_segments` apply path (no restart) so a safe
//! single-token alias takes effect immediately with zero false negatives. The matcher is unchanged;
//! multi-word groups are recorded as review candidates only (Phase 2).

use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};

use crate::dto::ApiError;
use crate::state::AppState;

fn default_min_count() -> usize {
    2
}

#[derive(Serialize)]
struct GetAliasesResponse {
    /// The full governed registry (provenance / kind / confidence / status per entry) for review.
    aliases: reverse_rusty::vocab::AliasRegistry,
    /// Status counts (active / candidate / rejected).
    summary: reverse_rusty::vocab::AliasSummary,
}

/// GET /_vocab/aliases — return the alias registry + status summary (ADR-060 item 9). Reads the
/// lock-free snapshot (ADR-016) like `GET /_vocab`, so review never blocks behind a writer.
pub(crate) async fn get_aliases(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let snap = state.snapshot.load();
    let vocab = snap.vocab().cloned().unwrap_or_default();
    Json(GetAliasesResponse {
        summary: vocab.alias_summary(),
        aliases: vocab.aliases().clone(),
    })
}

#[derive(Deserialize)]
pub(crate) struct ImportAliasesRequest {
    /// Raw Solr/Lucene synonym-file text (comma lists and `a, b => c` mappings; `#` comments).
    synonyms: String,
}

#[derive(Serialize)]
struct AliasApplyResponse {
    acknowledged: bool,
    /// Groups newly switched to active by this call.
    activated: usize,
    /// Stored queries recompiled so the change took effect immediately (zero false negatives).
    recompiled: usize,
    summary: reverse_rusty::vocab::AliasSummary,
}

/// POST /_vocab/aliases/import — import a Solr/Lucene synonym file into the registry and apply it
/// live (ADR-060 item 3). Safe single-token groups auto-activate (FN-safe expansion); multi-word
/// groups are recorded as review candidates. Recompiles in place — no restart.
pub(crate) async fn import_aliases(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ImportAliasesRequest>,
) -> Response {
    let result = {
        let mut engine = state.engine.lock();
        match engine.import_alias_synonyms(&req.synonyms) {
            Ok(report) => alias_apply_response(report),
            Err(e) => ApiError::response(StatusCode::BAD_REQUEST, "vocab_error", e.to_string())
                .into_response(),
        }
    };
    state.publish_snapshot();
    result
}

#[derive(Deserialize, Default)]
pub(crate) struct AliasLearnQuery {
    /// Minimum any-of occurrences for a group to be considered (default 2).
    #[serde(default = "default_min_count")]
    min_count: usize,
}

/// POST /_vocab/aliases/learn_and_apply — learn alias candidates from the engine's OWN stored
/// queries (any-of co-occurrence) into the registry and apply (ADR-060 item 2). Conservative:
/// only clear single-token variants auto-activate; everything else is a review candidate
/// (inspect via `GET /_vocab/aliases`). `?min_count=N` (default 2).
pub(crate) async fn learn_and_apply_aliases(
    State(state): State<Arc<AppState>>,
    Query(q): Query<AliasLearnQuery>,
) -> Response {
    let result = {
        let mut engine = state.engine.lock();
        match engine.learn_aliases_and_apply(q.min_count) {
            Ok(report) => alias_apply_response(report),
            Err(e) => ApiError::response(StatusCode::BAD_REQUEST, "vocab_error", e.to_string())
                .into_response(),
        }
    };
    state.publish_snapshot();
    result
}

/// Render a successful apply as the shared 200 response.
fn alias_apply_response(report: reverse_rusty::AliasApplyReport) -> Response {
    (
        StatusCode::OK,
        Json(AliasApplyResponse {
            acknowledged: true,
            activated: report.activated,
            recompiled: report.recompiled,
            summary: report.summary,
        }),
    )
        .into_response()
}

#[derive(Deserialize, Default)]
pub(crate) struct DiscoverAliasesRequest {
    /// Explicit `(id, dsl)` corpus to analyze. Absent ⇒ the engine's own stored queries.
    /// (The cluster-mode dry-run requires this — a coordinator has no single-engine corpus.)
    #[serde(default)]
    pub(crate) queries: Option<Vec<(u64, String)>>,
    // Knob overrides; defaults = `DistributionalConfig::default()` (ADR-102).
    #[serde(default)]
    min_token_freq: Option<usize>,
    #[serde(default)]
    min_similarity: Option<f64>,
    #[serde(default)]
    max_pairs: Option<usize>,
    #[serde(default)]
    max_vocab: Option<usize>,
    #[serde(default)]
    max_cooccurrence_rate: Option<f64>,
    #[serde(default)]
    glue_phrases: Option<bool>,
    #[serde(default)]
    include_numeric: Option<bool>,
}

impl DiscoverAliasesRequest {
    pub(crate) fn config(&self) -> reverse_rusty::vocab::DistributionalConfig {
        let d = reverse_rusty::vocab::DistributionalConfig::default();
        reverse_rusty::vocab::DistributionalConfig {
            min_token_freq: self.min_token_freq.unwrap_or(d.min_token_freq),
            min_similarity: self.min_similarity.unwrap_or(d.min_similarity),
            max_pairs: self.max_pairs.unwrap_or(d.max_pairs),
            max_vocab: self.max_vocab.unwrap_or(d.max_vocab),
            max_cooccurrence_rate: self
                .max_cooccurrence_rate
                .unwrap_or(d.max_cooccurrence_rate),
            glue_phrases: self.glue_phrases.unwrap_or(d.glue_phrases),
            include_numeric: self.include_numeric.unwrap_or(d.include_numeric),
            ..d
        }
    }
}

#[derive(Serialize)]
struct DiscoverAliasesResponse {
    /// Proposed pairs, best-first (similarity desc) — review evidence, nothing recorded.
    proposals: Vec<reverse_rusty::vocab::DiscoveredPair>,
    count: usize,
}

/// POST /_vocab/aliases/discover — distributional alias discovery, compute-only (ADR-102).
/// Analyzes the engine's own stored queries (or an explicit `queries` body) and returns
/// candidate pairs with their similarity/co-occurrence evidence. Records NOTHING — pair with
/// `/discover_and_record` to file the proposals as review candidates.
pub(crate) async fn discover_aliases(
    State(state): State<Arc<AppState>>,
    body: Option<Json<DiscoverAliasesRequest>>,
) -> Response {
    let req = body.map(|Json(r)| r).unwrap_or_default();
    let cfg = req.config();
    let proposals = match &req.queries {
        // An explicit corpus is a pure computation — no engine lock at all.
        Some(qs) => reverse_rusty::vocab::discover_pairs(qs, &cfg),
        None => state.engine.lock().discover_aliases(&cfg),
    };
    (
        StatusCode::OK,
        Json(DiscoverAliasesResponse {
            count: proposals.len(),
            proposals,
        }),
    )
        .into_response()
}

#[derive(Serialize)]
struct DiscoverRecordResponse {
    acknowledged: bool,
    /// Pairs the discoverer proposed (post-filter).
    proposed: usize,
    /// Proposals recorded as NEW review candidates.
    new_candidates: usize,
    /// Proposals that already existed (confidence refreshed, status untouched).
    rediscovered: usize,
    /// Proposals refused because the group was operator-rejected (stickiness).
    rejected_sticky: usize,
    /// Always 0 — candidates change no matching-relevant state, so nothing recompiles
    /// (the ADR-102 metadata-only install; match results are byte-identical).
    recompiled: usize,
    summary: reverse_rusty::vocab::AliasSummary,
}

/// POST /_vocab/aliases/discover_and_record — discover over the engine's OWN stored queries and
/// file every proposal as a review `Candidate` (ADR-102). Never activates anything (the
/// `LearnedDistributional` provenance is review-first by contract), so the vocabulary installs
/// through the metadata-only seam — no recompile, byte-identical matching. Activation stays an
/// explicit operator act (`PUT /_vocab` with edited statuses). Knobs as in `/discover`; an
/// explicit `queries` body is refused here — recording is about THIS engine's corpus.
pub(crate) async fn discover_and_record_aliases(
    State(state): State<Arc<AppState>>,
    body: Option<Json<DiscoverAliasesRequest>>,
) -> Response {
    let req = body.map(|Json(r)| r).unwrap_or_default();
    if req.queries.is_some() {
        return ApiError::response(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "discover_and_record analyzes this engine's own stored queries; \
             use /_vocab/aliases/discover for an explicit corpus"
                .to_string(),
        )
        .into_response();
    }
    let cfg = req.config();
    let result = {
        let mut engine = state.engine.lock();
        match engine.discover_aliases_and_record(&cfg) {
            Ok(report) => (
                StatusCode::OK,
                Json(DiscoverRecordResponse {
                    acknowledged: true,
                    proposed: report.proposed,
                    new_candidates: report.new_candidates,
                    rediscovered: report.rediscovered,
                    rejected_sticky: report.rejected_sticky,
                    recompiled: 0,
                    summary: report.summary,
                }),
            )
                .into_response(),
            Err(e) => ApiError::response(StatusCode::BAD_REQUEST, "vocab_error", e.to_string())
                .into_response(),
        }
    };
    state.publish_snapshot();
    result
}

#[cfg(test)]
mod tests {
    use super::{
        discover_aliases, discover_and_record_aliases, get_aliases, import_aliases,
        DiscoverAliasesRequest, ImportAliasesRequest,
    };
    use crate::metrics::PrometheusMetrics;
    use crate::state::AppState;
    use axum::extract::State;
    use axum::response::IntoResponse;
    use axum::Json;
    use reverse_rusty::segment::Engine;
    use reverse_rusty::Normalizer;
    use std::sync::Arc;

    fn state_with(eng: Engine) -> Arc<AppState> {
        let snap = Arc::new(eng.snapshot());
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .expect("pool");
        Arc::new(AppState {
            engine: parking_lot::Mutex::new(eng),
            snapshot: arc_swap::ArcSwap::new(snap),
            pool,
            search_permits: None,
            include_broad: false,
            prom: PrometheusMetrics::new(),
            slow_query_threshold_ms: 0,
            auth: None,
        })
    }

    async fn body_json(resp: axum::response::Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        serde_json::from_slice(&bytes).expect("json body")
    }

    /// Import → the snapshot is republished so the lock-free `GET /_vocab/aliases` reflects it:
    /// the variant pair is active, the category-alternative triple is a candidate.
    #[tokio::test]
    async fn import_then_get_reflects_registry_via_published_snapshot() {
        let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
        eng.build_from_queries(&[(1u64, "fleer autograph".to_string())]);
        let state = state_with(eng);

        let imp = import_aliases(
            State(Arc::clone(&state)),
            Json(ImportAliasesRequest {
                synonyms: "autograph, autographs\npsa, bgs, sgc".to_string(),
            }),
        )
        .await;
        let imp = body_json(imp).await;
        assert_eq!(imp["acknowledged"], true);
        // The single-token variant activates; the declared distinct triple ALSO activates
        // (operator intent), so two groups are active.
        assert_eq!(imp["activated"], 2);

        // GET reads the published snapshot (no engine lock) and sees the same state.
        let got = body_json(get_aliases(State(Arc::clone(&state))).await.into_response()).await;
        assert_eq!(got["summary"]["active"], 2);
        let entries = got["aliases"]["entries"].as_array().expect("entries");
        assert_eq!(entries.len(), 2);
    }

    /// A corpus with an obvious substitute pair: two tokens filling the same slot across
    /// family-private contexts, never together, plus filler so contexts carry positive PMI
    /// (the distributional/tests.rs corpus shape).
    fn discovery_corpus() -> Vec<(u64, String)> {
        let mut queries = Vec::new();
        let mut id = 1u64;
        for i in 0..40 {
            queries.push((id, format!("zzud ctxp{} ctxb{}", i % 7, i % 5)));
            id += 1;
            queries.push((id, format!("zzupperdeck ctxp{} ctxb{}", i % 7, i % 5)));
            id += 1;
        }
        for i in 0..200 {
            queries.push((id, format!("filler{i} junk{i}")));
            id += 1;
        }
        queries
    }

    /// Compute-only discovery over an explicit corpus returns proposals and records nothing.
    #[tokio::test]
    async fn discover_returns_proposals_and_records_nothing() {
        let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
        eng.build_from_queries(&[(1u64, "fleer autograph".to_string())]);
        let state = state_with(eng);

        let resp = discover_aliases(
            State(Arc::clone(&state)),
            Some(Json(DiscoverAliasesRequest {
                queries: Some(discovery_corpus()),
                ..Default::default()
            })),
        )
        .await;
        let got = body_json(resp).await;
        assert!(got["count"].as_u64().unwrap() >= 1, "got: {got}");
        let planted_found = got["proposals"].as_array().unwrap().iter().any(|p| {
            let forms: Vec<&str> = p["forms"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap())
                .collect();
            forms.contains(&"zzud") && forms.contains(&"zzupperdeck")
        });
        assert!(
            planted_found,
            "the planted pair must be proposed; got: {got}"
        );

        // Nothing recorded: the registry is untouched.
        let reg = body_json(get_aliases(State(Arc::clone(&state))).await.into_response()).await;
        assert_eq!(reg["summary"]["candidate"], 0);
        assert_eq!(reg["summary"]["active"], 0);
    }

    /// discover_and_record files candidates (never active) from the ENGINE's own queries,
    /// republishes the snapshot, and refuses an explicit corpus.
    #[tokio::test]
    async fn discover_and_record_files_candidates_only() {
        let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
        eng.build_from_queries(&discovery_corpus());
        let state = state_with(eng);

        let resp = discover_and_record_aliases(State(Arc::clone(&state)), None).await;
        let got = body_json(resp).await;
        assert_eq!(got["acknowledged"], true, "got: {got}");
        assert!(got["new_candidates"].as_u64().unwrap() >= 1);
        assert_eq!(got["recompiled"], 0, "metadata-only: nothing recompiles");

        // Visible via the published snapshot — as candidates, nothing active.
        let reg = body_json(get_aliases(State(Arc::clone(&state))).await.into_response()).await;
        assert!(reg["summary"]["candidate"].as_u64().unwrap() >= 1);
        assert_eq!(reg["summary"]["active"], 0);

        // An explicit corpus on the record path is refused (400).
        let resp = discover_and_record_aliases(
            State(Arc::clone(&state)),
            Some(Json(DiscoverAliasesRequest {
                queries: Some(vec![(1, "a b".to_string())]),
                ..Default::default()
            })),
        )
        .await;
        assert_eq!(resp.status(), axum::http::StatusCode::BAD_REQUEST);
    }
}
