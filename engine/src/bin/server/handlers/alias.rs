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

#[cfg(test)]
mod tests {
    use super::{get_aliases, import_aliases, ImportAliasesRequest};
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
}
