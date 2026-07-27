//! Handler-level document API tests: ES/OS-shaped GET/HEAD/PUT/DELETE contracts,
//! atomic replace-by-id visibility, strict controls, and source metadata.

use super::{bulk_route, delete_doc, get_doc, put_doc, DeleteDocParams};
use crate::metrics::PrometheusMetrics;
use crate::state::AppState;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{Request, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Json;
use axum::Router;
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::Normalizer;
use std::sync::Arc;
use tower::ServiceExt;

fn state() -> Arc<AppState> {
    let eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    state_with_engine(eng)
}

fn state_with_engine(eng: Engine) -> Arc<AppState> {
    let snap = Arc::new(eng.snapshot());
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .expect("pool");
    let prom = PrometheusMetrics::new();
    Arc::new(AppState {
        engine: parking_lot::Mutex::new(eng),
        snapshot: arc_swap::ArcSwap::new(snap),
        pool,
        search_permits: None,
        ranked_search_permits: Arc::new(tokio::sync::Semaphore::new(2)),
        exhaustive_jobs: crate::jobs::ExhaustiveJobs::for_tests(prom.clone()),
        max_ranked_enrichment_bytes: crate::state::DEFAULT_MAX_RANKED_ENRICHMENT_BYTES,
        include_broad: true,
        prom,
        slow_query_threshold_ms: 0,
        auth: None,
        feedback: parking_lot::Mutex::new(reverse_rusty::vocab::AliasFeedback::default()),
        pit_tokens: crate::pit::PitTokens::generate(),
        pits: parking_lot::Mutex::new(reverse_rusty::PitRegistry::new()),
        pit_config: reverse_rusty::PitConfig::default(),
    })
}

fn put_body(query: &str) -> super::PutDocBody {
    serde_json::from_value(serde_json::json!({ "query": query })).expect("valid body")
}

/// Run `put_doc` and return (status, parsed JSON body).
async fn do_put(state: &Arc<AppState>, id: u64, query: &str) -> (StatusCode, serde_json::Value) {
    let resp = put_doc(
        State(Arc::clone(state)),
        Path(id),
        Ok(Query(super::PutDocParams::default())),
        Json(put_body(query)),
    )
    .await
    .into_response();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("body");
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("json body");
    (status, json)
}

fn matches_in_snapshot(state: &Arc<AppState>, title: &str) -> Vec<u64> {
    let snap = state.snapshot.load();
    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    snap.match_title(title, &mut s, &mut out, true);
    out.sort_unstable();
    out
}

fn doc_router(state: &Arc<AppState>) -> Router {
    Router::new()
        .route("/_doc/{id}", get(get_doc).put(put_doc).delete(delete_doc))
        .with_state(Arc::clone(state))
}

async fn route_doc(
    state: &Arc<AppState>,
    request: Request<Body>,
) -> (StatusCode, axum::body::Bytes) {
    let response = doc_router(state)
        .oneshot(request)
        .await
        .expect("router response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    (status, body)
}

fn put_request(path: &str, body: &serde_json::Value) -> Request<Body> {
    Request::builder()
        .method("PUT")
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("PUT request")
}

async fn route_put_json(
    state: &Arc<AppState>,
    path: &str,
    body: &serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let (status, bytes) = route_doc(state, put_request(path, body)).await;
    let json = serde_json::from_slice(&bytes).expect("JSON response");
    (status, json)
}

// -- Tag-value coercion + loud rejects (ADR-073, closing ADR-064 item 4) ----

/// Shorthand: run `extract_ingest_tags` over a JSON body's top-level object.
fn tags_of(body: &serde_json::Value) -> Result<Vec<(String, String)>, String> {
    let obj = body.as_object().expect("test body is an object");
    super::extract_ingest_tags(obj)
}

mod bulk;
mod delete;
mod get;
mod parsing;
mod put;
mod ranked;
