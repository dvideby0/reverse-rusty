//! Handler-level tests for the `PUT /_doc/{id}` atomic upsert (ADR-067): the ES
//! 201-created / 200-updated status split, replace-by-id visible through the
//! published snapshot, and DELETE reporting one live copy after a re-PUT (the
//! ADR-064 audit observed `deleted_count: 2` on the pre-fix additive path).

use super::{delete_doc, put_doc};
use crate::metrics::PrometheusMetrics;
use crate::state::AppState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::Normalizer;
use std::sync::Arc;

fn state() -> Arc<AppState> {
    let eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    let snap = Arc::new(eng.snapshot());
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .expect("pool");
    Arc::new(AppState {
        engine: parking_lot::Mutex::new(eng),
        snapshot: arc_swap::ArcSwap::new(snap),
        pool,
        include_broad: true,
        prom: PrometheusMetrics::new(),
        slow_query_threshold_ms: 0,
        auth: None,
    })
}

fn put_body(query: &str) -> super::PutDocBody {
    serde_json::from_value(serde_json::json!({ "query": query })).expect("valid body")
}

/// Run `put_doc` and return (status, parsed JSON body).
async fn do_put(state: &Arc<AppState>, id: u64, query: &str) -> (StatusCode, serde_json::Value) {
    let resp = put_doc(State(Arc::clone(state)), Path(id), Json(put_body(query)))
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

#[tokio::test]
async fn put_doc_is_created_then_updated_with_replace_semantics() {
    let state = state();

    // First PUT: 201 created.
    let (status, body) = do_put(&state, 7, "michael jordan").await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["result"], "created");
    assert!(matches_in_snapshot(&state, "1986 fleer michael jordan rookie").contains(&7));

    // Re-PUT with different semantics: 200 updated, and the snapshot flips
    // atomically — the old version stops matching exactly when the new starts
    // (one lock, one publish; no matches-under-either-version window).
    let (status, body) = do_put(&state, 7, "lebron james").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"], "updated");
    assert!(
        !matches_in_snapshot(&state, "1986 fleer michael jordan rookie").contains(&7),
        "old semantics must stop matching after the re-PUT"
    );
    assert!(matches_in_snapshot(&state, "2003 topps lebron james rookie").contains(&7));
}

#[tokio::test]
async fn delete_after_reput_reports_one_copy() {
    let state = state();
    do_put(&state, 7, "michael jordan").await;
    do_put(&state, 7, "lebron james").await;

    let resp = delete_doc(State(Arc::clone(&state)), Path(7))
        .await
        .into_response();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("body");
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
    assert_eq!(
        json["deleted_count"], 1,
        "replace-by-id leaves exactly one live copy (the audit observed 2)"
    );
}

#[tokio::test]
async fn rejected_reput_leaves_old_version_live() {
    let state = state();
    do_put(&state, 7, "michael jordan").await;

    // A parse error never reaches the engine; the old version stays live.
    let (status, _) = do_put(&state, 7, "(").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(matches_in_snapshot(&state, "1986 fleer michael jordan rookie").contains(&7));

    // A class-D rejection (negation-only) also leaves the old version live.
    let (status, body) = do_put(&state, 7, "-graded").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["result"], "rejected");
    assert!(
        matches_in_snapshot(&state, "1986 fleer michael jordan rookie").contains(&7),
        "a failed replace must never delete"
    );
}
