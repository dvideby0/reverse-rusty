//! Match-feedback endpoints (ADR-103): `/_vocab/aliases/feedback*` + `validate_and_apply`.
//!
//! The read side renders the aggregator's per-candidate-pair behavioral evidence (with the
//! degenerate-evidence exclusion applied against the live snapshot's query sources); the apply
//! side stamps validated pairs into the registry — and, only with the explicit
//! `activate=true`, promotes them through the reject-refusing automated path.

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

fn default_min_overlap() -> f64 {
    0.5
}
fn default_min_titles() -> u64 {
    50
}
fn default_min_queries() -> u64 {
    20
}

#[derive(Deserialize, Default)]
pub(crate) struct FeedbackThresholds {
    /// Minimum Jaccard overlap of the (filtered) matched-query populations (default 0.5).
    #[serde(default = "default_min_overlap")]
    min_overlap: f64,
    /// Minimum single-form title observations per side (default 50).
    #[serde(default = "default_min_titles")]
    min_titles: u64,
    /// Minimum surviving sampled queries per side (default 20).
    #[serde(default = "default_min_queries")]
    min_queries: u64,
    /// `validate_and_apply` only: also activate validated pairs (default false — evidence is
    /// stamped and confidence raised, activation stays an operator act).
    #[serde(default)]
    activate: bool,
}

impl FeedbackThresholds {
    /// Guard NaN/negative garbage from the query string — clamp to sane ranges rather than
    /// letting a NaN threshold validate everything (`NaN >= NaN` is false, but `overlap >=
    /// NaN` comparisons silently reject; be explicit instead).
    fn sanitized(&self) -> (f64, u64, u64) {
        let overlap = if self.min_overlap.is_finite() {
            self.min_overlap.clamp(0.0, 1.0)
        } else {
            default_min_overlap()
        };
        (overlap, self.min_titles, self.min_queries)
    }
}

#[derive(Serialize)]
struct FeedbackResponse {
    capture_enabled: bool,
    tracked_pairs: usize,
    min_overlap: f64,
    min_titles: u64,
    min_queries: u64,
    pairs: Vec<reverse_rusty::vocab::PairFeedback>,
}

/// GET /_vocab/aliases/feedback — the per-candidate-pair evidence report (ADR-103). Reads the
/// aggregator + the lock-free snapshot (for the degenerate-evidence exclusion's source
/// lookups); thresholds are echoed so the caller sees what `validated` meant.
pub(crate) async fn get_alias_feedback(
    State(state): State<Arc<AppState>>,
    Query(q): Query<FeedbackThresholds>,
) -> impl IntoResponse {
    let (min_overlap, min_titles, min_queries) = q.sanitized();
    let snap = state.snapshot.load();
    let capture_enabled = snap.config().alias_feedback_capture;
    let fb = state.feedback.lock();
    let pairs = fb.report(min_overlap, min_titles, min_queries, |id| {
        snap.get_query_source(id)
    });
    Json(FeedbackResponse {
        capture_enabled,
        tracked_pairs: fb.tracked_pairs(),
        min_overlap,
        min_titles,
        min_queries,
        pairs,
    })
}

#[derive(Serialize)]
struct ValidateApplyResponse {
    acknowledged: bool,
    /// Pairs that met the thresholds this pass.
    validated: usize,
    /// Registry entries stamped with evidence (confidence raised to ≥ overlap).
    stamped: usize,
    /// Entries promoted to Active (only with `activate=true`; rejected/mixed-kind refused).
    activated: usize,
    /// Stored queries recompiled (0 unless something activated).
    recompiled: usize,
    summary: reverse_rusty::vocab::AliasSummary,
}

/// POST /_vocab/aliases/validate_and_apply — stamp validated pairs into the registry
/// (metadata-only: confidence + evidence, no recompile); with the explicit `?activate=true`,
/// also promote them (reject-refusing) through the genuine `set_vocab` + recompile path.
pub(crate) async fn validate_and_apply_feedback(
    State(state): State<Arc<AppState>>,
    Query(q): Query<FeedbackThresholds>,
) -> Response {
    let (min_overlap, min_titles, min_queries) = q.sanitized();
    // Evidence rows against the CURRENT snapshot, before taking the engine lock.
    let validated: Vec<(Vec<String>, reverse_rusty::vocab::FeedbackEvidence)> = {
        let snap = state.snapshot.load();
        let fb = state.feedback.lock();
        fb.report(min_overlap, min_titles, min_queries, |id| {
            snap.get_query_source(id)
        })
        .into_iter()
        .filter(|r| r.validated)
        .map(|r| {
            (
                r.forms.clone(),
                reverse_rusty::vocab::FeedbackEvidence {
                    overlap: r.overlap,
                    titles_a: r.titles_a,
                    titles_b: r.titles_b,
                    queries_sampled: r.sampled_a.min(r.sampled_b),
                },
            )
        })
        .collect()
    };
    let result = {
        let mut engine = state.engine.lock();
        match engine.apply_alias_feedback(&validated, q.activate) {
            Ok(report) => (
                StatusCode::OK,
                Json(ValidateApplyResponse {
                    acknowledged: true,
                    validated: validated.len(),
                    stamped: report.stamped,
                    activated: report.activated,
                    recompiled: report.recompiled,
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

#[derive(Serialize)]
struct ResetResponse {
    acknowledged: bool,
}

/// POST /_vocab/aliases/feedback/reset — wipe accumulated evidence (an explicit measurement
/// window boundary). Tracked pairs re-derive from the registry on the next snapshot publish.
pub(crate) async fn reset_alias_feedback(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    state.feedback.lock().reset();
    state.publish_snapshot(); // re-sync tracked pairs immediately
    Json(ResetResponse { acknowledged: true })
}
