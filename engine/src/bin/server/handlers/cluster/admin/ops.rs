//! Cluster-mode `_cluster/*` topology operations (ADR-070): data-moving reassignment
//! and reconcile. The strict handoff, rebalance, resync, and resize boundaries live in
//! sibling modules. Strict committed-state reads and node descriptor mutations live in
//! sibling modules.

use std::sync::Arc;

#[cfg(feature = "distributed")]
use axum::Json;
use axum::{extract::State, response::Response};
#[cfg(feature = "distributed")]
use serde::Deserialize;

#[cfg(feature = "distributed")]
use axum::http::StatusCode;
#[cfg(feature = "distributed")]
use axum::response::IntoResponse;
#[cfg(feature = "distributed")]
use tracing::{error, info, instrument};

#[cfg(feature = "distributed")]
use crate::dto::ApiError;
use crate::state::ClusterAppState;

#[cfg(feature = "distributed")]
use super::super::shard_error_response;
// `not_in_cluster_mode` is used only by the non-`distributed` 501 stubs.
#[cfg(not(feature = "distributed"))]
use super::super::not_in_cluster_mode;

/// POST /_cluster/reconcile — drive ONE unattended-style reconcile pass (ADR-092): converge the
/// committed shard→node map to the desired HRW placement by MOVING data, continuing past per-position
/// failures (the controller semantics — a manual one-shot of what the `--reconcile-interval-secs` loop
/// runs). Idempotent: a converged map moves nothing and commits nothing. Runs on the blocking pool
/// (each move uses the sync→async bridge); does NOT hold `write_serial` — each move's own fence +
/// retention lease + the engine's busy-endpoint move ledger provide concurrency safety (a reconcile
/// pass runs concurrently with ingestion by design, like `/_cluster/reassign`). An optional
/// `{"max_parallel": N}` body runs up to N conflict-free moves concurrently (ADR-095); an empty body
/// (the common call) is the sequential pass, byte-identical. `acknowledged` is true only when
/// the pass fully converged (no `uncommitted`/`failed` positions). Requires a `--features distributed`
/// build; else 501.
#[cfg(feature = "distributed")]
#[instrument(skip_all)]
pub(crate) async fn cluster_reconcile(
    State(state): State<Arc<ClusterAppState>>,
    body: axum::body::Bytes,
) -> Response {
    // Parse leniently, mirroring `cluster_rebalance`: an empty body is the sequential pass; a
    // present-but-invalid body is a clean 400.
    #[derive(Deserialize, Default)]
    struct ReconcileBody {
        #[serde(default)]
        max_parallel: Option<usize>,
    }
    let parsed = if body.is_empty() {
        ReconcileBody::default()
    } else {
        match serde_json::from_slice::<ReconcileBody>(&body) {
            Ok(b) => b,
            Err(e) => {
                return ApiError::response(
                    StatusCode::BAD_REQUEST,
                    "bad_request",
                    format!("invalid reconcile body: {e}"),
                )
                .into_response()
            }
        }
    };
    let max_parallel = parsed.max_parallel.unwrap_or(1).max(1);
    let handle = tokio::runtime::Handle::current();
    let state_inner = Arc::clone(&state);
    let result = tokio::task::spawn_blocking(move || {
        let _topology = state_inner.topology_guard.read();
        let cluster = state_inner.cluster.read();
        let rf = cluster.replication_factor();
        cluster.reconcile_with(rf, max_parallel, &handle)
    })
    .await;
    match result {
        Ok(Ok(report)) => {
            info!(
                reconciled = report.moved_count(),
                skipped = report.skipped.len(),
                uncommitted = report.uncommitted.len(),
                failed = report.failed.len(),
                converged = report.is_converged(),
                "reconcile pass complete"
            );
            let uncommitted: Vec<_> = report
                .uncommitted
                .iter()
                .map(|(p, from, to)| serde_json::json!({"position": p, "from": from.0, "to": to.0}))
                .collect();
            let failed: Vec<_> = report
                .failed
                .iter()
                .map(|(p, why)| serde_json::json!({"position": p, "reason": why}))
                .collect();
            Json(serde_json::json!({
                "acknowledged": report.is_converged(),
                "converged": report.is_converged(),
                "reconciled": report.reconciled,
                "skipped": report.skipped,
                "uncommitted": uncommitted,
                "failed": failed,
            }))
            .into_response()
        }
        Ok(Err(e)) => shard_error_response("reconcile failed", &e),
        Err(e) => {
            error!(error = %e, "reconcile task panicked");
            ApiError::response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "reconcile_error",
                "internal reconcile task failed",
            )
            .into_response()
        }
    }
}

/// The non-`distributed` build cannot drive the unattended reconciler (the gRPC transport is compiled
/// out) — answer the standard 501-with-reason instead of a silent 404.
#[cfg(not(feature = "distributed"))]
pub(crate) async fn cluster_reconcile(State(_state): State<Arc<ClusterAppState>>) -> Response {
    not_in_cluster_mode(
        "POST /_cluster/reconcile",
        "the unattended reconciler needs the gRPC transport — rebuild the server with \
         --features distributed",
    )
}
