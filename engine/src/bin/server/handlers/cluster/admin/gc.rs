//! `POST /_cluster/gc` — the one-shot orphan-slot GC sweep (ADR-096): reclaim the fenced,
//! unrouted slots data-moving reassignment strands on their old nodes. The manual trigger of what
//! the opt-in `--reconcile-gc-orphans` loop epilogue runs after each converged pass. Split from
//! [`ops`](super::ops) for the file-size budget.

use std::sync::Arc;

use axum::{extract::State, response::Response};

use crate::state::ClusterAppState;

/// POST /_cluster/gc — sweep every addr'd data node for orphan slots (hosted, not committed to
/// that node, not live-routed) and reclaim them (slot map + `shard_<id>/` disk). Idempotent — a
/// clean cluster sweeps to an empty report; per-slot failures are recorded and the sweep
/// continues. Runs on the blocking pool (each probe/drop uses the sync→async bridge) holding the
/// cluster read guard, and takes the engine's whole-sweep move-ledger reservation (never
/// interleaves a data move). `acknowledged` is true when nothing failed. Requires a
/// `--features distributed` build; else 501.
#[cfg(feature = "distributed")]
#[tracing::instrument(skip_all)]
pub(crate) async fn cluster_gc(State(state): State<Arc<ClusterAppState>>) -> Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::Json;
    use tracing::{error, info};

    use super::super::shard_error_response;
    use crate::dto::ApiError;

    let handle = tokio::runtime::Handle::current();
    let state_inner = Arc::clone(&state);
    let result = tokio::task::spawn_blocking(move || {
        let cluster = state_inner.cluster.read();
        cluster.gc_orphan_slots(&handle)
    })
    .await;
    match result {
        Ok(Ok(report)) => {
            info!(
                dropped = report.dropped.len(),
                kept_live_routed = report.kept_live_routed.len(),
                skipped_unassigned = report.skipped_unassigned.len(),
                failed = report.failed.len(),
                skipped_nodes = report.skipped_nodes.len(),
                "orphan-slot GC sweep complete"
            );
            let slot_json = |s: &reverse_rusty::cluster::OrphanSlot| {
                serde_json::json!({
                    "node": s.node.0, "shard": s.shard_id, "num_queries": s.num_queries
                })
            };
            let failed: Vec<_> = report
                .failed
                .iter()
                .map(|(s, why)| {
                    serde_json::json!({
                        "node": s.node.0, "shard": s.shard_id, "reason": why
                    })
                })
                .collect();
            let skipped_nodes: Vec<_> = report
                .skipped_nodes
                .iter()
                .map(|(n, why)| serde_json::json!({"node": n.0, "reason": why}))
                .collect();
            Json(serde_json::json!({
                "acknowledged": report.is_clean(),
                "dropped": report.dropped.iter().map(slot_json).collect::<Vec<_>>(),
                "kept_live_routed": report.kept_live_routed.iter().map(slot_json).collect::<Vec<_>>(),
                "skipped_unassigned": report.skipped_unassigned.iter().map(slot_json).collect::<Vec<_>>(),
                "failed": failed,
                "skipped_nodes": skipped_nodes,
            }))
            .into_response()
        }
        Ok(Err(e)) => shard_error_response("orphan-slot GC failed", &e),
        Err(e) => {
            error!(error = %e, "orphan-slot GC task panicked");
            ApiError::response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "gc_error",
                "internal GC task failed",
            )
            .into_response()
        }
    }
}

/// The non-`distributed` build cannot sweep remote nodes (the gRPC transport is compiled out) —
/// answer the standard 501-with-reason instead of a silent 404.
#[cfg(not(feature = "distributed"))]
pub(crate) async fn cluster_gc(State(_state): State<Arc<ClusterAppState>>) -> Response {
    super::super::not_in_cluster_mode(
        "POST /_cluster/gc",
        "the orphan-slot GC sweep needs the gRPC transport — rebuild the server with \
         --features distributed",
    )
}
