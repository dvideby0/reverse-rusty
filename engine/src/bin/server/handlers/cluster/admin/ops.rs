//! Cluster-mode `_cluster/*` topology operations (ADR-070): live handoff, data-moving
//! reassignment, reconcile, resize, and resync. The strict rebalance boundary lives in
//! the sibling `rebalance` module. Strict committed-state reads and node descriptor
//! mutations live in sibling modules.

use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::State,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use tracing::{info, instrument};

#[cfg(feature = "distributed")]
use axum::http::StatusCode;
#[cfg(feature = "distributed")]
use tracing::error;

#[cfg(feature = "distributed")]
use reverse_rusty::cluster::NodeId;

#[cfg(feature = "distributed")]
use crate::dto::ApiError;
use crate::state::ClusterAppState;

use super::super::shard_error_response;
// `not_in_cluster_mode` is used only by the non-`distributed` 501 stubs.
#[cfg(not(feature = "distributed"))]
use super::super::not_in_cluster_mode;

#[derive(Deserialize)]
// The non-`distributed` build's handoff handler ignores the body (it 501s), so the
// fields read only under the feature — gate the dead-code lint accordingly.
#[cfg_attr(not(feature = "distributed"), allow(dead_code))]
pub(crate) struct HandoffBody {
    /// The shard position to move.
    position: usize,
    /// The current owner's gRPC endpoint (will be fenced + drained).
    source: String,
    /// The new owner's gRPC endpoint (peer-recovered, then routing flips to it).
    target: String,
}

/// POST /_cluster/handoff — live data-moving handoff (ADR-044/048): peer-recover the
/// target from the source under a retention lease, fence the source, drain to
/// convergence, flip routing. The operator surface for the library mechanism (ADR-072);
/// runs on the blocking pool (the drive uses the sync→async bridge). A non-converging
/// (or any post-fence) failure aborts fail-closed and auto-unfences the source — the
/// error surfaces here with the engine's message and the cluster keeps serving.
/// Requires a `--features distributed` build; otherwise a clear 501.
///
/// Deliberately does NOT hold `write_serial`: a handoff is *designed* to run
/// concurrently with ingestion (peer-recover → fence → drain-to-convergence → flip,
/// ADR-044) — that IS the "under load" property the harness exercises. Its own
/// fence + retention lease + atomic backing swap provide the concurrency safety;
/// serializing it against every `/_doc` write would both defeat the under-load test
/// and stall cluster-wide ingestion for the whole (multi-RPC, possibly slow) move
/// (review finding). The cluster READ guard still excludes a concurrent vocab
/// rebuild (`&mut self`), which genuinely must not run mid-handoff.
#[cfg(feature = "distributed")]
#[instrument(skip_all)]
pub(crate) async fn cluster_handoff(
    State(state): State<Arc<ClusterAppState>>,
    Json(body): Json<HandoffBody>,
) -> Response {
    let handle = tokio::runtime::Handle::current();
    let state_inner = Arc::clone(&state);
    let result = tokio::task::spawn_blocking(move || {
        let _topology = state_inner.topology_guard.read();
        let cluster = state_inner.cluster.read();
        cluster.execute_handoff(body.position, &body.source, &body.target, &handle)
    })
    .await;
    match result {
        Ok(Ok(generation)) => {
            info!(generation, "handoff complete; routing flipped");
            Json(serde_json::json!({"acknowledged": true, "generation": generation}))
                .into_response()
        }
        Ok(Err(e)) => {
            error!(error = %e, "handoff failed (source auto-unfenced; cluster still serving)");
            shard_error_response("handoff failed", &e)
        }
        Err(e) => {
            error!(error = %e, "handoff task panicked");
            ApiError::response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "handoff_error",
                "internal handoff task failed",
            )
            .into_response()
        }
    }
}

/// The non-`distributed` build cannot drive a cross-node handoff (the gRPC transport
/// is compiled out) — answer the standard 501-with-reason instead of a silent 404.
#[cfg(not(feature = "distributed"))]
pub(crate) async fn cluster_handoff(Json(_body): Json<HandoffBody>) -> Response {
    not_in_cluster_mode(
        "POST /_cluster/handoff",
        "a live handoff needs the gRPC transport — rebuild the server with \
         --features distributed",
    )
}

#[derive(Deserialize)]
// The non-`distributed` build's reassign handler 501s and ignores the body — gate the dead-code lint.
#[cfg_attr(not(feature = "distributed"), allow(dead_code))]
pub(crate) struct ReassignBody {
    /// The shard position to move.
    position: usize,
    /// The new owner's logical node id (its endpoint is resolved from membership).
    node: u64,
}

/// POST /_cluster/reassign — data-moving reassignment (ADR-090): MOVE shard `position`'s data to
/// node `node` via live handoff, then commit the new owner (move-then-commit). The map-aware,
/// higher-level companion to `/_cluster/handoff` (which takes raw source/target endpoints): this
/// resolves the target endpoint from membership and keeps the committed shard→node map consistent
/// with the live routing, so a coordinator restart (resolve-only) routes to the new owner. Runs on
/// the blocking pool (the move uses the sync→async bridge); does NOT hold `write_serial` — a move
/// runs concurrently with ingestion by design (its own fence + retention lease + the engine-level
/// reassign guard provide concurrency safety). Fail-closed: a failed move moves nothing and commits
/// nothing; a move whose commit fails still serves (zero false negatives) and reports
/// `committed:false` for the operator to retry. Requires a `--features distributed` build; else 501.
#[cfg(feature = "distributed")]
#[instrument(skip_all)]
pub(crate) async fn cluster_reassign(
    State(state): State<Arc<ClusterAppState>>,
    Json(body): Json<ReassignBody>,
) -> Response {
    use reverse_rusty::cluster::ReassignOutcome;
    let handle = tokio::runtime::Handle::current();
    let state_inner = Arc::clone(&state);
    let result = tokio::task::spawn_blocking(move || {
        let _topology = state_inner.topology_guard.read();
        let cluster = state_inner.cluster.read();
        cluster.reassign_and_move(body.position, NodeId(body.node), &handle)
    })
    .await;
    match result {
        Ok(Ok(ReassignOutcome::NoChange { position })) => {
            info!(
                position,
                "reassign: no change (position already on the target)"
            );
            Json(serde_json::json!({
                "acknowledged": true, "moved": false, "committed": false, "position": position
            }))
            .into_response()
        }
        Ok(Ok(ReassignOutcome::Moved {
            position,
            from,
            to,
            generation,
        })) => {
            info!(
                position,
                from = from.0,
                to = to.0,
                generation,
                "reassign: data moved and committed"
            );
            Json(serde_json::json!({
                "acknowledged": true, "moved": true, "committed": true,
                "position": position, "node": to.0, "generation": generation
            }))
            .into_response()
        }
        Ok(Ok(ReassignOutcome::MovedButNotCommitted {
            position,
            from,
            to,
            generation,
        })) => {
            // Zero-FN safe: the data moved + routing flipped, but the durable map still names the
            // (reads-serving) source. Report 200 with committed:false so the operator retries.
            error!(
                position,
                from = from.0,
                to = to.0,
                "reassign: data moved but commit failed (still serving; re-run to reconcile)"
            );
            Json(serde_json::json!({
                "acknowledged": true, "moved": true, "committed": false,
                "position": position, "node": to.0, "generation": generation,
                "warning": "data moved and routing flipped, but committing the new owner failed; \
                            re-run to reconcile the durable map"
            }))
            .into_response()
        }
        Ok(Err(e)) => {
            error!(error = %e, "reassign failed (no data moved; cluster unchanged)");
            shard_error_response("reassign failed", &e)
        }
        Err(e) => {
            error!(error = %e, "reassign task panicked");
            ApiError::response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "reassign_error",
                "internal reassign task failed",
            )
            .into_response()
        }
    }
}

/// The non-`distributed` build cannot drive a cross-node data move (the gRPC transport is compiled
/// out) — answer the standard 501-with-reason instead of a silent 404.
#[cfg(not(feature = "distributed"))]
pub(crate) async fn cluster_reassign(Json(_body): Json<ReassignBody>) -> Response {
    not_in_cluster_mode(
        "POST /_cluster/reassign",
        "a data-moving reassignment needs the gRPC transport — rebuild the server with \
         --features distributed",
    )
}

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

#[derive(Deserialize)]
pub(crate) struct ResizeBody {
    /// The desired new shard count (≥ 1). Equal to the current count ⇒ a no-op.
    num_shards: usize,
}

/// POST /_cluster/resize — blue/green cluster resize (ADR-078, ADR-065 criterion 7):
/// re-place every live query under a fresh consistent-hash ring with `num_shards`
/// buckets, build fresh shards, atomically swap, and (for a durable cluster) checkpoint
/// the result. The vocabulary, dict, and per-query tags are preserved unchanged. The
/// operator surface for the library mechanism; in-process only — a cross-process /
/// handoff-wrapped cluster comes back as a 400 (same boundary as `PUT /_vocab`).
///
/// Holds the writer-serialization mutex + the cluster WRITE lock for the full rebuild
/// (`&mut self`), exactly like `PUT /_vocab` (`set_vocab`): a resize is a stop-the-world
/// blue/green rebuild, not interleavable with incremental writes. Cost is `O(corpus)`, so
/// this is a rare administrative operation (a multi-second pause on a large cluster).
#[instrument(skip_all)]
pub(crate) async fn cluster_resize(
    State(state): State<Arc<ClusterAppState>>,
    Json(body): Json<ResizeBody>,
) -> Response {
    let start = Instant::now();
    let result = {
        let _topology = state.topology_guard.write();
        let _w = state.write_serial.lock();
        let mut cluster = state.cluster.write();
        cluster.resize(body.num_shards)
    };
    match result {
        Ok(rebuilt) => {
            info!(
                num_shards = body.num_shards,
                rebuilt,
                took_ms = start.elapsed().as_millis() as u64,
                "cluster resized"
            );
            Json(serde_json::json!({
                "acknowledged": true,
                "num_shards": body.num_shards,
                "rebuilt": rebuilt,
            }))
            .into_response()
        }
        Err(e) => shard_error_response("resize refused", &e),
    }
}

/// POST /_cluster/resync — re-drive queued partial-apply repairs (ADR-047). Holds
/// the writer-serialization mutex so a resync pass cannot interleave with REST
/// writes for the same ids (the drain → re-drive window; the library-level race
/// with non-REST writers is the documented ADR-047 last-writer-wins scope, healed
/// authoritatively by log replay on reopen).
#[instrument(skip_all)]
pub(crate) async fn cluster_resync(State(state): State<Arc<ClusterAppState>>) -> Response {
    let report = {
        let _w = state.write_serial.lock();
        let cluster = state.cluster.read();
        cluster.resync()
    };
    info!(
        repaired = report.repaired,
        still_pending = report.still_pending,
        "resync pass complete"
    );
    Json(serde_json::json!({
        "repaired": report.repaired,
        "still_pending": report.still_pending,
    }))
    .into_response()
}
