//! Cluster-mode `_cluster/*` control-plane operations (ADR-070): the committed-state document, node
//! register/deregister, rebalance (+ the ADR-090 data-moving variant), live handoff, data-moving
//! reassignment, resize, and resync. Split out of `admin.rs` to keep each file under the size budget;
//! `admin.rs` retains the read-only introspection (root/stats/health/metrics/`_cat`) + the durability
//! commit points (flush/checkpoint/backup).

use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use tracing::{info, instrument};
// `error!` is used only by the `distributed` handlers (handoff/reassign/data-moving rebalance).
#[cfg(feature = "distributed")]
use tracing::error;

use reverse_rusty::cluster::{NodeDescriptor, NodeId, NodeRole};

use crate::dto::ApiError;
use crate::state::ClusterAppState;

use super::super::shard_error_response;
// `not_in_cluster_mode` is used only by the non-`distributed` 501 stubs.
#[cfg(not(feature = "distributed"))]
use super::super::not_in_cluster_mode;

/// GET /_cluster/state — the committed control-plane document (membership +
/// shard→node map + ring params + model version).
#[instrument(skip_all)]
pub(crate) async fn cluster_state(State(state): State<Arc<ClusterAppState>>) -> Response {
    let cluster = state.cluster.read();
    match cluster.control_state() {
        Ok(doc) => Json(doc).into_response(),
        Err(e) => shard_error_response("control state unavailable", &e),
    }
}

#[derive(Deserialize)]
pub(crate) struct RegisterNodeBody {
    id: u64,
    #[serde(default)]
    addr: Option<String>,
    /// "data" (default) or "manager".
    #[serde(default)]
    role: Option<String>,
}

/// POST /_cluster/nodes — register (or replace) a cluster member.
#[instrument(skip_all)]
pub(crate) async fn cluster_register_node(
    State(state): State<Arc<ClusterAppState>>,
    Json(body): Json<RegisterNodeBody>,
) -> Response {
    let role = match body.role.as_deref() {
        None | Some("data") => NodeRole::Data,
        Some("manager") => NodeRole::Manager,
        Some(other) => {
            return ApiError::response(
                StatusCode::BAD_REQUEST,
                "validation_error",
                format!("unknown node role {other:?}: expected \"data\" or \"manager\""),
            )
            .into_response()
        }
    };
    let node = NodeDescriptor {
        id: NodeId(body.id),
        addr: body.addr,
        role,
    };
    let result = {
        let cluster = state.cluster.read();
        cluster.register_node(node)
    };
    match result {
        Ok(()) => {
            info!(node_id = body.id, "node registered");
            Json(serde_json::json!({"acknowledged": true})).into_response()
        }
        Err(e) => shard_error_response("node registration failed", &e),
    }
}

/// DELETE /_cluster/nodes/{id} — deregister a member (idempotent).
#[instrument(skip(state))]
pub(crate) async fn cluster_deregister_node(
    State(state): State<Arc<ClusterAppState>>,
    Path(id): Path<u64>,
) -> Response {
    let result = {
        let cluster = state.cluster.read();
        cluster.deregister_node(NodeId(id))
    };
    match result {
        Ok(()) => {
            info!(node_id = id, "node deregistered");
            Json(serde_json::json!({"acknowledged": true})).into_response()
        }
        Err(e) => shard_error_response("node deregistration failed", &e),
    }
}

#[derive(Deserialize, Default)]
#[cfg_attr(not(feature = "distributed"), allow(dead_code))]
pub(crate) struct RebalanceBody {
    /// When true, MOVE each reassigned position's data via live handoff and commit the new owner —
    /// the data-moving rebalance (ADR-090, `distributed` only). Default false = the map-only HRW
    /// rebalance (ADR-042), byte-identical to the prior behavior (an empty body decodes to false, so
    /// existing no-body callers are unaffected).
    #[serde(default, rename = "move")]
    do_move: bool,
    /// Wave parallelism for the data-moving sweep (ADR-095): up to N conflict-free moves run
    /// concurrently. Absent/`1` (the default) = the sequential sweep, byte-identical. Ignored
    /// without `"move": true`.
    #[serde(default)]
    max_parallel: Option<usize>,
}

/// POST /_cluster/rebalance — recompute the desired shard→node map from membership
/// (rendezvous/HRW, ADR-042) and commit only the changed positions. With `{"move": true}` (ADR-090,
/// `distributed` only) it additionally MOVES each reassigned position's data via live handoff so
/// routing follows the new map live and across a restart; without it (the default, and any empty
/// body) it stays map-only — which must NOT be used alone to re-point a populated remote cluster.
#[instrument(skip_all)]
pub(crate) async fn cluster_rebalance(
    State(state): State<Arc<ClusterAppState>>,
    body: axum::body::Bytes,
) -> Response {
    // Parse leniently: an empty body (the common no-arg call) is map-only, preserving the prior
    // signature; a present-but-invalid body is a clean 400.
    let parsed = if body.is_empty() {
        RebalanceBody::default()
    } else {
        match serde_json::from_slice::<RebalanceBody>(&body) {
            Ok(b) => b,
            Err(e) => {
                return ApiError::response(
                    StatusCode::BAD_REQUEST,
                    "bad_request",
                    format!("invalid rebalance body: {e}"),
                )
                .into_response()
            }
        }
    };
    let do_move = parsed.do_move;

    if !do_move {
        // Map-only HRW rebalance (ADR-042) — unchanged; works in-process and remote.
        let result = {
            let cluster = state.cluster.read();
            let rf = cluster.replication_factor();
            cluster.rebalance(rf)
        };
        return match result {
            Ok(reassigned) => {
                info!(reassigned, "rebalance committed (map-only)");
                Json(serde_json::json!({
                    "acknowledged": true, "reassigned": reassigned, "moved_data": false
                }))
                .into_response()
            }
            Err(e) => shard_error_response("rebalance failed", &e),
        };
    }

    // Data-moving rebalance (ADR-090) — distributed only.
    rebalance_move(state, parsed.max_parallel.unwrap_or(1)).await
}

/// The `{"move": true}` arm of [`cluster_rebalance`]: drive a data-moving rebalance on the blocking
/// pool (the move uses the sync→async bridge). A per-position failure stops the sweep fail-forward;
/// the report names what moved, what failed, and what was not attempted, so an operator can resume.
/// `max_parallel` > 1 runs conflict-free moves in waves (ADR-095); 1 is the sequential default.
#[cfg(feature = "distributed")]
async fn rebalance_move(state: Arc<ClusterAppState>, max_parallel: usize) -> Response {
    let handle = tokio::runtime::Handle::current();
    let state_inner = Arc::clone(&state);
    let result = tokio::task::spawn_blocking(move || {
        let cluster = state_inner.cluster.read();
        let rf = cluster.replication_factor();
        cluster.rebalance_and_move_with(rf, max_parallel.max(1), &handle)
    })
    .await;
    match result {
        Ok(Ok(report)) => {
            let moved_count = report.moved.len();
            if let Some((pos, why)) = &report.failed {
                error!(
                    position = *pos,
                    reason = %why,
                    moved = moved_count,
                    "data-moving rebalance stopped at a position (resumable)"
                );
            } else {
                info!(moved = moved_count, "data-moving rebalance complete");
            }
            let acknowledged = report.failed.is_none();
            let failed_json = report
                .failed
                .map(|(p, why)| serde_json::json!({"position": p, "reason": why}));
            Json(serde_json::json!({
                "acknowledged": acknowledged,
                "moved_data": true,
                "moved": report.moved,
                "failed": failed_json,
                "not_attempted": report.not_attempted,
            }))
            .into_response()
        }
        Ok(Err(e)) => shard_error_response("data-moving rebalance failed", &e),
        Err(e) => {
            error!(error = %e, "rebalance task panicked");
            ApiError::response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "rebalance_error",
                "internal rebalance task failed",
            )
            .into_response()
        }
    }
}

/// The non-`distributed` build cannot drive a data move (the gRPC transport is compiled out) — the
/// map-only rebalance still runs; `{"move":true}` answers a 501-with-reason instead of silently
/// ignoring the flag.
// `async` (with no await) to match the distributed signature so `cluster_rebalance` can `.await` it
// uniformly across both builds.
#[cfg(not(feature = "distributed"))]
#[allow(clippy::unused_async)]
async fn rebalance_move(_state: Arc<ClusterAppState>, _max_parallel: usize) -> Response {
    not_in_cluster_mode(
        "POST /_cluster/rebalance {\"move\":true}",
        "a data-moving rebalance needs the gRPC transport — rebuild the server with --features \
         distributed, or omit \"move\" for the map-only rebalance",
    )
}

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
