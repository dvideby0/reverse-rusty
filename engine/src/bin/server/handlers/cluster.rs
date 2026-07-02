//! Coordinator-mode handlers (ADR-070): the same REST dialect served over a
//! [`ClusterEngine`](reverse_rusty::cluster::ClusterEngine) instead of a single-node
//! `Engine`. One endpoint set, honest deltas: surfaces with no cluster analogue yet
//! answer **501 with the reason and the alternative**, never a silent degrade — and
//! a request feature the cluster cannot honor (`rank`, `explain`) is a 400, never
//! silently ignored.
//!
//! Concurrency (see [`crate::state::ClusterAppState`]): percolates and ordinary
//! writes take the cluster READ lock (`ClusterEngine` reads are `&self` lock-free;
//! writes are `&self`, log-ordered); writes additionally hold `write_serial` so
//! batches don't interleave; only the vocabulary rebuilds take the WRITE lock.
//!
//! Submodule map:
//! - [`doc`]    — `_doc` CRUD (PUT = the single-frame cluster upsert) + `_bulk`.
//! - [`search`] — `_search` + `_mpercolate` over `percolate_filtered_with_stats`.
//! - [`admin`]  — root/stats/health/metrics/shards + flush/checkpoint + `_cluster/*` ops.
//! - [`vocab`]  — `_vocab*` (set/learn/apply + aliases) + `_settings`.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use reverse_rusty::cluster::ShardError;

use crate::dto::ApiError;

mod admin;
mod doc;
mod search;
mod vocab;

#[cfg(test)]
mod tests;

pub(crate) use admin::{
    cluster_backup, cluster_cat_segments, cluster_cat_shards, cluster_cat_stats,
    cluster_checkpoint, cluster_compact, cluster_deregister_node, cluster_flush, cluster_gc,
    cluster_handoff, cluster_health, cluster_metrics, cluster_reassign, cluster_rebalance,
    cluster_reconcile, cluster_register_node, cluster_resize, cluster_resync, cluster_root,
    cluster_state, cluster_stats,
};
pub(crate) use doc::{cluster_bulk, cluster_delete_doc, cluster_get_doc, cluster_put_doc};
pub(crate) use search::{cluster_mpercolate, cluster_search};
pub(crate) use vocab::{
    cluster_get_aliases, cluster_get_settings, cluster_get_vocab, cluster_import_aliases,
    cluster_learn_aliases, cluster_learn_and_apply_vocab, cluster_learn_vocab,
    cluster_put_settings, cluster_put_vocab,
};

/// Map a [`ShardError`] onto the HTTP layer. `PartiallyApplied` is deliberately NOT
/// here — it is not a failure of the request (the mutation is durably logged and
/// queued for repair); the write handlers surface it as a 200 with a `partial`
/// result so the caller is told without being told to retry (a re-PUT would
/// double-log).
fn shard_error_response(context: &str, e: &ShardError) -> Response {
    let (status, kind) = match e {
        ShardError::Config(_) => (StatusCode::BAD_REQUEST, "validation_error"),
        ShardError::Log(_) => (StatusCode::SERVICE_UNAVAILABLE, "durability_unavailable"),
        ShardError::Remote(_) => (StatusCode::BAD_GATEWAY, "shard_unreachable"),
        ShardError::DictMismatch { .. } => {
            (StatusCode::INTERNAL_SERVER_ERROR, "feature_space_mismatch")
        }
        ShardError::ControlPlane(_) => (StatusCode::SERVICE_UNAVAILABLE, "control_plane_error"),
        ShardError::PartiallyApplied { .. } => (StatusCode::OK, "partially_applied"),
    };
    ApiError::response(status, kind, format!("{context}: {e}")).into_response()
}

/// The shared 501 for a single-node-only surface: names the reason AND the
/// cluster-mode alternative, so hitting it is a doc lookup, not a dead end.
fn not_in_cluster_mode(what: &str, alternative: &str) -> Response {
    ApiError::response(
        StatusCode::NOT_IMPLEMENTED,
        "not_supported_in_cluster_mode",
        format!("{what} has no cluster analogue yet; {alternative}"),
    )
    .into_response()
}
