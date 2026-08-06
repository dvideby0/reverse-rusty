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
//! batches don't interleave. Descriptor mutation takes the exclusive side of
//! `topology_guard`, movement takes its shared side, and `&mut self` blue/green
//! vocabulary/resize operations take the cluster WRITE lock.
//!
//! Submodule map:
//! - [`doc`]    — `_doc` CRUD (PUT = the single-frame cluster upsert) + `_bulk`.
//! - [`search`] — `_search` + `_mpercolate` over `percolate_filtered_with_stats`.
//! - [`admin`]  — root/stats/health/metrics/shards + flush/checkpoint + `_cluster/*` ops.
//! - [`node_register`] / [`node_deregister`] — strict descriptor mutation boundaries.
//! - [`vocab`]  — `_vocab*` (set/learn/apply + aliases) + `_settings`.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use reverse_rusty::cluster::ShardError;

use crate::dto::ApiError;

mod admin;
mod checkpoint;
mod doc;
mod node_deregister;
mod node_register;
mod search;
mod state_read;
mod vocab;

#[cfg(test)]
mod tests;

pub(crate) use admin::{
    cluster_backup, cluster_cat_segments, cluster_cat_shards, cluster_cat_stats, cluster_compact,
    cluster_flush_route, cluster_gc, cluster_handoff, cluster_health, cluster_metrics,
    cluster_reassign, cluster_rebalance, cluster_reconcile, cluster_resize, cluster_resync,
    cluster_root, cluster_stats, CAT_SHARDS_BODY_LIMIT, CLUSTER_HANDOFF_BODY_LIMIT,
    CLUSTER_REASSIGN_BODY_LIMIT, CLUSTER_REBALANCE_BODY_LIMIT, CLUSTER_RECONCILE_BODY_LIMIT,
    CLUSTER_RESIZE_BODY_LIMIT, CLUSTER_RESYNC_BODY_LIMIT,
};
pub(crate) use checkpoint::{cluster_checkpoint, CHECKPOINT_BODY_LIMIT};
pub(crate) use doc::{cluster_bulk_route, cluster_delete_doc, cluster_get_doc, cluster_put_doc};
pub(crate) use node_deregister::{cluster_deregister_node, CLUSTER_NODE_DEREGISTER_BODY_LIMIT};
pub(crate) use node_register::{cluster_register_node, CLUSTER_NODE_REGISTER_BODY_LIMIT};
pub(crate) use search::{cluster_mpercolate_route, cluster_search_route};
pub(crate) use state_read::{cluster_state, CLUSTER_STATE_BODY_LIMIT};
pub(crate) use vocab::{
    cluster_discover_aliases, cluster_discover_and_record_aliases, cluster_get_alias_feedback,
    cluster_get_aliases, cluster_get_settings, cluster_get_vocab, cluster_import_aliases,
    cluster_learn_aliases, cluster_learn_and_apply_vocab, cluster_learn_vocab,
    cluster_put_settings, cluster_put_vocab, cluster_reset_alias_feedback,
    cluster_validate_and_apply_feedback,
};

/// Map a [`ShardError`] onto the HTTP layer via the classification the error
/// type owns ([`ShardError::write_http_class`] — the write/admin column of the
/// two-surface table in `cluster/http_status.rs`). `PartiallyApplied` classifies
/// as a 200 there for totality only: the write handlers surface it as a 200
/// `partial` result before reaching this generic response, so the caller is
/// told without being told to retry (a re-PUT would double-log).
fn shard_error_response(context: &str, e: &ShardError) -> Response {
    let (_, kind) = e.write_http_class();
    let status = shard_error_status(e);
    ApiError::response(status, kind, format!("{context}: {e}")).into_response()
}

/// Resolve the write/admin HTTP status once so response rendering and endpoint
/// metrics cannot classify the same [`ShardError`] differently.
fn shard_error_status(e: &ShardError) -> StatusCode {
    let (status, _) = e.write_http_class();
    StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
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
