//! HTTP request handlers, grouped by endpoint family. Each submodule owns the
//! request/response DTOs specific to its endpoints; cross-cutting response types
//! (the error envelope, the `_source` projection) live in [`crate::dto`]. The
//! [`cluster`] family is the coordinator-mode surface (ADR-070) — the same REST
//! dialect over a `ClusterEngine`.

mod admin;
pub(crate) mod alias;
mod backup;
mod cluster;
mod doc;
mod jobs;
mod pit;
mod search;
mod vocab;

pub(crate) use admin::{
    api_root, cat_segments, cat_stats, compact_route, flush_route, force_merge_route, health,
    prometheus_metrics, stats,
};
pub(crate) use alias::{
    discover_aliases, discover_and_record_aliases, get_alias_feedback, get_aliases, import_aliases,
    learn_and_apply_aliases, reset_alias_feedback, validate_and_apply_feedback,
};
pub(crate) use backup::backup;
pub(crate) use cluster::{
    cluster_backup, cluster_bulk_route, cluster_cat_segments, cluster_cat_shards,
    cluster_cat_stats, cluster_checkpoint, cluster_compact, cluster_delete_doc,
    cluster_deregister_node, cluster_discover_aliases, cluster_discover_and_record_aliases,
    cluster_flush_route, cluster_gc, cluster_get_alias_feedback, cluster_get_aliases,
    cluster_get_doc, cluster_get_settings, cluster_get_vocab, cluster_handoff, cluster_health,
    cluster_import_aliases, cluster_learn_aliases, cluster_learn_and_apply_vocab,
    cluster_learn_vocab, cluster_metrics, cluster_mpercolate_route, cluster_put_doc,
    cluster_put_settings, cluster_put_vocab, cluster_reassign, cluster_rebalance,
    cluster_reconcile, cluster_register_node, cluster_reset_alias_feedback, cluster_resize,
    cluster_resync, cluster_root, cluster_search_route, cluster_state, cluster_stats,
    cluster_validate_and_apply_feedback,
};
pub(crate) use doc::{bulk_route, delete_doc, get_doc, put_doc};
pub(crate) use jobs::{
    cancel_job, cluster_cancel_job, cluster_create_job_route, cluster_get_job,
    cluster_get_job_stream, create_job_route, get_job, get_job_stream, EXHAUSTIVE_JOB_BODY_LIMIT,
};
pub(crate) use pit::{
    close_pit_route, cluster_close_pit_route, cluster_open_pit_route, open_pit_route,
    PIT_BODY_LIMIT,
};
pub(crate) use search::{
    cluster_v2_mpercolate_route, cluster_v2_search_route, mpercolate_route, search_route,
    v2_mpercolate_route, v2_search_route,
};
pub(crate) use vocab::{
    get_settings, get_vocab, learn_and_apply_vocab, learn_vocab, put_settings, put_vocab,
};
