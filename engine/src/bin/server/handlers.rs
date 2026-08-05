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
mod settings_read;
mod settings_write;
mod vocab;

#[cfg(test)]
mod settings_read_tests;
#[cfg(test)]
mod settings_write_tests;

pub(crate) use admin::{
    api_root, cat_segments, cat_stats, compact_route, finish_metrics_response, flush_route,
    force_merge_route, health, prometheus_metrics, stats, CAT_SEGMENTS_BODY_LIMIT,
    HEALTH_BODY_LIMIT, METRICS_BODY_LIMIT, STATS_BODY_LIMIT,
};
pub(crate) use alias::{
    alias_discover_method_not_allowed, alias_discover_record_method_not_allowed,
    alias_feedback_apply_method_not_allowed, alias_feedback_read_method_not_allowed,
    alias_feedback_reset_method_not_allowed, alias_import_method_not_allowed,
    alias_learn_apply_method_not_allowed, alias_read_method_not_allowed, discover_aliases,
    discover_and_record_aliases, get_alias_feedback, get_aliases, import_aliases,
    learn_and_apply_aliases, reset_alias_feedback, validate_and_apply_feedback,
    ALIAS_DISCOVER_BODY_LIMIT, ALIAS_DISCOVER_RECORD_BODY_LIMIT, ALIAS_FEEDBACK_APPLY_BODY_LIMIT,
    ALIAS_FEEDBACK_READ_BODY_LIMIT, ALIAS_FEEDBACK_RESET_BODY_LIMIT, ALIAS_IMPORT_BODY_LIMIT,
    ALIAS_LEARN_APPLY_BODY_LIMIT, ALIAS_READ_BODY_LIMIT,
};
pub(crate) use backup::{backup_route, BACKUP_BODY_LIMIT};
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
    cluster_validate_and_apply_feedback, CAT_SHARDS_BODY_LIMIT, CHECKPOINT_BODY_LIMIT,
    CLUSTER_HANDOFF_BODY_LIMIT, CLUSTER_NODE_DEREGISTER_BODY_LIMIT,
    CLUSTER_NODE_REGISTER_BODY_LIMIT, CLUSTER_REBALANCE_BODY_LIMIT, CLUSTER_RESIZE_BODY_LIMIT,
    CLUSTER_RESYNC_BODY_LIMIT, CLUSTER_STATE_BODY_LIMIT,
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
pub(crate) use settings_read::{
    acquire_settings_read_permit, finish_settings_read_worker, get_settings,
    serialize_settings_response, settings_method_not_allowed, SettingsReadTransport,
    SETTINGS_READ_BODY_LIMIT,
};
pub(crate) use settings_write::{
    apply_settings_patch, finish_settings_write_response, put_settings,
    settings_write_error_response, SettingsWriteTransport, SETTINGS_WRITE_BODY_LIMIT,
};
pub(crate) use vocab::{
    get_vocab, learn_and_apply_vocab, learn_vocab, put_vocab, vocab_learn_apply_method_not_allowed,
    vocab_learn_method_not_allowed, vocab_method_not_allowed, VOCAB_LEARN_APPLY_BODY_LIMIT,
    VOCAB_LEARN_BODY_LIMIT, VOCAB_READ_BODY_LIMIT, VOCAB_WRITE_BODY_LIMIT,
};
