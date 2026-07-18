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
mod pit;
mod search;
mod vocab;

pub(crate) use admin::{
    api_root, cat_segments, cat_stats, compact, flush, health, prometheus_metrics, stats,
};
pub(crate) use alias::{
    discover_aliases, discover_and_record_aliases, get_alias_feedback, get_aliases, import_aliases,
    learn_and_apply_aliases, reset_alias_feedback, validate_and_apply_feedback,
};
pub(crate) use backup::backup;
pub(crate) use cluster::{
    cluster_backup, cluster_bulk, cluster_cat_segments, cluster_cat_shards, cluster_cat_stats,
    cluster_checkpoint, cluster_compact, cluster_delete_doc, cluster_deregister_node,
    cluster_discover_aliases, cluster_discover_and_record_aliases, cluster_flush, cluster_gc,
    cluster_get_alias_feedback, cluster_get_aliases, cluster_get_doc, cluster_get_settings,
    cluster_get_vocab, cluster_handoff, cluster_health, cluster_import_aliases,
    cluster_learn_aliases, cluster_learn_and_apply_vocab, cluster_learn_vocab, cluster_metrics,
    cluster_mpercolate, cluster_put_doc, cluster_put_settings, cluster_put_vocab, cluster_reassign,
    cluster_rebalance, cluster_reconcile, cluster_register_node, cluster_reset_alias_feedback,
    cluster_resize, cluster_resync, cluster_root, cluster_search, cluster_state, cluster_stats,
    cluster_validate_and_apply_feedback,
};
pub(crate) use doc::{bulk_ingest, delete_doc, get_doc, put_doc};
pub(crate) use pit::{close_pit, cluster_close_pit, cluster_open_pit, open_pit};
pub(crate) use search::{
    cluster_v2_mpercolate, cluster_v2_search, mpercolate, search, v2_mpercolate, v2_search,
};
pub(crate) use vocab::{
    get_settings, get_vocab, learn_and_apply_vocab, learn_vocab, put_settings, put_vocab,
};
