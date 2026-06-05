//! HTTP request handlers, grouped by endpoint family. Each submodule owns the
//! request/response DTOs specific to its endpoints; cross-cutting response types
//! (the error envelope, the `_source` projection) live in [`crate::dto`].

mod admin;
mod alias;
mod doc;
mod search;
mod vocab;

pub(crate) use admin::{
    api_root, cat_segments, cat_stats, compact, flush, health, prometheus_metrics, stats,
};
pub(crate) use alias::{get_aliases, import_aliases, learn_and_apply_aliases};
pub(crate) use doc::{bulk_ingest, delete_doc, get_doc, put_doc};
pub(crate) use search::{mpercolate, search};
pub(crate) use vocab::{
    get_settings, get_vocab, learn_and_apply_vocab, learn_vocab, put_settings, put_vocab,
};
