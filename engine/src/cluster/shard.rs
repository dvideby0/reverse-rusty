//! `Shard` — the local↔remote seam — and `LocalShard`, its in-process implementation.
//!
//! [`Shard`] abstracts the OPERATION a coordinator performs on a shard, never the
//! shard's internal data: a remote shard has no in-process [`EngineSnapshot`](crate::segment::EngineSnapshot), so the
//! trait exposes [`Shard::percolate_filtered`] (the matched ids + stats for one title) rather
//! than handing back a snapshot. [`LocalShard`] is the in-process impl — an owned
//! [`Engine`](crate::segment::Engine) (writes serialized behind a `std::sync::Mutex`) plus an
//! `ArcSwap<EngineSnapshot>` for lock-free reads, exactly the per-engine pattern the
//! HTTP server uses. It does NOT re-implement matching; `percolate_filtered` delegates to
//! [`EngineSnapshot::match_title_filtered`]. Every `LocalShard` is constructed with
//! [`Engine::with_shared`] over the coordinator's frozen normalizer + dict + tag dict, and all
//! writes go through the read-only `*_extracted` paths so the shared `Arc<Dict>` /
//! `Arc<TagDict>` is never forked.
//!
//! Every operation returns [`Result<_, ShardError>`]: a `LocalShard` is infallible
//! (it always returns `Ok`), but a remote shard can fail on the wire. Surfacing that
//! as an error — rather than swallowing it into an empty result — is load-bearing for
//! the zero-false-negative contract: a dropped shard probe must fail the percolate,
//! not silently shrink the answer. The remote implementation (`RemoteShard`, behind
//! the `distributed` feature) lives in `super::remote` and satisfies the same trait
//! by issuing gRPC calls.
//!
//! This file is the module ROOT: it holds the seam *definitions* shared across the
//! module — [`ShardError`], the [`EventSink`] alias, the [`Shard`] trait, and the
//! free-standing [`apply_mutation`] replay glue — while the `impl`-heavy concerns live
//! in focused submodules:
//!   - [`retention`] — the translog retention-lease bookkeeping ([`RetentionLeases`],
//!     ADR-040/048) plus the `resolve_lease_ttl` config helper.
//!   - [`local`]     — [`LocalShard`]: its struct, every constructor, the `Shard` impl,
//!     and the clock-injectable seal core (`seal_for_checkpoint_at`).

use crate::compile::{extract_readonly, Extracted};
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::exact::TagPredicate;
use crate::normalize::Normalizer;
use crate::segment::{IngestReport, MatchStats, PlacedQuery};
use crate::tagdict::TagDict;
use std::path::Path;
use std::sync::Arc;

use super::clog::{ClusterMutation, LogPos};

mod fetch;
mod local;
mod retention;

#[cfg(test)]
mod tests;

pub(crate) use fetch::fetch_source_step;
pub(crate) use local::LocalShard;

mod api;
mod error;
mod mutation;

pub(crate) use api::{
    BatchTitleRequest, EventSink, FetchedMatch, LiveTaggedQuery, Shard, ShardBatchRankedMatch,
    ShardRankedMatch, ShardRankedTitle,
};
pub use error::ShardError;
pub(crate) use mutation::apply_mutation;
