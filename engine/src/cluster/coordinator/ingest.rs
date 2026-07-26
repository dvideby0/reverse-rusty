//! `impl ClusterEngine` — the write path: bulk `ingest`, incremental `add_query` /
//! `remove_query`, the shared `apply` / `replay_apply` funnel, placement bucketing, and `flush`.

use crate::cluster::clog::ClusterMutation;
use crate::cluster::shard::ShardError;
use crate::compile::{extract_readonly, Extracted};
use crate::error::{ParseError, ParseErrorKind};
use crate::events::{DurabilityOp, EngineEvent};
use crate::segment::PlacedQuery;

use super::{placement_of, AddOutcome, ClusterEngine, PendingRepair, ResyncReport, Target};

/// One bulk-load entry: `(logical, version, dsl, raw tags)` (ADR-055) — the input to
/// [`ClusterEngine::bucket_and_ingest`], before placement turns it into a [`PlacedQuery`] per shard.
type TaggedEntry = (u64, u32, String, Vec<(String, String)>);

mod bulk;
mod mutate;
mod placement;
mod repair;
