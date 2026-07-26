//! `impl Engine` — runtime vocabulary: querying/recording a [`Vocab`](crate::vocab::Vocab),
//! the stale-epoch bookkeeping, the live-source / live-tag readers, and the
//! recompile-on-vocabulary-change pass ([`recompile_stale_segments`](Engine::recompile_stale_segments))
//! plus the corpus learn-and-apply drivers (ADR-046/053/054).

use crate::segment::{AliasApplyReport, AliasDiscoveryReport, Engine, Segment};
use crate::vocab::AliasSummary;
use std::sync::Arc;

mod aliases;
mod install;
mod recompile;
mod sources;

#[cfg(test)]
mod tests;
