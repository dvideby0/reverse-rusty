//! `MatchScratch` reusable buffers and `EngineSnapshot` — the immutable,
//! lock-free read view and THE HOT PATH (`match_title` and the rayon-parallel
//! batch matchers). Type definitions live in the `segment` module root.

mod read;

use super::{
    infallible, BaseSegment, BatchMatchOptions, DeadlineAt, DeadlineCheck, DeadlinePoll,
    EngineSnapshot, MatchCancelled, MatchScratch, MatchStats, NoDeadline, Segment,
};
use crate::collect::{
    AllCollector, CandidateHitCollector, ChunkCollector, MatchCollector, TopKCollector, TopKScorer,
};
use crate::compile::CostClass;
use crate::delivery::{
    ChunkSink, ExhaustiveMatchError, ExhaustiveMatchResult, ExhaustiveOptions, MAX_MATCH_CHUNK_SIZE,
};
use crate::dict::Dict;
use crate::exact::TagPredicate;
use crate::normalize::Normalizer;
use std::sync::Arc;
use std::time::Instant;

mod batch;
mod ranked;
mod scalar;
mod view;

use view::ExhaustiveDeduper;
pub(in crate::segment) use view::MatchView;

#[cfg(test)]
mod exhaustive_dedup_tests;
