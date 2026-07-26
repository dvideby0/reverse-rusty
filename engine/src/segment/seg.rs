//! `impl Segment` — the in-memory (or memtable) index slice: append, probe,
//! tombstone, and the per-segment memory accounting. Type definition lives in
//! the `segment` module root; the compaction merges live in the sibling
//! [`merge`](super::merge) submodule.

use super::{
    infallible, AddedCompiled, CompileKnobs, DeadlineCheck, DeadlinePoll, MatchStats, NoDeadline,
    ProbeLanes, Segment,
};
use crate::collect::{MatchSink, VecSink};
use crate::compile::{build_signatures, is_hot, CostClass, Extracted};
use crate::dict::Dict;
use crate::exact::ExactStore;
use crate::filter::SegmentFilter;
use crate::index::CandidateIndex;
use crate::util::sig_key;

/// Which candidate index a [`Segment::probe`] call is reading — routes the
/// per-lane [`MatchStats`] counters without a boolean pair.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::segment) enum ProbeLane {
    Main,
    Broad,
    Hot,
}

/// The single accept/reject predicate for a compiled plan's cost class (ADR-068):
/// class D is stored only when the lane is on AND the query has forbidden features
/// (a query with no positives and no negatives would match every title outright —
/// rejected regardless). Shared by [`Segment::add_compiled`] and the live write
/// paths' pre-WAL gate (`segment/ingest.rs`) so the two sites cannot drift — the
/// WAL records only accepted mutations, making replay unconditional.
pub(in crate::segment) fn rejects_class_d(
    class: CostClass,
    ex: &Extracted,
    accept_class_d: bool,
) -> bool {
    // Reject a class-D plan unless the lane is on AND there is something to
    // forbid, including a whole multi-feature any-of member.
    class == CostClass::D && (!accept_class_d || !ex.has_negative_predicate())
}

mod matching;
mod parts;
mod write;
