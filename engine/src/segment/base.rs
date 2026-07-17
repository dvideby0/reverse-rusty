//! `impl BaseSegment` — dispatch wrapper over a sealed segment that is either
//! in-memory (`Memory`) or file-backed (`Mmap`). Type definition lives in the
//! `segment` module root.

use super::{BaseSegment, MatchStats, Segment};
use crate::collect::{MatchSink, VecSink};
use crate::dict::Dict;

impl BaseSegment {
    /// How this sealed segment's payload is backed — in-memory heap (`Memory`) or
    /// file-backed/paged (`Mmap`). Lets introspection report off-heap vs resident
    /// without matching on the enum at the call site.
    pub fn storage_kind(&self) -> crate::events::SegmentKind {
        match self {
            BaseSegment::Memory(_) => crate::events::SegmentKind::Memory,
            BaseSegment::Mmap(_) => crate::events::SegmentKind::Mmap,
        }
    }

    /// The vocab epoch at which this segment's queries were compiled.
    pub fn vocab_epoch(&self) -> u64 {
        match self {
            BaseSegment::Memory(s) => s.vocab_epoch,
            BaseSegment::Mmap(s) => s.vocab_epoch,
        }
    }
    pub fn set_vocab_epoch(&mut self, epoch: u64) {
        match self {
            BaseSegment::Memory(s) => s.vocab_epoch = epoch,
            BaseSegment::Mmap(s) => s.vocab_epoch = epoch,
        }
    }
}

impl std::fmt::Debug for BaseSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BaseSegment::Memory(s) => f.debug_tuple("Memory").field(s).finish(),
            BaseSegment::Mmap(s) => f.debug_tuple("Mmap").field(s).finish(),
        }
    }
}

impl BaseSegment {
    pub fn len(&self) -> usize {
        match self {
            BaseSegment::Memory(s) => s.len(),
            BaseSegment::Mmap(s) => s.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn holes_ratio(&self) -> f64 {
        match self {
            BaseSegment::Memory(s) => s.holes_ratio(),
            BaseSegment::Mmap(s) => s.holes_ratio(),
        }
    }
    pub fn alive_count(&self) -> usize {
        match self {
            BaseSegment::Memory(s) => s.alive_count(),
            BaseSegment::Mmap(s) => s.alive_count(),
        }
    }
    pub fn is_alive(&self, local_id: u32) -> bool {
        match self {
            BaseSegment::Memory(s) => *s.alive.get(local_id as usize).unwrap_or(&false),
            BaseSegment::Mmap(s) => *s.alive_overlay.get(local_id as usize).unwrap_or(&false),
        }
    }
    pub fn logical(&self, local_id: u32) -> u64 {
        match self {
            BaseSegment::Memory(s) => s.exact.logical(local_id),
            BaseSegment::Mmap(s) => s.logical(local_id),
        }
    }
    /// The stored per-query version for a local id — read back for the cluster
    /// rebuild gather (ADR-074) so a `set_vocab`/resize preserves a query's stored
    /// version rather than resetting it to 1.
    pub fn version_of(&self, local_id: u32) -> u32 {
        match self {
            BaseSegment::Memory(s) => s.exact.version(local_id),
            BaseSegment::Mmap(s) => s.version(local_id),
        }
    }
    pub fn tombstone(&mut self, local_id: u32) {
        match self {
            BaseSegment::Memory(s) => s.tombstone(local_id),
            BaseSegment::Mmap(s) => s.tombstone(local_id),
        }
    }
    pub fn locals_for_logical(&self, logical_id: u64) -> &[u32] {
        match self {
            BaseSegment::Memory(s) => s.locals_for_logical(logical_id),
            BaseSegment::Mmap(s) => s.locals_for_logical(logical_id),
        }
    }
    /// The sorted `TagId` slice for a local id (ADR-049).
    pub fn tags_of(&self, local_id: u32) -> &[crate::tagdict::TagId] {
        match self {
            BaseSegment::Memory(s) => s.tags_of(local_id),
            BaseSegment::Mmap(s) => s.tags_of(local_id),
        }
    }
    /// Fixed typed rank values for a local row. Pre-v6 mmap segments expose
    /// zero here until their legacy tag fallback is resolved by the snapshot.
    pub fn rank_values(&self, local_id: u32) -> crate::rank::RankValues {
        match self {
            BaseSegment::Memory(s) => s.rank_values(local_id),
            BaseSegment::Mmap(s) => s.rank_values(local_id),
        }
    }

    pub fn placement(&self, local_id: u32) -> crate::ownership::QueryPlacementRef<'_> {
        match self {
            BaseSegment::Memory(s) => s.placement(local_id),
            BaseSegment::Mmap(s) => s.placement(local_id),
        }
    }
    // Compatibility dispatch wrapper — signature stays byte-for-byte stable.
    #[allow(clippy::too_many_arguments)]
    pub fn match_into(
        &self,
        view: &crate::exact::TitleView,
        dict: &Dict,
        epoch: u32,
        seen: &mut [u32],
        out: &mut Vec<u64>,
        lanes: super::ProbeLanes,
        pred: &crate::exact::TagPredicate,
        stats: &mut MatchStats,
    ) {
        let mut ignored_emissions = 0;
        let mut collector = VecSink::new(out, &mut ignored_emissions);
        self.match_collect(
            view,
            dict,
            epoch,
            seen,
            &mut collector,
            lanes,
            pred,
            stats,
            crate::ownership::EmitAll,
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::segment) fn match_collect<C: MatchSink, P: crate::ownership::EmissionPolicy>(
        &self,
        view: &crate::exact::TitleView,
        dict: &Dict,
        epoch: u32,
        seen: &mut [u32],
        collector: &mut C,
        lanes: super::ProbeLanes,
        pred: &crate::exact::TagPredicate,
        stats: &mut MatchStats,
        emission: P,
    ) {
        match self {
            BaseSegment::Memory(s) => {
                s.match_collect(
                    view, dict, epoch, seen, collector, lanes, pred, stats, emission,
                );
            }
            BaseSegment::Mmap(s) => {
                s.match_collect(
                    view, dict, epoch, seen, collector, lanes, pred, stats, emission,
                );
            }
        }
    }

    /// Whether this segment holds any hot-tier entries (class H, ADR-105).
    pub fn has_hot_entries(&self) -> bool {
        match self {
            BaseSegment::Memory(s) => s.has_hot_entries(),
            BaseSegment::Mmap(s) => s.has_hot_entries(),
        }
    }
    pub fn exact_bytes(&self) -> usize {
        match self {
            BaseSegment::Memory(s) => s.exact_bytes(),
            BaseSegment::Mmap(_) => 0,
        }
    }
    pub fn main_bytes(&self) -> usize {
        match self {
            BaseSegment::Memory(s) => s.main_bytes(),
            BaseSegment::Mmap(_) => 0,
        }
    }
    pub fn broad_bytes(&self) -> usize {
        match self {
            BaseSegment::Memory(s) => s.broad_bytes(),
            BaseSegment::Mmap(_) => 0,
        }
    }
    pub fn hot_bytes(&self) -> usize {
        match self {
            BaseSegment::Memory(s) => s.hot_bytes(),
            BaseSegment::Mmap(_) => 0,
        }
    }
    pub fn filter_bytes(&self) -> usize {
        match self {
            BaseSegment::Memory(s) => s.filter_bytes(),
            BaseSegment::Mmap(_) => 0,
        }
    }

    /// Resident reverse-index bytes. Unlike the file-backed accounting above,
    /// this returns the REAL value for both arms — the reverse index is resident
    /// heap even for mmap segments (rebuilt at open).
    pub fn logical_index_bytes(&self) -> usize {
        match self {
            BaseSegment::Memory(s) => s.logical_index_bytes(),
            BaseSegment::Mmap(s) => s.logical_index_bytes(),
        }
    }

    /// Resident liveness-overlay bytes. Real value for both arms (the alive
    /// overlay is a mutable in-RAM structure even for mmap segments).
    pub fn alive_bytes(&self) -> usize {
        match self {
            BaseSegment::Memory(s) => s.alive_bytes(),
            BaseSegment::Mmap(s) => s.alive_bytes(),
        }
    }

    /// Convert to an owned in-memory Segment (needed by compact_from).
    /// Memory segments are returned directly; mmap segments are materialized.
    pub(in crate::segment) fn into_memory(self, tag_dict: &crate::tagdict::TagDict) -> Segment {
        match self {
            BaseSegment::Memory(s) => s,
            BaseSegment::Mmap(s) => s.to_memory_segment(tag_dict),
        }
    }
}
