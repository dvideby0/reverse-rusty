//! `impl BaseSegment` — dispatch wrapper over a sealed segment that is either
//! in-memory (`Memory`) or file-backed (`Mmap`). Type definition lives in the
//! `segment` module root.

use super::{BaseSegment, MatchStats, Segment};
use crate::dict::{Dict, FeatureId};

impl BaseSegment {
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
    // Dispatch wrapper — signature must mirror the inner segment's match_into.
    #[allow(clippy::too_many_arguments)]
    pub fn match_into(
        &self,
        feats: &[FeatureId],
        tmask: u64,
        dict: &Dict,
        epoch: u32,
        seen: &mut [u32],
        out: &mut Vec<u64>,
        include_broad: bool,
        stats: &mut MatchStats,
    ) {
        match self {
            BaseSegment::Memory(s) => {
                s.match_into(feats, tmask, dict, epoch, seen, out, include_broad, stats);
            }
            BaseSegment::Mmap(s) => {
                s.match_into(feats, tmask, dict, epoch, seen, out, include_broad, stats);
            }
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
    pub(in crate::segment) fn into_memory(self) -> Segment {
        match self {
            BaseSegment::Memory(s) => s,
            BaseSegment::Mmap(s) => s.to_memory_segment(),
        }
    }
}
