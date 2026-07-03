//! `impl Engine` — introspection: the [`EngineMetrics`](crate::events::EngineMetrics)
//! snapshot, per-component byte accounting, and the count/index accessors used by
//! the server's `/_stats` and bench harnesses. Also holds the
//! [`EngineSnapshot`](super::EngineSnapshot) posting-length percentile collector
//! (the Broad-Query Cost Program's observe-first telemetry).

use std::sync::Arc;

use super::{BaseSegment, Engine, EngineSnapshot, Segment};
use crate::events::{LanePostingStats, PostingStats, SegmentInfo, SegmentKind};
use crate::index::CandidateIndex;
use crate::wal::Wal;

/// Nearest-rank percentiles over one lane's collected posting lengths.
/// Sorts in place; all-zero stats for an empty lane.
fn posting_stats(lens: &mut [u32]) -> PostingStats {
    if lens.is_empty() {
        return PostingStats::default();
    }
    lens.sort_unstable();
    let n = lens.len();
    // Nearest-rank: the smallest value with at least ⌈p·n⌉ values ≤ it.
    let rank = |p: usize| lens[((p * n).div_ceil(100)).clamp(1, n) - 1];
    PostingStats {
        count: n,
        p50: rank(50),
        p95: rank(95),
        p99: rank(99),
        max: lens[n - 1],
    }
}

impl EngineSnapshot {
    /// Posting-length percentiles per candidate-index lane, across every base
    /// segment + the memtable (the Broad-Query Cost Program's observe-first
    /// telemetry — a fat main-lane `max` against a modest `p99` is the top-64
    /// rank-cliff fingerprint ADR-104 measured). Computed on demand from the
    /// lock-free snapshot — an O(total postings) walk + sort, never on the
    /// match path. Backs the server's `GET /_stats` `postings` block.
    pub fn lane_posting_stats(&self) -> LanePostingStats {
        let mut main: Vec<u32> = Vec::new();
        let mut broad: Vec<u32> = Vec::new();
        for seg in &self.segments {
            match seg.as_ref() {
                BaseSegment::Memory(s) => {
                    s.main_index().collect_posting_lens(&mut main);
                    s.broad_index().collect_posting_lens(&mut broad);
                }
                BaseSegment::Mmap(m) => {
                    m.collect_posting_lens(false, &mut main);
                    m.collect_posting_lens(true, &mut broad);
                }
            }
        }
        self.memtable.main_index().collect_posting_lens(&mut main);
        self.memtable.broad_index().collect_posting_lens(&mut broad);
        LanePostingStats {
            main: posting_stats(&mut main),
            broad: posting_stats(&mut broad),
        }
    }
}

/// Build the per-segment introspection rows shared by [`Engine::segment_infos`]
/// and [`EngineSnapshot::segment_infos`](crate::segment::EngineSnapshot::segment_infos).
/// Base segments come first (ordinal `0..n`, oldest first); the mutable memtable
/// is appended as the final row at ordinal `n`. `current_epoch` is the engine's
/// live vocab epoch, used to flag segments compiled against an older normalizer.
pub(in crate::segment) fn collect_segment_infos(
    segments: &[Arc<BaseSegment>],
    memtable: &Segment,
    current_epoch: u64,
) -> Vec<SegmentInfo> {
    let mut infos = Vec::with_capacity(segments.len() + 1);
    for (ordinal, seg) in segments.iter().enumerate() {
        let entries = seg.len();
        let alive = seg.alive_count();
        let epoch = seg.vocab_epoch();
        infos.push(SegmentInfo {
            ordinal,
            kind: seg.storage_kind(),
            entries,
            alive,
            deleted: entries - alive,
            holes_ratio: seg.holes_ratio(),
            vocab_epoch: epoch,
            stale: epoch < current_epoch,
            resident_bytes: seg.exact_bytes()
                + seg.main_bytes()
                + seg.broad_bytes()
                + seg.filter_bytes(),
            overhead_bytes: seg.logical_index_bytes() + seg.alive_bytes(),
        });
    }
    // The memtable is the live tail — always reported, even when empty, so an
    // operator can see the hot delta. An empty memtable is never flagged stale.
    let entries = memtable.len();
    let alive = memtable.alive_count();
    let epoch = memtable.vocab_epoch;
    infos.push(SegmentInfo {
        ordinal: segments.len(),
        kind: SegmentKind::Memtable,
        entries,
        alive,
        deleted: entries - alive,
        holes_ratio: memtable.holes_ratio(),
        vocab_epoch: epoch,
        stale: epoch < current_epoch && !memtable.is_empty(),
        resident_bytes: memtable.exact_bytes()
            + memtable.main_bytes()
            + memtable.broad_bytes()
            + memtable.filter_bytes(),
        overhead_bytes: memtable.logical_index_bytes() + memtable.alive_bytes(),
    });
    infos
}

impl Engine {
    pub fn num_queries(&self) -> usize {
        self.segments.iter().map(|s| s.len()).sum::<usize>() + self.memtable.len()
    }

    /// Live (non-tombstoned) entries across the memtable + every base segment — one per live
    /// LOGICAL id (an upsert kills the superseded copy; a delete kills the last), unlike
    /// [`Self::num_queries`], which counts physical entries including dead copies. The
    /// index-side live count the content fingerprint's completeness cross-check compares
    /// against (ADR-097): it equals the live source enumeration's length exactly when the
    /// source store covers every query the index still serves.
    pub fn num_live_queries(&self) -> usize {
        self.segments.iter().map(|s| s.alive_count()).sum::<usize>() + self.memtable.alive_count()
    }
    pub fn num_segments(&self) -> usize {
        // base segments + the memtable as one logical segment
        self.segments.len() + 1
    }
    /// Total queries ever rejected (parse failures + class-D), across all
    /// ingest paths. Kept for back-compat; prefer the split accessors below.
    pub fn rejected(&self) -> u64 {
        self.rejected_parse + self.rejected_class_d
    }
    /// Queries dropped because their DSL string failed to parse.
    pub fn rejected_parse(&self) -> u64 {
        self.rejected_parse
    }
    /// Queries dropped as cost-class D (no anchorable required/any-of feature).
    pub fn rejected_class_d(&self) -> u64 {
        self.rejected_class_d
    }
    /// Observe-first hot-tier telemetry — accepted compiles whose plan
    /// [`would_be_hot`](crate::compile::SigPlan::would_be_hot) (the Broad-Query
    /// Cost Program's reclassification counter).
    pub fn would_be_hot(&self) -> u64 {
        self.would_be_hot
    }
    /// First base segment's main index (kept for bench/back-compat callers).
    /// Falls back to the memtable if no base segments exist.
    pub fn main_index(&self) -> &CandidateIndex {
        match self.segments.first().map(std::convert::AsRef::as_ref) {
            Some(BaseSegment::Memory(s)) => s.main_index(),
            _ => self.memtable.main_index(),
        }
    }
    pub fn broad_index(&self) -> &CandidateIndex {
        match self.segments.first().map(std::convert::AsRef::as_ref) {
            Some(BaseSegment::Memory(s)) => s.broad_index(),
            _ => self.memtable.broad_index(),
        }
    }
    pub fn class_counts(&self) -> [u64; 4] {
        let mut c = [0u64; 4];
        for seg in &self.segments {
            match seg.as_ref() {
                BaseSegment::Memory(s) => s.class_counts(&mut c),
                // O(n) byte scan over the mmap'd class column — fine off the hot
                // path, and required for an honest count: after a flush/reopen the
                // stored entries live HERE (mirrors the snapshot path; previously
                // skipped, which undercounted every sealed segment).
                BaseSegment::Mmap(m) => m.class_counts(&mut c),
            }
        }
        self.memtable.class_counts(&mut c);
        // c[3] counts STORED class-D always-candidates (ADR-068), symmetric with
        // A/B/C — zero unless the accept_class_d lane has stored entries.
        // Rejections are a separate metric (`rejected_class_d()`), no longer
        // mirrored into this array.
        c
    }

    /// Per-segment introspection rows for the whole LSM layout (base segments
    /// oldest-first, then the memtable). Powers the server's `GET /_cat/segments`.
    /// See [`SegmentInfo`](crate::events::SegmentInfo).
    pub fn segment_infos(&self) -> Vec<SegmentInfo> {
        collect_segment_infos(&self.segments, &self.memtable, self.vocab_epoch)
    }

    /// Snapshot of current engine metrics for monitoring and introspection.
    pub fn metrics(&self) -> crate::events::EngineMetrics {
        let segment_sizes: Vec<usize> = self.segments.iter().map(|s| s.len()).collect();
        let segment_holes: Vec<f64> = self.segments.iter().map(|s| s.holes_ratio()).collect();
        crate::events::EngineMetrics {
            total_queries: self.num_queries(),
            base_segments: self.segments.len(),
            memtable_entries: self.memtable.len(),
            segment_sizes,
            segment_holes,
            rejected_parse: self.rejected_parse,
            rejected_class_d: self.rejected_class_d,
            would_be_hot: self.would_be_hot,
            dict_features: self.dict.len(),
            exact_bytes: self.exact_bytes(),
            index_bytes: self.main_bytes() + self.broad_bytes(),
            filter_bytes: self.filter_bytes(),
            stale_segments: self.stale_segment_count(),
            dict_bytes: self.dict.heap_bytes(),
            query_store_bytes: self.query_store.resident_bytes(),
            logical_index_bytes: self
                .segments
                .iter()
                .map(|s| s.logical_index_bytes())
                .sum::<usize>()
                + self.memtable.logical_index_bytes(),
            alive_bytes: self.segments.iter().map(|s| s.alive_bytes()).sum::<usize>()
                + self.memtable.alive_bytes(),
            wal_size_bytes: self.wal.as_ref().map_or(0, Wal::size_bytes),
            wal_pending_entries: self.wal.as_ref().map_or(0, Wal::pending_entries),
        }
    }

    // ---- memory accounting for the perf report ----
    pub fn exact_bytes(&self) -> usize {
        self.segments.iter().map(|s| s.exact_bytes()).sum::<usize>() + self.memtable.exact_bytes()
    }
    pub fn main_bytes(&self) -> usize {
        self.segments.iter().map(|s| s.main_bytes()).sum::<usize>() + self.memtable.main_bytes()
    }
    pub fn broad_bytes(&self) -> usize {
        self.segments.iter().map(|s| s.broad_bytes()).sum::<usize>() + self.memtable.broad_bytes()
    }
    pub fn filter_bytes(&self) -> usize {
        self.segments
            .iter()
            .map(|s| s.filter_bytes())
            .sum::<usize>()
    }
    pub fn dict_len(&self) -> usize {
        self.dict.len()
    }
}

#[cfg(test)]
mod posting_stats_tests {
    use super::posting_stats;
    use crate::events::PostingStats;

    #[test]
    fn nearest_rank_percentiles() {
        // Empty lane -> all zeros (never a panic).
        assert_eq!(posting_stats(&mut []), PostingStats::default());
        // Single posting: every percentile IS that posting.
        let s = posting_stats(&mut [7]);
        assert_eq!((s.count, s.p50, s.p95, s.p99, s.max), (1, 7, 7, 7, 7));
        // 1..=100 (unsorted input): nearest-rank p50/p95/p99 are exactly 50/95/99.
        let mut lens: Vec<u32> = (1..=100).rev().collect();
        let s = posting_stats(&mut lens);
        assert_eq!(
            (s.count, s.p50, s.p95, s.p99, s.max),
            (100, 50, 95, 99, 100)
        );
        // Two postings: p50 = the 1st (⌈0.5·2⌉ = 1), p95/p99 = the 2nd.
        let s = posting_stats(&mut [10, 2]);
        assert_eq!((s.p50, s.p95, s.p99, s.max), (2, 10, 10, 10));
        // The rank-cliff fingerprint shape: one fat posting dominates max but not p99.
        let mut lens: Vec<u32> = vec![4; 999];
        lens.push(43_533);
        let s = posting_stats(&mut lens);
        assert_eq!((s.p99, s.max), (4, 43_533));
    }
}
