use super::{CandidateIndex, CostClass, ExactStore, Segment, SegmentFilter};

impl Segment {
    /// Number of alive (non-tombstoned) entries in this segment (O(1)).
    pub fn alive_count(&self) -> usize {
        self.alive_counter
    }

    /// Fraction of entries that are tombstoned (holes_ratio for merge scoring).
    pub fn holes_ratio(&self) -> f64 {
        let total = self.len();
        if total == 0 {
            return 0.0;
        }
        1.0 - (self.alive_count() as f64 / total as f64)
    }

    /// Reconstruct a Segment from pre-built parts. Used by MmapSegment::to_memory_segment
    /// to convert mmap'd data back into an in-memory segment (for compaction).
    pub fn from_parts(
        main: CandidateIndex,
        broad: CandidateIndex,
        hot: CandidateIndex,
        exact: ExactStore,
        class: Vec<CostClass>,
        alive: Vec<bool>,
    ) -> Self {
        // Precondition: `class`, `alive`, and `exact` are parallel columns indexed
        // by the same segment-local id (here, in `compact_from`, and in `class_counts`).
        // A length mismatch would silently drop entries from the reverse index below,
        // leaving alive queries that can never be deleted — fail loudly instead.
        assert_eq!(
            alive.len(),
            exact.len(),
            "from_parts: alive/exact length mismatch"
        );
        assert_eq!(
            class.len(),
            exact.len(),
            "from_parts: class/exact length mismatch"
        );
        let alive_counter = alive.iter().filter(|&&a| a).count();
        let live_phrase_predicates = if exact.has_phrase_predicates() {
            alive
                .iter()
                .enumerate()
                .filter(|&(i, &is_alive)| is_alive && exact.row_has_phrase_predicates(i as u32))
                .count()
        } else {
            0
        };
        let mut logical_index: crate::util::FastMap<u64, Vec<u32>> = crate::util::fast_map();
        for (i, &is_alive) in alive.iter().enumerate() {
            if is_alive {
                logical_index
                    .entry(exact.logical(i as u32))
                    .or_default()
                    .push(i as u32);
            }
        }
        let identity: Vec<u32> = (0..alive.len() as u32).collect();
        let mut seg = Segment {
            main,
            broad,
            hot,
            exact,
            class,
            alive,
            alive_counter,
            live_phrase_predicates,
            filter: None,
            vocab_epoch: 0,
            compiler_semantics_version: crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION,
            logical_index,
            // Rebuilt-from-parts segments carry EXPANDED postings (the on-disk
            // form) — identity groups, no sharing (dedup is re-derived only by
            // the group-aware merges).
            dup_of: identity,
            dup_members: crate::util::fast_map(),
            body_index: crate::util::fast_map(),
        };
        seg.build_filter();
        seg
    }

    // ---- accessors for serialization (storage.rs) ----
    pub fn exact_store(&self) -> &ExactStore {
        &self.exact
    }

    /// Sorted `(logical_id, local_id)` columns for the `.seg` v2 reverse-index
    /// section (ADR-020 Item 2). Sorted by `(logical_id, local_id)` so each
    /// logical id's local ids form a contiguous, binary-searchable run on read.
    /// Mirrors exactly what `logical_index` holds, so a reader reproduces
    /// `locals_for_logical` identically.
    pub fn logical_columns(&self) -> (Vec<u64>, Vec<u32>) {
        let mut pairs: Vec<(u64, u32)> = Vec::with_capacity(self.exact.len());
        for (&logical, locals) in &self.logical_index {
            for &local in locals {
                pairs.push((logical, local));
            }
        }
        pairs.sort_unstable();
        let logical = pairs.iter().map(|&(l, _)| l).collect();
        let local = pairs.iter().map(|&(_, c)| c).collect();
        (logical, local)
    }
    pub fn classes(&self) -> &[CostClass] {
        &self.class
    }
    pub fn alive_flags(&self) -> &[bool] {
        &self.alive
    }
    pub fn filter_ref(&self) -> Option<&SegmentFilter> {
        self.filter.as_ref()
    }

    // ---- memory accounting for the perf report ----
    pub fn exact_bytes(&self) -> usize {
        self.exact.heap_bytes()
    }
    pub fn main_bytes(&self) -> usize {
        self.main.heap_bytes()
    }
    pub fn broad_bytes(&self) -> usize {
        self.broad.heap_bytes()
    }
    pub fn hot_bytes(&self) -> usize {
        self.hot.heap_bytes()
    }
    pub fn filter_bytes(&self) -> usize {
        self.filter
            .as_ref()
            .map_or(0, crate::filter::SegmentFilter::heap_bytes)
    }

    /// Resident heap bytes used by the logical→local reverse index. This is
    /// resident even when the segment's SoA/index are mmap'd, and is uncounted by
    /// the file-backed accounting above — a `Vec` per logical id is a real cost.
    pub fn logical_index_bytes(&self) -> usize {
        use std::mem::size_of;
        let buckets = self.logical_index.capacity() * size_of::<(u64, Vec<u32>)>();
        let vecs: usize = self
            .logical_index
            .values()
            .map(|v| v.capacity() * size_of::<u32>())
            .sum();
        buckets + vecs
    }

    /// Resident heap bytes used by the liveness array. Resident even for mmap'd
    /// segments (it is the mutable tombstone overlay).
    pub fn alive_bytes(&self) -> usize {
        self.alive.capacity() * std::mem::size_of::<bool>()
    }
}
