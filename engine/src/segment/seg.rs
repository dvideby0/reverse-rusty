//! `impl Segment` — the in-memory (or memtable) index slice: append, probe,
//! tombstone, compaction merge, and the per-segment memory accounting. Type
//! definition lives in the `segment` module root.

use super::{MatchStats, Segment};
use crate::compile::{build_signatures, is_hot, CostClass, Extracted};
use crate::dict::{Dict, FeatureId};
use crate::exact::ExactStore;
use crate::filter::SegmentFilter;
use crate::index::CandidateIndex;
use crate::util::sig_key;

impl Segment {
    pub fn new() -> Self {
        Segment {
            main: CandidateIndex::new(),
            broad: CandidateIndex::new(),
            exact: ExactStore::new(),
            class: Vec::new(),
            alive: Vec::new(),
            alive_counter: 0,
            filter: None,
            vocab_epoch: 0,
            logical_index: crate::util::fast_map(),
        }
    }

    /// Build and attach the anchor filter from the current main + broad index
    /// keys. Called once when a segment is sealed (flush, bulk_ingest, compaction).
    /// After this, `match_into` will use the filter to skip probes.
    pub(in crate::segment) fn build_filter(&mut self) {
        let mut keys = self.main.keys();
        keys.extend(self.broad.keys());
        self.filter = Some(SegmentFilter::build(&keys));
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.exact.len()
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.exact.is_empty()
    }

    pub fn main_index(&self) -> &CandidateIndex {
        &self.main
    }
    pub fn broad_index(&self) -> &CandidateIndex {
        &self.broad
    }

    /// Append one already-extracted query. Returns the new segment-local id, or
    /// `None` if the query is class D (rejected, not stored).
    pub fn add_compiled(
        &mut self,
        ex: &Extracted,
        dict: &Dict,
        logical: u64,
        version: u32,
    ) -> Option<u32> {
        let plan = build_signatures(ex, dict);
        if plan.class == CostClass::D {
            return None;
        }
        let local = self.exact.push(ex, dict, version, logical);
        for &s in &plan.main_sigs {
            self.main.insert(s, local);
        }
        for &s in &plan.broad_sigs {
            self.broad.insert(s, local);
        }
        self.class.push(plan.class);
        self.alive.push(true);
        self.alive_counter += 1;
        self.logical_index.entry(logical).or_default().push(local);
        Some(local)
    }

    pub fn tombstone(&mut self, local_id: u32) {
        if let Some(slot) = self.alive.get_mut(local_id as usize) {
            if *slot {
                self.alive_counter -= 1;
            }
            *slot = false;
        }
    }

    pub fn locals_for_logical(&self, logical_id: u64) -> &[u32] {
        self.logical_index
            .get(&logical_id)
            .map_or(&[], |v| v.as_slice())
    }

    pub fn class_counts(&self, c: &mut [u64; 4]) {
        for &cl in &self.class {
            match cl {
                CostClass::A => c[0] += 1,
                CostClass::B => c[1] += 1,
                CostClass::C => c[2] += 1,
                CostClass::D => c[3] += 1,
            }
        }
    }

    /// Probe this segment for one title and append matched LOGICAL ids to `out`.
    /// `seen` is this segment's epoch-stamp dedup array (size = self.len()).
    ///
    /// If the segment has an anchor filter (sealed base segments), each signature
    /// key is tested against the filter first. Keys that are definitely not
    /// present are skipped without touching the candidate index, cutting read
    /// amplification across multiple segments.
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
        let filter = self.filter.as_ref();

        // arity-1 signatures (one per feature)
        for &f in feats {
            let key = sig_key(&[f]);
            stats.probes_attempted += 1;
            if let Some(flt) = filter {
                if !flt.may_contain(key) {
                    stats.probes_skipped += 1;
                    continue;
                }
            }
            self.probe(
                key, &self.main, epoch, tmask, feats, seen, out, stats, false,
            );
        }
        // arity-2 signatures: {hot feature} x {every other feature}
        for &h in feats {
            if is_hot(dict, h) {
                for &o in feats {
                    if o != h {
                        let (a, b) = if h < o { (h, o) } else { (o, h) };
                        let key = sig_key(&[a, b]);
                        stats.probes_attempted += 1;
                        if let Some(flt) = filter {
                            if !flt.may_contain(key) {
                                stats.probes_skipped += 1;
                                continue;
                            }
                        }
                        self.probe(
                            key, &self.main, epoch, tmask, feats, seen, out, stats, false,
                        );
                    }
                }
            }
        }
        // broad lane (arity-1 anchors), measured separately
        if include_broad {
            for &f in feats {
                let key = sig_key(&[f]);
                stats.probes_attempted += 1;
                if let Some(flt) = filter {
                    if !flt.may_contain(key) {
                        stats.probes_skipped += 1;
                        continue;
                    }
                }
                self.probe(
                    key,
                    &self.broad,
                    epoch,
                    tmask,
                    feats,
                    seen,
                    out,
                    stats,
                    true,
                );
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[inline]
    fn probe(
        &self,
        key: u64,
        index: &CandidateIndex,
        epoch: u32,
        tmask: u64,
        feats: &[FeatureId],
        seen: &mut [u32],
        out: &mut Vec<u64>,
        stats: &mut MatchStats,
        is_broad: bool,
    ) {
        if let Some(posting) = index.get(key) {
            stats.postings_scanned += posting.len() as u32;
            if is_broad {
                stats.broad_postings_scanned += posting.len() as u32;
            }
            posting.for_each(|local| {
                // dedup across signatures with an epoch stamp (O(1), no alloc)
                if seen[local as usize] == epoch {
                    return;
                }
                seen[local as usize] = epoch;
                stats.unique_candidates += 1;
                if is_broad {
                    stats.broad_candidates += 1;
                } else {
                    stats.main_candidates += 1;
                }
                if !self.alive[local as usize] {
                    return; // tombstoned
                }
                if self.exact.verify(local, tmask, feats) {
                    out.push(self.exact.logical(local));
                }
            });
        }
    }

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

    /// Merge multiple source segments into one fresh segment, dropping tombstoned
    /// entries and renumbering local IDs to be dense/contiguous. This is the core
    /// compaction mechanic.
    ///
    /// Correctness argument: every alive entry is copied verbatim (exact store
    /// data, cost class); every signature posting that pointed to an alive entry
    /// is remapped to the new local ID. Dead entries are simply skipped, reclaiming
    /// their space. The resulting segment is equivalent to the union of the alive
    /// entries from all sources.
    pub fn compact_from(sources: &[&Segment]) -> Segment {
        let mut dest = Segment::new();

        for &src in sources {
            // Build the old→new local-id remap for this source segment.
            // Dead entries get u32::MAX (sentinel); alive entries get dense IDs.
            let n = src.len();
            let mut remap: Vec<u32> = vec![u32::MAX; n];
            for (old, &is_alive) in src.alive.iter().enumerate() {
                if is_alive {
                    let new_id = src.exact.copy_entry(old as u32, &mut dest.exact);
                    let logical = dest.exact.logical(new_id);
                    dest.class.push(src.class[old]);
                    dest.alive.push(true);
                    dest.alive_counter += 1;
                    dest.logical_index.entry(logical).or_default().push(new_id);
                    remap[old] = new_id;
                }
            }

            // Remap main index postings
            src.main.for_each_posting(|key, posting| {
                posting.for_each(|old_id| {
                    let new_id = remap[old_id as usize];
                    if new_id != u32::MAX {
                        dest.main.insert(key, new_id);
                    }
                });
            });

            // Remap broad index postings
            src.broad.for_each_posting(|key, posting| {
                posting.for_each(|old_id| {
                    let new_id = remap[old_id as usize];
                    if new_id != u32::MAX {
                        dest.broad.insert(key, new_id);
                    }
                });
            });
        }
        // Build anchor filter for the newly compacted (sealed) segment.
        dest.build_filter();
        // Merged segment inherits the minimum epoch — still stale if any source was.
        dest.vocab_epoch = sources.iter().map(|s| s.vocab_epoch).min().unwrap_or(0);
        dest
    }

    /// Reconstruct a Segment from pre-built parts. Used by MmapSegment::to_memory_segment
    /// to convert mmap'd data back into an in-memory segment (for compaction).
    pub fn from_parts(
        main: CandidateIndex,
        broad: CandidateIndex,
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
        let mut logical_index: crate::util::FastMap<u64, Vec<u32>> = crate::util::fast_map();
        for (i, &is_alive) in alive.iter().enumerate() {
            if is_alive {
                logical_index
                    .entry(exact.logical(i as u32))
                    .or_default()
                    .push(i as u32);
            }
        }
        let mut seg = Segment {
            main,
            broad,
            exact,
            class,
            alive,
            alive_counter,
            filter: None,
            vocab_epoch: 0,
            logical_index,
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
