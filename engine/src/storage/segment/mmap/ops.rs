//! The [`MmapSegment`] read/match surface: the zero-cost slice accessors over the
//! mmap, the public introspection interface, the hot-path matchers (`match_into` /
//! `verify` / the broad-batch surface), and `to_memory_segment`.
//!
//! This is a descendant of [`super`] (the module that defines `MmapSegment`), so it
//! reads the struct's private fields and the private `MmapLogicalIndex` directly — no
//! visibility widening. The accessors and matchers live together so their mutual
//! private calls stay in-module.

use std::path::Path;

use super::super::read::frozen_probe;
use super::super::FrozenSlot;

/// Which candidate index a probe reads — routes the per-lane stats counters.
/// Local twin of the segment-side lane enums (those are `pub(in crate::segment)`
/// and this module lives under `crate::storage`).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Lane {
    Main,
    Broad,
    Hot,
}
use super::{MmapLogicalIndex, MmapSegment};
use crate::collect::{MatchSink, VecSink};
use crate::compile::CostClass;
use crate::dict::FeatureId;
use crate::index::CandidateIndex;
use crate::segment::{infallible, DeadlineCheck, DeadlinePoll, MatchStats, NoDeadline, Segment};

impl MmapSegment {
    /// Fixed typed rank values. Segment v6 replaces this compatibility default
    /// with the mmap-backed column; older formats legitimately expose zero.
    pub fn rank_values(&self, local: u32) -> crate::rank::RankValues {
        let priority = self
            .mmap_slice(self.priority_arr, self.priority_count)
            .get(local as usize)
            .copied()
            .unwrap_or(0);
        crate::rank::RankValues { priority }
    }

    /// Query-side ranking evidence derived from existing verified columns.
    pub(crate) fn rank_query_features(&self, local: u32) -> crate::rank::RankQueryFeatures {
        let i = local as usize;
        let proxy_anyof_groups = u32::from(self.q_group_count().get(i).copied().unwrap_or(0));
        let predicate_features = self
            .predicate_off()
            .get(i)
            .zip(self.predicate_len().get(i))
            .map_or(
                (proxy_anyof_groups, 0, proxy_anyof_groups),
                |(&off, &len)| {
                    let start = off as usize;
                    let end = start + len as usize;
                    crate::exact::predicate_rank_term_counts(
                        &self.predicate_blob()[start..end],
                        proxy_anyof_groups,
                    )
                },
            );
        crate::rank::RankQueryFeatures {
            positive_terms: self
                .req_mask()
                .get(i)
                .copied()
                .unwrap_or(0)
                .count_ones()
                .saturating_add(u32::from(self.req_len().get(i).copied().unwrap_or(0)))
                .saturating_add(predicate_features.0),
            negative_terms: self
                .forb_mask()
                .get(i)
                .copied()
                .unwrap_or(0)
                .count_ones()
                .saturating_add(u32::from(self.forb_len().get(i).copied().unwrap_or(0)))
                .saturating_add(predicate_features.1),
            any_of_groups: predicate_features.2,
            tag_count: u32::from(self.tag_len().get(i).copied().unwrap_or(0)),
        }
    }

    /// Allocation-free ADR-109 placement view. Pre-v7 standalone segments expose
    /// the reserved standalone identity without touching absent columns.
    #[inline]
    pub fn placement(&self, local: u32) -> crate::ownership::QueryPlacementRef<'_> {
        if self.placement_count == 0 {
            return crate::ownership::QueryPlacementRef {
                generation: crate::ownership::PlacementGeneration::STANDALONE,
                num_shards: 0,
                mode: crate::ownership::PlacementMode::Standalone,
                positions: &[],
            };
        }
        let i = local as usize;
        let generations = self.mmap_slice(self.placement_generation, self.placement_count);
        let shard_counts = self.mmap_slice(self.placement_num_shards, self.placement_count);
        let modes = self.mmap_slice(self.placement_mode, self.placement_count);
        let offs = self.mmap_slice(self.placement_off, self.placement_count);
        let lens = self.mmap_slice(self.placement_len, self.placement_count);
        let blob = self.mmap_slice(self.placement_blob, self.placement_blob_len);
        let off = offs[i] as usize;
        let len = lens[i] as usize;
        crate::ownership::QueryPlacementRef {
            generation: crate::ownership::PlacementGeneration(generations[i]),
            num_shards: shard_counts[i],
            mode: match modes[i] {
                1 => crate::ownership::PlacementMode::Selective,
                2 => crate::ownership::PlacementMode::ReplicatedAlwaysVisible,
                3 => crate::ownership::PlacementMode::ReplicatedBroad,
                _ => crate::ownership::PlacementMode::Standalone,
            },
            positions: &blob[off..off + len],
        }
    }

    // ---- slice accessors (zero-cost, just pointer arithmetic) ----

    /// View `len` elements of `T` at `ptr` as a slice borrowed from `&self`.
    ///
    /// Every section accessor below funnels through this one helper so the
    /// pointer-to-slice reconstruction has a single audited `unsafe` site.
    ///
    /// # The invariant that makes every caller sound
    ///
    /// All `(ptr, len)` pairs are the ones captured in [`MmapSegment::open`] from
    /// the mmap that `self` owns. At that point:
    /// * the mapping was fully validated — trailing CRC32 over the file body, plus
    ///   magic bytes and format version — before any pointer was taken, so the
    ///   bytes are exactly what the writer produced and `len` matches the section;
    /// * the writer pads every section to an 8-byte boundary, and the element
    ///   types used here (`u64`/`u32`/`u16`/`FrozenSlot`) all have alignment
    ///   dividing 8, so `ptr` is properly aligned;
    /// * `self` owns the backing `Arc<Mmap>`, which is immutable and never moves,
    ///   and it outlives the returned borrow, so the slice can neither dangle nor
    ///   be mutated underneath the reader.
    ///
    /// Callers must therefore only pass pointer/length pairs originating from
    /// `open`'s validated parse (never a null pointer — see `filter_data`).
    // `&self` is load-bearing: it ties the returned slice's lifetime to the mmap
    // owner so the borrow checker forbids use-after-unmap. clippy can't see that
    // the body's safety contract depends on the borrow.
    #[allow(clippy::unused_self)]
    #[inline]
    fn mmap_slice<T>(&self, ptr: *const T, len: usize) -> &[T] {
        // SAFETY: upheld by the construction invariant documented above — `ptr`
        // references `len` correctly-aligned, initialized `T`s inside the live,
        // immutable mmap owned by `self`.
        unsafe { std::slice::from_raw_parts(ptr, len) }
    }

    #[inline]
    fn req_mask(&self) -> &[u64] {
        self.mmap_slice(self.req_mask, self.num_queries as usize)
    }
    #[inline]
    fn forb_mask(&self) -> &[u64] {
        self.mmap_slice(self.forb_mask, self.num_queries as usize)
    }
    #[inline]
    fn req_off(&self) -> &[u32] {
        self.mmap_slice(self.req_off, self.num_queries as usize)
    }
    #[inline]
    fn req_len(&self) -> &[u16] {
        self.mmap_slice(self.req_len, self.num_queries as usize)
    }
    #[inline]
    fn req_blob(&self) -> &[u32] {
        self.mmap_slice(self.req_blob, self.req_blob_len)
    }
    #[inline]
    fn forb_off(&self) -> &[u32] {
        self.mmap_slice(self.forb_off, self.num_queries as usize)
    }
    #[inline]
    fn forb_len(&self) -> &[u16] {
        self.mmap_slice(self.forb_len, self.num_queries as usize)
    }
    #[inline]
    fn forb_blob(&self) -> &[u32] {
        self.mmap_slice(self.forb_blob, self.forb_blob_len)
    }
    #[inline]
    fn q_group_start(&self) -> &[u32] {
        self.mmap_slice(self.q_group_start, self.num_queries as usize)
    }
    #[inline]
    fn q_group_count(&self) -> &[u16] {
        self.mmap_slice(self.q_group_count, self.num_queries as usize)
    }
    #[inline]
    fn group_off(&self) -> &[u32] {
        self.mmap_slice(self.group_off, self.group_off_len)
    }
    #[inline]
    fn group_len(&self) -> &[u16] {
        self.mmap_slice(self.group_len, self.group_off_len)
    }
    #[inline]
    fn anyof_blob(&self) -> &[u32] {
        self.mmap_slice(self.anyof_blob, self.anyof_blob_len)
    }
    #[inline]
    fn predicate_off(&self) -> &[u32] {
        self.mmap_slice(self.predicate_off, self.predicate_count)
    }
    #[inline]
    fn predicate_len(&self) -> &[u32] {
        self.mmap_slice(self.predicate_len, self.predicate_count)
    }
    #[inline]
    fn predicate_blob(&self) -> &[u32] {
        self.mmap_slice(self.predicate_blob, self.predicate_blob_len)
    }
    #[inline]
    fn tag_off(&self) -> &[u32] {
        self.mmap_slice(self.tag_off, self.tag_count)
    }
    #[inline]
    fn tag_len(&self) -> &[u16] {
        self.mmap_slice(self.tag_len, self.tag_count)
    }
    #[inline]
    fn tag_blob(&self) -> &[crate::tagdict::TagId] {
        self.mmap_slice(self.tag_blob, self.tag_blob_len)
    }

    #[inline]
    fn main_slots(&self) -> &[FrozenSlot] {
        self.mmap_slice(self.main_slots, self.main_cap)
    }
    #[inline]
    fn main_blob(&self) -> &[u32] {
        self.mmap_slice(self.main_blob, self.main_blob_len)
    }
    #[inline]
    fn broad_slots(&self) -> &[FrozenSlot] {
        self.mmap_slice(self.broad_slots, self.broad_cap)
    }
    #[inline]
    fn broad_blob(&self) -> &[u32] {
        self.mmap_slice(self.broad_blob, self.broad_blob_len)
    }
    #[inline]
    fn hot_slots(&self) -> &[FrozenSlot] {
        self.mmap_slice(self.hot_slots, self.hot_cap)
    }
    #[inline]
    fn hot_blob(&self) -> &[u32] {
        self.mmap_slice(self.hot_blob, self.hot_blob_len)
    }

    /// Whether this segment holds any hot-tier entries (class H, ADR-105) — the
    /// per-segment skip keeping the hot lane free on hot-empty corpora.
    #[inline]
    pub fn has_hot_entries(&self) -> bool {
        self.hot_cap > 0
    }

    #[inline]
    pub fn has_phrase_predicates(&self) -> bool {
        self.live_phrase_predicates != 0
    }

    #[inline]
    fn row_has_phrase_predicates(&self, local_id: u32) -> bool {
        let i = local_id as usize;
        let Some((&off, &len)) = self.predicate_off().get(i).zip(self.predicate_len().get(i))
        else {
            return false;
        };
        let start = off as usize;
        let end = start + len as usize;
        crate::exact::predicate_has_phrases(&self.predicate_blob()[start..end])
    }

    #[inline]
    fn filter_data(&self) -> &[u64] {
        // Guard the null sentinel: a segment with no filter stores a null
        // `filter_data` pointer, which `mmap_slice`/`from_raw_parts` forbid.
        if self.filter_num_blocks == 0 {
            return &[];
        }
        self.mmap_slice(self.filter_data, self.filter_num_blocks * 8)
    }

    /// Append every occupied slot's posting length from one lane's frozen table —
    /// the mmap twin of
    /// [`CandidateIndex::collect_posting_lens`](crate::index::CandidateIndex::collect_posting_lens)
    /// (`/_stats` per-lane percentiles; off the hot path).
    pub fn collect_posting_lens(&self, broad: bool, into: &mut Vec<u32>) {
        let slots = if broad {
            self.broad_slots()
        } else {
            self.main_slots()
        };
        into.extend(slots.iter().filter(|s| s.key != 0).map(|s| s.len));
    }

    /// Hot-tier variant of [`collect_posting_lens`](Self::collect_posting_lens)
    /// (class H, ADR-105) — empty pre-v5 / on hot-free files.
    pub fn collect_hot_posting_lens(&self, into: &mut Vec<u32>) {
        into.extend(
            self.hot_slots()
                .iter()
                .filter(|s| s.key != 0)
                .map(|s| s.len),
        );
    }

    // ---- public interface ----

    pub fn len(&self) -> usize {
        self.num_queries as usize
    }

    pub fn is_empty(&self) -> bool {
        self.num_queries == 0
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn tombstone(&mut self, local_id: u32) {
        let had_phrase_predicate = self.row_has_phrase_predicates(local_id);
        if let Some(slot) = self.alive_overlay.get_mut(local_id as usize) {
            if *slot {
                self.alive_counter -= 1;
                if had_phrase_predicate {
                    self.live_phrase_predicates -= 1;
                }
                // Keep the incremental dead set ≡ the overlay (ADR-066) — the
                // already-dead branch is covered by the seed at open.
                self.dead_overlay.insert(local_id);
            }
            *slot = false;
        }
    }

    /// The DEAD locals as a roaring bitmap, maintained incrementally (≡ the dead
    /// entries of `alive_overlay`). The manifest commit serializes this in
    /// O(deletes) — never a full-segment rescan (ADR-066).
    pub fn dead_overlay(&self) -> &roaring::RoaringBitmap {
        &self.dead_overlay
    }

    /// The sorted `logical_id` column (borrowed from the mmap for v2, owned for v1).
    #[inline]
    fn li_logical(&self) -> &[u64] {
        match &self.logical_index {
            MmapLogicalIndex::Mapped { logical, count, .. } => self.mmap_slice(*logical, *count),
            MmapLogicalIndex::Owned { logical, .. } => logical,
        }
    }
    /// The parallel `local_id` column.
    #[inline]
    fn li_local(&self) -> &[u32] {
        match &self.logical_index {
            MmapLogicalIndex::Mapped { local, count, .. } => self.mmap_slice(*local, *count),
            MmapLogicalIndex::Owned { local, .. } => local,
        }
    }

    pub fn locals_for_logical(&self, logical_id: u64) -> &[u32] {
        // Columns are sorted by (logical_id, local_id), so a logical id's local
        // ids form a contiguous run — binary-search its bounds and slice.
        let logs = self.li_logical();
        let lo = logs.partition_point(|&l| l < logical_id);
        let hi = logs.partition_point(|&l| l <= logical_id);
        &self.li_local()[lo..hi]
    }

    /// Number of alive (non-tombstoned) entries (O(1)).
    pub fn alive_count(&self) -> usize {
        self.alive_counter
    }

    /// Tally entries by cost class into `c` (`[A, B, C, D]`), reading the persisted
    /// per-entry class bytes. Counts ALL entries (including tombstoned), matching
    /// [`Segment::class_counts`](crate::segment::Segment::class_counts) so introspection
    /// is identical whether a segment is in-memory or mmap'd (the latter is what a
    /// reopened durable cluster attaches — ADR-032).
    pub fn class_counts(&self, c: &mut [u64; 5]) {
        let n = self.len();
        for i in 0..n {
            // SAFETY: `i < n == num_queries`, the length of the `class_arr` byte array
            // parsed from the mmap (same bound `to_memory_segment` uses).
            let class_byte = unsafe { *self.class_arr.add(i) };
            // Bytes 0..=4 are the only values `open`'s class-byte validation admits
            // (≤3 pre-v5, ≤4 on v5), so this direct index cannot mis-bucket — the
            // old `.min(3)` clamp would have silently counted class H as D.
            c[class_byte as usize] += 1;
        }
    }

    pub fn holes_ratio(&self) -> f64 {
        let total = self.len();
        if total == 0 {
            return 0.0;
        }
        1.0 - (self.alive_count() as f64 / total as f64)
    }

    /// Resident heap bytes used by the logical→local reverse index. The SoA and
    /// candidate index are mmap'd (file-backed, paged), but this reverse index is
    /// rebuilt resident at `open` — a `Vec` per logical id — so it is a real
    /// resident cost the file-backed accounting misses.
    pub fn logical_index_bytes(&self) -> usize {
        match &self.logical_index {
            // v2 columns live in the mmap (file-backed/paged) — ~zero resident heap.
            MmapLogicalIndex::Mapped { .. } => 0,
            // v1 reconstruct holds flat owned columns (12 B/query, vs the old
            // per-logical Vec map) until the segment is recompacted to v2.
            MmapLogicalIndex::Owned { logical, local } => {
                logical.capacity() * std::mem::size_of::<u64>()
                    + local.capacity() * std::mem::size_of::<u32>()
            }
        }
    }

    /// Resident heap bytes used by the mutable alive overlay (tombstones). This
    /// stays in RAM even for an mmap'd segment because the mapping is read-only.
    pub fn alive_bytes(&self) -> usize {
        self.alive_overlay.capacity() * std::mem::size_of::<bool>()
    }

    #[inline]
    pub(crate) fn logical(&self, id: u32) -> u64 {
        // SAFETY: `logical_arr` is the `num_queries`-long u64 array parsed from the
        // mmap in `open`. Callers only pass local ids `< num_queries` (they come
        // from posting lists built over this segment's own entries), so the offset
        // is in bounds of the immutable mapping `self` owns.
        unsafe { *self.logical_arr.add(id as usize) }
    }

    /// The stored per-query version for a local id — read back for the cluster
    /// rebuild gather (ADR-074), so a `set_vocab`/resize re-places a query at the
    /// version it was durably stored with rather than resetting it to 1.
    #[inline]
    pub(crate) fn version(&self, id: u32) -> u32 {
        // SAFETY: same in-bounds argument as `logical` — `version_arr` is the
        // `num_queries`-long u32 array parsed in `open`, and `id < num_queries`.
        unsafe { *self.version_arr.add(id as usize) }
    }

    /// Internal source generation paired with this exact row. Pre-v8 segments
    /// expose the reserved legacy value zero without touching an absent column.
    #[inline]
    pub(crate) fn source_generation(&self, id: u32) -> u64 {
        self.mmap_slice(self.source_generation, self.source_generation_count)
            .get(id as usize)
            .copied()
            .unwrap_or(0)
    }

    pub(crate) fn max_source_generation(&self) -> u64 {
        self.mmap_slice(self.source_generation, self.source_generation_count)
            .iter()
            .copied()
            .max()
            .unwrap_or(0)
    }

    /// The sorted `TagId` slice for a local id (ADR-049) — read back for the
    /// `set_vocab` recompile. Empty for a pre-tag (v1/v2) segment.
    #[inline]
    pub(crate) fn tags_of(&self, id: u32) -> &[crate::tagdict::TagId] {
        let i = id as usize;
        match (self.tag_off().get(i), self.tag_len().get(i)) {
            (Some(&o), Some(&l)) => &self.tag_blob()[o as usize..o as usize + l as usize],
            _ => &[],
        }
    }
}

mod matching;
mod materialize;
mod verification;
