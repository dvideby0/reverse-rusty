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
use super::{MmapLogicalIndex, MmapSegment};
use crate::compile::CostClass;
use crate::dict::FeatureId;
use crate::index::CandidateIndex;
use crate::segment::{MatchStats, Segment};

impl MmapSegment {
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
        if let Some(slot) = self.alive_overlay.get_mut(local_id as usize) {
            if *slot {
                self.alive_counter -= 1;
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
    pub fn class_counts(&self, c: &mut [u64; 4]) {
        let n = self.len();
        for i in 0..n {
            // SAFETY: `i < n == num_queries`, the length of the `class_arr` byte array
            // parsed from the mmap (same bound `to_memory_segment` uses).
            let class_byte = unsafe { *self.class_arr.add(i) };
            c[(class_byte as usize).min(3)] += 1;
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

    /// Integer-only exact verification — same logic as ExactStore::verify but
    /// operating on mmap'd slices. `pred` is the request's compiled tag filter
    /// (`TagPredicate::empty()` ⇒ no filtering); the tag columns come from the mmap and are
    /// empty for a pre-tag (v1/v2) segment (every query reads back untagged).
    #[inline]
    pub fn verify(
        &self,
        id: u32,
        view: &crate::exact::TitleView,
        pred: &crate::exact::TagPredicate,
    ) -> bool {
        crate::exact::verify_slices(
            id,
            view.pos_mask,
            view.pos,
            view.neg_mask,
            view.neg,
            self.req_mask(),
            self.forb_mask(),
            self.req_off(),
            self.req_len(),
            self.req_blob(),
            self.forb_off(),
            self.forb_len(),
            self.forb_blob(),
            self.q_group_start(),
            self.q_group_count(),
            self.group_off(),
            self.group_len(),
            self.anyof_blob(),
            pred,
            self.tag_off(),
            self.tag_len(),
            self.tag_blob(),
        )
    }

    // ---- broad-lane batch evaluation surface (mmap twin of the in-memory
    // `Segment` accessors used by `segment::broad_batch`). Lets the columnar
    // broad evaluator drive mmap and in-memory segments through one body. ----

    /// Probe the broad frozen table for `key` (after the anchor-filter check),
    /// appending reachable local IDs to `cands` (epoch-deduped via `seen`). The
    /// reachability primitive for the batch broad lane — mirrors the broad block
    /// of `match_into` (filter gate + probe) so the columnar path skips the same
    /// probes the per-title path would.
    #[inline]
    pub(crate) fn broad_reach(
        &self,
        key: u64,
        epoch: u32,
        seen: &mut [u32],
        cands: &mut Vec<u32>,
        stats: &mut MatchStats,
    ) {
        stats.probes_attempted += 1;
        if self.filter_num_blocks > 0 && !self.may_contain(key) {
            stats.probes_skipped += 1;
            return;
        }
        if let Some(posting) =
            frozen_probe(key, self.broad_slots(), self.broad_blob(), self.broad_mask)
        {
            stats.postings_scanned += posting.len() as u32;
            stats.broad_postings_scanned += posting.len() as u32;
            for &local in posting {
                if seen[local as usize] != epoch {
                    seen[local as usize] = epoch;
                    cands.push(local);
                }
            }
        }
    }

    /// Liveness for one local ID (mmap tombstone overlay).
    #[inline]
    pub(crate) fn is_alive_at(&self, local: u32) -> bool {
        self.alive_overlay[local as usize]
    }

    /// Whether `local`'s entire semantics is its hot anchor — the pure-anchor
    /// skip-verify fast path. Mmap twin of [`crate::exact::ExactStore::is_pure_anchor`].
    #[inline]
    pub(crate) fn is_pure_anchor(&self, local: u32) -> bool {
        let i = local as usize;
        self.req_len()[i] == 0
            && self.forb_mask()[i] == 0
            && self.forb_len()[i] == 0
            && self.q_group_count()[i] == 0
            && self.req_mask()[i].is_power_of_two()
    }

    /// Columnar batch verification for one query against a title batch, writing
    /// the matching-title bitmap into `acc`. Mmap twin of
    /// [`crate::exact::ExactStore::eval_batch`]; shares
    /// [`crate::exact::eval_batch_slices`] so the in-memory and mmap broad-batch
    /// paths cannot drift.
    #[inline]
    pub(crate) fn eval_batch<'a>(
        &self,
        local: u32,
        tmask_batch: &[u64],
        lookup: impl Fn(FeatureId) -> Option<&'a [u64]>,
        acc: &mut [u64],
        grp: &mut [u64],
        pred: &crate::exact::TagPredicate,
    ) {
        crate::exact::eval_batch_slices(
            local as usize,
            tmask_batch,
            lookup,
            acc,
            grp,
            self.req_mask(),
            self.forb_mask(),
            self.req_off(),
            self.req_len(),
            self.req_blob(),
            self.forb_off(),
            self.forb_len(),
            self.forb_blob(),
            self.q_group_start(),
            self.q_group_count(),
            self.group_off(),
            self.group_len(),
            self.anyof_blob(),
            pred,
            self.tag_off(),
            self.tag_len(),
            self.tag_blob(),
        );
    }

    /// Filter check: is this signature key possibly in this segment?
    #[inline]
    fn may_contain(&self, key: u64) -> bool {
        if self.filter_num_blocks == 0 {
            return true; // no filter = don't skip
        }
        crate::filter::bloom_check(key, self.filter_data(), self.filter_mask)
    }

    /// Probe this segment for one title — same semantics as Segment::match_into.
    #[allow(clippy::too_many_arguments)]
    pub fn match_into(
        &self,
        view: &crate::exact::TitleView,
        dict: &crate::dict::Dict,
        epoch: u32,
        seen: &mut [u32],
        out: &mut Vec<u64>,
        include_broad: bool,
        pred: &crate::exact::TagPredicate,
        stats: &mut MatchStats,
    ) {
        let has_filter = self.filter_num_blocks > 0;
        // Retrieval uses the positive (superset) view; verify applies both (ADR-061).
        let feats = view.pos;

        // arity-1 signatures
        for &f in feats {
            let key = crate::util::sig_key(&[f]);
            stats.probes_attempted += 1;
            if has_filter && !self.may_contain(key) {
                stats.probes_skipped += 1;
                continue;
            }
            self.probe_index(key, true, epoch, view, seen, out, pred, stats, false);
        }
        // arity-2 signatures
        for &h in feats {
            if crate::compile::is_hot(dict, h) {
                for &o in feats {
                    if o != h {
                        let (a, b) = if h < o { (h, o) } else { (o, h) };
                        let key = crate::util::sig_key(&[a, b]);
                        stats.probes_attempted += 1;
                        if has_filter && !self.may_contain(key) {
                            stats.probes_skipped += 1;
                            continue;
                        }
                        self.probe_index(key, true, epoch, view, seen, out, pred, stats, false);
                    }
                }
            }
        }
        // broad lane
        if include_broad {
            for &f in feats {
                let key = crate::util::sig_key(&[f]);
                stats.probes_attempted += 1;
                if has_filter && !self.may_contain(key) {
                    stats.probes_skipped += 1;
                    continue;
                }
                self.probe_index(key, false, epoch, view, seen, out, pred, stats, true);
            }
            // Universal signature: class-D always-candidates (ADR-068). Probed
            // unconditionally (the accept knob gates ingest, never visibility);
            // with no class-D entries this is one filter miss. Mirrors
            // `Segment::match_into` exactly.
            let key = crate::util::universal_sig();
            stats.probes_attempted += 1;
            if has_filter && !self.may_contain(key) {
                stats.probes_skipped += 1;
            } else {
                self.probe_index(key, false, epoch, view, seen, out, pred, stats, true);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[inline]
    fn probe_index(
        &self,
        key: u64,
        is_main: bool,
        epoch: u32,
        view: &crate::exact::TitleView,
        seen: &mut [u32],
        out: &mut Vec<u64>,
        pred: &crate::exact::TagPredicate,
        stats: &mut MatchStats,
        is_broad: bool,
    ) {
        let (slots, blob, mask) = if is_main {
            (self.main_slots(), self.main_blob(), self.main_mask)
        } else {
            (self.broad_slots(), self.broad_blob(), self.broad_mask)
        };

        if let Some(posting) = frozen_probe(key, slots, blob, mask) {
            stats.postings_scanned += posting.len() as u32;
            // Broad subset of postings_scanned — the memory-path `Segment::probe` and the
            // columnar `broad_reach` above both count it; this per-title mmap path missed it
            // (codex, ADR-101), under-counting the exported per-shard broad-cost counter on
            // durable shards.
            if is_broad {
                stats.broad_postings_scanned += posting.len() as u32;
            }
            for &local in posting {
                if seen[local as usize] == epoch {
                    continue;
                }
                seen[local as usize] = epoch;
                stats.unique_candidates += 1;
                if is_broad {
                    stats.broad_candidates += 1;
                } else {
                    stats.main_candidates += 1;
                }
                if !self.alive_overlay[local as usize] {
                    continue;
                }
                // Tag filter (ADR-049) — applied post-candidate inside verify.
                if self.verify(local, view, pred) {
                    out.push(self.logical(local));
                }
            }
        }
    }

    /// Reconstruct an in-memory Segment from this mmap'd segment. Used by
    /// compaction to produce source data for Segment::compact_from.
    pub fn to_memory_segment(&self) -> Segment {
        use crate::exact::ExactStore;
        let n = self.num_queries as usize;

        let mut exact = ExactStore::new();
        let mut classes = Vec::with_capacity(n);
        let mut alive = Vec::with_capacity(n);

        // Copy exact store arrays
        for i in 0..n {
            let rm = self.req_mask()[i];
            let fm = self.forb_mask()[i];
            let ro = self.req_off()[i] as usize;
            let rl = self.req_len()[i] as usize;
            let fo = self.forb_off()[i] as usize;
            let fl = self.forb_len()[i] as usize;
            let gs = self.q_group_start()[i] as usize;
            let gc = self.q_group_count()[i] as usize;
            // SAFETY: the loop runs `i` over `0..n` where `n == num_queries`, and
            // `version_arr`/`logical_arr` are both `num_queries`-long arrays parsed
            // from the mmap in `open`, so both offsets are in bounds of the
            // immutable mapping `self` owns.
            let (ver, log) = unsafe { (*self.version_arr.add(i), *self.logical_arr.add(i)) };

            // Per-query tag slice (ADR-049); empty for a pre-tag (v1/v2) segment whose
            // tag column has no entries, so compaction carries tags through faithfully.
            let tags: &[crate::tagdict::TagId] =
                match (self.tag_off().get(i), self.tag_len().get(i)) {
                    (Some(&o), Some(&l)) => &self.tag_blob()[o as usize..o as usize + l as usize],
                    _ => &[],
                };

            exact.push_raw(
                rm,
                fm,
                &self.req_blob()[ro..ro + rl],
                &self.forb_blob()[fo..fo + fl],
                (
                    gs,
                    gc,
                    self.group_off(),
                    self.group_len(),
                    self.anyof_blob(),
                ),
                tags,
                ver,
                log,
            );

            // SAFETY: `i < n == num_queries`, and `class_arr` is the
            // `num_queries`-long class byte array parsed from the mmap, so the
            // offset is in bounds of the immutable mapping.
            let class_byte = unsafe { *self.class_arr.add(i) };
            classes.push(match class_byte {
                0 => CostClass::A,
                1 => CostClass::B,
                2 => CostClass::C,
                _ => CostClass::D,
            });
            alive.push(self.alive_overlay[i]);
        }

        // Rebuild candidate indexes from frozen tables
        let mut main = CandidateIndex::new();
        for slot in self.main_slots() {
            if slot.key != 0 {
                let start = slot.offset as usize;
                let end = start + slot.len as usize;
                for &id in &self.main_blob()[start..end] {
                    main.insert(slot.key, id);
                }
            }
        }

        let mut broad = CandidateIndex::new();
        for slot in self.broad_slots() {
            if slot.key != 0 {
                let start = slot.offset as usize;
                let end = start + slot.len as usize;
                for &id in &self.broad_blob()[start..end] {
                    broad.insert(slot.key, id);
                }
            }
        }

        let mut seg = Segment::from_parts(main, broad, exact, classes, alive);
        seg.vocab_epoch = self.vocab_epoch;
        seg
    }
}
