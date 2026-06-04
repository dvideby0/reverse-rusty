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
    /// `None` if the query is class D (rejected, not stored). `tags` are the query's
    /// interned, sorted `TagId`s (ADR-049); pass `&[]` for an untagged query.
    pub fn add_compiled(
        &mut self,
        ex: &Extracted,
        tags: &[crate::tagdict::TagId],
        dict: &Dict,
        logical: u64,
        version: u32,
    ) -> Option<u32> {
        let plan = build_signatures(ex, dict);
        if plan.class == CostClass::D {
            return None;
        }
        let local = self.exact.push(ex, tags, dict, version, logical);
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

    /// The sorted `TagId` slice for a local id (ADR-049) — read back for the
    /// `set_vocab` recompile so tags survive a vocabulary change.
    pub fn tags_of(&self, local_id: u32) -> &[crate::tagdict::TagId] {
        self.exact.tags_of(local_id)
    }

    /// Whether a local id is alive (not tombstoned).
    #[inline]
    pub fn is_alive(&self, local_id: u32) -> bool {
        self.alive.get(local_id as usize).copied().unwrap_or(false)
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
        pred: &crate::exact::TagPredicate,
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
                key, &self.main, epoch, tmask, feats, seen, out, pred, stats, false,
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
                            key, &self.main, epoch, tmask, feats, seen, out, pred, stats, false,
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
                    pred,
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
        pred: &crate::exact::TagPredicate,
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
                // Tag filter (ADR-049) — applied post-candidate inside verify.
                if self.exact.verify(local, tmask, feats, pred) {
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

    /// Compaction's "improve" variant (ADR-056): merge like [`compact_from`](Self::compact_from)
    /// but **re-anchor** each alive query — re-derive its signature cover with the *current*
    /// feature frequencies instead of carrying the old anchors forward. Returns the merged
    /// segment plus the number of queries whose cover actually changed.
    ///
    /// Correctness (zero false negatives): the cover is rebuilt by the SAME
    /// [`build_signatures`]/`anchor_plan` optimizer the title side is matched against
    /// ([`match_into`](Self::match_into)), using the same `dict`, so any title that matches a
    /// query still generates a signature that retrieves it — the anchor choice only governs
    /// *which* posting list the query lives in. The exact-store data is copied **verbatim**
    /// (`copy_entry`), so `verify`/`is_pure_anchor` are byte-identical and forbidden features
    /// are preserved; only the index postings and the per-query cost class are re-derived.
    ///
    /// The cost class *can* change (e.g. A→B): with the common mask frozen, a feature's
    /// frequency and its hotness diverge as the corpus drifts, so a query's rarest-by-current-
    /// frequency required feature can now be a hot one and escalate to an arity-2 cover. This
    /// is still lossless because `anchor_plan`'s class-B/C anchors are always hot features, and
    /// the title side ([`match_into`](Self::match_into)) generates exactly those {hot}×{other}
    /// and broad signatures — the same matched-pair guarantee. A query is never re-anchored to
    /// class D (a stored query always has a required/any-of feature).
    ///
    /// One transition is **refused**: a main-lane (A/B) query is never demoted into the broad
    /// (C) lane. The main index is always probed, but the broad lane is opt-in (the default
    /// percolate path has `include_broad = false`), so a main→broad move would hide the query
    /// on that path — a false negative. That crossing happens only for a query whose sole
    /// anchor became hot (e.g. an entry compiled before the mask was finalized), which is a
    /// hotness reclassification — a major-version blue/green concern (matching.md §8), not a
    /// silent compaction change — so such an entry keeps its original cover.
    ///
    /// Invariant preserved: entries are processed in ascending old-local-id order and each
    /// entry's fresh sigs are inserted at its (ascending) new id immediately, so every posting
    /// stays sorted by construction (no per-insert sort/dedup needed — same contract as
    /// `add_compiled`).
    pub fn compact_from_reanchored(sources: &[&Segment], dict: &Dict) -> (Segment, usize) {
        let mut dest = Segment::new();
        let mask_inverse = dict.mask_inverse();
        let mut reanchored = 0usize;

        for &src in sources {
            // Invert the indexes once, lane-separated (old_id -> the main / broad sig keys it
            // appears under), so we can tell which entries actually moved AND in which lane.
            // One pass, O(postings) — the same order as the merge, and it stands in for
            // compact_from's posting-remap passes.
            let mut old_main: Vec<Vec<u64>> = vec![Vec::new(); src.len()];
            let mut old_broad: Vec<Vec<u64>> = vec![Vec::new(); src.len()];
            src.main.for_each_posting(|key, posting| {
                posting.for_each(|old_id| old_main[old_id as usize].push(key));
            });
            src.broad.for_each_posting(|key, posting| {
                posting.for_each(|old_id| old_broad[old_id as usize].push(key));
            });

            for (old, &is_alive) in src.alive.iter().enumerate() {
                if !is_alive {
                    continue; // drop tombstoned entries, reclaiming their space
                }
                // Copy the exact-store entry verbatim (masks, forbidden, any-of, tags,
                // identity) — re-anchoring must not touch the verified semantics.
                let new_id = src.exact.copy_entry(old as u32, &mut dest.exact);
                let logical = dest.exact.logical(new_id);
                let old_class = src.class[old];

                // Re-derive the cover from the (unchanged) stored required/any-of features
                // against the current dict. `anchor_plan` reads only required + any-of, so the
                // empty `forbidden` here is irrelevant to selection.
                let (required, anyof) = dest.exact.anchoring_inputs(new_id, &mask_inverse);
                let ex = Extracted {
                    required,
                    forbidden: Vec::new(),
                    anyof,
                };
                let plan = build_signatures(&ex, dict);
                debug_assert_ne!(
                    plan.class,
                    CostClass::D,
                    "a stored (non-D) query must never re-anchor to class D"
                );

                // CORRECTNESS GUARD — never demote a main-lane (A/B) query into the broad
                // lane. The main index is probed on every percolate; the broad lane is opt-in
                // (the default path has `include_broad = false`), so moving a query main→broad
                // would hide it there — a false negative. A query crossing INTO broad because
                // its anchor went hot is a *hotness reclassification*, which is a major-version
                // blue/green concern (matching.md §8), NOT a silent compaction change — so keep
                // the original cover. (The reverse, broad→main, only adds findability and is
                // kept. This can leave a now-hot arity-1 anchor in main, but that pollution
                // already existed pre-compaction; re-anchoring just doesn't make it unfindable.)
                let prev_main = std::mem::take(&mut old_main[old]);
                let prev_broad = std::mem::take(&mut old_broad[old]);
                let demotes_to_broad =
                    matches!(old_class, CostClass::A | CostClass::B) && plan.class == CostClass::C;
                let (main_keys, broad_keys, class): (&[u64], &[u64], CostClass) =
                    if demotes_to_broad {
                        (&prev_main, &prev_broad, old_class)
                    } else {
                        (&plan.main_sigs, &plan.broad_sigs, plan.class)
                    };

                for &s in main_keys {
                    dest.main.insert(s, new_id);
                }
                for &s in broad_keys {
                    dest.broad.insert(s, new_id);
                }
                dest.class.push(class);
                dest.alive.push(true);
                dest.alive_counter += 1;
                dest.logical_index.entry(logical).or_default().push(new_id);

                // Did the cover actually change? Compare lane-tagged key sets, so a posting
                // that merely moved lane (same `u64`, different index) still counts.
                let lane_tagged = |main: &[u64], broad: &[u64]| {
                    let mut v: Vec<(u8, u64)> = main
                        .iter()
                        .map(|&k| (0u8, k))
                        .chain(broad.iter().map(|&k| (1u8, k)))
                        .collect();
                    v.sort_unstable();
                    v
                };
                if lane_tagged(main_keys, broad_keys) != lane_tagged(&prev_main, &prev_broad) {
                    reanchored += 1;
                }
            }
        }

        // Build anchor filter for the newly compacted (sealed) segment.
        dest.build_filter();
        // Merged segment inherits the minimum epoch — still stale if any source was.
        dest.vocab_epoch = sources.iter().map(|s| s.vocab_epoch).min().unwrap_or(0);
        (dest, reanchored)
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
