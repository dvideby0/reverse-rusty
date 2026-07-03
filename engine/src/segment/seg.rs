//! `impl Segment` — the in-memory (or memtable) index slice: append, probe,
//! tombstone, and the per-segment memory accounting. Type definition lives in
//! the `segment` module root; the compaction merges live in the sibling
//! [`merge`](super::merge) submodule.

use super::{MatchStats, ProbeLanes, Segment};
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
    // Reject a class-D plan unless the lane is on AND there is something to forbid.
    class == CostClass::D && (!accept_class_d || ex.forbidden.is_empty())
}

impl Segment {
    pub fn new() -> Self {
        Segment {
            main: CandidateIndex::new(),
            broad: CandidateIndex::new(),
            hot: CandidateIndex::new(),
            exact: ExactStore::new(),
            class: Vec::new(),
            alive: Vec::new(),
            alive_counter: 0,
            filter: None,
            vocab_epoch: 0,
            logical_index: crate::util::fast_map(),
        }
    }

    /// Build and attach the anchor filter from the current main + broad + hot
    /// index keys. Called once when a segment is sealed (flush, bulk_ingest,
    /// compaction). After this, `match_into` will use the filter to skip probes.
    pub(in crate::segment) fn build_filter(&mut self) {
        let mut keys = self.main.keys();
        keys.extend(self.broad.keys());
        keys.extend(self.hot.keys());
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
    /// The hot tier's candidate index (class H, ADR-105).
    pub fn hot_index(&self) -> &CandidateIndex {
        &self.hot
    }
    /// Whether this segment holds any hot-tier entries — the per-segment skip
    /// that makes the hot tier structurally free on hot-empty corpora.
    #[inline]
    pub fn has_hot_entries(&self) -> bool {
        self.hot.num_signatures() > 0
    }

    /// Append one already-extracted query. Returns the new segment-local id plus
    /// the plan's [`would_be_hot`](crate::compile::SigPlan::would_be_hot)
    /// observe-first flag (the Broad-Query Cost Program's reclassification
    /// telemetry — the `Engine` accumulates it per accepted compile), or `None`
    /// if the query is class D and rejected. `tags` are the query's interned,
    /// sorted `TagId`s (ADR-049); pass `&[]` for an untagged query.
    ///
    /// `accept_class_d` (ADR-068): when set, a negation-only query (class D with a
    /// non-empty forbidden set) is stored as an **always-candidate** under the
    /// universal broad signature its plan carries. A query with no positives AND no
    /// forbidden features (an effectively empty query — it would match every title
    /// outright) is rejected regardless. Ingest paths pass the
    /// `EngineConfig::accept_class_d` knob; WAL replay and the vocab recompile pass
    /// `true` unconditionally (an acknowledged/stored query must never be dropped
    /// by a since-flipped knob).
    // The knobs are threaded bare, matching the established accept_class_d
    // precedent (ADR-068); an options struct waits for lever 3's AnchorCtx.
    #[allow(clippy::too_many_arguments)]
    pub fn add_compiled(
        &mut self,
        ex: &Extracted,
        tags: &[crate::tagdict::TagId],
        dict: &Dict,
        logical: u64,
        version: u32,
        accept_class_d: bool,
        theta: u32,
    ) -> Option<(u32, bool)> {
        let plan = build_signatures(ex, dict, theta);
        if rejects_class_d(plan.class, ex, accept_class_d) {
            return None;
        }
        let local = self.exact.push(ex, tags, dict, version, logical);
        for &s in &plan.main_sigs {
            self.main.insert(s, local);
        }
        for &s in &plan.broad_sigs {
            self.broad.insert(s, local);
        }
        for &s in &plan.hot_sigs {
            self.hot.insert(s, local);
        }
        self.class.push(plan.class);
        self.alive.push(true);
        self.alive_counter += 1;
        self.logical_index.entry(logical).or_default().push(local);
        Some((local, plan.would_be_hot))
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

    /// The stored per-query version for a local id — read back for the cluster
    /// rebuild gather (ADR-074) so a `set_vocab`/resize preserves a query's stored
    /// version rather than resetting it to 1.
    pub fn version_of(&self, local_id: u32) -> u32 {
        self.exact.version(local_id)
    }

    /// Whether a local id is alive (not tombstoned).
    #[inline]
    pub fn is_alive(&self, local_id: u32) -> bool {
        self.alive.get(local_id as usize).copied().unwrap_or(false)
    }

    pub fn class_counts(&self, c: &mut [u64; 5]) {
        for &cl in &self.class {
            match cl {
                CostClass::A => c[0] += 1,
                CostClass::B => c[1] += 1,
                CostClass::C => c[2] += 1,
                CostClass::D => c[3] += 1,
                // Index 4 is APPENDED (never reordered): the autoscaler and the
                // class-D pins read c[2]/c[3] positionally.
                CostClass::H => c[4] += 1,
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
        view: &crate::exact::TitleView,
        dict: &Dict,
        epoch: u32,
        seen: &mut [u32],
        out: &mut Vec<u64>,
        lanes: ProbeLanes,
        pred: &crate::exact::TagPredicate,
        stats: &mut MatchStats,
    ) {
        let filter = self.filter.as_ref();
        // Signatures are generated from the POSITIVE (superset) view so an overlapping alias
        // entity retrieves its candidates (ADR-061); verify then applies both views.
        let feats = view.pos;

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
                key,
                &self.main,
                epoch,
                view,
                seen,
                out,
                pred,
                stats,
                ProbeLane::Main,
            );
        }
        // arity-2 signatures: {hot feature} x {every other feature}. Deliberately
        // keyed to the FROZEN top-64 mask (`is_hot`), never θ — this loop is the
        // title side of the class-B pair predicate, and extending it is lever 3's
        // fenced change, not the hot tier's (ADR-105).
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
                            key,
                            &self.main,
                            epoch,
                            view,
                            seen,
                            out,
                            pred,
                            stats,
                            ProbeLane::Main,
                        );
                    }
                }
            }
        }
        // Hot tier (class H, ADR-105): arity-1 anchors, probed on EVERY request —
        // always-visible like main, so this is NOT gated by `include_broad`. The
        // `lanes.include_hot` gate only lets the batch driver lift the lane into
        // its columnar pass (evaluated exactly once either way). Skipped outright
        // when the segment holds no hot entries — one branch per segment per
        // title, the structural zero-overhead answer for hot-free corpora.
        if lanes.include_hot && self.has_hot_entries() {
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
                    &self.hot,
                    epoch,
                    view,
                    seen,
                    out,
                    pred,
                    stats,
                    ProbeLane::Hot,
                );
            }
        }
        // broad lane (arity-1 anchors), measured separately
        if lanes.include_broad {
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
                    view,
                    seen,
                    out,
                    pred,
                    stats,
                    ProbeLane::Broad,
                );
            }
            // Universal signature: class-D always-candidates (ADR-068). Probed
            // unconditionally — the accept knob gates ingest, never visibility, so a
            // stored entry stays reachable however the knob is later toggled. With no
            // class-D entries this is one filter (or hash) miss per segment.
            let key = crate::util::universal_sig();
            stats.probes_attempted += 1;
            let skip = filter.is_some_and(|flt| !flt.may_contain(key));
            if skip {
                stats.probes_skipped += 1;
            } else {
                self.probe(
                    key,
                    &self.broad,
                    epoch,
                    view,
                    seen,
                    out,
                    pred,
                    stats,
                    ProbeLane::Broad,
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
        view: &crate::exact::TitleView,
        seen: &mut [u32],
        out: &mut Vec<u64>,
        pred: &crate::exact::TagPredicate,
        stats: &mut MatchStats,
        lane: ProbeLane,
    ) {
        if let Some(posting) = index.get(key) {
            stats.postings_scanned += posting.len() as u32;
            match lane {
                ProbeLane::Broad => stats.broad_postings_scanned += posting.len() as u32,
                ProbeLane::Hot => stats.hot_postings_scanned += posting.len() as u32,
                ProbeLane::Main => {}
            }
            posting.for_each(|local| {
                // dedup across signatures with an epoch stamp (O(1), no alloc)
                if seen[local as usize] == epoch {
                    return;
                }
                seen[local as usize] = epoch;
                stats.unique_candidates += 1;
                match lane {
                    ProbeLane::Broad => stats.broad_candidates += 1,
                    ProbeLane::Hot => stats.hot_candidates += 1,
                    ProbeLane::Main => stats.main_candidates += 1,
                }
                if !self.alive[local as usize] {
                    return; // tombstoned
                }
                // Tag filter (ADR-049) — applied post-candidate inside verify.
                if self.exact.verify(local, view, pred) {
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
            hot,
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
