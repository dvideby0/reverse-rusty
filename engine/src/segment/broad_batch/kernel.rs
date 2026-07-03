//! Columnar/bitmap eval kernels for the broad-lane batch path.
//!
//! Holds the per-segment [`BroadBackend`] surface — implemented by the in-memory
//! [`Segment`](crate::segment::Segment) and the file-backed
//! [`MmapSegment`](crate::storage::MmapSegment) so the columnar evaluator drives
//! both with one body — plus the pure-anchor materialization + full bitmap
//! verification of [`eval_one_segment`]. The driver ([`super::driver`]) feeds
//! these the destructured [`BroadBatchScratch`](super::driver::BroadBatchScratch)
//! buffers as plain slices/`Vec`s.

use crate::dict::FeatureId;
use crate::segment::{MatchStats, Segment};
use crate::storage::MmapSegment;
use crate::util::{sig_key, FastMap};

/// Walk the set bits of a title bitmap, calling `f(title_index)` for each.
#[inline]
fn for_each_set_bit(bits: &[u64], mut f: impl FnMut(usize)) {
    for (wi, &word) in bits.iter().enumerate() {
        let mut w = word;
        while w != 0 {
            let b = w.trailing_zeros() as usize;
            f((wi << 6) + b);
            w &= w - 1;
        }
    }
}

/// Which columnar lane [`eval_one_segment`] is evaluating: the opt-in broad
/// lane, or the always-visible hot tier (class H, ADR-105). One kernel body
/// serves both — the lane routes index selection, the per-lane [`MatchStats`]
/// counters, the vacuous-accept predicate, and the universal-signature probe
/// (class D lives in the broad index only).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(in crate::segment) enum Lane {
    Broad,
    Hot,
}

/// The per-segment columnar surface, implemented by the in-memory [`Segment`]
/// and the file-backed [`MmapSegment`], so the columnar evaluator drives both
/// with one body (no drift). Method names are distinct from each backing's
/// inherent methods so delegation is unambiguous.
///
/// Visibility note: `pub(in crate::segment)` only so it can appear as the trait
/// bound of [`eval_one_segment`] (which the sibling `driver` submodule calls).
/// It is not re-exported and stays invisible outside `crate::segment`.
pub(in crate::segment) trait BroadBackend {
    /// Probe `lane`'s index for `key` (after the anchor-filter check), appending
    /// reachable local IDs to `cands` (epoch-deduped via `seen`).
    fn reach(
        &self,
        lane: Lane,
        key: u64,
        epoch: u32,
        seen: &mut [u32],
        cands: &mut Vec<u32>,
        stats: &mut MatchStats,
    );
    fn alive(&self, local: u32) -> bool;
    /// Whether query `local` may take the skip-verify VACUOUS ACCEPT for a
    /// candidate reached through `anchor`: its entire positive semantics is that
    /// one anchor, so retrieval is proof of match. Lane-specific because the two
    /// tiers store their single anchor differently — a broad (class C) anchor is
    /// top-64 and lives in the masked required word (`is_pure_anchor`), while a
    /// hot (class H) anchor has NO mask bit and lives in the required tail
    /// (`pure_tail_anchor(local, anchor)`; without the lane split the fast path
    /// would never fire for exactly the single-token population the hot tier
    /// exists for).
    fn vacuous_accept(&self, lane: Lane, local: u32, anchor: FeatureId) -> bool;
    fn logical_id(&self, local: u32) -> u64;
    /// Whether query `local` satisfies the request's tag filter (ADR-049). The
    /// vacuous-accept fast path bypasses `verify`/`eval_into`, so it must check tags
    /// here to avoid leaking a filtered-out query.
    fn passes_tags(&self, local: u32, pred: &crate::exact::TagPredicate) -> bool;
    /// Batch-level count-gate pre-reject (lever 5a): `false` only when NO title
    /// in the batch can possibly satisfy `local` (a required feature or a whole
    /// any-of group is absent from the batch) — the caller then skips
    /// [`eval_into`](Self::eval_into) for it. Under-reject is the only possible
    /// error direction; never consults forbidden features.
    fn can_match_batch(
        &self,
        local: u32,
        batch_mask_union: u64,
        feat_row: &FastMap<FeatureId, u32>,
    ) -> bool;
    /// Write the matching-title bitmap for `local` into `acc` (bitmap transpose
    /// of `verify`); `grp` is reused scratch of the same width. `pred` is the request's
    /// tag filter, applied as a per-query scalar gate (same as the scalar path).
    #[allow(clippy::too_many_arguments)]
    fn eval_into(
        &self,
        local: u32,
        tmask_batch: &[u64],
        feat_row: &FastMap<FeatureId, u32>,
        feat_bits: &[u64],
        words: usize,
        acc: &mut [u64],
        grp: &mut [u64],
        pred: &crate::exact::TagPredicate,
    );
}

/// Build the feature → title-bitmap lookup closure for one batch.
#[inline]
fn lookup<'a>(
    feat_row: &'a FastMap<FeatureId, u32>,
    feat_bits: &'a [u64],
    words: usize,
) -> impl Fn(FeatureId) -> Option<&'a [u64]> {
    move |f: FeatureId| {
        feat_row
            .get(&f)
            .map(|&r| &feat_bits[r as usize * words..r as usize * words + words])
    }
}

impl BroadBackend for &Segment {
    #[inline]
    fn reach(
        &self,
        lane: Lane,
        key: u64,
        epoch: u32,
        seen: &mut [u32],
        cands: &mut Vec<u32>,
        stats: &mut MatchStats,
    ) {
        stats.probes_attempted += 1;
        if let Some(flt) = &self.filter {
            if !flt.may_contain(key) {
                stats.probes_skipped += 1;
                return;
            }
        }
        let index = match lane {
            Lane::Broad => &self.broad,
            Lane::Hot => &self.hot,
        };
        if let Some(posting) = index.get(key) {
            stats.postings_scanned += posting.len() as u32;
            match lane {
                Lane::Broad => stats.broad_postings_scanned += posting.len() as u32,
                Lane::Hot => stats.hot_postings_scanned += posting.len() as u32,
            }
            posting.for_each(|local| {
                if seen[local as usize] != epoch {
                    seen[local as usize] = epoch;
                    cands.push(local);
                }
            });
        }
    }
    #[inline]
    fn alive(&self, local: u32) -> bool {
        self.alive[local as usize]
    }
    #[inline]
    fn vacuous_accept(&self, lane: Lane, local: u32, anchor: FeatureId) -> bool {
        match lane {
            Lane::Broad => self.exact.is_pure_anchor(local),
            Lane::Hot => self.exact.pure_tail_anchor(local, anchor),
        }
    }
    #[inline]
    fn logical_id(&self, local: u32) -> u64 {
        self.exact.logical(local)
    }
    #[inline]
    fn passes_tags(&self, local: u32, pred: &crate::exact::TagPredicate) -> bool {
        pred.matches(self.exact.tags_of(local))
    }
    #[inline]
    fn can_match_batch(
        &self,
        local: u32,
        batch_mask_union: u64,
        feat_row: &FastMap<FeatureId, u32>,
    ) -> bool {
        self.exact
            .can_match_batch(local, batch_mask_union, |f| feat_row.contains_key(&f))
    }
    #[inline]
    fn eval_into(
        &self,
        local: u32,
        tmask_batch: &[u64],
        feat_row: &FastMap<FeatureId, u32>,
        feat_bits: &[u64],
        words: usize,
        acc: &mut [u64],
        grp: &mut [u64],
        pred: &crate::exact::TagPredicate,
    ) {
        self.exact.eval_batch(
            local,
            tmask_batch,
            lookup(feat_row, feat_bits, words),
            acc,
            grp,
            pred,
        );
    }
}

impl BroadBackend for &MmapSegment {
    #[inline]
    fn reach(
        &self,
        lane: Lane,
        key: u64,
        epoch: u32,
        seen: &mut [u32],
        cands: &mut Vec<u32>,
        stats: &mut MatchStats,
    ) {
        match lane {
            Lane::Broad => self.broad_reach(key, epoch, seen, cands, stats),
            Lane::Hot => self.hot_reach(key, epoch, seen, cands, stats),
        }
    }
    #[inline]
    fn alive(&self, local: u32) -> bool {
        self.is_alive_at(local)
    }
    #[inline]
    fn vacuous_accept(&self, lane: Lane, local: u32, anchor: FeatureId) -> bool {
        match lane {
            Lane::Broad => self.is_pure_anchor(local),
            Lane::Hot => self.pure_tail_anchor(local, anchor),
        }
    }
    #[inline]
    fn logical_id(&self, local: u32) -> u64 {
        self.logical(local)
    }
    #[inline]
    fn passes_tags(&self, local: u32, pred: &crate::exact::TagPredicate) -> bool {
        pred.matches(self.tags_of(local))
    }
    #[inline]
    fn can_match_batch(
        &self,
        local: u32,
        batch_mask_union: u64,
        feat_row: &FastMap<FeatureId, u32>,
    ) -> bool {
        MmapSegment::can_match_batch(self, local, batch_mask_union, |f| feat_row.contains_key(&f))
    }
    #[inline]
    fn eval_into(
        &self,
        local: u32,
        tmask_batch: &[u64],
        feat_row: &FastMap<FeatureId, u32>,
        feat_bits: &[u64],
        words: usize,
        acc: &mut [u64],
        grp: &mut [u64],
        pred: &crate::exact::TagPredicate,
    ) {
        self.eval_batch(
            local,
            tmask_batch,
            lookup(feat_row, feat_bits, words),
            acc,
            grp,
            pred,
        );
    }
}

/// Evaluate one columnar lane (broad or hot) of one segment against the whole
/// batch, appending matched logical IDs to each title's `outs[ti]`.
///
/// `prefilter` + `batch_mask_union` drive the count-gate pre-reject (lever 5a):
/// a reached, alive, non-vacuous candidate whose required features / any-of
/// groups cannot all be satisfied by ANY batch title is skipped before full
/// bitmap verification (result-identical by [`prefilter_slices`]'s necessary-
/// condition argument; counted per lane). The knob is the provable kill-switch —
/// `false` restores the exact pre-lever path.
#[allow(clippy::too_many_arguments, clippy::needless_pass_by_value)]
pub(in crate::segment) fn eval_one_segment<B: BroadBackend>(
    backend: B,
    lane: Lane,
    distinct: &[FeatureId],
    feat_row: &FastMap<FeatureId, u32>,
    feat_bits: &[u64],
    words: usize,
    tmask_batch: &[u64],
    batch_mask_union: u64,
    seen: &mut [u32],
    epoch: u32,
    cands: &mut Vec<u32>,
    non_pure: &mut Vec<u32>,
    acc: &mut [u64],
    grp: &mut [u64],
    outs: &mut [Vec<u64>],
    materialize: bool,
    prefilter: bool,
    pred: &crate::exact::TagPredicate,
    stats: &mut MatchStats,
) {
    cands.clear();
    non_pure.clear();
    match lane {
        // +1: the universal probe below is an anchor-table probe too — without it
        // an empty-feature batch would report zero anchors scanned despite probing.
        Lane::Broad => stats.broad_anchors_scanned += distinct.len() as u32 + 1,
        Lane::Hot => stats.hot_anchors_scanned += distinct.len() as u32,
    }

    // Universal signature: class-D always-candidates (ADR-068), ONE probe per
    // batch — the amortization this lane rides the batch path for. BROAD LANE
    // ONLY: class D lives in the broad index (probing it from the hot pass would
    // double-count its candidates). Reached entries go straight to full bitmap
    // verification: they are never pure-anchor (`is_pure_anchor` is structurally
    // false for an empty required mask), and `eval_batch_slices` on an
    // empty-positive entry computes exactly the vacuous semantics (titles
    // bearing no forbidden feature). The count-gate passes them vacuously by
    // construction (empty positives — nothing to be absent), so class-D
    // always-candidates can never be prefilter-skipped; the check is still run
    // so the shared predicate stays the single source of truth.
    if lane == Lane::Broad {
        let before = cands.len();
        backend.reach(
            lane,
            crate::util::universal_sig(),
            epoch,
            seen,
            cands,
            stats,
        );
        for &local in &cands[before..] {
            stats.unique_candidates += 1;
            stats.broad_candidates += 1;
            if !backend.alive(local) {
                continue;
            }
            if prefilter && !backend.can_match_batch(local, batch_mask_union, feat_row) {
                stats.broad_prefilter_skipped += 1;
                continue;
            }
            non_pure.push(local);
        }
    }

    // Reachability + vacuous-accept emit, one probe per distinct batch feature.
    for &f in distinct {
        let key = sig_key(&[f]);
        let before = cands.len();
        backend.reach(lane, key, epoch, seen, cands, stats);
        if cands.len() == before {
            continue;
        }
        // A single-anchor query has exactly one sig in this lane (its anchor), so
        // the feature `f` that reached it IS its anchor: it matches exactly the
        // batch titles containing `f` — emit straight from `f`'s bitmap.
        let Some(&r) = feat_row.get(&f) else {
            continue;
        };
        let fbits = &feat_bits[r as usize * words..r as usize * words + words];
        for &local in &cands[before..] {
            stats.unique_candidates += 1;
            match lane {
                Lane::Broad => stats.broad_candidates += 1,
                Lane::Hot => stats.hot_candidates += 1,
            }
            if !backend.alive(local) {
                continue;
            }
            // Vacuous-accept fast path: emit straight from the anchor bitmap. When
            // materialization is off, fall through to full verification — eval_into
            // on such a query computes the same bitmap (its required gate alone
            // selects exactly the titles containing the anchor), so results are
            // identical, just slower. The tag filter (ADR-049) must still be honored
            // here, since this path bypasses `verify`/`eval_into`: a query that
            // fails the filter emits nothing (an empty predicate always passes, so
            // the no-filter path is unchanged).
            if materialize && backend.vacuous_accept(lane, local, f) {
                if backend.passes_tags(local, pred) {
                    let logical = backend.logical_id(local);
                    for_each_set_bit(fbits, |ti| outs[ti].push(logical));
                }
            } else {
                // Count-gate pre-reject (lever 5a) before queueing full bitmap
                // verification. A vacuous-accept candidate never reaches this check
                // (its anchor IS in the batch — it was reached through it).
                if prefilter && !backend.can_match_batch(local, batch_mask_union, feat_row) {
                    match lane {
                        Lane::Broad => stats.broad_prefilter_skipped += 1,
                        Lane::Hot => stats.hot_prefilter_skipped += 1,
                    }
                    continue;
                }
                non_pure.push(local);
            }
        }
    }

    // Full bitmap verification for the rest (eval_into applies the tag filter as a
    // per-query scalar gate, so a filtered-out query writes an empty bitmap).
    for &local in non_pure.iter() {
        match lane {
            Lane::Broad => stats.broad_queries_evaluated += 1,
            Lane::Hot => stats.hot_queries_evaluated += 1,
        }
        backend.eval_into(
            local,
            tmask_batch,
            feat_row,
            feat_bits,
            words,
            acc,
            grp,
            pred,
        );
        let logical = backend.logical_id(local);
        for_each_set_bit(acc, |ti| outs[ti].push(logical));
    }
}
