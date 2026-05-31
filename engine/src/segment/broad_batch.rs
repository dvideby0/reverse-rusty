//! Broad-lane batch / columnar evaluation — the once-per-batch broad matcher.
//!
//! Design: docs/design/matching.md §4 (broad lane); ADR-026.
//! Invariant: produces a per-title match set BYTE-IDENTICAL to the scalar
//!   per-title broad path (`Segment::match_into(include_broad=true)`). Forbidden
//!   features are consulted ONLY in verification, never to retrieve candidates.
//! Hot path: yes — but amortized: each broad posting is walked ONCE PER BATCH,
//!   not once per title, and per-query verification is bitmap algebra.
//!
//! Today the broad lane is evaluated inline, per title: a hot anchor's huge
//! posting is re-scanned (and its candidates re-verified) for *every* title that
//! contains that feature. This module inverts the loop. For a batch of titles it
//! builds a per-batch inverted index (feature → bitmap-of-titles), collects the
//! broad queries reachable from the batch by probing each broad posting *once*,
//! and evaluates each reachable query with [`crate::exact::eval_batch_slices`]
//! (the bitmap transpose of `verify`). Pure-anchor queries — whose entire
//! semantics is their hot anchor — skip verification entirely and emit directly
//! from the anchor's title bitmap (the streaming-safe analog of "materialized
//! subscriptions").
//!
//! The selective lane (main index) is unchanged and still runs per title — it is
//! already fast and scale-flat. Only the broad lane is batched.

use super::snapshot::MatchView;
use super::{BaseSegment, BatchMatchOptions, BroadStrategy, MatchScratch, MatchStats, Segment};
use crate::dict::{FeatureId, NO_MASK_BIT};
use crate::storage::MmapSegment;
use crate::util::{fast_map, sig_key, FastMap};
use rayon::prelude::*;

/// Reusable scratch for the columnar broad pass — keeps the batch path
/// allocation-free in steady state (buffers are cleared, not freed, between
/// batches). One per rayon worker, sibling to [`MatchScratch`].
pub(in crate::segment) struct BroadBatchScratch {
    /// Per-batch inverted index: feature → row index into `feat_bits`.
    feat_row: FastMap<FeatureId, u32>,
    /// Flat title bitmaps, `words` u64 words per feature row (row `r` occupies
    /// `feat_bits[r*words .. (r+1)*words]`). Bit `t` set ⇔ batch-title `t` has
    /// the feature.
    feat_bits: Vec<u64>,
    /// Distinct features present in the batch (the keys of `feat_row`, in
    /// insertion order) — the set of broad anchors to probe.
    distinct: Vec<FeatureId>,
    /// Per-title common-mask word (the same `tmask` the scalar path computes).
    tmask_batch: Vec<u64>,
    /// Per-segment epoch-stamp dedup for reachable broad locals (base segments
    /// first, memtable last) — the broad twin of [`MatchScratch`]'s `seen`.
    broad_seen: Vec<Vec<u32>>,
    /// Monotonic epoch for `broad_seen` (bumped per segment; wraps reset all).
    broad_epoch: u32,
    /// Reachable broad locals for the current segment (scratch).
    cands: Vec<u32>,
    /// Reachable broad locals that need full bitmap verification (non pure-anchor).
    non_pure: Vec<u32>,
    /// Per-query match bitmap (`words` u64 words).
    acc: Vec<u64>,
    /// Per-any-of-group OR accumulator (`words` u64 words).
    grp: Vec<u64>,
}

impl BroadBatchScratch {
    pub(in crate::segment) fn new() -> Self {
        BroadBatchScratch {
            feat_row: fast_map(),
            feat_bits: Vec::new(),
            distinct: Vec::new(),
            tmask_batch: Vec::new(),
            broad_seen: Vec::new(),
            broad_epoch: 0,
            cands: Vec::new(),
            non_pure: Vec::new(),
            acc: Vec::new(),
            grp: Vec::new(),
        }
    }

    /// Size the per-segment dedup buffers and the per-query bitmaps. Reuses
    /// existing allocations (steady state: no-op).
    fn ensure(
        &mut self,
        segments: &[std::sync::Arc<BaseSegment>],
        memtable_len: usize,
        words: usize,
    ) {
        let n = segments.len() + 1;
        if self.broad_seen.len() < n {
            self.broad_seen.resize_with(n, Vec::new);
        }
        for (buf, seg) in self.broad_seen.iter_mut().zip(segments.iter()) {
            let len = seg.len();
            if buf.len() < len {
                buf.resize(len, 0);
            }
        }
        let mbuf = &mut self.broad_seen[segments.len()];
        if mbuf.len() < memtable_len {
            mbuf.resize(memtable_len, 0);
        }
        if self.acc.len() < words {
            self.acc.resize(words, 0);
        }
        if self.grp.len() < words {
            self.grp.resize(words, 0);
        }
    }
}

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

/// The per-segment broad surface, implemented by the in-memory [`Segment`] and
/// the file-backed [`MmapSegment`], so the columnar evaluator drives both with
/// one body (no drift). Method names are distinct from each backing's inherent
/// methods so delegation is unambiguous.
trait BroadBackend {
    /// Probe the broad index for `key` (after the anchor-filter check), appending
    /// reachable local IDs to `cands` (epoch-deduped via `seen`).
    fn reach(
        &self,
        key: u64,
        epoch: u32,
        seen: &mut [u32],
        cands: &mut Vec<u32>,
        stats: &mut MatchStats,
    );
    fn alive(&self, local: u32) -> bool;
    fn pure_anchor(&self, local: u32) -> bool;
    fn logical_id(&self, local: u32) -> u64;
    /// Write the matching-title bitmap for `local` into `acc` (bitmap transpose
    /// of `verify`); `grp` is reused scratch of the same width.
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
        if let Some(posting) = self.broad.get(key) {
            stats.postings_scanned += posting.len() as u32;
            stats.broad_postings_scanned += posting.len() as u32;
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
    fn pure_anchor(&self, local: u32) -> bool {
        self.exact.is_pure_anchor(local)
    }
    #[inline]
    fn logical_id(&self, local: u32) -> u64 {
        self.exact.logical(local)
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
    ) {
        self.exact.eval_batch(
            local,
            tmask_batch,
            lookup(feat_row, feat_bits, words),
            acc,
            grp,
        );
    }
}

impl BroadBackend for &MmapSegment {
    #[inline]
    fn reach(
        &self,
        key: u64,
        epoch: u32,
        seen: &mut [u32],
        cands: &mut Vec<u32>,
        stats: &mut MatchStats,
    ) {
        self.broad_reach(key, epoch, seen, cands, stats);
    }
    #[inline]
    fn alive(&self, local: u32) -> bool {
        self.is_alive_at(local)
    }
    #[inline]
    fn pure_anchor(&self, local: u32) -> bool {
        self.is_pure_anchor(local)
    }
    #[inline]
    fn logical_id(&self, local: u32) -> u64 {
        self.logical(local)
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
    ) {
        self.eval_batch(
            local,
            tmask_batch,
            lookup(feat_row, feat_bits, words),
            acc,
            grp,
        );
    }
}

/// Evaluate the broad lane of one segment against the whole batch, appending
/// matched logical IDs to each title's `outs[ti]`.
#[allow(clippy::too_many_arguments, clippy::needless_pass_by_value)]
fn eval_one_segment<B: BroadBackend>(
    backend: B,
    distinct: &[FeatureId],
    feat_row: &FastMap<FeatureId, u32>,
    feat_bits: &[u64],
    words: usize,
    tmask_batch: &[u64],
    seen: &mut [u32],
    epoch: u32,
    cands: &mut Vec<u32>,
    non_pure: &mut Vec<u32>,
    acc: &mut [u64],
    grp: &mut [u64],
    outs: &mut [Vec<u64>],
    materialize: bool,
    stats: &mut MatchStats,
) {
    cands.clear();
    non_pure.clear();
    stats.broad_anchors_scanned += distinct.len() as u32;

    // Reachability + pure-anchor emit, one probe per distinct batch feature.
    for &f in distinct {
        let key = sig_key(&[f]);
        let before = cands.len();
        backend.reach(key, epoch, seen, cands, stats);
        if cands.len() == before {
            continue;
        }
        // A pure-anchor query has exactly one broad sig (its anchor), so the
        // feature `f` that reached it IS its anchor: it matches exactly the
        // batch titles containing `f` — emit straight from `f`'s bitmap.
        let Some(&r) = feat_row.get(&f) else {
            continue;
        };
        let fbits = &feat_bits[r as usize * words..r as usize * words + words];
        for &local in &cands[before..] {
            stats.unique_candidates += 1;
            stats.broad_candidates += 1;
            if !backend.alive(local) {
                continue;
            }
            // Pure-anchor fast path: emit straight from the anchor bitmap. When
            // materialization is off, fall through to full verification — eval_into
            // on a pure-anchor query computes the same bitmap (its mask gate alone
            // selects exactly the titles containing the anchor), so results are
            // identical, just slower.
            if materialize && backend.pure_anchor(local) {
                let logical = backend.logical_id(local);
                for_each_set_bit(fbits, |ti| outs[ti].push(logical));
            } else {
                non_pure.push(local);
            }
        }
    }

    // Full bitmap verification for the rest.
    for &local in non_pure.iter() {
        stats.broad_queries_evaluated += 1;
        backend.eval_into(local, tmask_batch, feat_row, feat_bits, words, acc, grp);
        let logical = backend.logical_id(local);
        for_each_set_bit(acc, |ti| outs[ti].push(logical));
    }
}

/// Match one chunk of titles: selective lane per title (unchanged), broad lane
/// once over the chunk (columnar), merged into per-title `outs`.
fn match_batch_chunk(
    view: &MatchView,
    titles: &[impl AsRef<str>],
    opts: BatchMatchOptions,
    ms: &mut MatchScratch,
    bs: &mut BroadBatchScratch,
    outs: &mut Vec<Vec<u64>>,
    stats: &mut MatchStats,
) {
    let b = titles.len();
    if outs.len() < b {
        outs.resize_with(b, Vec::new);
    }
    for v in outs.iter_mut().take(b) {
        v.clear();
    }
    if b == 0 {
        return;
    }
    let words = b.div_ceil(64);
    let columnar = opts.include_broad && matches!(opts.broad_strategy, BroadStrategy::Columnar);
    let inline_broad = opts.include_broad && matches!(opts.broad_strategy, BroadStrategy::Inline);

    ms.ensure(view.segments, view.memtable.len());
    bs.ensure(view.segments, view.memtable.len(), words);
    bs.feat_row.clear();
    bs.feat_bits.clear();
    bs.distinct.clear();
    bs.tmask_batch.clear();

    let n_base = view.segments.len();

    // ---- Phase 0: per-title normalize + selective lane + build feat bitmaps ----
    for (ti, title) in titles.iter().enumerate() {
        // per-title epoch bump for the selective lane's cross-signature dedup
        ms.epoch = ms.epoch.wrapping_add(1);
        if ms.epoch == 0 {
            for buf in &mut ms.seen {
                for v in buf.iter_mut() {
                    *v = 0;
                }
            }
            ms.epoch = 1;
        }
        let epoch = ms.epoch;
        let out = &mut outs[ti];
        out.clear();

        // normalize once; take the buffer out so we can iterate it while mutating
        // ms.seen (no aliasing, no allocation) — same trick as match_title.
        view.norm
            .match_features(title.as_ref(), view.dict, &mut ms.lc, &mut ms.feats);
        let feats = std::mem::take(&mut ms.feats);
        let mut tmask = 0u64;
        for &f in &feats {
            let bit = view.dict.mask_bit(f);
            if bit != NO_MASK_BIT {
                tmask |= 1u64 << bit;
            }
        }

        for (i, base) in view.segments.iter().enumerate() {
            base.match_into(
                &feats,
                tmask,
                view.dict,
                epoch,
                &mut ms.seen[i],
                out,
                inline_broad,
                stats,
            );
        }
        view.memtable.match_into(
            &feats,
            tmask,
            view.dict,
            epoch,
            &mut ms.seen[n_base],
            out,
            inline_broad,
            stats,
        );

        bs.tmask_batch.push(tmask);
        if columnar {
            for &f in &feats {
                let row = if let Some(&r) = bs.feat_row.get(&f) {
                    r as usize
                } else {
                    let r = bs.feat_bits.len() / words;
                    bs.feat_bits.resize(bs.feat_bits.len() + words, 0);
                    bs.feat_row.insert(f, r as u32);
                    bs.distinct.push(f);
                    r
                };
                bs.feat_bits[row * words + (ti >> 6)] |= 1u64 << (ti & 63);
            }
        }

        ms.feats = feats; // restore the reusable buffer
        if !columnar {
            out.sort_unstable();
            out.dedup();
        }
    }

    if !columnar {
        return;
    }
    stats.broad_batches += 1;

    // ---- Phase 1+2: columnar broad lane, per segment ----
    let BroadBatchScratch {
        feat_row,
        feat_bits,
        distinct,
        tmask_batch,
        broad_seen,
        broad_epoch,
        cands,
        non_pure,
        acc,
        grp,
    } = bs;
    let acc: &mut [u64] = &mut acc[..words];
    let grp: &mut [u64] = &mut grp[..words];
    let materialize = opts.broad_materialize;

    for (si, base) in view.segments.iter().enumerate() {
        *broad_epoch = (*broad_epoch).wrapping_add(1);
        if *broad_epoch == 0 {
            for buf in broad_seen.iter_mut() {
                for v in buf.iter_mut() {
                    *v = 0;
                }
            }
            *broad_epoch = 1;
        }
        let epoch = *broad_epoch;
        let seen = &mut broad_seen[si];
        match base.as_ref() {
            BaseSegment::Memory(s) => eval_one_segment(
                s,
                distinct,
                feat_row,
                feat_bits,
                words,
                tmask_batch,
                seen,
                epoch,
                cands,
                non_pure,
                acc,
                grp,
                outs,
                materialize,
                stats,
            ),
            BaseSegment::Mmap(m) => eval_one_segment(
                m,
                distinct,
                feat_row,
                feat_bits,
                words,
                tmask_batch,
                seen,
                epoch,
                cands,
                non_pure,
                acc,
                grp,
                outs,
                materialize,
                stats,
            ),
        }
    }
    // memtable last (its broad_seen buffer is at index n_base)
    {
        *broad_epoch = (*broad_epoch).wrapping_add(1);
        if *broad_epoch == 0 {
            for buf in broad_seen.iter_mut() {
                for v in buf.iter_mut() {
                    *v = 0;
                }
            }
            *broad_epoch = 1;
        }
        let epoch = *broad_epoch;
        let seen = &mut broad_seen[n_base];
        eval_one_segment(
            view.memtable,
            distinct,
            feat_row,
            feat_bits,
            words,
            tmask_batch,
            seen,
            epoch,
            cands,
            non_pure,
            acc,
            grp,
            outs,
            materialize,
            stats,
        );
    }

    // ---- merge: dedup each title's matches across lanes + segments ----
    for v in outs.iter_mut().take(b) {
        v.sort_unstable();
        v.dedup();
    }
}

/// Sum two `MatchStats` field-by-field (for the parallel stats reduce).
fn add_stats(mut a: MatchStats, b: MatchStats) -> MatchStats {
    a.unique_candidates += b.unique_candidates;
    a.postings_scanned += b.postings_scanned;
    a.broad_postings_scanned += b.broad_postings_scanned;
    a.main_candidates += b.main_candidates;
    a.broad_candidates += b.broad_candidates;
    a.matches += b.matches;
    a.probes_attempted += b.probes_attempted;
    a.probes_skipped += b.probes_skipped;
    a.broad_queries_evaluated += b.broad_queries_evaluated;
    a.broad_anchors_scanned += b.broad_anchors_scanned;
    a.broad_batches += b.broad_batches;
    a
}

/// Batch match returning per-title `(global_index, matched_logical_ids)`. The
/// selective lane runs per title; the broad lane runs once per rayon chunk.
pub(in crate::segment) fn batch_results(
    view: &MatchView,
    titles: &[impl AsRef<str> + Sync],
    opts: BatchMatchOptions,
) -> Vec<(usize, Vec<u64>)> {
    batch_results_with_stats(view, titles, opts).0
}

/// Per-title batch results paired with an aggregate [`MatchStats`] — the return of
/// [`batch_results_with_stats`] and the per-chunk output it merges.
type BatchResults = (Vec<(usize, Vec<u64>)>, MatchStats);

/// Batch match returning per-title results AND the aggregate [`MatchStats`] in a
/// SINGLE pass — for callers (the HTTP `/_mpercolate` handler) that need both the
/// matches and the broad-lane meters without matching twice. `stats.matches` is
/// the total (query, title) match pairs across the batch.
pub(in crate::segment) fn batch_results_with_stats(
    view: &MatchView,
    titles: &[impl AsRef<str> + Sync],
    opts: BatchMatchOptions,
) -> BatchResults {
    let chunk = opts.broad_batch_size.max(1);
    let per_chunk: Vec<BatchResults> = titles
        .par_chunks(chunk)
        .enumerate()
        .map_init(
            || {
                (
                    MatchScratch::new(),
                    BroadBatchScratch::new(),
                    Vec::<Vec<u64>>::new(),
                )
            },
            |(ms, bs, outs), (ci, ct)| {
                let mut st = MatchStats::default();
                match_batch_chunk(view, ct, opts, ms, bs, outs, &mut st);
                let base = ci * chunk;
                let results: Vec<(usize, Vec<u64>)> = (0..ct.len())
                    .map(|ti| (base + ti, std::mem::take(&mut outs[ti])))
                    .collect();
                st.matches += results.iter().map(|(_, v)| v.len() as u32).sum::<u32>();
                (results, st)
            },
        )
        .collect();
    // Merge chunk outputs in order (the parallel matching above dominates; this
    // serial append + stats reduce is O(num_titles) pointer moves).
    let mut all = Vec::with_capacity(titles.len());
    let mut stats = MatchStats::default();
    for (mut chunk_results, st) in per_chunk {
        all.append(&mut chunk_results);
        stats = add_stats(stats, st);
    }
    (all, stats)
}

/// Batch match returning only aggregate [`MatchStats`] (for benchmarks).
pub(in crate::segment) fn batch_stats(
    view: &MatchView,
    titles: &[impl AsRef<str> + Sync],
    opts: BatchMatchOptions,
) -> MatchStats {
    let chunk = opts.broad_batch_size.max(1);
    titles
        .par_chunks(chunk)
        .map_init(
            || {
                (
                    MatchScratch::new(),
                    BroadBatchScratch::new(),
                    Vec::<Vec<u64>>::new(),
                )
            },
            |(ms, bs, outs), ct| {
                let mut st = MatchStats::default();
                match_batch_chunk(view, ct, opts, ms, bs, outs, &mut st);
                st.matches += outs
                    .iter()
                    .take(ct.len())
                    .map(|v| v.len() as u32)
                    .sum::<u32>();
                st
            },
        )
        .reduce(MatchStats::default, add_stats)
}
