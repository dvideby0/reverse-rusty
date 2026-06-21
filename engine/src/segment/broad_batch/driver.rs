//! The broad-lane batch driver — per-rayon-chunk matching + the public entry
//! points.
//!
//! Holds the reusable [`BroadBatchScratch`] (one per rayon worker), the
//! per-chunk `match_batch_chunk` (selective lane per title, columnar broad lane
//! once over the chunk), and the public `batch_results` / `batch_results_with_stats`
//! / `batch_stats` entry points the engine and snapshot call. The columnar broad
//! eval itself lives in [`super::kernel`].

use super::kernel::eval_one_segment;
use crate::dict::FeatureId;
use crate::segment::snapshot::MatchView;
use crate::segment::{BaseSegment, BatchMatchOptions, BroadStrategy, MatchScratch, MatchStats};
use crate::util::{fast_map, FastMap};
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
    // ADR-061: the columnar broad kernel is single-view, so while multi-word aliases are
    // active we route the broad lane through the two-view *inline* path (`match_into`) — the
    // documented kill-switch (matching.md §4) — keeping forbidden checks recall-correct.
    // Columnar two-view is a perf follow-on; the per-title selective lane is always two-view.
    let force_inline = view.norm.has_multiword_aliases();
    let columnar = opts.include_broad
        && !force_inline
        && matches!(opts.broad_strategy, BroadStrategy::Columnar);
    let inline_broad = opts.include_broad
        && (matches!(opts.broad_strategy, BroadStrategy::Inline) || (force_inline && !columnar));

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

        // normalize once. The default (no active multi-word alias) takes the **single-view fast
        // path** — one feature set + one mask, no second copy (ADR-061: zero-overhead default).
        // Only with multi-word aliases active (`force_inline`) do we build the canonical `N(T)` +
        // the overlapping superset `P(T)`. Take the buffers out so we can iterate them while
        // mutating ms.seen (no aliasing, no allocation) — same trick as match_title.
        let (feats, feats_pos);
        if force_inline {
            view.norm.match_features_dual(
                title.as_ref(),
                view.dict,
                &mut ms.lc,
                &mut ms.norm,
                &mut ms.feats,
                &mut ms.feats_pos,
            );
            feats = std::mem::take(&mut ms.feats);
            feats_pos = std::mem::take(&mut ms.feats_pos);
        } else {
            view.norm.match_features(
                title.as_ref(),
                view.dict,
                &mut ms.lc,
                &mut ms.norm,
                &mut ms.feats,
            );
            feats = std::mem::take(&mut ms.feats);
            feats_pos = Vec::new();
        }
        let neg_mask = view.title_mask(&feats);
        let tview = if force_inline {
            crate::exact::TitleView::dual(view.title_mask(&feats_pos), &feats_pos, neg_mask, &feats)
        } else {
            crate::exact::TitleView::single(neg_mask, &feats)
        };

        for (i, base) in view.segments.iter().enumerate() {
            base.match_into(
                &tview,
                view.dict,
                epoch,
                &mut ms.seen[i],
                out,
                inline_broad,
                view.pred,
                stats,
            );
        }
        view.memtable.match_into(
            &tview,
            view.dict,
            epoch,
            &mut ms.seen[n_base],
            out,
            inline_broad,
            view.pred,
            stats,
        );

        // The columnar broad kernel is single-view; it only runs when no multi-word alias is
        // active (`columnar` is forced off otherwise), so the canonical view == the superset
        // here and the inverted index + masks are built from `feats`.
        bs.tmask_batch.push(neg_mask);
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

        ms.feats = feats; // restore the reusable buffers (positive only when it was used)
        if force_inline {
            ms.feats_pos = feats_pos;
        }
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
                view.pred,
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
                view.pred,
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
            view.pred,
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
