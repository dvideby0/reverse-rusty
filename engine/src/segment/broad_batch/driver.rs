//! The broad-lane batch driver — per-rayon-chunk matching + the public entry
//! points.
//!
//! Holds the reusable [`BroadBatchScratch`] (one per rayon worker), the
//! per-chunk `match_batch_chunk` (selective lane per title, columnar broad lane
//! once over the chunk), and the public `batch_results` / `batch_results_with_stats`
//! / `batch_stats` entry points the engine and snapshot call. The columnar broad
//! eval itself lives in [`super::kernel`].

use super::kernel::{eval_one_segment, Lane};
use crate::dict::FeatureId;
use crate::segment::snapshot::MatchView;
use crate::segment::{
    infallible, BaseSegment, BatchMatchOptions, BroadStrategy, DeadlineAt, DeadlineCheck,
    MatchCancelled, MatchScratch, MatchStats, NoDeadline,
};
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
/// Errs only under an armed cooperative deadline ([`DeadlineAt`], ADR-099) — checked at
/// each Phase-0 title boundary and each Phase-1/2 segment block, never per candidate.
/// On Err the chunk's `outs` are cleared (no partial escape); the unarmed monomorph
/// ([`NoDeadline`]) compiles the checks away.
#[allow(clippy::too_many_arguments)] // mirrors the scratch-threading style of eval_one_segment
fn match_batch_chunk<D: DeadlineCheck>(
    view: &MatchView,
    titles: &[impl AsRef<str>],
    opts: BatchMatchOptions,
    ms: &mut MatchScratch,
    bs: &mut BroadBatchScratch,
    outs: &mut Vec<Vec<u64>>,
    stats: &mut MatchStats,
    dl: D,
) -> Result<(), D::Cancelled> {
    let b = titles.len();
    if outs.len() < b {
        outs.resize_with(b, Vec::new);
    }
    for v in outs.iter_mut().take(b) {
        v.clear();
    }
    if b == 0 {
        return Ok(());
    }
    let words = b.div_ceil(64);
    // ADR-061: the columnar kernel is single-view, so while multi-word aliases are
    // active we route the broad lane through the two-view *inline* path (`match_into`) — the
    // documented kill-switch (matching.md §4) — keeping forbidden checks recall-correct.
    // Columnar two-view is a perf follow-on; the per-title selective lane is always two-view.
    let force_inline = view.norm.has_multiword_aliases();
    let columnar = opts.include_broad
        && !force_inline
        && matches!(opts.broad_strategy, BroadStrategy::Columnar);
    let inline_broad = opts.include_broad
        && (matches!(opts.broad_strategy, BroadStrategy::Inline) || (force_inline && !columnar));
    // The hot tier (class H, ADR-105) is ALWAYS evaluated — it is default-visible,
    // never `include_broad`-gated. The only question is WHERE: lifted into the
    // columnar pass below (the amortization the tier exists for), or inline in the
    // per-title `match_into` when columnar is unavailable (`BroadStrategy::Inline`
    // — the shared kill-switch — or the ADR-061 multi-word-alias two-view forcing).
    // Exactly one of the two runs, so no query is double-evaluated. Hot-free
    // corpora skip the lane entirely in both forms.
    let hot_present =
        view.segments.iter().any(|s| s.has_hot_entries()) || view.memtable.has_hot_entries();
    let hot_columnar =
        hot_present && !force_inline && matches!(opts.broad_strategy, BroadStrategy::Columnar);
    let hot_inline = hot_present && !hot_columnar;
    // Feature bitmaps are needed by EITHER columnar lane (the broad pass may be
    // off while the hot pass still runs — e.g. include_broad=false).
    let any_columnar = columnar || hot_columnar;

    ms.ensure(view.segments, view.memtable.len());
    bs.ensure(view.segments, view.memtable.len(), words);
    bs.feat_row.clear();
    bs.feat_bits.clear();
    bs.distinct.clear();
    bs.tmask_batch.clear();

    let n_base = view.segments.len();

    // OR of every batch title's common-mask word — the count-gate pre-reject's
    // one-AND clause (lever 5a). Folded for free while Phase 0 pushes tmasks.
    let mut batch_mask_union = 0u64;

    // ---- Phase 0: per-title normalize + selective lane + build feat bitmaps ----
    for (ti, title) in titles.iter().enumerate() {
        // Cooperative-deadline title boundary (ADR-099): clear the chunk's outputs
        // before abandoning so nothing partial can be read.
        if let Err(c) = dl.check() {
            for v in outs.iter_mut().take(b) {
                v.clear();
            }
            return Err(c);
        }
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

        let lanes = crate::segment::ProbeLanes {
            include_broad: inline_broad,
            include_hot: hot_inline,
        };
        for (i, base) in view.segments.iter().enumerate() {
            base.match_into(
                &tview,
                view.dict,
                epoch,
                &mut ms.seen[i],
                out,
                lanes,
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
            lanes,
            view.pred,
            stats,
        );

        // The columnar kernel is single-view; it only runs when no multi-word alias is
        // active (both `columnar` and `hot_columnar` are forced off otherwise), so the
        // canonical view == the superset here and the inverted index + masks are built
        // from `feats`.
        bs.tmask_batch.push(neg_mask);
        batch_mask_union |= neg_mask;
        if any_columnar {
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
    }

    if !any_columnar {
        finalize_delivery(outs, b, stats);
        return Ok(());
    }
    if columnar {
        stats.broad_batches += 1;
    }
    if hot_columnar {
        stats.hot_batches += 1;
    }

    // ---- Phase 1+2: columnar lanes (broad + hot), per segment ----
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
    let prefilter = opts.broad_prefilter;

    for (si, base) in view.segments.iter().enumerate() {
        // Cooperative-deadline segment boundary in the columnar pass (ADR-099).
        if let Err(c) = dl.check() {
            for v in outs.iter_mut().take(b) {
                v.clear();
            }
            return Err(c);
        }
        if columnar {
            let epoch = next_epoch(broad_epoch, broad_seen);
            eval_base_lane(
                base.as_ref(),
                Lane::Broad,
                distinct,
                feat_row,
                feat_bits,
                words,
                tmask_batch,
                batch_mask_union,
                &mut broad_seen[si],
                epoch,
                cands,
                non_pure,
                acc,
                grp,
                outs,
                materialize,
                prefilter,
                view.pred,
                stats,
            );
        }
        if hot_columnar && base.has_hot_entries() {
            let epoch = next_epoch(broad_epoch, broad_seen);
            eval_base_lane(
                base.as_ref(),
                Lane::Hot,
                distinct,
                feat_row,
                feat_bits,
                words,
                tmask_batch,
                batch_mask_union,
                &mut broad_seen[si],
                epoch,
                cands,
                non_pure,
                acc,
                grp,
                outs,
                materialize,
                prefilter,
                view.pred,
                stats,
            );
        }
    }
    // memtable last (its broad_seen buffer is at index n_base)
    {
        if let Err(c) = dl.check() {
            for v in outs.iter_mut().take(b) {
                v.clear();
            }
            return Err(c);
        }
        if columnar {
            let epoch = next_epoch(broad_epoch, broad_seen);
            eval_one_segment(
                view.memtable,
                Lane::Broad,
                distinct,
                feat_row,
                feat_bits,
                words,
                tmask_batch,
                batch_mask_union,
                &mut broad_seen[n_base],
                epoch,
                cands,
                non_pure,
                acc,
                grp,
                outs,
                materialize,
                prefilter,
                view.pred,
                stats,
            );
        }
        if hot_columnar && view.memtable.has_hot_entries() {
            let epoch = next_epoch(broad_epoch, broad_seen);
            eval_one_segment(
                view.memtable,
                Lane::Hot,
                distinct,
                feat_row,
                feat_bits,
                words,
                tmask_batch,
                batch_mask_union,
                &mut broad_seen[n_base],
                epoch,
                cands,
                non_pure,
                acc,
                grp,
                outs,
                materialize,
                prefilter,
                view.pred,
                stats,
            );
        }
    }

    // ---- merge: dedup each title's matches across lanes + segments ----
    finalize_delivery(outs, b, stats);
    Ok(())
}

/// Finalize each title's result collector and account for duplicate logical
/// emissions. Kept in one helper so inline and columnar batches cannot drift.
fn finalize_delivery(outs: &mut [Vec<u64>], titles: usize, stats: &mut MatchStats) {
    for v in outs.iter_mut().take(titles) {
        let emissions = v.len();
        v.sort_unstable();
        v.dedup();
        stats.record_delivery(emissions, v.len());
    }
}

/// Advance the shared per-segment dedup epoch (each lane pass gets its own —
/// the two lanes' locals are disjoint by the one-index-per-query invariant,
/// but a fresh epoch keeps each pass's dedup domain self-contained).
fn next_epoch(broad_epoch: &mut u32, broad_seen: &mut [Vec<u32>]) -> u32 {
    *broad_epoch = (*broad_epoch).wrapping_add(1);
    if *broad_epoch == 0 {
        for buf in broad_seen.iter_mut() {
            for v in buf.iter_mut() {
                *v = 0;
            }
        }
        *broad_epoch = 1;
    }
    *broad_epoch
}

/// Dispatch one columnar-lane evaluation over a [`BaseSegment`]'s two backings —
/// collapses the Memory/Mmap duplication at the two lane-call sites above.
#[allow(clippy::too_many_arguments)]
fn eval_base_lane(
    base: &BaseSegment,
    lane: Lane,
    distinct: &[FeatureId],
    feat_row: &crate::util::FastMap<FeatureId, u32>,
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
    match base {
        BaseSegment::Memory(s) => eval_one_segment(
            s,
            lane,
            distinct,
            feat_row,
            feat_bits,
            words,
            tmask_batch,
            batch_mask_union,
            seen,
            epoch,
            cands,
            non_pure,
            acc,
            grp,
            outs,
            materialize,
            prefilter,
            pred,
            stats,
        ),
        BaseSegment::Mmap(m) => eval_one_segment(
            m,
            lane,
            distinct,
            feat_row,
            feat_bits,
            words,
            tmask_batch,
            batch_mask_union,
            seen,
            epoch,
            cands,
            non_pure,
            acc,
            grp,
            outs,
            materialize,
            prefilter,
            pred,
            stats,
        ),
    }
}

/// Sum two `MatchStats` field-by-field (for the parallel stats reduce).
/// Delegates to [`MatchStats::merge`] — the ONE shared body — so a new field
/// cannot be silently dropped from one of the reduce sites (the ADR-101
/// under-count lesson).
fn add_stats(mut a: MatchStats, b: MatchStats) -> MatchStats {
    a.merge(b);
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
                infallible(match_batch_chunk(
                    view, ct, opts, ms, bs, outs, &mut st, NoDeadline,
                ));
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

/// [`batch_results_with_stats`] with an optional cooperative deadline (ADR-099).
/// `None` delegates to the unarmed path (byte-identical). Armed, each chunk's
/// [`match_batch_chunk`] checks per title + per segment block, and the `Result`
/// collect short-circuits: the FIRST cancelled chunk abandons the whole batch (rayon
/// stops scheduling remaining chunks best-effort; in-flight chunks self-cancel at
/// their next boundary). All-or-nothing — never a partially-filled result set.
pub(in crate::segment) fn try_batch_results_with_stats(
    view: &MatchView,
    titles: &[impl AsRef<str> + Sync],
    opts: BatchMatchOptions,
    deadline: Option<std::time::Instant>,
) -> Result<BatchResults, MatchCancelled> {
    let Some(d) = deadline else {
        return Ok(batch_results_with_stats(view, titles, opts));
    };
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
                match_batch_chunk(view, ct, opts, ms, bs, outs, &mut st, DeadlineAt(d))?;
                let base = ci * chunk;
                let results: Vec<(usize, Vec<u64>)> = (0..ct.len())
                    .map(|ti| (base + ti, std::mem::take(&mut outs[ti])))
                    .collect();
                st.matches += results.iter().map(|(_, v)| v.len() as u32).sum::<u32>();
                Ok((results, st))
            },
        )
        .collect::<Result<Vec<_>, MatchCancelled>>()?;
    let mut all = Vec::with_capacity(titles.len());
    let mut stats = MatchStats::default();
    for (mut chunk_results, st) in per_chunk {
        all.append(&mut chunk_results);
        stats = add_stats(stats, st);
    }
    Ok((all, stats))
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
                infallible(match_batch_chunk(
                    view, ct, opts, ms, bs, outs, &mut st, NoDeadline,
                ));
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
