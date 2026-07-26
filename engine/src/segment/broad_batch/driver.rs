//! The broad-lane batch driver — per-rayon-chunk matching + the public entry
//! points.
//!
//! Holds the reusable [`BroadBatchScratch`] (one per rayon worker), the
//! per-chunk `match_batch_chunk` (selective lane per title, columnar broad lane
//! once over the chunk), and the public `batch_results` / `batch_results_with_stats`
//! / `batch_stats` entry points the engine and snapshot call. The columnar broad
//! eval itself lives in [`super::kernel`].

use super::kernel::{eval_one_segment, Lane};
use crate::collect::{
    AllBatchCollector, BatchMatchCollector, BatchMatchSink, CollectionSummary, MatchSink,
};
use crate::dict::FeatureId;
use crate::ownership::{BatchEmissionPolicy, EmitAll};
use crate::segment::snapshot::MatchView;
use crate::segment::{
    infallible, BaseSegment, BatchMatchOptions, BroadStrategy, DeadlineAt, DeadlineCheck,
    DeadlinePoll, MatchCancelled, MatchScratch, MatchStats, NoDeadline,
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
    /// Per-compound-member AND accumulator.
    member: Vec<u64>,
    /// Per-compound-requirement alternatives OR accumulator.
    choice: Vec<u64>,
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
            member: Vec::new(),
            choice: Vec::new(),
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
        if self.member.len() < words {
            self.member.resize(words, 0);
        }
        if self.choice.len() < words {
            self.choice.resize(words, 0);
        }
    }
}

/// One per-title [`MatchSink`] view over the chunk's indexed batch collector —
/// what lets the Phase-0 scalar lanes and the columnar lanes feed ONE
/// collection policy. For the compatibility `AllBatchCollector` monomorph this
/// inlines to the same per-title vector push the old `VecSink` did.
struct IndexedTitleSink<'a, C> {
    collector: &'a mut C,
    title_index: usize,
}

impl<C: BatchMatchSink> MatchSink for IndexedTitleSink<'_, C> {
    #[inline]
    fn on_match(&mut self, logical_id: u64) {
        self.collector.on_match(self.title_index, logical_id);
    }

    #[inline]
    fn on_match_at_with_poll(
        &mut self,
        logical_id: u64,
        _local_id: u32,
        should_stop: &mut dyn FnMut() -> bool,
    ) {
        self.collector
            .on_match_with_poll(self.title_index, logical_id, should_stop);
    }
}

mod chunk;
pub(super) use chunk::match_batch_chunk;

fn record_collection(stats: &mut MatchStats, summary: CollectionSummary) {
    stats.logical_emissions = stats
        .logical_emissions
        .saturating_add(summary.logical_emissions);
    stats.duplicate_emissions = stats
        .duplicate_emissions
        .saturating_add(summary.duplicate_emissions.unwrap_or(0));
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
fn eval_base_lane<S: BatchMatchSink, P: BatchEmissionPolicy, D: DeadlineCheck>(
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
    member: &mut [u64],
    choice: &mut [u64],
    collector: &mut S,
    materialize: bool,
    prefilter: bool,
    pred: &crate::exact::TagPredicate,
    stats: &mut MatchStats,
    policy: P,
    deadline: &mut DeadlinePoll<D>,
) -> Result<(), D::Cancelled> {
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
            member,
            choice,
            collector,
            materialize,
            prefilter,
            pred,
            stats,
            policy,
            deadline,
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
            member,
            choice,
            collector,
            materialize,
            prefilter,
            pred,
            stats,
            policy,
            deadline,
        ),
    }
}

/// Size + clear the per-chunk compatibility output buffers (per-title match
/// vectors + delivery-emission counters) the `AllBatchCollector` borrows.
fn prepare_outs(outs: &mut Vec<Vec<u64>>, emissions: &mut Vec<u64>, b: usize) {
    if outs.len() < b {
        outs.resize_with(b, Vec::new);
    }
    for v in outs.iter_mut().take(b) {
        v.clear();
    }
    if emissions.len() < b {
        emissions.resize(b, 0);
    }
    for count in emissions.iter_mut().take(b) {
        *count = 0;
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
                    Vec::<u64>::new(),
                )
            },
            |(ms, bs, outs, emissions), (ci, ct)| {
                let mut st = MatchStats::default();
                let b = ct.len();
                prepare_outs(outs, emissions, b);
                {
                    let mut collector = AllBatchCollector::new(&mut outs[..b], &mut emissions[..b]);
                    infallible(match_batch_chunk(
                        view,
                        ct,
                        opts,
                        ms,
                        bs,
                        &mut collector,
                        &mut st,
                        NoDeadline,
                        EmitAll,
                    ));
                }
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

/// [`batch_results_with_stats`] with an optional cooperative deadline
/// (ADR-099/123). `None` delegates to the unarmed path (byte-identical). Armed,
/// each chunk's [`match_batch_chunk`] checks per title/segment block and within
/// bounded runs of columnar work; the `Result` collect short-circuits: the FIRST
/// cancelled chunk abandons the whole batch (rayon stops scheduling remaining
/// chunks best-effort; in-flight chunks self-cancel at their next poll).
/// All-or-nothing — never a partially-filled result set.
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
                    Vec::<u64>::new(),
                )
            },
            |(ms, bs, outs, emissions), (ci, ct)| {
                let mut st = MatchStats::default();
                let b = ct.len();
                prepare_outs(outs, emissions, b);
                {
                    let mut collector = AllBatchCollector::new(&mut outs[..b], &mut emissions[..b]);
                    match_batch_chunk(
                        view,
                        ct,
                        opts,
                        ms,
                        bs,
                        &mut collector,
                        &mut st,
                        DeadlineAt(d),
                        EmitAll,
                    )?;
                }
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
                    Vec::<u64>::new(),
                )
            },
            |(ms, bs, outs, emissions), ct| {
                let mut st = MatchStats::default();
                let b = ct.len();
                prepare_outs(outs, emissions, b);
                {
                    let mut collector = AllBatchCollector::new(&mut outs[..b], &mut emissions[..b]);
                    infallible(match_batch_chunk(
                        view,
                        ct,
                        opts,
                        ms,
                        bs,
                        &mut collector,
                        &mut st,
                        NoDeadline,
                        EmitAll,
                    ));
                }
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

#[cfg(test)]
mod tests;
