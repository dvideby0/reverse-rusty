//! ADR-112 bounded ranked batch driver: per-rayon-chunk [`BatchTopKCollector`]
//! runs through the same [`match_batch_chunk`](super::driver) body as the
//! compatibility path, so the selective/hot/broad lane structure — and the
//! columnar amortization — cannot diverge between the vector and ranked forms.

use std::time::Instant;

use rayon::prelude::*;

use super::driver::{match_batch_chunk, BroadBatchScratch};
use crate::collect::BatchTopKCollector;
use crate::ownership::BatchEmissionPolicy;
use crate::rank::RankStats;
use crate::result::TotalHits;
use crate::segment::snapshot::MatchView;
use crate::segment::{
    infallible, BatchMatchOptions, DeadlineAt, MatchCancelled, MatchScratch, MatchStats, NoDeadline,
};

/// One title's harvested bounded result: sorted `(logical_id, score)` winners,
/// its honest total, and its rank counters. Match statistics are batch
/// aggregate (the columnar pass cannot attribute them per title).
pub(in crate::segment) struct RankedSlot {
    pub(in crate::segment) hits: Vec<(u64, i64)>,
    pub(in crate::segment) total_hits: TotalHits,
    pub(in crate::segment) rank_stats: RankStats,
}

/// Per-chunk resident bound for the lazy per-title total trackers (codex
/// review): a chunk's collector can hold up to `chunk_len × (threshold + 1)`
/// tracked ids while the chunk is in flight, so the ranked path clamps its
/// chunk length to keep that product bounded regardless of the operator's
/// `broad_batch_size` knob. At the default threshold (10 000) the clamp is
/// ~419 titles — above the default 256-chunk, so default behavior is
/// unchanged; only adversarially large knob settings are bounded.
const RANKED_TRACKER_CHUNK_ROWS: usize = 1 << 22;

/// The ranked chunk length: the configured batch chunk, clamped by the
/// tracker-residency budget for this request's total threshold.
fn ranked_chunk_len(configured: usize, total_threshold: usize) -> usize {
    let tracker_rows = total_threshold.saturating_add(1);
    configured
        .max(1)
        .min((RANKED_TRACKER_CHUNK_ROWS / tracker_rows).max(1))
}

/// Bounded ranked batch match: per-title slots in request order + aggregate
/// stats. `policy_for(chunk_base, chunk_len)` builds each chunk's emission
/// policy over the SAME base the chunk's titles were sliced from — the
/// index-alignment rule that keeps per-title ownership from crossing titles.
#[allow(clippy::too_many_arguments)]
pub(in crate::segment) fn try_batch_top_k<P, F, T>(
    view: &MatchView,
    titles: &[T],
    opts: BatchMatchOptions,
    k: usize,
    total_threshold: usize,
    scorer: &F,
    policy_for: &(impl Fn(usize, usize) -> P + Sync),
    deadline: Option<Instant>,
) -> Result<(Vec<RankedSlot>, MatchStats), MatchCancelled>
where
    P: BatchEmissionPolicy,
    F: Fn(u64) -> i64 + Sync,
    T: AsRef<str> + Sync,
{
    let chunk = ranked_chunk_len(opts.broad_batch_size, total_threshold);
    let Some(d) = deadline else {
        let per_chunk: Vec<(Vec<RankedSlot>, MatchStats)> = titles
            .par_chunks(chunk)
            .enumerate()
            .map_init(
                || (MatchScratch::new(), BroadBatchScratch::new()),
                |(ms, bs), (ci, ct)| {
                    let mut st = MatchStats::default();
                    let mut collector =
                        BatchTopKCollector::new(ct.len(), k, total_threshold, scorer);
                    infallible(match_batch_chunk(
                        view,
                        ct,
                        opts,
                        ms,
                        bs,
                        &mut collector,
                        &mut st,
                        NoDeadline,
                        policy_for(ci * chunk, ct.len()),
                    ));
                    (harvest(&collector, &mut st), st)
                },
            )
            .collect();
        return Ok(merge_chunks(titles.len(), per_chunk));
    };
    let per_chunk: Vec<(Vec<RankedSlot>, MatchStats)> = titles
        .par_chunks(chunk)
        .enumerate()
        .map_init(
            || (MatchScratch::new(), BroadBatchScratch::new()),
            |(ms, bs), (ci, ct)| {
                let mut st = MatchStats::default();
                let mut collector = BatchTopKCollector::new(ct.len(), k, total_threshold, scorer);
                match_batch_chunk(
                    view,
                    ct,
                    opts,
                    ms,
                    bs,
                    &mut collector,
                    &mut st,
                    DeadlineAt(d),
                    policy_for(ci * chunk, ct.len()),
                )?;
                Ok((harvest(&collector, &mut st), st))
            },
        )
        .collect::<Result<Vec<_>, MatchCancelled>>()?;
    Ok(merge_chunks(titles.len(), per_chunk))
}

/// Read the finalized slots (the chunk body already ran `finish`, sorting each
/// slot's winners) and fold the per-title totals into `stats.matches` — the
/// batch analogue of the scalar `stats.matches = total_hits.value`.
fn harvest<F: FnMut(u64) -> i64>(
    collector: &BatchTopKCollector<F>,
    st: &mut MatchStats,
) -> Vec<RankedSlot> {
    collector
        .slots()
        .iter()
        .map(|slot| {
            let total_hits = slot.total_hits();
            st.matches = st
                .matches
                .saturating_add(u32::try_from(total_hits.value).unwrap_or(u32::MAX));
            RankedSlot {
                hits: slot.winners().to_vec(),
                total_hits,
                rank_stats: slot.rank_stats(),
            }
        })
        .collect()
}

/// Append the per-chunk slot lists in chunk order (par collect preserves it)
/// and merge stats through the ONE shared [`MatchStats::merge`] body.
fn merge_chunks(
    total_titles: usize,
    per_chunk: Vec<(Vec<RankedSlot>, MatchStats)>,
) -> (Vec<RankedSlot>, MatchStats) {
    let mut slots = Vec::with_capacity(total_titles);
    let mut stats = MatchStats::default();
    for (mut chunk_slots, st) in per_chunk {
        slots.append(&mut chunk_slots);
        stats.merge(st);
    }
    (slots, stats)
}

#[cfg(test)]
mod tests {
    use super::ranked_chunk_len;

    #[test]
    fn ranked_chunk_clamp_bounds_tracker_residency_without_touching_defaults() {
        // Default knob (256) at the default threshold stays unchanged.
        assert_eq!(ranked_chunk_len(256, 10_000), 256);
        // An adversarially large knob is clamped by the tracker budget.
        assert_eq!(ranked_chunk_len(10_000, 10_000), (1 << 22) / 10_001);
        // A tiny threshold leaves the knob in charge.
        assert_eq!(ranked_chunk_len(256, 0), 256);
        assert_eq!(ranked_chunk_len(0, 10_000), 1);
    }
}
