//! Monomorphized post-verification result collectors (ADR-107).
//!
//! The matcher calls [`MatchSink::on_match`] only after Boolean verification and
//! member-level alive/tag checks. Collectors therefore cannot affect candidate
//! retrieval or the lossless signature cover.

use crate::result::{TotalHits, TotalHitsRelation};
use crate::util::FastSet;

mod chunk;
mod top_k;

pub(crate) use chunk::ChunkCollector;
pub(crate) use top_k::{BatchTopKCollector, TopKCollector};

/// Summary returned when a collector finalizes one exact matching pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CollectionSummary {
    pub(crate) retained: usize,
    pub(crate) total_hits: TotalHits,
    pub(crate) logical_emissions: u64,
    /// Exact only while the collector knows the exact unique total.
    pub(crate) duplicate_emissions: Option<u64>,
}

/// The single hot-path emission operation. Generic callers monomorphize it.
pub(crate) trait MatchSink {
    fn on_match(&mut self, logical_id: u64);

    /// Whether collection has failed or been cancelled and matching should
    /// return at the next candidate/probe boundary. Generic non-streaming
    /// collectors retain the statically-false default.
    #[inline]
    fn should_stop(&mut self) -> bool {
        false
    }

    /// Exhaustive delivery's physical-address callback (ADR-114). Existing
    /// collectors ignore the address through this default and retain their
    /// pre-ADR machine shape.
    #[inline]
    fn on_match_at(&mut self, logical_id: u64, _local_id: u32) {
        self.on_match(logical_id);
    }

    /// Identify which base segment (oldest-first) or memtable is about to emit.
    /// Used only by the bounded-memory exhaustive duplicate check.
    #[inline]
    fn begin_source(&mut self, _source: usize) {}
}

/// Lifecycle implemented by a complete single-title collector.
pub(crate) trait MatchCollector: MatchSink {
    fn reset(&mut self);
    fn finish(&mut self) -> CollectionSummary;
    fn abort(&mut self);
}

/// Compatibility collector over the caller's existing result vector.
pub(crate) struct AllCollector<'a> {
    out: &'a mut Vec<u64>,
    emissions: u64,
}

impl<'a> AllCollector<'a> {
    pub(crate) fn new(out: &'a mut Vec<u64>) -> Self {
        Self { out, emissions: 0 }
    }
}

impl MatchSink for AllCollector<'_> {
    #[inline]
    fn on_match(&mut self, logical_id: u64) {
        self.out.push(logical_id);
        self.emissions = self.emissions.saturating_add(1);
    }
}

impl MatchCollector for AllCollector<'_> {
    fn reset(&mut self) {
        self.out.clear();
        self.emissions = 0;
    }

    fn finish(&mut self) -> CollectionSummary {
        self.out.sort_unstable();
        self.out.dedup();
        let unique = u64::try_from(self.out.len()).unwrap_or(u64::MAX);
        CollectionSummary {
            retained: self.out.len(),
            total_hits: TotalHits::exact(unique),
            logical_emissions: self.emissions,
            duplicate_emissions: Some(self.emissions.saturating_sub(unique)),
        }
    }

    fn abort(&mut self) {
        self.reset();
    }
}

/// Append-only single-slot view used while a batch is still collecting lanes.
pub(crate) struct VecSink<'a> {
    out: &'a mut Vec<u64>,
    emissions: &'a mut u64,
}

impl<'a> VecSink<'a> {
    pub(crate) fn new(out: &'a mut Vec<u64>, emissions: &'a mut u64) -> Self {
        Self { out, emissions }
    }
}

impl MatchSink for VecSink<'_> {
    #[inline]
    fn on_match(&mut self, logical_id: u64) {
        self.out.push(logical_id);
        *self.emissions = (*self.emissions).saturating_add(1);
    }
}

/// Indexed collector seam for the columnar bitmap path.
pub(crate) trait BatchMatchSink {
    fn on_match(&mut self, title_index: usize, logical_id: u64);
}

/// Lifecycle implemented by a complete batch collector — the indexed analogue
/// of [`MatchCollector`], so the batch driver can finalize or abort without
/// knowing which collection policy it is running.
pub(crate) trait BatchMatchCollector: BatchMatchSink {
    fn finish(&mut self) -> CollectionSummary;
    fn abort(&mut self);
}

/// Reusable compatibility collector over one output vector per batch title.
pub(crate) struct AllBatchCollector<'a> {
    outs: &'a mut [Vec<u64>],
    emissions: &'a mut [u64],
}

impl<'a> AllBatchCollector<'a> {
    pub(crate) fn new(outs: &'a mut [Vec<u64>], emissions: &'a mut [u64]) -> Self {
        debug_assert_eq!(outs.len(), emissions.len());
        Self { outs, emissions }
    }
}

impl BatchMatchCollector for AllBatchCollector<'_> {
    fn finish(&mut self) -> CollectionSummary {
        let mut unique = 0u64;
        let mut emitted = 0u64;
        for (out, &count) in self.outs.iter_mut().zip(self.emissions.iter()) {
            out.sort_unstable();
            out.dedup();
            unique = unique.saturating_add(u64::try_from(out.len()).unwrap_or(u64::MAX));
            emitted = emitted.saturating_add(count);
        }
        CollectionSummary {
            retained: usize::try_from(unique).unwrap_or(usize::MAX),
            total_hits: TotalHits::exact(unique),
            logical_emissions: emitted,
            duplicate_emissions: Some(emitted.saturating_sub(unique)),
        }
    }

    fn abort(&mut self) {
        for out in self.outs.iter_mut() {
            out.clear();
        }
        for count in self.emissions.iter_mut() {
            *count = 0;
        }
    }
}

impl BatchMatchSink for AllBatchCollector<'_> {
    #[inline]
    fn on_match(&mut self, title_index: usize, logical_id: u64) {
        self.outs[title_index].push(logical_id);
        self.emissions[title_index] = self.emissions[title_index].saturating_add(1);
    }
}

/// Unique-total tracker whose resident set is capped at `threshold + 1`.
struct TotalTracker {
    threshold: usize,
    seen: FastSet<u64>,
    exceeded: bool,
}

impl TotalTracker {
    fn new(threshold: usize) -> Self {
        // Grow lazily: the typical selective title matches ~54 queries, so an
        // eager `reserve(threshold + 1)` (~150 KB at the default 10k threshold)
        // per collector bought nothing (review finding). `observe`'s threshold
        // cap still bounds the resident set, so rehash cost is bounded too.
        Self {
            threshold,
            seen: FastSet::default(),
            exceeded: false,
        }
    }

    fn reset(&mut self) {
        self.seen.clear();
        self.exceeded = false;
    }

    fn observe(&mut self, logical_id: u64) {
        if self.exceeded || self.seen.contains(&logical_id) {
            return;
        }
        self.seen.insert(logical_id);
        if self.seen.len() > self.threshold {
            self.exceeded = true;
        }
    }

    fn total_hits(&self) -> TotalHits {
        if self.exceeded {
            TotalHits::lower_bound(u64::try_from(self.threshold).unwrap_or(u64::MAX))
        } else {
            TotalHits::exact(u64::try_from(self.seen.len()).unwrap_or(u64::MAX))
        }
    }

    #[cfg(test)]
    fn tracked_len(&self) -> usize {
        self.seen.len()
    }
}

/// Count-only collector with bounded exact-total tracking.
pub(crate) struct CountCollector {
    totals: TotalTracker,
    emissions: u64,
}

impl CountCollector {
    pub(crate) fn new(threshold: usize) -> Self {
        Self {
            totals: TotalTracker::new(threshold),
            emissions: 0,
        }
    }
}

impl MatchSink for CountCollector {
    fn on_match(&mut self, logical_id: u64) {
        self.emissions = self.emissions.saturating_add(1);
        self.totals.observe(logical_id);
    }
}

impl MatchCollector for CountCollector {
    fn reset(&mut self) {
        self.totals.reset();
        self.emissions = 0;
    }

    fn finish(&mut self) -> CollectionSummary {
        let total_hits = self.totals.total_hits();
        CollectionSummary {
            retained: 0,
            total_hits,
            logical_emissions: self.emissions,
            duplicate_emissions: exact_duplicates(self.emissions, total_hits),
        }
    }

    fn abort(&mut self) {
        self.reset();
    }
}

fn exact_duplicates(emissions: u64, total_hits: TotalHits) -> Option<u64> {
    (total_hits.relation == TotalHitsRelation::Eq)
        .then(|| emissions.saturating_sub(total_hits.value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_collector_is_bounded_and_abort_discards_state() {
        let mut collector = CountCollector::new(3);
        collector.reset();
        for id in [9, 9, 8, 7, 6, 5] {
            collector.on_match(id);
        }
        assert_eq!(collector.totals.tracked_len(), 4);
        assert_eq!(collector.finish().total_hits, TotalHits::lower_bound(3));
        collector.abort();
        assert_eq!(collector.emissions, 0);
        assert_eq!(collector.totals.tracked_len(), 0);
    }

    #[test]
    fn all_collector_preserves_sorted_deduped_compatibility_order() {
        let mut out = vec![99];
        let mut collector = AllCollector::new(&mut out);
        collector.reset();
        for id in [7, 2, 7, 9, 2] {
            collector.on_match(id);
        }
        let summary = collector.finish();
        assert_eq!(out, vec![2, 7, 9]);
        assert_eq!(summary.total_hits, TotalHits::exact(3));
        assert_eq!(summary.logical_emissions, 5);
        assert_eq!(summary.duplicate_emissions, Some(2));
    }
}
