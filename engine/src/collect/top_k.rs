//! Bounded exact top-K collection (ADR-107/108) — the single-title collector
//! and its per-title batch composition (ADR-112).
//!
//! `TopKState` is the scorer-free K-heap + total tracker; `TopKCollector`
//! (one title, owns the scorer) and `BatchTopKCollector` (one slot per batch
//! title, ONE shared scorer) are thin compositions over it, so the bounded
//! collection rule cannot fork between the scalar and batch paths.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::result::{ranked_beats, ranked_order, TotalHits, TotalHitsRelation};
use crate::util::FastSet;

use super::{
    exact_duplicates, BatchMatchCollector, BatchMatchSink, CollectionSummary, MatchCollector,
    MatchSink, TotalTracker,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HeapHit {
    logical_id: u64,
    score: i64,
}

// BinaryHeap keeps its greatest value at the root. Under `ranked_order`,
// "precedes" is Less, so the max-heap root is always the current worst winner.
impl Ord for HeapHit {
    fn cmp(&self, other: &Self) -> Ordering {
        ranked_order(
            (self.score, self.logical_id),
            (other.score, other.logical_id),
        )
    }
}

impl PartialOrd for HeapHit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn better(candidate: HeapHit, worst: HeapHit) -> bool {
    ranked_beats(
        (candidate.score, candidate.logical_id),
        (worst.score, worst.logical_id),
    )
}

/// Scoring policy behind the bounded collectors. The plain policy ignores the
/// poll hook entirely; the polling policy may stop an unbounded metadata walk
/// and returns `None` only when that hook fired.
pub(crate) trait TopKScorer {
    fn score(
        &mut self,
        title_index: usize,
        logical_id: u64,
        should_stop: &mut dyn FnMut() -> bool,
    ) -> Option<i64>;
}

pub(crate) struct PlainScorer<F>(F);

impl<F> TopKScorer for PlainScorer<F>
where
    F: FnMut(usize, u64) -> i64,
{
    #[inline]
    fn score(
        &mut self,
        title_index: usize,
        logical_id: u64,
        _should_stop: &mut dyn FnMut() -> bool,
    ) -> Option<i64> {
        Some((self.0)(title_index, logical_id))
    }
}

pub(crate) struct PollingScorer<F>(F);

impl<F> TopKScorer for PollingScorer<F>
where
    F: FnMut(usize, u64, &mut dyn FnMut() -> bool) -> Option<i64>,
{
    #[inline]
    fn score(
        &mut self,
        title_index: usize,
        logical_id: u64,
        should_stop: &mut dyn FnMut() -> bool,
    ) -> Option<i64> {
        (self.0)(title_index, logical_id, should_stop)
    }
}

/// Scorer-free bounded top-K state: the preallocated K-heap, its membership
/// set, the thresholded unique-total tracker, and the collection counters.
pub(crate) struct TopKState {
    k: usize,
    /// ADR-113 exclusive pagination boundary: rows NOT strictly after it under
    /// `ranked_order` are counted (totals/emissions) but never retained.
    after: Option<(i64, u64)>,
    heap: BinaryHeap<HeapHit>,
    heap_ids: FastSet<u64>,
    winners: Vec<(u64, i64)>,
    totals: TotalTracker,
    emissions: u64,
    evaluations: u64,
    heap_replacements: u64,
}

impl TopKState {
    pub(crate) fn new(k: usize, total_threshold: usize, after: Option<(i64, u64)>) -> Self {
        let mut heap_ids = FastSet::default();
        heap_ids.reserve(k);
        Self {
            k,
            after,
            heap: BinaryHeap::with_capacity(k),
            heap_ids,
            winners: Vec::with_capacity(k),
            totals: TotalTracker::new(total_threshold),
            emissions: 0,
            evaluations: 0,
            heap_replacements: 0,
        }
    }

    /// One verified emission: count it, then score-and-retain under the K
    /// bound. The scorer is borrowed per call so one scorer can serve many
    /// states (the batch composition) without duplicating this rule.
    #[inline]
    pub(crate) fn observe(
        &mut self,
        title_index: usize,
        logical_id: u64,
        scorer: &mut impl TopKScorer,
        should_stop: &mut dyn FnMut() -> bool,
    ) {
        self.emissions = self.emissions.saturating_add(1);
        self.totals.observe(logical_id);
        if self.k == 0 || self.heap_ids.contains(&logical_id) {
            return;
        }

        self.evaluations = self.evaluations.saturating_add(1);
        let Some(score) = scorer.score(title_index, logical_id, should_stop) else {
            return;
        };
        let hit = HeapHit { logical_id, score };
        // Strictly-after gate: the boundary row itself is excluded, so a page
        // whose last row becomes the next boundary yields no dup and no gap.
        // (`evaluations` legitimately counts boundary-skipped rows — the
        // scorer ran; page oracles compare winners + totals only.)
        if let Some(after) = self.after {
            if !ranked_beats(after, (hit.score, hit.logical_id)) {
                return;
            }
        }
        if self.heap.len() < self.k {
            self.heap.push(hit);
            self.heap_ids.insert(logical_id);
            return;
        }

        let replace = self.heap.peek().is_some_and(|worst| better(hit, *worst));
        if replace {
            self.heap_replacements = self.heap_replacements.saturating_add(1);
            if let Some(removed) = self.heap.pop() {
                self.heap_ids.remove(&removed.logical_id);
            }
            self.heap.push(hit);
            self.heap_ids.insert(logical_id);
        }
    }

    pub(crate) fn reset(&mut self) {
        self.heap.clear();
        self.heap_ids.clear();
        self.winners.clear();
        self.totals.reset();
        self.emissions = 0;
        self.evaluations = 0;
        self.heap_replacements = 0;
    }

    /// Drain the heap into the sorted winner list and summarize.
    pub(crate) fn finish_summary(&mut self) -> CollectionSummary {
        self.winners.clear();
        self.winners
            .extend(self.heap.drain().map(|hit| (hit.logical_id, hit.score)));
        self.heap_ids.clear();
        self.winners
            .sort_unstable_by(|a, b| ranked_order((a.1, a.0), (b.1, b.0)));
        let total_hits = self.totals.total_hits();
        CollectionSummary {
            retained: self.winners.len(),
            total_hits,
            logical_emissions: self.emissions,
            duplicate_emissions: exact_duplicates(self.emissions, total_hits),
        }
    }

    pub(crate) fn winners(&self) -> &[(u64, i64)] {
        &self.winners
    }

    pub(crate) fn rank_stats(&self) -> crate::rank::RankStats {
        crate::rank::RankStats {
            evaluations: self.evaluations,
            heap_replacements: self.heap_replacements,
        }
    }

    pub(crate) fn total_hits(&self) -> TotalHits {
        self.totals.total_hits()
    }
}

/// Bounded exact top-K collector used by local ranked percolation and its oracle.
pub(crate) struct TopKCollector<F> {
    state: TopKState,
    scorer: F,
}

impl<F> TopKCollector<PlainScorer<F>>
where
    F: FnMut(usize, u64) -> i64,
{
    pub(crate) fn new(
        k: usize,
        total_threshold: usize,
        after: Option<(i64, u64)>,
        scorer: F,
    ) -> Self {
        Self {
            state: TopKState::new(k, total_threshold, after),
            scorer: PlainScorer(scorer),
        }
    }
}

impl<F> TopKCollector<PollingScorer<F>>
where
    F: FnMut(usize, u64, &mut dyn FnMut() -> bool) -> Option<i64>,
{
    pub(crate) fn new_polling(
        k: usize,
        total_threshold: usize,
        after: Option<(i64, u64)>,
        scorer: F,
    ) -> Self {
        Self {
            state: TopKState::new(k, total_threshold, after),
            scorer: PollingScorer(scorer),
        }
    }
}

impl<F> TopKCollector<F> {
    pub(crate) fn winners(&self) -> &[(u64, i64)] {
        self.state.winners()
    }

    pub(crate) fn rank_stats(&self) -> crate::rank::RankStats {
        self.state.rank_stats()
    }

    pub(crate) fn total_hits(&self) -> TotalHits {
        self.state.total_hits()
    }
}

impl<F> MatchSink for TopKCollector<F>
where
    F: TopKScorer,
{
    #[inline]
    fn on_match(&mut self, logical_id: u64) {
        self.state
            .observe(0, logical_id, &mut self.scorer, &mut || false);
    }

    #[inline]
    fn on_match_at_with_poll(
        &mut self,
        logical_id: u64,
        _local_id: u32,
        should_stop: &mut dyn FnMut() -> bool,
    ) {
        self.state
            .observe(0, logical_id, &mut self.scorer, should_stop);
    }
}

impl<F> MatchCollector for TopKCollector<F>
where
    F: TopKScorer,
{
    fn reset(&mut self) {
        self.state.reset();
    }

    fn finish(&mut self) -> CollectionSummary {
        self.state.finish_summary()
    }

    fn abort(&mut self) {
        self.state.reset();
    }
}

/// Per-title bounded top-K over the indexed batch seam (ADR-112): one
/// [`TopKState`] slot per batch title, ONE shared scorer (the rank program is
/// per-request, not per-title, so scores cannot diverge across slots).
pub(crate) struct BatchTopKCollector<F> {
    slots: Vec<TopKState>,
    scorer: F,
    title_base: usize,
}

impl<F> BatchTopKCollector<PlainScorer<F>>
where
    F: FnMut(usize, u64) -> i64,
{
    pub(crate) fn new(titles: usize, k: usize, total_threshold: usize, scorer: F) -> Self {
        Self::new_with_base(titles, k, total_threshold, 0, scorer)
    }

    pub(crate) fn new_with_base(
        titles: usize,
        k: usize,
        total_threshold: usize,
        title_base: usize,
        scorer: F,
    ) -> Self {
        Self {
            // Batch slots never carry a pagination boundary: `search_after`
            // is a single-title cursor primitive and batch admission rejects
            // it loudly upstream (ADR-113).
            slots: (0..titles)
                .map(|_| TopKState::new(k, total_threshold, None))
                .collect(),
            scorer: PlainScorer(scorer),
            title_base,
        }
    }
}

impl<F> BatchTopKCollector<PollingScorer<F>>
where
    F: FnMut(usize, u64, &mut dyn FnMut() -> bool) -> Option<i64>,
{
    pub(crate) fn new_polling(titles: usize, k: usize, total_threshold: usize, scorer: F) -> Self {
        Self::new_polling_with_base(titles, k, total_threshold, 0, scorer)
    }

    pub(crate) fn new_polling_with_base(
        titles: usize,
        k: usize,
        total_threshold: usize,
        title_base: usize,
        scorer: F,
    ) -> Self {
        Self {
            slots: (0..titles)
                .map(|_| TopKState::new(k, total_threshold, None))
                .collect(),
            scorer: PollingScorer(scorer),
            title_base,
        }
    }
}

impl<F> BatchTopKCollector<F> {
    /// Finalize every slot (sorting its winners) — call before reading
    /// per-title results; [`BatchMatchCollector::finish`] does this as part of
    /// producing the aggregate summary.
    pub(crate) fn slots_mut(&mut self) -> &mut [TopKState] {
        &mut self.slots
    }

    pub(crate) fn slots(&self) -> &[TopKState] {
        &self.slots
    }
}

impl<F> BatchMatchSink for BatchTopKCollector<F>
where
    F: TopKScorer,
{
    #[inline]
    fn on_match(&mut self, title_index: usize, logical_id: u64) {
        self.slots[title_index].observe(
            self.title_base.saturating_add(title_index),
            logical_id,
            &mut self.scorer,
            &mut || false,
        );
    }

    #[inline]
    fn on_match_with_poll(
        &mut self,
        title_index: usize,
        logical_id: u64,
        should_stop: &mut dyn FnMut() -> bool,
    ) {
        self.slots[title_index].observe(
            self.title_base.saturating_add(title_index),
            logical_id,
            &mut self.scorer,
            should_stop,
        );
    }
}

impl<F> BatchMatchCollector for BatchTopKCollector<F>
where
    F: TopKScorer,
{
    /// Aggregate summary across slots: the total value is the saturating sum
    /// of per-title totals, exact only while EVERY slot's total is exact —
    /// the same rule the coordinator applies when merging shard totals.
    fn finish(&mut self) -> CollectionSummary {
        let mut retained = 0usize;
        let mut value = 0u64;
        let mut all_exact = true;
        let mut emissions = 0u64;
        for slot in &mut self.slots {
            let summary = slot.finish_summary();
            retained = retained.saturating_add(summary.retained);
            value = value.saturating_add(summary.total_hits.value);
            all_exact &= summary.total_hits.relation == TotalHitsRelation::Eq;
            emissions = emissions.saturating_add(summary.logical_emissions);
        }
        let total_hits = if all_exact {
            TotalHits::exact(value)
        } else {
            TotalHits::lower_bound(value)
        };
        CollectionSummary {
            retained,
            total_hits,
            logical_emissions: emissions,
            duplicate_emissions: exact_duplicates(emissions, total_hits),
        }
    }

    fn abort(&mut self) {
        for slot in &mut self.slots {
            slot.reset();
        }
    }
}

#[cfg(test)]
mod tests;
