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

/// Scorer-free bounded top-K state: the preallocated K-heap, its membership
/// set, the thresholded unique-total tracker, and the collection counters.
pub(crate) struct TopKState {
    k: usize,
    heap: BinaryHeap<HeapHit>,
    heap_ids: FastSet<u64>,
    winners: Vec<(u64, i64)>,
    totals: TotalTracker,
    emissions: u64,
    evaluations: u64,
    heap_replacements: u64,
}

impl TopKState {
    pub(crate) fn new(k: usize, total_threshold: usize) -> Self {
        let mut heap_ids = FastSet::default();
        heap_ids.reserve(k);
        Self {
            k,
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
    pub(crate) fn observe(&mut self, logical_id: u64, scorer: &mut impl FnMut(u64) -> i64) {
        self.emissions = self.emissions.saturating_add(1);
        self.totals.observe(logical_id);
        if self.k == 0 || self.heap_ids.contains(&logical_id) {
            return;
        }

        self.evaluations = self.evaluations.saturating_add(1);
        let hit = HeapHit {
            logical_id,
            score: scorer(logical_id),
        };
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

impl<F> TopKCollector<F>
where
    F: FnMut(u64) -> i64,
{
    pub(crate) fn new(k: usize, total_threshold: usize, scorer: F) -> Self {
        Self {
            state: TopKState::new(k, total_threshold),
            scorer,
        }
    }

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
    F: FnMut(u64) -> i64,
{
    #[inline]
    fn on_match(&mut self, logical_id: u64) {
        self.state.observe(logical_id, &mut self.scorer);
    }
}

impl<F> MatchCollector for TopKCollector<F>
where
    F: FnMut(u64) -> i64,
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
}

impl<F> BatchTopKCollector<F>
where
    F: FnMut(u64) -> i64,
{
    pub(crate) fn new(titles: usize, k: usize, total_threshold: usize, scorer: F) -> Self {
        Self {
            slots: (0..titles)
                .map(|_| TopKState::new(k, total_threshold))
                .collect(),
            scorer,
        }
    }

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
    F: FnMut(u64) -> i64,
{
    #[inline]
    fn on_match(&mut self, title_index: usize, logical_id: u64) {
        self.slots[title_index].observe(logical_id, &mut self.scorer);
    }
}

impl<F> BatchMatchCollector for BatchTopKCollector<F>
where
    F: FnMut(u64) -> i64,
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
mod tests {
    use super::*;

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
    }

    fn score(id: u64) -> i64 {
        ((id.wrapping_mul(0x9E37_79B9) ^ (id >> 3)) % 41) as i64 - 20
    }

    #[test]
    fn randomized_top_k_equals_collect_all_sort_and_truncate() {
        for seed in 1..=64u64 {
            let mut rng = Rng(seed);
            let stream: Vec<u64> = (0..2_000).map(|_| rng.next() % 317).collect();
            for &k in &[0usize, 1, 3, 10, 100, 1_000] {
                for &threshold in &[0usize, 1, 10, 100, 10_000] {
                    let mut collector = TopKCollector::new(k, threshold, score);
                    collector.reset();
                    for &id in &stream {
                        collector.on_match(id);
                        assert!(collector.state.heap.len() <= k);
                        assert!(collector.state.heap_ids.len() <= k);
                        assert!(
                            collector.state.totals.tracked_len() <= threshold.saturating_add(1)
                        );
                    }
                    let summary = collector.finish();

                    let mut expected_ids = stream.clone();
                    expected_ids.sort_unstable();
                    expected_ids.dedup();
                    let exact_total = expected_ids.len();
                    let mut expected: Vec<(u64, i64)> =
                        expected_ids.into_iter().map(|id| (id, score(id))).collect();
                    expected.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
                    expected.truncate(k);

                    assert_eq!(collector.winners(), expected);
                    assert_eq!(summary.retained, expected.len());
                    assert_eq!(summary.logical_emissions, stream.len() as u64);
                    let expected_total = if exact_total > threshold {
                        TotalHits::lower_bound(threshold as u64)
                    } else {
                        TotalHits::exact(exact_total as u64)
                    };
                    assert_eq!(summary.total_hits, expected_total);
                    assert_eq!(
                        summary.duplicate_emissions,
                        (exact_total <= threshold)
                            .then(|| stream.len() as u64 - exact_total as u64)
                    );
                }
            }
        }
    }

    /// The ADR-112 composition rule: N slots fed through one
    /// `BatchTopKCollector` must be indistinguishable from N independent
    /// `TopKCollector`s fed the same per-title streams.
    #[test]
    fn randomized_batch_top_k_equals_independent_single_collectors() {
        for seed in 1..=32u64 {
            let mut rng = Rng(seed);
            let titles = 1 + (rng.next() % 7) as usize;
            let streams: Vec<Vec<u64>> = (0..titles)
                .map(|_| (0..500).map(|_| rng.next() % 211).collect())
                .collect();
            for &k in &[0usize, 1, 5, 64] {
                for &threshold in &[0usize, 5, 10_000] {
                    let mut batch = BatchTopKCollector::new(titles, k, threshold, score);
                    // Interleave emissions across titles the way the columnar
                    // kernel does (title-major within a segment bit-block).
                    let longest = streams.iter().map(Vec::len).max().unwrap_or(0);
                    for round in 0..longest {
                        for (ti, stream) in streams.iter().enumerate() {
                            if let Some(&id) = stream.get(round) {
                                batch.on_match(ti, id);
                            }
                        }
                    }
                    let aggregate = batch.finish();

                    let mut retained = 0usize;
                    let mut value = 0u64;
                    let mut all_exact = true;
                    let mut emissions = 0u64;
                    for (ti, stream) in streams.iter().enumerate() {
                        let mut single = TopKCollector::new(k, threshold, score);
                        for &id in stream {
                            single.on_match(id);
                        }
                        let summary = single.finish();
                        assert_eq!(batch.slots()[ti].winners(), single.winners());
                        assert_eq!(batch.slots()[ti].total_hits(), single.total_hits());
                        assert_eq!(
                            batch.slots()[ti].rank_stats().evaluations,
                            single.rank_stats().evaluations
                        );
                        retained += summary.retained;
                        value += summary.total_hits.value;
                        all_exact &= summary.total_hits.relation == TotalHitsRelation::Eq;
                        emissions += summary.logical_emissions;
                    }
                    assert_eq!(aggregate.retained, retained);
                    assert_eq!(aggregate.total_hits.value, value);
                    assert_eq!(
                        aggregate.total_hits.relation,
                        if all_exact {
                            TotalHitsRelation::Eq
                        } else {
                            TotalHitsRelation::Gte
                        }
                    );
                    assert_eq!(aggregate.logical_emissions, emissions);
                }
            }
        }
    }

    #[test]
    fn batch_abort_clears_every_slot() {
        let mut batch = BatchTopKCollector::new(3, 4, 10, score);
        for ti in 0..3 {
            for id in 0..9u64 {
                batch.on_match(ti, id);
            }
        }
        batch.abort();
        for slot in batch.slots() {
            assert_eq!(slot.total_hits(), TotalHits::exact(0));
            assert!(slot.winners().is_empty());
            assert_eq!(slot.rank_stats().evaluations, 0);
        }
        // Reusable after abort: the slots collect again from clean state.
        batch.on_match(1, 42);
        let summary = batch.finish();
        assert_eq!(summary.retained, 1);
        assert_eq!(batch.slots()[1].winners(), &[(42, score(42))]);
    }
}
