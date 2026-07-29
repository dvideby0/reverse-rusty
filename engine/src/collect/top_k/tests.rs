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

fn score(_title_index: usize, id: u64) -> i64 {
    ((id.wrapping_mul(0x9E37_79B9) ^ (id >> 3)) % 41) as i64 - 20
}

#[test]
fn randomized_top_k_equals_collect_all_sort_and_truncate() {
    for seed in 1..=64u64 {
        let mut rng = Rng(seed);
        let stream: Vec<u64> = (0..2_000).map(|_| rng.next() % 317).collect();
        for &k in &[0usize, 1, 3, 10, 100, 1_000] {
            for &threshold in &[0usize, 1, 10, 100, 10_000] {
                let mut collector = TopKCollector::new(k, threshold, None, score);
                collector.reset();
                for &id in &stream {
                    collector.on_match(id);
                    assert!(collector.state.heap.len() <= k);
                    assert!(collector.state.heap_ids.len() <= k);
                    assert!(collector.state.totals.tracked_len() <= threshold.saturating_add(1));
                }
                let summary = collector.finish();

                let mut expected_ids = stream.clone();
                expected_ids.sort_unstable();
                expected_ids.dedup();
                let exact_total = expected_ids.len();
                let mut expected: Vec<(u64, i64)> = expected_ids
                    .into_iter()
                    .map(|id| (id, score(0, id)))
                    .collect();
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
                    (exact_total <= threshold).then(|| stream.len() as u64 - exact_total as u64)
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
                    let mut single = TopKCollector::new(k, threshold, None, score);
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

/// ADR-113: paging with successive `search_after` boundaries concatenates
/// to exactly the full ranked list — boundary-exclusive (no dup, no gap) —
/// and every page reports the same total as the boundary-free collection.
#[test]
fn paging_by_search_after_concatenates_to_the_full_ranked_list() {
    for seed in 1..=32u64 {
        let mut rng = Rng(seed);
        let stream: Vec<u64> = (0..1_500).map(|_| rng.next() % 271).collect();

        let mut expected_ids = stream.clone();
        expected_ids.sort_unstable();
        expected_ids.dedup();
        let mut expected: Vec<(u64, i64)> =
            expected_ids.iter().map(|&id| (id, score(0, id))).collect();
        expected.sort_unstable_by(|a, b| ranked_order((a.1, a.0), (b.1, b.0)));

        for &page in &[1usize, 3, 7, 50] {
            for &threshold in &[3usize, 10_000] {
                let mut baseline = TopKCollector::new(page, threshold, None, score);
                for &id in &stream {
                    baseline.on_match(id);
                }
                baseline.finish();
                let baseline_total = baseline.total_hits();

                let mut pages: Vec<(u64, i64)> = Vec::new();
                let mut after: Option<(i64, u64)> = None;
                loop {
                    let mut collector = TopKCollector::new(page, threshold, after, score);
                    for &id in &stream {
                        collector.on_match(id);
                    }
                    collector.finish();
                    assert_eq!(collector.total_hits(), baseline_total);
                    let winners = collector.winners().to_vec();
                    if let (Some(after), Some(first)) = (after, winners.first()) {
                        assert!(ranked_beats(after, (first.1, first.0)));
                    }
                    if winners.is_empty() {
                        break;
                    }
                    after = winners.last().map(|&(id, s)| (s, id));
                    let full_page = winners.len() == page;
                    pages.extend(winners);
                    if !full_page {
                        break;
                    }
                }
                assert_eq!(pages, expected);
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
    assert_eq!(batch.slots()[1].winners(), &[(42, score(1, 42))]);
}
