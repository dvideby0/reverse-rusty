//! Cooperative cancellation (ADR-099): the **proves-work-stopped** test.
//!
//! `timeout_ms` was a response deadline only — a timed-out match kept burning the
//! rayon pool to completion. These tests measure an uncancelled slow match (`T_full`)
//! over a deliberately broad-heavy corpus, then run the SAME match armed with a
//! deadline a fraction of `T_full` and assert the wall clock actually stopped early
//! (self-calibrating: every bound is relative to the measured `T_full`, so machine
//! speed cannot flake it — only a cancellation that fails to cancel can).

use crate::harness::*;
use reverse_rusty::exact::TagPredicate;
use std::time::Duration;

/// Manual before/after evidence for cancellation *inside* one deliberately
/// pathological segment. Half the rows share one canonical body (one enormous
/// body group); half have distinct negative clauses but the same positive
/// anchor (one enormous posting). The tag predicate rejects every row so the
/// measurement isolates traversal instead of result allocation/sorting.
///
/// Run with:
/// `cargo test --release --test stress cancellation::large_segment_overshoot_benchmark -- --ignored --nocapture`
#[test]
#[ignore = "manual cancellation-latency benchmark"]
fn large_segment_overshoot_benchmark() {
    const EACH: u64 = 300_000;
    let mut queries = Vec::with_capacity((EACH * 2) as usize);
    for id in 0..EACH {
        queries.push((id, "anchorw".to_string()));
    }
    for id in 0..EACH {
        queries.push((EACH + id, format!("anchorw -blocked{id}")));
    }

    let mut eng = Engine::new(make_norm());
    let report = eng.build_from_queries(&queries);
    assert_eq!(report.ingested as u64, EACH * 2);
    let snap = eng.snapshot();
    let pred = TagPredicate::new(vec![vec![0]]);
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();

    let started = Instant::now();
    let stats = snap.match_title_filtered("anchorw", &mut scratch, &mut out, true, &pred);
    let full = started.elapsed();
    assert!(out.is_empty());
    assert_eq!(stats.matches, 0);

    let budget = std::cmp::max(Duration::from_micros(100), full / 50);
    let started = Instant::now();
    let cancelled = snap.try_match_title_filtered(
        "anchorw",
        &mut scratch,
        &mut out,
        true,
        &pred,
        Some(Instant::now() + budget),
    );
    let elapsed = started.elapsed();
    assert!(cancelled.is_err());
    assert!(out.is_empty(), "cancelled traversal leaked partial results");
    assert!(
        elapsed < full / 4,
        "bounded polling did not materially reduce one-segment overshoot: \
         full={full:?}, budget={budget:?}, cancelled={elapsed:?}"
    );
    eprintln!(
        "large-segment cancellation: full={full:?}, budget={budget:?}, \
         cancelled={elapsed:?}, overshoot={:?}",
        elapsed.saturating_sub(budget)
    );
}

#[test]
fn armed_deadline_actually_stops_broad_batch_work() {
    eprintln!("\n=== COOPERATIVE CANCELLATION (batch, inline broad) ===");
    // Broad-heavy corpus + the INLINE broad strategy (per-title broad probes — the
    // slowest honest path) makes the uncancelled run comfortably measurable.
    let cfg = GenConfig {
        num_queries: 600_000,
        num_titles: 20_000,
        broad_query_frac: 0.8,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0xADC_099,
        num_entities: 3_000,
        num_collections: 1_200,
    };
    let data = generate(&cfg);
    let mut eng = Engine::new(make_norm());
    eng.build_from_queries(&data.queries);
    let snap = eng.snapshot();
    let pred = TagPredicate::empty();
    let opts = BatchMatchOptions {
        include_broad: true,
        broad_strategy: BroadStrategy::Inline,
        ..BatchMatchOptions::default()
    };

    // 1) the uncancelled baseline
    let t0 = Instant::now();
    let (full, _) = snap
        .try_match_titles_batch_with_stats_filtered(&data.titles, opts, &pred, None)
        .expect("unarmed never cancels");
    let t_full = t0.elapsed();
    eprintln!(
        "uncancelled: {:?} over {} titles ({} results rows)",
        t_full,
        data.titles.len(),
        full.len()
    );
    assert!(
        t_full > Duration::from_millis(100),
        "corpus too easy to prove anything (T_full={t_full:?}); grow it"
    );

    // 2) armed at T_full/20: must ERR and must stop well before the full runtime.
    //    Generous 4x margin over the budget absorbs scheduling jitter; the claim
    //    proven is "cancellation stops the work", not a precise latency.
    let budget = t_full / 20;
    let t1 = Instant::now();
    let r = snap.try_match_titles_batch_with_stats_filtered(
        &data.titles,
        opts,
        &pred,
        Some(Instant::now() + budget),
    );
    let elapsed = t1.elapsed();
    assert!(r.is_err(), "an armed sub-runtime deadline must cancel");
    assert!(
        elapsed < t_full / 4,
        "cancelled run took {elapsed:?} — not meaningfully less than T_full={t_full:?}; \
         the deadline checks are not stopping the work"
    );
    eprintln!("cancelled: {elapsed:?} (budget {budget:?}) — work stopped early ✓");
}

#[test]
fn armed_deadline_actually_stops_par_work() {
    eprintln!("\n=== COOPERATIVE CANCELLATION (parallel per-title) ===");
    let cfg = GenConfig {
        num_queries: 600_000,
        num_titles: 20_000,
        broad_query_frac: 0.8,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0xADC_099,
        num_entities: 3_000,
        num_collections: 1_200,
    };
    let data = generate(&cfg);
    let mut eng = Engine::new(make_norm());
    eng.build_from_queries(&data.queries);
    let snap = eng.snapshot();
    let pred = TagPredicate::empty();

    let t0 = Instant::now();
    let full = snap
        .try_match_titles_par_filtered(&data.titles, true, &pred, None)
        .expect("unarmed never cancels");
    let t_full = t0.elapsed();
    eprintln!(
        "uncancelled: {:?} over {} titles ({} rows)",
        t_full,
        data.titles.len(),
        full.len()
    );
    assert!(
        t_full > Duration::from_millis(100),
        "corpus too easy to prove anything (T_full={t_full:?}); grow it"
    );

    let t1 = Instant::now();
    let r = snap.try_match_titles_par_filtered(
        &data.titles,
        true,
        &pred,
        Some(Instant::now() + t_full / 20),
    );
    let elapsed = t1.elapsed();
    assert!(r.is_err(), "an armed sub-runtime deadline must cancel");
    assert!(
        elapsed < t_full / 4,
        "cancelled par run took {elapsed:?} vs T_full={t_full:?} — work did not stop"
    );
    eprintln!("cancelled: {elapsed:?} — work stopped early ✓");
}
