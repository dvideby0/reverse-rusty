//! Parallel-read workloads under mutation + broad-lane batch equivalence.

use crate::harness::*;

// ═════════════════════════════════════════════════════════════════════════════
// 2. BURST WRITE + CONCURRENT READ — multi-threaded reads via rayon
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn burst_writes_with_parallel_reads() {
    eprintln!("\n=== BURST WRITES + PARALLEL READS ===");
    let t0 = Instant::now();

    let cfg = GenConfig {
        num_queries: 30_000,
        num_titles: 3_000,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0xBE_EF_CA_FE,
        num_players: 3_000,
        num_sets: 1_200,
    };
    let data = generate(&cfg);
    let q = &data.queries;
    let chunk = q.len() / 6;

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 5_000,
            auto_compact_on_flush: true,
            max_segments: 4,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    // ── Load initial batch ──
    eprintln!("  Loading initial {chunk} queries");
    eng.build_from_queries(&q[..chunk]);

    // ── Burst: bulk_ingest 3 batches, read between each ──
    for wave in 0..3 {
        let lo = (wave + 1) * chunk;
        let hi = ((wave + 2) * chunk).min(q.len());
        eprintln!("\n  Wave {}: bulk_ingest {} queries", wave, hi - lo);
        eng.bulk_ingest(&q[lo..hi]);
        print_metrics(&format!("wave-{wave}"), &eng.metrics());

        // Parallel read sweep between writes
        let par_results = eng.match_titles_par(&data.titles, true);
        let total: usize = par_results.iter().map(|(_, ids, _)| ids.len()).sum();
        eprintln!(
            "    parallel read: {} total matches across {} titles",
            total,
            data.titles.len()
        );
    }

    // ── Remaining queries as live inserts ──
    let live_start = 4 * chunk;
    eprintln!("\n  Streaming {} live inserts", q.len() - live_start);
    for (logical, text) in &q[live_start..] {
        eng.insert_live(text, *logical, 1);
    }

    // ── Delete every 10th query ──
    let delete_targets: Vec<u64> = q.iter().step_by(10).map(|(id, _)| *id).collect();
    eprintln!("  Deleting {} queries", delete_targets.len());
    for id in &delete_targets {
        let _ = eng.delete_by_logical_id(*id);
    }

    // ── Flush + compact ──
    eng.flush();
    eng.compact_all();
    print_metrics("final", &eng.metrics());

    // ── Parallel read: verify sequential == parallel ──
    eprintln!("\n  Verifying parallel == sequential...");
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut sequential: Vec<HashSet<u64>> = Vec::new();
    for title in &data.titles {
        eng.match_title(title, &mut scratch, &mut out, true);
        sequential.push(out.iter().copied().collect());
    }

    let par_results = eng.match_titles_par(&data.titles, true);
    let mut mismatches = 0usize;
    for (idx, matches, _) in &par_results {
        let par_set: HashSet<u64> = matches.iter().copied().collect();
        if par_set != sequential[*idx] {
            mismatches += 1;
        }
    }
    eprintln!(
        "    par vs seq: {} mismatches, elapsed={:.1}s",
        mismatches,
        t0.elapsed().as_secs_f64()
    );
    events.dump_summary("burst-par");
    assert_eq!(
        mismatches, 0,
        "parallel diverged from sequential under churn"
    );

    // ── Verify no deleted IDs appear ──
    let deleted_set: HashSet<u64> = delete_targets.into_iter().collect();
    let mut ghost = 0usize;
    for (_, matches, _) in &par_results {
        for id in matches {
            if deleted_set.contains(id) {
                ghost += 1;
            }
        }
    }
    assert_eq!(ghost, 0, "deleted IDs appeared in match results");
}

// ═════════════════════════════════════════════════════════════════════════════
// 4. HIGH-VOLUME PARALLEL MATCHING UNDER MUTATION
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn high_volume_parallel_read_after_mutations() {
    eprintln!("\n=== HIGH-VOLUME PARALLEL READS AFTER MUTATIONS ===");
    let t0 = Instant::now();

    let cfg = GenConfig {
        num_queries: 50_000,
        num_titles: 5_000,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0xFACE_B00C,
        num_players: 5_000,
        num_sets: 2_000,
    };
    let data = generate(&cfg);
    let q = &data.queries;

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 10_000,
            auto_compact_on_flush: true,
            max_segments: 6,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    // Phase 1: build base
    eprintln!("  Phase 1: bulk load {} queries", q.len() / 2);
    eng.build_from_queries(&q[..q.len() / 2]);

    // Phase 2: live inserts
    eprintln!("  Phase 2: live insert {} queries", q.len() / 2);
    for (logical, text) in &q[q.len() / 2..] {
        eng.insert_live(text, *logical, 1);
    }

    // Phase 3: delete 10% of queries
    let delete_set: HashSet<u64> = q.iter().step_by(10).map(|(id, _)| *id).collect();
    eprintln!("  Phase 3: deleting {} queries", delete_set.len());
    for id in &delete_set {
        let _ = eng.delete_by_logical_id(*id);
    }

    // Phase 4: flush + compact
    eng.flush();
    eng.compact_all();
    print_metrics("post-mutation", &eng.metrics());

    // Phase 5: parallel read of all 5k titles
    eprintln!(
        "  Phase 5: parallel read of {} titles...",
        data.titles.len()
    );
    let par_start = Instant::now();
    let par_results = eng.match_titles_par(&data.titles, true);
    let par_elapsed = par_start.elapsed();

    let total_par_matches: usize = par_results.iter().map(|(_, ids, _)| ids.len()).sum();
    let total_candidates: u32 = par_results
        .iter()
        .map(|(_, _, s)| s.unique_candidates)
        .sum();
    let total_skipped: u32 = par_results.iter().map(|(_, _, s)| s.probes_skipped).sum();

    eprintln!(
        "    parallel: {} matches, {} candidates, {} probes skipped, {:.1}ms",
        total_par_matches,
        total_candidates,
        total_skipped,
        par_elapsed.as_secs_f64() * 1000.0
    );

    // Phase 6: sequential read for comparison
    eprintln!("  Phase 6: sequential read for comparison...");
    let seq_start = Instant::now();
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut total_seq_matches = 0usize;
    for title in &data.titles {
        eng.match_title(title, &mut scratch, &mut out, true);
        total_seq_matches += out.len();
    }
    let seq_elapsed = seq_start.elapsed();

    eprintln!(
        "    sequential: {} matches, {:.1}ms",
        total_seq_matches,
        seq_elapsed.as_secs_f64() * 1000.0
    );
    eprintln!(
        "    speedup: {:.1}x",
        seq_elapsed.as_secs_f64() / par_elapsed.as_secs_f64().max(0.001)
    );

    // Verify no deleted IDs in results
    let mut ghosts = 0usize;
    for (_, matches, _) in &par_results {
        for id in matches {
            if delete_set.contains(id) {
                ghosts += 1;
            }
        }
    }

    events.dump_summary("high-vol");
    eprintln!("  elapsed={:.1}s", t0.elapsed().as_secs_f64());

    assert_eq!(ghosts, 0, "deleted IDs appeared in parallel results");
    assert!(total_par_matches > 0, "no matches at all");
    assert_eq!(
        total_par_matches, total_seq_matches,
        "parallel ({total_par_matches}) != sequential ({total_seq_matches}) match count"
    );
}

// ═════════════════════════════════════════════════════════════════════════════
// 4b. BROAD-LANE BATCH == PER-TITLE under churn (ADR-026)
// ═════════════════════════════════════════════════════════════════════════════

/// The columnar broad-batch path (`match_titles_batch`) must return EXACTLY the
/// per-title match set (`match_title`) after a realistic churn cycle (build + live
/// insert + delete + flush + compact + a trailing unflushed memtable). Both broad
/// strategies are checked, and tombstoned ids must not ghost — the batch twin of
/// the par==seq guards above.
#[test]
fn broad_batch_equals_per_title_under_churn() {
    eprintln!("\n=== BROAD BATCH == PER-TITLE UNDER CHURN ===");
    let t0 = Instant::now();

    let cfg = GenConfig {
        num_queries: 40_000,
        num_titles: 4_000,
        broad_query_frac: 0.08,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0xBA7C_8027,
        num_players: 4_000,
        num_sets: 1_500,
    };
    let data = generate(&cfg);
    let q = &data.queries;

    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 8_000,
            auto_compact_on_flush: true,
            max_segments: 6,
            ..EngineConfig::default()
        },
    );
    eng.build_from_queries(&q[..q.len() / 2]);
    for (logical, text) in &q[q.len() / 2..] {
        eng.insert_live(text, *logical, 1);
    }
    // Delete every 10th query by id.
    let delete_set: HashSet<u64> = q.iter().step_by(10).map(|(id, _)| *id).collect();
    for id in &delete_set {
        let _ = eng.delete_by_logical_id(*id);
    }
    eng.flush();
    eng.compact_all();
    // Trailing churn that lands in a fresh, unflushed memtable. Indices ≡3 (mod
    // 10) are disjoint from the deleted set (≡0 mod 10), so nothing is revived.
    for (logical, text) in q.iter().skip(3).step_by(500) {
        eng.insert_live(text, *logical, 2);
    }

    // Per-title baseline (the contract the batch path must reproduce).
    let snap = eng.snapshot();
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut seq: Vec<HashSet<u64>> = Vec::with_capacity(data.titles.len());
    for title in &data.titles {
        out.clear();
        snap.match_title(title, &mut scratch, &mut out, true);
        seq.push(out.iter().copied().collect());
    }

    for strat in [BroadStrategy::Columnar, BroadStrategy::Inline] {
        let results = snap.match_titles_batch(
            &data.titles,
            BatchMatchOptions {
                include_broad: true,
                broad_batch_size: 256,
                broad_strategy: strat,
                broad_materialize: true,
                broad_prefilter: true,
            },
        );
        let mut mismatches = 0usize;
        let mut ghosts = 0usize;
        for (idx, ids) in results {
            let set: HashSet<u64> = ids.into_iter().collect();
            for id in &set {
                if delete_set.contains(id) {
                    ghosts += 1;
                }
            }
            if set != seq[idx] {
                mismatches += 1;
            }
        }
        assert_eq!(mismatches, 0, "batch ({strat:?}) != per-title under churn");
        assert_eq!(
            ghosts, 0,
            "deleted ids ghosted in batch ({strat:?}) results"
        );
    }
    eprintln!(
        "  batch==per-title under churn OK, elapsed={:.1}s",
        t0.elapsed().as_secs_f64()
    );
}
