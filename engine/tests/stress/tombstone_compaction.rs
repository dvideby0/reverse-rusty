//! Tombstone reclamation, metrics consistency, and compaction-trigger workloads.

use crate::harness::*;

// ═════════════════════════════════════════════════════════════════════════════
// 3. RAPID INSERT-DELETE-FLUSH CYCLING — exercises tombstone reclamation
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn rapid_insert_delete_flush_cycles() {
    eprintln!("\n=== RAPID INSERT-DELETE-FLUSH CYCLES ===");
    let t0 = Instant::now();

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 200,
            auto_compact_on_flush: true,
            max_segments: 4,
            holes_ratio_threshold: 0.25,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    // Seed a base segment
    let seed_queries: Vec<(u64, String)> = (0..500)
        .map(|i| (i, format!("player{i} 1994 upper deck basketball")))
        .collect();
    eng.build_from_queries(&seed_queries);

    let mut live_set: HashSet<u64> = (0..500).collect();
    let mut next_id = 1000u64;

    // 10 rounds of: insert batch -> delete some -> flush -> read
    for round in 0..10 {
        eprintln!("\n  Round {round}/10:");

        // Insert 100 new queries
        let insert_count = 100;
        for _i in 0..insert_count {
            let id = next_id;
            next_id += 1;
            let text = format!(
                "round{} player{} {} fleer basketball card",
                round,
                id % 200,
                1990 + (id % 30)
            );
            eng.insert_live(&text, id, 1);
            live_set.insert(id);
        }
        eprintln!("    inserted {}, live_set={}", insert_count, live_set.len());

        // Delete 30 random-ish entries from live set
        let to_delete: Vec<u64> = live_set.iter().copied().take(30).collect();
        for id in &to_delete {
            let _ = eng.delete_by_logical_id(*id);
            live_set.remove(id);
        }
        eprintln!(
            "    deleted {}, live_set={}",
            to_delete.len(),
            live_set.len()
        );

        // Flush (may trigger auto-compact)
        eng.flush();

        let m = eng.metrics();
        eprintln!(
            "    segments={} total_queries={} holes={:?}",
            m.base_segments + 1,
            m.total_queries,
            m.segment_holes
                .iter()
                .map(|h| format!("{:.1}%", h * 100.0))
                .collect::<Vec<_>>()
        );

        // Read sweep — verify no deleted IDs
        let mut scratch = MatchScratch::new();
        let mut out = Vec::new();
        let test_title = "player5 1994 upper deck fleer basketball card round3";
        eng.match_title(test_title, &mut scratch, &mut out, true);

        for id in &out {
            assert!(
                live_set.contains(id) || *id >= 1000,
                "round {round}: deleted ID {id} appeared in results"
            );
        }
    }

    print_metrics("final", &eng.metrics());
    events.dump_summary("rapid-cycles");

    let f = events.flushes.load(Ordering::Relaxed);
    let c = events.compactions.load(Ordering::Relaxed);
    eprintln!(
        "  Total: {} flushes, {} compactions, elapsed={:.1}s",
        f,
        c,
        t0.elapsed().as_secs_f64()
    );
    assert!(f >= 10, "expected at least 10 flushes, got {f}");
    assert!(c > 0, "expected at least 1 auto-compaction, got 0");
}

// ═════════════════════════════════════════════════════════════════════════════
// 6. METRICS CONSISTENCY UNDER CHURN
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn metrics_stay_consistent_under_churn() {
    eprintln!("\n=== METRICS CONSISTENCY UNDER CHURN ===");

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 500,
            auto_compact_on_flush: true,
            max_segments: 5,
            holes_ratio_threshold: 0.2,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    let cfg = GenConfig {
        num_queries: 10_000,
        num_titles: 500,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0xDEAD_C0DE,
        num_players: 2_000,
        num_sets: 800,
    };
    let data = generate(&cfg);

    // Build initial
    let report = eng.build_from_queries(&data.queries);
    let initial_count = report.ingested;
    eprintln!("  initial: ingested={initial_count}");

    let mut m = eng.metrics();
    assert_eq!(
        m.total_queries, initial_count,
        "metrics.total_queries != ingested after build"
    );

    // Track live count manually
    let mut expected_live = initial_count;

    // 5 rounds of insert/delete/flush
    let mut next_id = 100_000u64;
    for round in 0..5 {
        // Insert 200
        let mut inserted = 0usize;
        for _ in 0..200 {
            let text = format!("round{round} player{next_id} 1994 topps");
            if eng.insert_live(&text, next_id, 1).is_some() {
                inserted += 1;
            }
            next_id += 1;
        }
        expected_live += inserted;

        // Delete 50 from the initial batch
        let del_start = round * 50;
        let mut deleted = 0usize;
        for i in del_start..del_start + 50 {
            let logical = data.queries[i].0;
            let n = eng.delete_by_logical_id(logical).unwrap();
            if n > 0 {
                deleted += 1;
                expected_live -= 1;
            }
        }

        eng.flush();
        m = eng.metrics();

        eprintln!(
            "  round {}: inserted={} deleted={} expected_live={} metrics.total={}",
            round, inserted, deleted, expected_live, m.total_queries
        );

        // total_queries should track: it counts alive entries
        // (Note: total_queries counts all entries including tombstoned, so this
        // is a >= check. After compaction, tombstones are reclaimed.)
        assert!(
            m.total_queries >= expected_live,
            "round {}: metrics.total_queries ({}) < expected_live ({})",
            round,
            m.total_queries,
            expected_live
        );

        // segment_sizes should sum to total_queries
        let size_sum: usize = m.segment_sizes.iter().sum::<usize>() + m.memtable_entries;
        assert_eq!(
            m.total_queries, size_sum,
            "round {}: total ({}) != sum of segment_sizes + memtable ({})",
            round, m.total_queries, size_sum
        );
    }

    // Final compact should reclaim tombstones
    eng.compact_all();
    m = eng.metrics();
    print_metrics("final", &m);
    events.dump_summary("metrics-churn");

    // After compaction, holes should be near 0
    for (i, &h) in m.segment_holes.iter().enumerate() {
        assert!(
            h < 0.01,
            "segment {} has {:.1}% holes after compact_all",
            i,
            h * 100.0
        );
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// 8. AUTO-COMPACTION TRIGGER VERIFICATION
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn auto_compaction_triggers_correctly() {
    eprintln!("\n=== AUTO-COMPACTION TRIGGER VERIFICATION ===");

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 300,
            auto_compact_on_flush: true,
            max_segments: 3, // aggressive — triggers often
            holes_ratio_threshold: 0.15,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    // Rapidly build segments to trigger segment-count compaction
    for batch in 0..8 {
        let queries: Vec<(u64, String)> = (0..400)
            .map(|i| {
                let id = (batch * 1000 + i) as u64;
                (id, format!("batch{batch} player{i} 1994 topps card"))
            })
            .collect();

        if batch == 0 {
            eng.build_from_queries(&queries);
        } else {
            for (id, text) in &queries {
                eng.insert_live(text, *id, 1);
            }
            eng.flush();
        }

        let m = eng.metrics();
        eprintln!(
            "  batch {}: segments={} total={}",
            batch,
            m.base_segments + 1,
            m.total_queries
        );
    }

    let compactions = events.compactions.load(Ordering::Relaxed);
    eprintln!("  total auto-compactions triggered: {compactions}");
    events.dump_summary("auto-compact");

    assert!(
        compactions >= 2,
        "expected >= 2 auto-compactions with max_segments=3 and 8 batches, got {compactions}"
    );

    // Engine should have compacted down
    let m = eng.metrics();
    assert!(
        m.base_segments <= 4,
        "expected <= 4 segments after auto-compaction, got {}",
        m.base_segments
    );
}

// ═════════════════════════════════════════════════════════════════════════════
// 9. DELETE-HEAVY WORKLOAD — stress tombstone / holes ratio
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn delete_heavy_workload() {
    eprintln!("\n=== DELETE-HEAVY WORKLOAD ===");
    let t0 = Instant::now();

    let cfg = GenConfig {
        num_queries: 15_000,
        num_titles: 1_000,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0xDE_1E_7E,
        num_players: 2_000,
        num_sets: 800,
    };
    let data = generate(&cfg);

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 5_000,
            auto_compact_on_flush: true,
            max_segments: 4,
            holes_ratio_threshold: 0.3,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    eng.build_from_queries(&data.queries);
    let initial_count = eng.metrics().total_queries;
    eprintln!("  initial: {initial_count} queries");

    // Delete 80% of queries
    let delete_count = (data.queries.len() * 8) / 10;
    eprintln!("  deleting {delete_count} queries (80%)");
    let mut deleted = HashSet::new();
    for (logical, _) in data.queries.iter().take(delete_count) {
        let _ = eng.delete_by_logical_id(*logical);
        deleted.insert(*logical);
    }

    // Insert replacements
    eprintln!("  inserting {} replacements", delete_count / 2);
    for i in 0..delete_count / 2 {
        let text = format!("replacement{i} michael jordan 1994 upper deck");
        eng.insert_live(&text, 10_000_000 + i as u64, 1);
    }
    eng.flush();

    let m = eng.metrics();
    eprintln!(
        "  pre-compact: segments={} total={} holes={:?}",
        m.base_segments,
        m.total_queries,
        m.segment_holes
            .iter()
            .map(|h| format!("{:.1}%", h * 100.0))
            .collect::<Vec<_>>()
    );

    // Compact to reclaim
    eng.compact_all();
    let m = eng.metrics();
    print_metrics("post-compact", &m);

    // Verify no ghosts in search results
    let par_results = eng.match_titles_par(&data.titles, true);
    let mut ghosts = 0usize;
    let total_matches: usize = par_results.iter().map(|(_, ids, _)| ids.len()).sum();
    for (_, matches, _) in &par_results {
        for id in matches {
            if deleted.contains(id) {
                ghosts += 1;
            }
        }
    }

    eprintln!(
        "  total_matches={} ghosts={} elapsed={:.1}s",
        total_matches,
        ghosts,
        t0.elapsed().as_secs_f64()
    );
    events.dump_summary("delete-heavy");
    assert_eq!(ghosts, 0, "deleted IDs appeared in results");
}
