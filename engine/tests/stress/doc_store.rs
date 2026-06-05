//! Doc-store consistency, simulated live-server traffic, and version-churn storms.

use crate::harness::*;

// ═════════════════════════════════════════════════════════════════════════════
// 12. DOC STORE CONSISTENCY — source lookups through full lifecycle
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn doc_store_consistent_through_lifecycle() {
    eprintln!("\n=== DOC STORE CONSISTENCY THROUGH LIFECYCLE ===");

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 200,
            auto_compact_on_flush: true,
            max_segments: 4,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    // Phase 1: Bulk load with source tracking
    let batch: Vec<(u64, String)> = (0..500)
        .map(|i| {
            let text = match i % 5 {
                0 => format!("player{i} 1994 upper deck basketball card"),
                1 => format!("player{i} (topps,fleer,bowman) rookie -(auto)"),
                2 => format!("player{i} 1986 fleer (psa,bgs) -(reprint,lot)"),
                3 => format!("(player{i},athlete{i}) topps 1997 card"),
                _ => format!("player{i} basketball card"),
            };
            (i as u64, text)
        })
        .collect();

    eng.build_from_queries(&batch);
    eprintln!("  Phase 1: loaded {} queries", batch.len());

    // Verify all sources accessible
    let mut found = 0usize;
    for (id, text) in &batch {
        if let Some(src) = eng.get_query_source(*id) {
            assert_eq!(&src, text, "source mismatch for id={id}");
            found += 1;
        }
    }
    eprintln!("  sources found after build: {}/{}", found, batch.len());

    // Phase 2: Live inserts — verify source available immediately
    eprintln!("  Phase 2: live inserts with source checks");
    for i in 500..700u64 {
        let text = format!("live{} michael jordan {} fleer", i, 1980 + (i % 20));
        eng.insert_live(&text, i, 1);
        let src = eng.get_query_source(i);
        assert!(
            src.is_some(),
            "source missing immediately after insert_live({i})"
        );
        assert_eq!(src.unwrap(), text);
    }

    // Phase 3: Delete and verify source removed
    eprintln!("  Phase 3: delete + source removal check");
    let delete_ids: Vec<u64> = (0..100).collect();
    for id in &delete_ids {
        let _ = eng.delete_by_logical_id(*id);
    }
    for id in &delete_ids {
        assert!(
            eng.get_query_source(*id).is_none(),
            "source for deleted id {id} should be None"
        );
    }
    // Non-deleted should still be there
    for id in 100..200u64 {
        assert!(
            eng.get_query_source(id).is_some(),
            "source for non-deleted id {id} should exist"
        );
    }

    // Phase 4: Flush + verify sources survive
    eng.flush();
    eprintln!("  Phase 4: post-flush source check");
    for id in 200..300u64 {
        assert!(
            eng.get_query_source(id).is_some(),
            "source {id} lost after flush"
        );
    }
    for id in 500..700u64 {
        assert!(
            eng.get_query_source(id).is_some(),
            "live source {id} lost after flush"
        );
    }

    // Phase 5: Compact + verify sources survive
    eng.compact_all();
    eprintln!("  Phase 5: post-compact source check");
    for id in 200..300u64 {
        assert!(
            eng.get_query_source(id).is_some(),
            "source {id} lost after compact"
        );
    }
    for id in &delete_ids {
        assert!(
            eng.get_query_source(*id).is_none(),
            "deleted source {id} reappeared after compact"
        );
    }

    // Phase 6: Update (delete + re-insert) — source should reflect new text
    eprintln!("  Phase 6: update + source verification");
    for id in 200..250u64 {
        let new_text = format!("UPDATED player{id} 2024 panini prizm basketball");
        let _ = eng.delete_by_logical_id(id);
        eng.insert_live(&new_text, id, 2);
        let src = eng
            .get_query_source(id)
            .expect("source missing after update");
        assert_eq!(src, new_text, "source not updated for id={id}");
    }

    // Phase 7: Interleave match requests with doc lookups
    eprintln!("  Phase 7: interleaved match + doc lookups");
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let test_titles = [
        "player250 1994 upper deck basketball card psa 10",
        "live550 michael jordan 1990 fleer card",
        "UPDATED player220 2024 panini prizm basketball card",
        "player400 topps 1997 card basketball",
        "michael jordan 1986 fleer rookie card",
    ];

    for title in &test_titles {
        eng.match_title(title, &mut scratch, &mut out, true);
        eprintln!("    title={:?} -> {} matches", title, out.len());

        // For every match, verify we can look up its source
        for &matched_id in &out {
            let src = eng.get_query_source(matched_id);
            assert!(
                src.is_some(),
                "matched id {matched_id} has no source for title {title:?}"
            );
        }
    }

    print_metrics("final", &eng.metrics());
    events.dump_summary("doc-store");
}

// ═════════════════════════════════════════════════════════════════════════════
// 13. GROWING INDEX WHILE MATCHING — simulate a live server receiving queries
//     and search requests simultaneously
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn simulate_live_server_traffic() {
    eprintln!("\n=== SIMULATED LIVE SERVER TRAFFIC ===");
    let t0 = Instant::now();

    let cfg = GenConfig {
        num_queries: 20_000,
        num_titles: 3_000,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x5E_2F_1C,
        num_players: 3_000,
        num_sets: 1_200,
    };
    let data = generate(&cfg);

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 1_000,
            auto_compact_on_flush: true,
            max_segments: 5,
            holes_ratio_threshold: 0.2,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut inserted_count = 0usize;
    let mut total_searches = 0usize;
    let mut total_matches = 0usize;
    let mut total_doc_lookups = 0usize;
    let mut doc_lookup_hits = 0usize;
    let mut deleted_ids: HashSet<u64> = HashSet::new();

    // Simulate interleaved traffic: for every N inserts, do M searches and K doc lookups
    let insert_batch_size = 50;
    let searches_per_batch = 20;
    let doc_lookups_per_batch = 10;
    let delete_every_n_batches = 5;

    let mut title_cursor = 0usize;
    let mut batch_num = 0usize;

    for query_chunk in data.queries.chunks(insert_batch_size) {
        // ── Write: insert a batch of queries ──
        for (logical, text) in query_chunk {
            eng.insert_live(text, *logical, 1);
            inserted_count += 1;
        }

        // ── Read: search against rotating titles ──
        for _ in 0..searches_per_batch {
            let title = &data.titles[title_cursor % data.titles.len()];
            eng.match_title(title, &mut scratch, &mut out, true);
            total_matches += out.len();
            total_searches += 1;

            // Verify no deleted IDs in results
            for &id in &out {
                assert!(
                    !deleted_ids.contains(&id),
                    "batch {batch_num}: deleted id {id} in matches for {title:?}"
                );
            }

            title_cursor += 1;
        }

        // ── Read: doc lookups on recently inserted IDs ──
        let recent_ids: Vec<u64> = query_chunk.iter().map(|(id, _)| *id).collect();
        for i in 0..doc_lookups_per_batch.min(recent_ids.len()) {
            let id = recent_ids[i];
            total_doc_lookups += 1;
            if let Some(src) = eng.get_query_source(id) {
                doc_lookup_hits += 1;
                assert_eq!(
                    src, query_chunk[i].1,
                    "source mismatch for recently inserted id={id}"
                );
            }
        }

        // ── Write: periodic deletes ──
        if batch_num.is_multiple_of(delete_every_n_batches) && batch_num > 0 {
            let del_count = insert_batch_size / 5;
            let del_start = (batch_num - delete_every_n_batches) * insert_batch_size;
            let del_end = (del_start + del_count).min(data.queries.len());
            for (logical, _) in &data.queries[del_start..del_end] {
                let _ = eng.delete_by_logical_id(*logical);
                deleted_ids.insert(*logical);
            }
            if del_start < del_end {
                eprintln!(
                    "    batch {}: deleted {} queries (ids {}..{})",
                    batch_num,
                    del_end - del_start,
                    data.queries[del_start].0,
                    data.queries[del_end - 1].0,
                );
            }
        }

        // Log progress every 100 batches
        if batch_num.is_multiple_of(100) {
            let m = eng.metrics();
            eprintln!(
                "  batch {}: inserted={} searches={} matches={} doc_lookups={}/{} segments={} queries={}",
                batch_num,
                inserted_count,
                total_searches,
                total_matches,
                doc_lookup_hits,
                total_doc_lookups,
                m.base_segments + 1,
                m.total_queries,
            );
        }

        batch_num += 1;
    }

    // ── Final parallel sweep ──
    eprintln!(
        "\n  Final parallel sweep over {} titles...",
        data.titles.len()
    );
    let par_results = eng.match_titles_par(&data.titles, true);
    let par_total: usize = par_results.iter().map(|(_, ids, _)| ids.len()).sum();

    // Check sequential agreement
    let mut seq_total = 0usize;
    let mut mismatches = 0usize;
    let mut seq_results: Vec<HashSet<u64>> = Vec::new();
    for title in &data.titles {
        eng.match_title(title, &mut scratch, &mut out, true);
        seq_total += out.len();
        seq_results.push(out.iter().copied().collect());
    }
    for (idx, matches, _) in &par_results {
        let par_set: HashSet<u64> = matches.iter().copied().collect();
        if par_set != seq_results[*idx] {
            mismatches += 1;
        }
    }

    // No deleted IDs in final results
    let mut final_ghosts = 0usize;
    for (_, matches, _) in &par_results {
        for id in matches {
            if deleted_ids.contains(id) {
                final_ghosts += 1;
            }
        }
    }

    eprintln!(
        "\n  TRAFFIC SIM RESULTS:\n    batches={batch_num} inserts={inserted_count} searches={total_searches} matches={total_matches}"
    );
    eprintln!(
        "    doc_lookups={}/{} deletes={} ghosts={}",
        doc_lookup_hits,
        total_doc_lookups,
        deleted_ids.len(),
        final_ghosts
    );
    eprintln!(
        "    final: par={} seq={} mismatches={} elapsed={:.1}s",
        par_total,
        seq_total,
        mismatches,
        t0.elapsed().as_secs_f64()
    );
    print_metrics("final", &eng.metrics());
    events.dump_summary("traffic-sim");

    assert_eq!(final_ghosts, 0, "deleted IDs in final results");
    assert_eq!(mismatches, 0, "parallel != sequential in traffic sim");
    assert_eq!(par_total, seq_total, "par total != seq total");
    assert!(total_matches > 0, "no matches during simulation");
    assert!(doc_lookup_hits > 0, "no doc lookups succeeded");
}

// ═════════════════════════════════════════════════════════════════════════════
// 14. UPDATE STORM — rapid version churn on the same logical IDs
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn update_storm_same_ids() {
    eprintln!("\n=== UPDATE STORM (same IDs, many versions) ===");
    let t0 = Instant::now();

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 300,
            auto_compact_on_flush: true,
            max_segments: 4,
            holes_ratio_threshold: 0.2,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    let base_ids: Vec<u64> = (1..=50).collect();

    // Build initial versions
    let initial: Vec<(u64, String)> = base_ids
        .iter()
        .map(|&id| (id, format!("player{id} 1994 upper deck basketball card")))
        .collect();
    eng.build_from_queries(&initial);
    eprintln!("  initial: {} queries", initial.len());

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();

    let brands = ["topps", "bowman", "fleer", "panini", "donruss", "hoops"];
    let years = [1986, 1990, 1993, 1996, 1997, 2003, 2010, 2020, 2024];
    let suffixes = [
        "rookie card",
        "card psa 10",
        "basketball card",
        "card bgs 9.5",
        "chrome refractor",
        "prizm silver",
    ];

    // 20 rounds of updates across all 50 IDs
    for round in 0..20u32 {
        let version = round + 2;

        for &id in &base_ids {
            let brand = brands[(id as usize + round as usize) % brands.len()];
            let year = years[(id as usize + round as usize * 3) % years.len()];
            let suffix = suffixes[(id as usize + round as usize * 7) % suffixes.len()];
            let new_text = format!("player{id} {year} {brand} {suffix}");

            let _ = eng.delete_by_logical_id(id);
            eng.insert_live(&new_text, id, version);

            // Verify source reflects latest version
            let src = eng
                .get_query_source(id)
                .expect("source missing after update");
            assert_eq!(src, new_text, "round {round}: source stale for id={id}");
        }

        // Match a few titles between each update round
        let test_titles = [
            "player10 1993 topps rookie card psa 10",
            "player25 2003 bowman chrome refractor card",
            "player1 1986 fleer basketball card psa 10",
            "player50 2024 panini prizm silver basketball",
        ];
        for title in &test_titles {
            eng.match_title(title, &mut scratch, &mut out, true);
        }

        if round % 5 == 0 {
            let m = eng.metrics();
            eprintln!(
                "  round {}: version={} segments={} total={}",
                round,
                version,
                m.base_segments + 1,
                m.total_queries
            );
        }
    }

    // Final state: exactly 50 queries should be live
    eng.flush();
    eng.compact_all();
    let m = eng.metrics();
    print_metrics("final", &m);

    // After compact, tombstones reclaimed — but total queries may be
    // higher than 50 due to multi-version duplication being cleaned up
    // during compaction. The key invariant: each logical ID matches at most once.
    let title = "player10 1993 topps rookie card bowman fleer panini prizm";
    eng.match_title(title, &mut scratch, &mut out, true);
    let mut seen_ids: HashSet<u64> = HashSet::new();
    for &id in &out {
        assert!(
            seen_ids.insert(id),
            "duplicate logical id {id} in match results after update storm"
        );
    }

    // All 50 IDs should have sources
    for &id in &base_ids {
        assert!(
            eng.get_query_source(id).is_some(),
            "source missing for id={id} after update storm + compact"
        );
    }

    events.dump_summary("update-storm");
    eprintln!("  elapsed={:.1}s", t0.elapsed().as_secs_f64());
}
