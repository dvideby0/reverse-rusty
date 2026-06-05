//! The large-scale 10M-query soak (run explicitly — `#[ignore]`d by default).

use crate::harness::*;

// ═════════════════════════════════════════════════════════════════════════════
// 16. 10M SCALE — large corpus then hammer with mixed operations
//
//     Run: cargo test --release --test stress ten_million -- --nocapture
// ═════════════════════════════════════════════════════════════════════════════

#[test]
#[ignore = "large-scale — run explicitly with --ignored or by name"]
fn ten_million_queries_mixed_ops() {
    eprintln!("\n=== 10M QUERIES — MIXED OPS AT SCALE ===");
    let t0 = Instant::now();

    // ── Generate 10M queries + 50K titles ──
    eprintln!("  Generating 10M queries + 50K titles...");
    let gen_start = Instant::now();
    let cfg = GenConfig {
        num_queries: 10_000_000,
        num_titles: 50_000,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x10_000_000,
        num_players: 20_000,
        num_sets: 8_000,
    };
    let data = generate(&cfg);
    eprintln!(
        "    generated {} queries, {} titles in {:.1}s",
        data.queries.len(),
        data.titles.len(),
        gen_start.elapsed().as_secs_f64()
    );

    // ── Build engine with 10M queries ──
    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 100_000,
            auto_compact_on_flush: true,
            max_segments: 8,
            holes_ratio_threshold: 0.3,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    // Load in 2 chunks to exercise multi-segment from the start
    let half = data.queries.len() / 2;
    eprintln!("  Building base segment (5M queries)...");
    let build_start = Instant::now();
    eng.build_from_queries(&data.queries[..half]);
    eprintln!("    built in {:.1}s", build_start.elapsed().as_secs_f64());

    eprintln!("  Bulk ingesting second segment (5M queries)...");
    let ingest_start = Instant::now();
    eng.bulk_ingest(&data.queries[half..]);
    eprintln!(
        "    ingested in {:.1}s",
        ingest_start.elapsed().as_secs_f64()
    );

    print_metrics("after-load", &eng.metrics());

    // ── Phase 1: Parallel search sweep over 50K titles ──
    eprintln!(
        "\n  Phase 1: parallel search over {} titles...",
        data.titles.len()
    );
    let search_start = Instant::now();
    let par_results = eng.match_titles_par(&data.titles, true);
    let search_elapsed = search_start.elapsed();

    let total_matches: usize = par_results.iter().map(|(_, ids, _)| ids.len()).sum();
    let total_candidates: u64 = par_results
        .iter()
        .map(|(_, _, s)| u64::from(s.unique_candidates))
        .sum();
    let total_skipped: u64 = par_results
        .iter()
        .map(|(_, _, s)| u64::from(s.probes_skipped))
        .sum();
    let titles_per_sec = data.titles.len() as f64 / search_elapsed.as_secs_f64();
    eprintln!(
        "    {total_matches} matches, {total_candidates} candidates, {total_skipped} probes skipped"
    );
    eprintln!(
        "    {:.1}ms ({:.0} titles/sec)",
        search_elapsed.as_secs_f64() * 1000.0,
        titles_per_sec
    );

    // ── Phase 2: Sequential search for comparison ──
    eprintln!("  Phase 2: sequential search (1K titles) for par agreement...");
    let check_count = 1_000;
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut mismatches = 0usize;
    for (i, (_, par_ids, _)) in par_results.iter().enumerate().take(check_count) {
        eng.match_title(&data.titles[i], &mut scratch, &mut out, true);
        let seq_set: HashSet<u64> = out.iter().copied().collect();
        let par_set: HashSet<u64> = par_ids.iter().copied().collect();
        if seq_set != par_set {
            mismatches += 1;
        }
    }
    eprintln!("    par vs seq: {mismatches} mismatches over {check_count} titles");
    assert_eq!(mismatches, 0, "parallel != sequential at 10M scale");

    // ── Phase 3: Live inserts (100K new queries) while searching ──
    eprintln!("\n  Phase 3: inserting 100K queries with interleaved searches...");
    let insert_start = Instant::now();
    let insert_count = 100_000u64;
    let mut insert_ok = 0usize;
    let mut search_during_insert = 0usize;
    let mut matches_during_insert = 0usize;

    for i in 0..insert_count {
        let id = 50_000_000 + i;
        let text = format!(
            "live{} player{} {} fleer basketball card",
            i,
            i % 20_000,
            1980 + (i % 40)
        );
        if eng.insert_live(&text, id, 1).is_some() {
            insert_ok += 1;
        }

        // Search every 500th insert
        if i % 500 == 0 {
            let title_idx = (i as usize) % data.titles.len();
            eng.match_title(&data.titles[title_idx], &mut scratch, &mut out, true);
            search_during_insert += 1;
            matches_during_insert += out.len();
        }
    }
    eprintln!(
        "    inserted {} in {:.1}s ({:.0}/sec)",
        insert_ok,
        insert_start.elapsed().as_secs_f64(),
        insert_ok as f64 / insert_start.elapsed().as_secs_f64()
    );
    eprintln!(
        "    {search_during_insert} searches during inserts, {matches_during_insert} matches"
    );

    // ── Phase 4: Delete 500K queries (5%) ──
    eprintln!("\n  Phase 4: deleting 500K queries (5%)...");
    let delete_start = Instant::now();
    let delete_count = 500_000;
    let deleted_ids: HashSet<u64> = data.queries[..delete_count]
        .iter()
        .map(|(id, _)| *id)
        .collect();
    for &id in &deleted_ids {
        let _ = eng.delete_by_logical_id(id);
    }
    eprintln!(
        "    deleted {} in {:.1}s ({:.0}/sec)",
        delete_count,
        delete_start.elapsed().as_secs_f64(),
        delete_count as f64 / delete_start.elapsed().as_secs_f64()
    );
    print_metrics("after-deletes", &eng.metrics());

    // ── Phase 5: Update 50K queries (delete + re-insert with new version) ──
    eprintln!("\n  Phase 5: updating 50K queries...");
    let update_start = Instant::now();
    let update_count = 50_000;
    let update_range = &data.queries[delete_count..delete_count + update_count];
    for (logical, text) in update_range {
        let _ = eng.delete_by_logical_id(*logical);
        let new_text = format!("{text} updated variant");
        eng.insert_live(&new_text, *logical, 99);
    }
    eprintln!(
        "    updated {} in {:.1}s ({:.0}/sec)",
        update_count,
        update_start.elapsed().as_secs_f64(),
        update_count as f64 / update_start.elapsed().as_secs_f64()
    );

    // ── Phase 6: Flush + compact ──
    eprintln!("\n  Phase 6: flush + compact...");
    let compact_start = Instant::now();
    eng.flush();
    eng.compact_all();
    eprintln!(
        "    flush+compact in {:.1}s",
        compact_start.elapsed().as_secs_f64()
    );
    print_metrics("after-compact", &eng.metrics());

    // ── Phase 7: Post-mutation parallel search ──
    eprintln!(
        "\n  Phase 7: post-mutation parallel search ({} titles)...",
        data.titles.len()
    );
    let search2_start = Instant::now();
    let par2 = eng.match_titles_par(&data.titles, true);
    let search2_elapsed = search2_start.elapsed();

    let total2: usize = par2.iter().map(|(_, ids, _)| ids.len()).sum();
    let titles_per_sec2 = data.titles.len() as f64 / search2_elapsed.as_secs_f64();
    eprintln!(
        "    {} matches in {:.1}ms ({:.0} titles/sec)",
        total2,
        search2_elapsed.as_secs_f64() * 1000.0,
        titles_per_sec2
    );

    // Verify no deleted IDs in results
    let mut ghosts = 0usize;
    for (_, matches, _) in &par2 {
        for id in matches {
            if deleted_ids.contains(id) {
                ghosts += 1;
            }
        }
    }
    eprintln!("    ghosts (deleted IDs in results): {ghosts}");

    // ── Phase 8: Doc store spot-checks ──
    eprintln!("\n  Phase 8: doc store spot-checks...");
    let mut source_ok = 0usize;
    let mut source_missing = 0usize;
    // Check some non-deleted queries have sources
    for (id, text) in data.queries[delete_count + update_count..]
        .iter()
        .take(1_000)
    {
        match eng.get_query_source(*id) {
            Some(src) => {
                assert_eq!(src, text.as_str(), "source mismatch for id={id}");
                source_ok += 1;
            }
            None => {
                source_missing += 1;
            }
        }
    }
    // Check deleted queries have no sources
    let mut deleted_source_ghosts = 0usize;
    for &id in deleted_ids.iter().take(1_000) {
        if eng.get_query_source(id).is_some() {
            deleted_source_ghosts += 1;
        }
    }
    eprintln!(
        "    sources: {}/{} found, {} deleted still present",
        source_ok,
        source_ok + source_missing,
        deleted_source_ghosts
    );

    // ── Summary ──
    let elapsed = t0.elapsed();
    eprintln!("\n  ══════════════════════════════════════");
    eprintln!("  10M SCALE SUMMARY");
    eprintln!("  ──────────────────────────────────────");
    eprintln!("  corpus:        10M queries, 50K titles");
    eprintln!("  inserts:       100K live");
    eprintln!("  deletes:       500K (5%)");
    eprintln!("  updates:       50K");
    eprintln!("  search (pre):  {total_matches} matches @ {titles_per_sec:.0} titles/sec");
    eprintln!("  search (post): {total2} matches @ {titles_per_sec2:.0} titles/sec");
    eprintln!("  ghosts:        {ghosts}");
    eprintln!("  par==seq:      {mismatches} mismatches");
    eprintln!("  total elapsed: {:.1}s", elapsed.as_secs_f64());
    eprintln!("  ══════════════════════════════════════");
    events.dump_summary("10M-scale");

    assert_eq!(ghosts, 0, "deleted IDs in results at 10M scale");
    assert_eq!(mismatches, 0, "parallel != sequential at 10M scale");
    assert_eq!(deleted_source_ghosts, 0, "deleted sources still in store");
    assert!(total_matches > 0, "no matches at all");
    assert!(total2 > 0, "no matches after mutations");
}
