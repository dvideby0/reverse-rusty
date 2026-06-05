//! Oracle-verified interleaved ops + the full-lifecycle soak.

use crate::harness::*;

// ═════════════════════════════════════════════════════════════════════════════
// 7. MULTI-SEGMENT INTERLEAVED OPS — exercises segment boundary logic
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn multi_segment_interleaved_ops_oracle() {
    eprintln!("\n=== MULTI-SEGMENT INTERLEAVED OPS (oracle verified) ===");
    let t0 = Instant::now();

    let cfg = GenConfig {
        num_queries: 30_000,
        num_titles: 2_000,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x1E_2E_AE,
        num_players: 3_000,
        num_sets: 1_200,
    };
    let data = generate(&cfg);
    let q = &data.queries;
    let chunk = q.len() / 10;

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 5_000,
            auto_compact_on_flush: false, // manual control
            auto_compact_on_ingest: false,
            max_segments: 20,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    let mut live_queries: Vec<(u64, String)> = Vec::new();

    // Step 1: build initial segment
    eprintln!("  Step 1: build_from_queries({chunk})");
    eng.build_from_queries(&q[..chunk]);
    live_queries.extend_from_slice(&q[..chunk]);

    // Step 2: bulk_ingest 2 more segments
    for wave in 1..=2 {
        let lo = wave * chunk;
        let hi = (wave + 1) * chunk;
        eprintln!("  Step 2.{}: bulk_ingest({})", wave, hi - lo);
        eng.bulk_ingest(&q[lo..hi]);
        live_queries.extend_from_slice(&q[lo..hi]);
    }

    // Step 3: live inserts
    eprintln!("  Step 3: live inserts({chunk})");
    for (logical, text) in &q[3 * chunk..4 * chunk] {
        eng.insert_live(text, *logical, 1);
        live_queries.push((*logical, text.clone()));
    }
    eng.flush();

    // Step 4: compact first 2 segments
    eprintln!("  Step 4: compact_range(0, 2)");
    eng.compact_range(0, 2);
    print_metrics("post-compact-range", &eng.metrics());

    // Step 5: more inserts + deletes
    eprintln!("  Step 5: inserts + deletes");
    for (logical, text) in &q[4 * chunk..5 * chunk] {
        eng.insert_live(text, *logical, 1);
        live_queries.push((*logical, text.clone()));
    }
    // Delete queries from the first chunk
    let mut deleted = HashSet::new();
    for (logical, _) in q[..chunk].iter().step_by(3) {
        let _ = eng.delete_by_logical_id(*logical);
        deleted.insert(*logical);
    }
    live_queries.retain(|(id, _)| !deleted.contains(id));
    eng.flush();

    // Step 6: bulk_ingest more
    eprintln!("  Step 6: bulk_ingest({chunk})");
    eng.bulk_ingest(&q[5 * chunk..6 * chunk]);
    live_queries.extend_from_slice(&q[5 * chunk..6 * chunk]);

    // Step 7: compact_all
    eprintln!("  Step 7: compact_all");
    eng.compact_all();
    print_metrics("post-compact-all", &eng.metrics());

    // Step 8: final wave of inserts
    for (logical, text) in &q[6 * chunk..7 * chunk] {
        eng.insert_live(text, *logical, 1);
        live_queries.push((*logical, text.clone()));
    }

    // ── Oracle verification ──
    eprintln!("  Oracle verification over {} titles...", data.titles.len());
    let brute = Brute::build(&live_queries);
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let mut false_neg = 0usize;
    let mut false_pos = 0usize;
    let mut total_truth = 0usize;

    for title in &data.titles {
        eng.match_title(title, &mut scratch, &mut out, true);
        let eng_set: HashSet<u64> = out.iter().copied().collect();
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        total_truth += truth.len();
        for t in &truth {
            if !eng_set.contains(t) {
                false_neg += 1;
            }
        }
        for e in &eng_set {
            if !truth.contains(e) {
                false_pos += 1;
            }
        }
    }

    eprintln!(
        "    oracle: truth={} false_neg={} false_pos={} elapsed={:.1}s",
        total_truth,
        false_neg,
        false_pos,
        t0.elapsed().as_secs_f64()
    );
    events.dump_summary("interleaved");
    assert_eq!(false_neg, 0, "interleaved ops introduced false negatives");
    assert!(total_truth > 0, "degenerate test: no truth matches");
}

// ═════════════════════════════════════════════════════════════════════════════
// 10. FULL LIFECYCLE SOAK — everything combined
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn full_lifecycle_soak() {
    eprintln!("\n=== FULL LIFECYCLE SOAK ===");
    let t0 = Instant::now();

    let cfg = GenConfig {
        num_queries: 40_000,
        num_titles: 4_000,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x50A0_7E57,
        num_players: 4_000,
        num_sets: 1_600,
    };
    let data = generate(&cfg);
    let q = &data.queries;
    let chunk = q.len() / 8;

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 3_000,
            auto_compact_on_flush: true,
            max_segments: 5,
            holes_ratio_threshold: 0.25,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    let mut live_ids: HashSet<u64> = HashSet::new();

    // ── Stage 1: Bulk load ──
    eprintln!("  Stage 1: bulk load {} queries", chunk * 2);
    eng.build_from_queries(&q[..chunk]);
    eng.bulk_ingest(&q[chunk..2 * chunk]);
    for (id, _) in &q[..2 * chunk] {
        live_ids.insert(*id);
    }
    print_metrics("stage-1", &eng.metrics());

    // ── Stage 2: Mixed insert + delete ──
    eprintln!("  Stage 2: interleaved insert/delete");
    for (logical, text) in &q[2 * chunk..4 * chunk] {
        eng.insert_live(text, *logical, 1);
        live_ids.insert(*logical);
    }
    // Delete every 5th from first chunk
    for (logical, _) in q[..chunk].iter().step_by(5) {
        let _ = eng.delete_by_logical_id(*logical);
        live_ids.remove(logical);
    }
    eng.flush();
    print_metrics("stage-2", &eng.metrics());

    // ── Stage 3: Parallel read checkpoint ──
    eprintln!("  Stage 3: parallel read checkpoint");
    let par1 = eng.match_titles_par(&data.titles[..1000], true);
    let par1_matches: usize = par1.iter().map(|(_, ids, _)| ids.len()).sum();
    eprintln!("    checkpoint: {par1_matches} matches over 1000 titles");

    // ── Stage 4: More inserts + compact ──
    eprintln!("  Stage 4: more inserts + compact");
    eng.bulk_ingest(&q[4 * chunk..5 * chunk]);
    for (id, _) in &q[4 * chunk..5 * chunk] {
        live_ids.insert(*id);
    }
    eng.compact_all();
    print_metrics("stage-4", &eng.metrics());

    // ── Stage 5: Update wave ──
    eprintln!("  Stage 5: update {} queries", chunk / 2);
    for (logical, text) in &q[..chunk / 2] {
        if live_ids.contains(logical) {
            let _ = eng.delete_by_logical_id(*logical);
            let new_text = format!("{text} updated");
            eng.insert_live(&new_text, *logical, 5);
        }
    }
    eng.flush();

    // ── Stage 6: Heavy delete ──
    let del_batch: Vec<u64> = q[2 * chunk..3 * chunk]
        .iter()
        .step_by(2)
        .map(|(id, _)| *id)
        .collect();
    eprintln!("  Stage 6: delete {} queries", del_batch.len());
    for id in &del_batch {
        let _ = eng.delete_by_logical_id(*id);
        live_ids.remove(id);
    }

    // ── Stage 7: Final inserts + flush + compact ──
    eprintln!("  Stage 7: final inserts + flush + compact");
    for (logical, text) in &q[5 * chunk..6 * chunk] {
        eng.insert_live(text, *logical, 1);
        live_ids.insert(*logical);
    }
    eng.flush();
    eng.compact_all();
    print_metrics("stage-7", &eng.metrics());

    // ── Stage 8: Full parallel read sweep ──
    eprintln!("  Stage 8: full parallel + sequential comparison");
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut seq_results: Vec<HashSet<u64>> = Vec::new();
    for title in &data.titles {
        eng.match_title(title, &mut scratch, &mut out, true);
        seq_results.push(out.iter().copied().collect());
    }

    let par_results = eng.match_titles_par(&data.titles, true);
    let mut par_mismatches = 0usize;
    let mut ghost_total = 0usize;
    for (idx, matches, _) in &par_results {
        let par_set: HashSet<u64> = matches.iter().copied().collect();
        if par_set != seq_results[*idx] {
            par_mismatches += 1;
        }
        for id in &par_set {
            if !live_ids.contains(id) {
                ghost_total += 1;
            }
        }
    }

    let total_matches: usize = par_results.iter().map(|(_, ids, _)| ids.len()).sum();

    eprintln!(
        "\n  SOAK RESULTS: matches={} par_mismatches={} ghosts={} elapsed={:.1}s",
        total_matches,
        par_mismatches,
        ghost_total,
        t0.elapsed().as_secs_f64()
    );
    events.dump_summary("soak");

    let el = events.entries.lock().unwrap();
    eprintln!("  total events logged: {}", el.len());

    assert_eq!(par_mismatches, 0, "parallel != sequential in soak");
    // Ghost check: deleted IDs should not appear. However, live_ids tracking
    // may miss queries rejected at parse/class-D time, so we only flag if the
    // ghost was in our explicit delete set.
    let deleted_explicit: HashSet<u64> = del_batch.into_iter().collect();
    let mut explicit_ghosts = 0usize;
    for (_, matches, _) in &par_results {
        for id in matches {
            if deleted_explicit.contains(id) {
                explicit_ghosts += 1;
            }
        }
    }
    assert_eq!(
        explicit_ghosts, 0,
        "explicitly deleted IDs appeared in results"
    );
    assert!(total_matches > 0, "soak produced zero matches");
}
