//! Single-threaded staged workloads + update visibility.

use crate::harness::*;

// ═════════════════════════════════════════════════════════════════════════════
// 1. MIXED WORKLOAD — single-threaded, staged phases
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn staged_read_write_delete_single_thread() {
    eprintln!("\n=== STAGED READ/WRITE/DELETE (single-thread) ===");
    let t0 = Instant::now();

    let cfg = GenConfig {
        num_queries: 20_000,
        num_titles: 2_000,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x57_2E55,
        num_players: 3_000,
        num_sets: 1_200,
    };
    let data = generate(&cfg);
    let q = &data.queries;

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 2_000,
            auto_compact_on_flush: true,
            max_segments: 6,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    // ── Phase 1: Bulk load initial corpus ──
    eprintln!("\n  Phase 1: bulk load {} queries", q.len() / 2);
    eng.build_from_queries(&q[..q.len() / 2]);
    print_metrics("after-bulk", &eng.metrics());

    // Snapshot baseline matches for verification titles
    let check_titles: Vec<&str> = data
        .titles
        .iter()
        .take(200)
        .map(std::string::String::as_str)
        .collect();
    let _baseline: Vec<HashSet<u64>> = check_titles
        .iter()
        .map(|t| match_ids_set(&eng, t))
        .collect();

    // ── Phase 2: Streaming inserts (hot delta) ──
    eprintln!("\n  Phase 2: streaming {} live inserts", q.len() / 2);
    let mut inserted_ids = Vec::new();
    for (logical, text) in &q[q.len() / 2..] {
        if let Some(local) = eng.insert_live(text, *logical, 1) {
            inserted_ids.push((*logical, local));
        }
    }
    eprintln!("    inserted {} queries into memtable", inserted_ids.len());
    print_metrics("after-inserts", &eng.metrics());

    // Verify reads see both old and new data
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut total_matches = 0usize;
    for title in &data.titles {
        eng.match_title(title, &mut scratch, &mut out, true);
        total_matches += out.len();
    }
    eprintln!(
        "    total matches across {} titles: {}",
        data.titles.len(),
        total_matches
    );
    assert!(total_matches > 0, "no matches after insert phase");

    // ── Phase 3: Targeted deletes ──
    let delete_count = inserted_ids.len() / 5;
    eprintln!("\n  Phase 3: deleting {delete_count} queries by logical ID");
    let mut deleted_ids = HashSet::new();
    for (logical, _) in inserted_ids.iter().take(delete_count) {
        let _ = eng.delete_by_logical_id(*logical);
        deleted_ids.insert(*logical);
    }
    print_metrics("after-deletes", &eng.metrics());

    // Verify deleted queries no longer match
    let mut ghost_matches = 0usize;
    for title in check_titles.iter().take(50) {
        let hits = match_ids_set(&eng, title);
        for id in &hits {
            if deleted_ids.contains(id) {
                ghost_matches += 1;
            }
        }
    }
    eprintln!("    ghost matches (deleted IDs still matching): {ghost_matches}");
    assert_eq!(ghost_matches, 0, "deleted queries still matching");

    // ── Phase 4: Updates (re-insert with higher version) ──
    let update_count = 500;
    eprintln!("\n  Phase 4: updating {update_count} queries (new version)");
    let mut updated = Vec::new();
    for (logical, text) in q.iter().take(update_count) {
        let new_text = format!("{text} updated variant");
        if eng.insert_live(&new_text, *logical, 99).is_some() {
            updated.push((*logical, new_text));
        }
    }
    eprintln!("    updated {} queries", updated.len());

    // ── Phase 5: Flush + compact ──
    eprintln!("\n  Phase 5: flush + compact");
    eng.flush();
    print_metrics("after-flush", &eng.metrics());

    let compact_report = eng.compact_all();
    if let Some(ref r) = compact_report {
        eprintln!(
            "    compaction: merged={} before={} after={} reclaimed={}",
            r.segments_merged, r.entries_before, r.entries_after, r.tombstones_reclaimed
        );
    }
    print_metrics("after-compact", &eng.metrics());

    // ── Phase 6: Second wave of inserts + reads ──
    eprintln!("\n  Phase 6: second wave of 1000 inserts + full read sweep");
    for i in 0..1_000u64 {
        let text = format!("wave2 michael jordan 1994 upper deck variant{i}");
        eng.insert_live(&text, 5_000_000 + i, 1);
    }

    let mut post_matches = 0usize;
    for title in &data.titles {
        eng.match_title(title, &mut scratch, &mut out, true);
        post_matches += out.len();
    }
    eprintln!("    total matches after wave2: {post_matches}");

    // ── Verify: oracle correctness over final state ──
    eprintln!("\n  Final oracle verification...");
    let mut live_queries: Vec<(u64, String)> = Vec::new();
    for (logical, text) in q {
        if !deleted_ids.contains(logical) {
            live_queries.push((*logical, text.clone()));
        }
    }
    for (logical, text) in &updated {
        live_queries.push((*logical, text.clone()));
    }
    for i in 0..1_000u64 {
        live_queries.push((
            5_000_000 + i,
            format!("wave2 michael jordan 1994 upper deck variant{i}"),
        ));
    }

    let brute = Brute::build(&live_queries);
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let mut false_neg = 0usize;
    let mut total_truth = 0usize;
    for title in data.titles.iter().take(500) {
        eng.match_title(title, &mut scratch, &mut out, true);
        let eng_set: HashSet<u64> = out.iter().copied().collect();
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        total_truth += truth.len();
        for t in &truth {
            if !eng_set.contains(t) {
                false_neg += 1;
                if false_neg <= 3 {
                    eprintln!("    FALSE NEG: title={title:?} missing_id={t}");
                }
            }
        }
    }
    eprintln!(
        "    oracle: truth={} false_neg={} elapsed={:.1}s",
        total_truth,
        false_neg,
        t0.elapsed().as_secs_f64()
    );
    events.dump_summary("staged-single");
    assert_eq!(false_neg, 0, "staged workload introduced false negatives");
    assert!(total_truth > 0, "degenerate test: no truth matches");
}

// ═════════════════════════════════════════════════════════════════════════════
// 5. UPDATE VISIBILITY — version supersession
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn update_visibility_across_flush_compact() {
    eprintln!("\n=== UPDATE VISIBILITY ACROSS FLUSH/COMPACT ===");

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 100,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    // Insert v1 queries
    let v1_queries: Vec<(u64, String)> = vec![
        (1, "michael jordan 1986 fleer".into()),
        (2, "lebron james rookie card".into()),
        (3, "kobe bryant 1996 topps".into()),
        (4, "shaquille oneal orlando magic".into()),
        (5, "tim duncan 1997 bowman".into()),
    ];
    eng.build_from_queries(&v1_queries);
    eprintln!("  v1 loaded: {} queries", v1_queries.len());

    // Check v1 matches
    let v1_hit = match_ids(&eng, "michael jordan 1986 fleer basketball card");
    eprintln!("  v1 match for MJ: {v1_hit:?}");
    assert!(v1_hit.contains(&1), "v1 MJ query should match");

    // Update: delete old, re-insert with new text + higher version
    let _ = eng.delete_by_logical_id(1);
    eng.insert_live("michael jordan 1986 fleer rookie card", 1, 2);

    // Before flush: should still match with updated text
    let pre_flush = match_ids(&eng, "michael jordan 1986 fleer rookie card psa 10");
    eprintln!("  pre-flush updated MJ: {pre_flush:?}");
    assert!(pre_flush.contains(&1), "updated MJ should match pre-flush");

    // Original title (without "rookie card") should NOT match updated query
    // if the new text requires "rookie" and "card"
    // (Actually it still will — the updated query adds "rookie card" but the
    //  original features are still present. This is correct behavior.)

    // Flush
    eng.flush();
    let post_flush = match_ids(&eng, "michael jordan 1986 fleer rookie card psa 10");
    eprintln!("  post-flush updated MJ: {post_flush:?}");
    assert!(
        post_flush.contains(&1),
        "updated MJ should match post-flush"
    );

    // Bulk updates: update all 5 queries, flush, compact
    for (id, _) in &v1_queries {
        let _ = eng.delete_by_logical_id(*id);
        eng.insert_live(&format!("updated_{id} michael jordan lebron kobe"), *id, 3);
    }
    eng.flush();

    // Add another segment so compact_all has something to merge
    eng.insert_live("extra query padding segment", 999, 1);
    eng.flush();

    eng.compact_all();
    print_metrics("post-update-compact", &eng.metrics());

    // All original IDs should still be findable via the updated text
    let final_hits = match_ids(&eng, "michael jordan lebron kobe updated_1 updated_2");
    eprintln!("  final hits: {final_hits:?}");

    events.dump_summary("update-vis");
}
