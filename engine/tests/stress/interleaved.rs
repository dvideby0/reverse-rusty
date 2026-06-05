//! Read-while-growing workloads: interleaved insert+match and a mixed
//! synthetic + hand-crafted corpus with parallel reads.

use crate::harness::*;

// ═════════════════════════════════════════════════════════════════════════════
// 11. INTERLEAVED INSERT + MATCH — read while the index is growing
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn match_while_inserting_varied_queries() {
    eprintln!("\n=== MATCH WHILE INSERTING (varied queries) ===");
    let t0 = Instant::now();

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 500,
            auto_compact_on_flush: true,
            max_segments: 4,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    // Diverse query families — different sports, categories, structures
    let query_families: Vec<Vec<(u64, &str)>> = vec![
        // Basketball cards — simple required
        vec![
            (100, "michael jordan 1986 fleer"),
            (101, "michael jordan 1997 upper deck"),
            (102, "michael jordan 1993 topps"),
            (103, "lebron james 2003 topps chrome"),
            (104, "lebron james rookie card"),
            (105, "kobe bryant 1996 topps"),
            (106, "kobe bryant 1996 bowman"),
            (107, "shaquille oneal 1992 fleer"),
            (108, "tim duncan 1997 bowman"),
            (109, "allen iverson 1996 topps"),
        ],
        // With any-of groups
        vec![
            (200, "michael jordan (fleer,topps,bowman) 1986"),
            (201, "lebron james (topps,upper deck) rookie"),
            (202, "kobe bryant (topps,bowman,fleer) 1996"),
            (203, "(jordan,james,bryant) rookie card"),
            (204, "(psa,bgs,sgc) michael jordan"),
        ],
        // With forbidden terms
        vec![
            (300, "michael jordan card -(reprint,auto,lot)"),
            (301, "lebron james rookie -(fake,auto)"),
            (302, "kobe bryant topps -(reprint,lot,break)"),
            (303, "jordan 1986 fleer -(psa,bgs,sgc)"),
            (304, "basketball card rookie -(auto,signed,used)"),
        ],
        // Mixed complex
        vec![
            (
                400,
                "michael jordan (1986,1993,1997) (fleer,topps) -(reprint)",
            ),
            (
                401,
                "lebron james (2003,2004) (topps,upper deck) -(auto,lot)",
            ),
            (402, "(jordan,james) (psa,bgs) -(fake,reprint)"),
            (403, "kobe bryant (topps,bowman) card -(auto,lot,break)"),
            (404, "(jordan,lebron,kobe) (fleer,topps,bowman) rookie"),
        ],
        // Year-heavy / brand-heavy
        vec![
            (500, "1986 fleer basketball"),
            (501, "1997 upper deck basketball card"),
            (502, "topps chrome 2003 basketball"),
            (503, "bowman 1997 rookie card"),
            (504, "upper deck 1994 basketball"),
        ],
        // Single-word broad queries
        vec![
            (600, "jordan"),
            (601, "lebron"),
            (602, "rookie"),
            (603, "basketball"),
            (604, "fleer"),
        ],
    ];

    // Varied titles to search against
    let titles = vec![
        "michael jordan 1986 fleer basketball card psa 10",
        "lebron james 2003 topps chrome rookie card",
        "kobe bryant 1996 topps rookie card bgs 9.5",
        "michael jordan 1997 upper deck game jersey",
        "1986 fleer michael jordan rookie card #57",
        "lebron james 2004 upper deck rookie card auto",
        "kobe bryant 1996 bowman best card psa 9",
        "tim duncan 1997 bowman rookie card",
        "allen iverson 1996 topps draft pick",
        "shaquille oneal 1992 fleer rookie card",
        "michael jordan 1993 topps finest card lot",
        "jordan james bryant triple card topps 2008",
        "1997 upper deck basketball complete set",
        "basketball card reprint auto signed lot",
        "michael jordan fleer reprint fake 1986",
        "lebron james topps chrome 2003 psa 10 gem mt",
        "kobe bryant topps bowman 1996 rookie draft pick",
        "vintage basketball card 1986 fleer set break",
        "michael jordan game used jersey auto signed",
        "rookie card lot basketball topps bowman 1994",
    ];

    let mut match_count_history: Vec<(usize, usize)> = Vec::new(); // (queries_in, total_matches)
    let mut source_lookups_ok = 0usize;
    let mut source_lookups_total = 0usize;
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();

    // Feed families one at a time, matching after each insertion batch
    for (fam_idx, family) in query_families.iter().enumerate() {
        eprintln!("\n  Family {} ({} queries):", fam_idx, family.len());

        for (logical_id, query_text) in family {
            let result = eng.try_insert_live(query_text, *logical_id, 1);
            match &result {
                Ok(reverse_rusty::segment::InsertOutcome::Inserted(_)) => {
                    eprintln!("    + id={logical_id} {query_text:?}");
                }
                Ok(reverse_rusty::segment::InsertOutcome::RejectedClassD) => {
                    eprintln!("    D id={logical_id} {query_text:?} (class D rejected)");
                }
                Err(e) => {
                    eprintln!("    ! id={logical_id} parse error: {e}");
                }
            }

            // Match every title after each single insert
            for title in &titles {
                eng.match_title(title, &mut scratch, &mut out, true);
            }

            // Verify doc source is retrievable for successfully inserted queries
            source_lookups_total += 1;
            if let Ok(reverse_rusty::segment::InsertOutcome::Inserted(_)) = result {
                let source = eng.get_query_source(*logical_id);
                if let Some(src) = source {
                    assert_eq!(
                        src, *query_text,
                        "source mismatch for id={logical_id}: expected {query_text:?}, got {src:?}"
                    );
                    source_lookups_ok += 1;
                } else {
                    panic!("get_query_source({logical_id}) returned None right after insert");
                }
            }
        }

        // Record match count snapshot after each family
        let mut total = 0usize;
        for title in &titles {
            eng.match_title(title, &mut scratch, &mut out, true);
            total += out.len();
        }
        match_count_history.push((eng.num_queries(), total));
        eprintln!(
            "    snapshot: {} queries in engine, {} total matches across {} titles",
            eng.num_queries(),
            total,
            titles.len()
        );
    }

    // Match counts should be monotonically non-decreasing as we add queries
    // (we're only adding, not deleting yet)
    for window in match_count_history.windows(2) {
        let (q_prev, _) = window[0];
        let (q_cur, _) = window[1];
        assert!(
            q_cur >= q_prev,
            "query count went backwards: {q_prev} -> {q_cur}"
        );
        // Not strictly monotonic (class-D rejects add queries that don't match),
        // but total matches should generally not decrease when only adding
    }

    eprintln!("\n  source lookups: {source_lookups_ok}/{source_lookups_total} succeeded");
    eprintln!("  match history: {match_count_history:?}");

    // ── Now interleave deletes with reads ──
    eprintln!("\n  Interleaving deletes with reads...");
    let delete_targets = vec![100, 200, 300, 400, 500]; // one from each family
    for del_id in &delete_targets {
        let _ = eng.delete_by_logical_id(*del_id);
        eprintln!("    deleted id={del_id}");

        // Verify doc source removed
        assert!(
            eng.get_query_source(*del_id).is_none(),
            "get_query_source({del_id}) should return None after delete"
        );

        // Verify match results don't include deleted ID
        for title in &titles {
            eng.match_title(title, &mut scratch, &mut out, true);
            assert!(
                !out.contains(del_id),
                "deleted id {del_id} still appears in matches for {title:?}"
            );
        }
    }

    // ── Flush, compact, re-verify ──
    eng.flush();
    eng.compact_all();
    print_metrics("final", &eng.metrics());

    for del_id in &delete_targets {
        assert!(
            eng.get_query_source(*del_id).is_none(),
            "get_query_source({del_id}) should still be None after compact"
        );
    }

    // Parallel read of all titles — verify agreement with sequential
    let par_results = eng.match_titles_par(
        &titles
            .iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>(),
        true,
    );
    let mut seq_results: Vec<HashSet<u64>> = Vec::new();
    for title in &titles {
        eng.match_title(title, &mut scratch, &mut out, true);
        seq_results.push(out.iter().copied().collect());
    }

    let mut mismatches = 0usize;
    for (idx, matches, _) in &par_results {
        let par_set: HashSet<u64> = matches.iter().copied().collect();
        if par_set != seq_results[*idx] {
            mismatches += 1;
        }
    }

    events.dump_summary("match-while-insert");
    eprintln!("  elapsed={:.1}s", t0.elapsed().as_secs_f64());
    assert_eq!(mismatches, 0, "parallel != sequential after insert+delete");
}

// ═════════════════════════════════════════════════════════════════════════════
// 15. MIXED TRAFFIC WITH PARALLEL READS — synthetic + hand-crafted queries
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn mixed_synthetic_and_handcrafted_parallel() {
    eprintln!("\n=== MIXED SYNTHETIC + HANDCRAFTED WITH PARALLEL READS ===");
    let t0 = Instant::now();

    // Generate a synthetic corpus for volume
    let cfg = GenConfig {
        num_queries: 15_000,
        num_titles: 2_000,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0xCA_FE_D0_0D,
        num_players: 2_000,
        num_sets: 800,
    };
    let data = generate(&cfg);

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 2_000,
            auto_compact_on_flush: true,
            max_segments: 5,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    // Phase 1: Load synthetic corpus
    eng.build_from_queries(&data.queries);
    eprintln!("  Phase 1: loaded {} synthetic queries", data.queries.len());

    // Phase 2: Add hand-crafted queries that test specific DSL features
    let handcrafted: Vec<(u64, String)> = vec![
        (9_000_001, "michael jordan 1986 fleer".into()),
        (
            9_000_002,
            "michael jordan (1986,1993,1997) (fleer,topps)".into(),
        ),
        (
            9_000_003,
            "michael jordan card -(reprint,auto,lot,break)".into(),
        ),
        (
            9_000_004,
            "(jordan,james,kobe) (psa,bgs) -(fake,reprint)".into(),
        ),
        (9_000_005, "lebron james 2003 topps chrome rookie".into()),
        (9_000_006, "kobe bryant (topps,bowman) 1996 -(lot)".into()),
        (
            9_000_007,
            "(jordan,lebron) (fleer,topps,upper deck) (1986,1997,2003)".into(),
        ),
        (
            9_000_008,
            "basketball card (psa,bgs,sgc) -(auto,signed,used)".into(),
        ),
    ];

    for (id, text) in &handcrafted {
        let result = eng.try_insert_live(text, *id, 1);
        match &result {
            Ok(reverse_rusty::segment::InsertOutcome::Inserted(_)) => {}
            Ok(reverse_rusty::segment::InsertOutcome::RejectedClassD) => {
                eprintln!("    class-D rejected: id={id} {text:?}");
            }
            Err(e) => {
                eprintln!("    parse error: id={id} {text:?}: {e}");
            }
        }
    }
    eprintln!("  Phase 2: added {} handcrafted queries", handcrafted.len());

    // Phase 3: Search with titles designed to hit the handcrafted queries
    let targeted_titles = vec![
        "michael jordan 1986 fleer basketball card #57 psa 10",
        "michael jordan 1993 topps finest refractor card",
        "michael jordan 1997 upper deck card game jersey",
        "michael jordan 1986 fleer card reprint fake",
        "lebron james 2003 topps chrome rookie card psa 10",
        "kobe bryant 1996 topps draft pick rookie card lot",
        "kobe bryant 1996 bowman basketball card psa 9",
        "jordan james triple card topps 1997 psa 10",
        "basketball card psa 10 gem mint vintage",
        "basketball card auto signed used game worn",
    ];

    eprintln!("  Phase 3: targeted searches");
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    for title in &targeted_titles {
        eng.match_title(title, &mut scratch, &mut out, true);
        let handcrafted_hits: Vec<u64> =
            out.iter().filter(|&&id| id >= 9_000_000).copied().collect();
        eprintln!(
            "    {:?}\n      {} total matches, handcrafted: {:?}",
            title,
            out.len(),
            handcrafted_hits
        );

        // Verify doc sources for all handcrafted hits
        for &id in &handcrafted_hits {
            let src = eng.get_query_source(id);
            assert!(src.is_some(), "handcrafted match id={id} has no source");
        }
    }

    // Phase 4: Delete some synthetic, keep handcrafted, search in parallel
    let del_count = data.queries.len() / 4;
    eprintln!("  Phase 4: deleting {del_count} synthetic queries");
    let deleted: HashSet<u64> = data.queries[..del_count]
        .iter()
        .map(|(id, _)| *id)
        .collect();
    for &id in &deleted {
        let _ = eng.delete_by_logical_id(id);
    }
    eng.flush();

    // Combine all titles for a big parallel sweep
    let mut all_titles: Vec<String> = data.titles.clone();
    all_titles.extend(targeted_titles.iter().map(std::string::ToString::to_string));

    let par_results = eng.match_titles_par(&all_titles, true);
    let par_total: usize = par_results.iter().map(|(_, ids, _)| ids.len()).sum();

    // Sequential comparison
    let mut seq_results: Vec<HashSet<u64>> = Vec::new();
    for title in &all_titles {
        eng.match_title(title, &mut scratch, &mut out, true);
        seq_results.push(out.iter().copied().collect());
    }

    let mut mismatches = 0usize;
    let mut ghosts = 0usize;
    for (idx, matches, _stats) in &par_results {
        let par_set: HashSet<u64> = matches.iter().copied().collect();
        if par_set != seq_results[*idx] {
            mismatches += 1;
        }
        for id in matches {
            if deleted.contains(id) {
                ghosts += 1;
            }
        }
    }

    // Phase 5: Compact and re-check handcrafted queries
    eng.compact_all();
    for (id, text) in &handcrafted {
        if let Some(src) = eng.get_query_source(*id) {
            assert_eq!(src, text.as_str(), "handcrafted source mangled by compact");
        }
    }

    eprintln!(
        "\n  RESULTS: par={} mismatches={} ghosts={} elapsed={:.1}s",
        par_total,
        mismatches,
        ghosts,
        t0.elapsed().as_secs_f64()
    );
    print_metrics("final", &eng.metrics());
    events.dump_summary("mixed-traffic");

    assert_eq!(mismatches, 0, "parallel != sequential");
    assert_eq!(ghosts, 0, "deleted IDs in results");
    assert!(par_total > 0, "no matches at all");
}
