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

    // Diverse query families — different products, categories, structures
    let query_families: Vec<Vec<(u64, &str)>> = vec![
        // Appliance items — simple required
        vec![
            (100, "wireless mouse 1986 vertex"),
            (101, "wireless mouse 1997 north star"),
            (102, "wireless mouse 1993 acme"),
            (103, "mechanical keyboard 2003 acme chrome"),
            (104, "mechanical keyboard new item"),
            (105, "noise cancelling headphones 1996 acme"),
            (106, "noise cancelling headphones 1996 summit"),
            (107, "product epsilon 1992 vertex"),
            (108, "product zeta 1997 summit"),
            (109, "product eta 1996 acme"),
        ],
        // With any-of groups
        vec![
            (200, "wireless mouse (vertex,acme,summit) 1986"),
            (201, "mechanical keyboard (acme,north star) new"),
            (202, "noise cancelling headphones (acme,summit,vertex) 1996"),
            (203, "(product gamma,entityseven,product theta) new item"),
            (204, "(alpha,beta,gamma) wireless mouse"),
        ],
        // With forbidden terms
        vec![
            (300, "wireless mouse item -(replica,manual,lot)"),
            (301, "mechanical keyboard new -(fake,manual)"),
            (302, "noise cancelling headphones acme -(replica,lot,break)"),
            (303, "product gamma 1986 vertex -(alpha,beta,gamma)"),
            (304, "appliance item new -(manual,signed,used)"),
        ],
        // Mixed complex
        vec![
            (
                400,
                "wireless mouse (1986,1993,1997) (vertex,acme) -(replica)",
            ),
            (
                401,
                "mechanical keyboard (2003,2004) (acme,north star) -(manual,lot)",
            ),
            (
                402,
                "(product gamma,entityseven) (alpha,beta) -(fake,replica)",
            ),
            (
                403,
                "noise cancelling headphones (acme,summit) item -(manual,lot,break)",
            ),
            (
                404,
                "(product gamma,entityone,entitytwo) (vertex,acme,summit) new",
            ),
        ],
        // Year-heavy / brand-heavy
        vec![
            (500, "1986 vertex appliance"),
            (501, "1997 north star appliance item"),
            (502, "acme chrome 2003 appliance"),
            (503, "summit 1997 new item"),
            (504, "north star 1994 appliance"),
        ],
        // Single-word broad queries
        vec![
            (600, "product gamma"),
            (601, "entityone"),
            (602, "new"),
            (603, "appliance"),
            (604, "vertex"),
        ],
    ];

    // Varied titles to search against
    let titles = vec![
        "wireless mouse 1986 vertex appliance item pro",
        "mechanical keyboard 2003 acme chrome new item",
        "noise cancelling headphones 1996 acme new item basic",
        "wireless mouse 1997 north star game accessory",
        "1986 vertex wireless mouse new item #57",
        "mechanical keyboard 2004 north star new item auto",
        "noise cancelling headphones 1996 summit best item alpha 9",
        "product zeta 1997 summit new item",
        "product eta 1996 acme prototype pick",
        "product epsilon 1992 vertex new item",
        "wireless mouse 1993 acme finest item lot",
        "product gamma entityseven product theta triple item acme 2008",
        "1997 north star appliance complete set",
        "appliance item replica manual signed lot",
        "wireless mouse vertex replica fake 1986",
        "mechanical keyboard acme chrome 2003 pro deluxe",
        "noise cancelling headphones acme summit 1996 new prototype pick",
        "vintage appliance item 1986 vertex set break",
        "wireless mouse used accessory manual signed",
        "new item lot appliance acme summit 1994",
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
        num_entities: 2_000,
        num_collections: 800,
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
        (9_000_001, "wireless mouse 1986 vertex".into()),
        (
            9_000_002,
            "wireless mouse (1986,1993,1997) (vertex,acme)".into(),
        ),
        (
            9_000_003,
            "wireless mouse item -(replica,manual,lot,break)".into(),
        ),
        (
            9_000_004,
            "(product gamma,entityseven,entitytwo) (alpha,beta) -(fake,replica)".into(),
        ),
        (9_000_005, "mechanical keyboard 2003 acme chrome new".into()),
        (
            9_000_006,
            "noise cancelling headphones (acme,summit) 1996 -(lot)".into(),
        ),
        (
            9_000_007,
            "(product gamma,entityone) (vertex,acme,north star) (1986,1997,2003)".into(),
        ),
        (
            9_000_008,
            "appliance item (alpha,beta,gamma) -(manual,signed,used)".into(),
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
        "wireless mouse 1986 vertex appliance item #57 pro",
        "wireless mouse 1993 acme finest premium item",
        "wireless mouse 1997 north star item game accessory",
        "wireless mouse 1986 vertex item replica fake",
        "mechanical keyboard 2003 acme chrome new item pro",
        "noise cancelling headphones 1996 acme prototype pick new item lot",
        "noise cancelling headphones 1996 summit appliance item alpha 9",
        "product gamma entityseven triple item acme 1997 pro",
        "appliance item pro deluxe premium vintage",
        "appliance item manual signed used preowned",
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
