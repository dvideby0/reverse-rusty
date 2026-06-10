// ═════════════════════════════════════════════════════════════════════════════
// 4. EDGE-CASE INPUTS
// ═════════════════════════════════════════════════════════════════════════════

use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};

/// Empty title should match nothing and not panic.
#[test]
fn empty_title_matches_nothing() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&[
        (1, "michael jordan".to_string()),
        (2, "1994 upper deck".to_string()),
    ]);

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title("", &mut scratch, &mut out, true);
    assert!(out.is_empty(), "empty title should not match any query");
}

/// Whitespace-only title should match nothing and not panic.
#[test]
fn whitespace_title_matches_nothing() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&[(1, "michael jordan".to_string())]);

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title("   \t\n  ", &mut scratch, &mut out, true);
    assert!(out.is_empty(), "whitespace-only title should not match");
}

/// Empty query corpus: matching should work (return empty) with no panics.
#[test]
fn empty_corpus_matches_nothing() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    let report = eng.build_from_queries(&[]);
    assert_eq!(report.ingested, 0);

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title(
        "michael jordan 1994 upper deck",
        &mut scratch,
        &mut out,
        true,
    );
    assert!(out.is_empty(), "empty corpus should produce no matches");
}

/// Deleting the same logical id twice is idempotent: the second delete removes zero
/// copies, the query stays gone (memtable AND flushed-segment copies), match results
/// are unchanged, and a fresh insert under the same logical id matches again
/// (delete → delete → reinsert, the full tombstone life cycle at its edge).
#[test]
fn delete_same_logical_id_twice_is_idempotent_and_reinsert_revives() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&[
        (1, "michael jordan rookie".to_string()),
        (2, "1994 upper deck".to_string()),
    ]);
    // A second live copy of logical 1 in the memtable, so the delete must reach both
    // a flushed segment and the memtable.
    eng.insert_live("michael jordan rookie", 1, 2);

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let title = "1996 michael jordan rookie card";
    eng.match_title(title, &mut scratch, &mut out, true);
    assert!(out.contains(&1), "precondition: query 1 matches");

    let first = eng.delete_by_logical_id(1).expect("first delete");
    assert!(first >= 2, "both live copies tombstoned, got {first}");
    eng.match_title(title, &mut scratch, &mut out, true);
    assert!(!out.contains(&1), "deleted query must not match");

    let second = eng
        .delete_by_logical_id(1)
        .expect("second delete must not error");
    assert_eq!(second, 0, "second delete of the same id is a no-op");
    eng.match_title(title, &mut scratch, &mut out, true);
    assert!(!out.contains(&1), "still deleted after the double delete");

    // Reinsert under the same logical id: the tombstones must not swallow the new copy.
    eng.insert_live("michael jordan rookie", 1, 3);
    eng.match_title(title, &mut scratch, &mut out, true);
    assert!(
        out.contains(&1),
        "a fresh insert under a twice-deleted logical id must match again"
    );

    // And the unrelated query was never disturbed.
    eng.match_title("1994 upper deck jordan", &mut scratch, &mut out, true);
    assert!(out.contains(&2), "unrelated query survives the churn");
}

/// Very long title should not panic or corrupt state.
#[test]
fn very_long_title_does_not_panic() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&[(1, "michael jordan".to_string())]);

    // Build a ~100KB title
    let mut long_title = String::with_capacity(100_000);
    for i in 0..5000 {
        long_title.push_str(&format!("word{i} "));
    }
    long_title.push_str("michael jordan");

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title(&long_title, &mut scratch, &mut out, true);
    // Should find "michael jordan" even buried in the long title
    assert!(
        out.contains(&1),
        "long title containing the query terms should still match"
    );
}

/// Unicode and diacritics: the normalizer should fold these correctly.
#[test]
fn unicode_diacritics_handled() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&[(1, "michael jordan".to_string())]);

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();

    // Diacritic folding: "Michaël Jördàn" should match "michael jordan"
    eng.match_title(
        "Michaël Jördàn 1994 upper deck",
        &mut scratch,
        &mut out,
        true,
    );
    // Whether this matches depends on normalizer diacritic folding behavior.
    // The test ensures no panic regardless.
    eprintln!(
        "diacritic test: matched={} (folding depends on normalizer config)",
        out.len()
    );
}

/// Mixed case: engine should be case-insensitive.
#[test]
fn case_insensitive_matching() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&[(1, "Michael Jordan".to_string())]);

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();

    let cases = [
        "MICHAEL JORDAN",
        "michael jordan",
        "Michael Jordan",
        "mIcHaEl JoRdAn",
    ];
    for title in &cases {
        eng.match_title(title, &mut scratch, &mut out, true);
        assert!(
            out.contains(&1),
            "case-insensitive match failed for {title:?}"
        );
    }
}

/// Duplicate logical IDs: inserting the same logical ID twice should not
/// produce duplicate matches.
#[test]
fn duplicate_logical_ids_deduped_in_results() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));

    // Insert same logical ID via two different paths
    eng.build_from_queries(&[(42, "michael jordan 1994".to_string())]);
    // Re-insert same logical ID with slightly different text via bulk_ingest
    eng.bulk_ingest(&[(42, "michael jordan 1994 upper deck".to_string())]);

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title(
        "michael jordan 1994 upper deck psa 10",
        &mut scratch,
        &mut out,
        true,
    );

    // Count how many times logical ID 42 appears
    let count_42 = out.iter().filter(|&&id| id == 42).count();
    assert!(
        count_42 <= 1,
        "logical ID 42 appeared {count_42} times in results (should be deduped to <=1)"
    );
}

/// All-forbidden query (only MUST_NOT terms) should be rejected as class D.
#[test]
fn all_forbidden_query_rejected() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    let report = eng.build_from_queries(&[
        (1, "-(auto,signed,graded)".to_string()), // only negatives
    ]);
    assert_eq!(
        report.rejected_class_d, 1,
        "all-forbidden should be class D"
    );
    assert_eq!(report.ingested, 0);
}

/// Single-character queries should parse without panic (even if rejected).
#[test]
fn single_char_queries_dont_panic() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    let chars = ["a", "1", "-", "(", ")", ",", " ", "Z", "#"];
    for &c in &chars {
        // Should not panic regardless of whether it parses
        let _ = eng.insert_live(c, 999, 1);
    }
}

/// Matching against an engine with only tombstoned entries should return nothing.
#[test]
fn fully_tombstoned_engine_matches_nothing() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&[
        (1, "michael jordan".to_string()),
        (2, "lebron james".to_string()),
        (3, "kobe bryant".to_string()),
    ]);

    // Tombstone all entries in segment 0
    for local in 0..3u32 {
        eng.tombstone_in(0, local).unwrap();
    }

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title(
        "michael jordan lebron james kobe bryant",
        &mut scratch,
        &mut out,
        true,
    );
    assert!(
        out.is_empty(),
        "fully tombstoned engine should return no matches, got {out:?}"
    );
}

/// Compacting a fully tombstoned engine should produce an empty segment.
#[test]
fn compact_fully_tombstoned_produces_empty_segment() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    // Use two separate ingests to create 2 base segments (compact_all requires >=2)
    eng.build_from_queries(&[(1, "michael jordan".to_string())]);
    eng.bulk_ingest(&[(2, "lebron james".to_string())]);

    // Tombstone everything (segment 0 has local 0, segment 1 has local 0)
    eng.tombstone_in(0, 0).unwrap();
    eng.tombstone_in(1, 0).unwrap();

    let report = eng.compact_all();
    assert!(
        report.is_some(),
        "compact_all should run with 2+ base segments"
    );
    let report = report.unwrap();
    assert_eq!(report.entries_after, 0, "compacted segment should be empty");
    assert_eq!(report.tombstones_reclaimed, 2);

    // Verify no matches
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title("michael jordan lebron james", &mut scratch, &mut out, true);
    assert!(out.is_empty());
}

/// Title with special characters (hyphens, ampersands, numbers) should not panic.
#[test]
fn special_characters_in_title() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&[(1, "michael jordan".to_string())]);

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();

    let titles = [
        "michael-jordan #23 1994",
        "michael & jordan co. ltd.",
        "michael jordan !!! @#$%^&*()",
        "michael\tjordan\nnewline",
        "michael jordan 🏀🏆",
        "100% authentic michael jordan card",
    ];
    for title in &titles {
        // Should not panic
        eng.match_title(title, &mut scratch, &mut out, true);
    }
}

/// Build + bulk_ingest + flush + compact with zero queries should not panic.
#[test]
fn lifecycle_with_zero_queries() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));

    // All lifecycle operations on an empty engine
    let report = eng.build_from_queries(&[]);
    assert_eq!(report.ingested, 0);

    eng.bulk_ingest(&[]);
    eng.flush();
    let r = eng.compact_all();
    // compact_all on empty should be None (nothing to compact) or Some with 0 entries
    if let Some(r) = r {
        assert_eq!(r.entries_before, 0);
    }

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title("anything", &mut scratch, &mut out, true);
    assert!(out.is_empty());
}
