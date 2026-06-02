//! Integration tests for the three hardening fixes:
//!   1. Vocab-epoch staleness enforcement after set_vocab()
//!   2. Zero unwrap() — corrupt data handled gracefully in storage/WAL
//!   3. Per-segment reverse index makes delete_by_logical_id O(segments)
//!
//! This file exercises all three interacting simultaneously: build → vocab change
//! → delete → flush → compact → persist → reopen → verify correctness throughout.

use reverse_rusty::config::EngineConfig;
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::vocab::Vocab;
use std::path::PathBuf;

fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("reverse_rusty_hardening_{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn make_norm() -> Normalizer {
    Normalizer::default_vocab().unwrap()
}

fn match_ids(engine: &Engine, title: &str) -> Vec<u64> {
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    engine.match_title(title, &mut scratch, &mut out, true);
    out.sort_unstable();
    out
}

fn sample_queries() -> Vec<(u64, String)> {
    vec![
        (1, "michael jordan 1986 fleer".into()),
        (2, "lebron james rookie".into()),
        (3, "kobe bryant psa 10".into()),
        (4, "mike trout 2011 topps update".into()),
        (5, "derek jeter bowman chrome refractor".into()),
        (6, "shohei ohtani rookie".into()),
        (7, "luka doncic prizm silver".into()),
        (8, "stephen curry select".into()),
        (9, "aaron judge topps chrome".into()),
        (10, "patrick mahomes prizm rookie".into()),
        // Duplicate logical IDs (different versions of same query)
        (1, "michael jordan 1986 fleer rookie".into()),
        (2, "lebron james rookie card".into()),
    ]
}

// ---------------------------------------------------------------------------
// Fix 1: vocab-epoch staleness tracking
// ---------------------------------------------------------------------------

#[test]
fn vocab_epoch_starts_at_zero_no_stale_segments() {
    let norm = make_norm();
    let mut engine = Engine::new(norm);
    engine.build_from_queries(&sample_queries());

    assert_eq!(engine.vocab_epoch(), 0);
    assert_eq!(engine.stale_segment_count(), 0);
    assert!(!engine.has_stale_segments());
    assert_eq!(engine.metrics().stale_segments, 0);
}

#[test]
fn set_vocab_increments_epoch_and_marks_segments_stale() {
    let norm = make_norm();
    let mut engine = Engine::new(norm);
    engine.build_from_queries(&sample_queries());

    assert_eq!(engine.vocab_epoch(), 0);
    let base_segments_before = engine.metrics().base_segments;
    assert!(base_segments_before > 0);

    // Change vocab — all existing segments become stale
    let mut vocab = Vocab::new();
    vocab.add_synonym(
        "rc",
        "term:rookie",
        reverse_rusty::dict::FeatureKind::Category,
    );
    let stale = engine
        .set_vocab(vocab)
        .expect("vocab change should succeed");

    assert_eq!(engine.vocab_epoch(), 1);
    assert!(stale > 0, "should report stale segments");
    assert_eq!(engine.stale_segment_count(), stale);
    assert!(engine.has_stale_segments());
    assert_eq!(engine.metrics().stale_segments, stale);
}

#[test]
fn new_segments_after_vocab_change_are_not_stale() {
    let norm = make_norm();
    let mut engine = Engine::new(norm);
    engine.build_from_queries(&sample_queries()[..5]);

    // Change vocab
    let vocab = Vocab::new();
    engine.set_vocab(vocab).unwrap();
    assert_eq!(engine.vocab_epoch(), 1);
    let stale_before = engine.stale_segment_count();

    // Ingest new queries AFTER the vocab change — new segment at epoch 1
    engine.build_from_queries(&sample_queries()[5..10]);

    // The old segment is stale, the new one is not
    assert_eq!(engine.stale_segment_count(), stale_before);
}

#[test]
fn multiple_vocab_changes_increment_monotonically() {
    let norm = make_norm();
    let mut engine = Engine::new(norm);
    engine.build_from_queries(&sample_queries()[..3]);

    for i in 1..=5u64 {
        let vocab = Vocab::new();
        engine.set_vocab(vocab).unwrap();
        assert_eq!(engine.vocab_epoch(), i);
    }

    // Build new segment at epoch 5
    engine.build_from_queries(&sample_queries()[3..6]);

    // Bump again — the epoch-5 segment is now stale too
    let vocab = Vocab::new();
    engine.set_vocab(vocab).unwrap();
    assert_eq!(engine.vocab_epoch(), 6);
    // All segments are stale (compiled at epoch 0 or 5, current is 6)
    assert_eq!(engine.stale_segment_count(), engine.metrics().base_segments);
}

#[test]
fn compaction_preserves_minimum_epoch() {
    let norm = make_norm();
    let mut engine = Engine::new(norm);

    // Segment at epoch 0
    engine.build_from_queries(&sample_queries()[..3]);

    // Bump to epoch 1, build another segment
    let vocab = Vocab::new();
    engine.set_vocab(vocab).unwrap();
    engine.build_from_queries(&sample_queries()[3..6]);

    assert_eq!(engine.metrics().base_segments, 2);
    // One stale (epoch 0), one current (epoch 1)
    assert_eq!(engine.stale_segment_count(), 1);

    // Compact merges the two — result inherits min epoch (0)
    engine.compact_all();
    assert_eq!(engine.metrics().base_segments, 1);
    // Merged segment is still stale (epoch 0 < current 1)
    assert_eq!(engine.stale_segment_count(), 1);
}

#[test]
fn memtable_staleness_tracked_after_vocab_change_and_insert() {
    let norm = make_norm();
    let mut engine = Engine::new(norm);

    // Insert into memtable at epoch 0
    engine.insert_live("michael jordan 1986 fleer", 100, 1);
    assert_eq!(engine.stale_segment_count(), 0);

    // Change vocab — the memtable (with entries) becomes stale
    let vocab = Vocab::new();
    engine.set_vocab(vocab).unwrap();
    assert_eq!(engine.stale_segment_count(), 1); // memtable counts

    // Flush seals the stale memtable as a base segment, new memtable is fresh
    engine.flush();
    assert_eq!(engine.stale_segment_count(), 1); // sealed segment is stale

    // Insert into fresh memtable — not stale
    engine.insert_live("lebron james rookie", 101, 1);
    assert_eq!(engine.stale_segment_count(), 1); // only the old segment
}

#[test]
fn recompile_stale_segments_absorbs_declared_alias() {
    // The headline mechanism-(2) property: a declared alias (rc ≡ rookie) makes
    // BOTH surface forms match after a recompile. set_vocab marks the segments
    // stale; recompile_stale_segments recompiles every live query under the new
    // normalizer so a query written one way matches a title written the other —
    // with zero false negatives. Without the recompile pass the stale segment
    // keeps the old feature ids and the cross-form title is silently missed.
    let mut engine = Engine::new(make_norm());
    engine.build_from_queries(&[
        (1, "rc fleer".into()),     // query phrased with the abbreviation
        (2, "rookie fleer".into()), // query phrased with the canonical form
    ]);

    // Before the alias, "rc" and "rookie" are distinct features — the forms do
    // not cross-match. (Also validates that "rc" is a real feature, not dropped.)
    assert_eq!(match_ids(&engine, "rc fleer"), vec![1]);
    assert_eq!(match_ids(&engine, "rookie fleer"), vec![2]);

    // Declare rc → rookie and recompile.
    let mut vocab = Vocab::new();
    vocab.add_synonym(
        "rc",
        "term:rookie",
        reverse_rusty::dict::FeatureKind::Category,
    );
    let stale = engine.set_vocab(vocab).unwrap();
    assert!(stale > 0, "set_vocab marks the existing segment stale");
    let recompiled = engine.recompile_stale_segments();
    assert_eq!(recompiled, 2, "both live queries recompiled");
    assert!(
        !engine.has_stale_segments(),
        "recompile clears all staleness"
    );

    // After the alias both surface forms collapse to one feature, so each query
    // matches a title written with EITHER form — and no false negatives.
    assert_eq!(
        match_ids(&engine, "rc fleer"),
        vec![1, 2],
        "abbreviation title now matches both queries"
    );
    assert_eq!(
        match_ids(&engine, "rookie fleer"),
        vec![1, 2],
        "canonical title now matches both queries"
    );
}

#[test]
fn learn_and_apply_absorbs_synonyms_from_anyof_groups() {
    // Engine::learn_and_apply learns `rc → rookie` from the corpus's any-of groups
    // (ADR-015) and recompiles (ADR-046) so a query phrased with the abbreviation
    // matches a title with the canonical form — zero false negatives.
    let mut engine = Engine::new(make_norm());
    let mut qs: Vec<(u64, String)> = vec![(1, "fleer rc".into())];
    for i in 0..4u64 {
        qs.push((100 + i, "(rookie,rc)".into())); // ≥ min_count any-of groups
    }
    engine.build_from_queries(&qs);

    // Before learning, "rc" and "rookie" are distinct, so the rookie title doesn't match.
    assert!(!match_ids(&engine, "fleer rookie").contains(&1));

    let recompiled = engine.learn_and_apply(2).expect("learn_and_apply");
    assert!(recompiled >= 1, "the corpus is recompiled");
    assert!(
        !engine.has_stale_segments(),
        "learn_and_apply clears staleness"
    );

    // After learning rc → rookie, the rc-phrased query matches a rookie title.
    assert!(
        match_ids(&engine, "fleer rookie").contains(&1),
        "after learning rc→rookie, a rookie title matches the rc-phrased query"
    );
    assert!(
        engine
            .vocab()
            .is_some_and(|v| v.synonyms().iter().any(|s| s.token == "rc")),
        "the learned rc→rookie synonym is recorded"
    );
}

// ---------------------------------------------------------------------------
// Fix 2: corrupt data graceful handling (no panics)
// ---------------------------------------------------------------------------

#[test]
fn corrupt_wal_file_recovers_gracefully() {
    let dir = test_dir("corrupt_wal");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    let mut engine = Engine::with_config(make_norm(), config.clone());
    engine.insert_live("michael jordan 1986 fleer", 1, 1);
    engine.insert_live("lebron james rookie", 2, 1);
    engine.flush();

    // Append garbage to the WAL file (simulates torn write)
    let wal_path = dir.join("wal.log");
    if wal_path.exists() {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap();
        f.write_all(&[0xFF; 37]).unwrap(); // corrupt trailing data
    }

    // Reopen should succeed — corrupt tail is skipped
    let reopened = Engine::open(make_norm(), config);
    assert!(
        reopened.is_ok(),
        "engine should open despite corrupt WAL tail"
    );
}

#[test]
fn corrupt_segment_file_skipped_on_open() {
    let dir = test_dir("corrupt_seg");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    let mut engine = Engine::with_config(make_norm(), config.clone());
    engine.build_from_queries(&sample_queries()[..5]);
    engine.flush();
    drop(engine);

    // Corrupt a segment file
    let seg_dir = dir.join("segments");
    if let Ok(entries) = std::fs::read_dir(&seg_dir) {
        for entry in entries.flatten() {
            if entry.path().extension().is_some_and(|e| e == "seg") {
                // Overwrite the middle of the file with garbage
                let data = std::fs::read(entry.path()).unwrap();
                if data.len() > 20 {
                    let mut corrupted = data;
                    for b in &mut corrupted[10..20] {
                        *b = 0xDE;
                    }
                    std::fs::write(entry.path(), &corrupted).unwrap();
                }
                break;
            }
        }
    }

    // Reopen should succeed — corrupt segment is skipped
    let reopened = Engine::open(make_norm(), config);
    assert!(
        reopened.is_ok(),
        "engine should open despite corrupt segment"
    );
    let engine = reopened.unwrap();
    assert!(
        engine.skipped_segments > 0,
        "should report skipped segments"
    );
}

// ---------------------------------------------------------------------------
// Fix 3: delete_by_logical_id via reverse index
// ---------------------------------------------------------------------------

#[test]
fn delete_removes_all_versions_across_segments() {
    let norm = make_norm();
    let mut engine = Engine::new(norm);

    // Version 1 of query 1 in first segment
    engine.build_from_queries(&[(1, "michael jordan 1986 fleer".into())]);
    // Version 2 via live insert (memtable)
    engine.insert_live("michael jordan 1986 fleer rookie", 1, 2);

    // Query 1 should match
    let pre = match_ids(&engine, "michael jordan 1986 fleer rookie card");
    assert!(pre.contains(&1), "query 1 should match before delete");

    // Delete by logical ID — should tombstone in both segment and memtable
    let tombstoned = engine.delete_by_logical_id(1).unwrap();
    assert_eq!(
        tombstoned, 2,
        "should tombstone 2 entries (one per segment/memtable)"
    );

    // Query 1 should no longer match
    let post = match_ids(&engine, "michael jordan 1986 fleer rookie card");
    assert!(!post.contains(&1), "query 1 should not match after delete");
}

#[test]
fn delete_across_many_segments() {
    let norm = make_norm();
    let mut engine = Engine::new(norm);

    // Spread query 42 across 4 segments
    for i in 0..4 {
        engine.build_from_queries(&[
            (42, format!("michael jordan 1986 fleer version{i}")),
            (100 + i, "kobe bryant psa 10".into()),
        ]);
    }
    assert_eq!(engine.metrics().base_segments, 4);

    let tombstoned = engine.delete_by_logical_id(42).unwrap();
    assert_eq!(
        tombstoned, 4,
        "should find and tombstone across all 4 segments"
    );

    // Other queries unaffected
    let matches = match_ids(&engine, "kobe bryant psa 10 gem mint");
    assert!(!matches.is_empty(), "other queries should still match");
    assert!(!matches.contains(&42), "deleted query should not appear");
}

#[test]
fn delete_nonexistent_id_returns_zero() {
    let norm = make_norm();
    let mut engine = Engine::new(norm);
    engine.build_from_queries(&sample_queries()[..5]);

    let tombstoned = engine.delete_by_logical_id(999_999).unwrap();
    assert_eq!(tombstoned, 0);
}

#[test]
fn delete_then_compact_reclaims_space() {
    let norm = make_norm();
    let mut engine = Engine::new(norm);
    // Use two segments so compact_all has something to merge
    engine.build_from_queries(&sample_queries()[..6]);
    engine.build_from_queries(&sample_queries()[6..10]);

    let before = engine.metrics().total_queries;
    assert!(before > 0);
    engine.delete_by_logical_id(1).unwrap();
    engine.delete_by_logical_id(7).unwrap();
    engine.compact_all();
    let after = engine.metrics().total_queries;

    assert!(
        after < before,
        "compaction should reclaim tombstoned entries: before={before}, after={after}"
    );
}

// ---------------------------------------------------------------------------
// Combined: all three fixes interacting
// ---------------------------------------------------------------------------

#[test]
fn full_lifecycle_vocab_delete_persist_compact() {
    let dir = test_dir("full_lifecycle");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        memtable_flush_threshold: 5,
        auto_compact_on_flush: false,
        ..Default::default()
    };

    let norm = make_norm();
    let mut engine = Engine::with_config(norm, config.clone());

    // Phase 1: initial bulk load
    engine.build_from_queries(&sample_queries()[..5]);
    assert_eq!(engine.vocab_epoch(), 0);
    assert_eq!(engine.stale_segment_count(), 0);

    // Phase 2: live inserts (some will auto-flush due to threshold=5)
    for i in 20..30 {
        engine.insert_live(&format!("topps chrome {i} base"), i, 1);
    }

    let metrics_mid = engine.metrics();
    assert!(
        metrics_mid.base_segments >= 2,
        "should have multiple segments"
    );

    // Phase 3: change vocab — everything becomes stale
    let mut vocab = Vocab::new();
    vocab.add_synonym(
        "rc",
        "term:rookie",
        reverse_rusty::dict::FeatureKind::Category,
    );
    let stale = engine.set_vocab(vocab).unwrap();
    assert!(stale > 0);
    assert!(engine.has_stale_segments());

    // Phase 4: delete some queries using the reverse index
    let del1 = engine.delete_by_logical_id(1).unwrap();
    let del2 = engine.delete_by_logical_id(25).unwrap();
    assert!(del1 > 0);
    assert!(del2 > 0);

    // Phase 5: new inserts at the new vocab epoch
    engine.insert_live("lebron james rookie card 2003", 50, 1);
    engine.insert_live("michael jordan 1997 topps chrome", 51, 1);
    engine.flush();

    // The new segment should NOT be stale
    let new_stale = engine.stale_segment_count();
    assert!(new_stale > 0, "old segments still stale");
    assert!(
        new_stale < engine.metrics().base_segments + 1,
        "not all segments+memtable should be stale — new ones are fresh"
    );

    // Phase 6: compact everything
    engine.compact_all();
    // Merged segment inherits min epoch — still stale
    assert!(engine.has_stale_segments());

    // Phase 7: verify matching still works correctly
    let deleted_match = match_ids(&engine, "michael jordan 1986 fleer rookie");
    assert!(
        !deleted_match.contains(&1),
        "deleted query should not match"
    );
    assert!(
        !deleted_match.contains(&25),
        "deleted query should not match"
    );

    let new_match = match_ids(&engine, "lebron james rookie card 2003 topps");
    assert!(new_match.contains(&50), "newly inserted query should match");

    // Phase 8: persist and reopen
    drop(engine);
    let norm2 = make_norm();
    let reopened = Engine::open(norm2, config).expect("reopen should succeed");

    // Verify same match results after reopen
    let post_reopen = match_ids(&reopened, "lebron james rookie card 2003 topps");
    assert!(
        post_reopen.contains(&50),
        "match results should survive reopen"
    );

    let post_del = match_ids(&reopened, "michael jordan 1986 fleer rookie");
    assert!(
        !post_del.contains(&1),
        "deleted query should stay deleted after reopen"
    );
}

#[test]
fn interleaved_delete_insert_flush_compact_stress() {
    let norm = make_norm();
    let mut engine = Engine::new(norm);

    // Build initial corpus
    let mut queries: Vec<(u64, String)> = Vec::new();
    for i in 0..100 {
        queries.push((
            i,
            format!("player{} team{} 2024 topps chrome", i % 20, i % 5),
        ));
    }
    engine.build_from_queries(&queries);

    // Interleave operations
    for round in 0..5 {
        // Delete every 10th query
        for i in (0..100).step_by(10) {
            engine.delete_by_logical_id(i + round).unwrap();
        }
        // Insert new queries
        for i in 0..10 {
            let id = 1000 + round * 10 + i;
            engine.insert_live(&format!("newplayer{} team{} 2025 prizm", id, i % 3), id, 1);
        }
        // Flush every other round
        if round % 2 == 0 {
            engine.flush();
        }
        // Compact on round 3
        if round == 3 {
            engine.compact_all();
        }
    }

    // Final compact
    engine.compact_all();

    // Verify: deleted queries should not match
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    for i in (0..100).step_by(10) {
        for round in 0..5u64 {
            let del_id = i + round;
            if del_id < 100 {
                engine.match_title(
                    &format!(
                        "player{} team{} 2024 topps chrome refractor",
                        del_id % 20,
                        del_id % 5
                    ),
                    &mut scratch,
                    &mut out,
                    true,
                );
                // The deleted ID should NOT appear (though others in the same "slot" might)
                assert!(
                    !out.contains(&del_id),
                    "deleted query {del_id} should not match after stress test"
                );
                out.clear();
            }
        }
    }

    // Verify: new queries should match
    for round in 0..5u64 {
        for i in 0..10u64 {
            let id = 1000 + round * 10 + i;
            engine.match_title(
                &format!("newplayer{} team{} 2025 prizm silver", id, i % 3),
                &mut scratch,
                &mut out,
                true,
            );
            assert!(
                out.contains(&id),
                "newly inserted query {id} should still match"
            );
            out.clear();
        }
    }

    // Metrics sanity check
    let m = engine.metrics();
    assert!(m.total_queries > 0);
    assert_eq!(m.stale_segments, 0, "no vocab change, no stale segments");
}

#[test]
fn persistence_round_trip_with_reverse_index() {
    let dir = test_dir("reverse_index_persist");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..Default::default()
    };

    let norm = make_norm();
    let mut engine = Engine::with_config(norm, config.clone());
    engine.build_from_queries(&sample_queries());
    engine.flush();

    // Delete some entries
    let del = engine.delete_by_logical_id(3).unwrap();
    assert!(del > 0);
    engine.flush();
    drop(engine);

    // Reopen and verify the delete is durable
    let norm2 = make_norm();
    let reopened = Engine::open(norm2, config).expect("reopen");

    // The deleted ID should not match
    let results = match_ids(&reopened, "kobe bryant psa 10 gem mint");
    assert!(
        !results.contains(&3),
        "deleted query should stay deleted after reopen"
    );

    // Other queries should still work
    let results2 = match_ids(&reopened, "mike trout 2011 topps update us175");
    assert!(results2.contains(&4), "surviving query should still match");

    // Delete via reverse index should work on reopened mmap segments
    let mut engine2 = reopened;
    let del2 = engine2.delete_by_logical_id(4).unwrap();
    assert!(
        del2 > 0,
        "reverse index should be rebuilt for mmap'd segments on open"
    );
}

// ---------------------------------------------------------------------------
// Engine::explain_hit — read-only explain via search API
// ---------------------------------------------------------------------------

#[test]
fn explain_hit_returns_structured_detail_for_matched_query() {
    let norm = make_norm();
    let mut engine = Engine::new(norm);

    let queries = vec![
        (1u64, "michael jordan 1986 fleer".to_string()),
        (2u64, "kobe bryant psa 10".to_string()),
    ];
    engine.build_from_queries(&queries);

    let title = "michael jordan 1986 fleer rookie card";
    let ids = match_ids(&engine, title);
    assert!(ids.contains(&1), "query 1 should match");

    let detail = engine.explain_hit(1, title);
    assert!(
        detail.is_some(),
        "explain_hit should return detail for stored query"
    );
    let detail = detail.unwrap();
    assert!(detail.candidate, "matched query must be a candidate");
    assert!(detail.matched, "matched query must pass exact verification");
    assert!(
        detail.failures.is_empty(),
        "no failures for a passing match"
    );
    assert!(
        !detail.title_features.is_empty(),
        "should extract title features"
    );
    assert!(
        !detail.required.is_empty(),
        "compiled query should have required features"
    );
}

#[test]
fn explain_hit_shows_failure_for_non_matching_title() {
    let norm = make_norm();
    let mut engine = Engine::new(norm);

    engine.build_from_queries(&[(1u64, "michael jordan 1986 fleer".to_string())]);

    let title = "kobe bryant 1996 topps chrome";
    let ids = match_ids(&engine, title);
    assert!(!ids.contains(&1), "query 1 should not match this title");

    let detail = engine.explain_hit(1, title);
    assert!(detail.is_some());
    let detail = detail.unwrap();
    assert!(!detail.matched, "should not pass exact verification");
    assert!(!detail.failures.is_empty(), "should report failure reasons");
}

#[test]
fn explain_hit_returns_none_for_unknown_id() {
    let norm = make_norm();
    let engine = Engine::new(norm);
    assert!(engine.explain_hit(999, "anything").is_none());
}
