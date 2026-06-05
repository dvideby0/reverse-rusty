//! Combined: all three fixes interacting.

use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::vocab::Vocab;

use crate::harness::{make_norm, match_ids, sample_queries, test_dir};

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
