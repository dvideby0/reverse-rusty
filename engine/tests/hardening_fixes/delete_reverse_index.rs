//! Fix 3: delete_by_logical_id via reverse index.

use reverse_rusty::segment::Engine;

use crate::harness::{make_norm, match_ids, sample_queries};

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
