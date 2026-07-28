//! Fix 1: vocab-epoch staleness tracking.

use reverse_rusty::segment::Engine;
use reverse_rusty::vocab::Vocab;

use crate::harness::{make_norm, match_ids, sample_queries};

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
        "pkg",
        "term:new",
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
    engine.insert_live("wireless mouse 1986 vertex", 100, 1);
    assert_eq!(engine.stale_segment_count(), 0);

    // Change vocab — the memtable (with entries) becomes stale
    let vocab = Vocab::new();
    engine.set_vocab(vocab).unwrap();
    assert_eq!(engine.stale_segment_count(), 1); // memtable counts

    // Flush seals the stale memtable as a base segment, new memtable is fresh
    engine.flush();
    assert_eq!(engine.stale_segment_count(), 1); // sealed segment is stale

    // Insert into fresh memtable — not stale
    engine.insert_live("mechanical keyboard new", 101, 1);
    assert_eq!(engine.stale_segment_count(), 1); // only the old segment
}

#[test]
fn recompile_stale_segments_absorbs_declared_alias() {
    // The headline mechanism-(2) property: a declared alias (pkg ≡ new) makes
    // BOTH surface forms match after a recompile. set_vocab marks the segments
    // stale; recompile_stale_segments recompiles every live query under the new
    // normalizer so a query written one way matches a title written the other —
    // with zero false negatives. Without the recompile pass the stale segment
    // keeps the old feature ids and the cross-form title is silently missed.
    let mut engine = Engine::new(make_norm());
    engine.build_from_queries(&[
        (1, "pkg vertex".into()), // query phrased with the abbreviation
        (2, "new vertex".into()), // query phrased with the canonical form
    ]);

    // Before the alias, "pkg" and "new" are distinct features — the forms do
    // not cross-match. (Also validates that "pkg" is a real feature, not dropped.)
    assert_eq!(match_ids(&engine, "pkg vertex"), vec![1]);
    assert_eq!(match_ids(&engine, "new vertex"), vec![2]);

    // Declare pkg → new and recompile.
    let mut vocab = Vocab::new();
    vocab.add_synonym(
        "pkg",
        "term:new",
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
        match_ids(&engine, "pkg vertex"),
        vec![1, 2],
        "abbreviation title now matches both queries"
    );
    assert_eq!(
        match_ids(&engine, "new vertex"),
        vec![1, 2],
        "canonical title now matches both queries"
    );
}

#[test]
fn learn_and_apply_absorbs_synonyms_from_anyof_groups() {
    // Engine::learn_and_apply learns `pkg → new` from the corpus's any-of groups
    // (ADR-015) and recompiles (ADR-046) so a query phrased with the abbreviation
    // matches a title with the canonical form — zero false negatives.
    let mut engine = Engine::new(make_norm());
    let mut qs: Vec<(u64, String)> = vec![(1, "vertex pkg".into())];
    for i in 0..4u64 {
        qs.push((100 + i, "(new,pkg)".into())); // ≥ min_count any-of groups
    }
    engine.build_from_queries(&qs);

    // Before learning, "pkg" and "new" are distinct, so the new title doesn't match.
    assert!(!match_ids(&engine, "vertex new").contains(&1));

    let recompiled = engine.learn_and_apply(2).expect("learn_and_apply");
    assert!(recompiled >= 1, "the corpus is recompiled");
    assert!(
        !engine.has_stale_segments(),
        "learn_and_apply clears staleness"
    );

    // After learning pkg → new, the pkg-phrased query matches a new title.
    assert!(
        match_ids(&engine, "vertex new").contains(&1),
        "after learning pkg→new, a new title matches the pkg-phrased query"
    );
    assert!(
        engine
            .vocab()
            .is_some_and(|v| v.synonyms().iter().any(|s| s.token == "pkg")),
        "the learned pkg→new synonym is recorded"
    );
}
