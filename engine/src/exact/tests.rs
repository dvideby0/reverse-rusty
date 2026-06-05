//! Tag-filter unit tests (ADR-049): the sorted-slice intersection primitives, the
//! AND-across-keys / OR-within-a-key predicate semantics, and the post-candidate
//! verify-stage filter (proving it only ever *removes* matches, and that the scalar
//! `verify` and columnar `eval_batch` paths agree).

use super::*;
use crate::compile::Extracted;
use crate::dict::{Dict, FeatureId, FeatureKind};

#[test]
fn sorted_intersects_basics() {
    assert!(sorted_intersects(&[1, 5, 9], &[5])); // shares 5
    assert!(sorted_intersects(&[5], &[1, 5, 9])); // order-independent
    assert!(!sorted_intersects(&[1, 2, 3], &[4, 5, 6])); // disjoint
    assert!(!sorted_intersects(&[1, 2, 3], &[])); // empty group ⇒ no match
    assert!(!sorted_intersects(&[], &[1, 2, 3])); // untagged query ⇒ no match
}

#[test]
fn predicate_is_and_across_keys_or_within_a_key() {
    // q0 tags {10,20}, q1 tags {11}, q2 untagged — laid out as the SoA tag column.
    let tag_blob = [10u32, 20, 11];
    let tag_off = [0u32, 2, 3];
    let tag_len = [2u16, 1, 0];
    let passes = |i: usize, pred: &TagPredicate| {
        pred.is_empty() || query_passes_tags(i, pred, &tag_off, &tag_len, &tag_blob)
    };

    // empty predicate ⇒ everything passes (no filter)
    let none = TagPredicate::empty();
    assert!(passes(0, &none) && passes(1, &none) && passes(2, &none));

    // category ∈ {A=10, B=11}: tagged q0/q1 pass, untagged q2 fails
    let cat = TagPredicate::new(vec![vec![11, 10]]); // unsorted input → new() sorts
    assert!(passes(0, &cat) && passes(1, &cat));
    assert!(!passes(2, &cat));

    // category ∈ {A=10} AND status ∈ {X=20}: only q0 has both (AND across keys)
    let both = TagPredicate::new(vec![vec![10], vec![20]]);
    assert!(passes(0, &both));
    assert!(!passes(1, &both) && !passes(2, &both));

    // a present-but-empty group matches nothing (filter on an all-unknown value) —
    // the load-bearing "can never over-return" rule.
    let empty_group = TagPredicate::new(vec![Vec::new()]);
    assert!(!empty_group.is_empty());
    assert!(!passes(0, &empty_group) && !passes(1, &empty_group));
}

#[test]
fn verify_filters_post_candidate_and_only_removes() {
    // A store with one query requiring feature id 7, tagged {10, 20} (sorted).
    let mut dict = Dict::new();
    for i in 0..8 {
        dict.intern(&format!("f{i}"), FeatureKind::Generic);
    }
    let ex = Extracted {
        required: vec![7],
        forbidden: vec![],
        anyof: vec![],
    };
    let mut store = ExactStore::new();
    store.push(&ex, &[10, 20], &dict, 1, 100);

    let tfeats = [7u32]; // a title that satisfies the query's expression
    let tmask = 0u64;

    // No filter → matches (the query's expression is satisfied).
    assert!(store.verify(0, tmask, &tfeats, &TagPredicate::empty()));
    // A filter the query satisfies (category=A=10) → still matches.
    assert!(store.verify(0, tmask, &tfeats, &TagPredicate::new(vec![vec![10]])));
    // A filter the query does NOT satisfy (category=99) → removed, even though the
    // expression matches. Proves filtering happens post-candidate and only removes.
    assert!(!store.verify(0, tmask, &tfeats, &TagPredicate::new(vec![vec![99]])));

    // eval_batch (columnar) must agree with verify for the same predicate.
    let mut acc = [0u64; 1];
    let mut grp = [0u64; 1];
    let lookup = |f: FeatureId| -> Option<&[u64]> {
        if f == 7 {
            Some(&[1u64]) // title 0 contains feature 7
        } else {
            None
        }
    };
    store.eval_batch(
        0,
        &[tmask],
        lookup,
        &mut acc,
        &mut grp,
        &TagPredicate::new(vec![vec![10]]),
    );
    assert_eq!(
        acc[0] & 1,
        1,
        "columnar path matches with a satisfied filter"
    );
    let mut acc2 = [0u64; 1];
    store.eval_batch(
        0,
        &[tmask],
        lookup,
        &mut acc2,
        &mut grp,
        &TagPredicate::new(vec![vec![99]]),
    );
    assert_eq!(
        acc2[0] & 1,
        0,
        "columnar path drops with an unsatisfied filter"
    );
}
