//! Unit tests for the equivalence expansion pass (ADR-054). These exercise the pure
//! `Extracted::expand_equivalences` rewrite in isolation; the end-to-end zero-false-
//! negative + monotonicity proofs live in tests/oracle/ and tests/cluster_oracle/.
use super::super::*;

fn equiv(pairs: &[(FeatureId, &[FeatureId])]) -> crate::dict::EquivMap {
    let mut m = crate::util::fast_map();
    for &(member, group) in pairs {
        m.insert(member, group.to_vec());
    }
    m
}

#[test]
fn moves_required_into_anyof_group() {
    // 10 belongs to the equivalence group {10,20}; it leaves `required` and becomes an
    // any-of, so a title with EITHER 10 or 20 still matches. 5 (no group) stays required.
    let g = equiv(&[(10, &[10, 20]), (20, &[10, 20])]);
    let mut ex = Extracted {
        required: vec![5, 10],
        forbidden: vec![99],
        anyof: vec![],
        anyof_predicates: vec![],
        forbidden_conjunctions: vec![],
        ..Extracted::default()
    };
    ex.expand_equivalences(&g);
    assert_eq!(ex.required, vec![5]);
    assert_eq!(ex.anyof, vec![vec![10, 20]]);
    assert_eq!(ex.forbidden, vec![99], "forbidden is never widened");
}

#[test]
fn widens_existing_anyof_group() {
    let g = equiv(&[(10, &[10, 20]), (20, &[10, 20])]);
    let mut ex = Extracted {
        required: vec![],
        forbidden: vec![],
        anyof: vec![vec![10, 30]],
        anyof_predicates: vec![],
        forbidden_conjunctions: vec![],
        ..Extracted::default()
    };
    ex.expand_equivalences(&g);
    assert_eq!(ex.anyof, vec![vec![10, 20, 30]]);
}

#[test]
fn widens_one_compound_requirement_without_flattening_the_member() {
    let g = equiv(&[(10, &[10, 20]), (20, &[10, 20])]);
    let mut ex = Extracted {
        required: vec![],
        forbidden: vec![],
        anyof: vec![vec![10, 40]],
        anyof_predicates: vec![AnyOfPredicate {
            members: vec![
                AnyOfMember {
                    requirements: vec![vec![10], vec![30]],
                },
                AnyOfMember {
                    requirements: vec![vec![40]],
                },
            ],
        }],
        forbidden_conjunctions: vec![],
        ..Extracted::default()
    };
    ex.expand_equivalences(&g);
    assert_eq!(ex.anyof, vec![vec![10, 20, 40]]);
    assert_eq!(
        ex.anyof_predicates[0].members[0].requirements,
        vec![vec![10, 20], vec![30]],
        "equivalents widen one requirement; feature 30 remains conjunctive"
    );
}

#[test]
fn empty_map_is_a_noop() {
    let g: crate::dict::EquivMap = crate::util::fast_map();
    let before = Extracted {
        required: vec![1, 2],
        forbidden: vec![3],
        anyof: vec![vec![4, 5]],
        anyof_predicates: vec![],
        forbidden_conjunctions: vec![],
        ..Extracted::default()
    };
    let mut ex = before.clone();
    ex.expand_equivalences(&g);
    assert_eq!(ex.required, before.required);
    assert_eq!(ex.forbidden, before.forbidden);
    assert_eq!(ex.anyof, before.anyof);
}

#[test]
fn is_idempotent() {
    let g = equiv(&[(10, &[10, 20]), (20, &[10, 20])]);
    let mut once = Extracted {
        required: vec![10],
        forbidden: vec![],
        anyof: vec![],
        anyof_predicates: vec![],
        forbidden_conjunctions: vec![],
        ..Extracted::default()
    };
    once.expand_equivalences(&g);
    let mut twice = once.clone();
    twice.expand_equivalences(&g);
    assert_eq!(once.required, twice.required);
    assert_eq!(once.anyof, twice.anyof);
}
