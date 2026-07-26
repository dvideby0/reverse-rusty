//! ADR-068: the cover of an empty positive set is the universal signature —
//! one empty broad-anchor group, hashed to `util::universal_sig()`. Derived
//! unconditionally by the optimizer so every re-derivation site (compaction
//! re-anchoring, the vocab recompile, explain) reproduces it by construction.

use crate::compile::{anchor_plan, build_signatures, CostClass, Extracted};
use crate::dict::Dict;

fn class_d_ex() -> Extracted {
    Extracted {
        required: vec![],
        forbidden: vec![7, 9],
        anyof: vec![],
        anyof_predicates: vec![],
        forbidden_conjunctions: vec![],
        ..Extracted::default()
    }
}

#[test]
fn anchor_plan_derives_the_universal_broad_group() {
    let mut dict = Dict::new();
    dict.finalize_mask();
    let plan = anchor_plan(&class_d_ex(), &dict, 0);
    assert_eq!(plan.class, CostClass::D);
    assert!(plan.main_anchors.is_empty(), "class D never anchors main");
    assert_eq!(
        plan.broad_anchors,
        vec![Vec::<u32>::new()],
        "exactly one EMPTY broad-anchor group — the universal cover"
    );
}

#[test]
fn build_signatures_hashes_it_to_universal_sig() {
    let mut dict = Dict::new();
    dict.finalize_mask();
    let plan = build_signatures(&class_d_ex(), &dict, 0);
    assert_eq!(plan.class, CostClass::D);
    assert!(plan.main_sigs.is_empty());
    assert_eq!(plan.broad_sigs, vec![crate::util::universal_sig()]);
}

#[test]
fn universal_sig_is_stable_and_nonzero() {
    // The constant must never change (it is baked into every `.seg` holding
    // an always-candidate) and never be 0 (the frozen-table empty sentinel).
    let u = crate::util::universal_sig();
    assert_ne!(u, 0);
    assert_eq!(u, crate::util::sig_key(&[]));
}

#[test]
fn forbidden_features_still_never_reach_anchors() {
    // The lossless-cover invariant, lane edition: the universal cover is
    // derived without reading `forbidden` — two class-D queries with
    // different forbidden sets share the identical plan.
    let mut dict = Dict::new();
    dict.finalize_mask();
    let a = build_signatures(&class_d_ex(), &dict, 0);
    let b = build_signatures(
        &Extracted {
            required: vec![],
            forbidden: vec![1],
            anyof: vec![],
            anyof_predicates: vec![],
            forbidden_conjunctions: vec![],
            ..Extracted::default()
        },
        &dict,
        0,
    );
    assert_eq!(a.broad_sigs, b.broad_sigs);
    assert_eq!(a.class, b.class);
}
