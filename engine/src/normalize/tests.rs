//! Golden normalization cases — exact feature-*name* sets, authored by hand from
//! the spec (docs/design/normalization.md §2–§4, docs/reference/dsl.md), NOT
//! captured from `emit`. They exist because the differential oracle
//! (tests/oracle.rs) runs THIS normalizer on both its engine and its brute-force
//! ground truth, and only ever under the EMPTY `default_vocab` — so a
//! normalization-model bug is invisible there, and the entire vocab-driven path
//! (phrases/synonyms/graders) is never exercised at all. These pins close that
//! gap with expectations a code bug cannot infect. See docs/DECISIONS.md ADR-050.
use super::*;
use crate::dict::Dict;

/// Sorted feature *names* for `text`. Uses the mutating compile path on purpose:
/// it interns every emitted feature, so `Dict::name` round-trips to a real name
/// (the read-only path would hash misses to a `"<oov>"` synthetic ID).
fn names(norm: &Normalizer, text: &str) -> Vec<String> {
    let mut dict = Dict::new();
    let mut lc = String::new();
    let ids = norm.compile_features(text, &mut dict, &mut lc);
    let mut out: Vec<String> = ids.iter().map(|&id| dict.name(id).to_string()).collect();
    out.sort();
    out
}

fn s(items: &[&str]) -> Vec<String> {
    items.iter().map(ToString::to_string).collect()
}

/// The spec's worked-example vocabulary (docs/design/normalization.md §1), built
/// explicitly so the expected canonical names are themselves part of the contract.
fn spec_vocab() -> Normalizer {
    NormalizerBuilder::new()
        .phrase(&["upper", "deck"], "brand:upper_deck", FeatureKind::Brand)
        .phrase(
            &["michael", "jordan"],
            "player:michael_jordan",
            FeatureKind::Player,
        )
        .synonym("ud", "brand:upper_deck", FeatureKind::Brand)
        .synonym("topps", "brand:topps", FeatureKind::Brand)
        .synonym("sp", "card_term:sp", FeatureKind::Category)
        .grader("psa")
        .grader("bgs")
        .grader("sgc")
        .grade_word("gem")
        .grade_word("mint")
        .build()
        .expect("spec vocab automaton")
}

// ---- vocab-independent pipeline (the empty default_vocab still does this) ----

#[test]
fn diacritics_fold_to_ascii() {
    let n = Normalizer::default_vocab().unwrap();
    // normalization.md §4: Café->cafe, Jokić->jokic, Acuña->acuna (ñ no longer splits).
    assert_eq!(names(&n, "café"), s(&["term:cafe"]));
    assert_eq!(names(&n, "Jokić"), s(&["term:jokic"]));
    assert_eq!(names(&n, "Ronald Acuña"), s(&["term:acuna", "term:ronald"]));
}

#[test]
fn number_disambiguation_matrix() {
    let n = Normalizer::default_vocab().unwrap();
    // normalization.md §4 hardening table: markers keep numbers from becoming grades.
    assert_eq!(names(&n, "#2 bulls"), s(&["term:2", "term:bulls"])); // card number
    assert_eq!(names(&n, "/5"), s(&["term:5"])); // serial
    assert_eq!(names(&n, "3/10"), s(&["term:10", "term:3"])); // serial halves
    assert_eq!(names(&n, "1994"), s(&["year:1994"])); // year
    assert_eq!(names(&n, "pop 1"), s(&["term:1", "term:pop"])); // population
}

#[test]
fn generic_fallback_term() {
    let n = Normalizer::default_vocab().unwrap();
    assert_eq!(names(&n, "unknownword"), s(&["term:unknownword"]));
}

// ---- vocab-driven pipeline (spec vocab) — never reached by the oracle ----

#[test]
fn multiword_phrases_collapse_to_one_feature() {
    let n = spec_vocab();
    // normalization.md §1/§2: a multiword entity is ONE feature, not its tokens.
    assert_eq!(names(&n, "michael jordan"), s(&["player:michael_jordan"]));
    assert_eq!(names(&n, "upper deck"), s(&["brand:upper_deck"]));
}

#[test]
fn synonyms_converge_alternate_surface_forms() {
    let n = spec_vocab();
    // normalization.md §2: "ud" and the "upper deck" phrase land on the SAME feature.
    assert_eq!(names(&n, "ud"), s(&["brand:upper_deck"]));
    assert_eq!(names(&n, "topps"), s(&["brand:topps"]));
}

#[test]
fn grader_path_emits_grader_grade_and_fused_form() {
    let n = spec_vocab();
    // normalization.md §1/§2: psa 10 / psa10 -> grader:psa + grade:10 + grader_grade:psa10.
    let expected = s(&["grade:10", "grader:psa", "grader_grade:psa10"]);
    assert_eq!(names(&n, "psa 10"), expected);
    assert_eq!(names(&n, "psa10"), expected, "fused form == spaced form");
    assert_eq!(
        names(&n, "psa 9.5"),
        s(&["grade:9.5", "grader:psa", "grader_grade:psa9.5"]),
        "half grades are kept"
    );
}

// ---- determinism (the §2 invariant; normalize∘normalize isn't typeable, so we
//      pin the two checkable properties it actually promises) ----

#[test]
fn fold_is_a_normalization_fixpoint() {
    let n = Normalizer::default_vocab().unwrap();
    assert_eq!(names(&n, "café"), names(&n, "cafe"));
    assert_eq!(names(&n, "Jokić"), names(&n, "jokic"));
}

#[test]
fn compile_does_not_drift_on_repeat() {
    let n = Normalizer::default_vocab().unwrap();
    let mut dict = Dict::new();
    let mut lc = String::new();
    let first = n.compile_features("psa 10 michael jordan", &mut dict, &mut lc);
    let len_after_first = dict.len();
    let second = n.compile_features("psa 10 michael jordan", &mut dict, &mut lc);
    assert_eq!(first, second, "same text -> same IDs");
    assert_eq!(
        dict.len(),
        len_after_first,
        "a repeat interns no new feature"
    );
}

// ---- punctuation-equivalence folding (ADR-058) ----

#[test]
fn default_punctuation_splits_apostrophe_and_hyphen() {
    // The historical default: `'` and `-` are word boundaries, so the punctuated
    // forms tokenize apart while the joined form is one token — the false-negative
    // gap (a query `obrien` misses an `O'Brien` title) that folding closes.
    let n = Normalizer::default_vocab().unwrap();
    assert_eq!(names(&n, "O'Brien"), s(&["term:brien", "term:o"]));
    assert_eq!(names(&n, "O-Brien"), s(&["term:brien", "term:o"]));
    assert_eq!(names(&n, "OBrien"), s(&["term:obrien"]));
}

#[test]
fn folding_collapses_punctuation_variants_to_one_token() {
    // Declaring apostrophe (ascii + curly U+2019) and mid-word hyphen as Fold makes
    // all four surface forms land on the SAME single token — so a query and a title
    // that differ only in punctuation now share a feature and match.
    let n = NormalizerBuilder::new()
        .punct('\'', PunctClass::Fold)
        .punct('\u{2019}', PunctClass::Fold)
        .punct('-', PunctClass::Fold)
        .build()
        .expect("folding normalizer");
    let expected = s(&["term:obrien"]);
    assert_eq!(names(&n, "O'Brien"), expected, "ascii apostrophe");
    assert_eq!(names(&n, "O\u{2019}Brien"), expected, "curly apostrophe");
    assert_eq!(names(&n, "O-Brien"), expected, "hyphen");
    assert_eq!(names(&n, "OBrien"), expected, "already joined");
}

#[test]
fn builder_batch_and_mut_fold_apis_fold() {
    // Exercise the `&mut` builder + batch helper (not just the fluent `.punct`).
    let mut b = NormalizerBuilder::new();
    b.fold_punctuation_chars(&['\'', '\u{2019}', '-']);
    let n = b.build().unwrap();
    assert_eq!(names(&n, "O-Brien"), s(&["term:obrien"]));
    assert_eq!(names(&n, "O\u{2019}Brien"), s(&["term:obrien"]));
}

#[test]
fn fold_merges_only_within_a_word_not_across_spaces() {
    // A folded character joins only ADJACENT alphanumerics; a hyphen flanked by
    // spaces still leaves two tokens (the surrounding spaces remain boundaries).
    let n = NormalizerBuilder::new()
        .punct('-', PunctClass::Fold)
        .build()
        .unwrap();
    assert_eq!(names(&n, "foo-bar"), s(&["term:foobar"]));
    assert_eq!(names(&n, "foo - bar"), s(&["term:bar", "term:foo"]));
}

#[test]
fn punct_class_keep_default_is_overridable_to_fold() {
    // `.` defaults to Keep (in place, so half-grades survive); reclassifying it to
    // Fold deletes it. A pure-letter token keeps clear of the number/grade pipeline.
    let keep = Normalizer::default_vocab().unwrap();
    assert_eq!(names(&keep, "a.b.c"), s(&["term:a.b.c"]));
    let fold = NormalizerBuilder::new()
        .punct('.', PunctClass::Fold)
        .build()
        .unwrap();
    assert_eq!(names(&fold, "a.b.c"), s(&["term:abc"]));
}

#[test]
fn marker_and_keep_defaults_are_unchanged_by_the_table() {
    // Regression guard: the default table reproduces the historical `#`/`/`/`.`
    // behaviors exactly (the same cases as `number_disambiguation_matrix`).
    let n = Normalizer::default_vocab().unwrap();
    assert_eq!(names(&n, "#2 bulls"), s(&["term:2", "term:bulls"]));
    assert_eq!(names(&n, "3/10"), s(&["term:10", "term:3"]));
}
