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
fn phrase_matches_across_repeated_whitespace() {
    // ADR-061: byte-cleaning collapses whitespace *runs*, so a phrase (registered single-spaced)
    // still matches a title with repeated spaces or adjacent split punctuation — closing a false
    // negative the phrase automaton (which scans the cleaned bytes literally) otherwise has. This
    // fails on the pre-fix cleaner, which preserved the extra spaces so the pattern missed.
    let n = spec_vocab();
    assert_eq!(
        names(&n, "upper  deck"),
        s(&["brand:upper_deck"]),
        "double space"
    );
    assert_eq!(
        names(&n, "upper - deck"),
        s(&["brand:upper_deck"]),
        "split punctuation between tokens"
    );
    assert_eq!(
        names(&n, "michael   jordan"),
        s(&["player:michael_jordan"]),
        "triple space"
    );
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

// ---- ADR-061: multi-word alias dual title view ----

/// An alias phrase collapses to ONE entity on the query side (so ADR-054 expansion can
/// widen it), but on the title side it is additive AND the overlap superset adds nested
/// alias entities — while the canonical (negative) view stays leftmost-longest. This is the
/// load-bearing normalizer behavior behind Phase 2's two-view matcher.
#[test]
fn alias_phrase_collapses_on_query_overlaps_on_title() {
    let mut b = NormalizerBuilder::new();
    b.add_phrase_alias(&["new", "york"], "term:new_york", FeatureKind::Generic);
    b.add_phrase_alias(
        &["new", "york", "city"],
        "term:new_york_city",
        FeatureKind::Generic,
    );
    let norm = b.build().expect("alias automaton");

    // Intern the entities (mutating compile of each alias form) so ids are dense + stable.
    let mut dict = Dict::new();
    let mut lc = String::new();
    let _ = norm.compile_features("new york", &mut dict, &mut lc);
    let _ = norm.compile_features("new york city", &mut dict, &mut lc);
    let ny = dict.get_or_synthetic("term:new_york");
    let nyc = dict.get_or_synthetic("term:new_york_city");

    // Query side: a multi-word alias form collapses to its single entity feature.
    let q = norm.compile_features_readonly("new york", &dict, &mut lc);
    assert_eq!(q, vec![ny], "query-side alias must collapse to one entity");

    // Title side: dual view of "new york city yankees".
    let (mut neg, mut pos) = (Vec::new(), Vec::new());
    norm.match_features_dual("new york city yankees", &dict, &mut lc, &mut neg, &mut pos);

    // Negative (canonical) view: leftmost-longest reads "new york city", NOT the nested
    // "new york" — so a forbidden clause stays recall-correct.
    assert!(neg.contains(&nyc), "neg has the leftmost-longest entity");
    assert!(
        !neg.contains(&ny),
        "neg must be leftmost-longest: no nested new york"
    );
    // Positive (superset) view: the overlap pass adds the nested "new york".
    assert!(
        pos.contains(&nyc) && pos.contains(&ny),
        "pos is the superset"
    );
    // N(T) ⊆ P(T), and the title side is additive (keeps component tokens, not just entities).
    for f in &neg {
        assert!(pos.contains(f), "N(T) must be a subset of P(T)");
    }
    assert!(neg.len() > 2, "additive title keeps component tokens");
}

/// With no alias phrase registered, `match_features_dual` yields identical views and they
/// equal `match_features` — the default path is byte-identical (the no-overhead guarantee).
#[test]
fn dual_view_equals_single_view_without_aliases() {
    let n = spec_vocab();
    let mut dict = Dict::new();
    let mut lc = String::new();
    let title = "1994 upper deck michael jordan psa 10 gem mint";
    // Seed the dict with a mutating compile so ids are dense.
    let _ = n.compile_features(title, &mut dict, &mut lc);

    let mut single = Vec::new();
    n.match_features(title, &dict, &mut lc, &mut single);
    let (mut neg, mut pos) = (Vec::new(), Vec::new());
    n.match_features_dual(title, &dict, &mut lc, &mut neg, &mut pos);
    assert_eq!(neg, single, "negative view == single view without aliases");
    assert_eq!(pos, single, "positive view == single view without aliases");
}
