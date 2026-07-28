//! Golden normalization cases — exact feature-*name* sets, authored by hand from
//! the spec (docs/design/normalization.md §2–§4, docs/reference/dsl.md), NOT
//! captured from `emit`. They exist because the differential oracle
//! (tests/oracle/) runs THIS normalizer on both its engine and its brute-force
//! ground truth, and only ever under the EMPTY `default_vocab` — so a
//! normalization-model bug is invisible there, and the entire vocab-driven path
//! (phrases/synonyms) is never exercised at all. These pins close that
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

/// A domain-neutral sample vocabulary, built explicitly so the expected canonical
/// names are themselves part of the contract.
fn sample_vocab() -> Normalizer {
    NormalizerBuilder::new()
        .phrase(&["acme", "labs"], "brand:acme_labs", FeatureKind::Brand)
        .phrase(
            &["wireless", "mouse"],
            "entity:wireless_mouse",
            FeatureKind::Entity,
        )
        .synonym("acme", "brand:acme_labs", FeatureKind::Brand)
        .synonym("refurb", "category:refurbished", FeatureKind::Category)
        .build()
        .expect("sample vocab automaton")
}

// ---- vocab-independent pipeline (the empty default_vocab still does this) ----

#[test]
fn diacritics_fold_to_ascii() {
    let n = Normalizer::default_vocab().unwrap();
    // normalization.md §4: Café->cafe, Jokić->jokic, Jalapeño->jalapeno (ñ no longer splits).
    assert_eq!(names(&n, "café"), s(&["term:cafe"]));
    assert_eq!(names(&n, "Jokić"), s(&["term:jokic"]));
    assert_eq!(
        names(&n, "Ronald Jalapeño"),
        s(&["term:jalapeno", "term:ronald"])
    );
}

#[test]
fn number_disambiguation_matrix() {
    let n = Normalizer::default_vocab().unwrap();
    // Structural markers keep identifier numbers generic.
    assert_eq!(names(&n, "#2 widget"), s(&["term:2", "term:widget"]));
    assert_eq!(names(&n, "/5"), s(&["term:5"])); // serial
    assert_eq!(names(&n, "3/10"), s(&["term:10", "term:3"])); // serial halves
    assert_eq!(names(&n, "1994"), s(&["year:1994"])); // year
    assert_eq!(names(&n, "count 1"), s(&["term:1", "term:count"]));
}

#[test]
fn generic_fallback_term() {
    let n = Normalizer::default_vocab().unwrap();
    assert_eq!(names(&n, "unknownword"), s(&["term:unknownword"]));
}

// ---- number-context words (ADR-069) ----

#[test]
fn number_context_is_empty_by_default() {
    let n = Normalizer::default_vocab().unwrap();
    assert_eq!(names(&n, "model 1995"), s(&["term:model", "year:1995"]));
    assert_eq!(names(&n, "1995 model"), s(&["term:model", "year:1995"]));
}

#[test]
fn number_context_empty_list_is_position_insensitive() {
    // An EMPTY list keeps the default position-insensitive behavior.
    let p = NormalizerBuilder::new()
        .number_context_words(&[])
        .build()
        .unwrap();
    assert_eq!(names(&p, "model 1995"), s(&["term:model", "year:1995"]));
    assert_eq!(names(&p, "1995 model"), s(&["term:model", "year:1995"]));
    assert_eq!(names(&p, "model 7"), s(&["term:7", "term:model"]));
    // Marker-driven typing (`#`/`/`) is punctuation-table territory (ADR-058), not this knob.
    assert_eq!(names(&p, "#1995"), s(&["term:1995"]));
}

#[test]
fn number_context_is_caller_supplied() {
    let q = NormalizerBuilder::new()
        .number_context_words(&["model"])
        .build()
        .unwrap();
    assert_eq!(names(&q, "model 1995"), s(&["term:1995", "term:model"]));
    assert_eq!(names(&q, "series 1995"), s(&["term:series", "year:1995"]));
}

// ---- vocab-driven pipeline (spec vocab) — never reached by the oracle ----

#[test]
fn multiword_phrases_collapse_to_one_feature() {
    let n = sample_vocab();
    // normalization.md §1/§2: a multiword entity is ONE feature, not its tokens.
    assert_eq!(names(&n, "wireless mouse"), s(&["entity:wireless_mouse"]));
    assert_eq!(names(&n, "acme labs"), s(&["brand:acme_labs"]));
}

#[test]
fn whitespace_runs_are_not_collapsed_in_canonical_features() {
    // ADR-061 (codex R8): `clean_with` does NOT collapse whitespace runs — the canonical / compile
    // feature output is byte-identical across versions, so a persisted segment never desyncs on a
    // binary upgrade. A double-spaced phrase therefore tokenizes to its COMPONENTS here. Matching a
    // whitespace-run TITLE against an alias is handled recall-safely by the positive-view overlap
    // scan (`tests/oracle/alias.rs::multiword_alias_matches_a_double_space_title`), which never
    // touches these canonical features.
    let n = sample_vocab();
    assert_eq!(
        names(&n, "wireless  mouse"),
        s(&["term:mouse", "term:wireless"]),
        "double space → components (not collapsed)"
    );
    assert_eq!(
        names(&n, "wireless mouse"),
        s(&["entity:wireless_mouse"]),
        "single space → the phrase entity (unchanged)"
    );
}

#[test]
fn query_side_collapses_whitespace_runs_only_when_aliases_active() {
    // ADR-061 (codex R11): alias patterns are registered single-spaced, and the DSL hands a
    // quoted phrase's inner text to `compile_features` verbatim — so a whitespace run inside a
    // query phrase (`"new  york"`) would hide the alias from the query-side collapse: the query
    // compiles to component terms, equivalence expansion never reaches the group, and
    // `"new  york" catalog` misses a `ny catalog` title (a false negative). With an alias active, the
    // QUERY side therefore collapses runs before the phrase scan. The title canonical view stays
    // verbatim (codex R8: persisted normalization never changes), and without an alias the query
    // side is byte-identical (`whitespace_runs_are_not_collapsed_in_canonical_features` above).
    let mut b = NormalizerBuilder::new();
    b.add_alias_form("new york");
    let n = b.build().expect("alias normalizer");

    assert_eq!(
        names(&n, "new  york catalog"),
        s(&["term:catalog", "term:new_york"]),
        "query side: a run inside the alias span still collapses to the entity"
    );

    // Title side under the same normalizer: canonical N(T) keeps the run verbatim (components,
    // no entity); the P(T) overlap scan — which collapses runs itself — recovers the entity.
    let mut dict = Dict::new();
    let mut lc = String::new();
    let _ = n.compile_features("new york", &mut dict, &mut lc); // intern the entity dense
    let entity = dict.get_or_synthetic("term:new_york");
    let mut sc = super::NormScratch::new();
    let (mut neg, mut pos) = (Vec::new(), Vec::new());
    n.match_features_dual(
        "new  york catalog",
        &dict,
        &mut lc,
        &mut sc,
        &mut neg,
        &mut pos,
    );
    assert!(
        !neg.contains(&entity),
        "title canonical N(T) keeps whitespace runs verbatim (codex R8)"
    );
    assert!(
        pos.contains(&entity),
        "the P(T) overlap scan recovers the entity across the run"
    );
}

#[test]
fn boundary_invalid_match_cannot_suppress_a_valid_overlapping_alias() {
    // ADR-061 (codex R12, P1): the shared leftmost-longest automaton commits to a match BEFORE
    // the word-boundary check. With aliases `a b` and `b c`, the text `xa b c` contains `a b`
    // mid-token (inside `xa b`) — the legacy pass selects it, consumes its span (suppressing the
    // genuinely valid `b c`), and then drops it at the boundary post-filter: no phrase at all.
    // On the query side that compiles an alias query to component terms, so equivalence
    // expansion never reaches the group (an FN). With aliases active, selection runs over the
    // boundary-VALID candidates only, so `b c` collapses to its entity.
    let mut b = NormalizerBuilder::new();
    b.add_alias_form("a b");
    b.add_alias_form("b c");
    let n = b.build().expect("alias normalizer");
    assert_eq!(
        names(&n, "xa b c"),
        s(&["term:b_c", "term:xa"]),
        "the valid `b c` must be selected despite the mid-token `a b` candidate"
    );
    // No mid-token candidate: identical to the legacy leftmost-longest selection.
    assert_eq!(names(&n, "a b c"), s(&["term:a_b", "term:c"]));
}

#[test]
fn synonyms_converge_alternate_surface_forms() {
    let n = sample_vocab();
    // A synonym and its declared phrase land on the same feature.
    assert_eq!(names(&n, "acme"), s(&["brand:acme_labs"]));
    assert_eq!(names(&n, "acme labs"), s(&["brand:acme_labs"]));
    assert_eq!(names(&n, "refurb"), s(&["category:refurbished"]));
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
    let first = n.compile_features("wireless mouse model 10", &mut dict, &mut lc);
    let len_after_first = dict.len();
    let second = n.compile_features("wireless mouse model 10", &mut dict, &mut lc);
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
    // `.` defaults to Keep; reclassifying it to Fold deletes it.
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
    assert_eq!(names(&n, "#2 widget"), s(&["term:2", "term:widget"]));
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

    // Title side: dual view of "new york city inventory".
    let mut sc = super::NormScratch::new();
    let (mut neg, mut pos) = (Vec::new(), Vec::new());
    norm.match_features_dual(
        "new york city inventory",
        &dict,
        &mut lc,
        &mut sc,
        &mut neg,
        &mut pos,
    );

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
fn positive_view_is_always_a_superset_of_negative() {
    // P(T) must union the canonical view with every additive/overlapping
    // entity and raw component; it can never replace N(T).
    let mut b = NormalizerBuilder::new();
    b.add_phrase(&["alpha", "beta"], "term:alpha_beta", FeatureKind::Generic);
    b.add_alias_form("new york"); // ⇒ the dual (P(T)/N(T)) path is active
    let n = b.build().expect("normalizer");
    let mut dict = Dict::new();
    let mut lc = String::new();
    let _ = n.compile_features("alpha beta 10", &mut dict, &mut lc);

    let mut sc = super::NormScratch::new();
    let (mut neg, mut pos) = (Vec::new(), Vec::new());
    n.match_features_dual("alpha beta 10", &dict, &mut lc, &mut sc, &mut neg, &mut pos);
    let ten = dict.get_or_synthetic("term:10");
    assert!(
        neg.contains(&ten),
        "N(T) reads the trailing number as term:10"
    );
    for f in &neg {
        assert!(
            pos.contains(f),
            "P(T) must contain every N(T) feature (superset) — incl. {}",
            dict.name(*f)
        );
    }
}

#[test]
fn dual_view_equals_single_view_without_aliases() {
    let n = sample_vocab();
    let mut dict = Dict::new();
    let mut lc = String::new();
    let title = "1994 acme labs wireless mouse model 10";
    // Seed the dict with a mutating compile so ids are dense.
    let _ = n.compile_features(title, &mut dict, &mut lc);

    let mut sc = super::NormScratch::new();
    let mut single = Vec::new();
    n.match_features(title, &dict, &mut lc, &mut sc, &mut single);
    let (mut neg, mut pos) = (Vec::new(), Vec::new());
    n.match_features_dual(title, &dict, &mut lc, &mut sc, &mut neg, &mut pos);
    assert_eq!(neg, single, "negative view == single view without aliases");
    assert_eq!(pos, single, "positive view == single view without aliases");
}
