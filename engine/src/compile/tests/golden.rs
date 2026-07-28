//! Golden extraction cases — exact required/forbidden/any-of feature-*name* sets,
//! authored by hand from the spec (docs/design/matching.md §1 +
//! docs/design/normalization.md §1), NOT captured from `extract`. The differential
//! oracle (tests/oracle/) builds its ground-truth queries by calling THIS
//! `extract`, so an extraction bug corrupts both sides equally and stays invisible
//! there. These pins close that gap, and additionally assert the load-bearing
//! "forbidden never anchors" invariant at the data level. See docs/DECISIONS.md ADR-050.
use super::super::*;
use crate::dict::{Dict, FeatureKind};
use crate::dsl::parse;
use crate::normalize::{Normalizer, NormalizerBuilder};

fn s(items: &[&str]) -> Vec<String> {
    items.iter().map(ToString::to_string).collect()
}

/// A domain-neutral sample vocabulary with equivalent singular/plural synonyms.
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
        .synonym("preview", "category:preview", FeatureKind::Category)
        .synonym("previews", "category:preview", FeatureKind::Category)
        .build()
        .expect("spec vocab automaton")
}

/// Extract `query` and resolve required/forbidden/any-of to sorted *name* sets.
/// Uses the mutating `extract` so `Dict::name` round-trips every feature.
#[allow(clippy::type_complexity)]
fn named(norm: &Normalizer, query: &str) -> (Vec<String>, Vec<String>, Vec<Vec<String>>) {
    let mut dict = Dict::new();
    let mut lc = String::new();
    let ast = parse(query).expect("parse");
    let ex = extract(&ast, norm, &mut dict, &mut lc);
    let to_names = |ids: &[FeatureId]| -> Vec<String> {
        let mut v: Vec<String> = ids.iter().map(|&f| dict.name(f).to_string()).collect();
        v.sort();
        v
    };
    let required = to_names(&ex.required);
    let forbidden = to_names(&ex.forbidden);
    let mut anyof: Vec<Vec<String>> = ex.anyof.iter().map(|g| to_names(g)).collect();
    anyof.sort();
    (required, forbidden, anyof)
}

fn semantic_match(norm: &Normalizer, dict: &Dict, ex: &Extracted, title: &str) -> bool {
    let mut lc = String::new();
    let mut scratch = crate::normalize::NormScratch::new();
    let mut neg = Vec::new();
    let mut pos = Vec::new();
    let mut probe = Vec::new();
    let mut neg_arcs = Vec::new();
    let mut pos_arcs = Vec::new();
    let (positions, complete) = norm.match_phrase_views(
        title,
        dict,
        &mut lc,
        &mut scratch,
        &mut neg,
        &mut pos,
        &mut probe,
        &mut neg_arcs,
        &mut pos_arcs,
    );
    ex.matches_positioned(
        &pos, &neg, positions, &pos_arcs, complete, positions, &neg_arcs,
    )
}

#[test]
fn required_from_positive_terms() {
    let n = Normalizer::default_vocab().unwrap();
    let (req, forb, anyof) = named(&n, "vintage leather jacket");
    assert_eq!(req, s(&["term:jacket", "term:leather", "term:vintage"]));
    assert!(forb.is_empty());
    assert!(anyof.is_empty());
}

#[test]
fn joint_multiword_normalization_aligns_query_and_title() {
    // The "feature spaces align" proof (compile.rs joins consecutive positive bare
    // words and normalizes them as ONE stream): "wireless mouse" compiles to the
    // same single feature a title produces, and a trailing synonym resolves in the
    // same pass.
    let n = sample_vocab();
    let (req, _, _) = named(&n, "wireless mouse");
    assert_eq!(req, s(&["entity:wireless_mouse"]));
    let (req, _, _) = named(&n, "wireless mouse refurb");
    assert_eq!(req, s(&["category:refurbished", "entity:wireless_mouse"]));
}

#[test]
fn forbidden_from_negations() {
    let n = Normalizer::default_vocab().unwrap();
    let (req, forb, anyof) = named(&n, "jacket -wallet -belt");
    assert_eq!(req, s(&["term:jacket"]));
    assert_eq!(forb, s(&["term:belt", "term:wallet"]));
    assert!(anyof.is_empty());

    // A negated phrase is one contiguous analyzed graph, not two
    // independent forbidden features (ADR-120).
    let mut dict = Dict::new();
    let mut lc = String::new();
    let ast = parse("jacket -\"for parts\"").expect("parse phrase");
    let ex = extract(&ast, &n, &mut dict, &mut lc);
    assert!(ex.forbidden.is_empty());
    assert_eq!(ex.forbidden_phrases.len(), 1);
    let mut labels: Vec<String> = ex.forbidden_phrases[0]
        .arcs
        .iter()
        .flat_map(|arc| arc.alternatives.iter())
        .map(|&feature| dict.name(feature).to_string())
        .collect();
    labels.sort();
    assert_eq!(labels, s(&["term:for", "term:parts"]));

    // a negated any-of forbids every member's features
    let (_, forb, _) = named(&n, "jacket -(used,returned)");
    assert_eq!(forb, s(&["term:returned", "term:used"]));
}

#[test]
fn query_frequency_deduplicates_positive_features_across_clause_families() {
    let norm = Normalizer::default_vocab().unwrap();
    let mut dict = Dict::new();
    let mut lc = String::new();
    let ast = parse("x (x,y) \"x a\" \"x b\" -\"x z\"").expect("parse");
    let _ = extract(&ast, &norm, &mut dict, &mut lc);

    let x = dict.get("term:x").expect("x feature");
    assert_eq!(
        dict.freq(x),
        1,
        "one query document must bump a shared bare/any-of/phrase label once"
    );
    for name in ["term:y", "term:a", "term:b"] {
        let feature = dict.get(name).expect("positive feature");
        assert_eq!(dict.freq(feature), 1, "{name} belongs to one query");
    }
    let forbidden_only = dict.get("term:z").expect("forbidden feature");
    assert_eq!(
        dict.freq(forbidden_only),
        0,
        "forbidden phrase labels never affect retrieval frequency"
    );
}

#[test]
fn anyof_group_keeps_one_rep_per_member() {
    let n = Normalizer::default_vocab().unwrap();
    let (req, forb, anyof) = named(&n, "(red,blue,green) jacket");
    assert_eq!(req, s(&["term:jacket"]));
    assert!(forb.is_empty());
    assert_eq!(anyof, vec![s(&["term:blue", "term:green", "term:red"])]);
}

#[test]
fn multi_token_anyof_members_remain_whole_predicates() {
    let norm = Normalizer::default_vocab().unwrap();
    let mut dict = Dict::new();
    let mut lc = String::new();
    let positive = extract(
        &parse("(red shoe,boot) marker").unwrap(),
        &norm,
        &mut dict,
        &mut lc,
    );
    assert_eq!(positive.anyof_predicates.len(), 1);
    assert_eq!(positive.anyof_predicates[0].members.len(), 2);
    assert!(semantic_match(&norm, &dict, &positive, "red shoe marker"));
    assert!(semantic_match(&norm, &dict, &positive, "boot marker"));
    assert!(!semantic_match(&norm, &dict, &positive, "red hat marker"));
    assert!(!semantic_match(&norm, &dict, &positive, "shoe marker"));

    let negative = extract(
        &parse("marker -(red shoe,boot)").unwrap(),
        &norm,
        &mut dict,
        &mut lc,
    );
    assert_eq!(negative.forbidden_conjunctions.len(), 1);
    assert!(semantic_match(&norm, &dict, &negative, "marker red hat"));
    assert!(!semantic_match(&norm, &dict, &negative, "marker red shoe"));
    assert!(!semantic_match(&norm, &dict, &negative, "marker boot"));
}

#[test]
fn distinct_members_survive_a_shared_retrieval_proxy() {
    let norm = Normalizer::default_vocab().unwrap();
    let mut dict = Dict::new();
    let mut lc = String::new();
    let ex = extract(
        &parse("(red shoe,red boot)").unwrap(),
        &norm,
        &mut dict,
        &mut lc,
    );
    assert_eq!(ex.anyof.len(), 1);
    assert_eq!(
        ex.anyof[0].len(),
        1,
        "the two members deliberately share one proxy"
    );
    assert_eq!(
        ex.anyof_predicates[0].members.len(),
        2,
        "proxy dedup must not collapse semantic members"
    );
    assert!(semantic_match(&norm, &dict, &ex, "red shoe"));
    assert!(semantic_match(&norm, &dict, &ex, "red boot"));
    assert!(!semantic_match(&norm, &dict, &ex, "red hat"));
}

#[test]
fn anyof_dedups_repeated_members() {
    let n = Normalizer::default_vocab().unwrap();
    let (_, _, anyof) = named(&n, "(refurb,used,used)");
    assert_eq!(anyof, vec![s(&["term:refurb", "term:used"])]);
}

#[test]
fn singleton_anyof_is_promoted_to_required() {
    // (acme labs, acme) both normalize to brand:acme_labs, so the group collapses to
    // a singleton; extract promotes that into `required` (strictly more selective).
    // normalization.md §1 ("several OR groups become singletons").
    let n = sample_vocab();
    let (req, forb, anyof) = named(&n, "(acme labs,acme) mouse");
    assert_eq!(req, s(&["brand:acme_labs", "term:mouse"]));
    assert!(forb.is_empty());
    assert!(
        anyof.is_empty(),
        "the collapsed group is NOT left as an any-of"
    );
}

#[test]
fn vocab_drives_generic_entity_semantics() {
    // Identical query text; the vocabulary alone decides whether it is two generic
    // terms or one declared entity.
    let (req_default, _, _) = named(&Normalizer::default_vocab().unwrap(), "wireless mouse");
    assert_eq!(req_default, s(&["term:mouse", "term:wireless"]));
    let (req_sample, _, _) = named(&sample_vocab(), "wireless mouse");
    assert_eq!(req_sample, s(&["entity:wireless_mouse"]));
}

#[test]
fn representative_generic_query_compiles_as_documented() {
    let n = sample_vocab();
    let q = "1994 (acme labs,acme) wireless mouse refurb (preview,previews) \
             -(broken,damaged,parts) -(blue,green)";
    let (req, forb, anyof) = named(&n, q);

    assert_eq!(
        req,
        s(&[
            "brand:acme_labs",
            "category:preview",
            "category:refurbished",
            "entity:wireless_mouse",
            "year:1994",
        ])
    );
    assert!(anyof.is_empty());
    assert_eq!(
        forb,
        s(&[
            "term:blue",
            "term:broken",
            "term:damaged",
            "term:green",
            "term:parts",
        ])
    );
}

#[test]
fn forbidden_never_appears_in_anchors() {
    // Signatures/anchors are built ONLY from positive requirements, never from forbidden
    // features or graphs (the lossless-cover invariant; ADR-006). Assert it at the data level
    // as a regression guard against a future refactor.
    let n = sample_vocab();
    let mut dict = Dict::new();
    let mut lc = String::new();
    let ast = parse("wireless mouse acme -(broken,damaged) -(blue,green)").unwrap();
    let ex = extract(&ast, &n, &mut dict, &mut lc);
    dict.finalize_mask();
    let plan = anchor_plan(&ex, &dict, 0);
    let forbidden: std::collections::HashSet<FeatureId> = ex.forbidden.iter().copied().collect();
    assert!(
        !forbidden.is_empty(),
        "test query must have forbidden features"
    );
    for group in plan.main_anchors.iter().chain(plan.broad_anchors.iter()) {
        for f in group {
            assert!(
                !forbidden.contains(f),
                "forbidden feature {} leaked into an anchor",
                dict.name(*f)
            );
        }
    }
    // build_signatures hashes exactly those groups, so the same holds for sig keys.
    let _ = build_signatures(&ex, &dict, 0);
}

#[test]
fn would_be_hot_flags_exactly_the_rank_cliff_shapes() {
    // The observe-first hot-tier counter (Broad-Query Cost Program increment 1):
    // `would_be_hot` must fire exactly when a plan keeps a query on the
    // always-probed main lane while its deciding anchor's frequency is already
    // ≥ DEFAULT_HOT_ANCHOR_THETA — the top-64 rank cliff ADR-104 measured (a
    // feature ranked #65+ carrying a fat posting yet classifying "selective").
    use crate::config::DEFAULT_HOT_ANCHOR_THETA;

    let theta = DEFAULT_HOT_ANCHOR_THETA;
    let mut dict = Dict::new();
    // 64 mask-holders: strictly more frequent than anything below, so they own
    // all 64 common-mask bits after finalize.
    let mut top64 = Vec::new();
    for i in 0..64u32 {
        let f = dict.intern(&format!("top{i}"), FeatureKind::Generic);
        for _ in 0..(theta * 2) {
            dict.bump_freq(f);
        }
        top64.push(f);
    }
    // The cliff features: ranked #65+ (no mask bit) with θ-level frequency.
    let fat = dict.intern("fatanchor", FeatureKind::Generic);
    for _ in 0..theta {
        dict.bump_freq(fat);
    }
    let fat2 = dict.intern("fatanchor2", FeatureKind::Generic);
    for _ in 0..theta + 100 {
        dict.bump_freq(fat2);
    }
    // Genuinely rare features.
    let rare = dict.intern("rareterm", FeatureKind::Generic);
    dict.bump_freq(rare);
    let just_under = dict.intern("justunder", FeatureKind::Generic);
    for _ in 0..theta - 1 {
        dict.bump_freq(just_under);
    }
    dict.finalize_mask();
    assert!(!is_hot(&dict, fat), "the cliff feature must not be top-64");
    assert!(is_hot(&dict, top64[0]));

    let ex = |required: Vec<FeatureId>, anyof: Vec<Vec<FeatureId>>| Extracted {
        required,
        forbidden: Vec::new(),
        anyof,
        anyof_predicates: Vec::new(),
        forbidden_conjunctions: Vec::new(),
        ..Extracted::default()
    };

    // Class A anchored on a θ-frequency non-top64 feature: the defect shape.
    let p = anchor_plan(&ex(vec![fat], vec![]), &dict, 0);
    assert_eq!(p.class, CostClass::A);
    assert!(p.would_be_hot, "θ-frequency class-A anchor must be flagged");

    // Exactly θ−1 stays unflagged (the boundary is ≥ θ).
    let p = anchor_plan(&ex(vec![just_under], vec![]), &dict, 0);
    assert_eq!(p.class, CostClass::A);
    assert!(!p.would_be_hot, "freq θ−1 is below the threshold");

    // A rare rarest-required keeps the query unflagged even with a fat co-feature
    // (the anchor is the rare one — nothing rides a fat posting).
    let p = anchor_plan(&ex(vec![rare, fat], vec![]), &dict, 0);
    assert_eq!(p.class, CostClass::A);
    assert!(!p.would_be_hot);

    // Rarest = the cliff feature while a top-64 co-feature exists: still the
    // defect shape (ADR-104's measured case — anchor_plan picks the #65 feature).
    let p = anchor_plan(&ex(vec![top64[0], fat], vec![]), &dict, 0);
    assert_eq!(p.class, CostClass::A);
    assert!(p.would_be_hot);

    // Top-64-anchored plans are never flagged: class C (single hot required)…
    let p = anchor_plan(&ex(vec![top64[0]], vec![]), &dict, 0);
    assert_eq!(p.class, CostClass::C);
    assert!(!p.would_be_hot);
    // …and the class-B arity-2 escalation.
    let p = anchor_plan(&ex(vec![top64[0], top64[1]], vec![]), &dict, 0);
    assert_eq!(p.class, CostClass::B);
    assert!(!p.would_be_hot);

    // Any-of class B: flagged iff the chosen group's WORST member is ≥ θ.
    let p = anchor_plan(&ex(vec![], vec![vec![rare, fat2]]), &dict, 0);
    assert_eq!(p.class, CostClass::B);
    assert!(p.would_be_hot, "group worst member ≥ θ must be flagged");
    let p = anchor_plan(&ex(vec![], vec![vec![rare, just_under]]), &dict, 0);
    assert_eq!(p.class, CostClass::B);
    assert!(!p.would_be_hot);
    // Any-of with a top-64 member is class C — never flagged.
    let p = anchor_plan(&ex(vec![], vec![vec![rare, top64[3]]]), &dict, 0);
    assert_eq!(p.class, CostClass::C);
    assert!(!p.would_be_hot);

    // Class D: never flagged.
    let mut d = ex(vec![], vec![]);
    d.forbidden.push(rare);
    let p = anchor_plan(&d, &dict, 0);
    assert_eq!(p.class, CostClass::D);
    assert!(!p.would_be_hot);

    // build_signatures carries the flag through unchanged.
    let limited = build_signatures(&ex(vec![fat], vec![]), &dict, 0);
    assert!(limited.would_be_hot);
}

#[test]
fn forbidden_only_query_is_class_d_with_the_universal_cover() {
    // A query with only a negation has no required feature and no any-of -> class D.
    // Its cover is the UNIVERSAL signature (one EMPTY broad-anchor group, ADR-068) —
    // still the strongest "forbidden never gates" check: no forbidden feature reaches
    // an anchor (the group is empty, derived without reading `forbidden` at all).
    let n = Normalizer::default_vocab().unwrap();
    let mut dict = Dict::new();
    let mut lc = String::new();
    let ex = extract(&parse("-refurbished").unwrap(), &n, &mut dict, &mut lc);
    assert!(ex.required.is_empty());
    assert!(ex.anyof.is_empty());
    assert_eq!(ex.forbidden.len(), 1);
    dict.finalize_mask();
    let plan = anchor_plan(&ex, &dict, 0);
    assert_eq!(plan.class, CostClass::D);
    assert!(plan.main_anchors.is_empty(), "class D never anchors main");
    assert_eq!(
        plan.broad_anchors,
        vec![Vec::<crate::dict::FeatureId>::new()],
        "one empty broad group — the universal cover, no feature in it"
    );
}
