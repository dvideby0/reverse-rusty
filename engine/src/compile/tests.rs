//! Compile-time unit tests: golden extraction cases + the equivalence-expansion
//! rewrite. Split out of `compile.rs` verbatim; both submodules keep their
//! `#[cfg(test)]` gate and pull the module surface in via `use super::super::*`.

#[cfg(test)]
mod golden {
    //! Golden extraction cases — exact required/forbidden/any-of feature-*name* sets,
    //! authored by hand from the spec (docs/design/matching.md §1 +
    //! docs/design/normalization.md §1), NOT captured from `extract`. The differential
    //! oracle (tests/oracle.rs) builds its ground-truth queries by calling THIS
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

    /// The spec's worked-example vocabulary, plus the preview/previews synonyms the
    /// §1 example relies on to collapse `(preview,previews)` into one feature.
    fn spec_vocab() -> Normalizer {
        NormalizerBuilder::new()
            .phrase(&["upper", "deck"], "brand:upper_deck", FeatureKind::Brand)
            .phrase(
                &["michael", "jordan"],
                "player:michael_jordan",
                FeatureKind::Player,
            )
            .synonym("ud", "brand:upper_deck", FeatureKind::Brand)
            .synonym("sp", "card_term:sp", FeatureKind::Category)
            .synonym("preview", "card_term:preview", FeatureKind::Category)
            .synonym("previews", "card_term:preview", FeatureKind::Category)
            .grader("psa")
            .grader("bgs")
            .grader("sgc")
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
        // words and normalizes them as ONE stream): "michael jordan" compiles to the
        // same single feature a title produces, and a trailing synonym resolves in the
        // same pass.
        let n = spec_vocab();
        let (req, _, _) = named(&n, "michael jordan");
        assert_eq!(req, s(&["player:michael_jordan"]));
        let (req, _, _) = named(&n, "michael jordan sp");
        assert_eq!(req, s(&["card_term:sp", "player:michael_jordan"]));
    }

    #[test]
    fn forbidden_from_negations() {
        let n = Normalizer::default_vocab().unwrap();
        let (req, forb, anyof) = named(&n, "jacket -wallet -belt");
        assert_eq!(req, s(&["term:jacket"]));
        assert_eq!(forb, s(&["term:belt", "term:wallet"]));
        assert!(anyof.is_empty());

        // a negated phrase forbids all its features
        let (_, forb, _) = named(&n, "jacket -\"for parts\"");
        assert_eq!(forb, s(&["term:for", "term:parts"]));

        // a negated any-of forbids every member's features
        let (_, forb, _) = named(&n, "jacket -(used,returned)");
        assert_eq!(forb, s(&["term:returned", "term:used"]));
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
    fn anyof_dedups_repeated_members() {
        let n = Normalizer::default_vocab().unwrap();
        let (_, _, anyof) = named(&n, "(rookie,rc,rc)");
        assert_eq!(anyof, vec![s(&["term:rc", "term:rookie"])]);
    }

    #[test]
    fn singleton_anyof_is_promoted_to_required() {
        // (upper deck, UD) both normalize to brand:upper_deck, so the group collapses to
        // a singleton; extract promotes that into `required` (strictly more selective).
        // normalization.md §1 ("several OR groups become singletons").
        let n = spec_vocab();
        let (req, forb, anyof) = named(&n, "(upper deck,ud) jordan");
        assert_eq!(req, s(&["brand:upper_deck", "term:jordan"]));
        assert!(forb.is_empty());
        assert!(
            anyof.is_empty(),
            "the collapsed group is NOT left as an any-of"
        );
    }

    #[test]
    fn vocab_drives_grader_semantics() {
        // Identical query text; the vocabulary alone decides whether "psa 10" is two
        // generic terms or the grader triple. This is exactly the stage the empty-vocab
        // oracle cannot exercise.
        let (req_default, _, _) = named(&Normalizer::default_vocab().unwrap(), "psa 10");
        assert_eq!(req_default, s(&["term:10", "term:psa"]));
        let (req_spec, _, _) = named(&spec_vocab(), "psa 10");
        assert_eq!(
            req_spec,
            s(&["grade:10", "grader:psa", "grader_grade:psa10"])
        );
    }

    #[test]
    fn worked_example_compiles_as_documented() {
        // docs/design/normalization.md §1 — the spec's own compiled-output example.
        let n = spec_vocab();
        let q = "1994 (upper deck,UD) michael jordan sp (preview,previews) \
                 -(next,checklist,checklists,heroes,long,count) \
                 -(minor,minors,top,classic,alumni) \
                 -(auto,autograph,autographs,autographed,signed,dna,signature) \
                 PSA 10 -(sgc,bgs)";
        let (req, forb, anyof) = named(&n, q);

        // REQUIRED — exactly the doc's set, with both OR-singletons promoted in.
        assert_eq!(
            req,
            s(&[
                "brand:upper_deck",
                "card_term:preview",
                "card_term:sp",
                "grade:10",
                "grader:psa",
                "grader_grade:psa10",
                "player:michael_jordan",
                "year:1994",
            ])
        );

        // Both positive OR groups collapsed to singletons -> no any-of survives.
        assert!(anyof.is_empty());

        // FORBIDDEN — the doc prints a DEDUPLICATED, illustrative summary that elides the
        // morphological variants (checklists, minors, autograph(s)(ed)). With no stemmer
        // those are distinct features, so we assert the mechanically-exact set every
        // negated member produces (extract builds `forbidden` member-by-member). The
        // graders sgc/bgs normalize to grader:* features.
        assert_eq!(
            forb,
            s(&[
                "grader:bgs",
                "grader:sgc",
                "term:alumni",
                "term:auto",
                "term:autograph",
                "term:autographed",
                "term:autographs",
                "term:checklist",
                "term:checklists",
                "term:classic",
                "term:count",
                "term:dna",
                "term:heroes",
                "term:long",
                "term:minor",
                "term:minors",
                "term:next",
                "term:signature",
                "term:signed",
                "term:top",
            ])
        );
    }

    #[test]
    fn forbidden_never_appears_in_anchors() {
        // Signatures/anchors are built ONLY from required + any-of, never from forbidden
        // (the lossless-cover invariant; ADR-006). anchor_plan reads only
        // ex.required/ex.anyof, so this holds by construction — assert it at the data
        // level as a regression guard against a future refactor.
        let n = spec_vocab();
        let mut dict = Dict::new();
        let mut lc = String::new();
        let ast = parse("michael jordan psa 10 -(auto,signed) -(sgc,bgs)").unwrap();
        let ex = extract(&ast, &n, &mut dict, &mut lc);
        dict.finalize_mask();
        let plan = anchor_plan(&ex, &dict);
        let forbidden: std::collections::HashSet<FeatureId> =
            ex.forbidden.iter().copied().collect();
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
        let _ = build_signatures(&ex, &dict);
    }

    #[test]
    fn forbidden_only_query_is_class_d_with_no_anchors() {
        // A query with only a negation has no required feature and no any-of -> class D,
        // and produces NO anchor — the strongest "forbidden never gates" check.
        let n = Normalizer::default_vocab().unwrap();
        let mut dict = Dict::new();
        let mut lc = String::new();
        let ex = extract(&parse("-refurbished").unwrap(), &n, &mut dict, &mut lc);
        assert!(ex.required.is_empty());
        assert!(ex.anyof.is_empty());
        assert_eq!(ex.forbidden.len(), 1);
        dict.finalize_mask();
        let plan = anchor_plan(&ex, &dict);
        assert_eq!(plan.class, CostClass::D);
        assert!(plan.main_anchors.is_empty() && plan.broad_anchors.is_empty());
    }
}

#[cfg(test)]
mod equiv_tests {
    //! Unit tests for the equivalence expansion pass (ADR-054). These exercise the pure
    //! `Extracted::expand_equivalences` rewrite in isolation; the end-to-end zero-false-
    //! negative + monotonicity proofs live in tests/oracle.rs and tests/cluster_oracle.rs.
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
        };
        ex.expand_equivalences(&g);
        assert_eq!(ex.anyof, vec![vec![10, 20, 30]]);
    }

    #[test]
    fn empty_map_is_a_noop() {
        let g: crate::dict::EquivMap = crate::util::fast_map();
        let before = Extracted {
            required: vec![1, 2],
            forbidden: vec![3],
            anyof: vec![vec![4, 5]],
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
        };
        once.expand_equivalences(&g);
        let mut twice = once.clone();
        twice.expand_equivalences(&g);
        assert_eq!(once.required, twice.required);
        assert_eq!(once.anyof, twice.anyof);
    }
}
