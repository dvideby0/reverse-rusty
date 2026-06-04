//! Query compiler + signature-cover optimizer + cost classifier.
//!
//! Design: docs/design/matching.md §1
//! Invariant: Signatures built ONLY from required features / any-of groups,
//!   never from forbidden features (lossless cover contract)
//! Hot path: no — compilation is off the match path entirely
//!
//! Turns a parsed AST into the integer form the matcher uses, and chooses a
//! *lossless* set of candidate signatures. The key correctness rule: signatures
//! are built ONLY from required features / any-of groups, never from forbidden
//! features.

use crate::dict::{Dict, FeatureId};
use crate::dsl::{Ast, Atom};
use crate::normalize::Normalizer;
use crate::util::sig_key;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CostClass {
    /// highly selective (rare arity-1 anchor) — main index, realtime
    A,
    /// acceptable (arity-2 anchor, or selective any-of reps) — main index, realtime
    B,
    /// broad (only a hot anchor available) — broad lane, not the selective path
    C,
    /// pathological (no required feature and no any-of) — rejected at compile
    D,
}

/// The positive/negative integer form of a query (no signatures yet).
#[derive(Clone, Debug)]
pub struct Extracted {
    pub required: Vec<FeatureId>,   // AND
    pub forbidden: Vec<FeatureId>,  // none may be present
    pub anyof: Vec<Vec<FeatureId>>, // each group: >=1 member-proxy present
}

impl Extracted {
    /// Expand learned equivalence groups (ADR-054) into the query — the FN-safe
    /// "expansion, not collapse" application of an alias. A required feature in a group
    /// `G` is moved out of `required` and added as an any-of group `G` (so a title bearing
    /// ANY member of `G` still retrieves the query), and each existing any-of group is
    /// widened by its members' groups. `forbidden` is never touched (negation semantics
    /// must not be widened).
    ///
    /// Because this only ever WIDENS the accepted positive feature set, the query's match
    /// set can only grow — it can never drop a true match, so it **cannot introduce a false
    /// negative**; a wrong/low-confidence equivalence degrades to a bounded false positive
    /// (the cardinal-sin-free failure mode this engine is built around). A no-op when
    /// `equiv` is empty, so the default path is byte-identical. Idempotent.
    pub fn expand_equivalences(&mut self, equiv: &crate::dict::EquivMap) {
        if equiv.is_empty() {
            return;
        }
        // A required feature in an equivalence group becomes an any-of over the group.
        let mut still_required = Vec::with_capacity(self.required.len());
        for &f in &self.required {
            match equiv.get(&f) {
                Some(group) => self.anyof.push(group.clone()),
                None => still_required.push(f),
            }
        }
        self.required = still_required;
        // Widen every any-of group (incl. the ones just added) by its members' groups.
        for g in &mut self.anyof {
            let mut widened: Vec<FeatureId> = Vec::with_capacity(g.len());
            for &m in g.iter() {
                match equiv.get(&m) {
                    Some(group) => widened.extend_from_slice(group),
                    None => widened.push(m),
                }
            }
            widened.sort_unstable();
            widened.dedup();
            *g = widened;
        }
        // Canonicalize for determinism (dedup identical groups + keep required tidy).
        self.required.sort_unstable();
        self.required.dedup();
        self.anyof.sort_unstable();
        self.anyof.dedup();
    }
}

/// Fully compiled query (used for explain/demo; the at-scale path streams into
/// the segment SoA instead of retaining these).
#[derive(Clone, Debug)]
pub struct CompiledQuery {
    pub logical_id: u64,
    pub version: u32,
    pub extracted: Extracted,
    pub main_sigs: Vec<u64>,
    pub broad_sigs: Vec<u64>,
    pub cost_class: CostClass,
}

/// "hot" == one of the 64 most frequent features (has a common-mask bit).
/// Both compile and match agree on this, which is what keeps the cover lossless.
#[inline]
pub fn is_hot(dict: &Dict, f: FeatureId) -> bool {
    dict.mask_bit(f) != crate::dict::NO_MASK_BIT
}

/// Extract required / forbidden / any-of from an AST, interning features and
/// bumping their query-frequency. Run for every query in pass A.
pub fn extract(ast: &Ast, norm: &Normalizer, dict: &mut Dict, lc: &mut String) -> Extracted {
    let mut required: Vec<FeatureId> = Vec::new();
    let mut forbidden: Vec<FeatureId> = Vec::new();
    let mut anyof: Vec<Vec<FeatureId>> = Vec::new();

    // Consecutive positive bare words are normalized JOINTLY (in original order)
    // so multiword entities ("michael jordan", "psa 10") are recognized exactly
    // as they are in titles. Without this the query and title feature spaces
    // would disagree and we'd get false negatives.
    let mut pos_words: Vec<&str> = Vec::new();

    for clause in &ast.clauses {
        match (&clause.atom, clause.negated) {
            (Atom::Term(w), false) => {
                pos_words.push(w.as_str());
            }
            (Atom::Term(w) | Atom::Phrase(w), true) => {
                let feats = norm.compile_features(w, dict, lc);
                forbidden.extend_from_slice(&feats);
            }
            (Atom::Phrase(w), false) => {
                let feats = norm.compile_features(w, dict, lc);
                required.extend_from_slice(&feats);
            }
            (Atom::AnyOf(members), neg) => {
                if neg {
                    // -(a,b,c): reject if ANY member feature present
                    for m in members {
                        let feats = norm.compile_features(m, dict, lc);
                        forbidden.extend_from_slice(&feats);
                    }
                } else {
                    // (a,b,c): >=1 member present. Represent each member by its
                    // rarest (most specific) normalized feature.
                    let mut group: Vec<FeatureId> = Vec::new();
                    for m in members {
                        let feats = norm.compile_features(m, dict, lc);
                        if let Some(&rep) = feats.iter().min_by_key(|&&f| dict.freq(f)) {
                            group.push(rep);
                        }
                    }
                    group.sort_unstable();
                    group.dedup();
                    if group.len() == 1 {
                        // singleton group is just a required feature (more selective)
                        required.push(group[0]);
                    } else if !group.is_empty() {
                        anyof.push(group);
                    }
                }
            }
        }
    }

    // normalize the joined positive bare words as one stream
    if !pos_words.is_empty() {
        let joined = pos_words.join(" ");
        let feats = norm.compile_features(&joined, dict, lc);
        required.extend_from_slice(&feats);
    }

    required.sort_unstable();
    required.dedup();
    forbidden.sort_unstable();
    forbidden.dedup();

    // bump frequency once per distinct required/anyof feature (gating-relevant).
    // Frequencies reflect the LITERAL query (before equivalence expansion below), so the
    // hot-mask and anchor selection stay a function of the real corpus distribution.
    for &f in &required {
        dict.bump_freq(f);
    }
    for g in &anyof {
        for &f in g {
            dict.bump_freq(f);
        }
    }

    let mut out = Extracted {
        required,
        forbidden,
        anyof,
    };
    // Apply learned equivalences (ADR-054). No-op unless a vocabulary installed them on the
    // dict; FN-safe (the match set only grows). See `Extracted::expand_equivalences`.
    out.expand_equivalences(dict.equivalences());
    out
}

pub struct SigPlan {
    pub main_sigs: Vec<u64>,
    pub broad_sigs: Vec<u64>,
    pub class: CostClass,
}

/// The pre-hash form of a [`SigPlan`]: the actual *feature groups* the lossless
/// cover is built from, before they are folded into `sig_key`s. Each `main`/`broad`
/// entry is one signature's feature group (arity 1, or arity 2 for the escalated
/// class-B pair). `build_signatures` is exactly `anchor_plan` followed by
/// `sig_key` over each group, so the two cannot drift.
///
/// Exists so the cluster coordinator can place a query by its *anchor feature
/// identity* (not just the opaque hash) while reusing the optimizer's per-class
/// selection verbatim — see [`crate::cluster`]. The forbidden-feature invariant
/// holds for free: like `build_signatures`, this only ever reads
/// `ex.required` / `ex.anyof`, never `ex.forbidden`.
#[derive(Clone, Debug)]
pub struct AnchorPlan {
    /// Each group = one main-index signature's features (arity 1, or 2 for the
    /// escalated class-B pair). Empty for class C and class D.
    pub main_anchors: Vec<Vec<FeatureId>>,
    /// Each group = one broad-lane signature's features (always arity 1). Non-empty
    /// only for class C. Empty otherwise.
    pub broad_anchors: Vec<Vec<FeatureId>>,
    pub class: CostClass,
}

/// Choose the lossless signature cover's *anchor feature groups* and the cost
/// class (pass B, after the mask is finalized so `is_hot` is meaningful). This is
/// the single source of truth for anchor selection; [`build_signatures`] just
/// hashes the groups it returns.
pub fn anchor_plan(ex: &Extracted, dict: &Dict) -> AnchorPlan {
    let mut main_anchors: Vec<Vec<FeatureId>> = Vec::new();
    let mut broad_anchors: Vec<Vec<FeatureId>> = Vec::new();

    if ex.required.is_empty() && ex.anyof.is_empty() {
        return AnchorPlan {
            main_anchors,
            broad_anchors,
            class: CostClass::D,
        };
    }

    if ex.required.is_empty() {
        // required empty: cover via the most-selective any-of group.
        // choose the group whose worst (most frequent) member is least frequent.
        // `anyof` is non-empty here (the both-empty case returned class D above),
        // but handle None defensively rather than panicking on the hot build path.
        let Some(best) = ex
            .anyof
            .iter()
            .min_by_key(|g| g.iter().map(|&f| dict.freq(f)).max().unwrap_or(u32::MAX))
        else {
            return AnchorPlan {
                main_anchors,
                broad_anchors,
                class: CostClass::D,
            };
        };
        let all_selective = best.iter().all(|&f| !is_hot(dict, f));
        if all_selective {
            for &f in best {
                main_anchors.push(vec![f]);
            }
            AnchorPlan {
                main_anchors,
                broad_anchors,
                class: CostClass::B,
            }
        } else {
            for &f in best {
                broad_anchors.push(vec![f]);
            }
            AnchorPlan {
                main_anchors,
                broad_anchors,
                class: CostClass::C,
            }
        }
    } else {
        // required features sorted rarest-first
        let mut r = ex.required.clone();
        r.sort_by_key(|&f| dict.freq(f));
        let r1 = r[0];
        if !is_hot(dict, r1) {
            // arity-1 selective anchor
            main_anchors.push(vec![r1]);
            AnchorPlan {
                main_anchors,
                broad_anchors,
                class: CostClass::A,
            }
        } else if r.len() >= 2 {
            // hot rarest feature -> escalate to arity-2 with next-rarest
            let r2 = r[1];
            let (a, b) = if r1 < r2 { (r1, r2) } else { (r2, r1) };
            main_anchors.push(vec![a, b]);
            AnchorPlan {
                main_anchors,
                broad_anchors,
                class: CostClass::B,
            }
        } else {
            // single, hot required feature and nothing to pair -> broad lane
            broad_anchors.push(vec![r1]);
            AnchorPlan {
                main_anchors,
                broad_anchors,
                class: CostClass::C,
            }
        }
    }
}

/// Choose a lossless signature cover and a cost class (pass B, after the mask
/// is finalized so `is_hot` is meaningful). Thin wrapper over [`anchor_plan`]:
/// hashes each anchor group into its `sig_key`. Keeping the two in lockstep is
/// what lets the cluster place by anchor identity without re-deriving selection.
pub fn build_signatures(ex: &Extracted, dict: &Dict) -> SigPlan {
    let plan = anchor_plan(ex, dict);
    SigPlan {
        main_sigs: plan.main_anchors.iter().map(|g| sig_key(g)).collect(),
        broad_sigs: plan.broad_anchors.iter().map(|g| sig_key(g)).collect(),
        class: plan.class,
    }
}

/// Convenience: full compile for a single query (explain/demo path).
pub fn compile_one(
    text: &str,
    logical_id: u64,
    version: u32,
    norm: &Normalizer,
    dict: &mut Dict,
    lc: &mut String,
) -> Result<CompiledQuery, crate::error::ParseError> {
    let ast = crate::dsl::parse(text)?;
    let ex = extract(&ast, norm, dict, lc);
    if !dict.is_finalized() {
        dict.finalize_mask();
    }
    let plan = build_signatures(&ex, dict);
    Ok(CompiledQuery {
        logical_id,
        version,
        extracted: ex,
        main_sigs: plan.main_sigs,
        broad_sigs: plan.broad_sigs,
        cost_class: plan.class,
    })
}

/// Read-only extract: resolves features against the frozen dict WITHOUT interning
/// (interning new vocabulary would fork the `Arc<Dict>` shared across shards). A term
/// absent from the dict is NOT skipped — `compile_features_readonly` resolves it to a
/// deterministic synthetic `FeatureId` via `dict.get_or_synthetic()` (dynamic
/// vocabulary, ADR-046), so a new required term still anchors its query (a collision is
/// a bounded over-match, never a dropped match). Safe for the read path and the cluster
/// coordinator's incremental adds against a frozen shared dict.
pub fn extract_readonly(ast: &Ast, norm: &Normalizer, dict: &Dict, lc: &mut String) -> Extracted {
    let mut required: Vec<FeatureId> = Vec::new();
    let mut forbidden: Vec<FeatureId> = Vec::new();
    let mut anyof: Vec<Vec<FeatureId>> = Vec::new();

    let mut pos_words: Vec<&str> = Vec::new();

    for clause in &ast.clauses {
        match (&clause.atom, clause.negated) {
            (Atom::Term(w), false) => {
                pos_words.push(w.as_str());
            }
            (Atom::Term(w) | Atom::Phrase(w), true) => {
                let feats = norm.compile_features_readonly(w, dict, lc);
                forbidden.extend_from_slice(&feats);
            }
            (Atom::Phrase(w), false) => {
                let feats = norm.compile_features_readonly(w, dict, lc);
                required.extend_from_slice(&feats);
            }
            (Atom::AnyOf(members), neg) => {
                if neg {
                    for m in members {
                        let feats = norm.compile_features_readonly(m, dict, lc);
                        forbidden.extend_from_slice(&feats);
                    }
                } else {
                    let mut group: Vec<FeatureId> = Vec::new();
                    for m in members {
                        let feats = norm.compile_features_readonly(m, dict, lc);
                        if let Some(&rep) = feats.iter().min_by_key(|&&f| dict.freq(f)) {
                            group.push(rep);
                        }
                    }
                    group.sort_unstable();
                    group.dedup();
                    if group.len() == 1 {
                        required.push(group[0]);
                    } else if !group.is_empty() {
                        anyof.push(group);
                    }
                }
            }
        }
    }

    if !pos_words.is_empty() {
        let joined = pos_words.join(" ");
        let feats = norm.compile_features_readonly(&joined, dict, lc);
        required.extend_from_slice(&feats);
    }

    required.sort_unstable();
    required.dedup();
    forbidden.sort_unstable();
    forbidden.dedup();

    let mut out = Extracted {
        required,
        forbidden,
        anyof,
    };
    // Apply learned equivalences (ADR-054); no-op unless installed on the dict. FN-safe.
    out.expand_equivalences(dict.equivalences());
    out
}

/// Read-only compile: re-derives a CompiledQuery from query text without
/// mutating the Dict. Used for explain on the read path.
pub fn compile_one_readonly(
    text: &str,
    logical_id: u64,
    norm: &Normalizer,
    dict: &Dict,
    lc: &mut String,
) -> Result<CompiledQuery, crate::error::ParseError> {
    let ast = crate::dsl::parse(text)?;
    let ex = extract_readonly(&ast, norm, dict, lc);
    let plan = build_signatures(&ex, dict);
    Ok(CompiledQuery {
        logical_id,
        version: 0,
        extracted: ex,
        main_sigs: plan.main_sigs,
        broad_sigs: plan.broad_sigs,
        cost_class: plan.class,
    })
}

#[cfg(test)]
mod golden {
    //! Golden extraction cases — exact required/forbidden/any-of feature-*name* sets,
    //! authored by hand from the spec (docs/design/matching.md §1 +
    //! docs/design/normalization.md §1), NOT captured from `extract`. The differential
    //! oracle (tests/oracle.rs) builds its ground-truth queries by calling THIS
    //! `extract`, so an extraction bug corrupts both sides equally and stays invisible
    //! there. These pins close that gap, and additionally assert the load-bearing
    //! "forbidden never anchors" invariant at the data level. See docs/DECISIONS.md ADR-050.
    use super::*;
    use crate::dict::FeatureKind;
    use crate::dsl::parse;
    use crate::normalize::NormalizerBuilder;

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
    use super::*;

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
