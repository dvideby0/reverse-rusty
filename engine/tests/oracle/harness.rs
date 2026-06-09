//! Shared oracle harness: the independent brute-force ground-truth matcher.
//!
//! The brute-force side uses its own Dict/Normalizer *instances* and independently
//! reimplements candidate retrieval + exact verification — so an index / retrieval /
//! verify bug can't hide here. It does NOT independently verify the FRONT END: it calls
//! the engine's own `dsl::parse`, `compile::extract`, and `Normalizer` (and, except in
//! `zero_false_negatives_with_populated_vocab`, the empty `default_vocab`). The parser,
//! extractor, and normalization-model semantics are pinned instead by the spec-authored
//! golden tests in `src/{dsl,normalize,compile}.rs` (`mod golden`). See DECISIONS.md ADR-050.

use reverse_rusty::compile::{extract, Extracted};
use reverse_rusty::dict::Dict;
use reverse_rusty::normalize::Normalizer;
use std::collections::HashSet;

/// Independent ground-truth matcher over extracted queries.
pub(crate) struct Brute {
    norm: Normalizer,
    dict: Dict,
    queries: Vec<(u64, Extracted)>,
}

impl Brute {
    pub(crate) fn build(queries: &[(u64, String)]) -> Self {
        Self::build_with(
            queries,
            Normalizer::default_vocab().expect("built-in vocab"),
        )
    }

    /// Build the brute reference with an explicit normalizer vocabulary. The default
    /// `build` uses the empty `default_vocab` (so the phrase/synonym/grader paths are
    /// never exercised); `zero_false_negatives_with_populated_vocab` passes a populated
    /// one so they are. See docs/DECISIONS.md ADR-050.
    pub(crate) fn build_with(queries: &[(u64, String)], norm: Normalizer) -> Self {
        let mut dict = Dict::new();
        let mut lc = String::new();
        let mut qs = Vec::new();
        for (logical, text) in queries {
            if let Ok(ast) = reverse_rusty::dsl::parse(text) {
                let ex = extract(&ast, &norm, &mut dict, &mut lc);
                // mirror the engine's class-D rejection: no required & no anyof
                if ex.required.is_empty() && ex.anyof.is_empty() {
                    continue;
                }
                qs.push((*logical, ex));
            }
        }
        dict.finalize_mask();
        Brute {
            norm,
            dict,
            queries: qs,
        }
    }

    /// Build an alias-aware brute reference: the normalizer carries the vocab's alias phrases and
    /// the dict carries the resolved equivalence map, set up exactly as `Engine::set_vocab` does
    /// (intern the forms, then resolve), so the ground truth independently applies the ADR-061
    /// two-view semantics. Used by the multi-word alias oracle.
    pub(crate) fn build_with_vocab(
        queries: &[(u64, String)],
        vocab: &reverse_rusty::vocab::Vocab,
    ) -> Self {
        let norm = vocab.to_normalizer().expect("vocab normalizer");
        let mut dict = Dict::new();
        let mut lc = String::new();
        // Mirror the engine: pin the alias-form ids, then install the equivalence map so
        // `extract` expands required→any-of through it.
        vocab.intern_equivalence_forms(&norm, &mut dict);
        let equiv = vocab.resolve_equivalences(&norm, &dict);
        dict.set_equivalences(equiv);
        let mut qs = Vec::new();
        for (logical, text) in queries {
            if let Ok(ast) = reverse_rusty::dsl::parse(text) {
                let ex = extract(&ast, &norm, &mut dict, &mut lc);
                if ex.required.is_empty() && ex.anyof.is_empty() {
                    continue;
                }
                qs.push((*logical, ex));
            }
        }
        dict.finalize_mask();
        Brute {
            norm,
            dict,
            queries: qs,
        }
    }

    /// Ground-truth match set for `title`, applying the ADR-061 **two-view** semantics: required
    /// and any-of are checked against the positive overlapping superset `P(T)`; forbidden is
    /// checked against the canonical leftmost-longest `N(T)`. `feats` is reused as the `N(T)`
    /// buffer. With no active multi-word alias the two views are identical and this is
    /// byte-identical to the single-view brute.
    pub(crate) fn matches(
        &self,
        title: &str,
        lc: &mut String,
        feats: &mut Vec<u32>,
    ) -> HashSet<u64> {
        let mut pos = Vec::new();
        self.norm
            .match_features_dual(title, &self.dict, lc, feats, &mut pos);
        let in_pos = |f: u32| pos.binary_search(&f).is_ok();
        let in_neg = |f: u32| feats.binary_search(&f).is_ok();
        let mut out = HashSet::new();
        for (logical, ex) in &self.queries {
            if ex.required.iter().all(|&f| in_pos(f))
                && !ex.forbidden.iter().any(|&f| in_neg(f))
                && ex.anyof.iter().all(|g| g.iter().any(|&f| in_pos(f)))
            {
                out.insert(*logical);
            }
        }
        out
    }
}
