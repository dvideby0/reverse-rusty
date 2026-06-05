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

    /// Build the brute reference under a `vocab` whose equivalence groups are resolved and
    /// applied exactly as the engine does at build time (ADR-054): resolve each form against the
    /// populated dict, install the map, and expand every extracted query's required features into
    /// any-of groups. The normalizer must be the vocab's own (`vocab.to_normalizer()`) so the
    /// gluing phrases for multi-token forms line up. Lets a differential assert engine ≡ brute
    /// (FN=0 AND FP=0) under declared synonyms/equivalences — not merely FN-safety. See ADR-060.
    pub(crate) fn build_with_vocab(
        queries: &[(u64, String)],
        norm: Normalizer,
        vocab: &reverse_rusty::vocab::Vocab,
    ) -> Self {
        let mut dict = Dict::new();
        let mut lc = String::new();
        let mut qs: Vec<(u64, Extracted)> = Vec::new();
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
        let equiv = vocab.resolve_equivalences(&norm, &dict);
        if !equiv.is_empty() {
            dict.set_equivalences(equiv);
            let map = dict.equivalences();
            for (_, ex) in &mut qs {
                ex.expand_equivalences(map);
            }
        }
        Brute {
            norm,
            dict,
            queries: qs,
        }
    }

    pub(crate) fn matches(
        &self,
        title: &str,
        lc: &mut String,
        feats: &mut Vec<u32>,
    ) -> HashSet<u64> {
        self.norm.match_features(title, &self.dict, lc, feats);
        let present = |f: u32| feats.binary_search(&f).is_ok();
        let mut out = HashSet::new();
        for (logical, ex) in &self.queries {
            if ex.required.iter().all(|&f| present(f))
                && !ex.forbidden.iter().any(|&f| present(f))
                && ex.anyof.iter().all(|g| g.iter().any(|&f| present(f)))
            {
                out.insert(*logical);
            }
        }
        out
    }
}
