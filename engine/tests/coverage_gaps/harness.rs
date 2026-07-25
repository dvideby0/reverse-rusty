//! Shared test harness for the coverage-gap suite.

use reverse_rusty::compile::{extract, Extracted};
use reverse_rusty::dict::Dict;
use reverse_rusty::normalize::Normalizer;
use std::collections::HashSet;

// ─────────────────────────────────────────────────────────────────────────────
// Helper: brute-force oracle (same as oracle.rs, reproduced here so this file
// is self-contained and can't share a bug with the main oracle).
// ─────────────────────────────────────────────────────────────────────────────

pub(crate) struct Brute {
    norm: Normalizer,
    dict: Dict,
    queries: Vec<(u64, Extracted)>,
}

impl Brute {
    pub(crate) fn build(queries: &[(u64, String)]) -> Self {
        let norm = Normalizer::default_vocab().expect("built-in vocab");
        let mut dict = Dict::new();
        let mut lc = String::new();
        let mut qs = Vec::new();
        for (logical, text) in queries {
            if let Ok(ast) = reverse_rusty::dsl::parse(text) {
                let ex = extract(&ast, &norm, &mut dict, &mut lc);
                if ex.required.is_empty() && ex.anyof.is_empty() && ex.required_phrases.is_empty() {
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

    pub(crate) fn matches(
        &self,
        title: &str,
        lc: &mut String,
        feats: &mut Vec<u32>,
    ) -> HashSet<u64> {
        let mut sc = reverse_rusty::normalize::NormScratch::new();
        let mut pos = Vec::new();
        let mut probe = Vec::new();
        let mut neg_arcs = Vec::new();
        let mut pos_arcs = Vec::new();
        let (positions, _complete) = self.norm.match_phrase_views(
            title,
            &self.dict,
            lc,
            &mut sc,
            feats,
            &mut pos,
            &mut probe,
            &mut neg_arcs,
            &mut pos_arcs,
        );
        let mut out = HashSet::new();
        for (logical, ex) in &self.queries {
            if ex.matches_positioned(&pos, feats, positions, &pos_arcs, positions, &neg_arcs) {
                out.insert(*logical);
            }
        }
        out
    }
}
