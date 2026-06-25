//! AST -> [`RefQuery`] — the positive/negative feature extraction. Independent reimplementation of
//! `engine/src/compile/extract.rs::extract` + `Extracted::expand_equivalences`.
//!
//! Two behaviours that are easy to miss and load-bearing:
//!   1. **Positive bare-word terms are normalized JOINTLY** — collected in order, space-joined, and
//!      run through the normalizer as ONE stream, so multi-word entities (`michael jordan`,
//!      `psa 10`) are recognized exactly as on the title side. Positive phrases, negations, and
//!      any-of members are each normalized separately.
//!   2. **Any-of members use a rarest-by-frequency proxy**: each member is represented by its
//!      single least-frequent normalized feature (frequency = how many prior queries carried the
//!      feature as required / any-of proxy — built across the corpus in `matcher`), a singleton
//!      group collapses to a required feature, and an empty group is dropped.

use crate::features::Feature;
use crate::normalize::{emit, Side};
use crate::parse::{Ast, Atom};
use crate::vocab::RefVocab;
use std::collections::HashMap;

/// Per-feature query frequency, accumulated across the corpus in `matcher` (read-only here).
pub type Freq = HashMap<Feature, u32>;

/// Equivalence map: a feature -> the full set of features in its equivalence group (ADR-054).
pub type EquivMap = HashMap<Feature, Vec<Feature>>;

/// A compiled query: required (AND), forbidden (none present), any-of (each group: >=1 present).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RefQuery {
    pub required: Vec<Feature>,
    pub forbidden: Vec<Feature>,
    pub anyof: Vec<Vec<Feature>>,
}

/// Normalize one atom string on the query/compile side (sorted + deduped features).
fn norm_query(vocab: &RefVocab, w: &str) -> Vec<Feature> {
    let mut v = emit(vocab, w, Side::Query, false);
    v.sort();
    v.dedup();
    v
}

/// The least-frequent feature of `feats` (the rarest member proxy). `feats` is sorted by string,
/// and `min_by_key` returns the first minimum, so a frequency tie breaks to the lexicographically
/// smallest feature. (The engine breaks the tie by smallest interned id; the choice only affects
/// titles bearing SOME but not all of a multi-token member, which the generated + gotcha corpora do
/// not produce — see ADR-087.)
fn rarest_proxy(feats: &[Feature], freq: &Freq) -> Option<Feature> {
    feats
        .iter()
        .min_by_key(|f| freq.get(*f).copied().unwrap_or(0))
        .cloned()
}

/// Extract a [`RefQuery`] from an AST. `freq` governs any-of proxy selection (reflecting queries
/// processed before this one); `equiv` drives equivalence expansion. The returned query is fully
/// expanded; the caller bumps `freq` with the PRE-expansion required + any-of proxies (it can
/// recover them via [`RefQuery::bump_features`] on the unexpanded form — but the engine bumps the
/// literal query, so `matcher` captures them before expansion).
#[must_use]
pub fn extract(ast: &Ast, vocab: &RefVocab, freq: &Freq, equiv: &EquivMap) -> RefQuery {
    let mut q = extract_literal(ast, vocab, freq);
    q.expand_equivalences(equiv);
    q
}

/// Extract the LITERAL query (before equivalence expansion) — the form whose features feed the
/// frequency counter (`engine/src/compile/extract.rs` bumps before `expand_equivalences`).
#[must_use]
pub fn extract_literal(ast: &Ast, vocab: &RefVocab, freq: &Freq) -> RefQuery {
    let mut required: Vec<Feature> = Vec::new();
    let mut forbidden: Vec<Feature> = Vec::new();
    let mut anyof: Vec<Vec<Feature>> = Vec::new();
    let mut pos_words: Vec<&str> = Vec::new();

    for clause in &ast.clauses {
        match (&clause.atom, clause.negated) {
            (Atom::Term(w), false) => pos_words.push(w.as_str()),
            (Atom::Term(w) | Atom::Phrase(w), true) => forbidden.extend(norm_query(vocab, w)),
            (Atom::Phrase(w), false) => required.extend(norm_query(vocab, w)),
            (Atom::AnyOf(members), true) => {
                for m in members {
                    forbidden.extend(norm_query(vocab, m));
                }
            }
            (Atom::AnyOf(members), false) => {
                let mut group: Vec<Feature> = Vec::new();
                for m in members {
                    let feats = norm_query(vocab, m);
                    if let Some(rep) = rarest_proxy(&feats, freq) {
                        group.push(rep);
                    }
                }
                group.sort();
                group.dedup();
                if group.len() == 1 {
                    required.push(group.pop().expect("len==1"));
                } else if !group.is_empty() {
                    anyof.push(group);
                }
            }
        }
    }

    if !pos_words.is_empty() {
        let joined = pos_words.join(" ");
        required.extend(norm_query(vocab, &joined));
    }

    required.sort();
    required.dedup();
    forbidden.sort();
    forbidden.dedup();

    RefQuery {
        required,
        forbidden,
        anyof,
    }
}

impl RefQuery {
    /// The distinct features whose frequency a query bumps: every required feature and every any-of
    /// proxy (NOT forbidden), reflecting the literal query (call before expansion).
    #[must_use]
    pub fn bump_features(&self) -> Vec<Feature> {
        let mut out = self.required.clone();
        for g in &self.anyof {
            out.extend(g.iter().cloned());
        }
        out
    }

    /// Whether the engine drops this query at ingest: no required feature AND no any-of group
    /// (a negation-only / empty query — class D). Forbidden-only queries are kept only by the
    /// always-candidate lane.
    #[must_use]
    pub fn is_class_d(&self) -> bool {
        self.required.is_empty() && self.anyof.is_empty()
    }

    /// Expand learned equivalences (ADR-054): a required feature in a group becomes an any-of over
    /// the group; every any-of group is widened by its members' groups. Forbidden is never touched.
    /// Only ever widens the positive set, so it cannot introduce a false negative. No-op when empty.
    pub fn expand_equivalences(&mut self, equiv: &EquivMap) {
        if equiv.is_empty() {
            return;
        }
        let mut still_required = Vec::with_capacity(self.required.len());
        for f in &self.required {
            match equiv.get(f) {
                Some(group) => self.anyof.push(group.clone()),
                None => still_required.push(f.clone()),
            }
        }
        self.required = still_required;
        for g in &mut self.anyof {
            let mut widened: Vec<Feature> = Vec::with_capacity(g.len());
            for m in g.iter() {
                match equiv.get(m) {
                    Some(group) => widened.extend(group.iter().cloned()),
                    None => widened.push(m.clone()),
                }
            }
            widened.sort();
            widened.dedup();
            *g = widened;
        }
        self.required.sort();
        self.required.dedup();
        self.anyof.sort();
        self.anyof.dedup();
    }
}
