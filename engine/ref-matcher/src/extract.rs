//! AST -> [`RefQuery`] — the positive/negative feature extraction. Independent reimplementation of
//! `engine/src/compile/extract.rs::extract` + `Extracted::expand_equivalences`.
//!
//! Two behaviours that are easy to miss and load-bearing:
//!   1. **Consecutive positive bare-word terms are normalized JOINTLY** — each maximal run is
//!      collected in order, space-joined, and run through the normalizer as ONE stream, so
//!      multi-word entities (`michael jordan`, `psa 10`) are recognized exactly as on the title
//!      side. Positive phrases, negations, and any-of clauses delimit those runs and are normalized
//!      separately.
//!   2. **Any-of member boundaries are semantic**: a multi-token member is the conjunction of all
//!      its normalized features; the surrounding group ORs those member predicates. One
//!      rarest-by-frequency feature per member remains only as a lossless retrieval proxy.

use crate::features::Feature;
use crate::normalize::{compile_phrase, emit, RefPhraseGraph, Side};
use crate::parse::{Ast, Atom};
use crate::vocab::RefVocab;
use std::collections::HashMap;

/// Per-feature query frequency, accumulated across the corpus in `matcher` (read-only here).
pub type Freq = HashMap<Feature, u32>;

/// Equivalence map: a feature -> the full set of features in its equivalence group (ADR-054).
pub type EquivMap = HashMap<Feature, Vec<Feature>>;

/// One any-of member: AND across requirements, OR across equivalent alternatives in one
/// requirement.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct RefAnyOfMember {
    pub requirements: Vec<Vec<Feature>>,
}

/// A positive any-of predicate: OR across members.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct RefAnyOfPredicate {
    pub members: Vec<RefAnyOfMember>,
}

/// A compiled query. `anyof` stores proxy groups (and the full simple-group predicate);
/// compound positive/negative member predicates retain their exact boundaries separately.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RefQuery {
    pub required: Vec<Feature>,
    pub forbidden: Vec<Feature>,
    pub anyof: Vec<Vec<Feature>>,
    pub anyof_predicates: Vec<RefAnyOfPredicate>,
    pub forbidden_conjunctions: Vec<Vec<Feature>>,
    pub required_phrases: Vec<RefPhraseGraph>,
    pub forbidden_phrases: Vec<RefPhraseGraph>,
}

/// Normalize one atom string on the query/compile side (sorted + deduped features).
fn norm_query(vocab: &RefVocab, w: &str) -> Vec<Feature> {
    let mut v = emit(vocab, w, Side::Query, false);
    v.sort();
    v.dedup();
    v
}

fn phrase_proxy(graph: &RefPhraseGraph) -> Vec<Feature> {
    let mut proxy: Vec<Feature> = graph
        .arcs
        .iter()
        .flat_map(|arc| arc.alternatives.iter().cloned())
        .collect();
    proxy.sort();
    proxy.dedup();
    proxy
}

/// The least-frequent feature of `feats` (the rarest member proxy). `feats` is sorted by string,
/// and `min_by_key` returns the first minimum, so a frequency tie breaks to the lexicographically
/// smallest feature. The engine breaks a tie by smallest interned id; either proxy is lossless
/// because exact matching independently checks the complete member predicate.
fn rarest_proxy(feats: &[Feature], freq: &Freq) -> Option<Feature> {
    feats
        .iter()
        .min_by_key(|f| freq.get(*f).copied().unwrap_or(0))
        .cloned()
}

/// Normalize and append one maximal run of consecutive positive bare terms.
/// A non-bare AST clause must flush the run before its own predicate is lowered;
/// otherwise separated terms can be misread as one multi-word entity.
fn flush_positive_run(words: &mut Vec<&str>, vocab: &RefVocab, required: &mut Vec<Feature>) {
    if words.is_empty() {
        return;
    }
    required.extend(norm_query(vocab, &words.join(" ")));
    words.clear();
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
    let mut anyof_predicates: Vec<RefAnyOfPredicate> = Vec::new();
    let mut forbidden_conjunctions: Vec<Vec<Feature>> = Vec::new();
    let mut required_phrases = Vec::new();
    let mut forbidden_phrases = Vec::new();
    let mut pos_words: Vec<&str> = Vec::new();

    for clause in &ast.clauses {
        if !matches!((&clause.atom, clause.negated), (Atom::Term(_), false)) {
            flush_positive_run(&mut pos_words, vocab, &mut required);
        }
        match (&clause.atom, clause.negated) {
            (Atom::Term(w), false) => pos_words.push(w.as_str()),
            (Atom::Term(w), true) => forbidden.extend(norm_query(vocab, w)),
            (Atom::Phrase(w), false) => {
                let phrase = compile_phrase(vocab, w);
                let proxy = phrase_proxy(&phrase);
                if !proxy.is_empty() {
                    anyof.push(proxy);
                    required_phrases.push(phrase);
                }
            }
            (Atom::Phrase(w), true) => {
                let phrase = compile_phrase(vocab, w);
                if !phrase.arcs.is_empty() {
                    forbidden_phrases.push(phrase);
                }
            }
            (Atom::AnyOf(members), true) => {
                for m in members {
                    let feats = norm_query(vocab, m);
                    match feats.as_slice() {
                        [feature] => forbidden.push(feature.clone()),
                        [] => {}
                        _ => forbidden_conjunctions.push(feats),
                    }
                }
            }
            (Atom::AnyOf(members), false) => {
                let mut semantic_members: Vec<RefAnyOfMember> = Vec::new();
                for m in members {
                    let feats = norm_query(vocab, m);
                    if !feats.is_empty() {
                        semantic_members.push(RefAnyOfMember {
                            requirements: feats.into_iter().map(|feature| vec![feature]).collect(),
                        });
                    }
                }
                semantic_members.sort();
                semantic_members.dedup();
                if semantic_members.len() == 1 {
                    for requirement in &semantic_members[0].requirements {
                        required.extend(requirement.iter().cloned());
                    }
                } else if !semantic_members.is_empty() {
                    let mut proxies = Vec::with_capacity(semantic_members.len());
                    for member in &semantic_members {
                        let features: Vec<Feature> = member
                            .requirements
                            .iter()
                            .filter_map(|requirement| requirement.first().cloned())
                            .collect();
                        if let Some(proxy) = rarest_proxy(&features, freq) {
                            proxies.push(proxy);
                        }
                    }
                    proxies.sort();
                    proxies.dedup();
                    if !proxies.is_empty() {
                        anyof.push(proxies);
                    }
                    if semantic_members
                        .iter()
                        .any(|member| member.requirements.len() > 1)
                    {
                        anyof_predicates.push(RefAnyOfPredicate {
                            members: semantic_members,
                        });
                    }
                }
            }
        }
    }

    flush_positive_run(&mut pos_words, vocab, &mut required);

    required.sort();
    required.dedup();
    forbidden.sort();
    forbidden.dedup();
    anyof_predicates.sort();
    anyof_predicates.dedup();
    for conjunction in &mut forbidden_conjunctions {
        conjunction.sort();
        conjunction.dedup();
    }
    forbidden_conjunctions.sort();
    forbidden_conjunctions.dedup();

    RefQuery {
        required,
        forbidden,
        anyof,
        anyof_predicates,
        forbidden_conjunctions,
        required_phrases,
        forbidden_phrases,
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
        for predicate in &mut self.anyof_predicates {
            for member in &mut predicate.members {
                for requirement in &mut member.requirements {
                    let mut widened = Vec::with_capacity(requirement.len());
                    for feature in requirement.iter() {
                        match equiv.get(feature) {
                            Some(group) => widened.extend(group.iter().cloned()),
                            None => widened.push(feature.clone()),
                        }
                    }
                    widened.sort();
                    widened.dedup();
                    *requirement = widened;
                }
                member.requirements.sort();
                member.requirements.dedup();
            }
            predicate.members.sort();
            predicate.members.dedup();
        }
        for phrase in &mut self.required_phrases {
            for arc in &mut phrase.arcs {
                let mut widened = Vec::with_capacity(arc.alternatives.len());
                for feature in &arc.alternatives {
                    match equiv.get(feature) {
                        Some(group) => widened.extend(group.iter().cloned()),
                        None => widened.push(feature.clone()),
                    }
                }
                widened.sort();
                widened.dedup();
                arc.alternatives = widened;
            }
        }
        self.required.sort();
        self.required.dedup();
        self.anyof.sort();
        self.anyof.dedup();
        self.anyof_predicates.sort();
        self.anyof_predicates.dedup();
        self.required_phrases.sort();
        self.required_phrases.dedup();
    }
}
