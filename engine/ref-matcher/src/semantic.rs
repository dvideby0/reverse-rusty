//! Grammar AST -> canonical semantic predicate tree.
//!
//! This module is deliberately **not** an independent copy of the production
//! compiler. It knows nothing about feature frequencies, retrieval anchors,
//! rarest-member proxies, cost classes, or storage columns. It preserves the
//! user-facing boolean structure from `docs/reference/dsl.md`:
//!
//! - top-level required clauses are ANDed;
//! - a forbidden clause negates its complete predicate;
//! - an any-of clause ORs complete member predicates;
//! - a term/member predicate ANDs its canonical feature requirements; and
//! - a phrase predicate retains its analyzed ordered graph.
//!
//! The reference matcher evaluates this tree directly against canonical title
//! views. Candidate generation belongs only to the production side of the
//! differential and is checked for recall by the integration harness.

use crate::features::Feature;
use crate::normalize::{
    compile_phrase, emit, match_phrase_views, phrase_graph_matches, RefPhraseGraph, RefPositionArc,
    Side,
};
use crate::parse::{Ast, Atom};
use crate::vocab::RefVocab;
use std::collections::{BTreeSet, HashMap};

/// One analyzed unquoted term expression.
///
/// Requirements are ANDed. Each requirement is a set of equivalent
/// alternatives and therefore ORed. Before positive equivalence expansion each
/// requirement contains exactly one canonical feature.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RefTermPredicate {
    pub requirements: Vec<Vec<Feature>>,
}

impl RefTermPredicate {
    fn from_text(vocab: &RefVocab, text: &str) -> Option<Self> {
        let mut features = emit(vocab, text, Side::Query, false);
        features.sort();
        features.dedup();
        if features.is_empty() {
            return None;
        }
        Some(Self {
            requirements: features.into_iter().map(|feature| vec![feature]).collect(),
        })
    }

    fn matches(&self, title_features: &[Feature]) -> bool {
        !self.requirements.is_empty()
            && self.requirements.iter().all(|alternatives| {
                alternatives
                    .iter()
                    .any(|feature| title_features.binary_search(feature).is_ok())
            })
    }

    fn expand_equivalences(&mut self, equivalents: &RefEquivMap) {
        for alternatives in &mut self.requirements {
            let mut widened = Vec::new();
            for feature in alternatives.iter() {
                if let Some(group) = equivalents.get(feature) {
                    widened.extend(group.iter().cloned());
                } else {
                    widened.push(feature.clone());
                }
            }
            widened.sort();
            widened.dedup();
            *alternatives = widened;
        }
    }
}

/// One user-facing top-level clause after independent canonical analysis.
///
/// Keeping polarity and predicate kind in the type prevents the semantic
/// oracle from accidentally flattening a negated group into individual
/// forbidden features or replacing an any-of member with a retrieval proxy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RefSemanticClause {
    RequiredTerm(RefTermPredicate),
    RequiredPhrase(RefPhraseGraph),
    RequiredAnyOf(Vec<RefTermPredicate>),
    ForbiddenTerm(RefTermPredicate),
    ForbiddenPhrase(RefPhraseGraph),
    ForbiddenAnyOf(Vec<RefTermPredicate>),
}

impl RefSemanticClause {
    fn is_required(&self) -> bool {
        matches!(
            self,
            Self::RequiredTerm(_) | Self::RequiredPhrase(_) | Self::RequiredAnyOf(_)
        )
    }

    fn expand_positive_equivalences(&mut self, equivalents: &RefEquivMap) {
        match self {
            Self::RequiredTerm(term) => term.expand_equivalences(equivalents),
            Self::RequiredAnyOf(members) => {
                for member in members {
                    member.expand_equivalences(equivalents);
                }
            }
            Self::RequiredPhrase(phrase) => {
                for arc in &mut phrase.arcs {
                    let mut widened = Vec::new();
                    for feature in &arc.alternatives {
                        if let Some(group) = equivalents.get(feature) {
                            widened.extend(group.iter().cloned());
                        } else {
                            widened.push(feature.clone());
                        }
                    }
                    widened.sort();
                    widened.dedup();
                    arc.alternatives = widened;
                }
            }
            Self::ForbiddenTerm(_) | Self::ForbiddenPhrase(_) | Self::ForbiddenAnyOf(_) => {
                // Equivalences widen only positive semantics. Applying them to
                // MUST_NOT clauses could reject titles the literal query permits.
            }
        }
    }

    fn holds(&self, title: &RefTitle) -> bool {
        match self {
            Self::RequiredTerm(term) => term.matches(&title.positive_features),
            Self::RequiredPhrase(phrase) => {
                phrase_graph_matches(phrase, title.positions, &title.positive_arcs)
            }
            Self::RequiredAnyOf(members) => members
                .iter()
                .any(|member| member.matches(&title.positive_features)),
            Self::ForbiddenTerm(term) => !term.matches(&title.canonical_features),
            Self::ForbiddenPhrase(phrase) => {
                !phrase_graph_matches(phrase, title.positions, &title.canonical_arcs)
            }
            Self::ForbiddenAnyOf(members) => !members
                .iter()
                .any(|member| member.matches(&title.canonical_features)),
        }
    }
}

/// The semantic ground truth for one stored DSL query.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RefSemanticQuery {
    pub clauses: Vec<RefSemanticClause>,
}

impl RefSemanticQuery {
    /// Evaluate the grammar tree directly against one canonical title model.
    #[must_use]
    pub fn matches(&self, title: &RefTitle) -> bool {
        self.clauses.iter().all(|clause| clause.holds(title))
    }

    /// Whether the production engine drops this query under its default
    /// class-D policy: it has no effective positive clause.
    #[must_use]
    pub fn is_class_d(&self) -> bool {
        !self.clauses.iter().any(RefSemanticClause::is_required)
    }

    /// Whether analysis produced no effective positive or forbidden predicate.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.clauses.is_empty()
    }

    fn expand_positive_equivalences(&mut self, equivalents: &RefEquivMap) {
        if equivalents.is_empty() {
            return;
        }
        for clause in &mut self.clauses {
            clause.expand_positive_equivalences(equivalents);
        }
    }
}

/// Canonical title representations consumed by the semantic predicate tree.
///
/// `positive_*` is the additive `P(T)` view used by required predicates;
/// `canonical_*` is `N(T)` and is used by forbidden predicates.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RefTitle {
    pub canonical_features: Vec<Feature>,
    pub positive_features: Vec<Feature>,
    pub positions: u32,
    pub canonical_arcs: Vec<RefPositionArc>,
    pub positive_arcs: Vec<RefPositionArc>,
}

impl RefTitle {
    #[must_use]
    pub fn analyze(vocab: &RefVocab, text: &str) -> Self {
        let (canonical_features, positive_features, positions, canonical_arcs, positive_arcs) =
            match_phrase_views(vocab, text);
        Self {
            canonical_features,
            positive_features,
            positions,
            canonical_arcs,
            positive_arcs,
        }
    }
}

/// Equivalence map: canonical feature -> its complete transitive class.
pub type RefEquivMap = HashMap<Feature, Vec<Feature>>;

/// Build a semantic query from the parsed grammar.
///
/// Consecutive positive bare terms are analyzed as one uninterrupted run so a
/// declared entity may be recognized. Every phrase, group, or forbidden clause
/// flushes that run before its own predicate is appended, preserving the public
/// clause boundaries.
#[must_use]
pub fn analyze(ast: &Ast, vocab: &RefVocab, equivalents: &RefEquivMap) -> RefSemanticQuery {
    let mut query = analyze_literal(ast, vocab);
    query.expand_positive_equivalences(equivalents);
    query
}

/// Build the literal semantic tree before positive equivalence expansion.
#[must_use]
pub fn analyze_literal(ast: &Ast, vocab: &RefVocab) -> RefSemanticQuery {
    let mut clauses = Vec::new();
    let mut positive_run: Vec<&str> = Vec::new();

    for clause in &ast.clauses {
        if !matches!((&clause.atom, clause.negated), (Atom::Term(_), false)) {
            flush_positive_run(&mut positive_run, vocab, &mut clauses);
        }

        match (&clause.atom, clause.negated) {
            (Atom::Term(word), false) => positive_run.push(word),
            (Atom::Term(word), true) => {
                if let Some(term) = RefTermPredicate::from_text(vocab, word) {
                    clauses.push(RefSemanticClause::ForbiddenTerm(term));
                }
            }
            (Atom::Phrase(text), false) => {
                let phrase = compile_phrase(vocab, text);
                if !phrase.arcs.is_empty() {
                    clauses.push(RefSemanticClause::RequiredPhrase(phrase));
                }
            }
            (Atom::Phrase(text), true) => {
                let phrase = compile_phrase(vocab, text);
                if !phrase.arcs.is_empty() {
                    clauses.push(RefSemanticClause::ForbiddenPhrase(phrase));
                }
            }
            (Atom::AnyOf(members), false) => {
                let members: Vec<_> = members
                    .iter()
                    .filter_map(|member| RefTermPredicate::from_text(vocab, member))
                    .collect();
                if !members.is_empty() {
                    clauses.push(RefSemanticClause::RequiredAnyOf(members));
                }
            }
            (Atom::AnyOf(members), true) => {
                let members: Vec<_> = members
                    .iter()
                    .filter_map(|member| RefTermPredicate::from_text(vocab, member))
                    .collect();
                if !members.is_empty() {
                    clauses.push(RefSemanticClause::ForbiddenAnyOf(members));
                }
            }
        }
    }

    flush_positive_run(&mut positive_run, vocab, &mut clauses);
    RefSemanticQuery { clauses }
}

fn flush_positive_run(
    words: &mut Vec<&str>,
    vocab: &RefVocab,
    clauses: &mut Vec<RefSemanticClause>,
) {
    if words.is_empty() {
        return;
    }
    if let Some(term) = RefTermPredicate::from_text(vocab, &words.join(" ")) {
        clauses.push(RefSemanticClause::RequiredTerm(term));
    }
    words.clear();
}

/// Resolve declared surface-form equivalences to transitive canonical feature
/// classes. A form participates only when it analyzes to one feature.
#[must_use]
pub fn resolve_equivalences(vocab: &RefVocab) -> RefEquivMap {
    let mut groups = Vec::new();
    for group in &vocab.equivalences {
        let mut features = BTreeSet::new();
        for form in group {
            let mut resolved = emit(vocab, form, Side::Query, false);
            resolved.sort();
            resolved.dedup();
            if resolved.len() == 1 {
                if let Some(feature) = resolved.into_iter().next() {
                    features.insert(feature);
                }
            }
        }
        if features.len() >= 2 {
            groups.push(features);
        }
    }

    let mut merged: Vec<BTreeSet<Feature>> = Vec::new();
    for group in groups {
        let mut class = group;
        let mut index = 0;
        while index < merged.len() {
            if merged[index].iter().any(|feature| class.contains(feature)) {
                class.extend(merged.swap_remove(index));
            } else {
                index += 1;
            }
        }
        merged.push(class);
    }

    let mut equivalents = HashMap::new();
    for class in merged {
        let group: Vec<_> = class.iter().cloned().collect();
        for feature in class {
            equivalents.insert(feature, group.clone());
        }
    }
    equivalents
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::parse;
    use crate::vocab::PhraseMode;

    fn names(term: &RefTermPredicate) -> Vec<Vec<&str>> {
        term.requirements
            .iter()
            .map(|alternatives| alternatives.iter().map(Feature::as_str).collect())
            .collect()
    }

    #[test]
    fn clause_boundaries_remain_visible_in_the_semantic_tree() {
        let vocab =
            RefVocab::default_vocab().phrase("new york", "term:new_york", PhraseMode::Alias);
        let cases = [
            "new -used york",
            "new \"vintage\" york",
            "new (vintage,modern) york",
        ];

        for source in cases {
            let ast = parse(source).expect("valid query");
            let query = analyze_literal(&ast, &vocab);
            let terms: Vec<_> = query
                .clauses
                .iter()
                .filter_map(|clause| match clause {
                    RefSemanticClause::RequiredTerm(term) => Some(names(term)),
                    _ => None,
                })
                .collect();
            assert_eq!(
                terms,
                vec![vec![vec!["term:new"]], vec![vec!["term:york"]]],
                "a non-term clause must prevent a synthetic new-york run: {source}"
            );
        }
    }

    #[test]
    fn any_of_members_are_complete_predicates_not_proxies() {
        let ast = parse("(red shoe,boot)").expect("valid query");
        let query = analyze_literal(&ast, &RefVocab::default_vocab());
        let [RefSemanticClause::RequiredAnyOf(members)] = query.clauses.as_slice() else {
            panic!("expected one required any-of clause: {query:?}");
        };
        assert_eq!(members.len(), 2);
        assert_eq!(
            names(&members[0]),
            vec![vec!["term:red"], vec!["term:shoe"]]
        );
        assert_eq!(names(&members[1]), vec![vec!["term:boot"]]);
    }

    #[test]
    fn required_and_forbidden_phrases_remain_graph_predicates() {
        let ast = parse("\"red shoe\" -\"for parts\"").expect("valid query");
        let query = analyze_literal(&ast, &RefVocab::default_vocab());
        assert!(matches!(
            query.clauses.first(),
            Some(RefSemanticClause::RequiredPhrase(graph)) if graph.positions == 2
        ));
        assert!(matches!(
            query.clauses.get(1),
            Some(RefSemanticClause::ForbiddenPhrase(graph)) if graph.positions == 2
        ));
    }

    #[test]
    fn forbidden_term_negates_its_analyzed_feature() {
        let vocab = RefVocab::default_vocab();
        let ast = parse("widget -broken").expect("valid query");
        let query = analyze_literal(&ast, &vocab);

        assert!(
            query.matches(&RefTitle::analyze(&vocab, "widget refurbished")),
            "an unrelated feature must remain allowed"
        );
        assert!(
            !query.matches(&RefTitle::analyze(&vocab, "widget broken")),
            "the analyzed forbidden feature must reject"
        );
    }
}
