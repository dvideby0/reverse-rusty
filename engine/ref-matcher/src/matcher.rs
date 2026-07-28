//! [`RefMatcher`] — the front-end-independent semantic reference.
//!
//! Queries are parsed into [`RefSemanticQuery`](crate::semantic::RefSemanticQuery), a direct
//! grammar tree with no production retrieval or storage concepts. Matching evaluates that tree
//! against independently analyzed canonical title views.

use crate::parse::parse;
use crate::semantic::{analyze, resolve_equivalences, RefSemanticQuery, RefTitle};
use crate::vocab::RefVocab;
use std::collections::HashSet;

/// A fixed vocabulary plus the semantic queries retained after class-D policy.
pub struct RefMatcher {
    vocab: RefVocab,
    queries: Vec<(u64, RefSemanticQuery)>,
}

impl RefMatcher {
    /// Build the reference, dropping queries with no effective positive clause
    /// to mirror the engine's default class-D policy.
    #[must_use]
    pub fn build(queries: &[(u64, String)], vocab: RefVocab) -> Self {
        Self::build_inner(queries, vocab, false)
    }

    /// Build the reference while keeping forbidden-only queries for the
    /// ADR-068 always-candidate lane. A semantically empty query is still
    /// dropped.
    #[must_use]
    pub fn build_accepting_class_d(queries: &[(u64, String)], vocab: RefVocab) -> Self {
        Self::build_inner(queries, vocab, true)
    }

    fn build_inner(queries: &[(u64, String)], vocab: RefVocab, accept_class_d: bool) -> Self {
        let equivalents = resolve_equivalences(&vocab);
        let mut retained = Vec::new();

        for (logical, text) in queries {
            let Ok(ast) = parse(text) else {
                continue;
            };
            let query = analyze(&ast, &vocab, &equivalents);
            let drop = if accept_class_d {
                query.is_empty()
            } else {
                query.is_class_d()
            };
            if !drop {
                retained.push((*logical, query));
            }
        }

        Self {
            vocab,
            queries: retained,
        }
    }

    /// The logical IDs whose semantic query trees hold for `title`.
    #[must_use]
    pub fn matches(&self, title: &str) -> HashSet<u64> {
        let title = RefTitle::analyze(&self.vocab, title);
        self.queries
            .iter()
            .filter_map(|(logical, query)| query.matches(&title).then_some(*logical))
            .collect()
    }

    /// The number of queries retained after parse and class-D policy.
    #[must_use]
    pub fn len(&self) -> usize {
        self.queries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vocab::PhraseMode;

    #[test]
    fn positive_bare_term_runs_stop_at_clause_boundaries() {
        let vocab = RefVocab::default_vocab()
            .phrase("new york", "term:new_york", PhraseMode::Alias)
            .equivalence(&["new york", "ny"]);
        let cases = [
            ("negated term", "new -used york", "new vintage product york"),
            (
                "negated phrase",
                "new -\"used item\" york",
                "new vintage product york",
            ),
            (
                "negated any-of",
                "new -(used,damaged) york",
                "new vintage product york",
            ),
            (
                "positive phrase",
                "new \"vintage\" york",
                "new vintage product york",
            ),
            (
                "positive any-of",
                "new (vintage,modern) york",
                "new vintage product york",
            ),
        ];

        for (boundary, query, title) in cases {
            let matcher = RefMatcher::build(&[(1, query.to_string())], vocab.clone());
            assert!(
                matcher.matches(title).contains(&1),
                "reference semantic analysis crossed the {boundary} boundary: query `{query}`, \
                 title `{title}`"
            );
        }
    }
}
