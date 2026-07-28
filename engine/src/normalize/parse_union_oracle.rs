//! Independent exhaustive check for the ADR-061 positive-view parse union.
//!
//! The production positive view combines the canonical parse, a force-additive
//! parse, raw tokens, and overlapping phrase entities. This test enumerates all
//! collapse-or-token choices for short inputs and checks that every feature any
//! parse can emit is present in `P(T)`.

use super::{NormScratch, NormalizerBuilder};
use crate::dict::{Dict, FeatureKind};
use std::collections::BTreeSet;

#[derive(Clone, Copy)]
struct Phrase<'a> {
    tokens: &'a [&'a str],
    feature: &'a str,
}

const PHRASES: &[Phrase<'_>] = &[
    Phrase {
        tokens: &["alpha", "beta"],
        feature: "entity:alpha_beta",
    },
    Phrase {
        tokens: &["beta", "gamma"],
        feature: "entity:beta_gamma",
    },
    Phrase {
        tokens: &["alpha", "beta", "gamma"],
        feature: "entity:alpha_beta_gamma",
    },
];

fn enumerate_parse_features(
    tokens: &[&str],
    at: usize,
    current: &mut BTreeSet<String>,
    union: &mut BTreeSet<String>,
) {
    if at == tokens.len() {
        union.extend(current.iter().cloned());
        return;
    }

    let raw = format!("term:{}", tokens[at]);
    current.insert(raw.clone());
    enumerate_parse_features(tokens, at + 1, current, union);
    current.remove(&raw);

    for phrase in PHRASES {
        let end = at.saturating_add(phrase.tokens.len());
        if tokens.get(at..end) != Some(phrase.tokens) {
            continue;
        }
        current.insert(phrase.feature.to_string());
        enumerate_parse_features(tokens, end, current, union);
        current.remove(phrase.feature);
    }
}

fn titles(alphabet: &[&'static str], max_len: usize) -> Vec<Vec<&'static str>> {
    fn extend(
        alphabet: &[&'static str],
        remaining: usize,
        current: &mut Vec<&'static str>,
        out: &mut Vec<Vec<&'static str>>,
    ) {
        if !current.is_empty() {
            out.push(current.clone());
        }
        if remaining == 0 {
            return;
        }
        for token in alphabet {
            current.push(token);
            extend(alphabet, remaining - 1, current, out);
            current.pop();
        }
    }

    let mut out = Vec::new();
    extend(alphabet, max_len, &mut Vec::new(), &mut out);
    out
}

#[test]
fn positive_view_covers_every_short_phrase_parse() {
    let mut builder = NormalizerBuilder::new();
    for phrase in PHRASES {
        builder.add_phrase(phrase.tokens, phrase.feature, FeatureKind::Entity);
    }
    // Activate the dual-view path without adding a pattern that occurs in the
    // generated titles.
    builder.add_alias_form("left right");
    let normalizer = builder.build().expect("generic parse-union normalizer");

    let mut dict = Dict::new();
    let mut lc = String::new();
    for token in ["alpha", "beta", "gamma"] {
        normalizer.compile_features(token, &mut dict, &mut lc);
    }
    for phrase in PHRASES {
        normalizer.compile_features(&phrase.tokens.join(" "), &mut dict, &mut lc);
    }

    let mut scratch = NormScratch::new();
    let mut negative = Vec::new();
    let mut positive = Vec::new();
    for title_tokens in titles(&["alpha", "beta", "gamma"], 5) {
        let title = title_tokens.join(" ");
        normalizer.match_features_dual(
            &title,
            &dict,
            &mut lc,
            &mut scratch,
            &mut negative,
            &mut positive,
        );

        let mut expected = BTreeSet::new();
        enumerate_parse_features(&title_tokens, 0, &mut BTreeSet::new(), &mut expected);
        for feature in expected {
            let id = dict.get_or_synthetic(&feature);
            assert!(
                positive.binary_search(&id).is_ok(),
                "`{title}` lost parse feature `{feature}` from P(T)"
            );
        }
        for feature in &negative {
            assert!(
                positive.binary_search(feature).is_ok(),
                "`{title}` violates N(T) subset P(T)"
            );
        }
    }
}
