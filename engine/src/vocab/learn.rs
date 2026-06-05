//! Corpus learners + composers that *build* a [`Vocab`] (ADR-015 any-of synonyms,
//! ADR-053 NPMI phrase induction, ADR-054 equivalence learning via expansion).
//!
//! These are admin/build-time only — never on the match hot path.

use std::collections::HashMap;

use super::{FeatureKindSer, PhraseEntry, SynonymEntry, Vocab};
use crate::dsl::{self, Atom};

// ── Learning ────────────────────────────────────────────────────────────────

/// Learn synonym relationships from query any-of groups.
///
/// Scans each query's DSL, finds any-of groups, and treats members of the
/// same group as user-declared equivalents. Only keeps relationships that
/// appear in at least `min_count` distinct queries.
pub fn learn_from_queries(queries: &[(u64, String)], min_count: usize) -> Vocab {
    // Collect (canonical, alias) pairs with occurrence counts.
    // For each any-of group, pick the canonical (longest member, ties broken
    // lexicographically) and map every other member to it.
    let mut pair_counts: HashMap<(String, String), usize> = HashMap::new();

    for (_id, text) in queries {
        let Ok(ast) = dsl::parse(text) else {
            continue;
        };
        for clause in &ast.clauses {
            if clause.negated {
                continue;
            }
            if let Atom::AnyOf(members) = &clause.atom {
                if members.len() < 2 {
                    continue;
                }
                let normalized: Vec<String> = members.iter().map(|m| normalize_token(m)).collect();

                // pick canonical: longest, then lexicographic (safe: members.len() >= 2)
                let Some(canonical) = normalized
                    .iter()
                    .max_by(|a, b| a.len().cmp(&b.len()).then_with(|| b.cmp(a)))
                    .cloned()
                else {
                    continue;
                };

                for member in &normalized {
                    if member != &canonical {
                        let key = (canonical.clone(), member.clone());
                        *pair_counts.entry(key).or_insert(0) += 1;
                    }
                }
            }
        }
    }

    let mut vocab = Vocab::default();
    let mut seen_aliases: HashMap<String, String> = HashMap::new();

    for ((canonical, alias), count) in &pair_counts {
        if *count < min_count {
            continue;
        }
        // skip if this alias is already mapped to a different canonical
        if let Some(existing) = seen_aliases.get(alias) {
            if existing != canonical {
                continue;
            }
        }
        seen_aliases.insert(alias.clone(), canonical.clone());

        let canon_tokens: Vec<&str> = canonical.split('_').collect();
        let alias_tokens: Vec<&str> = alias.split('_').collect();

        let canon_feature = format!("term:{canonical}");

        // register the canonical as a phrase if multi-word
        if canon_tokens.len() > 1 {
            let already = vocab.phrases.iter().any(|p| p.canonical == canon_feature);
            if !already {
                vocab.phrases.push(PhraseEntry {
                    tokens: canon_tokens
                        .iter()
                        .map(std::string::ToString::to_string)
                        .collect(),
                    canonical: canon_feature.clone(),
                    kind: FeatureKindSer::Generic,
                    additive: false, // collapse: an any-of-learned alias canonicalizes
                });
            }
        }

        // register the alias
        if alias_tokens.len() > 1 {
            vocab.phrases.push(PhraseEntry {
                tokens: alias_tokens
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect(),
                canonical: canon_feature,
                kind: FeatureKindSer::Generic,
                additive: false, // collapse: an any-of-learned alias canonicalizes
            });
        } else {
            vocab.synonyms.push(SynonymEntry {
                token: alias.clone(),
                canonical: canon_feature,
                kind: FeatureKindSer::Generic,
            });
        }
    }

    // sort for determinism
    vocab.synonyms.sort_by(|a, b| a.token.cmp(&b.token));
    vocab.phrases.sort_by(|a, b| a.tokens.cmp(&b.tokens));

    vocab
}

fn normalize_token(text: &str) -> String {
    let mut out = String::new();
    for ch in text.chars() {
        let c = crate::normalize::fold_diacritic(ch);
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else if !out.is_empty() && !out.ends_with('_') {
            out.push('_');
        }
    }
    out.trim_end_matches('_').to_string()
}

/// Collect positive any-of **groups** (each sorted + deduped) that appear in at least
/// `min_count` queries, with the count. Group-level, unlike
/// [`learn_equivalences_from_queries`] (which decomposes a group into pairs): this
/// preserves `(psa, bgs, sgc)` as ONE 3-form group so the alias registry can classify it
/// as a multi-form category alternative rather than three variant pairs (ADR-060). Forms
/// are kept raw — resolved through the normalizer when applied. Negated groups are skipped
/// (a `-(a,b)` is a forbidden disjunction, never an equivalence assertion). Output is sorted
/// for determinism.
pub fn learn_anyof_groups(
    queries: &[(u64, String)],
    min_count: usize,
) -> Vec<(Vec<String>, usize)> {
    let mut counts: HashMap<Vec<String>, usize> = HashMap::new();
    for (_id, text) in queries {
        let Ok(ast) = dsl::parse(text) else {
            continue;
        };
        for clause in &ast.clauses {
            if clause.negated {
                continue;
            }
            if let Atom::AnyOf(members) = &clause.atom {
                let mut forms: Vec<String> = members.iter().map(|m| m.trim().to_string()).collect();
                forms.retain(|m| !m.is_empty());
                forms.sort();
                forms.dedup();
                if forms.len() >= 2 {
                    *counts.entry(forms).or_insert(0) += 1;
                }
            }
        }
    }
    let mut groups: Vec<(Vec<String>, usize)> = counts
        .into_iter()
        .filter(|(_, c)| *c >= min_count)
        .collect();
    groups.sort();
    groups
}

/// Configuration for [`learn_vocab_from_corpus`] — composes the ADR-015 any-of
/// learner with opt-in NPMI corpus phrase induction (ADR-053) and opt-in equivalence
/// (alias) learning via expansion (ADR-054).
///
/// The default disables both opt-ins, so the result is byte-identical to
/// [`learn_from_queries`] alone — every existing caller and oracle is unaffected.
#[derive(Debug, Clone)]
pub struct CorpusLearnConfig {
    /// Minimum any-of occurrences for a rule to be learned (the bare `min_count` of
    /// [`learn_from_queries`]).
    pub anyof_min_count: usize,
    /// Enable NPMI corpus phrase induction (off by default).
    pub corpus_phrases: bool,
    /// NPMI binding-strength threshold for an induced phrase.
    pub npmi_tau: f64,
    /// Minimum adjacent co-occurrence count for an induced phrase. Defaults
    /// small — a live corpus is far smaller than the `learn` binary's
    /// 500k-query default (min_count 50).
    pub npmi_min_count: usize,
    /// Bigram -> trigram growth passes.
    pub npmi_iterations: usize,
    /// Learn any-of groups as **equivalence groups** applied via FN-safe expansion
    /// (ADR-054) instead of collapse synonyms (the default). Off by default.
    pub learn_equivalences: bool,
}

impl Default for CorpusLearnConfig {
    fn default() -> Self {
        Self {
            anyof_min_count: 2,
            corpus_phrases: false,
            npmi_tau: 0.30,
            npmi_min_count: 3,
            npmi_iterations: 2,
            learn_equivalences: false,
        }
    }
}

/// Learn equivalence relationships from query any-of co-occurrence (ADR-054): an any-of group
/// `(a, b, c)` declares its members interchangeable. Each unordered **pair** within a group is
/// counted — so `(rc,rookie)` and `(rc,rookie,rookie card)` both reinforce `rc≡rookie`, matching
/// the pair-level [`learn_from_queries`] synonym learner rather than keying on the exact group.
/// A pair seen in at least `min_count` any-of groups is emitted as a 2-element equivalence
/// group; overlapping pairs are unioned transitively at apply time
/// ([`Vocab::resolve_equivalences`]). Forms are kept raw — resolved through the normalizer when
/// applied.
pub fn learn_equivalences_from_queries(
    queries: &[(u64, String)],
    min_count: usize,
) -> Vec<Vec<String>> {
    let mut pair_counts: HashMap<(String, String), usize> = HashMap::new();
    for (_id, text) in queries {
        let Ok(ast) = dsl::parse(text) else {
            continue;
        };
        for clause in &ast.clauses {
            if clause.negated {
                continue;
            }
            if let Atom::AnyOf(members) = &clause.atom {
                let mut forms: Vec<String> = members.iter().map(|m| m.trim().to_string()).collect();
                forms.retain(|m| !m.is_empty());
                forms.sort();
                forms.dedup();
                // Every unordered pair (forms is sorted, so i<j yields a<=b — a stable key).
                for i in 0..forms.len() {
                    for j in (i + 1)..forms.len() {
                        *pair_counts
                            .entry((forms[i].clone(), forms[j].clone()))
                            .or_insert(0) += 1;
                    }
                }
            }
        }
    }
    let mut groups: Vec<Vec<String>> = pair_counts
        .into_iter()
        .filter(|(_, c)| *c >= min_count)
        .map(|((a, b), _)| vec![a, b])
        .collect();
    groups.sort();
    groups
}

/// Learn a vocabulary from a query corpus. By default this is the ADR-015 any-of synonym
/// learner; with `cfg.learn_equivalences` the any-of groups are learned as **equivalence
/// groups** (expansion, ADR-054) instead of collapse synonyms; and with `cfg.corpus_phrases`
/// NPMI-induced entity phrases ([`crate::corpus::learn_phrases_from_text`], ADR-053) are
/// merged on top.
///
/// With both opt-ins off this returns exactly `learn_from_queries(corpus, cfg.anyof_min_count)`.
pub fn learn_vocab_from_corpus(corpus: &[(u64, String)], cfg: &CorpusLearnConfig) -> Vocab {
    let mut vocab = if cfg.learn_equivalences {
        // Expansion mode: any-of co-occurrence becomes equivalence groups, not synonyms.
        let mut v = Vocab::new();
        for grp in learn_equivalences_from_queries(corpus, cfg.anyof_min_count) {
            let refs: Vec<&str> = grp.iter().map(String::as_str).collect();
            v.add_equivalence(&refs);
        }
        v
    } else {
        learn_from_queries(corpus, cfg.anyof_min_count)
    };
    if cfg.corpus_phrases {
        let phrases = crate::corpus::learn_phrases_from_text(
            corpus,
            cfg.npmi_min_count,
            cfg.npmi_tau,
            cfg.npmi_iterations,
        );
        vocab.merge(&phrases);
    }
    vocab
}
