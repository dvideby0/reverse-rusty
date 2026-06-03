//! Corpus-driven feature learning: NPMI collocation mining over query text.
//!
//! Induces multi-token ENTITIES (e.g. `upper deck` -> `upper_deck`) from the
//! query corpus alone — no hand-coded vocabulary — via normalized pointwise
//! mutual information (the word2vec/Mikolov phrase trick), iterating
//! bigram -> trigram. This is the library core behind the `learn` binary and,
//! via [`learn_phrases_from_text`], the runtime vocab source wired into
//! `learn_and_apply` (ADR-053).
//!
//! Correctness: emits PHRASES only (entity gluing), never aliases. A phrase is
//! applied by the SAME normalizer to queries (recompile) and titles (match), so
//! it is lossless-cover safe — it only shifts which anchors/candidates are
//! selected, never drops a match (docs/research/corpus-feature-learning.md §3,
//! docs/design/README.md §2). Alias/equivalence learning — the one
//! correctness-sensitive sub-problem — is deliberately out of scope here.
//!
//! Hot path: no — corpus learning is admin/build-time only.

use std::collections::{HashMap, HashSet};

use crate::dict::FeatureKind;
use crate::vocab::Vocab;

/// A discovered multi-token entity and its co-occurrence statistics.
#[derive(Clone, Debug)]
pub struct Phrase {
    /// The entity, parts joined with `_` (e.g. `"upper_deck"`).
    pub token: String,
    /// Adjacent co-occurrence count across the corpus.
    pub count: usize,
    /// Normalized pointwise mutual information — binding strength.
    pub npmi: f64,
    /// Document frequency: queries containing the adjacent pair at least once.
    pub df: usize,
}

/// Tokenize a query string into a lowercased token stream. Keeps
/// ascii-alphanumeric and `.`, and treats everything else — including DSL
/// punctuation `()`, `-`, `,` — as a token boundary, so `-(a,b)` contributes
/// tokens `a`, `b` exactly like the bare words do. This is the same naive
/// tokenizer the corpus learner has always used; it is intentionally distinct
/// from the matching normalizer (the learner sees only co-occurrence text).
pub fn tokenize(q: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for ch in q.chars() {
        if ch.is_ascii_alphanumeric() || ch == '.' {
            cur.push(ch.to_ascii_lowercase());
        } else if !cur.is_empty() {
            out.push(std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// NPMI collocation mining over adjacent token pairs. Returns the bigrams whose
/// normalized PMI is at least `tau` and whose count is at least `min_count`,
/// sorted by frequency (descending).
pub fn learn_phrases(corpus: &[Vec<String>], min_count: usize, tau: f64) -> Vec<Phrase> {
    let mut uni: HashMap<&str, usize> = HashMap::new();
    let mut bi: HashMap<(&str, &str), usize> = HashMap::new();
    let mut total_uni: u64 = 0;
    let mut total_bi: u64 = 0;

    for q in corpus {
        for t in q {
            *uni.entry(t.as_str()).or_insert(0) += 1;
            total_uni += 1;
        }
        for w in q.windows(2) {
            *bi.entry((w[0].as_str(), w[1].as_str())).or_insert(0) += 1;
            total_bi += 1;
        }
    }

    // df per bigram: count of queries containing the adjacent pair at least once.
    let mut df: HashMap<(&str, &str), usize> = HashMap::new();
    for q in corpus {
        let mut seen: HashSet<(&str, &str)> = HashSet::new();
        for w in q.windows(2) {
            seen.insert((w[0].as_str(), w[1].as_str()));
        }
        for k in &seen {
            *df.entry(*k).or_insert(0) += 1;
        }
    }

    let tu = total_uni as f64;
    let tb = total_bi as f64;
    let mut phrases = Vec::new();
    for (&(a, b), &c) in &bi {
        if c < min_count {
            continue;
        }
        let p_ab = c as f64 / tb;
        let p_a = *uni.get(a).unwrap_or(&1) as f64 / tu;
        let p_b = *uni.get(b).unwrap_or(&1) as f64 / tu;
        let pmi = (p_ab / (p_a * p_b)).ln();
        let npmi = pmi / (-p_ab.ln());
        if npmi >= tau {
            phrases.push(Phrase {
                token: format!("{a}_{b}"),
                count: c,
                npmi,
                df: *df.get(&(a, b)).unwrap_or(&0),
            });
        }
    }
    phrases.sort_by_key(|p| std::cmp::Reverse(p.count));
    phrases
}

/// Rewrite the corpus, merging any adjacent pair that was learned as a phrase
/// (greedy, left-to-right). Used to iterate bigram -> trigram.
pub fn apply_phrases(corpus: &[Vec<String>], phrases: &[Phrase]) -> Vec<Vec<String>> {
    let set: HashSet<String> = phrases.iter().map(|p| p.token.clone()).collect();
    corpus
        .iter()
        .map(|q| {
            let mut out = Vec::with_capacity(q.len());
            let mut i = 0;
            while i < q.len() {
                if i + 1 < q.len() {
                    let cand = format!("{}_{}", q[i], q[i + 1]);
                    if set.contains(&cand) {
                        out.push(cand);
                        i += 2;
                        continue;
                    }
                }
                out.push(q[i].clone());
                i += 1;
            }
            out
        })
        .collect()
}

/// Learn multi-token entity phrases from raw query text and return them as a
/// [`Vocab`] of phrase entries ready to apply through the normalizer.
///
/// Tokenizes each query (DSL punctuation stripped), runs NPMI collocation
/// mining, and iterates `iterations` times (bigram -> trigram -> …) to grow
/// longer entities — stopping early once a round discovers nothing. Each
/// discovered entity `a_b[_c…]` becomes a phrase mapping its token parts to the
/// canonical feature `term:a_b[_c…]` — the same `term:` convention as
/// [`crate::vocab::learn_from_queries`] — with kind [`FeatureKind::Generic`].
///
/// Emits PHRASES only (never synonyms/aliases), so applying the result is
/// lossless-cover safe. The phrases are token-sorted, so the output is
/// deterministic regardless of hash-map iteration order.
pub fn learn_phrases_from_text(
    corpus: &[(u64, String)],
    min_count: usize,
    tau: f64,
    iterations: usize,
) -> Vocab {
    let mut toks: Vec<Vec<String>> = corpus.iter().map(|(_, q)| tokenize(q)).collect();

    // Accumulate discovered entities across iterations (dedup by joined token).
    let mut discovered: HashSet<String> = HashSet::new();
    for _ in 0..iterations.max(1) {
        let phrases = learn_phrases(&toks, min_count, tau);
        if phrases.is_empty() {
            break;
        }
        for p in &phrases {
            discovered.insert(p.token.clone());
        }
        toks = apply_phrases(&toks, &phrases);
    }

    // Add in sorted token order so the resulting Vocab is deterministic.
    let mut tokens: Vec<&String> = discovered.iter().collect();
    tokens.sort();

    let mut vocab = Vocab::new();
    for token in tokens {
        let parts: Vec<&str> = token.split('_').collect();
        if parts.len() < 2 {
            continue;
        }
        let canonical = format!("term:{token}");
        vocab.add_phrase(&parts, &canonical, FeatureKind::Generic);
    }
    vocab
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A corpus of `n` queries each shaped `"<a> <b> filler<i>"`, so the pair
    /// `(a, b)` co-occurs `n` times and never apart — a maximally bound pair.
    fn planted_pair(a: &str, b: &str, n: usize) -> Vec<(u64, String)> {
        (0..n)
            .map(|i| (i as u64, format!("{a} {b} filler{i}")))
            .collect()
    }

    #[test]
    fn tokenize_strips_dsl_punctuation() {
        assert_eq!(tokenize("Upper Deck"), vec!["upper", "deck"]);
        assert_eq!(tokenize("-(a,b)"), vec!["a", "b"]);
        assert_eq!(tokenize("psa 10 -reprint"), vec!["psa", "10", "reprint"]);
    }

    #[test]
    fn discovers_planted_collocation() {
        let corpus = planted_pair("upper", "deck", 40);
        let vocab = learn_phrases_from_text(&corpus, 5, 0.30, 2);
        let phrases = vocab.phrases();
        let entry = phrases
            .iter()
            .find(|p| p.tokens == vec!["upper".to_string(), "deck".to_string()])
            .expect("the planted upper/deck pair must be induced");
        assert_eq!(entry.canonical, "term:upper_deck");
        assert_eq!(entry.kind, crate::vocab::FeatureKindSer::Generic);
    }

    #[test]
    fn respects_min_count() {
        let corpus = planted_pair("upper", "deck", 40);
        // min_count above the planted count -> the pair is filtered out.
        let vocab = learn_phrases_from_text(&corpus, 41, 0.30, 2);
        assert!(vocab.phrases().is_empty());
    }

    #[test]
    fn tau_gate_filters_when_too_high() {
        let corpus = planted_pair("upper", "deck", 40);
        // An unreachably high tau filters every candidate, even a strong one.
        let vocab = learn_phrases_from_text(&corpus, 5, 100.0, 2);
        assert!(vocab.phrases().is_empty());
    }

    #[test]
    fn dedup_and_determinism() {
        // Planting the same pair twice as much must still yield exactly one entry,
        // and two runs over identical input must be byte-identical.
        let corpus = planted_pair("upper", "deck", 60);
        let a = learn_phrases_from_text(&corpus, 5, 0.30, 2);
        let b = learn_phrases_from_text(&corpus, 5, 0.30, 2);
        let upper_deck: Vec<_> = a
            .phrases()
            .iter()
            .filter(|p| p.tokens == vec!["upper".to_string(), "deck".to_string()])
            .collect();
        assert_eq!(upper_deck.len(), 1, "phrase must be de-duplicated");
        assert_eq!(
            a.to_json().expect("vocab a serializes"),
            b.to_json().expect("vocab b serializes"),
            "learning must be deterministic"
        );
    }

    #[test]
    fn empty_corpus_yields_empty_vocab() {
        let vocab = learn_phrases_from_text(&[], 3, 0.30, 2);
        assert!(vocab.phrases().is_empty());
        assert!(vocab.synonyms().is_empty());
    }

    #[test]
    fn iterates_bigram_to_trigram() {
        // Each query plants the adjacent triple "alpha beta gamma": iteration 1
        // glues the bigrams, iteration 2 grows the trigram.
        let corpus: Vec<(u64, String)> = (0..40)
            .map(|i| (i as u64, format!("alpha beta gamma filler{i}")))
            .collect();
        let vocab = learn_phrases_from_text(&corpus, 5, 0.30, 2);
        let trigram = vocab
            .phrases()
            .iter()
            .find(|p| p.tokens.len() == 3)
            .expect("a 3-token entity must be induced with iterations >= 2");
        assert_eq!(
            trigram.tokens,
            vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()]
        );
        assert_eq!(trigram.canonical, "term:alpha_beta_gamma");
    }

    #[test]
    fn single_iteration_stays_bigram() {
        let corpus: Vec<(u64, String)> = (0..40)
            .map(|i| (i as u64, format!("alpha beta gamma filler{i}")))
            .collect();
        let vocab = learn_phrases_from_text(&corpus, 5, 0.30, 1);
        assert!(
            vocab.phrases().iter().all(|p| p.tokens.len() == 2),
            "a single iteration must not grow trigrams"
        );
    }
}
