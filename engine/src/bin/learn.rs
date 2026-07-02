//! Corpus-driven feature learner.
//!
//! Question: can we build the "tokenizer"/feature extractor FROM the supplied
//! queries, with zero hand-coded vocabulary — so we never have to enumerate
//! that "jo kep" is a player?
//!
//! Answer demonstrated here: yes for the parts that matter for candidate
//! selectivity. We take raw query text (no Vocab, no field taxonomy), and:
//!   1. tokenize naively (whitespace/punct),
//!   2. count unigrams and adjacent bigrams,
//!   3. induce multi-token ENTITIES via NPMI collocation mining
//!      (the word2vec/Mikolov phrase trick), iterating bigram -> trigram,
//!   4. measure the SELECTIVITY GAIN: a learned phrase's document-frequency
//!      vs its parts' — a rarer anchor = a smaller candidate posting.
//!
//! Nothing here knows what a "player" or "brand" is. It only uses co-occurrence
//! statistics from the query corpus.
//!
//! Usage: learn [num_queries] [min_count] [npmi_tau]

// This exploratory CLI uses `HashMap<_, ()>` as insertion sets that sit next to
// the NPMI counting maps (`HashMap<_, usize>`), keeping the analysis passes
// visually parallel. It's a throwaway research tool, not library code.
#![allow(clippy::zero_sized_map_values)]

use reverse_rusty::corpus::{apply_phrases, learn_phrases, tokenize, Phrase};
use reverse_rusty::gen::{generate, GenConfig};
use std::collections::HashMap;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let num_queries = args.get(1).and_then(|x| x.parse().ok()).unwrap_or(500_000);
    let min_count = args.get(2).and_then(|x| x.parse().ok()).unwrap_or(50usize);
    let tau = args.get(3).and_then(|x| x.parse().ok()).unwrap_or(0.30f64);

    // Generate raw query text. We deliberately ignore the hand-built normalizer
    // and treat each query as an opaque string — the learner sees only text.
    let cfg = GenConfig {
        num_queries,
        num_titles: 1,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0xFEED,
        num_players: (num_queries / 40).max(2_000),
        num_sets: (num_queries / 100).max(1_000),
    };
    let data = generate(&cfg);

    // Tokenize every query into a token stream (DSL punctuation stripped, so
    // "-(a,b)" contributes tokens a,b just like the raw words do).
    let corpus: Vec<Vec<String>> = data.queries.iter().map(|(_, q)| tokenize(q)).collect();

    println!(
        "corpus: {} queries, learning with no hand-coded vocabulary",
        corpus.len()
    );
    println!("params: min_count={min_count}, npmi_tau={tau}\n");

    // ---- ITERATION 1: bigrams -> phrases ----
    let phrases1 = learn_phrases(&corpus, min_count, tau);
    println!(
        "=== iteration 1: discovered {} multi-token entities (bigrams) ===",
        phrases1.len()
    );
    print_top(&phrases1, 18);

    // surface the NAME-LIKE entities (all-alphabetic parts) — these are the
    // "players"/"brands" the learner found without ever being told they exist.
    let mut name_like: Vec<&Phrase> = phrases1
        .iter()
        .filter(|p| {
            p.token
                .split('_')
                .all(|t| t.chars().all(|c| c.is_ascii_alphabetic()))
        })
        .collect();
    // sort by NPMI: tightly-bound pairs (players/brands appear ONLY together)
    // float to the top, above loosely co-occurring word pairs.
    name_like.sort_by(|a, b| b.npmi.partial_cmp(&a.npmi).unwrap());
    println!(
        "\n--- of which {} are name-like (all-alphabetic), top by binding strength (npmi): ---",
        name_like.len()
    );
    println!("{:<30} {:>9} {:>8}", "entity", "count", "npmi");
    for p in name_like.iter().take(12) {
        println!("{:<30} {:>9} {:>8.3}", p.token, p.count, p.npmi);
    }

    // apply: rewrite corpus merging discovered bigrams, then learn again to get trigrams
    let merged = apply_phrases(&corpus, &phrases1);
    let phrases2 = learn_phrases(&merged, min_count, tau);
    let new_trigrams: Vec<_> = phrases2
        .iter()
        .filter(|p| p.token.contains('_') && p.token.matches('_').count() >= 2)
        .cloned()
        .collect();
    println!(
        "\n=== iteration 2: discovered {} longer entities (trigram+) ===",
        new_trigrams.len()
    );
    print_top(&new_trigrams, 10);

    // ---- SELECTIVITY GAIN ----
    // Document-frequency (how many queries contain the token at least once).
    let df_uni = doc_freq_unigrams(&corpus);
    println!("\n=== selectivity gain: learned phrase vs its parts (lower df = better anchor) ===");
    println!(
        "{:<28} {:>10} {:>14} {:>10}",
        "learned entity", "df(phrase)", "min df(part)", "gain x"
    );
    let mut shown = 0;
    for p in &phrases1 {
        let parts: Vec<&str> = p.token.split('_').collect();
        if parts.len() < 2 {
            continue;
        }
        let df_phrase = p.df;
        let min_part = parts
            .iter()
            .map(|t| *df_uni.get(*t).unwrap_or(&0))
            .min()
            .unwrap_or(0)
            .max(1);
        let gain = min_part as f64 / df_phrase.max(1) as f64;
        println!(
            "{:<28} {:>10} {:>14} {:>9.1}x",
            p.token, df_phrase, min_part, gain
        );
        shown += 1;
        if shown >= 14 {
            break;
        }
    }

    // ---- FEATURE UNIVERSE (derived purely from queries) ----
    let mut uni: HashMap<&str, ()> = HashMap::new();
    for q in &corpus {
        for t in q {
            uni.insert(t.as_str(), ());
        }
    }
    println!("\n=== learned feature universe (no hand-coded vocab) ===");
    println!("distinct unigrams in corpus : {}", uni.len());
    println!("entities learned (iter 1)   : {}", phrases1.len());
    println!("entities learned (iter 2)   : {}", new_trigrams.len());
    println!(
        "\nTakeaway: every anchor the matcher needs is derivable from query co-occurrence\n\
         statistics. The learner glues multi-token entities (raising selectivity) and the\n\
         signature optimizer already ranks anchors purely by frequency — no taxonomy required."
    );

    // ---- Distributional alias candidates (ADR-102) ----
    // The same corpus, asked a different question: which token pairs fill the SAME slot
    // (similar neighbor distributions) while rarely co-occurring — substitute candidates the
    // registry would file for review. Generated corpora have few true substitutes, so this
    // section mostly demonstrates the noise floor + the co-occurrence penalty at work.
    let dcfg = reverse_rusty::vocab::DistributionalConfig {
        min_token_freq: min_count,
        ..reverse_rusty::vocab::DistributionalConfig::default()
    };
    let pairs = reverse_rusty::vocab::discover_pairs(&data.queries, &dcfg);
    println!("\n=== distributional alias candidates (ADR-102, review-first) ===");
    println!(
        "{:<44} {:>7} {:>9}",
        "candidate pair (similarity desc)", "cosine", "cooc rate"
    );
    for p in pairs.iter().take(15) {
        println!(
            "{:<44} {:>7.3} {:>9.4}",
            format!("{} ≡ {}", p.forms[0], p.forms[1]),
            p.similarity,
            p.cooccurrence_rate
        );
    }
    if pairs.is_empty() {
        println!("(none above the similarity threshold — expected on a synthetic corpus)");
    }
}

fn doc_freq_unigrams(corpus: &[Vec<String>]) -> HashMap<String, usize> {
    let mut df: HashMap<String, usize> = HashMap::new();
    for q in corpus {
        let mut seen: HashMap<&str, ()> = HashMap::new();
        for t in q {
            seen.entry(t.as_str()).or_insert(());
        }
        for k in seen.keys() {
            *df.entry(k.to_string()).or_insert(0) += 1;
        }
    }
    df
}

fn print_top(phrases: &[Phrase], k: usize) {
    println!(
        "{:<30} {:>9} {:>8}",
        "entity (by frequency)", "count", "npmi"
    );
    for p in phrases.iter().take(k) {
        println!("{:<30} {:>9} {:>8.3}", p.token, p.count, p.npmi);
    }
}
