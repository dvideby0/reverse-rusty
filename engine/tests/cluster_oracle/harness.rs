//! Shared harness for the multi-shard differential oracle: cluster builders across K,
//! the single-node + brute-force reference matchers, the per-query tag helpers, and the
//! corpus/coverage-query generators. Submodules reach these via `use crate::harness::*;`.

use reverse_rusty::compile::{extract, Extracted};
use reverse_rusty::dict::Dict;
use reverse_rusty::gen::{generate, GenConfig, BRANDS};
use reverse_rusty::normalize::Normalizer;
use std::collections::HashSet;

pub(crate) fn vocab() -> Normalizer {
    Normalizer::default_vocab().expect("built-in vocab")
}

/// Independent ground-truth matcher over extracted queries (copied structure from
/// `tests/oracle.rs` — deliberately shares nothing with the engine or cluster).
pub(crate) struct Brute {
    norm: Normalizer,
    dict: Dict,
    queries: Vec<(u64, Extracted)>,
}

impl Brute {
    pub(crate) fn build(queries: &[(u64, String)]) -> Self {
        Self::build_with_vocab(queries, vocab())
    }

    /// Build the oracle with an EXPLICIT normalizer (e.g. one carrying a declared
    /// alias), so it stays an independent ground truth that applies the SAME
    /// alias the cluster was given — its own dict, its own code path, never the
    /// engine's normalizer instance.
    pub(crate) fn build_with_vocab(queries: &[(u64, String)], norm: Normalizer) -> Self {
        let mut dict = Dict::new();
        let mut lc = String::new();
        let mut qs = Vec::new();
        for (logical, text) in queries {
            if let Ok(ast) = reverse_rusty::dsl::parse(text) {
                let ex = extract(&ast, &norm, &mut dict, &mut lc);
                if ex.required.is_empty() && ex.anyof.is_empty() {
                    continue; // mirror class-D rejection
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

    /// Like `build` but KEEPS negation-only (class-D) queries as always-candidates —
    /// the ground truth for the cluster always-candidate lane (ADR-068/080). A class-D
    /// query matches exactly the titles bearing none of its forbidden features (the
    /// `matches` loop already computes this: empty required/any-of pass vacuously, the
    /// forbidden check gates). Only the effectively-empty query (no positives AND no
    /// negatives — a match-all the engine also rejects) is dropped.
    pub(crate) fn build_accepting_class_d(queries: &[(u64, String)]) -> Self {
        let norm = vocab();
        let mut dict = Dict::new();
        let mut lc = String::new();
        let mut qs = Vec::new();
        for (logical, text) in queries {
            if let Ok(ast) = reverse_rusty::dsl::parse(text) {
                let ex = extract(&ast, &norm, &mut dict, &mut lc);
                if ex.required.is_empty() && ex.anyof.is_empty() && ex.forbidden.is_empty() {
                    continue; // an effectively-empty query (match-all) — rejected on both sides
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

    /// Like `build_with_vocab` but additionally applies the vocab's equivalence groups
    /// (ADR-054) via expansion — an independent ground truth that widens each query exactly
    /// as the cluster does, so `cluster ≡ brute` proves the expanded queries match correctly.
    pub(crate) fn build_with_equiv(
        queries: &[(u64, String)],
        norm: Normalizer,
        vocab: &reverse_rusty::vocab::Vocab,
    ) -> Self {
        let mut dict = Dict::new();
        let mut lc = String::new();
        let mut qs = Vec::new();
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
        for (_, ex) in &mut qs {
            ex.expand_equivalences(&equiv);
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

// ---- per-query tags + filtered percolation (ADR-049/055) ----
// Mirrors the single-node `tests/oracle.rs` scheme so the cluster path is held to the same
// deterministic-tags + AND-across-keys/OR-within-a-key filter semantics.
const CATEGORIES: [&str; 6] = ["cards", "coins", "stamps", "comics", "toys", "art"];
const STATUSES: [&str; 3] = ["active", "inactive", "archived"];

/// Deterministic per-query tags, a pure function of the logical id — so the cluster, the single-node
/// reference, and the brute oracle assign identical metadata with no shared state.
pub(crate) fn tags_for(logical: u64) -> Vec<(String, String)> {
    let cat = CATEGORIES[(logical % CATEGORIES.len() as u64) as usize];
    let status = STATUSES[((logical / 7) % STATUSES.len() as u64) as usize];
    vec![
        ("category".to_string(), cat.to_string()),
        ("status".to_string(), status.to_string()),
    ]
}

/// `tags_for` over every query in submission order — the `tags` slice `build_with_tags` expects.
pub(crate) fn tags_parallel(queries: &[(u64, String)]) -> Vec<Vec<(String, String)>> {
    queries.iter().map(|(l, _)| tags_for(*l)).collect()
}

/// Reference filter semantics: AND across keys, OR within a key's value set.
pub(crate) fn passes_filter(qtags: &[(String, String)], filter: &[(String, Vec<String>)]) -> bool {
    filter.iter().all(|(k, vals)| {
        qtags
            .iter()
            .any(|(qk, qv)| qk == k && vals.iter().any(|v| v == qv))
    })
}

/// A deterministic sweep of filters keyed off `i`: a single category (the dominant production
/// pattern), a two-value category set, category+status, and a never-ingested value (must return ∅).
pub(crate) fn filters_for(i: usize) -> Vec<Vec<(String, Vec<String>)>> {
    let c1 = CATEGORIES[i % CATEGORIES.len()].to_string();
    let c2 = CATEGORIES[(i + 1) % CATEGORIES.len()].to_string();
    let st = STATUSES[i % STATUSES.len()].to_string();
    vec![
        vec![("category".to_string(), vec![c1.clone()])],
        vec![("category".to_string(), vec![c1.clone(), c2])],
        vec![
            ("category".to_string(), vec![c1]),
            ("status".to_string(), vec![st]),
        ],
        vec![("category".to_string(), vec!["never-ingested".to_string()])],
    ]
}

/// Build the test corpus: a generated base (class A + C) plus injected coverage.
/// Returns `(queries, titles)`. Injected logical ids start above the generated
/// range so nothing collides.
pub(crate) fn build_corpus() -> (Vec<(u64, String)>, Vec<String>) {
    let cfg = GenConfig {
        num_queries: 12_000,
        num_titles: 1_200,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x0CEA_5ADE,
        num_players: 2_000,
        num_sets: 800,
    };
    let data = generate(&cfg);
    let mut queries = data.queries;
    let mut titles = data.titles;
    let mut next_id = queries.iter().map(|(id, _)| *id).max().unwrap_or(0) + 1;

    // class-B any-of: pure any-of of two RARE players (no required term, so the
    // any-of cover path fires). "rareplayerN" appears only here -> non-hot.
    for i in 0..150u64 {
        queries.push((next_id, format!("(rareplayer{i},rareplayer{})", i + 1000)));
        next_id += 1;
    }
    // class-B arity-2: all-hot required (year + brand), no rare anchor -> the
    // replicated lane.
    for i in 0..100u64 {
        let year = 1986 + (i % 39);
        let brand = BRANDS[(i % BRANDS.len() as u64) as usize];
        queries.push((next_id, format!("{year} {brand}")));
        next_id += 1;
    }
    // a few class-A queries anchored on the injected rare players, so multi-entity
    // titles below actually match something across shards.
    for i in 0..150u64 {
        let year = 1986 + (i % 39);
        let brand = BRANDS[(i % BRANDS.len() as u64) as usize];
        queries.push((next_id, format!("{year} {brand} rareplayer{i}")));
        next_id += 1;
    }

    // multi-entity titles: two rare players (both in the dict via the any-of
    // queries) -> fan out to two selective shards plus the replicated lane.
    for i in 0..200u64 {
        let year = 1986 + (i % 39);
        let brand = BRANDS[(i % BRANDS.len() as u64) as usize];
        let a = i % 150;
        titles.push(format!(
            "{year} {brand} rareplayer{a} rareplayer{} psa 10",
            a + 1000
        ));
    }

    (queries, titles)
}
