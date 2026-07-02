//! Unit tests for the distributional alias discoverer (ADR-102): the planted-substitute /
//! co-hyponym separation, the noise filters, determinism, and the degenerate guards.
//!
//! Corpus-construction note: PPMI needs *informative* contexts — a perfectly uniform corpus has
//! PMI == 0 everywhere (co-occurrence exactly matches independence) and proposes nothing. Every
//! builder therefore adds disjoint filler queries so the family contexts are rarer than chance.

use super::*;

/// `a` and `b` fill the same slot across a family-private context vocabulary and never
/// co-occur — the paradigmatic (true-substitute) shape. `ctx` namespaces the context tokens so
/// two families are NOT distributionally similar to each other.
fn substitute_family(
    out: &mut Vec<(u64, String)>,
    id: &mut u64,
    a: &str,
    b: &str,
    ctx: &str,
    n: usize,
) {
    for i in 0..n {
        let c1 = format!("{ctx}p{}", i % 7);
        let c2 = format!("{ctx}b{}", i % 5);
        out.push((*id, format!("{a} {c1} {c2}")));
        *id += 1;
        out.push((*id, format!("{b} {c1} {c2}")));
        *id += 1;
    }
}

/// Disjoint filler queries — dilute the corpus so family contexts carry positive PMI.
fn filler(out: &mut Vec<(u64, String)>, id: &mut u64, n: usize) {
    for i in 0..n {
        out.push((*id, format!("filler{i} junk{i} noise{i}")));
        *id += 1;
    }
}

fn cfg() -> DistributionalConfig {
    DistributionalConfig {
        min_token_freq: 5,
        min_similarity: 0.5,
        glue_phrases: false,
        ..DistributionalConfig::default()
    }
}

fn has_pair(pairs: &[DiscoveredPair], a: &str, b: &str) -> bool {
    let want = {
        let mut v = vec![a.to_string(), b.to_string()];
        v.sort();
        v
    };
    pairs.iter().any(|p| {
        let mut got = p.forms.clone();
        got.sort();
        got == want
    })
}

#[test]
fn planted_alias_pair_is_discovered() {
    let mut queries = Vec::new();
    let mut id = 1u64;
    substitute_family(&mut queries, &mut id, "zzud", "zzupperdeck", "ua", 40);
    filler(&mut queries, &mut id, 200);
    let pairs = discover_pairs(&queries, &cfg());
    assert!(
        has_pair(&pairs, "zzud", "zzupperdeck"),
        "identical-context, never-co-occurring pair must be discovered; got {pairs:?}"
    );
    let p = pairs
        .iter()
        .find(|p| p.forms.contains(&"zzud".to_string()))
        .unwrap();
    assert!(p.similarity >= 0.5 && p.similarity.is_finite());
    assert!(
        p.cooccurrence_rate == 0.0,
        "substitutes never co-occur here"
    );
}

#[test]
fn co_hyponym_pair_suppressed_by_cooccurrence() {
    // Same shared contexts (high similarity), but the two forms ALSO appear together (the
    // syntagmatic co-hyponym shape — an any-of alternative). The similarity signal alone
    // cannot tell these apart; the co-occurrence penalty must.
    let mut queries = Vec::new();
    let mut id = 1u64;
    substitute_family(&mut queries, &mut id, "zzpsa", "zzbgs", "gr", 40);
    for i in 0..40 {
        queries.push((id, format!("(zzpsa,zzbgs) grp{}", i % 7)));
        id += 1;
    }
    filler(&mut queries, &mut id, 200);
    let pairs = discover_pairs(&queries, &cfg());
    assert!(
        !has_pair(&pairs, "zzpsa", "zzbgs"),
        "a frequently co-occurring pair must be dropped by the co-occurrence penalty; got {pairs:?}"
    );
}

#[test]
fn numeric_tokens_excluded_by_default_and_opt_in() {
    // Two years in identical contexts — textbook co-hyponyms the default keeps out.
    let mut queries = Vec::new();
    let mut id = 1u64;
    substitute_family(&mut queries, &mut id, "1994", "1995", "yr", 40);
    filler(&mut queries, &mut id, 200);
    let pairs = discover_pairs(&queries, &cfg());
    assert!(
        !has_pair(&pairs, "1994", "1995"),
        "numeric-only tokens are excluded by default"
    );
    let pairs = discover_pairs(
        &queries,
        &DistributionalConfig {
            include_numeric: true,
            ..cfg()
        },
    );
    assert!(
        has_pair(&pairs, "1994", "1995"),
        "include_numeric opts the same pair back in (proving exclusion was the filter)"
    );
}

#[test]
fn negated_clauses_contribute_no_context() {
    // The two forms share contexts ONLY through negated clauses — no positive evidence.
    let mut queries = Vec::new();
    let mut id = 1u64;
    for i in 0..40 {
        queries.push((id, format!("anchor{} -zzud -negp{}", i % 3, i % 7)));
        id += 1;
        queries.push((id, format!("anchor{} -zzupperdeck -negp{}", i % 3, i % 7)));
        id += 1;
    }
    filler(&mut queries, &mut id, 200);
    let pairs = discover_pairs(&queries, &cfg());
    assert!(
        !has_pair(&pairs, "zzud", "zzupperdeck"),
        "forbidden terms are not semantic context; got {pairs:?}"
    );
}

#[test]
fn phrase_glue_discovers_token_vs_multiword_pair() {
    // `zzupper zzdeck` is a 40-support adjacent bigram; with glue on it becomes one unit whose
    // contexts mirror `zzud`'s — the canonical abbreviation case, emitted space-joined.
    // npmi_min_count is raised above the incidental `zzud <ctx>` bigram support so only the
    // planted phrase glues.
    let mut queries = Vec::new();
    let mut id = 1u64;
    for i in 0..40 {
        let c1 = format!("uap{}", i % 7);
        let c2 = format!("uab{}", i % 5);
        queries.push((id, format!("zzud {c1} {c2}")));
        id += 1;
        queries.push((id, format!("zzupper zzdeck {c1} {c2}")));
        id += 1;
    }
    filler(&mut queries, &mut id, 200);
    let pairs = discover_pairs(
        &queries,
        &DistributionalConfig {
            glue_phrases: true,
            npmi_min_count: 20,
            ..cfg()
        },
    );
    assert!(
        has_pair(&pairs, "zzud", "zzupper zzdeck"),
        "the glued multi-word unit must surface space-joined; got {pairs:?}"
    );
}

#[test]
fn output_is_deterministic_and_capped() {
    // Two independent substitute families (namespaced contexts, so no cross-family
    // similarity); two runs must be byte-identical and max_pairs must cap best-first.
    let mut queries = Vec::new();
    let mut id = 1u64;
    substitute_family(&mut queries, &mut id, "zzaa", "zzbb", "fa", 30);
    substitute_family(&mut queries, &mut id, "zzcc", "zzdd", "fb", 30);
    filler(&mut queries, &mut id, 200);

    let a = discover_pairs(&queries, &cfg());
    let b = discover_pairs(&queries, &cfg());
    assert_eq!(a, b, "two identical runs must produce identical output");
    assert!(
        has_pair(&a, "zzaa", "zzbb") && has_pair(&a, "zzcc", "zzdd"),
        "premise: both planted pairs discovered; got {a:?}"
    );
    for w in a.windows(2) {
        let ord = w[0]
            .similarity
            .partial_cmp(&w[1].similarity)
            .expect("finite similarities");
        assert!(
            ord != std::cmp::Ordering::Less,
            "similarity must be non-increasing"
        );
        if ord == std::cmp::Ordering::Equal {
            assert!(w[0].forms <= w[1].forms, "ties must break forms-ascending");
        }
    }
    let capped = discover_pairs(
        &queries,
        &DistributionalConfig {
            max_pairs: 1,
            ..cfg()
        },
    );
    assert_eq!(capped.len(), 1);
    assert_eq!(capped[0], a[0], "the cap keeps the best pair");
}

#[test]
fn uniform_corpus_and_empty_corpus_propose_nothing() {
    // Degenerate guards: an empty corpus, and a perfectly uniform one (PMI == 0 everywhere —
    // co-occurrence exactly matches independence, so there is no distributional signal and the
    // PPMI vectors are empty; the zero-norm guard holds).
    assert!(discover_pairs(&[], &cfg()).is_empty());
    let mut queries = Vec::new();
    let mut id = 1u64;
    substitute_family(&mut queries, &mut id, "zzuu", "zzvv", "un", 40); // NO filler ⇒ uniform
    assert!(
        discover_pairs(&queries, &cfg()).is_empty(),
        "a corpus with no positive-PMI structure proposes nothing"
    );
}
