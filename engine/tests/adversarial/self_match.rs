//! Self-match: the zero-FN **diagonal**, reference-free.
//!
//! For any stored query, a title consisting of exactly the query's own positive terms
//! (in order) must match it: the engine compiles those very tokens into the query's
//! required features through the SAME normalizer that will process the title, so a miss
//! is — by construction — a query↔title normalization divergence or an engine pipeline
//! drop. No brute-force reference is involved, so a bug in shared front-end code
//! (parser/extractor/normalizer) cannot hide by corrupting "both sides".
//!
//! The historical escapes this pins: query-side whitespace-run handling (codex R11),
//! case/diacritic asymmetry, and any future drift between `compile_features` and
//! `match_features` on the same byte stream.

use crate::harness::*;
use reverse_rusty::gen::{generate, messify_query, GenConfig, Rng};
use reverse_rusty::segment::MatchScratch;

fn small_corpus(seed: u64) -> reverse_rusty::gen::Dataset {
    generate(&GenConfig {
        num_queries: 20_000,
        num_titles: 10, // titles unused; self-match builds its own
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed,
        num_players: 2_000,
        num_sets: 800,
    })
}

/// A query's positive title: its tokens minus the `-`-negated ones, joined in order.
/// Mirrors `compile::extract`, which joins consecutive positive bare words into one
/// normalization stream — so query-side and title-side contexts align token-for-token.
fn positive_title(query: &str) -> String {
    query
        .split_whitespace()
        .filter(|t| !t.starts_with('-'))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Every clean generated query matches the title built from its own positive terms.
#[test]
fn every_query_matches_its_own_positive_terms() {
    let data = small_corpus(0x5E1F_0001);
    let eng = engine_from(&data.queries);
    let mut s = MatchScratch::new();

    let mut checked = 0usize;
    for (id, q) in &data.queries {
        let title = positive_title(q);
        let out = matched(&eng, &mut s, &title);
        assert!(
            out.binary_search(id).is_ok(),
            "SELF-MATCH FN: query {id} `{q}` does not match its own positive title `{title}` \
             (matched {} others)",
            out.len()
        );
        checked += 1;
    }
    assert!(
        checked > 10_000,
        "degenerate corpus: only {checked} queries"
    );
}

/// MESSY queries (case noise, foldable diacritics, whitespace runs between clauses)
/// against their CLEAN positive titles. The query surface and the title surface now
/// differ — only the shared normalizer's folding makes them meet. A divergence between
/// the query-side and title-side pipelines on any of these surfaces fails here.
#[test]
fn messy_queries_match_their_clean_positive_terms() {
    let data = small_corpus(0x5E1F_0002);
    let mut rng = Rng::new(0x5E1F_0002);

    // Clean titles captured BEFORE messing the queries.
    let clean_titles: Vec<(u64, String)> = data
        .queries
        .iter()
        .map(|(id, q)| (*id, positive_title(q)))
        .collect();

    let messy_queries: Vec<(u64, String)> = data
        .queries
        .iter()
        .map(|(id, q)| (*id, messify_query(&mut rng, q)))
        .collect();
    let perturbed = messy_queries
        .iter()
        .zip(&data.queries)
        .filter(|((_, m), (_, c))| m != c)
        .count();
    assert!(
        perturbed * 2 > messy_queries.len(),
        "messify_query perturbed only {perturbed}/{} queries",
        messy_queries.len()
    );

    let eng = engine_from(&messy_queries);
    let mut s = MatchScratch::new();
    for ((id, clean_title), (_, mq)) in clean_titles.iter().zip(&messy_queries) {
        let out = matched(&eng, &mut s, clean_title);
        assert!(
            out.binary_search(id).is_ok(),
            "MESSY-QUERY FN: query {id} `{mq}` does not match its clean positive title \
             `{clean_title}`"
        );
    }
}

/// CLEAN queries against identity-perturbed positive titles (case, diacritics,
/// whitespace runs, Split punctuation, appended junk — each op alone, then all
/// composed). The title surface moves; the match may not.
#[test]
fn clean_queries_match_perturbed_positive_titles() {
    let data = small_corpus(0x5E1F_0003);
    let eng = engine_from(&data.queries);
    let mut s = MatchScratch::new();
    let mut rng = Rng::new(0x5E1F_0003);

    for (id, q) in &data.queries {
        let title = positive_title(q);
        let op = (*id as usize) % IDENTITY_OPS;
        let single = identity_perturb(&mut rng, &title, op);
        assert!(
            matched(&eng, &mut s, &single).binary_search(id).is_ok(),
            "PERTURBED-TITLE FN: query {id} `{q}` missed op#{op} title `{single}`"
        );
        let composed = identity_perturb_all(&mut rng, &title);
        assert!(
            matched(&eng, &mut s, &composed).binary_search(id).is_ok(),
            "PERTURBED-TITLE FN: query {id} `{q}` missed composed title `{composed}`"
        );
    }
}

/// Structured DSL self-match: any-of groups (every member choice), quoted phrases, and
/// negation interleaving — hand-built because the generator never emits these shapes.
#[test]
fn structured_queries_self_match_on_every_branch() {
    let queries: Vec<(u64, String)> = vec![
        (1, "1994 (upper,ud) jordan".into()),
        (2, "bowman (rookie,rc) -lot".into()),
        (3, "\"gem mt\" psa 10".into()),
        (4, "(upper deck,ud) refractor".into()),
        (5, "1986 fleer -auto -(signed,reprint) sticker".into()),
        (6, "topps (red,blue,green) parallel -damaged".into()),
    ];
    let eng = engine_from(&queries);
    let mut s = MatchScratch::new();

    // (query id, title that must match) — one title per any-of branch.
    let must_match: &[(u64, &str)] = &[
        (1, "1994 upper jordan"),
        (1, "1994 ud jordan"),
        (2, "bowman rookie"),
        (2, "bowman rc"),
        (3, "gem mt psa 10"),
        (4, "upper deck refractor"),
        (4, "ud refractor"),
        (5, "1986 fleer sticker"),
        (6, "topps red parallel"),
        (6, "topps blue parallel"),
        (6, "topps green parallel"),
    ];
    for (id, title) in must_match {
        assert!(
            matched(&eng, &mut s, title).binary_search(id).is_ok(),
            "STRUCTURED FN: query {id} must match `{title}`"
        );
    }

    // Negation sanity: the removed negative term really does block.
    let must_not: &[(u64, &str)] = &[
        (2, "bowman rookie lot"),
        (5, "1986 fleer sticker auto"),
        (5, "1986 fleer sticker reprint"),
        (6, "topps red parallel damaged"),
    ];
    for (id, title) in must_not {
        assert!(
            matched(&eng, &mut s, title).binary_search(id).is_err(),
            "STRUCTURED FP: query {id} must NOT match `{title}`"
        );
    }
}

/// The θ-on self-match diagonal (ADR-105): the hot tier is a COST quarantine,
/// never a visibility change — every query still matches the title built from
/// its own positive terms with the hot tier enforcing (same broad-on probe as
/// the θ-off diagonal). The reference-free safety net for the classification split.
#[test]
fn every_query_matches_its_own_positive_terms_with_hot_tier_on() {
    let data = small_corpus(0x5E1F_0407);
    let cfg = reverse_rusty::config::EngineConfig {
        hot_anchor_threshold: 64,
        ..Default::default()
    };
    let mut eng = reverse_rusty::segment::Engine::with_config(
        reverse_rusty::normalize::Normalizer::default_vocab().expect("vocab"),
        cfg,
    );
    eng.build_from_queries(&data.queries);
    assert!(
        eng.class_counts()[4] > 0,
        "degenerate: θ=64 produced no class H at this scale"
    );
    let mut s = MatchScratch::new();
    let mut checked = 0usize;
    for (id, q) in &data.queries {
        let title = positive_title(q);
        let out = matched(&eng, &mut s, &title);
        assert!(
            out.binary_search(id).is_ok(),
            "SELF-MATCH FN under θ: query {id} `{q}` does not match its own \
             positive title `{title}`"
        );
        checked += 1;
    }
    assert!(
        checked > 10_000,
        "degenerate corpus: only {checked} queries"
    );
}
