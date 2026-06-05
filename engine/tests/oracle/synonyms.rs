//! Bulk synonym / alias file loading (ADR-060) differential oracle.
//!
//! The loader parses a Solr/Lucene-format synonym table into FN-safe equivalence groups
//! (expansion, ADR-054). These tests prove (1) the runtime apply path (`set_vocab` — what the
//! `POST /_vocab/synonyms` endpoint drives) grows recall without ever dropping a prior match, and
//! (2) the build-time path produces a match set byte-identical to an independent equivalence-aware
//! brute oracle (zero false negatives AND zero false positives) over a large synthetic corpus.

use crate::harness::*;
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::vocab::{parse_synonyms, Vocab};
use reverse_rusty::EngineConfig;
use std::collections::HashSet;

/// The runtime path (`POST /_vocab/synonyms` -> `set_vocab` + `recompile_stale_segments`):
/// loading a multi-word alias (`ud ≡ upper deck`) makes it **bidirectional** — the ES
/// `synonym_graph` win (ADR-061) — while preserving component-token matches and never dropping a
/// prior match on the single-token / adjacent paths.
#[test]
fn synonym_file_alias_is_bidirectional_and_recall_safe() {
    // Distinct features under the empty default vocab. Extra queries intern every token.
    let mut queries: Vec<(u64, String)> = vec![
        (1, "ud rookie".into()), // single-token alias form (forward direction)
        (2, "upper deck rookie".into()), // multi-token alias form (reverse direction)
        (3, "deck".into()),      // bare COMPONENT of the multi-word alias `upper deck`
        (4, "auto".into()),      // single-token alias set member
    ];
    for i in 0..20u64 {
        for tok in ["ud", "upper deck", "rookie", "auto", "signature"] {
            queries.push((100 + i * 10 + tok.len() as u64, format!("{tok} u{i}")));
        }
    }
    let updeck_title = "1994 upper deck rookie psa 10"; // `upper deck` (adjacent) + rookie, no `ud`
    let ud_title = "1994 ud rookie psa 10"; // the token `ud` + rookie, no `upper deck`
    let sig_title = "2003 signature card"; // `signature`, no `auto`

    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&queries);

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let snap = |eng: &mut Engine, s: &mut MatchScratch, out: &mut Vec<u64>, t: &str| {
        eng.match_title(t, s, out, true);
        out.iter().copied().collect::<HashSet<u64>>()
    };

    let before_updeck = snap(&mut eng, &mut s, &mut out, updeck_title);
    let before_ud = snap(&mut eng, &mut s, &mut out, ud_title);
    assert!(
        !before_updeck.contains(&1),
        "before: ud-query must not match an upper-deck title"
    );
    assert!(
        !before_ud.contains(&2),
        "before: upper-deck-query must not match a ud title"
    );
    assert!(
        before_updeck.contains(&3),
        "before: bare `deck` matches the upper-deck title"
    );

    // Load the alias table at runtime (the endpoint's path).
    let mut v = Vocab::new();
    let stats = v
        .extend_from_synonyms("ud, upper deck\nauto, signature")
        .expect("synonym table parses");
    assert_eq!(stats.groups, 2);
    assert_eq!(
        stats.phrases, 1,
        "`upper deck` glued by one alias-entity phrase"
    );
    eng.set_vocab(v).expect("set_vocab");
    eng.recompile_stale_segments();

    let after_updeck = snap(&mut eng, &mut s, &mut out, updeck_title);
    let after_ud = snap(&mut eng, &mut s, &mut out, ud_title);
    let after_sig = snap(&mut eng, &mut s, &mut out, sig_title);

    // Forward: a `ud` query matches an `upper deck` title.
    assert!(
        after_updeck.contains(&1),
        "forward: ud-query matches the upper-deck title"
    );
    // Reverse (the synonym_graph win): an `upper deck` query matches a `ud` title.
    assert!(
        after_ud.contains(&2),
        "reverse: upper-deck-query matches the ud title (bidirectional)"
    );
    // Component preserved: the bare `deck` query still matches the upper-deck title (no FN).
    assert!(
        after_updeck.contains(&3),
        "component `deck` still matches the upper-deck title"
    );
    // Single-token alias also bidirectional.
    assert!(
        after_sig.contains(&4),
        "auto-query matches a signature title"
    );
    // Monotonic on the adjacent / single-token paths: no prior match dropped.
    assert!(before_updeck.is_subset(&after_updeck) && before_ud.is_subset(&after_ud));
}

/// Adjacency is the ES `synonym_graph` behavior: a query phrased with the multi-word alias form
/// becomes phrasal, so it does NOT match a title where the components are non-adjacent (just as ES's
/// graph query for `upper deck` matches the phrase or `ud`, not loose `upper` + `deck`). This is the
/// documented trade-off of multi-word aliases, asserted so it is intentional, not accidental.
#[test]
fn multiword_alias_query_is_phrasal_like_elasticsearch() {
    let mut queries: Vec<(u64, String)> = vec![(1, "upper deck".into())];
    for i in 0..10u64 {
        queries.push((100 + i, format!("ud u{i}")));
        queries.push((200 + i, format!("upper deck u{i}")));
    }
    let mut v = Vocab::new();
    v.extend_from_synonyms("ud, upper deck").expect("parse");
    let mut eng = Engine::with_vocab(v, EngineConfig::default()).expect("with_vocab");
    eng.build_from_queries(&queries);

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    // Adjacent → matches (the phrase path).
    eng.match_title("a upper deck b", &mut s, &mut out, true);
    assert!(
        out.contains(&1),
        "adjacent `upper deck` matches the alias query"
    );
    // The `ud` synonym → matches (the entity path).
    eng.match_title("a ud b", &mut s, &mut out, true);
    assert!(
        out.contains(&1),
        "the `ud` synonym matches the `upper deck` alias query"
    );
    // Non-adjacent components → does NOT match (phrasal, like ES synonym_graph).
    eng.match_title("upper big deck", &mut s, &mut out, true);
    assert!(
        !out.contains(&1),
        "non-adjacent `upper ... deck` must not match (ES-equivalent phrasal)"
    );
}

/// The build-time path against an independent **equivalence-aware** brute oracle: build the engine
/// `with_vocab(parsed)` and a `Brute::build_with_vocab` under the SAME vocab-normalizer, then assert
/// the match sets are identical over a large synthetic corpus — zero false negatives AND zero false
/// positives under a loaded synonym table.
#[test]
fn synonym_file_engine_equals_equivalence_aware_brute() {
    let cfg = GenConfig {
        num_queries: 12_000,
        num_titles: 3_000,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x5A11_AD60,
        num_players: 800,
        num_sets: 400,
    };
    let data = generate(&cfg);

    // Augment with alias-using queries + titles so the loaded equivalences resolve to real
    // interned features and actually fire (otherwise the differential is vacuous).
    let mut queries = data.queries.clone();
    let mut titles = data.titles.clone();
    for i in 0..40u64 {
        queries.push((8_000_000 + i, format!("auto u{i}")));
        queries.push((8_100_000 + i, format!("signature u{i}")));
        queries.push((8_200_000 + i, format!("ud u{i}")));
        queries.push((8_300_000 + i, format!("upper deck u{i}")));
        titles.push(format!("signature u{i} psa 10"));
        titles.push(format!("upper deck u{i} bgs 9.5"));
        titles.push(format!("autograph u{i}"));
    }

    // A loaded Solr-format synonym table: a single-token equivalence set + a multi-token
    // (phrase-glued) mapping. Parsing yields the vocab the engine and brute both build from.
    let vocab = parse_synonyms(
        "# loaded aliases\n\
         auto, autograph, autographed, signature, signed\n\
         ud, upperdeck => upper deck\n",
    )
    .expect("synonym table parses");
    assert!(!vocab.equivalences().is_empty());

    let norm = vocab.to_normalizer().expect("vocab normalizer");
    let mut eng = Engine::with_vocab(vocab.clone(), EngineConfig::default()).expect("with_vocab");
    eng.build_from_queries(&queries);

    let brute = Brute::build_with_vocab(&queries, norm, &vocab);

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();

    let mut total_truth = 0usize;
    let mut false_neg = 0usize;
    let mut false_pos = 0usize;
    let mut alias_hits = 0usize;

    for title in &titles {
        eng.match_title(title, &mut s, &mut out, true);
        let engine_set: HashSet<u64> = out.iter().copied().collect();
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        total_truth += truth.len();
        for t in &truth {
            if !engine_set.contains(t) {
                false_neg += 1;
            }
        }
        for e in &engine_set {
            if !truth.contains(e) {
                false_pos += 1;
            }
            // An `auto`-form query (8_000_000+) matching a `signature`/`autograph` title is an
            // equivalence-induced hit — evidence the loaded table actually fired.
            if (8_000_000..8_000_040).contains(e)
                && (title.contains("signature") || title.contains("autograph"))
            {
                alias_hits += 1;
            }
        }
    }

    eprintln!(
        "synonym-file oracle: truth={total_truth} fn={false_neg} fp={false_pos} alias_hits={alias_hits}"
    );
    assert_eq!(
        false_neg, 0,
        "FALSE NEGATIVES under a loaded synonym table — contract violated"
    );
    assert_eq!(
        false_pos, 0,
        "false positives under a loaded synonym table — engine != equivalence-aware brute"
    );
    assert!(total_truth > 0, "degenerate: no matches");
    assert!(
        alias_hits > 0,
        "degenerate: the loaded equivalences never fired (differential would be vacuous)"
    );
}
