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
/// loading a synonym table makes a query phrased with one form match a title bearing another —
/// including a multi-token form (`upper deck`) glued by a phrase — while NEVER dropping a prior
/// match (the match set only grows; FN-safe).
#[test]
fn synonym_file_expansion_is_fn_safe_and_recall_grows() {
    // Distinct features under the empty default vocab. Extra queries intern every token.
    let mut queries: Vec<(u64, String)> = vec![
        (1, "auto".into()),      // requires `auto`
        (2, "ud rookie".into()), // requires `ud` + `rookie`
    ];
    for i in 0..20u64 {
        for tok in ["auto", "signature", "autograph", "ud", "rookie"] {
            queries.push((100 + i * 10 + tok.len() as u64, format!("{tok} u{i}")));
        }
        queries.push((900 + i, format!("upper deck u{i}")));
    }
    let sig_title = "2003 signature card psa 10"; // has `signature`, not `auto`
    let ud_title = "1994 upper deck rookie"; // has `upper deck` + `rookie`, not the token `ud`

    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&queries);

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title(sig_title, &mut s, &mut out, true);
    let before_sig: HashSet<u64> = out.iter().copied().collect();
    eng.match_title(ud_title, &mut s, &mut out, true);
    let before_ud: HashSet<u64> = out.iter().copied().collect();
    assert!(
        !before_sig.contains(&1),
        "before synonyms, `auto` must not match a signature-only title"
    );
    assert!(
        !before_ud.contains(&2),
        "before synonyms, `ud rookie` must not match an `upper deck` title"
    );

    // Load a Solr-format synonym table at runtime (the endpoint's path).
    let mut v = Vocab::new();
    let stats = v
        .extend_from_synonyms("auto, autograph, autographed, signature, signed\nud, upper deck")
        .expect("synonym table parses");
    assert_eq!(stats.groups, 2);
    assert_eq!(stats.phrases, 1, "`upper deck` glued to one feature");
    eng.set_vocab(v).expect("set_vocab");
    eng.recompile_stale_segments();

    eng.match_title(sig_title, &mut s, &mut out, true);
    let after_sig: HashSet<u64> = out.iter().copied().collect();
    eng.match_title(ud_title, &mut s, &mut out, true);
    let after_ud: HashSet<u64> = out.iter().copied().collect();

    assert!(
        after_sig.contains(&1),
        "after auto≡signature, the `auto` query matches a signature title (recall grew)"
    );
    assert!(
        after_ud.contains(&2),
        "after ud≡`upper deck`, the `ud rookie` query matches an `upper deck rookie` title"
    );
    assert!(
        before_sig.is_subset(&after_sig) && before_ud.is_subset(&after_ud),
        "loading synonyms must never drop a prior match (FN-safe / monotone)"
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
