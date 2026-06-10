//! Degenerate-input differential: queries and titles at the edges of the grammar and
//! the feature model — pure punctuation, empty-ish strings, marker soup, any-of members
//! that normalize to nothing, self-contradictory queries. The engine and the brute
//! reference must agree EXACTLY (which queries are indexable at all, and what matches),
//! and nothing may panic. The clean generator can produce none of these shapes.

use crate::harness::*;
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};
use std::collections::HashSet;

/// Degenerate / edge queries. Some parse-fail (unclosed quote, lone dash), some are
/// class-D rejected (no positive semantics), some normalize members away entirely —
/// the engine and the brute must make the SAME call on every one.
fn degenerate_queries() -> Vec<(u64, String)> {
    let strs: Vec<String> = vec![
        "!!!".into(),                      // positive term that cleans to nothing
        "...".into(),                      // Keep-class dots survive as a token
        "(a)".into(),                      // singleton any-of → plain required
        "(aa,aa)".into(),                  // duplicate members → singleton
        "(!!,??)".into(),                  // every member cleans to nothing → group vanishes
        "(!!,rookie)".into(),              // one member survives → singleton required
        "-(rookie)".into(),                // negated group only → class D
        "-auto -lot".into(),               // pure negative → class D
        "psa -psa".into(),                 // require + forbid the same feature
        "(rc,rookie) -(rc,rookie)".into(), // require + forbid the same group
        "#".into(),                        // lone marker
        "/".into(),                        // lone marker
        "# 1985".into(),                   // marker + number: card-number, not year
        "pop 3".into(),                    // pop-context number
        "10".into(),                       // bare number
        "9.5.5".into(),                    // not a number, survives as a dotted token
        "a".into(),                        // single letter
        "™".into(),                        // cleans to nothing
        "ñ".into(),                        // folds to a letter
        "\"\"".into(),                     // empty phrase
        "\" \"".into(),                    // whitespace phrase
        "-\"gem mt\"".into(),              // pure negative phrase → class D
        "()".into(),                       // parse error: empty group
        "(,)".into(),                      // parse error: no members
        "rookie -".into(),                 // parse error: trailing dash
        "\"unclosed".into(),               // parse error: unclosed quote
        "card (rc,rookie".into(),          // parse error: unclosed group
    ];
    let mut qs: Vec<(u64, String)> = strs
        .into_iter()
        .enumerate()
        .map(|(i, q)| (i as u64 + 1, q))
        .collect();
    // A few normal queries so the corpus has real matches to protect.
    qs.push((1001, "1994 fleer rookie".into()));
    qs.push((1002, "psa rookie -auto".into()));
    qs.push((1003, "(rc,rookie) card".into()));
    qs
}

fn degenerate_titles() -> Vec<String> {
    vec![
        String::new(),
        " ".into(),
        "\t\t".into(),
        "!!!".into(),
        "...".into(),
        "™🔥カード".into(),
        "#".into(),
        "# 1985 / 99 pop 3".into(),
        "a".into(),
        "ñ".into(),
        "10".into(),
        "9.5.5".into(),
        "rookie".into(),
        "rookie card".into(),
        "rc card".into(),
        "psa rookie".into(),
        "psa rookie auto".into(),
        "1994 fleer rookie psa 10".into(),
        "psa".into(),
        "gem mt".into(),
        "  rookie   card  ".into(),
        "a".repeat(5_000),
    ]
}

/// Engine == brute on every degenerate title, with both the build path and the live
/// insert path carrying the degenerate queries (the two ingest paths must reject /
/// accept identically).
#[test]
fn degenerate_inputs_engine_equals_brute_and_never_panics() {
    let queries = degenerate_queries();
    let titles = degenerate_titles();

    // Build path: all at once.
    let mut built = Engine::new(Normalizer::default_vocab().expect("vocab"));
    built.build_from_queries(&queries);

    // Live path: one by one; rejections are typed errors, never panics.
    let mut live = Engine::new(Normalizer::default_vocab().expect("vocab"));
    live.build_from_queries(&[(9999, "seedqueryzz placeholder".to_string())]);
    for (id, q) in &queries {
        let _ = live.try_insert_live(q, *id, 1); // Err for unparseable/class-D is fine
    }

    let brute = Brute::build(&queries);

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let mut total_truth = 0usize;

    for title in &titles {
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        total_truth += truth.len();

        for (eng, label) in [(&built, "build"), (&live, "live")] {
            eng.match_title(title, &mut s, &mut out, true);
            let engine_set: HashSet<u64> = out.iter().copied().filter(|&id| id != 9999).collect();
            assert_eq!(
                engine_set, truth,
                "degenerate divergence ({label} path) on title {title:?}"
            );
        }
    }
    assert!(
        total_truth > 0,
        "degenerate corpus produced no matches at all — assertions are vacuous"
    );
}
