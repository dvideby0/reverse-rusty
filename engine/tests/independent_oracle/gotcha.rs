//! Hand-written spec gotcha table: `(query, [(title, expect_match)])` cases authored by hand from
//! the spec and asserted against BOTH the engine and the independent reference. A human-authored
//! expectation is the tiebreaker — if the reference disagrees it's a reference bug; if the engine
//! disagrees it's an engine bug; if they agree with each other but not the human, re-read the spec.
//!
//! These pin the exact boundaries most prone to engine-vs-spec drift: negation adjacency, the
//! `#`/`/` markers, `psa10` fusion (default vs grader vocab), grader aging, number-context, diacritic
//! fold, half-grades, any-of, number-typing boundaries, and class-D drops.
//!
//! NOTE on any-of: cases only ever use titles that contain a multi-token member COMPLETELY or not at
//! all. The engine represents a multi-token any-of member by its rarest-by-id proxy; the reference
//! uses the lexicographically-smallest feature on a frequency tie, so the two can differ on a title
//! bearing SOME-but-not-all of a member's tokens (ADR-087). Such titles never arise from real
//! surface forms; the gotchas avoid them deliberately.

use reverse_rusty::normalize::{Normalizer, NormalizerBuilder};
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty_ref_matcher::{RefMatcher, RefVocab};

fn def_norm() -> Normalizer {
    Normalizer::default_vocab().expect("default vocab")
}
fn def_vocab() -> RefVocab {
    RefVocab::default_vocab()
}

fn grader_norm() -> Normalizer {
    let mut b = NormalizerBuilder::new();
    b.add_grader("psa");
    b.add_grader("bgs");
    b.add_grader("sgc");
    b.add_grade_word("gem");
    b.add_grade_word("mint");
    b.build().expect("grader norm")
}
fn grader_vocab() -> RefVocab {
    RefVocab::default_vocab()
        .grader("psa")
        .grader("bgs")
        .grader("sgc")
        .grade_word("gem")
        .grade_word("mint")
}

/// Build a single-query engine + reference under the given vocab, and assert BOTH agree with the
/// hand-authored expectation for every `(title, expect_match)` case.
fn check(
    make_norm: impl Fn() -> Normalizer,
    make_vocab: impl Fn() -> RefVocab,
    query: &str,
    cases: &[(&str, bool)],
) {
    let queries = vec![(1u64, query.to_string())];
    let mut eng = Engine::new(make_norm());
    eng.build_from_queries(&queries);
    let reference = RefMatcher::build(&queries, make_vocab());

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    for &(title, expect) in cases {
        eng.match_title(title, &mut s, &mut out, true);
        let eng_match = out.contains(&1);
        let ref_match = reference.matches(title).contains(&1);
        assert_eq!(
            eng_match, expect,
            "ENGINE disagrees: query {query:?} vs title {title:?} (expected {expect})"
        );
        assert_eq!(
            ref_match, expect,
            "REFERENCE disagrees: query {query:?} vs title {title:?} (expected {expect})"
        );
    }
}

#[test]
fn negation_adjacency() {
    // `-bar` negates; the title with `bar` is rejected, without it accepted.
    check(
        def_norm,
        def_vocab,
        "foo -bar",
        &[("foo baz", true), ("foo bar", false)],
    );
    // `foo - bar` is a PARSE ERROR -> the query is dropped -> it matches nothing (NOT `foo AND bar`).
    check(
        def_norm,
        def_vocab,
        "foo - bar",
        &[("foo bar", false), ("foo", false), ("foo baz", false)],
    );
    // A trailing dash is the same parse error -> dropped.
    check(def_norm, def_vocab, "jordan -", &[("jordan", false)]);
}

#[test]
fn markers_card_number_and_serial() {
    // `#2` -> the `2` is a card-number (generic `term:2`), NOT a year/grade. A bare `2002` is a year,
    // so the `#2` query must NOT reach a `2002` title.
    check(
        def_norm,
        def_vocab,
        "mantle #2",
        &[
            ("mantle #2", true),
            ("mantle 2002", false),
            ("mantle", false),
        ],
    );
    // `/1999` is a serial (generic `term:1999`), distinct from the bare year `year:1999`.
    check(
        def_norm,
        def_vocab,
        "card /1999",
        &[("card /1999", true), ("card 1999", false)],
    );
    // A marker token never becomes a feature: a plain `card` query still matches a `card # 2` title.
    check(def_norm, def_vocab, "card", &[("card # 2", true)]);
}

#[test]
fn psa10_fusion_default_vs_grader_vocab() {
    // DEFAULT vocab: no graders, so `psa10` is one generic token and does NOT cross-match `psa 10`.
    check(
        def_norm,
        def_vocab,
        "psa10",
        &[("psa10", true), ("psa 10", false)],
    );
    // GRADER vocab: `psa10` fuses to grader:psa + grade:10 + grader_grade:psa10, and so does
    // `psa 10` (pending grader grades the next number) — so they DO cross-match.
    check(
        grader_norm,
        grader_vocab,
        "psa10",
        &[("psa10", true), ("psa 10", true)],
    );
}

#[test]
fn grader_aging_window() {
    // The pending grader survives <=3 intervening tokens, then ages out (`> 3`). `psa <=3 toks> 10`
    // grades (matches the `psa 10` query); `psa <4 toks> 10` does not (the `10` falls back to
    // `term:10`, so grade:10 / grader_grade:psa10 are absent).
    check(
        grader_norm,
        grader_vocab,
        "psa 10",
        &[
            ("psa a b c 10", true),    // 3 fillers: still in window
            ("psa a b c d 10", false), // 4 fillers: aged out -> generic 10
        ],
    );
}

#[test]
fn number_context_pop() {
    // A number immediately after `pop` is demoted to a generic term, never typed as a year. So a
    // `pop 1995` query (term:1995) does NOT match a `card 1995` title (year:1995), and vice-versa.
    check(
        def_norm,
        def_vocab,
        "pop 1995",
        &[("pop 1995", true), ("card 1995", false)],
    );
    check(
        def_norm,
        def_vocab,
        "1995 topps",
        &[("1995 topps", true), ("pop 1995 topps", false)],
    );
}

#[test]
fn diacritic_fold() {
    check(
        def_norm,
        def_vocab,
        "jokic",
        &[
            ("Jokić", true),
            ("JOKIĆ", true),
            ("jokic", true),
            ("jordan", false),
        ],
    );
    check(def_norm, def_vocab, "acuna", &[("Acuña rookie", true)]);
}

#[test]
fn half_grade_stays_one_token() {
    // `.` is Keep, so `9.5` is a single token (not split into `9` and `5`).
    check(
        def_norm,
        def_vocab,
        "9.5",
        &[("card 9.5", true), ("card 9 5", false)],
    );
}

#[test]
fn any_of_groups() {
    // Single-token members: satisfied iff >=1 present.
    check(
        def_norm,
        def_vocab,
        "(red,blue) car",
        &[("red car", true), ("blue car", true), ("green car", false)],
    );
    // Multi-token member ("upper deck"): titles carry the COMPLETE member or none (see module note).
    check(
        def_norm,
        def_vocab,
        "(upper deck,ud)",
        &[
            ("upper deck card", true),
            ("ud card", true),
            ("topps card", false),
        ],
    );
}

#[test]
fn number_typing_boundaries() {
    // 1900..=2099 is a year; 1899 and 2100 are generic terms.
    check(
        def_norm,
        def_vocab,
        "1900",
        &[("card 1900", true), ("card 1899", false)],
    );
    check(
        def_norm,
        def_vocab,
        "2099",
        &[("card 2099", true), ("card 2100", false)],
    );
    // 1899 is a generic term — matches a 1899 title, not a 1900 one.
    check(
        def_norm,
        def_vocab,
        "1899",
        &[("card 1899", true), ("card 1900", false)],
    );
}

#[test]
fn class_d_queries_are_dropped() {
    // A forbidden-only (class-D) query is dropped at ingest -> it matches nothing.
    check(
        def_norm,
        def_vocab,
        "-auto",
        &[("auto card", false), ("card", false)],
    );
    // An empty / whitespace-only query parses to zero clauses -> dropped.
    check(def_norm, def_vocab, "   ", &[("anything at all", false)]);
}
