//! Unicode-soup fuzz: no panics, deterministic output, and the documented structural
//! relationships between the title views, over inputs no clean generator would emit.
//!
//! Library code must never panic (typed errors only), and `match_features_dual`
//! documents two invariants that hold for ANY input: `P(T) ⊇ N(T)` (the positive view
//! only ever adds), and `N(T)` equals the single-view `match_features` output. Both are
//! asserted here directly at the normalizer seam — the exact place the differential
//! oracle is structurally blind to (it calls this function for its own ground truth).
//! Multi-byte boundaries, control chars, combining marks, markers, and DSL syntax soup
//! all flow through both the alias-active and the plain normalizer, plus the DSL parser.

use reverse_rusty::dict::{Dict, FeatureKind};
use reverse_rusty::gen::Rng;
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::vocab::{AliasProvenance, AliasStatus, Vocab};

/// Char pool: ASCII text + digits, every punctuation class (Split / Keep `.` / Marker
/// `#` `/`), DSL syntax, whitespace variants, foldable diacritics, and multi-byte
/// unicode (CJK, emoji, combining marks, zero-width, controls).
const POOL: &[char] = &[
    'a', 'b', 'p', 's', '9', '1', '0', '5', ' ', ' ', ' ', '\t', '\n', '.', '#', '/', '-', '\'',
    '(', ')', '"', ',', '!', '&', '+', 'á', 'é', 'ö', 'ñ', 'ç', 'š', 'Á', 'ž', 'カ', 'ー', 'ド',
    '新', '🔥', '★', '½', '™', '\u{301}', '\u{200d}', '\u{feff}', '\u{0}', '\u{7f}',
];

/// Hand-picked nasties: token-length extremes, marker/number ambiguity, unbalanced DSL
/// syntax, pure punctuation, half-grade lookalikes.
fn pinned_nasties() -> Vec<String> {
    let mut v: Vec<String> = [
        "",
        " ",
        "  ",
        "\t\t",
        "----",
        "''''",
        "((((((((a",
        "\"abc",
        "()",
        "(,)",
        ",",
        "-",
        "--x",
        "-(",
        "9.5.5.",
        "1985#",
        "#1985",
        "# 1985",
        "10/",
        "/10",
        "10 / 99",
        "pop 9",
        "psa10!!",
        "psa psa psa 10",
        "gem gem 9",
        "ñ",
        "™",
        "🔥",
        "a\u{301}b",
        "\u{feff}psa 10",
    ]
    .iter()
    .map(ToString::to_string)
    .collect();
    v.push("a".repeat(10_000));
    v.push("ñ".repeat(2_000));
    v.push("psa 10 ".repeat(500));
    v
}

fn random_soup(rng: &mut Rng) -> String {
    let len = rng.below(81);
    (0..len).map(|_| POOL[rng.below(POOL.len())]).collect()
}

/// An alias-active normalizer (multi-word alias registered ⇒ the dual view is real) and
/// a plain one, plus a dict mixing dense and synthetic ids.
///
/// The alias vocab also carries a GRADER (`psa`), a grade word (`gem`), and a COLLAPSE
/// phrase (`p s`) over pool letters: without those, `force_additive` (the `P(T)` pass)
/// is indistinguishable from the canonical pass on any fuzz input — alias-mode phrases
/// are already additive on the title side — and a mutation that computes `N(T)` with the
/// positive-view semantics survives the `match_features == N(T)` assertion. The grader /
/// grade-word / collapse-phrase state machines are exactly where the two passes diverge.
fn fuzz_fixtures() -> (Normalizer, Normalizer, Dict) {
    let plain = Normalizer::default_vocab().expect("default vocab");
    let mut v = Vocab::new();
    v.add_grader("psa");
    v.add_grade_word("gem");
    v.add_phrase(&["p", "s"], "term:p_s", FeatureKind::Generic);
    let status = v.aliases_mut().add_classified(
        &["ny".into(), "new york".into()],
        AliasProvenance::Manual,
        1.0,
        &plain,
        &Dict::new(),
    );
    assert_eq!(status, Some(AliasStatus::Active), "manual alias activates");
    let alias_norm = v.to_normalizer().expect("alias normalizer");
    assert!(
        alias_norm.has_multiword_aliases(),
        "fixture must exercise the dual-view path"
    );

    let mut dict = Dict::new();
    for n in ["term:psa", "term:a", "term:b", "grader:psa", "grade:10"] {
        dict.intern(n, FeatureKind::Generic);
    }
    (
        alias_norm,
        Normalizer::default_vocab().expect("vocab"),
        dict,
    )
}

fn assert_sorted_dedup(ids: &[u32], what: &str, input: &str) {
    assert!(
        ids.windows(2).all(|w| w[0] < w[1]),
        "{what} not sorted+deduped for input {input:?}"
    );
}

fn is_superset(sup: &[u32], sub: &[u32]) -> bool {
    sub.iter().all(|f| sup.binary_search(f).is_ok())
}

#[test]
fn normalizer_and_parser_survive_unicode_soup_with_invariants_intact() {
    let (alias_norm, plain_norm, dict) = fuzz_fixtures();
    let mut rng = Rng::new(0xFA57_F00D);

    let mut inputs = pinned_nasties();
    for _ in 0..30_000 {
        inputs.push(random_soup(&mut rng));
    }

    let mut lc = String::new();
    let (mut neg, mut pos) = (Vec::new(), Vec::new());
    let (mut neg2, mut pos2) = (Vec::new(), Vec::new());
    let mut single = Vec::new();

    for input in &inputs {
        // Alias-active normalizer: the real dual-view path.
        alias_norm.match_features_dual(input, &dict, &mut lc, &mut neg, &mut pos);
        assert_sorted_dedup(&neg, "N(T)", input);
        assert_sorted_dedup(&pos, "P(T)", input);
        assert!(
            is_superset(&pos, &neg),
            "P(T) ⊉ N(T) for input {input:?} — the positive view dropped a canonical feature"
        );

        // Documented equality: N(T) == the single-view match_features output.
        alias_norm.match_features(input, &dict, &mut lc, &mut single);
        assert_eq!(
            single, neg,
            "match_features != N(T) of match_features_dual for input {input:?}"
        );

        // Determinism: same input, same views.
        alias_norm.match_features_dual(input, &dict, &mut lc, &mut neg2, &mut pos2);
        assert_eq!(neg, neg2, "non-deterministic N(T) for {input:?}");
        assert_eq!(pos, pos2, "non-deterministic P(T) for {input:?}");

        // Plain normalizer: single view ⇒ the two outputs are identical by contract.
        plain_norm.match_features_dual(input, &dict, &mut lc, &mut neg, &mut pos);
        assert_eq!(
            neg, pos,
            "no-alias normalizer must produce identical views for {input:?}"
        );

        // Query-side compile path + the DSL parser: must not panic on any byte soup.
        let _ = alias_norm.compile_features_readonly(input, &dict, &mut lc);
        let _ = reverse_rusty::dsl::parse(input);
    }
}
