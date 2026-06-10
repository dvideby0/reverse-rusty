//! Shared helpers for the adversarial property suite: engine construction, match-set
//! capture, and the catalogue of **identity perturbations** — surface edits that, under
//! the phrase-free default vocab, provably leave a title's feature set unchanged
//! (case folds away, foldable diacritics fold away, whitespace runs and Split-class
//! punctuation only re-shape token gaps, end-appended junk adds features no query
//! references).

use reverse_rusty::gen::Rng;
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};

pub(crate) fn engine_from(queries: &[(u64, String)]) -> Engine {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(queries);
    eng
}

pub(crate) fn matched(eng: &Engine, s: &mut MatchScratch, title: &str) -> Vec<u64> {
    let mut out = Vec::new();
    eng.match_title(title, s, &mut out, true);
    out.sort_unstable();
    out
}

/// Foldable diacritic substitutions — every right-hand char folds back to the left-hand
/// base in `normalize::fold_diacritic`, so the cleaned bytes are identical.
const DIACRITICS: &[(char, char)] = &[
    ('a', 'á'),
    ('e', 'é'),
    ('i', 'î'),
    ('o', 'ö'),
    ('u', 'ü'),
    ('n', 'ñ'),
    ('c', 'ç'),
    ('s', 'š'),
    ('z', 'ž'),
];

fn flip_case(rng: &mut Rng, s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphabetic() && rng.frac() < 0.5 {
                c.to_ascii_uppercase()
            } else {
                c
            }
        })
        .collect()
}

fn add_diacritics(rng: &mut Rng, s: &str) -> String {
    s.chars()
        .map(|c| {
            if rng.frac() < 0.25 {
                DIACRITICS
                    .iter()
                    .find(|&&(base, _)| base == c)
                    .map_or(c, |&(_, d)| d)
            } else {
                c
            }
        })
        .collect()
}

fn widen_whitespace(rng: &mut Rng, s: &str) -> String {
    let mut out = String::from(if rng.frac() < 0.5 { " " } else { "" });
    for ch in s.chars() {
        if ch == ' ' && rng.frac() < 0.6 {
            out.push_str(if rng.frac() < 0.3 { " \t " } else { "   " });
        } else {
            out.push(ch);
        }
    }
    out.push_str("  ");
    out
}

/// Split-class punctuation around (never inside) tokens: commas, bangs, a parenthesized
/// token. All of it cleans to spaces, so the token stream is unchanged.
fn sprinkle_split_punct(rng: &mut Rng, s: &str) -> String {
    let toks: Vec<&str> = s.split_whitespace().collect();
    let mut out: Vec<String> = Vec::with_capacity(toks.len());
    for t in toks {
        let mut t = t.to_string();
        match rng.below(6) {
            0 => t.push(','),
            1 => t.push_str("!!"),
            2 => t = format!("({t})"),
            _ => {}
        }
        out.push(t);
    }
    out.join(" ")
}

/// Append junk no stored query references: unicode soup that cleans to nothing, and a
/// fresh out-of-dict token (a synthetic `FeatureId` at match time). Appended at the END
/// so the stateful number pipeline upstream is untouched.
fn append_junk(rng: &mut Rng, s: &str) -> String {
    let mut out = s.to_string();
    if rng.frac() < 0.6 {
        out.push_str(" ™🔥カード");
    }
    out.push_str(&format!(
        " zzjunk{:012x}",
        rng.next_u64() & 0xffff_ffff_ffff
    ));
    out
}

pub(crate) const IDENTITY_OPS: usize = 5;

/// Apply identity perturbation `op` (0..IDENTITY_OPS). Every op preserves the title's
/// match set exactly under the phrase-free default vocab.
pub(crate) fn identity_perturb(rng: &mut Rng, title: &str, op: usize) -> String {
    match op {
        0 => flip_case(rng, title),
        1 => add_diacritics(rng, title),
        2 => widen_whitespace(rng, title),
        3 => sprinkle_split_punct(rng, title),
        _ => append_junk(rng, title),
    }
}

/// Compose every identity op in sequence — the worst realistic surface all at once.
pub(crate) fn identity_perturb_all(rng: &mut Rng, title: &str) -> String {
    let mut t = title.to_string();
    for op in 0..IDENTITY_OPS {
        t = identity_perturb(rng, &t, op);
    }
    t
}
