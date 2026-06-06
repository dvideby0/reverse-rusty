//! Independent **parse-union oracle** for the ADR-061 positive title view `P(T)`.
//!
//! The differential oracle (`tests/oracle/alias.rs`) computes its ground-truth title views by
//! calling the engine's OWN [`Normalizer::match_features_dual`], so it is structurally blind to a
//! bug *inside* that function — the `P(T)` construction. `P(T)`'s load-bearing claim (ADR-061) is
//! that it is a **strict superset of every parse**: for every way of collapsing the title's phrase
//! occurrences, every feature that reading emits must be in `P(T)`, or a query compiled from that
//! reading is a false negative (the zero-FN contract).
//!
//! This oracle verifies that claim directly and independently: it **enumerates every
//! phrase-collapse parse** of short titles (a structure deliberately different from `emit`'s single
//! pass) and asserts the engine's `P(T) ⊇` the union of all their features. It is restricted to the
//! feature classes a parse can actually vary — collapse-phrase entities, generic terms, and the
//! stateful grader/grade features — and the fuzz alphabet avoids years (4-digit), the `#`/`/`/`pop`
//! number markers, synonyms, and fused graders, so those orthogonal (parse-invariant) paths never
//! enter and the reference stays small.

use super::{Normalizer, NormalizerBuilder};
use crate::dict::{Dict, FeatureId, FeatureKind};
use std::collections::HashSet;

/// A collapse phrase for the fuzz: its pattern tokens and the entity feature name it emits.
struct Phrase {
    pat: &'static [&'static str],
    entity: &'static str,
}

/// Independent reference for `P(T)`: the union of emitted feature names over EVERY non-overlapping
/// subset of phrase occurrences (each subset = one parse), using the engine's NON-sticky per-parse
/// stateful semantics. Built from first principles (subset enumeration), not copied from `emit`.
struct ParseUnion<'a> {
    phrases: &'a [Phrase],
    graders: &'a [&'a str],
    grade_words: &'a [&'a str],
}

impl ParseUnion<'_> {
    /// Every word-aligned phrase occurrence in `tokens`: `(start, len, entity)`.
    fn occurrences(&self, tokens: &[&str]) -> Vec<(usize, usize, &'static str)> {
        let mut v = Vec::new();
        for p in self.phrases {
            let n = p.pat.len();
            if n == 0 || n > tokens.len() {
                continue;
            }
            for s in 0..=tokens.len() - n {
                if (0..n).all(|k| tokens[s + k] == p.pat[k]) {
                    v.push((s, n, p.entity));
                }
            }
        }
        v
    }

    fn is_grade(n: &str) -> bool {
        n.parse::<f32>().is_ok_and(|v| (1.0..=10.0).contains(&v))
    }
    fn is_num(t: &str) -> bool {
        !t.is_empty() && t.bytes().all(|b| b.is_ascii_digit())
    }

    /// Emit the feature names of ONE parse: the chosen occurrences are collapsed to their entity
    /// (their tokens consumed), the rest run the NON-sticky grader/grade/number/term pipeline. The
    /// consumed-token aging mirrors `emit`'s consumed branch (each consumed token still ages the
    /// pending grader / grade context by one); graders/numbers age nothing at the tail, generics do.
    fn emit_parse(&self, tokens: &[&str], consumed: &[bool], out: &mut HashSet<String>) {
        let mut pending: Option<String> = None;
        let mut pending_age = 0u8;
        let mut ctx = false;
        let mut ctx_age = 0u8;
        for (i, &tok) in tokens.iter().enumerate() {
            if consumed[i] {
                if pending.is_some() {
                    pending_age += 1;
                    if pending_age > 3 {
                        pending = None;
                    }
                }
                if ctx {
                    ctx_age += 1;
                    if ctx_age > 2 {
                        ctx = false;
                    }
                }
                continue;
            }
            if self.graders.contains(&tok) {
                out.insert(format!("grader:{tok}"));
                pending = Some(tok.to_string());
                pending_age = 0;
                continue;
            }
            if self.grade_words.contains(&tok) {
                ctx = true;
                ctx_age = 0;
                if pending.is_some() {
                    pending_age += 1;
                }
                continue;
            }
            if Self::is_num(tok) {
                if let Some(g) = pending.clone() {
                    if Self::is_grade(tok) {
                        out.insert(format!("grade:{tok}"));
                        out.insert(format!("grader_grade:{g}{tok}"));
                        pending = None;
                    } else {
                        out.insert(format!("term:{tok}"));
                    }
                } else if ctx && Self::is_grade(tok) {
                    out.insert(format!("grade:{tok}"));
                    ctx = false;
                } else {
                    out.insert(format!("term:{tok}"));
                }
                continue;
            }
            out.insert(format!("term:{tok}"));
            if pending.is_some() {
                pending_age += 1;
                if pending_age > 3 {
                    pending = None;
                }
            }
            if ctx {
                ctx_age += 1;
                if ctx_age > 2 {
                    ctx = false;
                }
            }
        }
    }

    /// The full parse-union: enumerate every subset of occurrences, keep the pairwise
    /// non-overlapping ones, and union their per-parse features (plus the chosen entities).
    fn union(&self, tokens: &[&str]) -> HashSet<String> {
        let occ = self.occurrences(tokens);
        assert!(occ.len() <= 20, "occurrence explosion in the fuzz vocab");
        let mut out = HashSet::new();
        for mask in 0u32..(1u32 << occ.len()) {
            // pairwise-overlap check for the chosen occurrences
            let mut consumed = vec![false; tokens.len()];
            let mut ok = true;
            let mut chosen_entities: Vec<&'static str> = Vec::new();
            for (bit, &(s, len, entity)) in occ.iter().enumerate() {
                if mask & (1 << bit) == 0 {
                    continue;
                }
                if consumed[s..s + len].iter().any(|&c| c) {
                    ok = false;
                    break;
                }
                consumed[s..s + len].fill(true);
                chosen_entities.push(entity);
            }
            if !ok {
                continue;
            }
            for e in chosen_entities {
                out.insert(e.to_string());
            }
            self.emit_parse(tokens, &consumed, &mut out);
        }
        out
    }
}

/// The fuzz vocabulary: a grader (`psa`), a second grader (`bgs`) to exercise pending-grader
/// overwrite, a grade-word (`gem`), and a set of OVERLAPPING collapse phrases that bind graders /
/// gradeable numbers — the arrangements that create "Goldilocks" parses. An alias form turns the
/// dual (`P(T)`/`N(T)`) view on so `match_features_dual` exercises the real positive superset.
fn fuzz_normalizer() -> (
    Normalizer,
    &'static [Phrase],
    &'static [&'static str],
    &'static [&'static str],
) {
    static PHRASES: &[Phrase] = &[
        Phrase {
            pat: &["psa", "9"],
            entity: "term:psa_9",
        },
        Phrase {
            pat: &["9", "x"],
            entity: "term:9_x",
        },
        Phrase {
            pat: &["psa", "a"],
            entity: "term:psa_a",
        },
        Phrase {
            pat: &["a", "bgs"],
            entity: "term:a_bgs",
        },
        Phrase {
            pat: &["gem", "8"],
            entity: "term:gem_8",
        },
    ];
    const GRADERS: &[&str] = &["psa", "bgs"];
    const GRADE_WORDS: &[&str] = &["gem"];
    let mut b = NormalizerBuilder::new();
    for g in GRADERS {
        b.add_grader(g);
    }
    for w in GRADE_WORDS {
        b.add_grade_word(w);
    }
    for p in PHRASES {
        b.add_phrase(p.pat, p.entity, FeatureKind::Generic);
    }
    b.add_alias_form("new york"); // activate the dual view
    (
        b.build().expect("fuzz normalizer"),
        PHRASES,
        GRADERS,
        GRADE_WORDS,
    )
}

/// Exhaustively enumerate short titles over the fuzz alphabet and assert the engine's `P(T)` is a
/// superset of the independent parse-union for every one. A miss is a false negative of the
/// "Goldilocks" class (a parse emits a feature `P(T)` lacks).
#[test]
fn engine_positive_view_is_a_superset_of_every_parse() {
    let (norm, phrases, graders, grade_words) = fuzz_normalizer();
    let reference = ParseUnion {
        phrases,
        graders,
        grade_words,
    };

    // Alphabet: two graders, a grade-word, two gradeable numbers, two generics.
    let alphabet = ["psa", "bgs", "gem", "8", "9", "x", "a"];

    // Pre-intern every feature name the reference can emit, so `get_or_synthetic` resolves them to
    // the SAME dense ids the engine's `match_features_dual` produces (no synthetic-hash ambiguity).
    let mut dict = Dict::new();
    let mut intern = |name: &str| dict.intern(name, FeatureKind::Generic);
    for g in graders {
        intern(&format!("grader:{g}"));
        for n in 1..=10 {
            intern(&format!("grade:{n}"));
            intern(&format!("grader_grade:{g}{n}"));
        }
    }
    for tok in alphabet {
        intern(&format!("term:{tok}"));
    }
    for p in phrases {
        intern(p.entity);
    }

    let mut lc = String::new();
    let mut neg: Vec<FeatureId> = Vec::new();
    let mut pos: Vec<FeatureId> = Vec::new();
    let mut checked = 0usize;

    // All titles of length 1..=4 over the alphabet (7^4 = 2401 of the longest tier).
    let mut stack: Vec<Vec<&str>> = alphabet.iter().map(|&t| vec![t]).collect();
    while let Some(title_toks) = stack.pop() {
        // Run the engine's dual view on the space-joined title.
        let title = title_toks.join(" ");
        norm.match_features_dual(&title, &dict, &mut lc, &mut neg, &mut pos);

        // Every reference feature must be present in P(T).
        let want = reference.union(&title_toks);
        for name in &want {
            let id = dict.get_or_synthetic(name);
            assert!(
                pos.binary_search(&id).is_ok(),
                "P(T) MISSING `{name}` for title `{title}` — a parse emits it but the positive \
                 superset does not (false negative). N(T)={:?} P(T)={:?}",
                names(&neg, &dict),
                names(&pos, &dict),
            );
        }
        checked += 1;

        if title_toks.len() < 5 {
            for &t in &alphabet {
                let mut next = title_toks.clone();
                next.push(t);
                stack.push(next);
            }
        }
    }
    assert!(
        checked > 19_000,
        "expected an exhaustive sweep, only checked {checked}"
    );
}

fn names(ids: &[FeatureId], dict: &Dict) -> Vec<String> {
    let mut v: Vec<String> = ids.iter().map(|&i| dict.name(i).to_string()).collect();
    v.sort();
    v
}
