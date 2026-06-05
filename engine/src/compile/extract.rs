//! AST → [`Extracted`] interning — the positive/negative feature extraction.
//!
//! Two paths that must stay in lockstep on what they read from the AST:
//!   - [`extract`] mutates the `Dict` (interns new vocabulary, bumps query
//!     frequency) — the compile-time pass A over every stored query.
//!   - [`extract_readonly`] resolves against a *frozen* shared dict without
//!     interning (so the `Arc<Dict>` shared across shards is never forked),
//!     falling back to deterministic synthetic ids for out-of-dict terms
//!     (dynamic vocabulary, ADR-046).
//!
//! Both honour the lossless-cover invariant structurally: forbidden features are
//! collected separately and never participate in anchor/signature selection.

use super::Extracted;
use crate::dict::{Dict, FeatureId};
use crate::dsl::{Ast, Atom};
use crate::normalize::Normalizer;

/// "hot" == one of the 64 most frequent features (has a common-mask bit).
/// Both compile and match agree on this, which is what keeps the cover lossless.
#[inline]
pub fn is_hot(dict: &Dict, f: FeatureId) -> bool {
    dict.mask_bit(f) != crate::dict::NO_MASK_BIT
}

/// Extract required / forbidden / any-of from an AST, interning features and
/// bumping their query-frequency. Run for every query in pass A.
pub fn extract(ast: &Ast, norm: &Normalizer, dict: &mut Dict, lc: &mut String) -> Extracted {
    let mut required: Vec<FeatureId> = Vec::new();
    let mut forbidden: Vec<FeatureId> = Vec::new();
    let mut anyof: Vec<Vec<FeatureId>> = Vec::new();

    // Consecutive positive bare words are normalized JOINTLY (in original order)
    // so multiword entities ("michael jordan", "psa 10") are recognized exactly
    // as they are in titles. Without this the query and title feature spaces
    // would disagree and we'd get false negatives.
    let mut pos_words: Vec<&str> = Vec::new();

    for clause in &ast.clauses {
        match (&clause.atom, clause.negated) {
            (Atom::Term(w), false) => {
                pos_words.push(w.as_str());
            }
            (Atom::Term(w) | Atom::Phrase(w), true) => {
                let feats = norm.compile_features(w, dict, lc);
                forbidden.extend_from_slice(&feats);
            }
            (Atom::Phrase(w), false) => {
                let feats = norm.compile_features(w, dict, lc);
                required.extend_from_slice(&feats);
            }
            (Atom::AnyOf(members), neg) => {
                if neg {
                    // -(a,b,c): reject if ANY member feature present
                    for m in members {
                        let feats = norm.compile_features(m, dict, lc);
                        forbidden.extend_from_slice(&feats);
                    }
                } else {
                    // (a,b,c): >=1 member present. Represent each member by its
                    // rarest (most specific) normalized feature.
                    let mut group: Vec<FeatureId> = Vec::new();
                    for m in members {
                        let feats = norm.compile_features(m, dict, lc);
                        if let Some(&rep) = feats.iter().min_by_key(|&&f| dict.freq(f)) {
                            group.push(rep);
                        }
                    }
                    group.sort_unstable();
                    group.dedup();
                    if group.len() == 1 {
                        // singleton group is just a required feature (more selective)
                        required.push(group[0]);
                    } else if !group.is_empty() {
                        anyof.push(group);
                    }
                }
            }
        }
    }

    // normalize the joined positive bare words as one stream
    if !pos_words.is_empty() {
        let joined = pos_words.join(" ");
        let feats = norm.compile_features(&joined, dict, lc);
        required.extend_from_slice(&feats);
    }

    required.sort_unstable();
    required.dedup();
    forbidden.sort_unstable();
    forbidden.dedup();

    // bump frequency once per distinct required/anyof feature (gating-relevant).
    // Frequencies reflect the LITERAL query (before equivalence expansion below), so the
    // hot-mask and anchor selection stay a function of the real corpus distribution.
    for &f in &required {
        dict.bump_freq(f);
    }
    for g in &anyof {
        for &f in g {
            dict.bump_freq(f);
        }
    }

    let mut out = Extracted {
        required,
        forbidden,
        anyof,
    };
    // Apply learned equivalences (ADR-054). No-op unless a vocabulary installed them on the
    // dict; FN-safe (the match set only grows). See `Extracted::expand_equivalences`.
    out.expand_equivalences(dict.equivalences());
    out
}

/// Read-only extract: resolves features against the frozen dict WITHOUT interning
/// (interning new vocabulary would fork the `Arc<Dict>` shared across shards). A term
/// absent from the dict is NOT skipped — `compile_features_readonly` resolves it to a
/// deterministic synthetic `FeatureId` via `dict.get_or_synthetic()` (dynamic
/// vocabulary, ADR-046), so a new required term still anchors its query (a collision is
/// a bounded over-match, never a dropped match). Safe for the read path and the cluster
/// coordinator's incremental adds against a frozen shared dict.
pub fn extract_readonly(ast: &Ast, norm: &Normalizer, dict: &Dict, lc: &mut String) -> Extracted {
    let mut required: Vec<FeatureId> = Vec::new();
    let mut forbidden: Vec<FeatureId> = Vec::new();
    let mut anyof: Vec<Vec<FeatureId>> = Vec::new();

    let mut pos_words: Vec<&str> = Vec::new();

    for clause in &ast.clauses {
        match (&clause.atom, clause.negated) {
            (Atom::Term(w), false) => {
                pos_words.push(w.as_str());
            }
            (Atom::Term(w) | Atom::Phrase(w), true) => {
                let feats = norm.compile_features_readonly(w, dict, lc);
                forbidden.extend_from_slice(&feats);
            }
            (Atom::Phrase(w), false) => {
                let feats = norm.compile_features_readonly(w, dict, lc);
                required.extend_from_slice(&feats);
            }
            (Atom::AnyOf(members), neg) => {
                if neg {
                    for m in members {
                        let feats = norm.compile_features_readonly(m, dict, lc);
                        forbidden.extend_from_slice(&feats);
                    }
                } else {
                    let mut group: Vec<FeatureId> = Vec::new();
                    for m in members {
                        let feats = norm.compile_features_readonly(m, dict, lc);
                        if let Some(&rep) = feats.iter().min_by_key(|&&f| dict.freq(f)) {
                            group.push(rep);
                        }
                    }
                    group.sort_unstable();
                    group.dedup();
                    if group.len() == 1 {
                        required.push(group[0]);
                    } else if !group.is_empty() {
                        anyof.push(group);
                    }
                }
            }
        }
    }

    if !pos_words.is_empty() {
        let joined = pos_words.join(" ");
        let feats = norm.compile_features_readonly(&joined, dict, lc);
        required.extend_from_slice(&feats);
    }

    required.sort_unstable();
    required.dedup();
    forbidden.sort_unstable();
    forbidden.dedup();

    let mut out = Extracted {
        required,
        forbidden,
        anyof,
    };
    // Apply learned equivalences (ADR-054); no-op unless installed on the dict. FN-safe.
    out.expand_equivalences(dict.equivalences());
    out
}
