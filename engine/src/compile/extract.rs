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

use super::{AnyOfMember, AnyOfPredicate, Extracted};
use crate::dict::{Dict, FeatureId};
use crate::dsl::{Ast, Atom};
use crate::normalize::Normalizer;

/// "hot" == one of the 64 most frequent features (has a common-mask bit).
/// Both compile and match agree on this, which is what keeps the cover lossless.
///
/// This predicate serves the title side too (the arity-2 pair loop and the
/// cluster's `route()` exclude exactly the top-64 features) — it must stay
/// mask-keyed. The θ extension below is COMPILE-SIDE ONLY.
#[inline]
pub fn is_hot(dict: &Dict, f: FeatureId) -> bool {
    dict.mask_bit(f) != crate::dict::NO_MASK_BIT
}

/// The Broad-Query Cost Program's classification predicate (ADR-105): a feature
/// is a *hot anchor* when it is top-64 (`is_hot`) OR its query frequency has
/// reached the engine's `hot_anchor_threshold` θ (`theta == 0` disables the
/// extension — byte-identical to [`is_hot`]).
///
/// **Compile-side only.** The title side never consults θ: hot-tier entries are
/// retrieved by exhaustive arity-1 probes of the hot index, so no title-side
/// predicate has to mirror this one (the ADR-105 safety argument — unlike the
/// pair predicate, which stays strictly `is_hot`-keyed on both sides).
#[inline]
pub fn is_hot_anchor(dict: &Dict, f: FeatureId, theta: u32) -> bool {
    is_hot(dict, f) || (theta != 0 && dict.freq(f) >= theta)
}

/// Normalize one maximal run of consecutive positive bare terms through the
/// mutable query-side pipeline. Every other AST clause is a hard boundary:
/// joining across it can manufacture a multi-word entity that the source query
/// never contained contiguously, which can make candidate retrieval lossy.
fn flush_positive_run(
    words: &mut Vec<&str>,
    norm: &Normalizer,
    dict: &mut Dict,
    lc: &mut String,
    required: &mut Vec<FeatureId>,
) {
    if words.is_empty() {
        return;
    }
    let joined = words.join(" ");
    required.extend(norm.compile_features(&joined, dict, lc));
    words.clear();
}

/// Read-only twin of [`flush_positive_run`]. Kept separate so this path cannot
/// accidentally intern into the frozen dictionary shared by cluster shards.
fn flush_positive_run_readonly(
    words: &mut Vec<&str>,
    norm: &Normalizer,
    dict: &Dict,
    lc: &mut String,
    required: &mut Vec<FeatureId>,
) {
    if words.is_empty() {
        return;
    }
    let joined = words.join(" ");
    required.extend(norm.compile_features_readonly(&joined, dict, lc));
    words.clear();
}

/// Extract required / forbidden / any-of from an AST, interning features and
/// bumping their query-frequency. Run for every query in pass A.
pub fn extract(ast: &Ast, norm: &Normalizer, dict: &mut Dict, lc: &mut String) -> Extracted {
    let mut required: Vec<FeatureId> = Vec::new();
    let mut forbidden: Vec<FeatureId> = Vec::new();
    let mut anyof: Vec<Vec<FeatureId>> = Vec::new();
    let mut anyof_predicates: Vec<AnyOfPredicate> = Vec::new();
    let mut forbidden_conjunctions: Vec<Vec<FeatureId>> = Vec::new();

    // Consecutive positive bare words are normalized JOINTLY (in original order)
    // so multiword entities ("michael jordan", "psa 10") are recognized exactly
    // as they are in titles. Without this the query and title feature spaces
    // would disagree and we'd get false negatives.
    let mut pos_words: Vec<&str> = Vec::new();

    for clause in &ast.clauses {
        if !matches!((&clause.atom, clause.negated), (Atom::Term(_), false)) {
            flush_positive_run(&mut pos_words, norm, dict, lc, &mut required);
        }
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
                    // A negated group rejects if ANY WHOLE member matches.
                    // Singleton members retain the flat forbidden fast path;
                    // multi-feature members remain conjunctions for exact verify.
                    for m in members {
                        let mut feats = norm.compile_features(m, dict, lc);
                        feats.sort_unstable();
                        feats.dedup();
                        match feats.as_slice() {
                            [feature] => forbidden.push(*feature),
                            [] => {}
                            _ => forbidden_conjunctions.push(feats),
                        }
                    }
                } else {
                    // OR across members, AND across every normalized feature in
                    // one member. Keep rarest-feature proxies only as a lossless
                    // retrieval condition; compound exact predicates carry the
                    // complete member semantics.
                    let mut semantic_members: Vec<AnyOfMember> = Vec::new();
                    for m in members {
                        let feats = norm.compile_features(m, dict, lc);
                        if let Some(member) = AnyOfMember::from_features(feats) {
                            semantic_members.push(member);
                        }
                    }
                    semantic_members.sort_unstable();
                    semantic_members.dedup();
                    if semantic_members.len() == 1 {
                        // OR over one member is simply that member's conjunction.
                        for requirement in &semantic_members[0].requirements {
                            required.extend_from_slice(requirement);
                        }
                    } else if !semantic_members.is_empty() {
                        let mut proxies = Vec::with_capacity(semantic_members.len());
                        for member in &semantic_members {
                            if let Some(proxy) = member
                                .requirements
                                .iter()
                                .filter_map(|alternatives| alternatives.first().copied())
                                .min_by_key(|&feature| dict.freq(feature))
                            {
                                proxies.push(proxy);
                            }
                        }
                        proxies.sort_unstable();
                        proxies.dedup();
                        if !proxies.is_empty() {
                            anyof.push(proxies);
                        }
                        if semantic_members
                            .iter()
                            .any(|member| member.requirements.len() > 1)
                        {
                            anyof_predicates.push(AnyOfPredicate {
                                members: semantic_members,
                            });
                        }
                    }
                }
            }
        }
    }

    flush_positive_run(&mut pos_words, norm, dict, lc, &mut required);

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
        anyof_predicates,
        forbidden_conjunctions,
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
    let mut anyof_predicates: Vec<AnyOfPredicate> = Vec::new();
    let mut forbidden_conjunctions: Vec<Vec<FeatureId>> = Vec::new();

    let mut pos_words: Vec<&str> = Vec::new();

    for clause in &ast.clauses {
        if !matches!((&clause.atom, clause.negated), (Atom::Term(_), false)) {
            flush_positive_run_readonly(&mut pos_words, norm, dict, lc, &mut required);
        }
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
                        let mut feats = norm.compile_features_readonly(m, dict, lc);
                        feats.sort_unstable();
                        feats.dedup();
                        match feats.as_slice() {
                            [feature] => forbidden.push(*feature),
                            [] => {}
                            _ => forbidden_conjunctions.push(feats),
                        }
                    }
                } else {
                    let mut semantic_members: Vec<AnyOfMember> = Vec::new();
                    for m in members {
                        let feats = norm.compile_features_readonly(m, dict, lc);
                        if let Some(member) = AnyOfMember::from_features(feats) {
                            semantic_members.push(member);
                        }
                    }
                    semantic_members.sort_unstable();
                    semantic_members.dedup();
                    if semantic_members.len() == 1 {
                        for requirement in &semantic_members[0].requirements {
                            required.extend_from_slice(requirement);
                        }
                    } else if !semantic_members.is_empty() {
                        let mut proxies = Vec::with_capacity(semantic_members.len());
                        for member in &semantic_members {
                            if let Some(proxy) = member
                                .requirements
                                .iter()
                                .filter_map(|alternatives| alternatives.first().copied())
                                .min_by_key(|&feature| dict.freq(feature))
                            {
                                proxies.push(proxy);
                            }
                        }
                        proxies.sort_unstable();
                        proxies.dedup();
                        if !proxies.is_empty() {
                            anyof.push(proxies);
                        }
                        if semantic_members
                            .iter()
                            .any(|member| member.requirements.len() > 1)
                        {
                            anyof_predicates.push(AnyOfPredicate {
                                members: semantic_members,
                            });
                        }
                    }
                }
            }
        }
    }

    flush_positive_run_readonly(&mut pos_words, norm, dict, lc, &mut required);

    required.sort_unstable();
    required.dedup();
    forbidden.sort_unstable();
    forbidden.dedup();

    let mut out = Extracted {
        required,
        forbidden,
        anyof,
        anyof_predicates,
        forbidden_conjunctions,
    };
    // Apply learned equivalences (ADR-054); no-op unless installed on the dict. FN-safe.
    out.expand_equivalences(dict.equivalences());
    out
}
