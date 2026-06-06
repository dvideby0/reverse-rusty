//! Structural classification of an alias group + the auto-activation policy (ADR-060).
//!
//! Classification is deliberately *structural* — it reads only the forms' token count, their
//! `FeatureKind`s, and a narrow surface-string variant test. It never asks "are these
//! semantically the same"; that is exactly the judgement Phase 1 defers to a human reviewer
//! for anything but a clear spelling variant.

use serde::{Deserialize, Serialize};

use super::{AliasProvenance, AliasStatus};
use crate::dict::{Dict, FeatureKind};
use crate::normalize::Normalizer;

/// Structural classification of an alias group — the input to the auto-activation policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AliasKind {
    /// Every form is single-token and all pairs are spelling / abbreviation variants (share a
    /// ≥3-character common prefix — plurals, truncations, hyphenation folds). The only kind
    /// auto-activated regardless of source: a structural variant is a near-certain same-entity.
    SingleTokenVariant,
    /// Every form is single-token but they are **not** all variants — distinct tokens that may
    /// be genuine synonyms (when an operator declares them) or merely co-listed alternatives
    /// like graders `(psa, bgs, sgc)` (when learned from an any-of disjunction). Active only
    /// when declared / manual; a candidate when learned.
    SingleTokenDistinct,
    /// At least one form spans multiple tokens — a token-graph (multi-word) alias. Expressed
    /// by the **Phase-2** matcher (ADR-061: query-side collapse + title-side overlap superset +
    /// the two-view verifier). Active when declared / manual (operator intent); a candidate when
    /// learned from an any-of disjunction.
    MultiWord,
    /// The forms resolve to more than one known `FeatureKind` (e.g. a Brand and a Player).
    /// **Always** a candidate — expanding across kinds is unsafe.
    MixedKind,
}

/// Classify a group's [`AliasKind`] against the current normalizer + dict.
///
/// Order matters: a multi-token form short-circuits to [`MultiWord`](AliasKind::MultiWord)
/// (Phase 2 owns it); otherwise a >1 known-kind split is [`MixedKind`](AliasKind::MixedKind);
/// otherwise the surface-string variant test decides
/// [`SingleTokenVariant`](AliasKind::SingleTokenVariant) vs
/// [`SingleTokenDistinct`](AliasKind::SingleTokenDistinct).
pub(super) fn classify_kind(forms: &[String], norm: &Normalizer, dict: &Dict) -> AliasKind {
    let mut lc = String::new();
    let mut kinds: Vec<FeatureKind> = Vec::new();
    let mut multiword = false;
    for f in forms {
        let feats = norm.compile_features_readonly(f, dict, &mut lc);
        // A multi-word case = a RAW whitespace token count > 1 (checked *before* phrase folding,
        // so the boundary can't depend on which phrases happen to fold it, e.g. importing
        // `ud => upper deck` while `upper deck` is a declared phrase) OR a single-word form that
        // resolves to ≠1 feature (zero = all punctuation; several = a punctuation-split word).
        if f.split_whitespace().count() != 1 || feats.len() != 1 {
            multiword = true;
        }
        // Collect the kinds of EVERY resolved feature — single- AND multi-token — so a cross-kind
        // group is caught even when a form is multi-word. Without this, a multi-word short-circuit
        // would let a Brand phrase ≡ Player phrase bypass the MixedKind refusal and auto-activate
        // an unsafe equivalence (codex review, ADR-061).
        for &id in &feats {
            kinds.push(dict.kind(id));
        }
    }

    // Mixed kind — checked BEFORE the multi-word classification — only when ≥2 *different* known
    // (non-Generic) kinds appear: an un-interned form reads as Generic, so a fresh import (nothing
    // interned yet) never trips this — it is a guard against merging an already-known Brand with an
    // already-known Player (in any form, single- or multi-token), not a hair-trigger.
    let known = kinds
        .iter()
        .copied()
        .filter(|k| *k != FeatureKind::Generic)
        .collect::<Vec<_>>();
    if let Some(&first) = known.first() {
        if known.iter().any(|&k| k != first) {
            return AliasKind::MixedKind;
        }
    }

    if multiword {
        return AliasKind::MultiWord;
    }

    if all_pairwise_variant(forms) {
        AliasKind::SingleTokenVariant
    } else {
        AliasKind::SingleTokenDistinct
    }
}

/// The auto-activation policy (ADR-060). Conservative by construction: anything the Phase-1
/// matcher cannot express, or any learned guess that is not a clear variant, defaults to a
/// review candidate — never silently active.
pub(super) fn default_status_for(kind: AliasKind, provenance: AliasProvenance) -> AliasStatus {
    use AliasProvenance::{DeclaredFile, LearnedFromQueries, Manual};
    let auto_active = match kind {
        // Cross-kind expansion is unsafe — always review-only.
        AliasKind::MixedKind => false,
        // A clear structural variant is trusted from any source.
        AliasKind::SingleTokenVariant => true,
        // Distinct single tokens, or a multi-word token-graph alias (ADR-061): honor an operator
        // declaration (declared / manual), but treat a learned any-of disjunction (the
        // `(psa, bgs, sgc)` case, or a learned multi-word guess) as a review candidate.
        AliasKind::SingleTokenDistinct | AliasKind::MultiWord => match provenance {
            DeclaredFile | Manual => true,
            LearnedFromQueries => false,
        },
    };
    if auto_active {
        AliasStatus::Active
    } else {
        AliasStatus::Candidate
    }
}

/// True iff every pair of forms is a spelling / abbreviation variant.
fn all_pairwise_variant(forms: &[String]) -> bool {
    let surfaces: Vec<String> = forms.iter().map(|f| surface(f)).collect();
    for i in 0..surfaces.len() {
        for j in (i + 1)..surfaces.len() {
            if !is_variant_like(&surfaces[i], &surfaces[j]) {
                return false;
            }
        }
    }
    true
}

/// Two surface tokens are spelling / abbreviation variants iff they share a common prefix of
/// at least 3 characters (plurals `refractor`/`refractors`, truncations `auto`/`autograph`,
/// hyphenation folds). Deliberately narrow + explainable: it errs toward `false` (→ candidate),
/// so a recall-first deployment never *silently* merges two distinct tokens. Richer signals
/// (subsequence abbreviations, bounded edit distance) are a deferred refinement that can only
/// *widen* the auto-active set.
fn is_variant_like(a: &str, b: &str) -> bool {
    common_prefix_len(a, b) >= 3
}

/// Length (in `char`s) of the shared leading prefix of `a` and `b`.
fn common_prefix_len(a: &str, b: &str) -> usize {
    a.chars().zip(b.chars()).take_while(|(x, y)| x == y).count()
}

/// Lower-cased, diacritic-folded, alphanumeric-only surface of a raw form — the string the
/// variant test compares (the feature *name* would carry a shared `term:` prefix and is
/// useless for this). Mirrors the token folding the normalizer applies.
fn surface(form: &str) -> String {
    let mut out = String::with_capacity(form.len());
    for ch in form.chars() {
        let c = crate::normalize::fold_diacritic(ch);
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        }
    }
    out
}
