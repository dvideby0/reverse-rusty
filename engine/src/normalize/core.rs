//! `impl Normalizer` — the shared query/title normalization core.
//!
//! Hot path: yes — `emit` (and its public entry points `match_features` /
//! `compile_features` / `compile_features_readonly`) run per incoming title.
//! Holds the `Normalizer` struct definition, its byte-cleaning (`clean_into`),
//! the two-phase `emit` pipeline (daachorse multiword scan → number/synonym
//! /generic tokenization), and the small free helpers `emit` relies on
//! (`fold_diacritic`, number/year parsing, generic emission).

use super::{
    NormScratch, PhraseArc, PhraseEntry, PhraseGraph, PhraseMode, PositionArc, PunctClass,
    PunctTable, Side,
};
use crate::dict::{Dict, FeatureId, FeatureKind};
use daachorse::DoubleArrayAhoCorasick;

mod alias_overlap;
mod helpers;
pub(super) use alias_overlap::PhraseOverlap;
pub use helpers::fold_diacritic;
use helpers::{as_year, collapse_ws_runs_in_place, emit_generic, parse_number};

#[inline]
fn position_index(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[derive(Clone, Copy)]
struct EmitMode {
    side: Side,
    force_additive: bool,
    retain_positioned_starts: bool,
}

impl EmitMode {
    fn flat(side: Side, force_additive: bool) -> Self {
        Self {
            side,
            force_additive,
            retain_positioned_starts: false,
        }
    }

    fn positioned(side: Side, force_additive: bool) -> Self {
        Self {
            side,
            force_additive,
            retain_positioned_starts: true,
        }
    }
}

pub struct Normalizer {
    /// daachorse automaton over space-joined phrase strings. Pattern value indexes
    /// into `phrase_entries`.
    pub(super) automaton: DoubleArrayAhoCorasick<usize>,
    pub(super) phrase_entries: Vec<PhraseEntry>,
    /// Overlapping (`MatchKind::Standard`) automaton over every registered phrase.
    /// ADR-061 uses it for alias-enabled flat `P(T)`; ADR-120 also uses it for
    /// phrase-aware `P(T)` even when no alias is registered. `None` means there
    /// are no multi-word phrases at all.
    pub(super) phrase_overlap: Option<PhraseOverlap>,
    /// Kept separate from `phrase_overlap`: ordinary vocabulary phrases must
    /// not activate ADR-061's distinct flat positive view.
    pub(super) has_multiword_aliases: bool,

    /// single-token synonyms -> (canonical feature, kind).
    pub(super) synonyms: Vec<(String, String, FeatureKind)>,
    pub(super) syn_index: std::collections::HashMap<String, usize>,
    /// Byte-cleaning punctuation classification (ADR-058). Default = historical behavior.
    pub(super) punct: PunctTable,
    /// Number-context words (ADR-069): a number immediately after one of these tokens is
    /// demoted to a generic term instead of typed as a year. Empty by default.
    /// Lowercased at build, so entries compare directly against cleaned tokens.
    pub(super) number_context: Vec<String>,
}

impl std::fmt::Debug for Normalizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Normalizer")
            .field("phrases", &self.phrase_entries.len())
            .field("synonyms", &self.synonyms.len())
            .field("number_context", &self.number_context)
            .finish()
    }
}

impl Normalizer {
    /// Create a [`NormalizerBuilder`](super::NormalizerBuilder) for assembling a custom vocabulary.
    pub fn builder() -> super::NormalizerBuilder {
        super::NormalizerBuilder::new()
    }

    /// The cleaned whitespace tokens of `text` under this normalizer's punctuation table — the
    /// same tokenization the phrase automaton is registered against (ADR-061). A form cleans to
    /// **≥2** tokens iff it can be registered as a multi-word alias phrase (and so reduce to one
    /// entity); a 1-token form that does not resolve to exactly one feature cannot. Used by the
    /// alias classifier to keep an unexpressible form a review candidate rather than auto-activate
    /// a group `resolve_equivalences` would silently drop.
    #[must_use]
    pub fn clean_tokens(&self, text: &str) -> Vec<String> {
        alias_form_tokens(&self.punct, text)
    }

    /// True if any **multi-word alias** phrase is registered (ADR-061) — i.e. the title side
    /// produces a distinct positive superset view via [`match_features_dual`](Self::match_features_dual).
    /// When `false`, the two title views are always identical and every lane stays byte-identical
    /// to the pre-ADR-061 single-view path. Used to keep the broad lane on its two-view inline
    /// path while multi-word aliases are active.
    #[must_use]
    pub fn has_multiword_aliases(&self) -> bool {
        self.has_multiword_aliases
    }

    /// Build a domain-agnostic normalizer with no pre-loaded vocabulary.
    ///
    /// The normalizer still handles year detection, number disambiguation,
    /// diacritic folding, and lowercase normalization. Domain-specific vocabulary
    /// (phrases, synonyms, equivalences, and punctuation rules) should be supplied via
    /// [`NormalizerBuilder`](super::NormalizerBuilder) or learned from query any-of groups at runtime.
    pub fn default_vocab() -> Result<Self, crate::error::NormalizerError> {
        super::NormalizerBuilder::new().build()
    }

    /// Lowercase + fold diacritics + apply the punctuation table into `out` (reused).
    /// Alphanumerics pass through lowercased; every other character is handled by its
    /// [`PunctClass`]. Defaults (ADR-058): `.` is kept in place, `#`/`/`
    /// become standalone marker tokens (so identifiers such as `#2` and `/199` remain
    /// generic numbers), and everything else becomes a space. A [`PunctClass::Fold`] character is
    /// deleted, so its neighbors join into one token (`O'Brien` -> `obrien`). The same
    /// table runs over queries and titles, keeping the feature spaces aligned (§2).
    fn clean_into(&self, text: &str, out: &mut String) {
        clean_with(&self.punct, text, out);
    }
}

mod emit;
mod features;
mod phrase;

/// Byte-clean `text` into `out` (reused): lowercase + fold diacritics + apply the punctuation
/// table. Shared by [`Normalizer::clean_into`] (the hot path) and the builder's alias-phrase
/// registration (ADR-061). **Whitespace runs are NOT collapsed** — the cleaned text is verbatim,
/// so this is byte-identical across versions and a persisted segment's features never desync on a
/// binary upgrade (codex R8). Matching an alias against a title with whitespace runs is instead
/// handled, recall-safely, by the positive-view overlap scan ([`PhraseOverlap::collect_into`]).
pub(super) fn clean_with(punct: &PunctTable, text: &str, out: &mut String) {
    out.clear();
    for ch in text.chars() {
        let c = fold_diacritic(ch);
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else {
            match punct.class_of(c) {
                PunctClass::Split => out.push(' '),
                PunctClass::Fold => {} // delete: neighbors join into one token
                PunctClass::Keep => out.push(c),
                PunctClass::Marker => {
                    out.push(' ');
                    out.push(c);
                    out.push(' ');
                }
            }
        }
    }
}

/// The cleaned whitespace tokens of an alias `form` under `punct` (ADR-061). Returns the same
/// token sequence the normalizer's phase-2 tokenizer sees, so a registered alias phrase pattern
/// aligns with cleaned title text. An empty result (all-punctuation form) registers nothing.
pub(super) fn alias_form_tokens(punct: &PunctTable, form: &str) -> Vec<String> {
    let mut buf = String::new();
    clean_with(punct, form, &mut buf);
    buf.split_whitespace().map(ToString::to_string).collect()
}
