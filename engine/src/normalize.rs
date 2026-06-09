//! Shared normalizer for queries (compile time) and titles (match time).
//!
//! Design: docs/design/normalization.md §2–4
//! Invariant: The SAME normalizer processes both queries and titles — feature
//!   spaces must align or correctness breaks
//! Hot path: yes — title normalization runs per incoming title
//!
//! Pipeline:
//!   clean bytes -> daachorse leftmost-longest multiword alias scan
//!              -> tokenize non-matched spans -> grader/grade/year patterns
//!              -> synonyms -> generic.
//!
//! The multiword phase uses a daachorse double-array Aho-Corasick automaton in
//! leftmost-longest mode. This replaced the original token-trie with identical
//! semantics but O(n) scan time regardless of vocabulary size.
//!
//! This file holds the data-type *definitions* shared across the module (the
//! phrase metadata + the byte-cleaning punctuation table); the `impl` blocks and
//! the public surface live in focused submodules:
//!   - [`core`]    — the `Normalizer` struct + its byte-cleaning + two-phase `emit` hot path + the compile/match entry points + free helpers
//!   - [`builder`] — `NormalizerBuilder` (off-hot-path construction + automaton build)

use crate::dict::FeatureKind;

mod builder;
mod core;

#[cfg(test)]
mod tests;

/// Independent parse-union oracle for the ADR-061 positive view `P(T)` (the matcher's
/// FN-safety crux): exhaustively enumerates every phrase-collapse parse of short titles and
/// asserts the engine's `P(T)` is a superset — a cross-check the differential oracle cannot
/// do because it reuses `match_features_dual` itself.
#[cfg(test)]
mod parse_union_oracle;

pub use builder::NormalizerBuilder;
pub use core::{fold_diacritic, Normalizer};

/// Metadata for a phrase pattern registered in the automaton.
#[derive(Debug, Clone)]
struct PhraseEntry {
    feature: String,
    kind: FeatureKind,
    mode: PhraseMode,
}

/// How a phrase match treats its component tokens — and whether it is query/title
/// **asymmetric** (the multi-word alias case, ADR-061).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PhraseMode {
    /// A phrase match **consumes** its component tokens on both sides — only the phrase
    /// feature is emitted (collapse / entity-disambiguation; declared + hand-built vocab).
    Collapse,
    /// The phrase feature is emitted **in addition to** the component tokens on both sides
    /// (additive — the component features are still produced), so a query referencing a
    /// component never loses the match. Corpus-learned phrases (ADR-053) are additive:
    /// this engine is a recall-first candidate generator, so a phrase must never drop a
    /// candidate a component query would have matched.
    Additive,
    /// A **multi-word alias** form (ADR-061): asymmetric by [`Side`]. On the query/compile
    /// side it **collapses** (components consumed) so the form reduces to its single entity
    /// feature — which ADR-054 equivalence expansion then widens to the alias group. On the
    /// title/match side it is **additive** (entity + components) so a component query still
    /// matches, and it additionally participates in the title-side overlap superset (the
    /// positive view `P(T)`) so nested/overlapping aliases are all found.
    Alias,
}

/// Which side of the matcher a normalization pass serves. The feature spaces are shared
/// (the §2 invariant), but a [`PhraseMode::Alias`] phrase is collapsed on the query side
/// and additive on the title side — the ES `synonym_graph` asymmetry (ADR-061). Every
/// other phrase mode is side-independent, so the default (no alias phrases) path is
/// byte-identical regardless of `Side`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    /// Query / compile path (`compile_features` / `compile_features_readonly`): alias
    /// phrases collapse to their entity.
    Query,
    /// Title / match path (`match_features` / `match_features_dual`): alias phrases are
    /// additive.
    Title,
}

/// How a single non-alphanumeric character is treated during byte-cleaning
/// (configured via [`NormalizerBuilder::set_punct_class`]). `clean_into` consults the
/// class for every non-alphanumeric character; alphanumerics always pass through
/// lowercased.
///
/// The same table runs over queries (compile time) and titles (match time), so any
/// reclassification applies to *both* sides and the feature spaces stay aligned (the
/// shared-normalizer invariant, normalization.md §2). See docs/DECISIONS.md ADR-058.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PunctClass {
    /// Map the character to a space — a word boundary. The default for every character
    /// not otherwise classified (commas, brackets, raw whitespace, …).
    Split,
    /// Delete the character so the alphanumerics on either side **join** into one token
    /// (`O'Brien` -> `obrien`) — the punctuation-equivalence rule (ADR-058). Declaring
    /// mid-word `'`/`'`/`-` as `Fold` makes `O'Brien`, `O-Brien`, and `OBrien` collapse
    /// to the same token. Recall note: folding is the operator's informed trade — it
    /// gains the joined-form match and gives up the split-form one (`brien` alone no
    /// longer matches `O'Brien`); whichever is chosen, queries and titles fold
    /// identically, so the lossless cover still holds.
    Fold,
    /// Keep the character literally, in place, inside the surrounding token (`9.5`
    /// stays `9.5`). The default for `.` so half-grades survive byte-cleaning.
    Keep,
    /// Emit the character as its own standalone marker token (` c `). The default for
    /// `#` and `/` so the number logic can tell card-numbers (`#2`) and serials
    /// (`/199`, `3/10`) apart from grades.
    Marker,
}

/// Per-character punctuation classification consulted by byte-cleaning. The [`Default`]
/// reproduces the historical hardcoded behavior exactly (`.` kept, `#`/`/` markers,
/// everything else split), so a normalizer built without touching it is **byte-identical
/// to before ADR-058**.
///
/// ASCII characters resolve through a flat 128-entry array (branchless, no hashing on
/// the per-title hot path); the rare non-ASCII rule (e.g. the curly apostrophe `'`,
/// U+2019) falls back to a small map that stays empty unless one is registered.
#[derive(Debug, Clone)]
struct PunctTable {
    ascii: [PunctClass; 128],
    non_ascii: std::collections::HashMap<char, PunctClass>,
}

impl Default for PunctTable {
    fn default() -> Self {
        let mut ascii = [PunctClass::Split; 128];
        ascii['.' as usize] = PunctClass::Keep;
        ascii['#' as usize] = PunctClass::Marker;
        ascii['/' as usize] = PunctClass::Marker;
        Self {
            ascii,
            non_ascii: std::collections::HashMap::new(),
        }
    }
}

impl PunctTable {
    /// The class for `c`. Only ever called for non-alphanumeric characters (the
    /// alphanumeric fast path never reaches it), so the ASCII index is in range.
    #[inline]
    fn class_of(&self, c: char) -> PunctClass {
        let u = c as u32;
        if u < 128 {
            self.ascii[u as usize]
        } else {
            self.non_ascii.get(&c).copied().unwrap_or(PunctClass::Split)
        }
    }

    /// Override the class for a single character.
    fn set(&mut self, c: char, class: PunctClass) {
        let u = c as u32;
        if u < 128 {
            self.ascii[u as usize] = class;
        } else {
            self.non_ascii.insert(c, class);
        }
    }
}
