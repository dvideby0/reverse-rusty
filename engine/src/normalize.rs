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

pub use builder::NormalizerBuilder;
pub use core::{fold_diacritic, Normalizer};

/// Metadata for a phrase pattern registered in the automaton.
#[derive(Debug, Clone)]
struct PhraseEntry {
    feature: String,
    kind: FeatureKind,
    /// When `false` (the default): a phrase match **consumes** its component tokens — only
    /// the phrase feature is emitted (collapse / entity-disambiguation, used by declared +
    /// hand-built vocab). When `true`: the phrase feature is emitted **in addition to** the
    /// component tokens (additive — the component features are still produced), so a query
    /// referencing a component does not lose the match. Corpus-learned phrases (ADR-053) are
    /// additive: this engine is a recall-first candidate generator, so a phrase must never
    /// drop a candidate a component query would have matched.
    additive: bool,
    /// **Alias entity** (ADR-061), the [`synonym_graph`](https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-synonym-graph-tokenfilter.html)
    /// equivalent: emitted **additively on the title/match side** (entity feature + component
    /// tokens kept — a component query still matches) but **collapsed on the query/compile side**
    /// (only the entity feature; components consumed). So a query phrased with the multi-word form
    /// requires just the entity feature, which equivalence expansion (ADR-054) then widens to its
    /// synonyms — giving bidirectional multi-word aliases. The asymmetry is in the *safe* direction
    /// (titles emit a superset of what queries require), so the lossless cover still holds; it
    /// mirrors ES, which applies multi-word synonyms at search time while the index keeps
    /// components. `alias` takes precedence over `additive`. Default `false`.
    alias: bool,
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
