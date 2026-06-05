//! Vocabulary: learned synonyms from query any-of groups + manual management.
//!
//! Invariant: A Vocab produces a deterministic Normalizer; the same Vocab
//!   always yields the same feature space
//! Hot path: no — vocab operations are admin/build-time only
//!
//! This file holds the serializable type *definitions* (the [`Vocab`] document and
//! its entry/enum mirrors); the behavior is split into focused submodules so each
//! concern is self-contained:
//!   - [`learn`]   — corpus learners + composers that *build* a `Vocab`
//!     (`learn_from_queries`, `learn_equivalences_from_queries`, `CorpusLearnConfig`,
//!     `learn_vocab_from_corpus`)
//!   - [`methods`] — `impl Vocab`: normalizer build, merge, the management accessors,
//!     equivalence resolution, and JSON (de)serialization

use serde::{Deserialize, Serialize};

use crate::dict::FeatureKind;
use crate::normalize::PunctClass;

mod learn;
mod methods;
mod synonyms;

#[cfg(test)]
mod tests;

pub use learn::{
    learn_equivalences_from_queries, learn_from_queries, learn_vocab_from_corpus, CorpusLearnConfig,
};
pub use synonyms::{parse_synonyms, SynonymLoadError, SynonymLoadStats, SynonymParseError};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Vocab {
    #[serde(default)]
    synonyms: Vec<SynonymEntry>,
    #[serde(default)]
    phrases: Vec<PhraseEntry>,
    #[serde(default)]
    graders: Vec<String>,
    #[serde(default)]
    grade_words: Vec<String>,
    /// Learned/declared equivalence groups (ADR-054): each inner vec is a set of surface
    /// forms treated as the same entity (e.g. `["ud", "upper deck"]`). Applied via
    /// **expansion, not collapse** — a query requiring one form is widened to an any-of over
    /// the group's features, so it matches a title bearing any form, FN-safe. Distinct from
    /// `synonyms` (which collapse a form to a canonical via the normalizer).
    #[serde(default)]
    equivalences: Vec<Vec<String>>,
    /// Per-character byte-cleaning punctuation rules (ADR-058). Each rule reclassifies one
    /// character in the shared normalizer's `clean_into` pass — e.g. `{ch: '\'', class: fold}`
    /// makes `O'Brien` collapse to `obrien`. Empty (the default, and the shape of old vocab
    /// JSON that predates the field) ⇒ the historical behavior, byte-identical. The same
    /// table runs over queries and titles, so the feature spaces stay aligned (§2).
    #[serde(default)]
    punctuation: Vec<PunctRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SynonymEntry {
    pub token: String,
    pub canonical: String,
    #[serde(default = "default_kind")]
    pub kind: FeatureKindSer,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhraseEntry {
    pub tokens: Vec<String>,
    pub canonical: String,
    #[serde(default = "default_kind")]
    pub kind: FeatureKindSer,
    /// When true the phrase is applied **additively** — a match emits the phrase feature
    /// AND keeps the component features, so a query referencing a component never loses the
    /// match (the recall-first contract). Corpus-learned phrases (ADR-053) set this; declared
    /// / any-of-learned phrases default to `false` (collapse). Old vocab JSON without the
    /// field deserializes to `false`, preserving prior behavior.
    #[serde(default)]
    pub additive: bool,
    /// When true the phrase is an **alias entity** (ADR-061, the ES `synonym_graph` equivalent):
    /// additive on the title side but collapsed on the query side, so a multi-word alias form
    /// resolves to a single entity feature that equivalence expansion can widen to its synonyms.
    /// Set by the synonym-file loader for multi-token forms. Takes precedence over `additive`.
    /// Old vocab JSON without the field deserializes to `false`, preserving prior behavior.
    #[serde(default)]
    pub alias: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FeatureKindSer {
    Year,
    Brand,
    Player,
    Category,
    Grader,
    Grade,
    GraderGrade,
    Flag,
    Generic,
}

fn default_kind() -> FeatureKindSer {
    FeatureKindSer::Generic
}

impl From<FeatureKindSer> for FeatureKind {
    fn from(k: FeatureKindSer) -> Self {
        match k {
            FeatureKindSer::Year => FeatureKind::Year,
            FeatureKindSer::Brand => FeatureKind::Brand,
            FeatureKindSer::Player => FeatureKind::Player,
            FeatureKindSer::Category => FeatureKind::Category,
            FeatureKindSer::Grader => FeatureKind::Grader,
            FeatureKindSer::Grade => FeatureKind::Grade,
            FeatureKindSer::GraderGrade => FeatureKind::GraderGrade,
            FeatureKindSer::Flag => FeatureKind::Flag,
            FeatureKindSer::Generic => FeatureKind::Generic,
        }
    }
}

impl From<FeatureKind> for FeatureKindSer {
    fn from(k: FeatureKind) -> Self {
        match k {
            FeatureKind::Year => FeatureKindSer::Year,
            FeatureKind::Brand => FeatureKindSer::Brand,
            FeatureKind::Player => FeatureKindSer::Player,
            FeatureKind::Category => FeatureKindSer::Category,
            FeatureKind::Grader => FeatureKindSer::Grader,
            FeatureKind::Grade => FeatureKindSer::Grade,
            FeatureKind::GraderGrade => FeatureKindSer::GraderGrade,
            FeatureKind::Flag => FeatureKindSer::Flag,
            FeatureKind::Generic => FeatureKindSer::Generic,
        }
    }
}

/// Serializable mirror of [`PunctClass`] (ADR-058). Snake-cased in JSON: `split` / `fold`
/// / `keep` / `marker`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PunctClassSer {
    Split,
    Fold,
    Keep,
    Marker,
}

impl From<PunctClassSer> for PunctClass {
    fn from(c: PunctClassSer) -> Self {
        match c {
            PunctClassSer::Split => PunctClass::Split,
            PunctClassSer::Fold => PunctClass::Fold,
            PunctClassSer::Keep => PunctClass::Keep,
            PunctClassSer::Marker => PunctClass::Marker,
        }
    }
}

impl From<PunctClass> for PunctClassSer {
    fn from(c: PunctClass) -> Self {
        match c {
            PunctClass::Split => PunctClassSer::Split,
            PunctClass::Fold => PunctClassSer::Fold,
            PunctClass::Keep => PunctClassSer::Keep,
            PunctClass::Marker => PunctClassSer::Marker,
        }
    }
}

/// One byte-cleaning punctuation rule (ADR-058): reclassify a single character `ch` to
/// `class` in the shared normalizer. JSON shape: `{ "ch": "'", "class": "fold" }`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PunctRule {
    pub ch: char,
    pub class: PunctClassSer,
}
