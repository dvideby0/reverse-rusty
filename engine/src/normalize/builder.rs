//! [`NormalizerBuilder`] — the off-hot-path construction surface for a
//! [`Normalizer`](super::Normalizer).
//!
//! Assembles the four vocabulary categories (phrases / synonyms / graders / grade
//! words) plus the byte-cleaning punctuation table, then `build()`s the daachorse
//! automaton and hands the populated fields to the `Normalizer`.

use super::{Normalizer, PhraseEntry, PunctClass, PunctTable};
use crate::dict::FeatureKind;
use daachorse::{DoubleArrayAhoCorasickBuilder, MatchKind};

/// Builder for assembling a [`Normalizer`](super::Normalizer) from custom vocabulary.
///
/// A normalizer needs four categories of vocabulary:
///
/// - **Phrases** — multiword token sequences mapped to canonical features via an
///   Aho-Corasick automaton (e.g. `["michael", "jordan"] → "player:michael_jordan"`).
/// - **Synonyms** — single-token aliases mapped to canonical features (e.g.
///   `"topps" → "brand:topps"`).
/// - **Graders** — tokens that trigger the grader/grade detection pipeline (e.g.
///   `"psa"`, `"bgs"`). When a grader token is seen, adjacent numbers become grades.
/// - **Grade words** — tokens that trigger grade-context mode (e.g. `"gem"`, `"mint"`).
///
/// # Example
///
/// ```
/// use reverse_rusty::normalize::NormalizerBuilder;
/// use reverse_rusty::dict::FeatureKind;
///
/// let norm = NormalizerBuilder::new()
///     .phrase(&["michael", "jordan"], "player:michael_jordan", FeatureKind::Player)
///     .synonym("topps", "brand:topps", FeatureKind::Brand)
///     .grader("psa")
///     .grade_word("gem")
///     .build()
///     .expect("automaton build");
/// ```
#[derive(Debug, Clone, Default)]
pub struct NormalizerBuilder {
    phrase_patterns: Vec<String>,
    phrase_entries: Vec<PhraseEntry>,
    synonyms: Vec<(String, String, FeatureKind)>,
    syn_index: std::collections::HashMap<String, usize>,
    graders: Vec<String>,
    grade_words: Vec<String>,
    /// Byte-cleaning punctuation classification (ADR-058). Defaults to the historical
    /// behavior, so a builder that never touches it yields a byte-identical normalizer.
    punct: PunctTable,
}

impl NormalizerBuilder {
    /// Create an empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a multiword phrase pattern. `tokens` are the space-separated words
    /// to match (lowercased, after diacritic folding). `feature` is the canonical
    /// feature name emitted on match. `kind` is the feature kind for the dictionary.
    pub fn add_phrase(&mut self, tokens: &[&str], feature: &str, kind: FeatureKind) {
        self.add_phrase_inner(tokens, feature, kind, false, false);
    }

    /// Like [`add_phrase`](Self::add_phrase) but **additive**: a match emits the phrase
    /// feature AND leaves the component tokens to also emit their own features, so a query
    /// referencing a component never loses the match. Used for corpus-learned phrases
    /// (ADR-053) to keep the recall-first contract.
    pub fn add_phrase_additive(&mut self, tokens: &[&str], feature: &str, kind: FeatureKind) {
        self.add_phrase_inner(tokens, feature, kind, true, false);
    }

    /// Register an **alias entity** phrase (ADR-061) — the Elasticsearch `synonym_graph`
    /// equivalent for multi-word synonyms. On the **title/match** side it is additive (emits the
    /// entity `feature` AND keeps the component tokens, so a component query still matches); on the
    /// **query/compile** side it collapses (emits only the entity `feature`, consuming components),
    /// so a query phrased with the multi-word form requires just the entity feature — which
    /// equivalence expansion (ADR-054) then widens to its synonyms. This gives bidirectional
    /// multi-word aliases while staying false-negative-safe (the title emits a superset of what the
    /// query requires).
    pub fn add_phrase_alias(&mut self, tokens: &[&str], feature: &str, kind: FeatureKind) {
        self.add_phrase_inner(tokens, feature, kind, false, true);
    }

    fn add_phrase_inner(
        &mut self,
        tokens: &[&str],
        feature: &str,
        kind: FeatureKind,
        additive: bool,
        alias: bool,
    ) {
        self.phrase_patterns.push(tokens.join(" "));
        self.phrase_entries.push(PhraseEntry {
            feature: feature.to_string(),
            kind,
            additive,
            alias,
        });
    }

    /// Fluent version of [`add_phrase`](Self::add_phrase).
    pub fn phrase(mut self, tokens: &[&str], feature: &str, kind: FeatureKind) -> Self {
        self.add_phrase(tokens, feature, kind);
        self
    }

    /// Register a single-token synonym. `token` is the lowercased input token;
    /// `canon` is the canonical feature name. Duplicate tokens are silently ignored
    /// (first registration wins).
    pub fn add_synonym(&mut self, token: &str, canon: &str, kind: FeatureKind) {
        if self.syn_index.contains_key(token) {
            return;
        }
        self.syn_index
            .insert(token.to_string(), self.synonyms.len());
        self.synonyms
            .push((token.to_string(), canon.to_string(), kind));
    }

    /// Fluent version of [`add_synonym`](Self::add_synonym).
    pub fn synonym(mut self, token: &str, canon: &str, kind: FeatureKind) -> Self {
        self.add_synonym(token, canon, kind);
        self
    }

    /// Register many single-token synonyms at once (bulk [`add_synonym`](Self::add_synonym)).
    /// For large alias tables maintained outside of code, prefer the Solr/Lucene-format file
    /// loader on [`Vocab`](crate::vocab::Vocab) (`extend_from_synonyms[_file]`, ADR-060), which
    /// also handles multi-token forms and FN-safe equivalence expansion.
    pub fn add_synonyms(&mut self, entries: &[(&str, &str, FeatureKind)]) {
        for &(token, canon, kind) in entries {
            self.add_synonym(token, canon, kind);
        }
    }

    /// Fluent version of [`add_synonyms`](Self::add_synonyms).
    pub fn synonyms(mut self, entries: &[(&str, &str, FeatureKind)]) -> Self {
        self.add_synonyms(entries);
        self
    }

    /// Register a grader keyword (e.g. `"psa"`, `"bgs"`). Grader tokens trigger
    /// grade detection: adjacent numbers become `grade:N` and `grader_grade:psaN`.
    pub fn add_grader(&mut self, name: &str) {
        self.graders.push(name.to_string());
    }

    /// Fluent version of [`add_grader`](Self::add_grader).
    pub fn grader(mut self, name: &str) -> Self {
        self.add_grader(name);
        self
    }

    /// Register a grade-context word (e.g. `"gem"`, `"mint"`). These tokens activate
    /// a short-lived grade-context window so that a following number is treated as a
    /// grade even without an explicit grader prefix.
    pub fn add_grade_word(&mut self, word: &str) {
        self.grade_words.push(word.to_string());
    }

    /// Fluent version of [`add_grade_word`](Self::add_grade_word).
    pub fn grade_word(mut self, word: &str) -> Self {
        self.add_grade_word(word);
        self
    }

    /// Classify a punctuation character for byte-cleaning (ADR-058). By default `.` is
    /// kept in place, `#`/`/` are standalone markers, and every other non-alphanumeric
    /// character becomes a word boundary ([`PunctClass::Split`]); override any of them
    /// here. The same table runs over queries and titles, so a reclassification applies
    /// to both sides and the feature spaces stay aligned.
    pub fn set_punct_class(&mut self, c: char, class: PunctClass) {
        self.punct.set(c, class);
    }

    /// Mark a character as **folding** — deleted during byte-cleaning so the
    /// alphanumerics on either side join into one token (`O'Brien` -> `obrien`). The
    /// punctuation-equivalence rule from ADR-058; shorthand for
    /// `set_punct_class(c, PunctClass::Fold)`.
    pub fn fold_punctuation(&mut self, c: char) {
        self.punct.set(c, PunctClass::Fold);
    }

    /// Batch form of [`fold_punctuation`](Self::fold_punctuation): mark every character
    /// in `chars` as folding. Convenient for a corpus's mid-word punctuation set, e.g.
    /// `&['\'', '\u{2019}', '-']` to collapse `O'Brien`/`O'Brien`/`O-Brien` to `obrien`.
    pub fn fold_punctuation_chars(&mut self, chars: &[char]) {
        for &c in chars {
            self.punct.set(c, PunctClass::Fold);
        }
    }

    /// Fluent version of [`set_punct_class`](Self::set_punct_class).
    pub fn punct(mut self, c: char, class: PunctClass) -> Self {
        self.set_punct_class(c, class);
        self
    }

    /// Consume the builder and construct a [`Normalizer`](super::Normalizer).
    ///
    /// Returns `Err` if the Aho-Corasick automaton cannot be built from the
    /// registered phrase patterns (e.g. degenerate patterns that daachorse
    /// cannot encode).
    pub fn build(self) -> Result<Normalizer, crate::error::NormalizerError> {
        let automaton = DoubleArrayAhoCorasickBuilder::new()
            .match_kind(MatchKind::LeftmostLongest)
            .build(&self.phrase_patterns)
            .map_err(|e| crate::error::NormalizerError::new(e.to_string()))?;

        Ok(Normalizer {
            automaton,
            phrase_entries: self.phrase_entries,
            graders: self.graders,
            synonyms: self.synonyms,
            syn_index: self.syn_index,
            grade_words: self.grade_words,
            punct: self.punct,
        })
    }
}
