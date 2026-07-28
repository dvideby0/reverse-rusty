//! [`NormalizerBuilder`] — the off-hot-path construction surface for a
//! [`Normalizer`](super::Normalizer).
//!
//! Assembles phrases and synonyms plus the byte-cleaning punctuation table (ADR-058)
//! and the number-context
//! word list (ADR-069), then `build()`s the daachorse automaton and hands the
//! populated fields to the `Normalizer`.

use super::{Normalizer, PhraseEntry, PhraseMode, PunctClass, PunctTable};
use crate::dict::FeatureKind;
use daachorse::{DoubleArrayAhoCorasickBuilder, MatchKind};

/// Builder for assembling a [`Normalizer`](super::Normalizer) from custom vocabulary.
///
/// A normalizer accepts two categories of vocabulary:
///
/// - **Phrases** — multiword token sequences mapped to canonical features via an
///   Aho-Corasick automaton (e.g. `["wireless", "mouse"] → "entity:wireless_mouse"`).
/// - **Synonyms** — single-token aliases mapped to canonical features (e.g.
///   `"acme" → "brand:acme"`).
///
/// # Example
///
/// ```
/// use reverse_rusty::normalize::NormalizerBuilder;
/// use reverse_rusty::dict::FeatureKind;
///
/// let norm = NormalizerBuilder::new()
///     .phrase(&["wireless", "mouse"], "entity:wireless_mouse", FeatureKind::Entity)
///     .synonym("acme", "brand:acme", FeatureKind::Brand)
///     .build()
///     .expect("automaton build");
/// ```
#[derive(Debug, Clone, Default)]
pub struct NormalizerBuilder {
    phrase_patterns: Vec<String>,
    phrase_entries: Vec<PhraseEntry>,
    synonyms: Vec<(String, String, FeatureKind)>,
    syn_index: std::collections::HashMap<String, usize>,
    /// Byte-cleaning punctuation classification (ADR-058). Defaults to the historical
    /// behavior, so a builder that never touches it yields a byte-identical normalizer.
    punct: PunctTable,
    /// Raw multi-word alias forms (ADR-061), cleaned + registered as alias-mode phrases at
    /// [`build`](Self::build) (after the punctuation table is final, so cleaning matches titles).
    alias_forms: Vec<String>,
    /// Number-context words. Empty by default; callers may declare tokens whose
    /// following number must remain generic instead of being typed as a year.
    number_context: Vec<String>,
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
        self.add_phrase_inner(tokens, feature, kind, PhraseMode::Collapse);
    }

    /// Like [`add_phrase`](Self::add_phrase) but **additive**: a match emits the phrase
    /// feature AND leaves the component tokens to also emit their own features, so a query
    /// referencing a component never loses the match. Used for corpus-learned phrases
    /// (ADR-053) to keep the recall-first contract.
    pub fn add_phrase_additive(&mut self, tokens: &[&str], feature: &str, kind: FeatureKind) {
        self.add_phrase_inner(tokens, feature, kind, PhraseMode::Additive);
    }

    /// Register a **multi-word alias** form (ADR-061): asymmetric by [`Side`](super::Side).
    /// On the query/compile side the phrase **collapses** to its single `feature` entity (so
    /// ADR-054 expansion can widen it to the alias group); on the title/match side it is
    /// **additive** (entity + components) and also participates in the title-side overlap
    /// superset, so nested/overlapping aliases (`new york` ⊂ `new york city`) are all found.
    pub fn add_phrase_alias(&mut self, tokens: &[&str], feature: &str, kind: FeatureKind) {
        self.add_phrase_inner(tokens, feature, kind, PhraseMode::Alias);
    }

    /// Register a multi-word alias by its **raw form string** (ADR-061). Cleaned + tokenized at
    /// [`build`](Self::build) with the final punctuation table (so it tokenizes exactly as a
    /// title does), then registered as an alias-mode phrase emitting the derived entity
    /// `term:<tokens joined by '_'>`. A form that cleans to fewer than two tokens registers no
    /// phrase (it is a single-token alias, handled by the equivalence map). When the cleaned
    /// tokens already match a declared/corpus phrase, that entry is upgraded to alias mode and
    /// keeps its feature, so resolution and emission stay consistent.
    pub fn add_alias_form(&mut self, form: &str) {
        self.alias_forms.push(form.to_string());
    }

    /// Fold the pending raw alias forms into the phrase tables. Called once at the start of
    /// [`build`](Self::build), after the punctuation table is final.
    fn register_alias_phrases(&mut self) {
        let forms = std::mem::take(&mut self.alias_forms);
        for form in &forms {
            let toks = super::core::alias_form_tokens(&self.punct, form);
            if toks.len() < 2 {
                continue; // single-token / empty: the equivalence map handles it, not a phrase
            }
            let pattern = toks.join(" ");
            if let Some(i) = self.phrase_patterns.iter().position(|p| *p == pattern) {
                // A declared/corpus phrase over the same tokens already exists: upgrade it to
                // alias mode (collapse-on-query wins) but keep its feature.
                self.phrase_entries[i].mode = PhraseMode::Alias;
            } else {
                let entity = format!("term:{}", toks.join("_"));
                self.phrase_patterns.push(pattern);
                self.phrase_entries.push(PhraseEntry {
                    feature: entity,
                    kind: FeatureKind::Generic,
                    mode: PhraseMode::Alias,
                });
            }
        }
    }

    fn add_phrase_inner(
        &mut self,
        tokens: &[&str],
        feature: &str,
        kind: FeatureKind,
        mode: PhraseMode,
    ) {
        self.phrase_patterns.push(tokens.join(" "));
        self.phrase_entries.push(PhraseEntry {
            feature: feature.to_string(),
            kind,
            mode,
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

    /// Replace the **number-context word list** (ADR-069): a number token immediately after
    /// one of these words is demoted to a generic term (`model 1995` -> `term:1995`), rather
    /// than typed as a year. The default is empty, so number typing is position-insensitive.
    /// Entries are matched against single cleaned tokens (lowercased at build); the same
    /// list runs over queries and titles, so the feature spaces stay aligned (§2).
    pub fn set_number_context_words(&mut self, words: &[&str]) {
        self.number_context = words.iter().map(|w| w.to_ascii_lowercase()).collect();
    }

    /// Fluent version of [`set_number_context_words`](Self::set_number_context_words).
    pub fn number_context_words(mut self, words: &[&str]) -> Self {
        self.set_number_context_words(words);
        self
    }

    /// Consume the builder and construct a [`Normalizer`](super::Normalizer).
    ///
    /// Returns `Err` if the Aho-Corasick automaton cannot be built from the
    /// registered phrase patterns (e.g. degenerate patterns that daachorse
    /// cannot encode).
    pub fn build(mut self) -> Result<Normalizer, crate::error::NormalizerError> {
        // ADR-061: fold raw alias forms into the phrase tables now that the punctuation table is
        // final, so they clean/tokenize exactly as a title does.
        self.register_alias_phrases();

        let automaton = DoubleArrayAhoCorasickBuilder::new()
            .match_kind(MatchKind::LeftmostLongest)
            .build(&self.phrase_patterns)
            .map_err(|e| crate::error::NormalizerError::new(e.to_string()))?;

        let has_multiword_aliases = self
            .phrase_entries
            .iter()
            .any(|entry| entry.mode == PhraseMode::Alias);
        // The overlapping automaton covers every registered phrase. ADR-061
        // consults it only when `has_multiword_aliases`; ADR-120 consults it for
        // every phrase-aware title graph.
        let phrase_overlap = build_phrase_overlap(&self.phrase_patterns, &self.phrase_entries)?;

        Ok(Normalizer {
            automaton,
            phrase_entries: self.phrase_entries,
            phrase_overlap,
            has_multiword_aliases,
            synonyms: self.synonyms,
            syn_index: self.syn_index,
            punct: self.punct,
            number_context: self.number_context,
        })
    }
}

/// Build the overlapping (`MatchKind::Standard`) automaton used by the title
/// positive views. Returns `None` only when no phrase is registered.
///
/// The automaton covers **every** phrase (alias AND non-alias). This is the
/// codex-R6 fix for ADR-061: adding an alias to the shared
/// leftmost-longest automaton can *displace* an overlapping non-alias phrase from the canonical
/// `N(T)` parse (e.g. activating `new york` makes `new york city` no longer emit a pre-existing
/// `york city` entity), so `P(T)` must re-include **every** phrase entity present — alias and
/// displaced non-alias alike — or a query on the displaced phrase becomes a false negative.
/// ADR-120 additionally needs the same union even with no aliases: an ordinary
/// collapse phrase may otherwise hide a quoted component or overlapping entity.
/// The overlap pass only ever adds entities to the applicable positive view.
/// Patterns are deduped (a duplicate would make daachorse reject the build).
fn build_phrase_overlap(
    patterns: &[String],
    entries: &[PhraseEntry],
) -> Result<Option<super::core::PhraseOverlap>, crate::error::NormalizerError> {
    if patterns.is_empty() {
        return Ok(None);
    }
    let mut pats: Vec<String> = Vec::new();
    let mut feats: Vec<(String, FeatureKind)> = Vec::new();
    let mut entry_idx: Vec<usize> = Vec::new();
    let mut token_lens: Vec<u32> = Vec::new();
    for (i, (pat, entry)) in patterns.iter().zip(entries).enumerate() {
        if !pats.iter().any(|p| p == pat) {
            pats.push(pat.clone());
            feats.push((entry.feature.clone(), entry.kind));
            entry_idx.push(i);
            token_lens.push(u32::try_from(pat.split_whitespace().count()).unwrap_or(u32::MAX));
        }
    }
    let automaton = DoubleArrayAhoCorasickBuilder::new()
        .match_kind(MatchKind::Standard)
        .build(&pats)
        .map_err(|e| crate::error::NormalizerError::new(e.to_string()))?;
    Ok(Some(super::core::PhraseOverlap {
        automaton,
        entries: feats,
        entry_idx,
        token_lens,
    }))
}
