//! [`NormalizerBuilder`] — the off-hot-path construction surface for a
//! [`Normalizer`](super::Normalizer).
//!
//! Assembles the four vocabulary categories (phrases / synonyms / graders / grade
//! words) plus the byte-cleaning punctuation table (ADR-058) and the number-context
//! word list (ADR-069), then `build()`s the daachorse automaton and hands the
//! populated fields to the `Normalizer`.

use super::{Normalizer, PhraseEntry, PhraseMode, PunctClass, PunctTable};
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
    /// Raw multi-word alias forms (ADR-061), cleaned + registered as alias-mode phrases at
    /// [`build`](Self::build) (after the punctuation table is final, so cleaning matches titles).
    alias_forms: Vec<String>,
    /// Number-context words (ADR-069). `None` (the default) resolves to `["pop"]` at
    /// [`build`](Self::build) — the historical hard-coded rule, byte-identical.
    number_context: Option<Vec<String>>,
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

    /// Replace the **number-context word list** (ADR-069): a number token immediately after
    /// one of these words is demoted to a generic term (`pop 1995` -> `term:1995`), never
    /// typed as a year or grade. The default — when this is never called — is `["pop"]`,
    /// the historical hard-coded population rule, byte-identical. Passing an **empty** list
    /// disables the rule entirely (the percolator-parity mode, ADR-064 item 3): number
    /// typing becomes position-insensitive, so a 4-digit year is `year:N` everywhere.
    /// Entries are matched against single cleaned tokens (lowercased at build); the same
    /// list runs over queries and titles, so the feature spaces stay aligned (§2).
    pub fn set_number_context_words(&mut self, words: &[&str]) {
        self.number_context = Some(words.iter().map(|w| w.to_ascii_lowercase()).collect());
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

        // ADR-061: a second, **overlapping** automaton over the alias phrases only, used on
        // the title side to build the positive superset `P(T)` (every nested/overlapping
        // alias entity, not just leftmost-longest). `None` when there are no alias phrases,
        // so the default path stays single-view and byte-identical.
        let alias_overlap = build_alias_overlap(&self.phrase_patterns, &self.phrase_entries)?;

        Ok(Normalizer {
            automaton,
            phrase_entries: self.phrase_entries,
            alias_overlap,
            graders: self.graders,
            synonyms: self.synonyms,
            syn_index: self.syn_index,
            grade_words: self.grade_words,
            punct: self.punct,
            // ADR-069: unset resolves to the historical `pop` rule, byte-identical.
            number_context: self
                .number_context
                .unwrap_or_else(|| vec!["pop".to_string()]),
        })
    }
}

/// Build the overlapping (`MatchKind::Standard`) automaton for the title positive view `P(T)`
/// (ADR-061). Returns `None` unless ≥1 **alias-mode** phrase is registered (otherwise the title is
/// single-view and byte-identical to pre-ADR-061).
///
/// When alias phrases ARE present, the automaton covers **every** phrase (alias AND non-alias),
/// not just the alias subset. This is the codex-R6 fix: adding an alias to the shared
/// leftmost-longest automaton can *displace* an overlapping non-alias phrase from the canonical
/// `N(T)` parse (e.g. activating `new york` makes `new york city` no longer emit a pre-existing
/// `york city` entity), so `P(T)` must re-include **every** phrase entity present — alias and
/// displaced non-alias alike — or a query on the displaced phrase becomes a false negative. The
/// overlap pass only ever *adds* entities to the positive view, so this is recall-safe.
/// Patterns are deduped (a duplicate would make daachorse reject the build).
fn build_alias_overlap(
    patterns: &[String],
    entries: &[PhraseEntry],
) -> Result<Option<super::core::AliasOverlap>, crate::error::NormalizerError> {
    if !entries.iter().any(|e| e.mode == PhraseMode::Alias) {
        return Ok(None);
    }
    let mut pats: Vec<String> = Vec::new();
    let mut feats: Vec<(String, FeatureKind)> = Vec::new();
    let mut entry_idx: Vec<usize> = Vec::new();
    for (i, (pat, entry)) in patterns.iter().zip(entries).enumerate() {
        if !pats.iter().any(|p| p == pat) {
            pats.push(pat.clone());
            feats.push((entry.feature.clone(), entry.kind));
            entry_idx.push(i);
        }
    }
    let automaton = DoubleArrayAhoCorasickBuilder::new()
        .match_kind(MatchKind::Standard)
        .build(&pats)
        .map_err(|e| crate::error::NormalizerError::new(e.to_string()))?;
    Ok(Some(super::core::AliasOverlap {
        automaton,
        entries: feats,
        entry_idx,
    }))
}
