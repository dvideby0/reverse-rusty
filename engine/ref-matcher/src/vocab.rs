//! [`RefVocab`] — the reference's OWN plain-data vocabulary.
//!
//! This is deliberately a separate type from `reverse_rusty::vocab::Vocab`: the reference must not
//! depend on the engine. The differential harness builds BOTH a `Vocab` (for the engine) and a
//! `RefVocab` (for the reference) from one neutral description, so the same phrases / synonyms /
//! graders / aliases / equivalences drive both sides while only the normalization *logic* differs.

use crate::clean::{PunctClass, PunctTable};

/// How a registered phrase treats its component tokens (mirrors the engine's `PhraseMode`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PhraseMode {
    /// Consume the components — only the entity feature survives (manual multiword phrases).
    Collapse,
    /// Emit the entity feature AND keep the components (corpus-learned phrases, ADR-053).
    Additive,
    /// Asymmetric (ADR-061): collapse on the query side, additive on the title side.
    Alias,
}

/// A multi-word phrase: its cleaned token sequence -> a canonical entity feature.
#[derive(Clone, Debug)]
pub struct RefPhrase {
    /// The cleaned tokens the phrase matches (e.g. `["upper","deck"]`).
    pub tokens: Vec<String>,
    /// The canonical entity feature emitted (e.g. `term:upper_deck`), used verbatim.
    pub feature: String,
    pub mode: PhraseMode,
}

/// A single-token synonym: `token` -> a canonical feature (e.g. `rc` -> `term:rookie`).
#[derive(Clone, Debug)]
pub struct RefSynonym {
    pub token: String,
    pub canonical: String,
}

/// The reference vocabulary. Construct with [`RefVocab::default_vocab`] then the builder methods,
/// or set the public fields directly from the harness.
#[derive(Clone, Debug)]
pub struct RefVocab {
    pub phrases: Vec<RefPhrase>,
    pub synonyms: Vec<RefSynonym>,
    pub graders: Vec<String>,
    pub grade_words: Vec<String>,
    /// A number immediately after one of these tokens is demoted to a generic term (ADR-069).
    /// Default `["pop"]`; empty = parity mode (position-insensitive number typing).
    pub number_context: Vec<String>,
    /// Equivalence groups as **forms** (surface strings). At extract time a required feature that
    /// resolves from one form in a group is widened required -> any-of over the group (ADR-054).
    pub equivalences: Vec<Vec<String>>,
    pub punct: PunctTable,
}

impl RefVocab {
    /// The empty default vocabulary: no phrases / synonyms / graders / grade words / equivalences,
    /// `number_context = ["pop"]`, and the default punctuation table. This is the exact shape of
    /// the engine's `Normalizer::default_vocab()` (an empty `NormalizerBuilder`), under which
    /// graders never fire and `psa10` stays a single generic `term:psa10`.
    #[must_use]
    pub fn default_vocab() -> Self {
        RefVocab {
            phrases: Vec::new(),
            synonyms: Vec::new(),
            graders: Vec::new(),
            grade_words: Vec::new(),
            number_context: vec!["pop".to_string()],
            equivalences: Vec::new(),
            punct: PunctTable::new(),
        }
    }

    /// Register a grader keyword (lowercased), e.g. `psa`, `bgs`, `sgc`.
    #[must_use]
    pub fn grader(mut self, name: &str) -> Self {
        self.graders.push(name.to_ascii_lowercase());
        self
    }

    /// Register a grade-context word (lowercased), e.g. `gem`, `mint`, `pristine`.
    #[must_use]
    pub fn grade_word(mut self, word: &str) -> Self {
        self.grade_words.push(word.to_ascii_lowercase());
        self
    }

    /// Register a single-token synonym `token` -> `canonical`.
    #[must_use]
    pub fn synonym(mut self, token: &str, canonical: &str) -> Self {
        self.synonyms.push(RefSynonym {
            token: token.to_ascii_lowercase(),
            canonical: canonical.to_string(),
        });
        self
    }

    /// Register a phrase from a raw form (cleaned into tokens under this vocab's punct table).
    #[must_use]
    pub fn phrase(mut self, form: &str, feature: &str, mode: PhraseMode) -> Self {
        let tokens = crate::clean::clean_tokens(form, &self.punct);
        if !tokens.is_empty() {
            self.phrases.push(RefPhrase {
                tokens,
                feature: feature.to_string(),
                mode,
            });
        }
        self
    }

    /// Register an equivalence group from its surface forms.
    #[must_use]
    pub fn equivalence(mut self, forms: &[&str]) -> Self {
        self.equivalences
            .push(forms.iter().map(|f| (*f).to_string()).collect());
        self
    }

    /// Reclassify a punctuation character (ADR-058), e.g. `'`/`-` as `Fold`.
    #[must_use]
    pub fn fold_punct(mut self, ch: char) -> Self {
        self.punct.set(ch, PunctClass::Fold);
        self
    }

    /// Set the number-context word list (ADR-069). Empty = parity mode.
    #[must_use]
    pub fn number_context(mut self, words: &[&str]) -> Self {
        self.number_context = words.iter().map(|w| w.to_ascii_lowercase()).collect();
        self
    }

    /// True if any phrase is registered in [`Alias`](PhraseMode::Alias) mode — the title then has a
    /// distinct positive view `P(T)` (ADR-061). Mirrors `Normalizer::has_multiword_aliases`.
    #[must_use]
    pub fn has_multiword_aliases(&self) -> bool {
        self.phrases.iter().any(|p| p.mode == PhraseMode::Alias)
    }
}

impl Default for RefVocab {
    fn default() -> Self {
        Self::default_vocab()
    }
}
