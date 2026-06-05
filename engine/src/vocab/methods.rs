//! `impl Vocab` — building a [`Normalizer`], merging vocabs, the management
//! accessors (synonyms / phrases / graders / grade words / punctuation /
//! equivalences), equivalence resolution to an [`EquivMap`], and JSON
//! (de)serialization. Admin/build-time only — off the match hot path.

use std::io;
use std::path::Path;

use super::{PhraseEntry, PunctRule, SynonymEntry, Vocab};
use crate::dict::{Dict, EquivMap, FeatureId, FeatureKind};
use crate::normalize::{Normalizer, NormalizerBuilder, PunctClass};
use crate::util::fast_map;

/// Merge groups that share any member into disjoint **transitive** groups (so `[a,b]` + `[b,c]`
/// become `[a,b,c]`). Each output group is sorted + deduped. Used by
/// [`Vocab::resolve_equivalences`] so overlapping declared/learned equivalences collapse into one
/// transitive class instead of order-dependently overwriting a shared member.
fn merge_overlapping_groups(groups: Vec<Vec<FeatureId>>) -> Vec<Vec<FeatureId>> {
    let mut result: Vec<Vec<FeatureId>> = Vec::new();
    for g in groups {
        let mut overlap: Vec<usize> = Vec::new();
        for (i, r) in result.iter().enumerate() {
            if g.iter().any(|f| r.contains(f)) {
                overlap.push(i);
            }
        }
        if overlap.is_empty() {
            result.push(g);
        } else {
            let mut merged = g;
            // Remove the highest index first so the remaining indices stay valid.
            for &i in overlap.iter().rev() {
                merged.extend(result.remove(i));
            }
            merged.sort_unstable();
            merged.dedup();
            result.push(merged);
        }
    }
    result
}

// ── Vocab methods ───────────────────────────────────────────────────────────

impl Vocab {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a [`Normalizer`] from this vocabulary.
    pub fn to_normalizer(&self) -> Result<Normalizer, crate::error::NormalizerError> {
        let mut b = NormalizerBuilder::new();

        for entry in &self.phrases {
            let toks: Vec<&str> = entry
                .tokens
                .iter()
                .map(std::string::String::as_str)
                .collect();
            if entry.additive {
                b.add_phrase_additive(&toks, &entry.canonical, entry.kind.into());
            } else {
                b.add_phrase(&toks, &entry.canonical, entry.kind.into());
            }
        }
        for entry in &self.synonyms {
            b.add_synonym(&entry.token, &entry.canonical, entry.kind.into());
        }
        for g in &self.graders {
            b.add_grader(g);
        }
        for w in &self.grade_words {
            b.add_grade_word(w);
        }
        for rule in &self.punctuation {
            b.set_punct_class(rule.ch, rule.class.into());
        }

        b.build()
    }

    /// Merge another vocab into this one. Entries from `other` are appended;
    /// duplicate synonyms (same token) are skipped (first wins).
    pub fn merge(&mut self, other: &Vocab) {
        let existing_syns: std::collections::HashSet<String> =
            self.synonyms.iter().map(|e| e.token.clone()).collect();
        for entry in &other.synonyms {
            if !existing_syns.contains(&entry.token) {
                self.synonyms.push(entry.clone());
            }
        }

        let existing_phrases: std::collections::HashSet<Vec<String>> =
            self.phrases.iter().map(|e| e.tokens.clone()).collect();
        for entry in &other.phrases {
            if !existing_phrases.contains(&entry.tokens) {
                self.phrases.push(entry.clone());
            }
        }

        for g in &other.graders {
            if !self.graders.contains(g) {
                self.graders.push(g.clone());
            }
        }
        for w in &other.grade_words {
            if !self.grade_words.contains(w) {
                self.grade_words.push(w.clone());
            }
        }
        for grp in &other.equivalences {
            if !self.equivalences.contains(grp) {
                self.equivalences.push(grp.clone());
            }
        }
        for rule in &other.punctuation {
            if !self.punctuation.iter().any(|r| r.ch == rule.ch) {
                self.punctuation.push(rule.clone());
            }
        }
    }

    // ── Punctuation management (ADR-058) ────────────────────────────────

    /// Reclassify a byte-cleaning punctuation character (a later call for the same `ch`
    /// replaces the earlier one). See [`PunctClass`] for the behaviors.
    pub fn set_punct_class(&mut self, ch: char, class: PunctClass) {
        let class = class.into();
        if let Some(rule) = self.punctuation.iter_mut().find(|r| r.ch == ch) {
            rule.class = class;
        } else {
            self.punctuation.push(PunctRule { ch, class });
        }
    }

    /// Mark `ch` as **folding** — deleted during byte-cleaning so its neighbors join into
    /// one token (`O'Brien` -> `obrien`, ADR-058). Shorthand for
    /// `set_punct_class(ch, PunctClass::Fold)`.
    pub fn fold_punctuation(&mut self, ch: char) {
        self.set_punct_class(ch, PunctClass::Fold);
    }

    /// The registered punctuation rules, in declaration order.
    pub fn punctuation(&self) -> &[PunctRule] {
        &self.punctuation
    }

    // ── Synonym management ──────────────────────────────────────────────

    pub fn add_synonym(&mut self, token: &str, canonical: &str, kind: FeatureKind) {
        if self.synonyms.iter().any(|e| e.token == token) {
            return;
        }
        self.synonyms.push(SynonymEntry {
            token: token.to_string(),
            canonical: canonical.to_string(),
            kind: kind.into(),
        });
    }

    pub fn remove_synonym(&mut self, token: &str) -> bool {
        let before = self.synonyms.len();
        self.synonyms.retain(|e| e.token != token);
        self.synonyms.len() < before
    }

    pub fn get_synonym(&self, token: &str) -> Option<&SynonymEntry> {
        self.synonyms.iter().find(|e| e.token == token)
    }

    pub fn synonyms(&self) -> &[SynonymEntry] {
        &self.synonyms
    }

    // ── Phrase management ───────────────────────────────────────────────

    pub fn add_phrase(&mut self, tokens: &[&str], canonical: &str, kind: FeatureKind) {
        self.add_phrase_with(tokens, canonical, kind, false);
    }

    /// Like [`add_phrase`](Self::add_phrase) but **additive** (ADR-053): a match emits the
    /// phrase feature AND keeps the component features, so a query referencing a component
    /// never loses the match (recall-first). Used for corpus-learned phrases.
    pub fn add_phrase_additive(&mut self, tokens: &[&str], canonical: &str, kind: FeatureKind) {
        self.add_phrase_with(tokens, canonical, kind, true);
    }

    fn add_phrase_with(
        &mut self,
        tokens: &[&str],
        canonical: &str,
        kind: FeatureKind,
        additive: bool,
    ) {
        let tok_vec: Vec<String> = tokens
            .iter()
            .map(std::string::ToString::to_string)
            .collect();
        if self.phrases.iter().any(|e| e.tokens == tok_vec) {
            return;
        }
        self.phrases.push(PhraseEntry {
            tokens: tok_vec,
            canonical: canonical.to_string(),
            kind: kind.into(),
            additive,
        });
    }

    pub fn remove_phrase(&mut self, tokens: &[&str]) -> bool {
        let tok_vec: Vec<String> = tokens
            .iter()
            .map(std::string::ToString::to_string)
            .collect();
        let before = self.phrases.len();
        self.phrases.retain(|e| e.tokens != tok_vec);
        self.phrases.len() < before
    }

    pub fn phrases(&self) -> &[PhraseEntry] {
        &self.phrases
    }

    // ── Grader management ───────────────────────────────────────────────

    pub fn add_grader(&mut self, name: &str) {
        if !self.graders.iter().any(|g| g == name) {
            self.graders.push(name.to_string());
        }
    }

    pub fn remove_grader(&mut self, name: &str) -> bool {
        let before = self.graders.len();
        self.graders.retain(|g| g != name);
        self.graders.len() < before
    }

    pub fn graders(&self) -> &[String] {
        &self.graders
    }

    // ── Grade word management ───────────────────────────────────────────

    pub fn add_grade_word(&mut self, word: &str) {
        if !self.grade_words.iter().any(|w| w == word) {
            self.grade_words.push(word.to_string());
        }
    }

    pub fn remove_grade_word(&mut self, word: &str) -> bool {
        let before = self.grade_words.len();
        self.grade_words.retain(|w| w != word);
        self.grade_words.len() < before
    }

    pub fn grade_words(&self) -> &[String] {
        &self.grade_words
    }

    // ── Equivalence management (ADR-054) ────────────────────────────────

    /// Declare an equivalence group: surface forms treated as the same entity, applied
    /// via FN-safe expansion. A duplicate group (same forms, same order) is skipped.
    pub fn add_equivalence(&mut self, forms: &[&str]) {
        let grp: Vec<String> = forms.iter().map(|s| (*s).to_string()).collect();
        if grp.len() >= 2 && !self.equivalences.contains(&grp) {
            self.equivalences.push(grp);
        }
    }

    pub fn equivalences(&self) -> &[Vec<String>] {
        &self.equivalences
    }

    /// Resolve the declared/learned equivalence groups to a compile-time [`EquivMap`]
    /// (member `FeatureId` → its full group) against a normalizer + dict. Each form is
    /// resolved to its feature(s) via the read-only compile path (so a form absent from a
    /// frozen dict still gets a stable synthetic id, ADR-046); a form that does not resolve
    /// to exactly ONE feature is skipped (an equivalence is entity↔entity, so a multi-token
    /// form should be a glued phrase first), and a group needs ≥2 distinct features to count.
    /// The result is installed on the dict ([`Dict::set_equivalences`]) so `extract`/
    /// `extract_readonly` expand queries through it.
    pub fn resolve_equivalences(&self, norm: &Normalizer, dict: &Dict) -> EquivMap {
        let mut lc = String::new();
        // 1. Resolve each declared group's forms to a feature set.
        let mut groups: Vec<Vec<FeatureId>> = Vec::new();
        for group in &self.equivalences {
            let mut feats: Vec<FeatureId> = Vec::with_capacity(group.len());
            for form in group {
                let fs = norm.compile_features_readonly(form, dict, &mut lc);
                if fs.len() == 1 {
                    feats.push(fs[0]);
                }
            }
            feats.sort_unstable();
            feats.dedup();
            if feats.len() >= 2 {
                groups.push(feats);
            }
        }
        // 2. Merge groups that share any feature into one transitive group, so overlapping
        //    declarations `[a,b]` + `[b,c]` become `{a,b,c}` (an equivalence is transitive) —
        //    otherwise a shared member would be order-dependently overwritten.
        let merged = merge_overlapping_groups(groups);
        // 3. Map each member -> its full (merged) group.
        let mut map: EquivMap = fast_map();
        for g in &merged {
            for &f in g {
                map.insert(f, g.clone());
            }
        }
        map
    }

    // ── Serialization ───────────────────────────────────────────────────

    pub fn save_json(&self, path: &Path) -> io::Result<()> {
        let json = serde_json::to_string_pretty(self).map_err(io::Error::other)?;
        std::fs::write(path, json)
    }

    pub fn load_json(path: &Path) -> io::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        serde_json::from_str(&content).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Number of entries (synonyms + phrases + graders + grade words).
    pub fn len(&self) -> usize {
        self.synonyms.len() + self.phrases.len() + self.graders.len() + self.grade_words.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
