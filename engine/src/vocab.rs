//! Vocabulary: learned synonyms from query any-of groups + manual management.
//!
//! Invariant: A Vocab produces a deterministic Normalizer; the same Vocab
//!   always yields the same feature space
//! Hot path: no — vocab operations are admin/build-time only

use std::collections::HashMap;
use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::dict::FeatureKind;
use crate::dsl::{self, Atom};
use crate::normalize::{Normalizer, NormalizerBuilder};

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

// ── Learning ────────────────────────────────────────────────────────────────

/// Learn synonym relationships from query any-of groups.
///
/// Scans each query's DSL, finds any-of groups, and treats members of the
/// same group as user-declared equivalents. Only keeps relationships that
/// appear in at least `min_count` distinct queries.
pub fn learn_from_queries(queries: &[(u64, String)], min_count: usize) -> Vocab {
    // Collect (canonical, alias) pairs with occurrence counts.
    // For each any-of group, pick the canonical (longest member, ties broken
    // lexicographically) and map every other member to it.
    let mut pair_counts: HashMap<(String, String), usize> = HashMap::new();

    for (_id, text) in queries {
        let Ok(ast) = dsl::parse(text) else {
            continue;
        };
        for clause in &ast.clauses {
            if clause.negated {
                continue;
            }
            if let Atom::AnyOf(members) = &clause.atom {
                if members.len() < 2 {
                    continue;
                }
                let normalized: Vec<String> = members.iter().map(|m| normalize_token(m)).collect();

                // pick canonical: longest, then lexicographic (safe: members.len() >= 2)
                let Some(canonical) = normalized
                    .iter()
                    .max_by(|a, b| a.len().cmp(&b.len()).then_with(|| b.cmp(a)))
                    .cloned()
                else {
                    continue;
                };

                for member in &normalized {
                    if member != &canonical {
                        let key = (canonical.clone(), member.clone());
                        *pair_counts.entry(key).or_insert(0) += 1;
                    }
                }
            }
        }
    }

    let mut vocab = Vocab::default();
    let mut seen_aliases: HashMap<String, String> = HashMap::new();

    for ((canonical, alias), count) in &pair_counts {
        if *count < min_count {
            continue;
        }
        // skip if this alias is already mapped to a different canonical
        if let Some(existing) = seen_aliases.get(alias) {
            if existing != canonical {
                continue;
            }
        }
        seen_aliases.insert(alias.clone(), canonical.clone());

        let canon_tokens: Vec<&str> = canonical.split('_').collect();
        let alias_tokens: Vec<&str> = alias.split('_').collect();

        let canon_feature = format!("term:{canonical}");

        // register the canonical as a phrase if multi-word
        if canon_tokens.len() > 1 {
            let already = vocab.phrases.iter().any(|p| p.canonical == canon_feature);
            if !already {
                vocab.phrases.push(PhraseEntry {
                    tokens: canon_tokens
                        .iter()
                        .map(std::string::ToString::to_string)
                        .collect(),
                    canonical: canon_feature.clone(),
                    kind: FeatureKindSer::Generic,
                });
            }
        }

        // register the alias
        if alias_tokens.len() > 1 {
            vocab.phrases.push(PhraseEntry {
                tokens: alias_tokens
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect(),
                canonical: canon_feature,
                kind: FeatureKindSer::Generic,
            });
        } else {
            vocab.synonyms.push(SynonymEntry {
                token: alias.clone(),
                canonical: canon_feature,
                kind: FeatureKindSer::Generic,
            });
        }
    }

    // sort for determinism
    vocab.synonyms.sort_by(|a, b| a.token.cmp(&b.token));
    vocab.phrases.sort_by(|a, b| a.tokens.cmp(&b.tokens));

    vocab
}

fn normalize_token(text: &str) -> String {
    let mut out = String::new();
    for ch in text.chars() {
        let c = crate::normalize::fold_diacritic(ch);
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else if !out.is_empty() && !out.ends_with('_') {
            out.push('_');
        }
    }
    out.trim_end_matches('_').to_string()
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
            b.add_phrase(&toks, &entry.canonical, entry.kind.into());
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn learn_discovers_synonyms_from_anyof_groups() {
        let queries: Vec<(u64, String)> = (0..20)
            .map(|i| (i, format!("(rookie,rc) somethingunique{i:03}")))
            .collect();
        let vocab = learn_from_queries(&queries, 2);
        assert!(
            !vocab.synonyms.is_empty() || !vocab.phrases.is_empty(),
            "should learn at least one synonym from repeated any-of groups"
        );
    }

    #[test]
    fn learn_ignores_below_threshold() {
        let queries = vec![(1, "(alpha,beta) stuff".to_string())];
        let vocab = learn_from_queries(&queries, 5);
        assert!(
            vocab.is_empty(),
            "single occurrence should be below threshold of 5"
        );
    }

    #[test]
    fn learn_ignores_negated_groups() {
        let queries: Vec<(u64, String)> = (0..20)
            .map(|i| (i, format!("-(badterm,anotherbad) good{i:03}")))
            .collect();
        let vocab = learn_from_queries(&queries, 2);
        assert!(
            vocab.is_empty(),
            "negated groups should not produce synonyms"
        );
    }

    #[test]
    fn learn_discovers_phrase_synonyms() {
        let queries: Vec<(u64, String)> = (0..20)
            .map(|i| (i, format!("(\"michael jordan\",mj) rare{i:03}")))
            .collect();
        let vocab = learn_from_queries(&queries, 2);
        let has_phrase = vocab
            .phrases
            .iter()
            .any(|p| p.tokens == vec!["michael", "jordan"]);
        assert!(has_phrase, "should learn 'michael jordan' as a phrase");
        let has_syn = vocab.synonyms.iter().any(|s| s.token == "mj");
        assert!(has_syn, "should learn 'mj' as a synonym");
    }

    #[test]
    fn manual_synonym_management() {
        let mut vocab = Vocab::new();
        vocab.add_synonym("rc", "term:rookie", FeatureKind::Category);
        assert_eq!(vocab.synonyms().len(), 1);
        assert!(vocab.get_synonym("rc").is_some());

        // duplicate is ignored
        vocab.add_synonym("rc", "term:rookie", FeatureKind::Category);
        assert_eq!(vocab.synonyms().len(), 1);

        assert!(vocab.remove_synonym("rc"));
        assert!(vocab.synonyms().is_empty());
        assert!(!vocab.remove_synonym("nonexistent"));
    }

    #[test]
    fn manual_phrase_management() {
        let mut vocab = Vocab::new();
        vocab.add_phrase(&["upper", "deck"], "term:upper_deck", FeatureKind::Generic);
        assert_eq!(vocab.phrases().len(), 1);

        // duplicate is ignored
        vocab.add_phrase(&["upper", "deck"], "term:upper_deck", FeatureKind::Generic);
        assert_eq!(vocab.phrases().len(), 1);

        assert!(vocab.remove_phrase(&["upper", "deck"]));
        assert!(vocab.phrases().is_empty());
    }

    #[test]
    fn json_round_trip() {
        let mut vocab = Vocab::new();
        vocab.add_synonym("rc", "term:rookie", FeatureKind::Category);
        vocab.add_phrase(
            &["michael", "jordan"],
            "term:michael_jordan",
            FeatureKind::Generic,
        );
        vocab.add_grader("psa");
        vocab.add_grade_word("gem");

        let json = vocab.to_json().unwrap();
        let restored = Vocab::from_json(&json).unwrap();
        assert_eq!(restored.synonyms().len(), 1);
        assert_eq!(restored.phrases().len(), 1);
        assert_eq!(restored.graders().len(), 1);
        assert_eq!(restored.grade_words().len(), 1);
    }

    #[test]
    fn to_normalizer_produces_valid_normalizer() {
        let mut vocab = Vocab::new();
        vocab.add_synonym("rc", "term:rookie", FeatureKind::Category);
        vocab.add_phrase(&["upper", "deck"], "term:upper_deck", FeatureKind::Generic);
        let norm = vocab.to_normalizer().expect("should build normalizer");

        let mut dict = crate::dict::Dict::new();
        let mut lc = String::new();
        let feats = norm.compile_features("upper deck rc", &mut dict, &mut lc);
        assert!(feats.len() >= 2, "should produce features for known vocab");
    }

    #[test]
    fn merge_combines_vocabs() {
        let mut v1 = Vocab::new();
        v1.add_synonym("rc", "term:rookie", FeatureKind::Category);
        v1.add_grader("psa");

        let mut v2 = Vocab::new();
        v2.add_synonym("ud", "term:upper_deck", FeatureKind::Generic);
        v2.add_synonym("rc", "term:different", FeatureKind::Generic); // duplicate token
        v2.add_grader("bgs");

        v1.merge(&v2);
        assert_eq!(v1.synonyms().len(), 2); // rc + ud
        assert_eq!(v1.graders().len(), 2); // psa + bgs
                                           // rc should keep original mapping (first wins)
        assert_eq!(v1.get_synonym("rc").unwrap().canonical, "term:rookie");
    }

    #[test]
    fn empty_vocab_builds_valid_normalizer() {
        let vocab = Vocab::new();
        let norm = vocab.to_normalizer().expect("empty vocab should build");
        let mut dict = crate::dict::Dict::new();
        let mut lc = String::new();
        let feats = norm.compile_features("hello world", &mut dict, &mut lc);
        assert_eq!(feats.len(), 2, "should produce generic features");
    }

    #[test]
    fn engine_with_vocab() {
        let mut vocab = Vocab::new();
        vocab.add_synonym("rc", "term:rookie", FeatureKind::Category);

        let eng = crate::segment::Engine::with_vocab(vocab, crate::config::EngineConfig::default())
            .expect("should build engine from vocab");
        assert!(eng.vocab().is_some());
        assert_eq!(eng.vocab().unwrap().synonyms().len(), 1);
    }

    #[test]
    fn snapshot_carries_vocab_for_lock_free_reads() {
        // The lock-free read path (GET /_vocab via ArcSwap) depends on the vocab
        // living in EngineSnapshot — not just on the Engine behind the write
        // mutex (ADR-016). Verify the snapshot reflects the vocab at snapshot
        // time, and that a published snapshot is immutable across a later
        // set_vocab (an older snapshot keeps its own view).
        let mut vocab = Vocab::new();
        vocab.add_synonym("rc", "term:rookie", FeatureKind::Category);
        let mut eng =
            crate::segment::Engine::with_vocab(vocab, crate::config::EngineConfig::default())
                .expect("should build engine from vocab");

        // Snapshot taken now sees the initial vocab.
        let snap_v1 = eng.snapshot();
        assert_eq!(
            snap_v1.vocab().map(|v| v.synonyms().len()),
            Some(1),
            "snapshot must carry the vocab so /_vocab can read it lock-free"
        );

        // Swap in a larger vocab on the engine.
        let mut vocab2 = Vocab::new();
        vocab2.add_synonym("rc", "term:rookie", FeatureKind::Category);
        vocab2.add_synonym("ud", "term:upper_deck", FeatureKind::Generic);
        eng.set_vocab(vocab2).expect("set_vocab should succeed");

        // A fresh snapshot reflects the update; the old snapshot is unchanged.
        let snap_v2 = eng.snapshot();
        assert_eq!(snap_v2.vocab().map(|v| v.synonyms().len()), Some(2));
        assert_eq!(
            snap_v1.vocab().map(|v| v.synonyms().len()),
            Some(1),
            "an already-published snapshot must keep its own vocab view"
        );
    }

    #[test]
    fn snapshot_vocab_is_none_without_vocab() {
        // An engine built without a vocab (the default path) has no snapshot
        // vocab; GET /_vocab then serves Vocab::default().
        let eng = crate::segment::Engine::new(
            crate::normalize::Normalizer::default_vocab().expect("default vocab"),
        );
        assert!(eng.snapshot().vocab().is_none());
    }
}
