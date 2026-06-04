//! Vocabulary: learned synonyms from query any-of groups + manual management.
//!
//! Invariant: A Vocab produces a deterministic Normalizer; the same Vocab
//!   always yields the same feature space
//! Hot path: no — vocab operations are admin/build-time only

use std::collections::HashMap;
use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::dict::{Dict, EquivMap, FeatureId, FeatureKind};
use crate::dsl::{self, Atom};
use crate::normalize::{Normalizer, NormalizerBuilder};
use crate::util::fast_map;

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
                    additive: false, // collapse: an any-of-learned alias canonicalizes
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
                additive: false, // collapse: an any-of-learned alias canonicalizes
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

/// Configuration for [`learn_vocab_from_corpus`] — composes the ADR-015 any-of
/// learner with opt-in NPMI corpus phrase induction (ADR-053) and opt-in equivalence
/// (alias) learning via expansion (ADR-054).
///
/// The default disables both opt-ins, so the result is byte-identical to
/// [`learn_from_queries`] alone — every existing caller and oracle is unaffected.
#[derive(Debug, Clone)]
pub struct CorpusLearnConfig {
    /// Minimum any-of occurrences for a rule to be learned (the bare `min_count` of
    /// [`learn_from_queries`]).
    pub anyof_min_count: usize,
    /// Enable NPMI corpus phrase induction (off by default).
    pub corpus_phrases: bool,
    /// NPMI binding-strength threshold for an induced phrase.
    pub npmi_tau: f64,
    /// Minimum adjacent co-occurrence count for an induced phrase. Defaults
    /// small — a live corpus is far smaller than the `learn` binary's
    /// 500k-query default (min_count 50).
    pub npmi_min_count: usize,
    /// Bigram -> trigram growth passes.
    pub npmi_iterations: usize,
    /// Learn any-of groups as **equivalence groups** applied via FN-safe expansion
    /// (ADR-054) instead of collapse synonyms (the default). Off by default.
    pub learn_equivalences: bool,
}

impl Default for CorpusLearnConfig {
    fn default() -> Self {
        Self {
            anyof_min_count: 2,
            corpus_phrases: false,
            npmi_tau: 0.30,
            npmi_min_count: 3,
            npmi_iterations: 2,
            learn_equivalences: false,
        }
    }
}

/// Learn equivalence relationships from query any-of co-occurrence (ADR-054): an any-of group
/// `(a, b, c)` declares its members interchangeable. Each unordered **pair** within a group is
/// counted — so `(rc,rookie)` and `(rc,rookie,rookie card)` both reinforce `rc≡rookie`, matching
/// the pair-level [`learn_from_queries`] synonym learner rather than keying on the exact group.
/// A pair seen in at least `min_count` any-of groups is emitted as a 2-element equivalence
/// group; overlapping pairs are unioned transitively at apply time
/// ([`Vocab::resolve_equivalences`]). Forms are kept raw — resolved through the normalizer when
/// applied.
pub fn learn_equivalences_from_queries(
    queries: &[(u64, String)],
    min_count: usize,
) -> Vec<Vec<String>> {
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
                let mut forms: Vec<String> = members.iter().map(|m| m.trim().to_string()).collect();
                forms.retain(|m| !m.is_empty());
                forms.sort();
                forms.dedup();
                // Every unordered pair (forms is sorted, so i<j yields a<=b — a stable key).
                for i in 0..forms.len() {
                    for j in (i + 1)..forms.len() {
                        *pair_counts
                            .entry((forms[i].clone(), forms[j].clone()))
                            .or_insert(0) += 1;
                    }
                }
            }
        }
    }
    let mut groups: Vec<Vec<String>> = pair_counts
        .into_iter()
        .filter(|(_, c)| *c >= min_count)
        .map(|((a, b), _)| vec![a, b])
        .collect();
    groups.sort();
    groups
}

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

/// Learn a vocabulary from a query corpus. By default this is the ADR-015 any-of synonym
/// learner; with `cfg.learn_equivalences` the any-of groups are learned as **equivalence
/// groups** (expansion, ADR-054) instead of collapse synonyms; and with `cfg.corpus_phrases`
/// NPMI-induced entity phrases ([`crate::corpus::learn_phrases_from_text`], ADR-053) are
/// merged on top.
///
/// With both opt-ins off this returns exactly `learn_from_queries(corpus, cfg.anyof_min_count)`.
pub fn learn_vocab_from_corpus(corpus: &[(u64, String)], cfg: &CorpusLearnConfig) -> Vocab {
    let mut vocab = if cfg.learn_equivalences {
        // Expansion mode: any-of co-occurrence becomes equivalence groups, not synonyms.
        let mut v = Vocab::new();
        for grp in learn_equivalences_from_queries(corpus, cfg.anyof_min_count) {
            let refs: Vec<&str> = grp.iter().map(String::as_str).collect();
            v.add_equivalence(&refs);
        }
        v
    } else {
        learn_from_queries(corpus, cfg.anyof_min_count)
    };
    if cfg.corpus_phrases {
        let phrases = crate::corpus::learn_phrases_from_text(
            corpus,
            cfg.npmi_min_count,
            cfg.npmi_tau,
            cfg.npmi_iterations,
        );
        vocab.merge(&phrases);
    }
    vocab
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

    #[test]
    fn corpus_learn_default_off_equals_anyof_only() {
        // The default CorpusLearnConfig disables NPMI, so the composer must be
        // byte-identical to any-of learning alone (the back-compat guarantee, ADR-053).
        let queries: Vec<(u64, String)> = (0..30)
            .map(|i| (i, format!("(rookie,rc) upper deck unique{i:03}")))
            .collect();
        let cfg = CorpusLearnConfig {
            anyof_min_count: 2,
            ..Default::default()
        };
        let composed = learn_vocab_from_corpus(&queries, &cfg);
        let anyof_only = learn_from_queries(&queries, 2);
        assert_eq!(
            composed.to_json().unwrap(),
            anyof_only.to_json().unwrap(),
            "with corpus_phrases off the composer must equal any-of learning alone"
        );
    }

    #[test]
    fn corpus_learn_on_adds_npmi_phrases() {
        // No any-of groups -> any-of learning finds nothing; turning on NPMI induces the
        // repeated adjacent "upper deck" entity as a phrase.
        let queries: Vec<(u64, String)> = (0..30)
            .map(|i| (i, format!("upper deck unique{i:03}")))
            .collect();
        let off = learn_vocab_from_corpus(
            &queries,
            &CorpusLearnConfig {
                anyof_min_count: 2,
                ..Default::default()
            },
        );
        assert!(
            off.phrases().is_empty(),
            "no any-of groups -> no phrases when NPMI is off"
        );
        let on = learn_vocab_from_corpus(
            &queries,
            &CorpusLearnConfig {
                anyof_min_count: 2,
                corpus_phrases: true,
                npmi_min_count: 3,
                ..Default::default()
            },
        );
        assert!(
            on.phrases()
                .iter()
                .any(|p| p.tokens == vec!["upper".to_string(), "deck".to_string()]),
            "NPMI on must induce the upper/deck phrase"
        );
    }

    #[test]
    fn learns_equivalences_from_anyof_groups() {
        let queries: Vec<(u64, String)> = (0..10)
            .map(|i| (i, format!("(rookie,rc) card{i:03}")))
            .collect();
        let groups = learn_equivalences_from_queries(&queries, 2);
        assert!(
            groups
                .iter()
                .any(|g| g.contains(&"rc".to_string()) && g.contains(&"rookie".to_string())),
            "an any-of group seen >= min_count must be learned as an equivalence group"
        );
        // Below threshold -> nothing learned.
        assert!(learn_equivalences_from_queries(&queries, 11).is_empty());
    }

    #[test]
    fn corpus_learn_equivalences_mode_emits_groups_not_synonyms() {
        let queries: Vec<(u64, String)> = (0..10)
            .map(|i| (i, format!("(rookie,rc) card{i:03}")))
            .collect();
        let cfg = CorpusLearnConfig {
            anyof_min_count: 2,
            learn_equivalences: true,
            ..Default::default()
        };
        let v = learn_vocab_from_corpus(&queries, &cfg);
        assert!(
            v.synonyms().is_empty() && v.phrases().is_empty(),
            "expansion mode must not emit collapse synonyms/phrases"
        );
        assert!(
            !v.equivalences().is_empty(),
            "expansion mode must emit equivalence groups"
        );
    }

    #[test]
    fn learn_equivalences_reinforces_pairs_across_group_sizes() {
        // (rc,rookie) once + (rc,rookie,rcfull) once: pair-level counting reinforces rc≡rookie
        // (count 2), so it survives min_count=2 — exact-group counting would see two distinct
        // groups (count 1 each) and learn nothing.
        let queries = vec![
            (1u64, "(rc,rookie)".to_string()),
            (2u64, "(rc,rookie,rcfull)".to_string()),
        ];
        let groups = learn_equivalences_from_queries(&queries, 2);
        assert!(
            groups
                .iter()
                .any(|g| g.contains(&"rc".to_string()) && g.contains(&"rookie".to_string())),
            "rc≡rookie must reinforce across the two differently-sized any-of groups"
        );
    }

    #[test]
    fn resolve_equivalences_unions_overlapping_groups() {
        // Overlapping declared groups [aaa,bbb] + [bbb,ccc] must resolve to ONE transitive
        // group {aaa,bbb,ccc}, not order-dependently overwrite the shared member.
        let mut v = Vocab::new();
        v.add_equivalence(&["aaa", "bbb"]);
        v.add_equivalence(&["bbb", "ccc"]);
        let norm = crate::normalize::Normalizer::default_vocab().expect("vocab");
        let dict = Dict::new();
        let map = v.resolve_equivalences(&norm, &dict);

        let mut lc = String::new();
        let fa = norm.compile_features_readonly("aaa", &dict, &mut lc)[0];
        let fb = norm.compile_features_readonly("bbb", &dict, &mut lc)[0];
        let fc = norm.compile_features_readonly("ccc", &dict, &mut lc)[0];

        let ga = map.get(&fa).expect("aaa resolved");
        assert!(
            ga.contains(&fa) && ga.contains(&fb) && ga.contains(&fc),
            "aaa/bbb/ccc must merge into one transitive group"
        );
        assert_eq!(
            map.get(&fa),
            map.get(&fc),
            "aaa and ccc share the merged group (transitive via bbb)"
        );
    }
}
