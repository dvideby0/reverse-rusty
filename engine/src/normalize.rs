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
//! leftmost-longest mode. This replaced the PoC token-trie with identical
//! semantics but O(n) scan time regardless of vocabulary size.

use crate::dict::{Dict, FeatureId, FeatureKind};
use daachorse::{DoubleArrayAhoCorasick, DoubleArrayAhoCorasickBuilder, MatchKind};

/// Metadata for a phrase pattern registered in the automaton.
#[derive(Debug, Clone)]
struct PhraseEntry {
    feature: String,
    kind: FeatureKind,
}

pub struct Normalizer {
    /// daachorse automaton over space-joined phrase strings. Pattern value indexes
    /// into `phrase_entries`.
    automaton: DoubleArrayAhoCorasick<usize>,
    phrase_entries: Vec<PhraseEntry>,

    graders: Vec<String>,
    /// single-token synonyms -> (canonical feature, kind).
    synonyms: Vec<(String, String, FeatureKind)>,
    syn_index: std::collections::HashMap<String, usize>,
    grade_words: Vec<String>,
}

impl std::fmt::Debug for Normalizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Normalizer")
            .field("phrases", &self.phrase_entries.len())
            .field("graders", &self.graders)
            .field("synonyms", &self.synonyms.len())
            .field("grade_words", &self.grade_words)
            .finish()
    }
}

impl Normalizer {
    /// Create a [`NormalizerBuilder`] for assembling a custom vocabulary.
    pub fn builder() -> NormalizerBuilder {
        NormalizerBuilder::new()
    }

    /// Build the default trading-card vocabulary. Rich enough to exercise the
    /// spec's worked example and the synthetic generator; not exhaustive.
    ///
    /// Build a domain-agnostic normalizer with no pre-loaded vocabulary.
    ///
    /// The normalizer still handles year detection, number disambiguation,
    /// diacritic folding, and lowercase normalization. Domain-specific vocabulary
    /// (phrases, synonyms, graders, grade words) should be supplied via
    /// [`NormalizerBuilder`] or learned from query any-of groups at runtime.
    pub fn default_vocab() -> Result<Self, crate::error::NormalizerError> {
        NormalizerBuilder::new().build()
    }

    /// Lowercase + fold diacritics + tokenize punctuation into `out` (reused).
    /// `#` and `/` are kept as standalone marker tokens so the number logic can
    /// tell card-numbers (`#2`) and serials (`/199`, `3/10`) apart from grades.
    fn clean_into(text: &str, out: &mut String) {
        out.clear();
        for ch in text.chars() {
            let c = fold_diacritic(ch);
            if c.is_ascii_alphanumeric() {
                out.push(c.to_ascii_lowercase());
            } else if c == '.' {
                out.push('.'); // keep dots inside numbers (half grades)
            } else if c == '#' || c == '/' {
                out.push(' ');
                out.push(c);
                out.push(' ');
            } else {
                out.push(' ');
            }
        }
    }

    /// Core: emit canonical feature names for `text`. Calls `emit(name, kind)`
    /// for each feature found. Shared by compile and match paths so the two
    /// always agree. `lc` is a reusable scratch String.
    ///
    /// Two-phase approach:
    ///   1) Run the daachorse automaton over the cleaned text to find all
    ///      leftmost-longest multiword phrase matches. Record which byte ranges
    ///      are consumed.
    ///   2) Iterate through tokens. Tokens fully inside a phrase match are
    ///      skipped (the phrase feature is emitted once). All other tokens go
    ///      through the existing grader/number/synonym/generic pipeline.
    pub fn emit<F: FnMut(&str, FeatureKind)>(&self, text: &str, lc: &mut String, emit: &mut F) {
        Self::clean_into(text, lc);

        // Phase 1: find multiword phrase matches via the automaton.
        // We collect (byte_start, byte_end, pattern_index) for each match.
        // The automaton operates on the cleaned string, matching space-joined
        // token sequences. We need to ensure matches align on word boundaries.
        let mut phrase_matches: Vec<(usize, usize, usize)> = Vec::new();
        for m in self.automaton.leftmost_find_iter(&**lc) {
            let start = m.start();
            let end = m.end();
            // Word-boundary check: match must start at beginning or after a space,
            // and end at end-of-string or before a space.
            let ok_start = start == 0 || lc.as_bytes()[start - 1] == b' ';
            let ok_end = end == lc.len() || lc.as_bytes()[end] == b' ';
            if ok_start && ok_end {
                phrase_matches.push((start, end, m.value()));
            }
        }

        // Build a byte-position set of consumed ranges for fast lookup.
        // For each token we'll check if its start byte falls inside a phrase match.
        // Phrase matches are non-overlapping (leftmost-longest), so a sorted list
        // with binary search works.

        // Phase 2: tokenize and iterate, skipping phrase-consumed spans.
        let tokens: Vec<&str> = lc.split_whitespace().collect();
        // Compute byte offsets for each token in `lc`.
        let token_offsets: Vec<usize> = {
            let mut offsets = Vec::with_capacity(tokens.len());
            let mut pos = 0usize;
            let bytes = lc.as_bytes();
            for &tok in &tokens {
                // skip whitespace
                while pos < bytes.len() && bytes[pos] == b' ' {
                    pos += 1;
                }
                offsets.push(pos);
                pos += tok.len();
            }
            offsets
        };

        // For each token, determine if it's inside a phrase match.
        // If so, emit the phrase feature at the FIRST token of the match (skip rest).
        let mut phrase_emitted: Vec<bool> = vec![false; phrase_matches.len()];
        let mut token_consumed: Vec<bool> = vec![false; tokens.len()];

        for (ti, &toff) in token_offsets.iter().enumerate() {
            for (pi, &(ps, pe, _)) in phrase_matches.iter().enumerate() {
                if toff >= ps && toff + tokens[ti].len() <= pe {
                    token_consumed[ti] = true;
                    if !phrase_emitted[pi] {
                        phrase_emitted[pi] = true;
                        let entry = &self.phrase_entries[phrase_matches[pi].2];
                        emit(&entry.feature, entry.kind);
                    }
                    break;
                }
            }
        }

        // Phase 2b: process non-consumed tokens through the existing pipeline.
        let mut scratch = String::new();
        let mut i = 0;
        let mut pending_grader: Option<String> = None;
        let mut pending_grader_age = 0u8;
        let mut grade_ctx = false;
        let mut grade_ctx_age = 0u8;

        while i < tokens.len() {
            if token_consumed[i] {
                // This token was part of a phrase match — skip it.
                // But still age out pending grader/grade context.
                if pending_grader.is_some() {
                    pending_grader_age = pending_grader_age.saturating_add(1);
                    if pending_grader_age > 3 {
                        pending_grader = None;
                    }
                }
                if grade_ctx {
                    grade_ctx_age = grade_ctx_age.saturating_add(1);
                    if grade_ctx_age > 2 {
                        grade_ctx = false;
                    }
                }
                i += 1;
                continue;
            }

            let tok = tokens[i];

            // 0) structural markers from cleaning: skip
            if tok == "#" || tok == "/" {
                i += 1;
                continue;
            }

            // 1) grader keyword (possibly fused like "psa10")
            if let Some((g, rest)) = self.split_grader(tok) {
                let gcanon = canon_grader(&g);
                scratch.clear();
                scratch.push_str("grader:");
                scratch.push_str(&gcanon);
                emit(&scratch, FeatureKind::Grader);
                if let Some(num) = rest {
                    self.emit_grade(&gcanon, &num, &mut scratch, emit);
                    pending_grader = None;
                } else {
                    pending_grader = Some(gcanon);
                    pending_grader_age = 0;
                }
                i += 1;
                continue;
            }

            // 2) grade modifier / context word
            if self.grade_words.iter().any(|w| w == tok) {
                grade_ctx = true;
                grade_ctx_age = 0;
                if pending_grader.is_some() {
                    pending_grader_age = pending_grader_age.saturating_add(1);
                }
                i += 1;
                continue;
            }

            // 3) numbers: disambiguate card-numbers, serials, pop, grades, years
            if let Some(numstr) = parse_number(tok) {
                let prev = if i > 0 { Some(tokens[i - 1]) } else { None };
                let next = tokens.get(i + 1).copied();
                let is_cardnum = prev == Some("#");
                let is_serial = prev == Some("/") || next == Some("/");
                let is_pop = prev
                    .map(|p| p.eq_ignore_ascii_case("pop"))
                    .unwrap_or(false);

                if is_cardnum || is_serial || is_pop {
                    emit_generic(&numstr, &mut scratch, emit);
                } else if let Some(y) = as_year(&numstr) {
                    scratch.clear();
                    scratch.push_str("year:");
                    scratch.push_str(&y);
                    emit(&scratch, FeatureKind::Year);
                } else if let Some(g) = pending_grader.clone() {
                    if is_grade_value(&numstr) {
                        self.emit_grade(&g, &numstr, &mut scratch, emit);
                        pending_grader = None;
                    } else {
                        emit_generic(&numstr, &mut scratch, emit);
                    }
                } else if grade_ctx && is_grade_value(&numstr) {
                    scratch.clear();
                    scratch.push_str("grade:");
                    scratch.push_str(&numstr);
                    emit(&scratch, FeatureKind::Grade);
                    grade_ctx = false;
                } else {
                    emit_generic(&numstr, &mut scratch, emit);
                }
                i += 1;
                continue;
            }

            // 4) closed-vocab synonym
            if let Some(&si) = self.syn_index.get(tok) {
                let (_, canon, kind) = &self.synonyms[si];
                emit(canon, *kind);
                i += 1;
                continue;
            }

            // 5) generic fallback term
            emit_generic(tok, &mut scratch, emit);
            i += 1;

            // age out stale pending grader / grade context
            if pending_grader.is_some() {
                pending_grader_age = pending_grader_age.saturating_add(1);
                if pending_grader_age > 3 {
                    pending_grader = None;
                }
            }
            if grade_ctx {
                grade_ctx_age = grade_ctx_age.saturating_add(1);
                if grade_ctx_age > 2 {
                    grade_ctx = false;
                }
            }
        }
    }

    fn emit_grade<F: FnMut(&str, FeatureKind)>(
        &self,
        grader: &str,
        num: &str,
        scratch: &mut String,
        emit: &mut F,
    ) {
        scratch.clear();
        scratch.push_str("grade:");
        scratch.push_str(num);
        emit(scratch, FeatureKind::Grade);
        scratch.clear();
        scratch.push_str("grader_grade:");
        scratch.push_str(grader);
        scratch.push_str(num);
        emit(scratch, FeatureKind::GraderGrade);
    }

    /// Split a possibly-fused grader token like "psa10" -> ("psa", Some("10")).
    fn split_grader(&self, tok: &str) -> Option<(String, Option<String>)> {
        for g in &self.graders {
            if tok == g.as_str() {
                return Some((g.clone(), None));
            }
            if let Some(rest) = tok.strip_prefix(g.as_str()) {
                if rest.chars().next().is_some_and(|c| c.is_ascii_digit()) {
                    if let Some(num) = parse_number(rest) {
                        return Some((g.clone(), Some(num)));
                    }
                }
            }
        }
        None
    }

    // ---- compile-time and match-time entry points ----

    /// Compile path: intern features (creating new ones), returning sorted+deduped IDs.
    pub fn compile_features(&self, text: &str, dict: &mut Dict, lc: &mut String) -> Vec<FeatureId> {
        let mut ids: Vec<FeatureId> = Vec::new();
        let mut names: Vec<(String, FeatureKind)> = Vec::new();
        self.emit(text, lc, &mut |name, kind| {
            names.push((name.to_string(), kind))
        });
        for (name, kind) in names {
            ids.push(dict.intern(&name, kind));
        }
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// Match path: look up existing features only (titles can't create features).
    /// Unknown tokens are skipped — no query references them, so they can't
    /// affect any match. Fills `out` with sorted+deduped existing IDs.
    pub fn match_features(
        &self,
        text: &str,
        dict: &Dict,
        lc: &mut String,
        out: &mut Vec<FeatureId>,
    ) {
        out.clear();
        let mut tmp: Vec<FeatureId> = Vec::new();
        self.emit(text, lc, &mut |name, _kind| {
            if let Some(id) = dict.get(name) {
                tmp.push(id);
            }
        });
        tmp.sort_unstable();
        tmp.dedup();
        out.extend_from_slice(&tmp);
    }
}

/// Builder for assembling a [`Normalizer`] from custom vocabulary.
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
/// use percolator::normalize::NormalizerBuilder;
/// use percolator::dict::FeatureKind;
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
        self.phrase_patterns.push(tokens.join(" "));
        self.phrase_entries.push(PhraseEntry {
            feature: feature.to_string(),
            kind,
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
        self.syn_index.insert(token.to_string(), self.synonyms.len());
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

    /// Consume the builder and construct a [`Normalizer`].
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
        })
    }
}

/// Fold common Latin diacritics to ASCII so "Jokić"->"jokic", "Acuña"->"acuna".
pub fn fold_diacritic(ch: char) -> char {
    match ch {
        'á' | 'à' | 'â' | 'ä' | 'ã' | 'å' | 'ā' | 'ą' | 'Á' | 'À' | 'Â' | 'Ä' | 'Ã' | 'Å' => {
            'a'
        }
        'é' | 'è' | 'ê' | 'ë' | 'ē' | 'ė' | 'ę' | 'É' | 'È' | 'Ê' | 'Ë' => 'e',
        'í' | 'ì' | 'î' | 'ï' | 'ī' | 'į' | 'Í' | 'Ì' | 'Î' | 'Ï' => 'i',
        'ó' | 'ò' | 'ô' | 'ö' | 'õ' | 'ø' | 'ō' | 'Ó' | 'Ò' | 'Ô' | 'Ö' | 'Õ' => 'o',
        'ú' | 'ù' | 'û' | 'ü' | 'ū' | 'Ú' | 'Ù' | 'Û' | 'Ü' => 'u',
        'ñ' | 'ń' | 'Ñ' => 'n',
        'ç' | 'ć' | 'č' | 'Ç' | 'Ć' | 'Č' => 'c',
        'š' | 'ś' | 'Š' | 'Ś' => 's',
        'ž' | 'ź' | 'ż' | 'Ž' | 'Ź' | 'Ż' => 'z',
        'ý' | 'ÿ' | 'Ý' => 'y',
        'ł' | 'Ł' => 'l',
        other => other,
    }
}

fn canon_grader(g: &str) -> String {
    match g {
        "beckett" => "bgs".to_string(),
        other => other.to_string(),
    }
}

fn emit_generic<F: FnMut(&str, FeatureKind)>(tok: &str, scratch: &mut String, emit: &mut F) {
    scratch.clear();
    scratch.push_str("term:");
    scratch.push_str(tok);
    emit(scratch, FeatureKind::Generic);
}

/// Parse a token into a clean numeric string (digits with optional .5), or None.
fn parse_number(tok: &str) -> Option<String> {
    let mut seen_digit = false;
    let mut seen_dot = false;
    for ch in tok.chars() {
        if ch.is_ascii_digit() {
            seen_digit = true;
        } else if ch == '.' {
            if seen_dot {
                return None;
            }
            seen_dot = true;
        } else {
            return None;
        }
    }
    if seen_digit {
        Some(tok.to_string())
    } else {
        None
    }
}

fn as_year(num: &str) -> Option<String> {
    if num.len() == 4 && !num.contains('.') {
        if let Ok(y) = num.parse::<u32>() {
            if (1900..=2099).contains(&y) {
                return Some(num.to_string());
            }
        }
    }
    None
}

fn is_grade_value(num: &str) -> bool {
    if let Ok(v) = num.parse::<f32>() {
        (1.0..=10.0).contains(&v)
    } else {
        false
    }
}
