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

use crate::dict::{Dict, FeatureId, FeatureKind};
use daachorse::{DoubleArrayAhoCorasick, DoubleArrayAhoCorasickBuilder, MatchKind};

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
    /// Byte-cleaning punctuation classification (ADR-058). Default = historical behavior.
    punct: PunctTable,
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

    /// Lowercase + fold diacritics + apply the punctuation table into `out` (reused).
    /// Alphanumerics pass through lowercased; every other character is handled by its
    /// [`PunctClass`]. Defaults (ADR-058): `.` is kept in place (half-grades), `#`/`/`
    /// become standalone marker tokens (so the number logic can tell `#2`/`/199` from
    /// grades), and everything else becomes a space. A [`PunctClass::Fold`] character is
    /// deleted, so its neighbors join into one token (`O'Brien` -> `obrien`). The same
    /// table runs over queries and titles, keeping the feature spaces aligned (§2).
    fn clean_into(&self, text: &str, out: &mut String) {
        out.clear();
        for ch in text.chars() {
            let c = fold_diacritic(ch);
            if c.is_ascii_alphanumeric() {
                out.push(c.to_ascii_lowercase());
            } else {
                match self.punct.class_of(c) {
                    PunctClass::Split => out.push(' '),
                    PunctClass::Fold => {} // delete: neighbors join into one token
                    PunctClass::Keep => out.push(c),
                    PunctClass::Marker => {
                        out.push(' ');
                        out.push(c);
                        out.push(' ');
                    }
                }
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
        self.clean_into(text, lc);

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
                    let entry = &self.phrase_entries[phrase_matches[pi].2];
                    // Additive phrases (corpus-learned, ADR-053) emit the phrase feature but
                    // leave the component tokens for phase 2b, so the component features are
                    // also produced (recall-preserving). Collapse phrases consume them.
                    if !entry.additive {
                        token_consumed[ti] = true;
                    }
                    if !phrase_emitted[pi] {
                        phrase_emitted[pi] = true;
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
                    Self::emit_grade(&gcanon, &num, &mut scratch, emit);
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
                let is_pop = prev.is_some_and(|p| p.eq_ignore_ascii_case("pop"));

                if is_cardnum || is_serial || is_pop {
                    emit_generic(&numstr, &mut scratch, emit);
                } else if let Some(y) = as_year(&numstr) {
                    scratch.clear();
                    scratch.push_str("year:");
                    scratch.push_str(&y);
                    emit(&scratch, FeatureKind::Year);
                } else if let Some(g) = pending_grader.clone() {
                    if is_grade_value(&numstr) {
                        Self::emit_grade(&g, &numstr, &mut scratch, emit);
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
            names.push((name.to_string(), kind));
        });
        for (name, kind) in names {
            ids.push(dict.intern(&name, kind));
        }
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// Read-only compile: resolve features by name without interning new ones. A term
    /// absent from the (frozen) dict is assigned a deterministic *synthetic* ID
    /// (dynamic vocabulary, ADR-046) rather than dropped — so a query added after the
    /// dict is frozen is *absorbed* with its full semantics instead of silently
    /// broadening. Used by the cluster live-write path and by explain.
    pub fn compile_features_readonly(
        &self,
        text: &str,
        dict: &Dict,
        lc: &mut String,
    ) -> Vec<FeatureId> {
        let mut ids: Vec<FeatureId> = Vec::new();
        self.emit(text, lc, &mut |name, _kind| {
            ids.push(dict.get_or_synthetic(name));
        });
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// Match path: resolve title features by name. A token absent from the (frozen)
    /// dict is assigned a deterministic *synthetic* ID (dynamic vocabulary, ADR-046)
    /// rather than dropped — so a live-added query that references a new term still
    /// matches a title containing it (the title side must hash too, or that match
    /// would be a false negative). Interned tokens keep their dense ID. Fills `out`
    /// with sorted+deduped IDs.
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
            tmp.push(dict.get_or_synthetic(name));
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
        self.add_phrase_inner(tokens, feature, kind, false);
    }

    /// Like [`add_phrase`](Self::add_phrase) but **additive**: a match emits the phrase
    /// feature AND leaves the component tokens to also emit their own features, so a query
    /// referencing a component never loses the match. Used for corpus-learned phrases
    /// (ADR-053) to keep the recall-first contract.
    pub fn add_phrase_additive(&mut self, tokens: &[&str], feature: &str, kind: FeatureKind) {
        self.add_phrase_inner(tokens, feature, kind, true);
    }

    fn add_phrase_inner(
        &mut self,
        tokens: &[&str],
        feature: &str,
        kind: FeatureKind,
        additive: bool,
    ) {
        self.phrase_patterns.push(tokens.join(" "));
        self.phrase_entries.push(PhraseEntry {
            feature: feature.to_string(),
            kind,
            additive,
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
            punct: self.punct,
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

#[cfg(test)]
mod golden {
    //! Golden normalization cases — exact feature-*name* sets, authored by hand from
    //! the spec (docs/design/normalization.md §2–§4, docs/reference/dsl.md), NOT
    //! captured from `emit`. They exist because the differential oracle
    //! (tests/oracle.rs) runs THIS normalizer on both its engine and its brute-force
    //! ground truth, and only ever under the EMPTY `default_vocab` — so a
    //! normalization-model bug is invisible there, and the entire vocab-driven path
    //! (phrases/synonyms/graders) is never exercised at all. These pins close that
    //! gap with expectations a code bug cannot infect. See docs/DECISIONS.md ADR-050.
    use super::*;

    /// Sorted feature *names* for `text`. Uses the mutating compile path on purpose:
    /// it interns every emitted feature, so `Dict::name` round-trips to a real name
    /// (the read-only path would hash misses to a `"<oov>"` synthetic ID).
    fn names(norm: &Normalizer, text: &str) -> Vec<String> {
        let mut dict = Dict::new();
        let mut lc = String::new();
        let ids = norm.compile_features(text, &mut dict, &mut lc);
        let mut out: Vec<String> = ids.iter().map(|&id| dict.name(id).to_string()).collect();
        out.sort();
        out
    }

    fn s(items: &[&str]) -> Vec<String> {
        items.iter().map(ToString::to_string).collect()
    }

    /// The spec's worked-example vocabulary (docs/design/normalization.md §1), built
    /// explicitly so the expected canonical names are themselves part of the contract.
    fn spec_vocab() -> Normalizer {
        NormalizerBuilder::new()
            .phrase(&["upper", "deck"], "brand:upper_deck", FeatureKind::Brand)
            .phrase(
                &["michael", "jordan"],
                "player:michael_jordan",
                FeatureKind::Player,
            )
            .synonym("ud", "brand:upper_deck", FeatureKind::Brand)
            .synonym("topps", "brand:topps", FeatureKind::Brand)
            .synonym("sp", "card_term:sp", FeatureKind::Category)
            .grader("psa")
            .grader("bgs")
            .grader("sgc")
            .grade_word("gem")
            .grade_word("mint")
            .build()
            .expect("spec vocab automaton")
    }

    // ---- vocab-independent pipeline (the empty default_vocab still does this) ----

    #[test]
    fn diacritics_fold_to_ascii() {
        let n = Normalizer::default_vocab().unwrap();
        // normalization.md §4: Café->cafe, Jokić->jokic, Acuña->acuna (ñ no longer splits).
        assert_eq!(names(&n, "café"), s(&["term:cafe"]));
        assert_eq!(names(&n, "Jokić"), s(&["term:jokic"]));
        assert_eq!(names(&n, "Ronald Acuña"), s(&["term:acuna", "term:ronald"]));
    }

    #[test]
    fn number_disambiguation_matrix() {
        let n = Normalizer::default_vocab().unwrap();
        // normalization.md §4 hardening table: markers keep numbers from becoming grades.
        assert_eq!(names(&n, "#2 bulls"), s(&["term:2", "term:bulls"])); // card number
        assert_eq!(names(&n, "/5"), s(&["term:5"])); // serial
        assert_eq!(names(&n, "3/10"), s(&["term:10", "term:3"])); // serial halves
        assert_eq!(names(&n, "1994"), s(&["year:1994"])); // year
        assert_eq!(names(&n, "pop 1"), s(&["term:1", "term:pop"])); // population
    }

    #[test]
    fn generic_fallback_term() {
        let n = Normalizer::default_vocab().unwrap();
        assert_eq!(names(&n, "unknownword"), s(&["term:unknownword"]));
    }

    // ---- vocab-driven pipeline (spec vocab) — never reached by the oracle ----

    #[test]
    fn multiword_phrases_collapse_to_one_feature() {
        let n = spec_vocab();
        // normalization.md §1/§2: a multiword entity is ONE feature, not its tokens.
        assert_eq!(names(&n, "michael jordan"), s(&["player:michael_jordan"]));
        assert_eq!(names(&n, "upper deck"), s(&["brand:upper_deck"]));
    }

    #[test]
    fn synonyms_converge_alternate_surface_forms() {
        let n = spec_vocab();
        // normalization.md §2: "ud" and the "upper deck" phrase land on the SAME feature.
        assert_eq!(names(&n, "ud"), s(&["brand:upper_deck"]));
        assert_eq!(names(&n, "topps"), s(&["brand:topps"]));
    }

    #[test]
    fn grader_path_emits_grader_grade_and_fused_form() {
        let n = spec_vocab();
        // normalization.md §1/§2: psa 10 / psa10 -> grader:psa + grade:10 + grader_grade:psa10.
        let expected = s(&["grade:10", "grader:psa", "grader_grade:psa10"]);
        assert_eq!(names(&n, "psa 10"), expected);
        assert_eq!(names(&n, "psa10"), expected, "fused form == spaced form");
        assert_eq!(
            names(&n, "psa 9.5"),
            s(&["grade:9.5", "grader:psa", "grader_grade:psa9.5"]),
            "half grades are kept"
        );
    }

    // ---- determinism (the §2 invariant; normalize∘normalize isn't typeable, so we
    //      pin the two checkable properties it actually promises) ----

    #[test]
    fn fold_is_a_normalization_fixpoint() {
        let n = Normalizer::default_vocab().unwrap();
        assert_eq!(names(&n, "café"), names(&n, "cafe"));
        assert_eq!(names(&n, "Jokić"), names(&n, "jokic"));
    }

    #[test]
    fn compile_does_not_drift_on_repeat() {
        let n = Normalizer::default_vocab().unwrap();
        let mut dict = Dict::new();
        let mut lc = String::new();
        let first = n.compile_features("psa 10 michael jordan", &mut dict, &mut lc);
        let len_after_first = dict.len();
        let second = n.compile_features("psa 10 michael jordan", &mut dict, &mut lc);
        assert_eq!(first, second, "same text -> same IDs");
        assert_eq!(
            dict.len(),
            len_after_first,
            "a repeat interns no new feature"
        );
    }

    // ---- punctuation-equivalence folding (ADR-058) ----

    #[test]
    fn default_punctuation_splits_apostrophe_and_hyphen() {
        // The historical default: `'` and `-` are word boundaries, so the punctuated
        // forms tokenize apart while the joined form is one token — the false-negative
        // gap (a query `obrien` misses an `O'Brien` title) that folding closes.
        let n = Normalizer::default_vocab().unwrap();
        assert_eq!(names(&n, "O'Brien"), s(&["term:brien", "term:o"]));
        assert_eq!(names(&n, "O-Brien"), s(&["term:brien", "term:o"]));
        assert_eq!(names(&n, "OBrien"), s(&["term:obrien"]));
    }

    #[test]
    fn folding_collapses_punctuation_variants_to_one_token() {
        // Declaring apostrophe (ascii + curly U+2019) and mid-word hyphen as Fold makes
        // all four surface forms land on the SAME single token — so a query and a title
        // that differ only in punctuation now share a feature and match.
        let n = NormalizerBuilder::new()
            .punct('\'', PunctClass::Fold)
            .punct('\u{2019}', PunctClass::Fold)
            .punct('-', PunctClass::Fold)
            .build()
            .expect("folding normalizer");
        let expected = s(&["term:obrien"]);
        assert_eq!(names(&n, "O'Brien"), expected, "ascii apostrophe");
        assert_eq!(names(&n, "O\u{2019}Brien"), expected, "curly apostrophe");
        assert_eq!(names(&n, "O-Brien"), expected, "hyphen");
        assert_eq!(names(&n, "OBrien"), expected, "already joined");
    }

    #[test]
    fn builder_batch_and_mut_fold_apis_fold() {
        // Exercise the `&mut` builder + batch helper (not just the fluent `.punct`).
        let mut b = NormalizerBuilder::new();
        b.fold_punctuation_chars(&['\'', '\u{2019}', '-']);
        let n = b.build().unwrap();
        assert_eq!(names(&n, "O-Brien"), s(&["term:obrien"]));
        assert_eq!(names(&n, "O\u{2019}Brien"), s(&["term:obrien"]));
    }

    #[test]
    fn fold_merges_only_within_a_word_not_across_spaces() {
        // A folded character joins only ADJACENT alphanumerics; a hyphen flanked by
        // spaces still leaves two tokens (the surrounding spaces remain boundaries).
        let n = NormalizerBuilder::new()
            .punct('-', PunctClass::Fold)
            .build()
            .unwrap();
        assert_eq!(names(&n, "foo-bar"), s(&["term:foobar"]));
        assert_eq!(names(&n, "foo - bar"), s(&["term:bar", "term:foo"]));
    }

    #[test]
    fn punct_class_keep_default_is_overridable_to_fold() {
        // `.` defaults to Keep (in place, so half-grades survive); reclassifying it to
        // Fold deletes it. A pure-letter token keeps clear of the number/grade pipeline.
        let keep = Normalizer::default_vocab().unwrap();
        assert_eq!(names(&keep, "a.b.c"), s(&["term:a.b.c"]));
        let fold = NormalizerBuilder::new()
            .punct('.', PunctClass::Fold)
            .build()
            .unwrap();
        assert_eq!(names(&fold, "a.b.c"), s(&["term:abc"]));
    }

    #[test]
    fn marker_and_keep_defaults_are_unchanged_by_the_table() {
        // Regression guard: the default table reproduces the historical `#`/`/`/`.`
        // behaviors exactly (the same cases as `number_disambiguation_matrix`).
        let n = Normalizer::default_vocab().unwrap();
        assert_eq!(names(&n, "#2 bulls"), s(&["term:2", "term:bulls"]));
        assert_eq!(names(&n, "3/10"), s(&["term:10", "term:3"]));
    }
}
