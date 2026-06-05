//! `impl Normalizer` — the shared query/title normalization core.
//!
//! Hot path: yes — `emit` (and its public entry points `match_features` /
//! `compile_features` / `compile_features_readonly`) run per incoming title.
//! Holds the `Normalizer` struct definition, its byte-cleaning (`clean_into`),
//! the two-phase `emit` pipeline (daachorse multiword scan → grader/number/synonym
//! /generic tokenization), and the small free helpers `emit` relies on
//! (`fold_diacritic`, number/year/grade parsing, generic emission).

use super::{PhraseEntry, PunctClass, PunctTable};
use crate::dict::{Dict, FeatureId, FeatureKind};
use daachorse::DoubleArrayAhoCorasick;

pub struct Normalizer {
    /// daachorse automaton over space-joined phrase strings. Pattern value indexes
    /// into `phrase_entries`.
    pub(super) automaton: DoubleArrayAhoCorasick<usize>,
    pub(super) phrase_entries: Vec<PhraseEntry>,

    /// **Overlapping** automaton over phrase patterns (`MatchKind::Standard`), present iff any alias
    /// phrase is registered (ADR-061). The main (leftmost-longest) automaton reports only ONE match
    /// per span, so on the **title** side any phrase hidden by a longer overlapping one is lost —
    /// a false negative for a query that used the hidden phrase. Examples: a `new york` query vs a
    /// `new york city` title (nested aliases), or a stored `upper deck` (collapse-phrase) query vs
    /// an `upper deck gold` (alias) title. This second pass emits **every** phrase entity that
    /// occurs (the ES `synonym_graph` graph behavior), so none is hidden; `overlap_features[value]`
    /// is the `(entity, kind)` for each pattern. It indexes the SAME pattern order as the main
    /// automaton, so it covers alias, collapse, and additive phrases alike. Run on the title side
    /// only — a query collapses to its own longest entity. `None` ⇒ no alias phrases ⇒ the match
    /// path is byte-identical to before (the no-alias path is untouched).
    pub(super) overlap_automaton: Option<DoubleArrayAhoCorasick<usize>>,
    pub(super) overlap_features: Vec<(String, FeatureKind)>,

    pub(super) graders: Vec<String>,
    /// single-token synonyms -> (canonical feature, kind).
    pub(super) synonyms: Vec<(String, String, FeatureKind)>,
    pub(super) syn_index: std::collections::HashMap<String, usize>,
    pub(super) grade_words: Vec<String>,
    /// Byte-cleaning punctuation classification (ADR-058). Default = historical behavior.
    pub(super) punct: PunctTable,
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
    /// Create a [`NormalizerBuilder`](super::NormalizerBuilder) for assembling a custom vocabulary.
    pub fn builder() -> super::NormalizerBuilder {
        super::NormalizerBuilder::new()
    }

    /// Build the default trading-card vocabulary. Rich enough to exercise the
    /// spec's worked example and the synthetic generator; not exhaustive.
    ///
    /// Build a domain-agnostic normalizer with no pre-loaded vocabulary.
    ///
    /// The normalizer still handles year detection, number disambiguation,
    /// diacritic folding, and lowercase normalization. Domain-specific vocabulary
    /// (phrases, synonyms, graders, grade words) should be supplied via
    /// [`NormalizerBuilder`](super::NormalizerBuilder) or learned from query any-of groups at runtime.
    pub fn default_vocab() -> Result<Self, crate::error::NormalizerError> {
        super::NormalizerBuilder::new().build()
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
    pub fn emit<F: FnMut(&str, FeatureKind)>(
        &self,
        text: &str,
        lc: &mut String,
        query_side: bool,
        emit: &mut F,
    ) {
        self.clean_into(text, lc);

        // Title-side overlapping phrase pass (ADR-061): emit EVERY phrase entity that occurs, so
        // none is hidden by a longer overlapping match. The leftmost-longest automaton below reports
        // only one phrase per span, which would drop (a) a shorter-alias query against a longer-alias
        // title (`new york` vs `new york city`) and (b) a stored collapse-phrase query against a
        // longer-alias title (`upper deck` vs `upper deck gold`). This is the ES `synonym_graph` graph
        // behavior; the dedup in match_features folds the duplicate with the main pass. Title side
        // only — a query collapses to its own longest entity. `None` ⇒ no alias phrases ⇒ never
        // reached, so the no-alias match path is byte-identical to before.
        if !query_side {
            if let Some(aa) = &self.overlap_automaton {
                let bytes = lc.as_bytes();
                for m in aa.find_overlapping_iter(&**lc) {
                    let (start, end) = (m.start(), m.end());
                    let ok_start = start == 0 || bytes[start - 1] == b' ';
                    let ok_end = end == bytes.len() || bytes[end] == b' ';
                    if ok_start && ok_end {
                        let (feat, kind) = &self.overlap_features[m.value()];
                        emit(feat, *kind);
                    }
                }
            }
        }

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
                    // Whether to CONSUME the component tokens (emit only the phrase feature):
                    //  - collapse phrase (additive=false, alias=false): always consume.
                    //  - additive phrase (ADR-053): never consume — components are kept on both
                    //    sides (recall-preserving).
                    //  - alias entity (ADR-061): consume only on the QUERY side (collapse to the
                    //    entity feature, which equivalence expansion then widens to its synonyms);
                    //    keep components on the TITLE side (additive) so a component query still
                    //    matches. The ES `synonym_graph` asymmetry — safe because the title emits a
                    //    superset of what the query requires.
                    let consume = if entry.alias {
                        query_side
                    } else {
                        !entry.additive
                    };
                    if consume {
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
        self.emit(text, lc, true, &mut |name, kind| {
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
        self.emit(text, lc, true, &mut |name, _kind| {
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
        self.emit(text, lc, false, &mut |name, _kind| {
            tmp.push(dict.get_or_synthetic(name));
        });
        tmp.sort_unstable();
        tmp.dedup();
        out.extend_from_slice(&tmp);
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
