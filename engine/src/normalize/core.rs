//! `impl Normalizer` — the shared query/title normalization core.
//!
//! Hot path: yes — `emit` (and its public entry points `match_features` /
//! `compile_features` / `compile_features_readonly`) run per incoming title.
//! Holds the `Normalizer` struct definition, its byte-cleaning (`clean_into`),
//! the two-phase `emit` pipeline (daachorse multiword scan → grader/number/synonym
//! /generic tokenization), and the small free helpers `emit` relies on
//! (`fold_diacritic`, number/year/grade parsing, generic emission).

use super::{PhraseEntry, PhraseMode, PunctClass, PunctTable, Side};
use crate::dict::{Dict, FeatureId, FeatureKind};
use daachorse::DoubleArrayAhoCorasick;

pub struct Normalizer {
    /// daachorse automaton over space-joined phrase strings. Pattern value indexes
    /// into `phrase_entries`.
    pub(super) automaton: DoubleArrayAhoCorasick<usize>,
    pub(super) phrase_entries: Vec<PhraseEntry>,
    /// ADR-061: overlapping (`MatchKind::Standard`) automaton over the alias phrases, used on
    /// the title side to build the positive superset `P(T)`. `None` ⇒ no active multi-word
    /// alias ⇒ the title is single-view (`P(T) == N(T)`) and byte-identical to pre-ADR-061.
    pub(super) alias_overlap: Option<AliasOverlap>,

    pub(super) graders: Vec<String>,
    /// single-token synonyms -> (canonical feature, kind).
    pub(super) synonyms: Vec<(String, String, FeatureKind)>,
    pub(super) syn_index: std::collections::HashMap<String, usize>,
    pub(super) grade_words: Vec<String>,
    /// Byte-cleaning punctuation classification (ADR-058). Default = historical behavior.
    pub(super) punct: PunctTable,
}

/// The overlapping alias automaton + its per-pattern entity features (ADR-061). Built by
/// [`NormalizerBuilder::build`](super::NormalizerBuilder::build) from the alias-mode phrase
/// subset; consulted only on the title side by [`Normalizer::match_features_dual`].
pub(super) struct AliasOverlap {
    pub(super) automaton: DoubleArrayAhoCorasick<usize>,
    /// pattern index -> (entity feature name, kind).
    pub(super) entries: Vec<(String, FeatureKind)>,
}

impl AliasOverlap {
    /// Append the entity feature id of every word-boundary-aligned alias-phrase occurrence in
    /// the already-cleaned text `lc` (overlapping matches included) to `out`. Unknown entities
    /// hash to a stable synthetic id (ADR-046), exactly as the leftmost-longest pass resolves.
    fn collect_into(&self, lc: &str, dict: &Dict, out: &mut Vec<FeatureId>) {
        let bytes = lc.as_bytes();
        for m in self.automaton.find_overlapping_iter(lc) {
            let (s, e) = (m.start(), m.end());
            let ok_start = s == 0 || bytes[s - 1] == b' ';
            let ok_end = e == lc.len() || bytes[e] == b' ';
            if ok_start && ok_end {
                out.push(dict.get_or_synthetic(&self.entries[m.value()].0));
            }
        }
    }
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

    /// The cleaned whitespace tokens of `text` under this normalizer's punctuation table — the
    /// same tokenization the phrase automaton is registered against (ADR-061). A form cleans to
    /// **≥2** tokens iff it can be registered as a multi-word alias phrase (and so reduce to one
    /// entity); a 1-token form that does not resolve to exactly one feature cannot. Used by the
    /// alias classifier to keep an unexpressible form a review candidate rather than auto-activate
    /// a group `resolve_equivalences` would silently drop.
    #[must_use]
    pub fn clean_tokens(&self, text: &str) -> Vec<String> {
        alias_form_tokens(&self.punct, text)
    }

    /// True if any **multi-word alias** phrase is registered (ADR-061) — i.e. the title side
    /// produces a distinct positive superset view via [`match_features_dual`](Self::match_features_dual).
    /// When `false`, the two title views are always identical and every lane stays byte-identical
    /// to the pre-ADR-061 single-view path. Used to keep the broad lane on its two-view inline
    /// path while multi-word aliases are active.
    #[must_use]
    pub fn has_multiword_aliases(&self) -> bool {
        self.alias_overlap.is_some()
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
        clean_with(&self.punct, text, out);
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
        side: Side,
        emit: &mut F,
    ) {
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
                    // also produced (recall-preserving). Collapse phrases consume them. An
                    // alias phrase (ADR-061) is asymmetric: it collapses on the query side (so
                    // the form reduces to its single entity for ADR-054 expansion) but stays
                    // additive on the title side (so a component query still matches).
                    let consume = match entry.mode {
                        PhraseMode::Collapse => true,
                        PhraseMode::Additive => false,
                        PhraseMode::Alias => side == Side::Query,
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
        self.emit(text, lc, Side::Query, &mut |name, kind| {
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
        self.emit(text, lc, Side::Query, &mut |name, _kind| {
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
        self.emit(text, lc, Side::Title, &mut |name, _kind| {
            tmp.push(dict.get_or_synthetic(name));
        });
        tmp.sort_unstable();
        tmp.dedup();
        out.extend_from_slice(&tmp);
    }

    /// Match path producing the **two title feature views** of ADR-061:
    ///
    /// - `neg` = the canonical leftmost-longest set `N(T)` — the same set
    ///   [`match_features`](Self::match_features) produces. Used **only** for forbidden
    ///   (MUST_NOT) checks, so a forbidden clause stays recall-correct (`foo -"new york"`
    ///   still matches `foo new york city`).
    /// - `pos` = the overlapping superset `P(T) ⊇ N(T)` — `N(T)` plus every nested/overlapping
    ///   alias entity. Used for retrieval + required + any-of, so a `new york` query finds a
    ///   `new york city` title.
    ///
    /// With no active multi-word alias (`alias_overlap` is `None`), `P(T) == N(T)` and the two
    /// outputs are identical — the caller then passes one slice for both views and the
    /// verifier is byte-identical to the single-view path. Both outputs are sorted + deduped.
    pub fn match_features_dual(
        &self,
        text: &str,
        dict: &Dict,
        lc: &mut String,
        neg: &mut Vec<FeatureId>,
        pos: &mut Vec<FeatureId>,
    ) {
        neg.clear();
        pos.clear();
        let mut tmp: Vec<FeatureId> = Vec::new();
        // `emit` cleans `text` into `lc` first, so after this call `lc` holds the cleaned
        // text the overlap automaton scans below — no second clean.
        self.emit(text, lc, Side::Title, &mut |name, _kind| {
            tmp.push(dict.get_or_synthetic(name));
        });
        tmp.sort_unstable();
        tmp.dedup();
        neg.extend_from_slice(&tmp);

        match &self.alias_overlap {
            // No alias phrases: positive view == negative view.
            None => pos.extend_from_slice(&tmp),
            Some(ov) => {
                ov.collect_into(lc, dict, &mut tmp);
                tmp.sort_unstable();
                tmp.dedup();
                pos.extend_from_slice(&tmp);
            }
        }
    }
}

/// Byte-clean `text` into `out` (reused): lowercase + fold diacritics + apply the punctuation
/// table. Shared by [`Normalizer::clean_into`] (the hot path) and the builder's alias-phrase
/// registration (ADR-061), so an alias form is tokenized exactly as a title is — the phrase
/// pattern then matches the cleaned title text and the form resolves to its single entity.
pub(super) fn clean_with(punct: &PunctTable, text: &str, out: &mut String) {
    out.clear();
    for ch in text.chars() {
        let c = fold_diacritic(ch);
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else {
            match punct.class_of(c) {
                PunctClass::Split => push_space(out),
                PunctClass::Fold => {} // delete: neighbors join into one token
                PunctClass::Keep => out.push(c),
                PunctClass::Marker => {
                    push_space(out);
                    out.push(c);
                    out.push(' ');
                }
            }
        }
    }
}

/// Append a single word-separating space, **collapsing runs** — a no-op if `out` is empty or
/// already ends in a space. Multiple split characters or repeated whitespace (`new  york`,
/// `new---york`) therefore clean to a single space, so a registered phrase pattern (joined with
/// single spaces) matches the cleaned title (ADR-061). Token output is unchanged — the
/// downstream `split_whitespace` already collapsed runs; this only fixes the phrase automaton,
/// which scans the cleaned bytes literally.
#[inline]
fn push_space(out: &mut String) {
    if !out.is_empty() && !out.ends_with(' ') {
        out.push(' ');
    }
}

/// The cleaned whitespace tokens of an alias `form` under `punct` (ADR-061). Returns the same
/// token sequence the normalizer's phase-2 tokenizer sees, so a registered alias phrase pattern
/// aligns with cleaned title text. An empty result (all-punctuation form) registers nothing.
pub(super) fn alias_form_tokens(punct: &PunctTable, form: &str) -> Vec<String> {
    let mut buf = String::new();
    clean_with(punct, form, &mut buf);
    buf.split_whitespace().map(ToString::to_string).collect()
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
