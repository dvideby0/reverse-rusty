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

mod alias_overlap;
mod helpers;
pub(super) use alias_overlap::AliasOverlap;
pub use helpers::fold_diacritic;
use helpers::{
    age_active_graders, as_year, canon_grader, collapse_ws_runs_in_place, emit_generic,
    is_grade_value, parse_number,
};

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
    /// Number-context words (ADR-069): a number immediately after one of these tokens is
    /// demoted to a generic term (never typed as a year/grade). Default `["pop"]` = the
    /// historical hard-coded rule; empty = parity mode (position-insensitive number typing).
    /// Lowercased at build, so entries compare directly against cleaned tokens.
    pub(super) number_context: Vec<String>,
}

impl std::fmt::Debug for Normalizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Normalizer")
            .field("phrases", &self.phrase_entries.len())
            .field("graders", &self.graders)
            .field("synonyms", &self.synonyms.len())
            .field("grade_words", &self.grade_words)
            .field("number_context", &self.number_context)
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
        force_additive: bool,
        emit: &mut F,
    ) {
        self.clean_into(text, lc);

        // ADR-061 (codex R11): on the QUERY side, when multi-word aliases are active, collapse
        // whitespace runs before the phrase scan. Alias patterns are registered single-spaced, so
        // a run inside a quoted phrase (`"new  york"`) or any-of member would hide the alias from
        // the leftmost-longest automaton: the query compiles to component terms, equivalence
        // expansion never reaches the group, and `"new  york" mets` misses a `ny mets` title — a
        // false negative. Tokenization is whitespace-agnostic, so token features are unchanged;
        // only phrase alignment can differ — and a title-with-runs still matches a collapsed query
        // entity through the `P(T)` overlap scan, which collapses runs itself. The title side
        // keeps `lc` VERBATIM (codex R8: persisted canonical normalization must not change), and
        // the gate on `alias_overlap` keeps the no-alias configuration byte-identical.
        if side == Side::Query && self.alias_overlap.is_some() {
            collapse_ws_runs_in_place(lc);
        }

        // Phase 1: find multiword phrase matches via the automaton.
        // We collect (byte_start, byte_end, pattern_index) for each match.
        // The automaton operates on the cleaned string, matching space-joined
        // token sequences. We need to ensure matches align on word boundaries.
        let mut phrase_matches: Vec<(usize, usize, usize)> = Vec::new();
        if let Some(ov) = &self.alias_overlap {
            // ADR-061 (codex R12): with multi-word aliases active, boundary validity must
            // participate in match SELECTION — see `AliasOverlap::select_phrases`. The legacy
            // pass below commits to a boundary-invalid mid-token match and lets it suppress a
            // valid overlapping phrase (a query-side FN). Gated on `alias_overlap`, so the
            // no-alias configuration keeps the legacy pass byte-identical (its pathological
            // collapse-phrase cases are baked into persisted canonical features — codex R8).
            ov.select_phrases(lc, &mut phrase_matches);
        } else {
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
                    // `force_additive` (the positive title view `P(T)`, ADR-061) consumes nothing,
                    // so EVERY token also reaches phase 2b — the maximal, parse-union feature set
                    // that keeps a component query matchable even when its phrase is displaced from
                    // the leftmost-longest parse by an overlapping one (codex R7).
                    let consume = !force_additive
                        && match entry.mode {
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
        // ADR-061 positive view (`force_additive` ⇒ P(T)) only: a single `pending_grader` cannot
        // express the parse-union of grades — a parse that consumes a phrase can FREE an earlier
        // grader to grade a later number, and a second grader OVERWRITES the pending one. So in the
        // positive pass we track EVERY grader still in window and grade each number with all of
        // them. The query/compile and single-view title paths keep the single-pending semantics
        // (byte-identical) and this Vec stays empty ⇒ `age_active_graders` is a no-op, no alloc.
        let mut active_graders: Vec<(String, u8)> = Vec::new();

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
                age_active_graders(&mut active_graders);
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
                let fused = rest.is_some();
                if let Some(num) = rest {
                    Self::emit_grade(&gcanon, &num, &mut scratch, emit);
                }
                if force_additive {
                    // Positive view: keep this grader active (don't overwrite earlier ones), so a
                    // later number grades with it too — the parse-union over which graders a parse
                    // frees by consuming the others. A grader token ages nothing (matches the
                    // single-pending path, where the new grader resets only its own age).
                    // Deduped per CANONICAL grader, refreshing the age (codex R12): the freshest
                    // occurrence outlives any older same-name one, so the parse-union superset is
                    // strictly preserved, while the set stays bounded by the (small) distinct
                    // grader vocabulary — repeated grader tokens otherwise grow the set without
                    // bound and every number then emits per entry (a quadratic crafted-title DoS).
                    if let Some(entry) = active_graders.iter_mut().find(|(g, _)| *g == gcanon) {
                        entry.1 = 0;
                    } else {
                        active_graders.push((gcanon, 0));
                    }
                } else if fused {
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
                age_active_graders(&mut active_graders);
                i += 1;
                continue;
            }

            // 3) numbers: disambiguate card-numbers, serials, number-context words
            //    (default `pop`, configurable — ADR-069), grades, years
            if let Some(numstr) = parse_number(tok) {
                let prev = if i > 0 { Some(tokens[i - 1]) } else { None };
                let next = tokens.get(i + 1).copied();
                let is_cardnum = prev == Some("#");
                let is_serial = prev == Some("/") || next == Some("/");
                let is_numctx = prev.is_some_and(|p| {
                    self.number_context
                        .iter()
                        .any(|w| p.eq_ignore_ascii_case(w))
                });

                if is_cardnum || is_serial || is_numctx {
                    emit_generic(&numstr, &mut scratch, emit);
                } else if let Some(y) = as_year(&numstr) {
                    scratch.clear();
                    scratch.push_str("year:");
                    scratch.push_str(&y);
                    emit(&scratch, FeatureKind::Year);
                } else if force_additive {
                    // Positive view (P(T)) parse-union: grade this number with EVERY active grader
                    // still in window AND the grade context, all STICKY (never cleared by this
                    // number). A number consumed by a phrase in some parse frees a grader for a
                    // later number, and a second grader overwrites the pending one — both readings
                    // live here, so P(T) keeps every grade any parse could emit. Over-emitting a
                    // grade no single parse produces is a bounded false positive (recall-safe).
                    let gradeable = is_grade_value(&numstr);
                    let mut graded = false;
                    if gradeable {
                        for (g, _) in &active_graders {
                            Self::emit_grade(g, &numstr, &mut scratch, emit);
                            graded = true;
                        }
                        if grade_ctx {
                            scratch.clear();
                            scratch.push_str("grade:");
                            scratch.push_str(&numstr);
                            emit(&scratch, FeatureKind::Grade);
                            graded = true;
                        }
                    }
                    if !graded {
                        emit_generic(&numstr, &mut scratch, emit);
                    }
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
            age_active_graders(&mut active_graders);
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
        self.emit(text, lc, Side::Query, false, &mut |name, kind| {
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
        self.emit(text, lc, Side::Query, false, &mut |name, _kind| {
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
        self.emit(text, lc, Side::Title, false, &mut |name, _kind| {
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
    /// - `pos` = the **maximal positive view** `P(T) ⊇ N(T)`. Computed as the parse-union: a second
    ///   emit with **all phrases forced additive** (nothing consumed ⇒ every token feature plus
    ///   every leftmost-longest entity) ∪ the **overlapping** entity pass. So `P(T)` contains every
    ///   feature any parse could emit — every nested/overlapping alias entity AND the component
    ///   tokens of a phrase displaced from the leftmost-longest parse. Used for retrieval +
    ///   required + any-of, so a `new york` query finds a `new york city` title and a component
    ///   query is never dropped. A strict superset of every parse ⇒ FN-safe; it only ever adds to
    ///   the positive view (a wider positive read is a bounded false positive, never a negative).
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
        // N(T): the canonical leftmost-longest parse (phrase modes respected). `emit` cleans
        // `text` into `lc` first.
        let mut tmp: Vec<FeatureId> = Vec::new();
        self.emit(text, lc, Side::Title, false, &mut |name, _kind| {
            tmp.push(dict.get_or_synthetic(name));
        });
        tmp.sort_unstable();
        tmp.dedup();
        neg.extend_from_slice(&tmp);

        match &self.alias_overlap {
            // No alias phrases: positive view == negative view (single-view fast path elsewhere).
            None => pos.extend_from_slice(&tmp),
            Some(ov) => {
                // P(T) = N(T) ∪ force-additive parse-union ∪ raw token features ∪ overlapping
                // entities. `tmp` already holds N(T); only ever ADD (never replace), so P(T) is a
                // strict superset of every parse and activating an alias can never drop a feature.
                // The force-additive re-emit recovers components of a displaced additive phrase; it
                // can, however, change a *stateful* token read (a grader un-consumed from a phrase
                // turns a trailing `10` from `term:10` into `grade:10`), so we also add every cleaned
                // token's RAW `term:<token>` reading below — the generic feature a stateful re-parse
                // would otherwise drop (codex R7/R8/R9). The force-additive pass also tracks ALL
                // active graders (see `emit`'s `active_graders`): each number grades with every
                // grader still in window, so a number consumed by a phrase in some parse cannot hide
                // a later grade from P(T) and a second grader does not overwrite the first (the
                // "Goldilocks parse" — `psa 9 lives 8` reads `psa 8` once `9 lives` collapses;
                // `psa a bgs 8` reads `psa 8` once `a bgs` collapses). The second `emit` re-cleans
                // into `lc`, leaving it holding the text the overlap pass + token scan use.
                self.emit(text, lc, Side::Title, true, &mut |name, _kind| {
                    tmp.push(dict.get_or_synthetic(name));
                });
                let mut name = String::from("term:");
                for tok in lc.split_whitespace() {
                    if tok == "#" || tok == "/" {
                        continue; // structural markers, never a term feature
                    }
                    name.truncate(5); // keep the "term:" prefix
                    name.push_str(tok);
                    tmp.push(dict.get_or_synthetic(&name));
                }
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
/// registration (ADR-061). **Whitespace runs are NOT collapsed** — the cleaned text is verbatim,
/// so this is byte-identical across versions and a persisted segment's features never desync on a
/// binary upgrade (codex R8). Matching an alias against a title with whitespace runs is instead
/// handled, recall-safely, by the positive-view overlap scan ([`AliasOverlap::collect_into`]).
pub(super) fn clean_with(punct: &PunctTable, text: &str, out: &mut String) {
    out.clear();
    for ch in text.chars() {
        let c = fold_diacritic(ch);
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else {
            match punct.class_of(c) {
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

/// The cleaned whitespace tokens of an alias `form` under `punct` (ADR-061). Returns the same
/// token sequence the normalizer's phase-2 tokenizer sees, so a registered alias phrase pattern
/// aligns with cleaned title text. An empty result (all-punctuation form) registers nothing.
pub(super) fn alias_form_tokens(punct: &PunctTable, form: &str) -> Vec<String> {
    let mut buf = String::new();
    clean_with(punct, form, &mut buf);
    buf.split_whitespace().map(ToString::to_string).collect()
}
