//! The normalization pipeline: cleaned text -> canonical features, and the ADR-061 two title
//! views. An independent reimplementation of `engine/src/normalize/core.rs::emit` /
//! `match_features` / `match_features_dual`.
//!
//! Two phases (mirroring the engine): (1) find boundary-valid leftmost-longest phrase matches;
//! (2) tokenize and run each non-phrase token through the grader / grade-context / number /
//! synonym / generic pipeline. The grader/grade state machine's aging is subtle and reproduced
//! exactly: the pending-grader window (`> 3`) and grade-context window (`> 2`) advance only on a
//! consumed (phrase) token, a grade-context word (no clear), or a generic fallback token — NOT on
//! markers, grader tokens, number tokens, or synonym tokens.

use crate::clean::clean;
use crate::features::Feature;
use crate::phrases;
use crate::vocab::{PhraseMode, RefVocab};

/// Which side is being normalized (the query/compile side collapses whitespace runs before the
/// phrase scan when aliases are active; the title side keeps cleaned text verbatim).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Side {
    Query,
    Title,
}

// ---- pure helpers (translated from core/helpers.rs) ----

/// Parse a token into a clean numeric string (digits with optional single `.`), or `None`.
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
    seen_digit.then(|| tok.to_string())
}

/// A 4-digit number in 1900..=2099 is a year (the engine's bound — note: 2099, not 2100).
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

/// A number in 1.0..=10.0 can be a grade value.
fn is_grade_value(num: &str) -> bool {
    num.parse::<f32>().is_ok_and(|v| (1.0..=10.0).contains(&v))
}

/// Canonicalize a grader name (`beckett` -> `bgs`).
fn canon_grader(g: &str) -> String {
    if g == "beckett" {
        "bgs".to_string()
    } else {
        g.to_string()
    }
}

/// Split a possibly-fused grader token like `psa10` -> `("psa", Some("10"))`. The first registered
/// grader that matches wins (so registration order is significant, as in the engine).
fn split_grader(graders: &[String], tok: &str) -> Option<(String, Option<String>)> {
    for g in graders {
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

/// Emit `grade:<n>` + `grader_grade:<g><n>` (the engine's `emit_grade`).
fn emit_grade(out: &mut Vec<Feature>, grader: &str, num: &str) {
    out.push(Feature::grade(num));
    out.push(Feature::grader_grade(grader, num));
}

/// Age every active positive-view grader one step, dropping those past the `> 3` window
/// (`age_active_graders`). A no-op on the empty Vec the single-view paths always hold.
fn age_active_graders(active: &mut Vec<(String, u8)>) {
    if active.is_empty() {
        return;
    }
    active.retain_mut(|(_, age)| {
        *age = age.saturating_add(1);
        *age <= 3
    });
}

/// Tokenize cleaned text into `(start, end)` byte ranges, splitting on ASCII space (cleaning has
/// already mapped every other whitespace to a space).
fn tokenize(lc: &str) -> Vec<(usize, usize)> {
    let bytes = lc.as_bytes();
    let mut tokens = Vec::new();
    let mut pos = 0;
    while pos < bytes.len() {
        while pos < bytes.len() && bytes[pos] == b' ' {
            pos += 1;
        }
        if pos >= bytes.len() {
            break;
        }
        let start = pos;
        while pos < bytes.len() && bytes[pos] != b' ' {
            pos += 1;
        }
        tokens.push((start, pos));
    }
    tokens
}

fn slice(lc: &str, r: (usize, usize)) -> &str {
    &lc[r.0..r.1]
}

/// Emit canonical features for `text` under `vocab`. With `force_additive` (the positive view
/// `P(T)`), nothing is consumed by a phrase and every grader stays active so each number grades
/// with all of them (the parse-union). Faithful translation of `core.rs::emit`.
#[must_use]
pub fn emit(vocab: &RefVocab, text: &str, side: Side, force_additive: bool) -> Vec<Feature> {
    let mut out = Vec::new();
    let mut lc = clean(text, &vocab.punct);
    let has_aliases = vocab.has_multiword_aliases();
    // Query side, aliases active: collapse runs so a single-spaced alias pattern still aligns.
    if side == Side::Query && has_aliases {
        lc = phrases::collapse_ws_runs(&lc);
    }

    // Phase 1: boundary-aware leftmost-longest phrase matches (empty without phrases).
    let phrase_matches = if vocab.phrases.is_empty() {
        Vec::new()
    } else {
        phrases::select_leftmost_longest(&lc, &vocab.phrases)
    };

    let tokens = tokenize(&lc);

    // Phase 2a: emit each matched phrase's entity once; mark its tokens consumed (per mode, unless
    // force_additive consumes nothing).
    let mut token_consumed = vec![false; tokens.len()];
    let mut phrase_emitted = vec![false; phrase_matches.len()];
    for ti in 0..tokens.len() {
        let (tstart, tend) = tokens[ti];
        for (pi, &(ps, pe, idx)) in phrase_matches.iter().enumerate() {
            if tstart >= ps && tend <= pe {
                let entry = &vocab.phrases[idx];
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
                    out.push(Feature::raw(entry.feature.clone()));
                }
                break;
            }
        }
    }

    // Phase 2b: the token pipeline.
    let mut i = 0;
    let mut pending_grader: Option<String> = None;
    let mut pending_grader_age = 0u8;
    let mut grade_ctx = false;
    let mut grade_ctx_age = 0u8;
    let mut active_graders: Vec<(String, u8)> = Vec::new();

    while i < tokens.len() {
        if token_consumed[i] {
            // Phrase-consumed token: still age out the pending grader / grade context.
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

        let tok = slice(&lc, tokens[i]);

        // 0) structural markers from cleaning: skip (no aging).
        if tok == "#" || tok == "/" {
            i += 1;
            continue;
        }

        // 1) grader keyword (possibly fused like "psa10").
        if let Some((g, rest)) = split_grader(&vocab.graders, tok) {
            let gcanon = canon_grader(&g);
            out.push(Feature::grader(&gcanon));
            let fused = rest.is_some();
            if let Some(num) = rest {
                emit_grade(&mut out, &gcanon, &num);
            }
            if force_additive {
                // Positive view: keep this grader active, refreshing the age of a same-name entry.
                if let Some(entry) = active_graders.iter_mut().find(|(gg, _)| *gg == gcanon) {
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

        // 2) grade modifier / context word.
        if vocab.grade_words.iter().any(|w| w == tok) {
            grade_ctx = true;
            grade_ctx_age = 0;
            if pending_grader.is_some() {
                pending_grader_age = pending_grader_age.saturating_add(1);
            }
            age_active_graders(&mut active_graders);
            i += 1;
            continue;
        }

        // 3) numbers: card-numbers, serials, number-context words, grades, years.
        if let Some(numstr) = parse_number(tok) {
            let prev = if i > 0 {
                Some(slice(&lc, tokens[i - 1]))
            } else {
                None
            };
            let next = tokens.get(i + 1).map(|&r| slice(&lc, r));
            let is_cardnum = prev == Some("#");
            let is_serial = prev == Some("/") || next == Some("/");
            let is_numctx = prev.is_some_and(|p| {
                vocab
                    .number_context
                    .iter()
                    .any(|w| p.eq_ignore_ascii_case(w))
            });

            if is_cardnum || is_serial || is_numctx {
                out.push(Feature::term(&numstr));
            } else if let Some(y) = as_year(&numstr) {
                out.push(Feature::year(&y));
            } else if force_additive {
                // Positive view: grade with EVERY active grader still in window AND grade context,
                // all sticky (never cleared by this number).
                let gradeable = is_grade_value(&numstr);
                let mut graded = false;
                if gradeable {
                    for (g, _) in &active_graders {
                        emit_grade(&mut out, g, &numstr);
                        graded = true;
                    }
                    if grade_ctx {
                        out.push(Feature::grade(&numstr));
                        graded = true;
                    }
                }
                if !graded {
                    out.push(Feature::term(&numstr));
                }
            } else if let Some(g) = pending_grader.clone() {
                if is_grade_value(&numstr) {
                    emit_grade(&mut out, &g, &numstr);
                    pending_grader = None;
                } else {
                    out.push(Feature::term(&numstr));
                }
            } else if grade_ctx && is_grade_value(&numstr) {
                out.push(Feature::grade(&numstr));
                grade_ctx = false;
            } else {
                out.push(Feature::term(&numstr));
            }
            i += 1;
            continue;
        }

        // 4) closed-vocab synonym.
        if let Some(syn) = vocab.synonyms.iter().find(|s| s.token == tok) {
            out.push(Feature::raw(syn.canonical.clone()));
            i += 1;
            continue;
        }

        // 5) generic fallback term.
        out.push(Feature::term(tok));
        i += 1;

        // Age out stale pending grader / grade context (only after a generic token).
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

    out
}

/// The canonical leftmost-longest feature set `N(T)` (sorted + deduped). Used for forbidden checks.
#[must_use]
pub fn match_features(vocab: &RefVocab, text: &str) -> Vec<Feature> {
    let mut v = emit(vocab, text, Side::Title, false);
    v.sort();
    v.dedup();
    v
}

/// The two title views (ADR-061): `neg` = canonical `N(T)` (forbidden checks), `pos` = the maximal
/// positive superset `P(T) ⊇ N(T)` (retrieval + required + any-of). With no active multi-word
/// alias the two are identical. Translation of `core.rs::match_features_dual`.
#[must_use]
pub fn match_features_dual(vocab: &RefVocab, text: &str) -> (Vec<Feature>, Vec<Feature>) {
    let mut neg = emit(vocab, text, Side::Title, false);
    neg.sort();
    neg.dedup();

    if !vocab.has_multiword_aliases() {
        let pos = neg.clone();
        return (neg, pos);
    }

    // P(T) = N(T) ∪ force-additive parse-union ∪ raw term:<token> ∪ overlapping entities.
    let mut pos = neg.clone();
    pos.extend(emit(vocab, text, Side::Title, true));

    // The title side keeps cleaned text verbatim (no whitespace-run collapse).
    let lc = clean(text, &vocab.punct);
    for tok in lc.split_whitespace() {
        if tok == "#" || tok == "/" {
            continue; // structural markers, never a term feature
        }
        pos.push(Feature::term(tok));
    }
    for idx in phrases::scan_overlapping(&lc, &vocab.phrases) {
        pos.push(Feature::raw(vocab.phrases[idx].feature.clone()));
    }

    pos.sort();
    pos.dedup();
    (neg, pos)
}
