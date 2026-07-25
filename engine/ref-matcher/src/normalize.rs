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

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RefPositionArc {
    pub feature: Feature,
    pub start: u32,
    pub end: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RefPhraseArc {
    pub start: u32,
    pub end: u32,
    pub alternatives: Vec<Feature>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct RefPhraseGraph {
    pub positions: u32,
    pub arcs: Vec<RefPhraseArc>,
}

#[inline]
fn position_index(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
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
fn emit_grade(
    out: &mut Vec<RefPositionArc>,
    grader: &str,
    num: &str,
    grader_start: u32,
    start: u32,
    end: u32,
) {
    out.push(RefPositionArc {
        feature: Feature::grade(num),
        start,
        end,
    });
    out.push(RefPositionArc {
        feature: Feature::grader_grade(grader, num),
        start: grader_start,
        end,
    });
}

fn push_feature(out: &mut Vec<RefPositionArc>, feature: Feature, start: u32, end: u32) {
    out.push(RefPositionArc {
        feature,
        start,
        end,
    });
}

/// Age every active positive-view grader one step, dropping those past the `> 3` window
/// (`age_active_graders`). A no-op on the empty Vec the single-view paths always hold.
fn age_active_graders(active: &mut Vec<(String, u8, u32)>) {
    if active.is_empty() {
        return;
    }
    active.retain_mut(|(_, age, _)| {
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
    emit_positioned(vocab, text, side, force_additive)
        .1
        .into_iter()
        .map(|arc| arc.feature)
        .collect()
}

/// Positioned analyzer translation used only by the independent ADR-120 phrase
/// oracle. It shares no engine code or automata.
#[must_use]
pub fn emit_positioned(
    vocab: &RefVocab,
    text: &str,
    side: Side,
    force_additive: bool,
) -> (u32, Vec<RefPositionArc>) {
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
                    let mut end_pos = ti + 1;
                    while end_pos < tokens.len() && tokens[end_pos].1 <= pe {
                        end_pos += 1;
                    }
                    push_feature(
                        &mut out,
                        Feature::raw(entry.feature.clone()),
                        position_index(ti),
                        position_index(end_pos),
                    );
                }
                break;
            }
        }
    }

    // Phase 2b: the token pipeline.
    let mut i = 0;
    let mut pending_grader: Option<(String, u32)> = None;
    let mut pending_grader_age = 0u8;
    let mut grade_ctx = false;
    let mut grade_ctx_age = 0u8;
    let mut active_graders: Vec<(String, u8, u32)> = Vec::new();

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
            push_feature(
                &mut out,
                Feature::grader(&gcanon),
                position_index(i),
                position_index(i.saturating_add(1)),
            );
            let fused = rest.is_some();
            if let Some(num) = rest {
                emit_grade(
                    &mut out,
                    &gcanon,
                    &num,
                    position_index(i),
                    position_index(i),
                    position_index(i.saturating_add(1)),
                );
            }
            if force_additive {
                // Positioned parse-union: retain every live same-name start.
                // An overlapping phrase may consume a later grader while an
                // earlier occurrence remains connected to the grade edge.
                active_graders.push((gcanon, 0, position_index(i)));
            } else if fused {
                pending_grader = None;
            } else {
                pending_grader = Some((gcanon, position_index(i)));
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
                push_feature(
                    &mut out,
                    Feature::term(&numstr),
                    position_index(i),
                    position_index(i.saturating_add(1)),
                );
            } else if let Some(y) = as_year(&numstr) {
                push_feature(
                    &mut out,
                    Feature::year(&y),
                    position_index(i),
                    position_index(i.saturating_add(1)),
                );
            } else if force_additive {
                // Positive view: grade with EVERY active grader still in window AND grade context,
                // all sticky (never cleared by this number).
                let gradeable = is_grade_value(&numstr);
                let mut graded = false;
                if gradeable {
                    for (g, _, grader_start) in &active_graders {
                        emit_grade(
                            &mut out,
                            g,
                            &numstr,
                            *grader_start,
                            position_index(i),
                            position_index(i.saturating_add(1)),
                        );
                        graded = true;
                    }
                    if grade_ctx {
                        push_feature(
                            &mut out,
                            Feature::grade(&numstr),
                            position_index(i),
                            position_index(i.saturating_add(1)),
                        );
                        graded = true;
                    }
                }
                if !graded {
                    push_feature(
                        &mut out,
                        Feature::term(&numstr),
                        position_index(i),
                        position_index(i.saturating_add(1)),
                    );
                }
            } else if let Some((g, grader_start)) = pending_grader.clone() {
                if is_grade_value(&numstr) {
                    emit_grade(
                        &mut out,
                        &g,
                        &numstr,
                        grader_start,
                        position_index(i),
                        position_index(i.saturating_add(1)),
                    );
                    pending_grader = None;
                } else {
                    push_feature(
                        &mut out,
                        Feature::term(&numstr),
                        position_index(i),
                        position_index(i.saturating_add(1)),
                    );
                }
            } else if grade_ctx && is_grade_value(&numstr) {
                push_feature(
                    &mut out,
                    Feature::grade(&numstr),
                    position_index(i),
                    position_index(i.saturating_add(1)),
                );
                grade_ctx = false;
            } else {
                push_feature(
                    &mut out,
                    Feature::term(&numstr),
                    position_index(i),
                    position_index(i.saturating_add(1)),
                );
            }
            i += 1;
            continue;
        }

        // 4) closed-vocab synonym.
        if let Some(syn) = vocab.synonyms.iter().find(|s| s.token == tok) {
            push_feature(
                &mut out,
                Feature::raw(syn.canonical.clone()),
                position_index(i),
                position_index(i.saturating_add(1)),
            );
            i += 1;
            continue;
        }

        // 5) generic fallback term.
        push_feature(
            &mut out,
            Feature::term(tok),
            position_index(i),
            position_index(i.saturating_add(1)),
        );
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

    (position_index(tokens.len()), out)
}

fn filled_position_arcs(
    vocab: &RefVocab,
    text: &str,
    side: Side,
    force_additive: bool,
) -> (u32, Vec<RefPositionArc>) {
    // Quoted query phrases are whitespace-insensitive independently of alias
    // activation. Keep flat `emit` unchanged; this normalization belongs only
    // to the positioned reference path.
    let normalized_query;
    let analysis_text = if side == Side::Query {
        normalized_query = phrases::collapse_ws_runs(&clean(text, &vocab.punct));
        normalized_query.as_str()
    } else {
        text
    };
    let (positions, mut arcs) = emit_positioned(vocab, analysis_text, side, force_additive);
    // The flat analyzer's grader window may span intervening words, but a
    // quoted graph must not let that composite edge bypass those positions.
    // Preserve only fused (`psa10`) and adjacent (`psa 10`) shortcuts.
    arcs.retain(|arc| {
        !arc.feature.as_str().starts_with("grader_grade:") || arc.end.saturating_sub(arc.start) <= 2
    });
    arcs.sort();
    arcs.dedup();

    let lc = clean(analysis_text, &vocab.punct);
    let raw_tokens: Vec<&str> = lc.split_whitespace().collect();
    for i in 0..positions {
        let has_start = arcs.iter().any(|arc| arc.start == i);
        let covered = arcs.iter().any(|arc| arc.start < i && arc.end > i);
        if !has_start && !covered {
            push_feature(&mut arcs, Feature::term(raw_tokens[i as usize]), i, i + 1);
        }
    }
    arcs.sort_by(|a, b| (a.start, a.end, &a.feature).cmp(&(b.start, b.end, &b.feature)));
    arcs.dedup();
    (positions, arcs)
}

/// Query-side analyzed graph for one quoted clause.
#[must_use]
pub fn compile_phrase(vocab: &RefVocab, text: &str) -> RefPhraseGraph {
    let (positions, mut arcs) = filled_position_arcs(vocab, text, Side::Query, false);
    let composites: Vec<RefPositionArc> = arcs
        .iter()
        .filter(|arc| arc.feature.as_str().starts_with("grader_grade:"))
        .cloned()
        .collect();
    if !composites.is_empty() {
        arcs.retain(|arc| {
            let composite_span = composites
                .iter()
                .any(|composite| (composite.start, composite.end) == (arc.start, arc.end));
            !composite_span || composites.contains(arc)
        });
    }
    let mut grouped: Vec<RefPhraseArc> = Vec::new();
    for arc in arcs {
        if let Some(last) = grouped.last_mut() {
            if last.start == arc.start && last.end == arc.end {
                last.alternatives.push(arc.feature);
                continue;
            }
        }
        grouped.push(RefPhraseArc {
            start: arc.start,
            end: arc.end,
            alternatives: vec![arc.feature],
        });
    }
    RefPhraseGraph {
        positions,
        arcs: grouped,
    }
}

/// Phrase-aware title flat views + canonical/positive token graphs.
#[must_use]
pub fn match_phrase_views(
    vocab: &RefVocab,
    text: &str,
) -> (
    Vec<Feature>,
    Vec<Feature>,
    u32,
    Vec<RefPositionArc>,
    Vec<RefPositionArc>,
) {
    let (neg, pos) = match_features_dual(vocab, text);
    let (positions, neg_arcs) = filled_position_arcs(vocab, text, Side::Title, false);
    let (_, mut pos_arcs) = filled_position_arcs(vocab, text, Side::Title, true);
    let lc = clean(text, &vocab.punct);
    pos_arcs.extend(neg_arcs.iter().cloned());
    for (i, token) in lc.split_whitespace().enumerate() {
        if token == "#" || token == "/" {
            continue;
        }
        pos_arcs.push(RefPositionArc {
            feature: Feature::term(token),
            start: position_index(i),
            end: position_index(i.saturating_add(1)),
        });
    }
    for (start, end, idx) in phrases::scan_overlapping_spans(&lc, &vocab.phrases) {
        pos_arcs.push(RefPositionArc {
            feature: Feature::raw(vocab.phrases[idx].feature.clone()),
            start,
            end,
        });
    }
    pos_arcs.sort_by(|a, b| (a.start, a.end, &a.feature).cmp(&(b.start, b.end, &b.feature)));
    pos_arcs.dedup();
    (neg, pos, positions, neg_arcs, pos_arcs)
}

/// Independent graph-language intersection for quoted clauses.
#[must_use]
pub fn phrase_graph_matches(
    query: &RefPhraseGraph,
    title_positions: u32,
    title: &[RefPositionArc],
) -> bool {
    if query.positions == 0 || query.arcs.is_empty() {
        return false;
    }
    let mut stack = Vec::new();
    let mut seen = Vec::new();
    for title_start in 0..title_positions {
        stack.push((0u32, title_start));
        seen.push((0u32, title_start));
    }
    while let Some((query_node, title_node)) = stack.pop() {
        if query_node == query.positions {
            return true;
        }
        for query_arc in query.arcs.iter().filter(|arc| arc.start == query_node) {
            for title_arc in title.iter().filter(|arc| arc.start == title_node) {
                if query_arc
                    .alternatives
                    .binary_search(&title_arc.feature)
                    .is_err()
                {
                    continue;
                }
                let next = (query_arc.end, title_arc.end);
                if !seen.contains(&next) {
                    seen.push(next);
                    stack.push(next);
                }
            }
        }
    }
    false
}

/// The canonical leftmost-longest feature set `N(T)` (sorted + deduped). Used for forbidden checks.
#[must_use]
pub fn match_features(vocab: &RefVocab, text: &str) -> Vec<Feature> {
    let mut v = emit(vocab, text, Side::Title, false);
    v.sort();
    v.dedup();
    v
}

/// The two semantic title views (ADR-061): `neg` = canonical `N(T)` (forbidden checks), `pos` =
/// the maximal flat positive superset `P(T) ⊇ N(T)` (flat retrieval + required + any-of).
/// Phrase-aware callers build their candidate-only graph-label probe separately. With no active
/// multi-word alias the two are identical. Translation of `core.rs::match_features_dual`.
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
