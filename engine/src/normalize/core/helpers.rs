//! Pure, `self`-free helpers for the normalization core: diacritic folding,
//! the generic `term:` emit, and number/year parsing. Split out of `core.rs` to keep that file
//! focused on the `Normalizer` struct + the two-phase `emit` pipeline.

use crate::dict::FeatureKind;

/// Fold common Latin diacritics to ASCII so "Jokić"->"jokic", "Jalapeño"->"jalapeno".
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

pub(super) fn emit_generic<F: FnMut(&str, FeatureKind, u32, u32)>(
    tok: &str,
    scratch: &mut String,
    start: u32,
    end: u32,
    emit: &mut F,
) {
    scratch.clear();
    scratch.push_str("term:");
    scratch.push_str(tok);
    emit(scratch, FeatureKind::Generic, start, end);
}

/// Collapse whitespace runs in place (and strip a leading space). Phrase patterns are registered
/// single-spaced, so a run inside the cleaned text hides a phrase from the automaton. Flat
/// normalization applies this only to alias-enabled queries (ADR-061); ADR-120 positioned
/// normalization applies it symmetrically to query and title graphs. Flat title-side runs remain
/// handled by the additive overlap scan (`PhraseOverlap::collect_into`).
pub(super) fn collapse_ws_runs_in_place(s: &mut String) {
    let mut prev_space = true; // initial `true` also strips a leading space
    s.retain(|c| {
        let keep = c != ' ' || !prev_space;
        prev_space = c == ' ';
        keep
    });
}

/// Parse a token into a clean numeric string (digits with optional .5), or None.
pub(super) fn parse_number(tok: &str) -> Option<String> {
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

pub(super) fn as_year(num: &str) -> Option<String> {
    if num.len() == 4 && !num.contains('.') {
        if let Ok(y) = num.parse::<u32>() {
            if (1900..=2099).contains(&y) {
                return Some(num.to_string());
            }
        }
    }
    None
}
