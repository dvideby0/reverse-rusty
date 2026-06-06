//! Pure, `self`-free helpers for the normalization core: diacritic folding, grader
//! canonicalization, the generic `term:` emit, the positive-view active-grader aging
//! (ADR-061), and number/year/grade parsing. Split out of `core.rs` to keep that file
//! focused on the `Normalizer` struct + the two-phase `emit` pipeline.

use crate::dict::FeatureKind;

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

pub(super) fn canon_grader(g: &str) -> String {
    match g {
        "beckett" => "bgs".to_string(),
        other => other.to_string(),
    }
}

pub(super) fn emit_generic<F: FnMut(&str, FeatureKind)>(
    tok: &str,
    scratch: &mut String,
    emit: &mut F,
) {
    scratch.clear();
    scratch.push_str("term:");
    scratch.push_str(tok);
    emit(scratch, FeatureKind::Generic);
}

/// Age every active positive-view grader (ADR-061 `P(T)`) one window step, dropping those past the
/// grader window (`> 3`, the same bound `pending_grader` uses). Called wherever `pending_grader` is
/// aged. A no-op with no allocation on the empty Vec the query/compile and single-view title paths
/// always hold — only the positive (`force_additive`) pass ever populates it.
pub(super) fn age_active_graders(active: &mut Vec<(String, u8)>) {
    if active.is_empty() {
        return;
    }
    active.retain_mut(|(_, age)| {
        *age = age.saturating_add(1);
        *age <= 3
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

pub(super) fn is_grade_value(num: &str) -> bool {
    if let Ok(v) = num.parse::<f32>() {
        (1.0..=10.0).contains(&v)
    } else {
        false
    }
}
