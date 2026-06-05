//! Solr / Lucene synonym-file parser (ADR-060 item 3).
//!
//! Parses the format ES's `synonyms_path` consumes into raw equivalence groups. The registry
//! then classifies + governs them; this module only turns text into groups.
//!
//! Supported syntax:
//!   - `# comment` lines and blank lines are ignored.
//!   - `a, b, c`            — a comma-separated list of equivalent forms (one group).
//!   - `a, b => c, d`       — an explicit mapping. Solr's `=>` is *directional* (LHS replaced
//!     by RHS); RR equivalences are **bidirectional** (expansion, ADR-054), so both sides are
//!     unioned into one group. That is a recall-safe over-approximation — it can only widen a
//!     query's match set, never drop a match (the engine's cardinal rule).
//!   - `\,` is a literal comma inside a form (Solr's escape).

/// Parse Solr/Lucene synonym text into equivalence groups of raw surface forms. Each group has
/// its forms trimmed, internal whitespace collapsed, deduped; groups with < 2 distinct forms
/// are dropped (an equivalence needs ≥2). Form *classification* happens later in the registry.
pub(super) fn parse_solr_synonyms(text: &str) -> Vec<Vec<String>> {
    let mut groups = Vec::new();
    for raw in text.lines() {
        let line = strip_comment(raw);
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let forms = match split_mapping(line) {
            // `lhs => rhs`: union both sides into one bidirectional group.
            Some((lhs, rhs)) => {
                let mut f = split_forms(lhs);
                f.extend(split_forms(rhs));
                f
            }
            None => split_forms(line),
        };
        let mut f: Vec<String> = forms.into_iter().filter(|s| !s.is_empty()).collect();
        f.sort();
        f.dedup();
        if f.len() >= 2 {
            groups.push(f);
        }
    }
    groups
}

/// Drop an unescaped `#` comment (and everything after it). A `\#` is kept literal.
fn strip_comment(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut escaped = false;
    for ch in line.chars() {
        if escaped {
            out.push(ch);
            escaped = false;
        } else if ch == '\\' {
            out.push(ch);
            escaped = true;
        } else if ch == '#' {
            break;
        } else {
            out.push(ch);
        }
    }
    out
}

/// Split a line on its (first) unescaped `=>` mapping arrow, if present.
fn split_mapping(line: &str) -> Option<(&str, &str)> {
    let bytes = line.as_bytes();
    let mut i = 0;
    while i + 1 < bytes.len() {
        if bytes[i] == b'=' && bytes[i + 1] == b'>' {
            // `=>` is never escaped in Solr's grammar; a `\` before it is part of a form.
            return Some((&line[..i], &line[i + 2..]));
        }
        i += 1;
    }
    None
}

/// Split a comma-separated form list, honoring `\,` as a literal comma and collapsing internal
/// whitespace in each form (so `i  pod` and `i pod` are the same multi-word form).
fn split_forms(s: &str) -> Vec<String> {
    let mut forms = Vec::new();
    let mut cur = String::new();
    let mut escaped = false;
    for ch in s.chars() {
        if escaped {
            cur.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == ',' {
            forms.push(collapse_ws(&cur));
            cur.clear();
        } else {
            cur.push(ch);
        }
    }
    forms.push(collapse_ws(&cur));
    forms.retain(|f| !f.is_empty());
    forms
}

/// Trim and collapse runs of internal whitespace to a single space.
fn collapse_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}
