//! Strict Solr / Lucene synonym-file parser (ADR-060 item 3).
//!
//! Parses the format Elasticsearch's `synonyms_path` consumes into raw
//! equivalence groups. The registry then classifies and governs them; this
//! module only turns text into groups.

use std::fmt;

/// Elasticsearch's synonym-set API accepts at most 10,000 rules in one set.
pub const MAX_ALIAS_IMPORT_RULES: usize = 10_000;

/// Keep classification's pairwise variant checks bounded for one rule.
pub const MAX_ALIAS_FORMS_PER_RULE: usize = 256;

/// A syntax or resource-bound failure in one Solr-format alias import.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AliasImportError {
    line: Option<usize>,
    message: String,
}

impl AliasImportError {
    fn at(line: usize, message: impl Into<String>) -> Self {
        Self {
            line: Some(line),
            message: message.into(),
        }
    }

    fn whole(message: impl Into<String>) -> Self {
        Self {
            line: None,
            message: message.into(),
        }
    }

    /// One-based source line, when the failure belongs to a specific rule.
    #[must_use]
    pub fn line(&self) -> Option<usize> {
        self.line
    }
}

impl fmt::Display for AliasImportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(line) = self.line {
            write!(
                f,
                "invalid Solr synonym rule at line {line}: {}",
                self.message
            )
        } else {
            f.write_str(&self.message)
        }
    }
}

impl std::error::Error for AliasImportError {}

/// Parse Solr/Lucene synonym text into equivalence groups of raw surface forms.
///
/// Supported syntax:
///
/// - blank lines and unescaped `#` comments are ignored;
/// - `a, b, c` is one equivalence group;
/// - `a, b => c, d` is unioned into one bidirectional group because Reverse
///   Rusty implements expansion equivalences, not directional replacement;
/// - backslash escapes the next character, including `,`, `#`, and `\`.
///
/// Unlike the historical parser, malformed or degenerate rules fail the whole
/// import instead of being silently discarded.
pub(super) fn parse_solr_synonyms(text: &str) -> Result<Vec<Vec<String>>, AliasImportError> {
    let mut groups = Vec::new();
    for (offset, raw) in text.lines().enumerate() {
        let line_number = offset + 1;
        let line = strip_comment(raw).trim().to_string();
        if line.is_empty() {
            continue;
        }
        if groups.len() == MAX_ALIAS_IMPORT_RULES {
            return Err(AliasImportError::at(
                line_number,
                format!("at most {MAX_ALIAS_IMPORT_RULES} rules are accepted"),
            ));
        }

        let arrow_count = line.match_indices("=>").count();
        if arrow_count > 1 {
            return Err(AliasImportError::at(
                line_number,
                "a rule may contain at most one `=>` mapping",
            ));
        }
        let mut forms = if let Some((lhs, rhs)) = line.split_once("=>") {
            if lhs.trim().is_empty() || rhs.trim().is_empty() {
                return Err(AliasImportError::at(
                    line_number,
                    "both sides of `=>` must contain at least one form",
                ));
            }
            let mut forms = split_forms(lhs, line_number)?;
            forms.extend(split_forms(rhs, line_number)?);
            forms
        } else {
            split_forms(&line, line_number)?
        };

        forms.sort();
        forms.dedup();
        if forms.len() < 2 {
            return Err(AliasImportError::at(
                line_number,
                "an equivalence rule needs at least two distinct forms",
            ));
        }
        if forms.len() > MAX_ALIAS_FORMS_PER_RULE {
            return Err(AliasImportError::at(
                line_number,
                format!("at most {MAX_ALIAS_FORMS_PER_RULE} forms are accepted per rule"),
            ));
        }
        groups.push(forms);
    }

    if groups.is_empty() {
        return Err(AliasImportError::whole(
            "alias import must contain at least one non-comment Solr synonym rule",
        ));
    }
    Ok(groups)
}

/// Drop an unescaped `#` comment (and everything after it). A `\#` is literal.
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

/// Split a comma-separated form list, honoring backslash escapes and collapsing
/// internal whitespace in each form.
fn split_forms(s: &str, line: usize) -> Result<Vec<String>, AliasImportError> {
    let mut forms = Vec::new();
    let mut current = String::new();
    let mut escaped = false;
    for ch in s.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == ',' {
            push_form(&mut forms, &current, line)?;
            current.clear();
        } else {
            current.push(ch);
        }
    }
    if escaped {
        return Err(AliasImportError::at(
            line,
            "a trailing backslash does not escape a character",
        ));
    }
    push_form(&mut forms, &current, line)?;
    Ok(forms)
}

fn push_form(forms: &mut Vec<String>, raw: &str, line: usize) -> Result<(), AliasImportError> {
    let form = collapse_ws(raw);
    if form.is_empty() {
        return Err(AliasImportError::at(
            line,
            "comma-separated forms may not be empty",
        ));
    }
    forms.push(form);
    Ok(())
}

fn collapse_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::{parse_solr_synonyms, MAX_ALIAS_FORMS_PER_RULE, MAX_ALIAS_IMPORT_RULES};

    #[test]
    fn parses_comments_escapes_and_bidirectional_mappings() {
        let groups = parse_solr_synonyms(
            "# header\npackage, pkg\nwireless\\, mouse => cordless mouse # note\nx\\#1, x1",
        )
        .expect("valid rules");
        assert_eq!(
            groups,
            vec![
                vec!["package".to_string(), "pkg".to_string()],
                vec!["cordless mouse".to_string(), "wireless, mouse".to_string()],
                vec!["x#1".to_string(), "x1".to_string()],
            ]
        );
    }

    #[test]
    fn rejects_malformed_and_degenerate_rules_with_line_numbers() {
        for (text, line, needle) in [
            ("a, b\nc =>", 2, "both sides"),
            ("a => b => c", 1, "at most one"),
            ("a,,b", 1, "may not be empty"),
            ("a, a", 1, "two distinct"),
            ("a, b\\", 1, "trailing backslash"),
        ] {
            let error = parse_solr_synonyms(text).expect_err(text);
            assert_eq!(error.line(), Some(line));
            assert!(error.to_string().contains(needle), "{error}");
        }
        assert!(parse_solr_synonyms("# only comments")
            .expect_err("empty")
            .to_string()
            .contains("at least one"));
    }

    #[test]
    fn enforces_rule_and_form_bounds() {
        let too_many_rules = std::iter::repeat_n("a, b", MAX_ALIAS_IMPORT_RULES + 1)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(parse_solr_synonyms(&too_many_rules)
            .expect_err("rule bound")
            .to_string()
            .contains("at most 10000"));

        let too_many_forms = (0..=MAX_ALIAS_FORMS_PER_RULE)
            .map(|index| format!("form-{index}"))
            .collect::<Vec<_>>()
            .join(",");
        assert!(parse_solr_synonyms(&too_many_forms)
            .expect_err("form bound")
            .to_string()
            .contains("at most 256"));
    }
}
