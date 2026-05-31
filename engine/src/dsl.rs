//! Query DSL parser — produces an AST consumed only at compile time.
//!
//! Design: docs/design/normalization.md §1
//! Invariant: AST is never walked on the hot path; all parsing is compile-time
//! Hot path: no — this module is off the match path entirely
//!
//! Grammar:
//!   word                  -> required term
//!   "a b c"               -> required phrase
//!   (a,b,c)               -> required any-of group (a OR b OR c)
//!   -word / -"a b" / -(a,b)  -> the MUST_NOT versions of the above

use crate::error::{ParseError, ParseErrorKind};

/// Maximum query string length in bytes (default 10KB)
pub const MAX_QUERY_LENGTH: usize = 10_240;
/// Maximum number of top-level clauses in a query
pub const MAX_CLAUSES: usize = 256;
/// Maximum number of members in an any-of group
pub const MAX_ANY_OF_SIZE: usize = 64;

/// Per-query complexity limits applied at parse time.
///
/// Defaults are the module constants ([`MAX_QUERY_LENGTH`], [`MAX_CLAUSES`],
/// [`MAX_ANY_OF_SIZE`]), which also serve as the hard ceiling for callers that
/// parse without an [`EngineConfig`] (the explain / read-only path, tests). The
/// engine threads its configured limits in via
/// [`EngineConfig::parse_limits`](crate::config::EngineConfig::parse_limits) so
/// the runtime knobs — and `PUT /_settings` — actually govern every ingest path,
/// not just the compiled-in defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseLimits {
    /// Maximum query string length in bytes.
    pub max_query_length: usize,
    /// Maximum number of top-level clauses (terms + groups).
    pub max_clauses: usize,
    /// Maximum number of members in a single any-of group.
    pub max_any_of_size: usize,
}

impl Default for ParseLimits {
    fn default() -> Self {
        ParseLimits {
            max_query_length: MAX_QUERY_LENGTH,
            max_clauses: MAX_CLAUSES,
            max_any_of_size: MAX_ANY_OF_SIZE,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Atom {
    Term(String),
    Phrase(String),
    AnyOf(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Clause {
    pub negated: bool,
    pub atom: Atom,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Ast {
    pub clauses: Vec<Clause>,
}

/// Parse a query DSL string using the default (compiled-in) [`ParseLimits`].
///
/// Equivalent to [`parse_with_limits`] with [`ParseLimits::default`]. Used by
/// the explain / read-only path and by callers that have no [`EngineConfig`];
/// the ingest paths call [`parse_with_limits`] so the configured limits govern.
pub fn parse(input: &str) -> Result<Ast, ParseError> {
    parse_with_limits(input, &ParseLimits::default())
}

/// Parse a query DSL string into an [`Ast`], enforcing `limits`.
pub fn parse_with_limits(input: &str, limits: &ParseLimits) -> Result<Ast, ParseError> {
    if input.len() > limits.max_query_length {
        return Err(ParseError::new(ParseErrorKind::QueryTooLong, 0));
    }
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;
    let n = chars.len();
    let mut clauses = Vec::new();

    while i < n {
        // skip whitespace
        while i < n && chars[i].is_whitespace() {
            i += 1;
        }
        if i >= n {
            break;
        }

        let mut negated = false;
        if chars[i] == '-' {
            let dash = i;
            negated = true;
            i += 1;
            if i >= n {
                return Err(ParseError::new(ParseErrorKind::TrailingDash, dash));
            }
        }

        match chars[i] {
            '(' => {
                let open = i;
                i += 1;
                let mut members = Vec::new();
                let mut cur = String::new();
                while i < n && chars[i] != ')' {
                    match chars[i] {
                        ',' => {
                            push_member(&mut members, &mut cur);
                        }
                        c if c.is_whitespace() => {
                            /* allow spaces inside group */
                            cur.push(' ');
                        }
                        c => cur.push(c),
                    }
                    i += 1;
                }
                if i >= n {
                    return Err(ParseError::new(ParseErrorKind::UnclosedGroup, open));
                }
                i += 1; // consume ')'
                push_member(&mut members, &mut cur);
                if members.is_empty() {
                    return Err(ParseError::new(ParseErrorKind::EmptyAnyOfGroup, open));
                }
                if members.len() > limits.max_any_of_size {
                    return Err(ParseError::new(ParseErrorKind::AnyOfGroupTooLarge, open));
                }
                clauses.push(Clause {
                    negated,
                    atom: Atom::AnyOf(members),
                });
            }
            '"' => {
                let open = i;
                i += 1;
                let mut phrase = String::new();
                while i < n && chars[i] != '"' {
                    phrase.push(chars[i]);
                    i += 1;
                }
                if i >= n {
                    return Err(ParseError::new(ParseErrorKind::UnclosedQuote, open));
                }
                i += 1; // consume closing quote
                clauses.push(Clause {
                    negated,
                    atom: Atom::Phrase(phrase.trim().to_string()),
                });
            }
            _ => {
                let mut word = String::new();
                while i < n && !chars[i].is_whitespace() && chars[i] != '(' && chars[i] != '"' {
                    word.push(chars[i]);
                    i += 1;
                }
                clauses.push(Clause {
                    negated,
                    atom: Atom::Term(word),
                });
            }
        }
    }

    if clauses.len() > limits.max_clauses {
        return Err(ParseError::new(ParseErrorKind::TooManyClauses, 0));
    }

    Ok(Ast { clauses })
}

fn push_member(members: &mut Vec<String>, cur: &mut String) {
    let t = cur.trim();
    if !t.is_empty() {
        members.push(t.to_string());
    }
    cur.clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_spec_example() {
        let q = "1994 (upper deck,UD) michael jordan sp (preview,previews) \
                 -(next,checklist,checklists,heroes,long,count) \
                 -(minor,minors,top,classic,alumni) \
                 -(auto,autograph,autographs,autographed,signed,dna,signature) \
                 PSA 10 -(sgc,bgs)";
        let ast = parse(q).unwrap();
        // a few sanity checks
        assert!(ast
            .clauses
            .iter()
            .any(|c| !c.negated && c.atom == Atom::Term("1994".into())));
        assert!(ast
            .clauses
            .iter()
            .any(|c| matches!(&c.atom, Atom::AnyOf(m) if m.contains(&"upper deck".to_string()))));
        assert!(ast
            .clauses
            .iter()
            .any(|c| c.negated
                && matches!(&c.atom, Atom::AnyOf(m) if m.contains(&"bgs".to_string()))));
    }

    #[test]
    fn empty_input_is_ok_and_empty() {
        assert_eq!(parse("").unwrap(), Ast::default());
        assert_eq!(parse("   ").unwrap(), Ast::default());
    }

    #[test]
    fn trailing_dash_errors_at_dash() {
        let e = parse("jordan -").unwrap_err();
        assert_eq!(e.kind, ParseErrorKind::TrailingDash);
        assert_eq!(e.pos, 7); // index of the '-'
    }

    #[test]
    fn unclosed_group_errors_at_open_paren() {
        let e = parse("jordan (upper deck,ud").unwrap_err();
        assert_eq!(e.kind, ParseErrorKind::UnclosedGroup);
        assert_eq!(e.pos, 7); // index of the '('
    }

    #[test]
    fn empty_group_errors_at_open_paren() {
        let e = parse("jordan ()").unwrap_err();
        assert_eq!(e.kind, ParseErrorKind::EmptyAnyOfGroup);
        assert_eq!(e.pos, 7);
    }

    #[test]
    fn unclosed_quote_errors_at_open_quote() {
        let e = parse("jordan \"upper deck").unwrap_err();
        assert_eq!(e.kind, ParseErrorKind::UnclosedQuote);
        assert_eq!(e.pos, 7); // index of the opening quote
    }

    #[test]
    fn error_implements_display_and_std_error() {
        let e = parse("-").unwrap_err();
        // Display is non-empty and mentions the position.
        let msg = e.to_string();
        assert!(msg.contains("position 0"), "got: {msg}");
        // usable as a std::error::Error trait object
        let _boxed: Box<dyn std::error::Error> = Box::new(e);
    }

    #[test]
    fn query_too_long_is_rejected() {
        let long = "a ".repeat(MAX_QUERY_LENGTH); // well over the byte limit
        let e = parse(&long).unwrap_err();
        assert_eq!(e.kind, ParseErrorKind::QueryTooLong);
        assert_eq!(e.pos, 0);
    }

    #[test]
    fn too_many_clauses_is_rejected() {
        // Build a query with MAX_CLAUSES + 1 single-word clauses
        let q: String = (0..=MAX_CLAUSES)
            .map(|i| format!("t{i}"))
            .collect::<Vec<_>>()
            .join(" ");
        let e = parse(&q).unwrap_err();
        assert_eq!(e.kind, ParseErrorKind::TooManyClauses);
    }

    #[test]
    fn any_of_group_too_large_is_rejected() {
        // Build a group with MAX_ANY_OF_SIZE + 1 members
        let members: String = (0..=MAX_ANY_OF_SIZE)
            .map(|i| format!("m{i}"))
            .collect::<Vec<_>>()
            .join(",");
        let q = format!("({members})");
        let e = parse(&q).unwrap_err();
        assert_eq!(e.kind, ParseErrorKind::AnyOfGroupTooLarge);
        assert_eq!(e.pos, 0); // position of the '('
    }

    #[test]
    fn within_limits_parses_ok() {
        // Exactly MAX_CLAUSES clauses should be fine
        let q: String = (0..MAX_CLAUSES)
            .map(|i| format!("t{i}"))
            .collect::<Vec<_>>()
            .join(" ");
        let ast = parse(&q).unwrap();
        assert_eq!(ast.clauses.len(), MAX_CLAUSES);

        // Exactly MAX_ANY_OF_SIZE members should be fine
        let members: String = (0..MAX_ANY_OF_SIZE)
            .map(|i| format!("m{i}"))
            .collect::<Vec<_>>()
            .join(",");
        let q = format!("({members})");
        let ast = parse(&q).unwrap();
        assert_eq!(ast.clauses.len(), 1);
        match &ast.clauses[0].atom {
            Atom::AnyOf(m) => assert_eq!(m.len(), MAX_ANY_OF_SIZE),
            other => panic!("expected AnyOf, got {other:?}"),
        }
    }

    #[test]
    fn parse_with_limits_enforces_custom_bounds() {
        // A tighter-than-default limit rejects input the default would accept.
        // The length budget is generous so the clause / any-of checks are
        // isolated from the length check (which the parser tests first).
        let tight = ParseLimits {
            max_query_length: 64,
            max_clauses: 2,
            max_any_of_size: 2,
        };
        assert_eq!(
            parse_with_limits(&"x".repeat(65), &tight).unwrap_err().kind,
            ParseErrorKind::QueryTooLong
        );
        assert_eq!(
            parse_with_limits("aa bb cc", &tight).unwrap_err().kind,
            ParseErrorKind::TooManyClauses,
            "8-byte input is within length but 3 clauses exceeds max_clauses=2"
        );
        assert_eq!(
            parse_with_limits("(x,y,z)", &tight).unwrap_err().kind,
            ParseErrorKind::AnyOfGroupTooLarge
        );

        // A looser-than-default limit accepts input the default would reject.
        let loose = ParseLimits {
            max_clauses: MAX_CLAUSES + 8,
            ..ParseLimits::default()
        };
        let q: String = (0..=MAX_CLAUSES)
            .map(|i| format!("t{i}"))
            .collect::<Vec<_>>()
            .join(" ");
        assert!(parse(&q).is_err(), "default limit rejects MAX_CLAUSES + 1");
        assert_eq!(
            parse_with_limits(&q, &loose).unwrap().clauses.len(),
            MAX_CLAUSES + 1,
            "a raised max_clauses accepts the same query"
        );
    }
}
