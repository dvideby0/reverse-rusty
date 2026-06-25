//! The query DSL parser — an independent reimplementation of `engine/src/dsl.rs`.
//!
//! Grammar (`docs/reference/dsl.md`):
//!   word                     -> required term
//!   "a b c"                  -> required phrase (content trimmed)
//!   (a,b,c)                  -> required any-of group (>=1 must match)
//!   -word / -"a b" / -(a,b)  -> the MUST_NOT forms
//! All top-level clauses are ANDed. A `-` must be IMMEDIATELY followed by its atom: `foo - bar`,
//! `foo -`, and `- bar` are parse errors (rejecting the silent intent-inversion), while `-bar`
//! negates. The error *kind* and *position* are not reproduced (the differential only cares whether
//! a query parses or is dropped, and what AST it yields), but a typed kind is kept for test
//! readability.

/// The byte-length / structural limits, matching the engine's compiled-in defaults
/// (`MAX_QUERY_LENGTH` / `MAX_CLAUSES` / `MAX_ANY_OF_SIZE`).
pub const MAX_QUERY_LENGTH: usize = 10_240;
pub const MAX_CLAUSES: usize = 256;
pub const MAX_ANY_OF_SIZE: usize = 64;

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Atom {
    Term(String),
    Phrase(String),
    AnyOf(Vec<String>),
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Clause {
    pub negated: bool,
    pub atom: Atom,
}

#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Ast {
    pub clauses: Vec<Clause>,
}

/// Why a query failed to parse. The engine drops such queries at ingest, so the reference drops
/// them too — only success-vs-drop and the resulting AST matter for the diff.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ParseError {
    QueryTooLong,
    TrailingDash,
    UnclosedGroup,
    EmptyAnyOfGroup,
    AnyOfGroupTooLarge,
    UnclosedQuote,
    TooManyClauses,
}

/// Parse a query DSL string. Mirrors `dsl::parse` (default limits).
pub fn parse(input: &str) -> Result<Ast, ParseError> {
    if input.len() > MAX_QUERY_LENGTH {
        return Err(ParseError::QueryTooLong);
    }
    let chars: Vec<char> = input.chars().collect();
    let n = chars.len();
    let mut i = 0;
    let mut clauses = Vec::new();

    while i < n {
        while i < n && chars[i].is_whitespace() {
            i += 1;
        }
        if i >= n {
            break;
        }

        let mut negated = false;
        if chars[i] == '-' {
            negated = true;
            i += 1;
            // A '-' must be immediately followed by its atom — reject EOF and a following space.
            if i >= n || chars[i].is_whitespace() {
                return Err(ParseError::TrailingDash);
            }
        }

        match chars[i] {
            '(' => {
                i += 1;
                let mut members = Vec::new();
                let mut cur = String::new();
                while i < n && chars[i] != ')' {
                    let c = chars[i];
                    if c == ',' {
                        push_member(&mut members, &mut cur);
                    } else if c.is_whitespace() {
                        cur.push(' '); // allow (single) spaces inside a member
                    } else {
                        cur.push(c);
                    }
                    i += 1;
                }
                if i >= n {
                    return Err(ParseError::UnclosedGroup);
                }
                i += 1; // consume ')'
                push_member(&mut members, &mut cur);
                if members.is_empty() {
                    return Err(ParseError::EmptyAnyOfGroup);
                }
                if members.len() > MAX_ANY_OF_SIZE {
                    return Err(ParseError::AnyOfGroupTooLarge);
                }
                clauses.push(Clause {
                    negated,
                    atom: Atom::AnyOf(members),
                });
            }
            '"' => {
                i += 1;
                let mut phrase = String::new();
                while i < n && chars[i] != '"' {
                    phrase.push(chars[i]);
                    i += 1;
                }
                if i >= n {
                    return Err(ParseError::UnclosedQuote);
                }
                i += 1; // consume closing quote
                clauses.push(Clause {
                    negated,
                    atom: Atom::Phrase(phrase.trim().to_string()),
                });
            }
            _ => {
                // A bare term runs until whitespace, '(' or '"'. It MAY contain '-', ',', ')'.
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

    if clauses.len() > MAX_CLAUSES {
        return Err(ParseError::TooManyClauses);
    }
    Ok(Ast { clauses })
}

/// Trim a pending any-of member and push it unless empty (mirrors `dsl::push_member`).
fn push_member(members: &mut Vec<String>, cur: &mut String) {
    let t = cur.trim();
    if !t.is_empty() {
        members.push(t.to_string());
    }
    cur.clear();
}
