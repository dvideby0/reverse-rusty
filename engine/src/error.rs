//! Typed errors for the compile/ingest boundary and normalizer construction.
//!
//! Design: — (cross-cutting concern, no dedicated design doc)
//! Invariant: No panicking `unwrap()` in library code; all caller-facing
//!   failures use `ParseError { kind, pos }`, `NormalizerError`, or `IngestReport`
//! Hot path: no — match path is infallible by construction
//!
//! Query parsing is the only fallible step caused by caller input (a malformed
//! stored-query DSL string), so it gets a real, inspectable error type rather
//! than a `String`. Normalizer construction can fail if the Aho-Corasick
//! automaton builder rejects the phrase patterns. Both implement
//! [`std::error::Error`], so they compose with `?`, `Box<dyn Error>`, and
//! `anyhow`/`thiserror` stacks.

use std::fmt;

/// A syntax error in a stored-query DSL string.
///
/// Returned by [`crate::dsl::parse`] and propagated by
/// [`crate::compile::compile_one`]. Carries the character position where the
/// problem was detected so callers can point at the offending input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    /// What went wrong.
    pub kind: ParseErrorKind,
    /// Character index (not byte index) into the input where the problem was
    /// detected. Points at the opening delimiter for unbalanced groups/quotes.
    pub pos: usize,
}

impl ParseError {
    pub(crate) fn new(kind: ParseErrorKind, pos: usize) -> Self {
        ParseError { kind, pos }
    }
}

/// The specific kind of [`ParseError`]. Non-exhaustive so new DSL diagnostics
/// can be added without breaking downstream `match`es.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParseErrorKind {
    /// A negation `-` appeared with no atom following it.
    TrailingDash,
    /// An any-of group `(` was opened but never closed with `)`.
    UnclosedGroup,
    /// A quoted phrase `"` was opened but never closed.
    UnclosedQuote,
    /// An any-of group `()` contained no members.
    EmptyAnyOfGroup,
    /// Query string exceeds the configured maximum length.
    QueryTooLong,
    /// Query has too many clauses (terms + groups).
    TooManyClauses,
    /// An any-of group exceeds the configured maximum member count.
    AnyOfGroupTooLarge,
}

impl ParseErrorKind {
    /// Stable human-readable description (no position).
    pub fn as_str(&self) -> &'static str {
        match self {
            ParseErrorKind::TrailingDash => "negation '-' with no following term",
            ParseErrorKind::UnclosedGroup => "unclosed any-of group '('",
            ParseErrorKind::UnclosedQuote => "unclosed quoted phrase '\"'",
            ParseErrorKind::EmptyAnyOfGroup => "empty any-of group '()'",
            ParseErrorKind::QueryTooLong => "query string exceeds maximum length",
            ParseErrorKind::TooManyClauses => "query has too many clauses",
            ParseErrorKind::AnyOfGroupTooLarge => "any-of group has too many members",
        }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "query parse error at position {}: {}",
            self.pos,
            self.kind.as_str()
        )
    }
}

impl std::error::Error for ParseError {}

/// An error that occurs when building a [`Normalizer`](crate::normalize::Normalizer).
///
/// Currently the only failure mode is the Aho-Corasick automaton builder
/// rejecting the phrase patterns (e.g. overlapping or degenerate patterns that
/// daachorse cannot encode). The inner message is the string form of the
/// daachorse error — the upstream type is not re-exported so that callers don't
/// depend on daachorse directly.
#[derive(Debug, Clone)]
pub struct NormalizerError {
    msg: String,
}

impl NormalizerError {
    pub(crate) fn new(msg: impl Into<String>) -> Self {
        NormalizerError { msg: msg.into() }
    }
}

impl fmt::Display for NormalizerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "normalizer build error: {}", self.msg)
    }
}

impl std::error::Error for NormalizerError {}
