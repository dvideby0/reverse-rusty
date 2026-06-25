//! Canonical features — compared by **string**, never by an interned id.
//!
//! Every feature the normalizer emits has a canonical, kind-prefixed name (`year:1994`,
//! `term:psa`, `grade:10`, `grader:psa`, `grader_grade:psa10`). The prefix makes the name
//! self-describing, so two features are equal iff their canonical strings are equal — which is
//! exactly the equality the engine's interned `FeatureId`s give (the synthetic-id path hashes
//! the same name). Comparing by string is what frees this crate from the engine's dictionary.
//!
//! The constructors below reproduce the engine's canonical formats (`engine/src/normalize/core.rs`
//! `emit_generic` / `emit_grade` and the inline `year:` / `grade:` / `grader:` builders).

/// A canonical feature name (e.g. `"term:psa"`). Ordered + hashable so feature *sets* are cheap.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct Feature(pub String);

impl Feature {
    /// A feature whose canonical name is given verbatim — used for vocabulary-supplied
    /// canonicals (a synonym's or phrase's `canonical`, which is already kind-prefixed, e.g.
    /// `term:upper_deck`).
    #[must_use]
    pub fn raw(canonical: impl Into<String>) -> Self {
        Feature(canonical.into())
    }

    /// `year:<YYYY>` (a 4-digit number in 1900..=2099).
    #[must_use]
    pub fn year(yyyy: &str) -> Self {
        Feature(format!("year:{yyyy}"))
    }

    /// `term:<token>` — the generic fallback feature (`emit_generic`).
    #[must_use]
    pub fn term(token: &str) -> Self {
        Feature(format!("term:{token}"))
    }

    /// `grade:<n>` (the numeric grade value, e.g. `10` or `9.5`).
    #[must_use]
    pub fn grade(n: &str) -> Self {
        Feature(format!("grade:{n}"))
    }

    /// `grader:<g>` (a canonicalized grader name, e.g. `psa`, `bgs`).
    #[must_use]
    pub fn grader(g: &str) -> Self {
        Feature(format!("grader:{g}"))
    }

    /// `grader_grade:<g><n>` (the fused grader+grade, e.g. `grader_grade:psa10`).
    #[must_use]
    pub fn grader_grade(g: &str, n: &str) -> Self {
        Feature(format!("grader_grade:{g}{n}"))
    }

    /// The canonical name as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
