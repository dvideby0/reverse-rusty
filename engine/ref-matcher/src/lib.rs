//! # reverse-rusty-ref-matcher — the front-end-independent correctness reference (ADR-087)
//!
//! A from-scratch reimplementation of Reverse Rusty's matching front end — the DSL parser,
//! the shared query/title normalizer, the feature extractor, and the match predicate — written
//! **purely from the spec** (`docs/reference/dsl.md`, `docs/design/normalization.md`, ADR-058 /
//! 060 / 061 / 069, and the engine's spec-authored golden tests). It reuses **none** of the
//! `reverse-rusty` crate.
//!
//! ## Why this exists
//! The in-tree differential oracle (`engine/tests/oracle/`) compares the engine to a
//! "brute-force" reference, but that reference calls the engine's OWN `dsl::parse`,
//! `compile::extract`, and `Normalizer`. So a bug in the parser/normalizer/extractor corrupts
//! both sides identically and the oracle stays green — the documented shared-front-end blind
//! spot (ADR-050). Diffing the engine against THIS reference, which shares no front-end code,
//! catches engine-vs-spec drift the in-tree oracle structurally cannot.
//!
//! ## The independence contract
//! This crate has **zero dependencies** — no `daachorse`, no `serde`, and above all no
//! `reverse-rusty`. That is enforced by the `ref-matcher independence` lane in `engine/check.sh`
//! (`cargo tree` must show no `reverse-rusty` edge). The algorithms are deliberately naive
//! (linear phrase scans instead of an Aho-Corasick automaton): a test oracle optimizes for
//! correctness and independence, not speed, and a second independent implementation of the same
//! algorithm is more likely to expose an integration bug than a shared library would be.
//!
//! ## Comparison is by canonical feature STRING
//! The reference compares matches by the engine's canonical feature names (`year:1994`,
//! `term:psa`, `grade:10`, `grader_grade:psa10`, …) — never the engine's interned integer
//! `FeatureId`s. That is what lets it reuse none of the dictionary machinery (synthetic hashing
//! included): two titles match a query iff they produce the same canonical feature set, by name.
//!
//! ## Layout
//! - [`features`] — the feature kinds + their canonical string forms.
//! - [`vocab`] — [`vocab::RefVocab`], the reference's own plain-data vocabulary (phrases,
//!   synonyms, graders, grade words, number-context words, aliases, equivalences, punctuation).
//!   The differential harness builds this AND the engine's `Vocab` from one neutral description.
//! - [`clean`] — byte cleaning: lowercase + diacritic fold + the punctuation-class table.
//! - [`normalize`] — the two-phase emit pipeline producing canonical features, including the
//!   ADR-061 two title views `N(T)` / `P(T)`.
//! - [`parse`] — the DSL parser (AND clauses, any-of groups, phrases, adjacent-`-` negation).
//! - [`extract`] — AST → [`extract::RefQuery`] (required / forbidden / any-of) + equivalence
//!   expansion (required → any-of, ADR-054).
//! - [`matcher`] — [`matcher::RefMatcher`]: build from queries + a vocab, then `matches(title)`.

pub mod clean;
pub mod extract;
pub mod features;
pub mod matcher;
pub mod normalize;
pub mod parse;
pub mod phrases;
pub mod vocab;

pub use matcher::RefMatcher;
pub use vocab::RefVocab;
