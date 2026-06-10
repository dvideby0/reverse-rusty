//! Adversarial property suite — reference-free correctness checks.
//!
//! The differential oracles (`tests/oracle*`) compare the engine against a brute-force
//! reference that SHARES the front end (parser, extractor, normalizer — ADR-050), so a
//! bug inside that shared code corrupts both sides identically and never fails the
//! differential. Every suite here instead asserts a property whose ground truth needs
//! **no reference implementation**:
//!
//! - `self_match`: a title built from a query's own positive terms MUST match that
//!   query (the zero-FN diagonal). Any query↔title normalization divergence —
//!   historically the escape class of codex rounds 8/11 (whitespace runs, case) —
//!   fails this directly, shared code or not.
//! - `perturbation`: surface-only title edits (case, foldable diacritics, whitespace
//!   runs, Split-class punctuation, appended junk) must leave the match set EXACTLY
//!   unchanged under the phrase-free default vocab.
//! - `forms`: every surface form of a declared equivalence / punctuation-fold class /
//!   multi-word alias must cross-match every other form, in both directions, including
//!   whitespace-run and case-perturbed variants (the ADR-058/060/061 product contract).
//! - `fuzz`: no-panic + determinism + the documented `P(T) ⊇ N(T)` and
//!   `match_features == N(T)` relationships over random unicode soup.

mod harness;

mod forms;
mod fuzz;
mod perturbation;
mod self_match;
