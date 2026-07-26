//! Compile-time unit tests: golden extraction cases + the equivalence-expansion
//! rewrite. Split out of `compile.rs` verbatim; both submodules keep their
//! `#[cfg(test)]` gate and pull the module surface in via `use super::super::*`.

#[cfg(test)]
mod class_d_universal_cover;
#[cfg(test)]
mod equiv_tests;
#[cfg(test)]
mod golden;
