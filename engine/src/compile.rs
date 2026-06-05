//! Query compiler + signature-cover optimizer + cost classifier.
//!
//! Design: docs/design/matching.md §1
//! Invariant: Signatures built ONLY from required features / any-of groups,
//!   never from forbidden features (lossless cover contract)
//! Hot path: no — compilation is off the match path entirely
//!
//! Turns a parsed AST into the integer form the matcher uses, and chooses a
//! *lossless* set of candidate signatures. The key correctness rule: signatures
//! are built ONLY from required features / any-of groups, never from forbidden
//! features.
//!
//! This file holds the shared type *definitions*; their associated functions live
//! in focused submodules so each concern is self-contained:
//!   - [`extract`] — AST → [`Extracted`] interning (`is_hot`, `extract`,
//!     `extract_readonly`), both the mutating compile-time and read-only paths
//!   - [`plan`]    — the signature-cover optimizer + cost classifier
//!     (`anchor_plan`, `build_signatures`) + the full-compile convenience
//!     (`compile_one`, `compile_one_readonly`)
//!   - `tests`     — golden extraction cases + equivalence-expansion unit tests

use crate::dict::FeatureId;

mod extract;
mod plan;

#[cfg(test)]
mod tests;

pub use extract::{extract, extract_readonly, is_hot};
pub use plan::{anchor_plan, build_signatures, compile_one, compile_one_readonly};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CostClass {
    /// highly selective (rare arity-1 anchor) — main index, realtime
    A,
    /// acceptable (arity-2 anchor, or selective any-of reps) — main index, realtime
    B,
    /// broad (only a hot anchor available) — broad lane, not the selective path
    C,
    /// pathological (no required feature and no any-of) — rejected at compile
    D,
}

/// The positive/negative integer form of a query (no signatures yet).
#[derive(Clone, Debug)]
pub struct Extracted {
    pub required: Vec<FeatureId>,   // AND
    pub forbidden: Vec<FeatureId>,  // none may be present
    pub anyof: Vec<Vec<FeatureId>>, // each group: >=1 member-proxy present
}

impl Extracted {
    /// Expand learned equivalence groups (ADR-054) into the query — the FN-safe
    /// "expansion, not collapse" application of an alias. A required feature in a group
    /// `G` is moved out of `required` and added as an any-of group `G` (so a title bearing
    /// ANY member of `G` still retrieves the query), and each existing any-of group is
    /// widened by its members' groups. `forbidden` is never touched (negation semantics
    /// must not be widened).
    ///
    /// Because this only ever WIDENS the accepted positive feature set, the query's match
    /// set can only grow — it can never drop a true match, so it **cannot introduce a false
    /// negative**; a wrong/low-confidence equivalence degrades to a bounded false positive
    /// (the cardinal-sin-free failure mode this engine is built around). A no-op when
    /// `equiv` is empty, so the default path is byte-identical. Idempotent.
    pub fn expand_equivalences(&mut self, equiv: &crate::dict::EquivMap) {
        if equiv.is_empty() {
            return;
        }
        // A required feature in an equivalence group becomes an any-of over the group.
        let mut still_required = Vec::with_capacity(self.required.len());
        for &f in &self.required {
            match equiv.get(&f) {
                Some(group) => self.anyof.push(group.clone()),
                None => still_required.push(f),
            }
        }
        self.required = still_required;
        // Widen every any-of group (incl. the ones just added) by its members' groups.
        for g in &mut self.anyof {
            let mut widened: Vec<FeatureId> = Vec::with_capacity(g.len());
            for &m in g.iter() {
                match equiv.get(&m) {
                    Some(group) => widened.extend_from_slice(group),
                    None => widened.push(m),
                }
            }
            widened.sort_unstable();
            widened.dedup();
            *g = widened;
        }
        // Canonicalize for determinism (dedup identical groups + keep required tidy).
        self.required.sort_unstable();
        self.required.dedup();
        self.anyof.sort_unstable();
        self.anyof.dedup();
    }
}

/// Fully compiled query (used for explain/demo; the at-scale path streams into
/// the segment SoA instead of retaining these).
#[derive(Clone, Debug)]
pub struct CompiledQuery {
    pub logical_id: u64,
    pub version: u32,
    pub extracted: Extracted,
    pub main_sigs: Vec<u64>,
    pub broad_sigs: Vec<u64>,
    pub cost_class: CostClass,
}

pub struct SigPlan {
    pub main_sigs: Vec<u64>,
    pub broad_sigs: Vec<u64>,
    pub class: CostClass,
}

/// The pre-hash form of a [`SigPlan`]: the actual *feature groups* the lossless
/// cover is built from, before they are folded into `sig_key`s. Each `main`/`broad`
/// entry is one signature's feature group (arity 1, or arity 2 for the escalated
/// class-B pair). `build_signatures` is exactly `anchor_plan` followed by
/// `sig_key` over each group, so the two cannot drift.
///
/// Exists so the cluster coordinator can place a query by its *anchor feature
/// identity* (not just the opaque hash) while reusing the optimizer's per-class
/// selection verbatim — see [`crate::cluster`]. The forbidden-feature invariant
/// holds for free: like `build_signatures`, this only ever reads
/// `ex.required` / `ex.anyof`, never `ex.forbidden`.
#[derive(Clone, Debug)]
pub struct AnchorPlan {
    /// Each group = one main-index signature's features (arity 1, or 2 for the
    /// escalated class-B pair). Empty for class C and class D.
    pub main_anchors: Vec<Vec<FeatureId>>,
    /// Each group = one broad-lane signature's features (always arity 1). Non-empty
    /// only for class C. Empty otherwise.
    pub broad_anchors: Vec<Vec<FeatureId>>,
    pub class: CostClass,
}
