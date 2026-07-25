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
use crate::normalize::{PhraseGraph, PositionArc};

mod extract;
mod plan;

#[cfg(test)]
mod tests;

pub use extract::{extract, extract_readonly, is_hot, is_hot_anchor};
pub(crate) use plan::uses_required_phrase_proxy;
pub use plan::{anchor_plan, build_signatures, compile_one, compile_one_readonly};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CostClass {
    /// highly selective (rare arity-1 anchor) — main index, realtime
    A,
    /// acceptable (arity-2 anchor, or selective any-of reps) — main index, realtime
    B,
    /// broad (only a top-64-hot anchor available) — broad lane, **opt-in
    /// visibility** (`include_broad`), not the selective path
    C,
    /// negation-only (no required feature and no any-of) — rejected at ingest by
    /// default; the opt-in always-candidate lane stores it under the universal
    /// signature in the broad lane (ADR-068)
    D,
    /// θ-hot anchor (ADR-105, the Broad-Query Cost Program's hot tier): the
    /// deciding anchor has no top-64 mask bit but its frequency is ≥ the
    /// engine's `hot_anchor_threshold` — a fat posting that would pollute the
    /// realtime lane. Stored in the per-segment **hot index**: columnar-
    /// evaluated like the broad lane, **probed on every request** like the main
    /// lane — cost quarantine only, NEVER a visibility change (the two-axis
    /// placement rule: cost movement must never imply visibility movement).
    /// Exists only when the θ knob is on; θ=0 classifies exactly as before.
    H,
}

/// One semantic member of a positive any-of group.
///
/// A member is an AND of `requirements`; each requirement is an OR of equivalent
/// feature ids. Before equivalence expansion the unquoted member `open box`
/// lowers to `[[open], [box]]`. Learned aliases widen an individual requirement
/// (for example `[[open, opened], [box]]`) without losing the conjunction.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct AnyOfMember {
    pub requirements: Vec<Vec<FeatureId>>,
}

impl AnyOfMember {
    pub(crate) fn from_features(mut features: Vec<FeatureId>) -> Option<Self> {
        features.sort_unstable();
        features.dedup();
        if features.is_empty() {
            return None;
        }
        Some(Self {
            requirements: features.into_iter().map(|feature| vec![feature]).collect(),
        })
    }

    fn canonicalize(&mut self) {
        for alternatives in &mut self.requirements {
            alternatives.sort_unstable();
            alternatives.dedup();
        }
        self.requirements
            .retain(|alternatives| !alternatives.is_empty());
        self.requirements.sort_unstable();
        self.requirements.dedup();
    }
}

/// One positive any-of predicate: OR across [`AnyOfMember`]s.
///
/// Only groups containing a multi-requirement member need this exact form.
/// Ordinary `(red,blue)` groups remain entirely in the compact any-of SoA.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct AnyOfPredicate {
    pub members: Vec<AnyOfMember>,
}

impl AnyOfPredicate {
    fn canonicalize(&mut self) {
        for member in &mut self.members {
            member.canonicalize();
        }
        self.members
            .retain(|member| !member.requirements.is_empty());
        self.members.sort_unstable();
        self.members.dedup();
    }
}

/// The positive/negative integer form of a query (no signatures yet).
#[derive(Clone, Debug, Default)]
pub struct Extracted {
    pub required: Vec<FeatureId>,  // AND
    pub forbidden: Vec<FeatureId>, // none may be present
    /// A necessary OR of retrieval proxies per group. For an ordinary
    /// single-token any-of this is also the complete exact predicate. Compound
    /// groups additionally use `anyof_predicates` for the sufficient check.
    pub anyof: Vec<Vec<FeatureId>>,
    /// Exact OR-of-AND predicates for groups containing a multi-token member.
    pub anyof_predicates: Vec<AnyOfPredicate>,
    /// Multi-feature members of negated any-of groups. A query rejects when any
    /// whole conjunction matches; singleton members use the flat `forbidden`
    /// column.
    pub forbidden_conjunctions: Vec<Vec<FeatureId>>,
    /// Required quoted clauses as analyzed token graphs (ADR-120).
    pub required_phrases: Vec<PhraseGraph>,
    /// Forbidden quoted clauses, evaluated against the canonical title graph.
    pub forbidden_phrases: Vec<PhraseGraph>,
}

impl Extracted {
    /// Whether the query has any negative semantic predicate, including a
    /// multi-feature member of a negated any-of group.
    #[inline]
    pub fn has_negative_predicate(&self) -> bool {
        !self.forbidden.is_empty()
            || !self.forbidden_conjunctions.is_empty()
            || !self.forbidden_phrases.is_empty()
    }

    /// Whether the compiled query contains no positive or negative predicate.
    #[inline]
    pub fn is_semantically_empty(&self) -> bool {
        self.required.is_empty()
            && self.anyof.is_empty()
            && self.anyof_predicates.is_empty()
            && self.required_phrases.is_empty()
            && !self.has_negative_predicate()
    }

    /// Direct semantic predicate over sorted positive/negative feature views.
    /// Quoted predicates require [`matches_positioned`](Self::matches_positioned);
    /// this feature-only compatibility entry point therefore returns `false`
    /// rather than silently treating a phrase as a conjunction.
    pub fn matches_features(&self, pos: &[FeatureId], neg: &[FeatureId]) -> bool {
        self.required_phrases.is_empty()
            && self.forbidden_phrases.is_empty()
            && self.matches_flat_features(pos, neg)
    }

    /// Direct semantic predicate including quoted token graphs. Used by explain
    /// and the shared-front-end randomized oracles; the independent reference
    /// matcher implements the same graph-language intersection separately.
    ///
    /// `pos_graph_complete` carries the positive analyzer's bounded-work signal.
    /// An incomplete positive graph, or either graph exhausting the intersection
    /// budget, fails open by predicate polarity exactly like the hot verifier:
    /// required phrases do not reject and forbidden phrases do not trip.
    #[allow(clippy::too_many_arguments)]
    pub fn matches_positioned(
        &self,
        pos: &[FeatureId],
        neg: &[FeatureId],
        pos_positions: u32,
        pos_arcs: &[PositionArc],
        pos_graph_complete: bool,
        neg_positions: u32,
        neg_arcs: &[PositionArc],
    ) -> bool {
        self.matches_flat_features(pos, neg)
            && self.required_phrases.iter().all(|phrase| {
                crate::normalize::phrase_graph_matches_bounded(
                    phrase,
                    pos_positions,
                    pos_arcs,
                    pos_graph_complete,
                ) != Some(false)
            })
            && !self.forbidden_phrases.iter().any(|phrase| {
                crate::normalize::phrase_graph_matches_bounded(
                    phrase,
                    neg_positions,
                    neg_arcs,
                    true,
                ) == Some(true)
            })
    }

    fn matches_flat_features(&self, pos: &[FeatureId], neg: &[FeatureId]) -> bool {
        let in_pos = |feature: &FeatureId| pos.binary_search(feature).is_ok();
        let in_neg = |feature: &FeatureId| neg.binary_search(feature).is_ok();
        self.required.iter().all(&in_pos)
            && !self.forbidden.iter().any(&in_neg)
            && self.anyof.iter().all(|group| group.iter().any(&in_pos))
            && self.anyof_predicates.iter().all(|predicate| {
                predicate.members.iter().any(|member| {
                    member
                        .requirements
                        .iter()
                        .all(|requirement| requirement.iter().any(&in_pos))
                })
            })
            && !self
                .forbidden_conjunctions
                .iter()
                .any(|member| member.iter().all(&in_neg))
    }

    fn canonicalize(&mut self) {
        self.required.sort_unstable();
        self.required.dedup();
        self.forbidden.sort_unstable();
        self.forbidden.dedup();
        self.anyof.sort_unstable();
        self.anyof.dedup();
        for predicate in &mut self.anyof_predicates {
            predicate.canonicalize();
        }
        self.anyof_predicates
            .retain(|predicate| !predicate.members.is_empty());
        self.anyof_predicates.sort_unstable();
        self.anyof_predicates.dedup();
        for conjunction in &mut self.forbidden_conjunctions {
            conjunction.sort_unstable();
            conjunction.dedup();
        }
        self.forbidden_conjunctions
            .retain(|conjunction| !conjunction.is_empty());
        self.forbidden_conjunctions.sort_unstable();
        self.forbidden_conjunctions.dedup();
        canonicalize_phrases(&mut self.required_phrases);
        canonicalize_phrases(&mut self.forbidden_phrases);
    }

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
            self.canonicalize();
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
        // Widen every proxy any-of group (incl. the ones just added) by its
        // members' equivalence groups.
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
        // Widen each individual requirement of a compound member. Aliases are
        // alternatives for that requirement, never substitutes for the member's
        // other conjuncts.
        for predicate in &mut self.anyof_predicates {
            for member in &mut predicate.members {
                for requirement in &mut member.requirements {
                    let mut widened = Vec::with_capacity(requirement.len());
                    for &feature in requirement.iter() {
                        match equiv.get(&feature) {
                            Some(group) => widened.extend_from_slice(group),
                            None => widened.push(feature),
                        }
                    }
                    widened.sort_unstable();
                    widened.dedup();
                    *requirement = widened;
                }
            }
        }
        // Positive phrase edges are analyzer tokens too: widen each label by
        // its learned equivalents. Forbidden phrases deliberately retain the
        // canonical, unexpanded graph (ADR-061's MUST_NOT policy).
        for phrase in &mut self.required_phrases {
            for arc in &mut phrase.arcs {
                let mut widened = Vec::with_capacity(arc.alternatives.len());
                for &feature in &arc.alternatives {
                    match equiv.get(&feature) {
                        Some(group) => widened.extend_from_slice(group),
                        None => widened.push(feature),
                    }
                }
                widened.sort_unstable();
                widened.dedup();
                arc.alternatives = widened;
            }
        }
        // Canonicalize for deterministic exact-program bytes and semantic-body
        // dedup. Forbidden predicates deliberately are not equivalence-expanded.
        self.canonicalize();
    }

    /// Reject a compiled query whose any column would overflow the SoA exact store's
    /// `u16` count encoding (`req_len`/`forb_len`/`q_group_count`/`group_len` in
    /// [`ExactStore::push`](crate::exact::ExactStore::push)). The independent parser
    /// ceilings (`max_query_clauses`, `max_anyof_group_size`) bound the *AST* but NOT
    /// the *compiled* columns: e.g. two negated any-of clauses each near
    /// `max_anyof_group_size` flatten into one forbidden column that can exceed
    /// `u16::MAX` even though both knobs validate (the per-knob ceilings live in
    /// [`EngineConfig::validate`](crate::config::EngineConfig::validate)). Equivalence
    /// expansion can widen the columns too, so the check must run on the FINAL
    /// `Extracted` (post-[`expand_equivalences`](Self::expand_equivalences)), at the
    /// ingest front door — exactly where this is called. A `u16` truncation here would
    /// silently drop required / any-of / forbidden features (a false negative, or — for
    /// a dropped forbidden — a silent over-match), so reject LOUDLY instead.
    ///
    /// Conservative: each checked count is `>=` what the column actually stores (the
    /// store splits a few required/forbidden features into the u64 common-mask, never
    /// into the tail), so a guarded query can never overflow the cast. Returns the
    /// total feature count of the offending column on overflow.
    pub fn column_overflow(&self) -> Option<usize> {
        let ceiling = u16::MAX as usize;
        if self.required.len() > ceiling {
            return Some(self.required.len());
        }
        if self.forbidden.len() > ceiling {
            return Some(self.forbidden.len());
        }
        if self.anyof.len() > ceiling {
            return Some(self.anyof.len());
        }
        for g in &self.anyof {
            if g.len() > ceiling {
                return Some(g.len());
            }
        }
        // Compound programs use u32 word counts and offsets. Guard every count
        // plus the full row length before ExactStore narrows them.
        let u32_ceiling = u32::MAX as usize;
        if self.anyof_predicates.len() > u32_ceiling {
            return Some(self.anyof_predicates.len());
        }
        let mut words = 3usize; // program version + positive count + negative count
        for predicate in &self.anyof_predicates {
            if predicate.members.len() > u32_ceiling {
                return Some(predicate.members.len());
            }
            words = match words.checked_add(1) {
                Some(words) => words,
                None => return Some(usize::MAX),
            };
            for member in &predicate.members {
                if member.requirements.len() > u32_ceiling {
                    return Some(member.requirements.len());
                }
                words = match words.checked_add(1) {
                    Some(words) => words,
                    None => return Some(usize::MAX),
                };
                for alternatives in &member.requirements {
                    if alternatives.len() > u32_ceiling {
                        return Some(alternatives.len());
                    }
                    words = match words.checked_add(1 + alternatives.len()) {
                        Some(words) => words,
                        None => return Some(usize::MAX),
                    };
                }
            }
        }
        if self.forbidden_conjunctions.len() > u32_ceiling {
            return Some(self.forbidden_conjunctions.len());
        }
        for conjunction in &self.forbidden_conjunctions {
            if conjunction.len() > u32_ceiling {
                return Some(conjunction.len());
            }
            words = match words.checked_add(1 + conjunction.len()) {
                Some(words) => words,
                None => return Some(usize::MAX),
            };
        }
        if !self.required_phrases.is_empty() || !self.forbidden_phrases.is_empty() {
            words = match words.checked_add(2) {
                Some(words) => words,
                None => return Some(usize::MAX),
            };
            for phrases in [&self.required_phrases, &self.forbidden_phrases] {
                if phrases.len() > u32_ceiling {
                    return Some(phrases.len());
                }
                for phrase in phrases {
                    if phrase.arcs.len() > u32_ceiling {
                        return Some(phrase.arcs.len());
                    }
                    words = match words.checked_add(2) {
                        Some(words) => words,
                        None => return Some(usize::MAX),
                    };
                    for arc in &phrase.arcs {
                        if arc.alternatives.len() > u32_ceiling {
                            return Some(arc.alternatives.len());
                        }
                        words = match words.checked_add(3 + arc.alternatives.len()) {
                            Some(words) => words,
                            None => return Some(usize::MAX),
                        };
                    }
                }
            }
        }
        if words > u32_ceiling {
            return Some(words);
        }
        None
    }
}

fn canonicalize_phrases(phrases: &mut Vec<PhraseGraph>) {
    for phrase in phrases.iter_mut() {
        for arc in &mut phrase.arcs {
            arc.alternatives.sort_unstable();
            arc.alternatives.dedup();
        }
        phrase.arcs.retain(|arc| {
            arc.start < arc.end && arc.end <= phrase.positions && !arc.alternatives.is_empty()
        });
        phrase.arcs.sort_unstable();
        phrase.arcs.dedup();
    }
    phrases.retain(|phrase| phrase.positions != 0 && !phrase.arcs.is_empty());
    phrases.sort_unstable();
    phrases.dedup();
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
    pub hot_sigs: Vec<u64>,
    pub cost_class: CostClass,
}

pub struct SigPlan {
    pub main_sigs: Vec<u64>,
    pub broad_sigs: Vec<u64>,
    /// Hot-tier signatures (class H, ADR-105): arity-1 anchors stored in the
    /// per-segment hot index — always probed, columnar-evaluated. Empty for
    /// every other class (a query lives in exactly ONE index per segment).
    pub hot_sigs: Vec<u64>,
    pub class: CostClass,
    /// Observe-first telemetry for the Broad-Query Cost Program (roadmap
    /// Increment 1): true when this plan keeps the query on the always-probed
    /// main lane (class A, or an all-selective any-of class B) but its deciding
    /// anchor's frequency is already ≥
    /// [`DEFAULT_HOT_ANCHOR_THETA`](crate::config::DEFAULT_HOT_ANCHOR_THETA) —
    /// i.e. the query *would* reclassify to the hot tier under the default
    /// threshold. Computed only while the θ knob is OFF (with θ on, class H
    /// itself is the signal — `class_counts()[4]`). Purely observational:
    /// nothing reads it on the match path.
    pub would_be_hot: bool,
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
/// holds for free: like `build_signatures`, this only ever reads positive
/// requirements (`ex.required` / `ex.anyof` / `ex.required_phrases`), never
/// `ex.forbidden` or `ex.forbidden_phrases`.
#[derive(Clone, Debug)]
pub struct AnchorPlan {
    /// Each group = one main-index signature's features (arity 1, or 2 for the
    /// escalated class-B pair). Empty for classes C, D and H.
    pub main_anchors: Vec<Vec<FeatureId>>,
    /// Each group = one broad-lane signature's features: arity 1 for class C; for
    /// class D one **empty** group — the universal signature, the lossless cover of
    /// an empty positive set (ADR-068). Empty for classes A/B/H.
    pub broad_anchors: Vec<Vec<FeatureId>>,
    /// Each group = one hot-tier signature's features (arity 1 — the θ-hot
    /// required anchor, or one per member of the chosen θ-hot any-of group;
    /// ADR-105). Empty for every class but H. Like main anchors these are
    /// REQUIRED-side features, so the cluster ring-places them selectively
    /// (`Target::Selective`), identically to class A.
    pub hot_anchors: Vec<Vec<FeatureId>>,
    pub class: CostClass,
    /// Observe-first hot-tier telemetry — see [`SigPlan::would_be_hot`].
    pub would_be_hot: bool,
}
