//! [`RefMatcher`] — the front-end-independent reference: build from `(logical_id, dsl)` queries +
//! a [`RefVocab`], then `matches(title)` returns the set of logical ids that match.
//!
//! Build mirrors `engine/src/segment` `build_from_queries` + `compile/extract`: queries are
//! processed in order, a per-feature frequency counter is accumulated (governing any-of proxy
//! selection), and class-D (negation-only / empty) queries are dropped — exactly as the engine
//! drops them at ingest. The predicate is the engine's, over the ADR-061 two title views.

use crate::extract::{extract_literal, EquivMap, Freq, RefQuery};
use crate::features::Feature;
use crate::normalize::{emit, match_features_dual, Side};
use crate::parse::parse;
use crate::vocab::RefVocab;
use std::collections::{BTreeSet, HashMap, HashSet};

/// The reference matcher: a fixed vocabulary + the compiled queries kept after class-D drops.
pub struct RefMatcher {
    vocab: RefVocab,
    queries: Vec<(u64, RefQuery)>,
}

impl RefMatcher {
    /// Build the reference, dropping class-D queries (no required AND no any-of) — the engine's
    /// default ingest behaviour.
    #[must_use]
    pub fn build(queries: &[(u64, String)], vocab: RefVocab) -> Self {
        Self::build_inner(queries, vocab, false)
    }

    /// Build the reference KEEPING forbidden-only (class-D) queries — the ground truth for the
    /// ADR-068 always-candidate lane. Only the truly-empty query (no required, any-of, OR forbidden)
    /// is dropped, matching the lane's accept rule.
    #[must_use]
    pub fn build_accepting_class_d(queries: &[(u64, String)], vocab: RefVocab) -> Self {
        Self::build_inner(queries, vocab, true)
    }

    fn build_inner(queries: &[(u64, String)], vocab: RefVocab, accept_class_d: bool) -> Self {
        let equiv = build_equiv_map(&vocab);
        let mut freq: Freq = HashMap::new();
        let mut out: Vec<(u64, RefQuery)> = Vec::new();

        for (logical, text) in queries {
            let Ok(ast) = parse(text) else {
                continue; // unparseable: dropped on both sides (engine skips it at ingest)
            };
            // The LITERAL query drives the frequency bump (the engine bumps before expansion).
            let mut q = extract_literal(&ast, &vocab, &freq);
            for f in q.bump_features() {
                *freq.entry(f).or_insert(0) += 1;
            }
            // The EXPANDED query drives matching.
            q.expand_equivalences(&equiv);

            let drop = if accept_class_d {
                q.required.is_empty() && q.anyof.is_empty() && q.forbidden.is_empty()
            } else {
                q.is_class_d()
            };
            if drop {
                continue;
            }
            out.push((*logical, q));
        }

        RefMatcher {
            vocab,
            queries: out,
        }
    }

    /// The set of logical query ids matching `title`. Required + any-of are checked against the
    /// positive view `P(T)`; forbidden against the canonical view `N(T)` (ADR-061). With no active
    /// multi-word alias the two views are identical.
    #[must_use]
    pub fn matches(&self, title: &str) -> HashSet<u64> {
        let (neg, pos) = match_features_dual(&self.vocab, title);
        let in_pos = |f: &Feature| pos.binary_search(f).is_ok();
        let in_neg = |f: &Feature| neg.binary_search(f).is_ok();
        let mut out = HashSet::new();
        for (logical, q) in &self.queries {
            if q.required.iter().all(&in_pos)
                && !q.forbidden.iter().any(&in_neg)
                && q.anyof.iter().all(|g| g.iter().any(&in_pos))
            {
                out.insert(*logical);
            }
        }
        out
    }

    /// The number of queries retained (after class-D drops). For test diagnostics.
    #[must_use]
    pub fn len(&self) -> usize {
        self.queries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queries.is_empty()
    }
}

/// Resolve the vocabulary's equivalence groups (surface forms) into a feature->group map (ADR-054),
/// reproducing `Vocab::resolve_equivalences`: (1) resolve each declared group's forms to a feature
/// set — a form participates only if it resolves to exactly one feature (the conservative
/// single-entity rule), and a group needs >=2 such features; (2) **merge overlapping groups
/// transitively** so `[a,b]` + `[b,c]` collapse into one class `{a,b,c}` (an equivalence is
/// transitive, NOT an order-dependent overwrite of the shared member); (3) map each member to its
/// full merged group. Empty when no equivalences are declared, so the default / grader phases pass
/// an empty map (expansion is a no-op).
fn build_equiv_map(vocab: &RefVocab) -> EquivMap {
    if vocab.equivalences.is_empty() {
        return HashMap::new();
    }
    // 1. Resolve each declared group to a feature set.
    let mut groups: Vec<BTreeSet<Feature>> = Vec::new();
    for group in &vocab.equivalences {
        let mut feats: BTreeSet<Feature> = BTreeSet::new();
        for form in group {
            let mut f = emit(vocab, form, Side::Query, false);
            f.sort();
            f.dedup();
            if f.len() == 1 {
                feats.insert(f.into_iter().next().expect("len==1"));
            }
        }
        if feats.len() >= 2 {
            groups.push(feats);
        }
    }
    // 2. Merge groups that share any feature into one transitive class (connected components).
    let merged = merge_overlapping_groups(groups);
    // 3. Map each member -> its full (merged, sorted) group.
    let mut map: EquivMap = HashMap::new();
    for g in &merged {
        let group: Vec<Feature> = g.iter().cloned().collect();
        for f in g {
            map.insert(f.clone(), group.clone());
        }
    }
    map
}

/// Merge any feature-sets that share a member into one (connected components), so an equivalence is
/// transitive. Mirrors `vocab::methods::merge_overlapping_groups`; the resulting partition is
/// order-independent.
fn merge_overlapping_groups(groups: Vec<BTreeSet<Feature>>) -> Vec<BTreeSet<Feature>> {
    let mut result: Vec<BTreeSet<Feature>> = Vec::new();
    for g in groups {
        // Absorb every existing class g overlaps (g may chain several together).
        let mut acc = g;
        let mut i = 0;
        while i < result.len() {
            if result[i].iter().any(|f| acc.contains(f)) {
                let other = result.swap_remove(i); // order-independent; re-check this slot
                acc.extend(other);
            } else {
                i += 1;
            }
        }
        result.push(acc);
    }
    result
}
