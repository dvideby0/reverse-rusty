//! The signature-cover optimizer + cost classifier.
//!
//! [`anchor_plan`] is the single source of truth for anchor selection (it chooses
//! the lossless cover's anchor feature groups and the A/B/C/D cost class);
//! [`build_signatures`] is a thin wrapper that just hashes those groups into
//! `sig_key`s, so the two cannot drift. [`compile_one`] / [`compile_one_readonly`]
//! are the full-compile convenience used by the explain/demo path.
//!
//! Invariant: only `ex.required` / `ex.anyof` are ever read here — forbidden
//! features can never reach an anchor (the lossless-cover contract).

use super::{is_hot, AnchorPlan, CompiledQuery, CostClass, Extracted, SigPlan};
use crate::dict::{Dict, FeatureId};
use crate::normalize::Normalizer;
use crate::util::sig_key;

/// Choose the lossless signature cover's *anchor feature groups* and the cost
/// class (pass B, after the mask is finalized so `is_hot` is meaningful). This is
/// the single source of truth for anchor selection; [`build_signatures`] just
/// hashes the groups it returns.
pub fn anchor_plan(ex: &Extracted, dict: &Dict) -> AnchorPlan {
    let mut main_anchors: Vec<Vec<FeatureId>> = Vec::new();
    let mut broad_anchors: Vec<Vec<FeatureId>> = Vec::new();

    if ex.required.is_empty() && ex.anyof.is_empty() {
        // Class D: the cover of an empty positive set is the UNIVERSAL signature
        // (one empty broad-anchor group, hashed to `util::universal_sig()`), which
        // the match path probes once per segment — so an accepted class-D query is
        // an always-candidate (ADR-068). Whether such a query may be *stored* is
        // gated at ingest (`Segment::add_compiled`), not here; deriving the cover
        // unconditionally keeps every re-derivation site (compaction re-anchoring,
        // the vocab recompile, explain) reproducing it by construction.
        broad_anchors.push(Vec::new());
        return AnchorPlan {
            main_anchors,
            broad_anchors,
            class: CostClass::D,
            would_be_hot: false,
        };
    }

    if ex.required.is_empty() {
        // required empty: cover via the most-selective any-of group.
        // choose the group whose worst (most frequent) member is least frequent.
        // `anyof` is non-empty here (the both-empty case returned class D above),
        // but handle None defensively rather than panicking on the hot build path.
        let Some(best) = ex
            .anyof
            .iter()
            .min_by_key(|g| g.iter().map(|&f| dict.freq(f)).max().unwrap_or(u32::MAX))
        else {
            broad_anchors.push(Vec::new());
            return AnchorPlan {
                main_anchors,
                broad_anchors,
                class: CostClass::D,
                would_be_hot: false,
            };
        };
        let all_selective = best.iter().all(|&f| !is_hot(dict, f));
        if all_selective {
            // Observe-first counter for the Broad-Query Cost Program: a group kept
            // on the main lane whose worst member's frequency already exceeds the
            // default hot-anchor threshold would reclassify to the hot tier.
            let worst = best.iter().map(|&f| dict.freq(f)).max().unwrap_or(0);
            let would_be_hot = worst >= crate::config::DEFAULT_HOT_ANCHOR_THETA;
            for &f in best {
                main_anchors.push(vec![f]);
            }
            AnchorPlan {
                main_anchors,
                broad_anchors,
                class: CostClass::B,
                would_be_hot,
            }
        } else {
            for &f in best {
                broad_anchors.push(vec![f]);
            }
            AnchorPlan {
                main_anchors,
                broad_anchors,
                class: CostClass::C,
                would_be_hot: false,
            }
        }
    } else {
        // required features sorted rarest-first
        let mut r = ex.required.clone();
        r.sort_by_key(|&f| dict.freq(f));
        let r1 = r[0];
        if !is_hot(dict, r1) {
            // arity-1 selective anchor. `would_be_hot`: the anchor has no top-64
            // mask bit yet its frequency exceeds the default hot-anchor threshold —
            // the top-64 rank cliff the Broad-Query Cost Program's hot tier fixes
            // (a fat posting riding the realtime lane). Observe-first telemetry.
            let would_be_hot = dict.freq(r1) >= crate::config::DEFAULT_HOT_ANCHOR_THETA;
            main_anchors.push(vec![r1]);
            AnchorPlan {
                main_anchors,
                broad_anchors,
                class: CostClass::A,
                would_be_hot,
            }
        } else if r.len() >= 2 {
            // hot rarest feature -> escalate to arity-2 with next-rarest
            let r2 = r[1];
            let (a, b) = if r1 < r2 { (r1, r2) } else { (r2, r1) };
            main_anchors.push(vec![a, b]);
            AnchorPlan {
                main_anchors,
                broad_anchors,
                class: CostClass::B,
                would_be_hot: false,
            }
        } else {
            // single, hot required feature and nothing to pair -> broad lane
            broad_anchors.push(vec![r1]);
            AnchorPlan {
                main_anchors,
                broad_anchors,
                class: CostClass::C,
                would_be_hot: false,
            }
        }
    }
}

/// Choose a lossless signature cover and a cost class (pass B, after the mask
/// is finalized so `is_hot` is meaningful). Thin wrapper over [`anchor_plan`]:
/// hashes each anchor group into its `sig_key`. Keeping the two in lockstep is
/// what lets the cluster place by anchor identity without re-deriving selection.
pub fn build_signatures(ex: &Extracted, dict: &Dict) -> SigPlan {
    let plan = anchor_plan(ex, dict);
    SigPlan {
        main_sigs: plan.main_anchors.iter().map(|g| sig_key(g)).collect(),
        broad_sigs: plan.broad_anchors.iter().map(|g| sig_key(g)).collect(),
        class: plan.class,
        would_be_hot: plan.would_be_hot,
    }
}

/// Convenience: full compile for a single query (explain/demo path).
pub fn compile_one(
    text: &str,
    logical_id: u64,
    version: u32,
    norm: &Normalizer,
    dict: &mut Dict,
    lc: &mut String,
) -> Result<CompiledQuery, crate::error::ParseError> {
    let ast = crate::dsl::parse(text)?;
    let ex = super::extract(&ast, norm, dict, lc);
    if !dict.is_finalized() {
        dict.finalize_mask();
    }
    let plan = build_signatures(&ex, dict);
    Ok(CompiledQuery {
        logical_id,
        version,
        extracted: ex,
        main_sigs: plan.main_sigs,
        broad_sigs: plan.broad_sigs,
        cost_class: plan.class,
    })
}

/// Read-only compile: re-derives a CompiledQuery from query text without
/// mutating the Dict. Used for explain on the read path.
pub fn compile_one_readonly(
    text: &str,
    logical_id: u64,
    norm: &Normalizer,
    dict: &Dict,
    lc: &mut String,
) -> Result<CompiledQuery, crate::error::ParseError> {
    let ast = crate::dsl::parse(text)?;
    let ex = super::extract_readonly(&ast, norm, dict, lc);
    let plan = build_signatures(&ex, dict);
    Ok(CompiledQuery {
        logical_id,
        version: 0,
        extracted: ex,
        main_sigs: plan.main_sigs,
        broad_sigs: plan.broad_sigs,
        cost_class: plan.class,
    })
}
