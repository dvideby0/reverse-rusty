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

use crate::dict::{Dict, FeatureId};
use crate::dsl::{Ast, Atom};
use crate::normalize::Normalizer;
use crate::util::sig_key;

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

/// "hot" == one of the 64 most frequent features (has a common-mask bit).
/// Both compile and match agree on this, which is what keeps the cover lossless.
#[inline]
pub fn is_hot(dict: &Dict, f: FeatureId) -> bool {
    dict.mask_bit(f) != crate::dict::NO_MASK_BIT
}

/// Extract required / forbidden / any-of from an AST, interning features and
/// bumping their query-frequency. Run for every query in pass A.
pub fn extract(ast: &Ast, norm: &Normalizer, dict: &mut Dict, lc: &mut String) -> Extracted {
    let mut required: Vec<FeatureId> = Vec::new();
    let mut forbidden: Vec<FeatureId> = Vec::new();
    let mut anyof: Vec<Vec<FeatureId>> = Vec::new();

    // Consecutive positive bare words are normalized JOINTLY (in original order)
    // so multiword entities ("michael jordan", "psa 10") are recognized exactly
    // as they are in titles. Without this the query and title feature spaces
    // would disagree and we'd get false negatives.
    let mut pos_words: Vec<&str> = Vec::new();

    for clause in &ast.clauses {
        match (&clause.atom, clause.negated) {
            (Atom::Term(w), false) => {
                pos_words.push(w.as_str());
            }
            (Atom::Term(w) | Atom::Phrase(w), true) => {
                let feats = norm.compile_features(w, dict, lc);
                forbidden.extend_from_slice(&feats);
            }
            (Atom::Phrase(w), false) => {
                let feats = norm.compile_features(w, dict, lc);
                required.extend_from_slice(&feats);
            }
            (Atom::AnyOf(members), neg) => {
                if neg {
                    // -(a,b,c): reject if ANY member feature present
                    for m in members {
                        let feats = norm.compile_features(m, dict, lc);
                        forbidden.extend_from_slice(&feats);
                    }
                } else {
                    // (a,b,c): >=1 member present. Represent each member by its
                    // rarest (most specific) normalized feature.
                    let mut group: Vec<FeatureId> = Vec::new();
                    for m in members {
                        let feats = norm.compile_features(m, dict, lc);
                        if let Some(&rep) = feats.iter().min_by_key(|&&f| dict.freq(f)) {
                            group.push(rep);
                        }
                    }
                    group.sort_unstable();
                    group.dedup();
                    if group.len() == 1 {
                        // singleton group is just a required feature (more selective)
                        required.push(group[0]);
                    } else if !group.is_empty() {
                        anyof.push(group);
                    }
                }
            }
        }
    }

    // normalize the joined positive bare words as one stream
    if !pos_words.is_empty() {
        let joined = pos_words.join(" ");
        let feats = norm.compile_features(&joined, dict, lc);
        required.extend_from_slice(&feats);
    }

    required.sort_unstable();
    required.dedup();
    forbidden.sort_unstable();
    forbidden.dedup();

    // bump frequency once per distinct required/anyof feature (gating-relevant)
    for &f in &required {
        dict.bump_freq(f);
    }
    for g in &anyof {
        for &f in g {
            dict.bump_freq(f);
        }
    }

    Extracted {
        required,
        forbidden,
        anyof,
    }
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

/// Choose the lossless signature cover's *anchor feature groups* and the cost
/// class (pass B, after the mask is finalized so `is_hot` is meaningful). This is
/// the single source of truth for anchor selection; [`build_signatures`] just
/// hashes the groups it returns.
pub fn anchor_plan(ex: &Extracted, dict: &Dict) -> AnchorPlan {
    let mut main_anchors: Vec<Vec<FeatureId>> = Vec::new();
    let mut broad_anchors: Vec<Vec<FeatureId>> = Vec::new();

    if ex.required.is_empty() && ex.anyof.is_empty() {
        return AnchorPlan {
            main_anchors,
            broad_anchors,
            class: CostClass::D,
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
            return AnchorPlan {
                main_anchors,
                broad_anchors,
                class: CostClass::D,
            };
        };
        let all_selective = best.iter().all(|&f| !is_hot(dict, f));
        if all_selective {
            for &f in best {
                main_anchors.push(vec![f]);
            }
            AnchorPlan {
                main_anchors,
                broad_anchors,
                class: CostClass::B,
            }
        } else {
            for &f in best {
                broad_anchors.push(vec![f]);
            }
            AnchorPlan {
                main_anchors,
                broad_anchors,
                class: CostClass::C,
            }
        }
    } else {
        // required features sorted rarest-first
        let mut r = ex.required.clone();
        r.sort_by_key(|&f| dict.freq(f));
        let r1 = r[0];
        if !is_hot(dict, r1) {
            // arity-1 selective anchor
            main_anchors.push(vec![r1]);
            AnchorPlan {
                main_anchors,
                broad_anchors,
                class: CostClass::A,
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
            }
        } else {
            // single, hot required feature and nothing to pair -> broad lane
            broad_anchors.push(vec![r1]);
            AnchorPlan {
                main_anchors,
                broad_anchors,
                class: CostClass::C,
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
    let ex = extract(&ast, norm, dict, lc);
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

/// Read-only extract: resolves features against the frozen dict WITHOUT interning
/// (interning new vocabulary would fork the `Arc<Dict>` shared across shards). A term
/// absent from the dict is NOT skipped — `compile_features_readonly` resolves it to a
/// deterministic synthetic `FeatureId` via `dict.get_or_synthetic()` (dynamic
/// vocabulary, ADR-046), so a new required term still anchors its query (a collision is
/// a bounded over-match, never a dropped match). Safe for the read path and the cluster
/// coordinator's incremental adds against a frozen shared dict.
pub fn extract_readonly(ast: &Ast, norm: &Normalizer, dict: &Dict, lc: &mut String) -> Extracted {
    let mut required: Vec<FeatureId> = Vec::new();
    let mut forbidden: Vec<FeatureId> = Vec::new();
    let mut anyof: Vec<Vec<FeatureId>> = Vec::new();

    let mut pos_words: Vec<&str> = Vec::new();

    for clause in &ast.clauses {
        match (&clause.atom, clause.negated) {
            (Atom::Term(w), false) => {
                pos_words.push(w.as_str());
            }
            (Atom::Term(w) | Atom::Phrase(w), true) => {
                let feats = norm.compile_features_readonly(w, dict, lc);
                forbidden.extend_from_slice(&feats);
            }
            (Atom::Phrase(w), false) => {
                let feats = norm.compile_features_readonly(w, dict, lc);
                required.extend_from_slice(&feats);
            }
            (Atom::AnyOf(members), neg) => {
                if neg {
                    for m in members {
                        let feats = norm.compile_features_readonly(m, dict, lc);
                        forbidden.extend_from_slice(&feats);
                    }
                } else {
                    let mut group: Vec<FeatureId> = Vec::new();
                    for m in members {
                        let feats = norm.compile_features_readonly(m, dict, lc);
                        if let Some(&rep) = feats.iter().min_by_key(|&&f| dict.freq(f)) {
                            group.push(rep);
                        }
                    }
                    group.sort_unstable();
                    group.dedup();
                    if group.len() == 1 {
                        required.push(group[0]);
                    } else if !group.is_empty() {
                        anyof.push(group);
                    }
                }
            }
        }
    }

    if !pos_words.is_empty() {
        let joined = pos_words.join(" ");
        let feats = norm.compile_features_readonly(&joined, dict, lc);
        required.extend_from_slice(&feats);
    }

    required.sort_unstable();
    required.dedup();
    forbidden.sort_unstable();
    forbidden.dedup();

    Extracted {
        required,
        forbidden,
        anyof,
    }
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
    let ex = extract_readonly(&ast, norm, dict, lc);
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
