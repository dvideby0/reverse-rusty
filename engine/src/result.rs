//! Result-contract primitives for ranked percolation (ADR-107).
//!
//! These types describe the local v2 search contract while leaving compatibility
//! matching and ranking APIs unchanged. Keeping the primitives in the lean core
//! gives future cluster and serving layers the same vocabulary.

use serde::{Deserialize, Serialize};

/// Default number of winners for a v2 ranked-search request.
pub const DEFAULT_TOP_K: usize = 100;
/// Hard admission ceiling for v2 ranked search.
pub const MAX_TOP_K: usize = 10_000;
/// Default threshold above which total hits become a lower bound.
pub const DEFAULT_TRACK_TOTAL_HITS_UP_TO: u64 = 10_000;
/// Hard admission ceiling on titles in one ranked batch (ADR-112) — aligned
/// with the HTTP layer's `max_percolate_batch` default.
pub const MAX_RANKED_BATCH_TITLES: usize = 10_000;
/// Aggregate eager-heap budget for one ranked batch: `size × titles` must stay
/// under this bound (ADR-112). Each per-title collector eagerly reserves K heap
/// slots + K id-set slots (~40 B/row ⇒ ~40 MiB at this ceiling); the lazy
/// total tracker is additionally threshold-capped per title.
pub const MAX_RANKED_BATCH_HEAP_ROWS: u64 = 1 << 20;

/// Admission-bounded options for one local ranked percolation request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TopKOptions {
    pub size: usize,
    pub track_total_hits_up_to: u64,
    pub query_scope: QueryScope,
}

impl Default for TopKOptions {
    fn default() -> Self {
        Self {
            size: DEFAULT_TOP_K,
            track_total_hits_up_to: DEFAULT_TRACK_TOTAL_HITS_UP_TO,
            query_scope: QueryScope::Standard,
        }
    }
}

/// Typed admission failures shared by the lean core and HTTP layer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopKAdmissionError {
    SizeTooLarge {
        requested: usize,
        max: usize,
    },
    TotalHitsThresholdTooLarge {
        requested: u64,
        max: u64,
    },
    /// ADR-112: too many titles in one ranked batch.
    BatchTitlesTooLarge {
        requested: usize,
        max: usize,
    },
    /// ADR-112: `size × titles` exceeds the aggregate eager-heap budget.
    BatchHeapBudgetExceeded {
        requested_rows: u64,
        max: u64,
    },
}

impl std::fmt::Display for TopKAdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SizeTooLarge { requested, max } => {
                write!(f, "size {requested} exceeds maximum {max}")
            }
            Self::TotalHitsThresholdTooLarge { requested, max } => write!(
                f,
                "track_total_hits_up_to {requested} exceeds maximum {max}"
            ),
            Self::BatchTitlesTooLarge { requested, max } => {
                write!(f, "batch of {requested} titles exceeds maximum {max}")
            }
            Self::BatchHeapBudgetExceeded {
                requested_rows,
                max,
            } => write!(
                f,
                "batch heap budget of {requested_rows} rows (size x titles) exceeds maximum {max}"
            ),
        }
    }
}

impl std::error::Error for TopKAdmissionError {}

/// The one ranked total order over `(score, logical_id)`: score descending,
/// then logical id ascending (ADR-107/110). Every ranked surface — the bounded
/// collector's heap and presentation sort, the coordinator merge and its
/// shard-reply ordering guard, and the ADR-059 presentation sort — must express
/// ordering through this function (or [`ranked_beats`]) so the tie rule cannot
/// drift between them. `Less` means `a` precedes `b` in presentation order.
#[inline]
#[must_use]
pub fn ranked_order(a: (i64, u64), b: (i64, u64)) -> std::cmp::Ordering {
    b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1))
}

/// `a` strictly precedes `b` under [`ranked_order`]; an identical pair does not
/// beat itself, which is what makes the strict shard-reply ordering guard and
/// the heap-replacement predicate the same rule.
#[inline]
#[must_use]
pub fn ranked_beats(a: (i64, u64), b: (i64, u64)) -> bool {
    ranked_order(a, b) == std::cmp::Ordering::Less
}

/// Which accepted visibility classes a request asks the matcher to evaluate.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryScope {
    /// Default-visible queries only.
    #[default]
    Standard,
    /// Default-visible queries plus the opt-in class-C broad visibility lane.
    WithBroad,
}

/// Projection of the conceptual exact Boolean match set requested by a caller.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultMode {
    /// Exact best K under one deterministic total rank order.
    #[default]
    TopK,
    /// Exhaustive delivery of every exact match, chunked by a later serving layer.
    All,
    /// Explicitly approximate work bounded by a declared budget.
    Terminated,
}

/// Whether a reported hit count is exact or only a proven lower bound.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TotalHitsRelation {
    /// `value` is the exact number of matches.
    Eq,
    /// At least `value` matches exist; counting stopped at the declared threshold.
    Gte,
}

/// Honest total-hit count for bounded result collection.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TotalHits {
    /// Exact count, or the configured threshold when [`Self::relation`] is `Gte`.
    pub value: u64,
    /// Exact-versus-lower-bound discriminator.
    pub relation: TotalHitsRelation,
}

impl TotalHits {
    /// Construct an exact total.
    #[must_use]
    pub const fn exact(value: u64) -> Self {
        Self {
            value,
            relation: TotalHitsRelation::Eq,
        }
    }

    /// Construct a thresholded lower bound.
    #[must_use]
    pub const fn lower_bound(value: u64) -> Self {
        Self {
            value,
            relation: TotalHitsRelation::Gte,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_names_and_defaults_are_pinned() {
        assert_eq!(QueryScope::default(), QueryScope::Standard);
        assert_eq!(ResultMode::default(), ResultMode::TopK);
        assert_eq!(
            serde_json::to_string(&QueryScope::WithBroad).unwrap(),
            "\"with_broad\""
        );
        assert_eq!(
            serde_json::to_string(&ResultMode::TopK).unwrap(),
            "\"top_k\""
        );
        assert_eq!(
            serde_json::to_string(&TotalHitsRelation::Gte).unwrap(),
            "\"gte\""
        );
    }

    #[test]
    fn total_hits_round_trip() {
        let total = TotalHits::lower_bound(DEFAULT_TRACK_TOTAL_HITS_UP_TO);
        let json = serde_json::to_string(&total).unwrap();
        assert_eq!(serde_json::from_str::<TotalHits>(&json).unwrap(), total);
    }

    #[test]
    fn ranked_order_is_score_desc_then_id_asc() {
        use std::cmp::Ordering;
        // Higher score precedes lower, regardless of id.
        assert_eq!(ranked_order((10, 9), (5, 1)), Ordering::Less);
        assert_eq!(ranked_order((-1, 0), (0, 9)), Ordering::Greater);
        // Score tie: lower id precedes.
        assert_eq!(ranked_order((5, 1), (5, 2)), Ordering::Less);
        assert_eq!(ranked_order((5, 2), (5, 1)), Ordering::Greater);
        // Only an identical pair is Equal.
        assert_eq!(ranked_order((5, 1), (5, 1)), Ordering::Equal);
    }

    #[test]
    fn ranked_beats_is_the_strict_form() {
        assert!(ranked_beats((6, 9), (5, 0)));
        assert!(ranked_beats((5, 1), (5, 2)));
        assert!(!ranked_beats((5, 2), (5, 1)));
        assert!(!ranked_beats((5, 1), (5, 1)));
    }

    #[test]
    fn ranked_order_is_a_total_order() {
        use std::cmp::Ordering;
        let mut pairs = Vec::new();
        for score in -2i64..=2 {
            for id in 0u64..4 {
                pairs.push((score, id));
            }
        }
        for &a in &pairs {
            for &b in &pairs {
                // Antisymmetry, and Equal exactly on identical pairs.
                assert_eq!(ranked_order(a, b), ranked_order(b, a).reverse());
                assert_eq!(ranked_order(a, b) == Ordering::Equal, a == b);
                for &c in &pairs {
                    // Transitivity of strict precedence.
                    if ranked_beats(a, b) && ranked_beats(b, c) {
                        assert!(ranked_beats(a, c));
                    }
                }
            }
        }
    }
}
