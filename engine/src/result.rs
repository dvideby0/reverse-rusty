//! Result-contract primitives for ranked percolation (ADR-107).
//!
//! These types describe the reserved v2 search contract. They do not register an
//! HTTP route or change the compatibility APIs; the bounded serving path lands in
//! a later increment. Keeping the primitives in the lean core gives the local,
//! cluster, and server layers one vocabulary when that work begins.

use serde::{Deserialize, Serialize};

/// Reserved default number of winners for a v2 ranked-search request.
pub const DEFAULT_TOP_K: usize = 100;
/// Hard admission ceiling reserved for v2 ranked search.
pub const MAX_TOP_K: usize = 10_000;
/// Reserved default threshold above which total hits become a lower bound.
pub const DEFAULT_TRACK_TOTAL_HITS_UP_TO: u64 = 10_000;

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
}
