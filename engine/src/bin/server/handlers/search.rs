//! Percolate read handlers: `GET|POST /_search` (the rich, per-title path with explain
//! and per-slot stats) and `POST /_mpercolate` (the batch throughput path, columnar
//! broad lane amortized per title-batch — ADR-026). Owns the request-resolution
//! helpers that normalize both the native and ES percolate envelopes (ADR-049 filters).
//!
//! Submodule map:
//! - [`percolate`] — the `GET|POST /_search` handler + its request/response DTOs.
//! - [`mpercolate`] — the `POST /_mpercolate` batch handler + its DTOs.
//! - [`resolve`] — request resolution: native + ES percolate envelopes → `(titles, single, FilterSpec)`.
//! - [`rank`] — the `rank` block + `order_and_page` (post-match reorder + `from`/`size`, ADR-059).
//! - shared hit DTOs (`DocBody`, `SearchHits`, `SearchHitItem`) live in this root, below.

use serde::{Deserialize, Serialize};

use crate::dto::HitSource;

mod controls;
mod mpercolate;
mod percolate;
mod rank;
mod resolve;
mod v2;

#[cfg(test)]
mod pit_tests;
#[cfg(test)]
mod tests;

pub(crate) use mpercolate::mpercolate;
pub(crate) use percolate::search_route;
pub(crate) use v2::{
    cluster_v2_mpercolate_route, cluster_v2_search_route, v2_mpercolate_route, v2_search_route,
};
// The request-resolution helper is shared with the coordinator-mode handlers
// (ADR-070), so both modes parse the identical native + ES envelopes.
pub(crate) use resolve::{resolve_percolate, resolve_percolate_strict};
// The `rank` block + its lowering are shared with the coordinator-mode handlers too
// (ADR-075), so both modes parse the identical ranking request shape.
pub(crate) use controls::{resolve_search_controls, SearchControlInput, SearchParams};
pub(crate) use rank::{to_rank_spec, RankBody};

#[derive(Deserialize)]
pub(crate) struct DocBody {
    pub(crate) title: String,
}

/// Strict native document accepted only by compatibility `GET|POST /_search`.
/// Other percolate surfaces keep their established permissive product-document
/// parsing instead of inheriting ADR-126's compatibility-boundary change.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CompatibilityDocBody {
    pub(crate) title: String,
}

impl From<CompatibilityDocBody> for DocBody {
    fn from(document: CompatibilityDocBody) -> Self {
        Self {
            title: document.title,
        }
    }
}

#[derive(Serialize)]
struct SearchHits {
    total: usize,
    hits: Vec<SearchHitItem>,
}

#[derive(Serialize)]
struct SearchHitItem {
    _index: &'static str,
    _id: u64,
    /// Ranking score (ADR-059) — present only when the request supplied a `rank`
    /// block; omitted (so the response is byte-identical) on the unranked path.
    #[serde(skip_serializing_if = "Option::is_none")]
    _score: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    _source: Option<HitSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    _explanation: Option<reverse_rusty::ExplainDetail>,
}
