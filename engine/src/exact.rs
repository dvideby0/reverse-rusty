//! Exact, integer-only verification — the final filter on the match hot path.
//!
//! Design: docs/design/matching.md §3
//! Invariant: No strings, no regex, no virtual dispatch, no alloc — integer
//!   masks and sorted-slice galloping only
//! Hot path: yes — called for every candidate that passes the index probe
//!
//! Struct-of-arrays layout indexed by SegmentLocalQueryId. Most candidates are
//! rejected by the common-mask gate (two u64 ops) before any memory traffic
//! beyond the candidate's own two mask words.
//!
//! This file holds the shared tag-filter primitives ([`TagPredicate`] + the
//! sorted-slice tag helpers both the SoA store and the raw-slice verifiers use);
//! the rest splits into focused submodules so each concern is self-contained:
//!   - [`slices`] — the free raw-slice verifiers ([`verify_slices`] scalar +
//!     [`eval_batch_slices`] columnar), shared by the in-memory `ExactStore` and
//!     the mmap `MmapSegment` so the two read paths cannot drift
//!   - [`store`]  — the [`ExactStore`] SoA struct + build/insertion + `verify` /
//!     `eval_batch` / pure-anchor derivation / serialization accessors
//!   - [`tests`]  — the tag-filter unit tests (`#[cfg(test)]`)

use crate::dict::FeatureId;
use crate::tagdict::TagId;

mod slices;
mod store;

#[cfg(test)]
mod tests;

pub use slices::{eval_batch_slices, verify_slices};
pub use store::ExactStore;

/// The two title feature views threaded through exact verification (ADR-061).
///
/// - **Positive** (`pos_mask` / `pos`) is the overlapping superset `P(T)` — used for the
///   required-mask gate, the required tail, and any-of groups. Built from all overlapping
///   alias entities so a `new york` query finds a `new york city` title.
/// - **Negative** (`neg_mask` / `neg`) is the canonical leftmost-longest `N(T) ⊆ P(T)` — used
///   **only** for the forbidden-mask gate and the forbidden tail, so a MUST_NOT clause stays
///   recall-correct (`foo -"new york"` still matches `foo new york city`).
///
/// With no active multi-word alias the two views are the same slice ([`TitleView::single`]) and
/// the verifier is byte-for-byte the pre-ADR-061 single-view path. `Copy` (two masks + two fat
/// pointers); the per-query SoA columns stay raw args in [`verify_slices`] per the hot-path note.
#[derive(Clone, Copy)]
pub struct TitleView<'a> {
    pub pos_mask: u64,
    pub pos: &'a [FeatureId],
    pub neg_mask: u64,
    pub neg: &'a [FeatureId],
}

impl<'a> TitleView<'a> {
    /// A single-view title (no multi-word aliases): the positive and negative views are the
    /// same mask + slice, so verification is identical to the pre-ADR-061 path.
    #[inline]
    #[must_use]
    pub fn single(mask: u64, feats: &'a [FeatureId]) -> Self {
        Self {
            pos_mask: mask,
            pos: feats,
            neg_mask: mask,
            neg: feats,
        }
    }

    /// Distinct positive (superset `P(T)`) and negative (canonical `N(T)`) views.
    #[inline]
    #[must_use]
    pub fn dual(pos_mask: u64, pos: &'a [FeatureId], neg_mask: u64, neg: &'a [FeatureId]) -> Self {
        Self {
            pos_mask,
            pos,
            neg_mask,
            neg,
        }
    }
}

/// A compiled tag filter (ADR-049): a conjunction of per-key value-sets, each value-set a
/// sorted, deduped list of `TagId`s. A query passes iff EVERY group shares at least one
/// `TagId` with the query's sorted tag set (AND across keys, OR within a key). Compiled
/// once per request from the REST filter; tested only in the post-candidate verify stage,
/// never in candidate retrieval — so it can only ever *remove* queries the caller did not
/// ask for, never drop a wanted match (the "tags never gate" invariant, matching.md §5.3).
///
/// - **Empty** (`groups` empty) ⇒ no filter ⇒ every query passes. The verify clause is a
///   single never-taken branch, so the no-filter path is unchanged from before tags.
/// - **A present-but-empty group** ⇒ matches nothing (a filter on an all-unknown value):
///   intersecting a sorted set with an empty slice is empty, so the group fails — the
///   safe direction (a filter can never over-return).
#[derive(Clone, Debug, Default)]
pub struct TagPredicate {
    groups: Vec<Vec<TagId>>,
}

impl TagPredicate {
    /// The empty predicate — matches everything (no filtering). `const` so callers can
    /// pass `&TagPredicate::empty()` with no allocation.
    #[inline]
    #[must_use]
    pub const fn empty() -> Self {
        TagPredicate { groups: Vec::new() }
    }

    /// Build from per-key value-sets — each inner vec is one key's accepted `TagId`s.
    /// Each group is sorted+deduped here (off the hot path) so the per-candidate check is
    /// a sorted-slice intersection.
    #[must_use]
    pub fn new(mut groups: Vec<Vec<TagId>>) -> Self {
        for g in &mut groups {
            g.sort_unstable();
            g.dedup();
        }
        TagPredicate { groups }
    }

    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    #[inline]
    #[must_use]
    pub fn groups(&self) -> &[Vec<TagId>] {
        &self.groups
    }

    /// Whether a query's sorted `TagId` slice satisfies this predicate (every group must
    /// intersect `qtags`). Empty predicate ⇒ always true. Used by the broad-lane
    /// pure-anchor fast path, which has the query's tags but bypasses `verify`.
    #[inline]
    #[must_use]
    pub fn matches(&self, qtags: &[TagId]) -> bool {
        for group in &self.groups {
            if !sorted_intersects(qtags, group) {
                return false;
            }
        }
        true
    }
}

/// True if two sorted `TagId` slices share at least one element. Gallops the smaller
/// slice via binary search through the larger — integer-only, allocation-free. An empty
/// slice on either side yields `false` (a group with no resolvable value matches nothing).
#[inline]
fn sorted_intersects(a: &[TagId], b: &[TagId]) -> bool {
    let (small, large) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    for &x in small {
        if large.binary_search(&x).is_ok() {
            return true;
        }
    }
    false
}

/// Whether query `i`'s tag set satisfies `pred`: every group must intersect the query's
/// sorted `tag_blob[tag_off[i]..+tag_len[i]]`. An untagged query (empty tag slice) fails
/// any non-empty predicate — exactly the back-compat reading of a tag-less (v1/v2) segment.
/// Integer-only and allocation-free; only reached when `pred` is non-empty.
#[inline]
fn query_passes_tags(
    i: usize,
    pred: &TagPredicate,
    tag_off: &[u32],
    tag_len: &[u16],
    tag_blob: &[TagId],
) -> bool {
    // A query with no tag entry — a pre-tag (v1/v2) segment exposes empty tag columns —
    // is untagged, so its tag slice is empty and it fails any non-empty group.
    let qtags: &[TagId] = match (tag_off.get(i), tag_len.get(i)) {
        (Some(&o), Some(&l)) => &tag_blob[o as usize..o as usize + l as usize],
        _ => &[],
    };
    for group in &pred.groups {
        if !sorted_intersects(qtags, group) {
            return false;
        }
    }
    true
}
