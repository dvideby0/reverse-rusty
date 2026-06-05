//! Broad-lane batch / columnar evaluation — the once-per-batch broad matcher.
//!
//! Design: docs/design/matching.md §4 (broad lane); ADR-026.
//! Invariant: produces a per-title match set BYTE-IDENTICAL to the scalar
//!   per-title broad path (`Segment::match_into(include_broad=true)`). Forbidden
//!   features are consulted ONLY in verification, never to retrieve candidates.
//! Hot path: yes — but amortized: each broad posting is walked ONCE PER BATCH,
//!   not once per title, and per-query verification is bitmap algebra.
//!
//! Today the broad lane is evaluated inline, per title: a hot anchor's huge
//! posting is re-scanned (and its candidates re-verified) for *every* title that
//! contains that feature. This module inverts the loop. For a batch of titles it
//! builds a per-batch inverted index (feature → bitmap-of-titles), collects the
//! broad queries reachable from the batch by probing each broad posting *once*,
//! and evaluates each reachable query with [`crate::exact::eval_batch_slices`]
//! (the bitmap transpose of `verify`). Pure-anchor queries — whose entire
//! semantics is their hot anchor — skip verification entirely and emit directly
//! from the anchor's title bitmap (the streaming-safe analog of "materialized
//! subscriptions").
//!
//! The selective lane (main index) is unchanged and still runs per title — it is
//! already fast and scale-flat. Only the broad lane is batched.
//!
//! This file is the module root; the implementation is split into focused
//! submodules so each concern is self-contained:
//!   - [`kernel`] — the columnar/bitmap eval kernels: the per-segment
//!     [`BroadBackend`](kernel::BroadBackend) surface (in-memory `Segment` +
//!     file-backed `MmapSegment`), pure-anchor materialization, and bitmap
//!     verification (`eval_one_segment`).
//!   - [`driver`] — the batch driver: the per-rayon-chunk `match_batch_chunk`
//!     (selective lane per title + columnar broad lane once over the chunk), the
//!     reusable [`BroadBatchScratch`](driver::BroadBatchScratch), and the public
//!     batch entry points (`batch_results`, `batch_results_with_stats`,
//!     `batch_stats`).

mod driver;
mod kernel;

// Re-export the batch entry points so the module's external surface
// (`super::broad_batch::batch_results` etc., as consumed by `matching.rs` and
// `snapshot.rs`) is byte-identical to before the split.
pub(in crate::segment) use driver::{batch_results, batch_results_with_stats, batch_stats};
