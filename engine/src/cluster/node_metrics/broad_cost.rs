//! Lean per-shard broad-lane cost counters (ADR-101) — the ADR-091 "broad-lane batch cost"
//! residual, re-deferred by ADR-100 and closed here.
//!
//! Std-only, like the rest of this module: four `AtomicU64` monotone totals accumulated from the
//! [`MatchStats`] every percolate already returns, recorded at the gRPC handler boundary (never
//! inside the engine's match hot path) and rendered as native Prometheus COUNTER exposition. The
//! four names mirror the coordinator's `reverse_rusty_broad_*_total` registry counters exactly
//! (a shard IS an engine — the ADR-091 wire-name rule), with the additive `{shard}` label.
//!
//! Unlike the ADR-100 histogram there is no cross-counter invariant to protect at render time:
//! each total is independent, so relaxed increments + relaxed reads are the whole story (the
//! `transport_metrics` pattern).

use std::sync::atomic::{AtomicU64, Ordering};

use crate::segment::MatchStats;

/// One slot's cumulative broad-lane cost — the field a `ShardSlot` carries (ADR-101). Living on
/// the SLOT (not the swappable `ServerState`) means an in-place `recover_from` state swap keeps
/// the totals continuous; a whole-slot replacement or process restart is an ordinary Prometheus
/// counter reset (the ADR-100 lifetime semantics).
pub(crate) struct SlotBroadCost {
    candidates: AtomicU64,
    postings_scanned: AtomicU64,
    queries_evaluated: AtomicU64,
    batches: AtomicU64,
}

impl SlotBroadCost {
    pub(crate) fn new() -> Self {
        Self {
            candidates: AtomicU64::new(0),
            postings_scanned: AtomicU64::new(0),
            queries_evaluated: AtomicU64::new(0),
            batches: AtomicU64::new(0),
        }
    }

    /// Accumulate one percolate's broad-lane stats. Called unconditionally on the success path —
    /// an `include_broad=false` call carries all-zero broad fields and a `fetch_add(0)` is
    /// branch-free noise, so the handler needs no conditional. The two columnar-only fields
    /// (`queries_evaluated`, `batches`) are structurally 0 on today's per-title `Percolate` wire
    /// (the columnar evaluator runs only under `match_titles_batch`, which no shard RPC reaches);
    /// they are accumulated + rendered anyway so the family is name-symmetric with the
    /// coordinator's and a future batch RPC lights them up without a naming change.
    pub(crate) fn record(&self, stats: &MatchStats) {
        self.candidates
            .fetch_add(u64::from(stats.broad_candidates), Ordering::Relaxed);
        self.postings_scanned
            .fetch_add(u64::from(stats.broad_postings_scanned), Ordering::Relaxed);
        self.queries_evaluated
            .fetch_add(u64::from(stats.broad_queries_evaluated), Ordering::Relaxed);
        self.batches
            .fetch_add(u64::from(stats.broad_batches), Ordering::Relaxed);
    }

    /// A point-in-time copy for rendering (relaxed reads — each total is independent).
    pub(crate) fn snapshot(&self) -> BroadCostSnapshot {
        BroadCostSnapshot {
            candidates: self.candidates.load(Ordering::Relaxed),
            postings_scanned: self.postings_scanned.load(Ordering::Relaxed),
            queries_evaluated: self.queries_evaluated.load(Ordering::Relaxed),
            batches: self.batches.load(Ordering::Relaxed),
        }
    }
}

/// A rendered-side copy of one slot's broad-lane totals. `Default` is the all-zero snapshot —
/// a fresh slot renders zeros, so the series exist from the first scrape (ADR-100 precedent).
#[derive(Default, Clone, Copy)]
pub(crate) struct BroadCostSnapshot {
    pub(crate) candidates: u64,
    pub(crate) postings_scanned: u64,
    pub(crate) queries_evaluated: u64,
    pub(crate) batches: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats(
        broad_candidates: u32,
        broad_postings_scanned: u32,
        broad_queries_evaluated: u32,
        broad_batches: u32,
    ) -> MatchStats {
        MatchStats {
            broad_postings_scanned,
            broad_candidates,
            broad_queries_evaluated,
            broad_batches,
            ..MatchStats::default()
        }
    }

    #[test]
    fn record_accumulates_monotonically_across_calls() {
        let c = SlotBroadCost::new();
        c.record(&stats(3, 10, 0, 0));
        c.record(&stats(2, 5, 7, 1));
        let s = c.snapshot();
        assert_eq!(s.candidates, 5);
        assert_eq!(s.postings_scanned, 15);
        assert_eq!(s.queries_evaluated, 7);
        assert_eq!(s.batches, 1);
    }

    #[test]
    fn fields_accumulate_independently_and_zero_stats_are_a_noop() {
        let c = SlotBroadCost::new();
        c.record(&stats(0, 0, 0, 0));
        let s = c.snapshot();
        assert_eq!(
            (
                s.candidates,
                s.postings_scanned,
                s.queries_evaluated,
                s.batches
            ),
            (0, 0, 0, 0)
        );
        c.record(&stats(1, 0, 0, 0));
        let s = c.snapshot();
        assert_eq!(
            (
                s.candidates,
                s.postings_scanned,
                s.queries_evaluated,
                s.batches
            ),
            (1, 0, 0, 0)
        );
    }

    #[test]
    fn default_snapshot_is_all_zero() {
        let s = BroadCostSnapshot::default();
        assert_eq!(
            (
                s.candidates,
                s.postings_scanned,
                s.queries_evaluated,
                s.batches
            ),
            (0, 0, 0, 0)
        );
    }
}
