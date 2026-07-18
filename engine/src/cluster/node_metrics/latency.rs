//! Lean per-shard RPC latency histograms (ADR-100) — the ADR-091 "per-shard p95/p99" residual.
//!
//! Std-only, like the rest of this module: fixed log-ladder buckets of `AtomicU64`, recorded at
//! the gRPC handler boundary (never inside the engine's match hot path) and rendered as native
//! Prometheus HISTOGRAM exposition, so operators get quantiles via `histogram_quantile()` —
//! nothing is precomputed server-side. Recording is unconditional (~two `Instant` reads + three
//! relaxed `fetch_add`s per RPC — noise next to any RPC); the exposition is only reachable via
//! the opt-in `--metrics-addr` listener.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Finite bucket upper bounds: the inclusive nanosecond threshold paired with the EXACT `le`
/// label rendered for it (seconds), so bound and label can never drift apart. A 1–2.5–5 log
/// ladder from 2.5 µs to 30 s: the bottom decade resolves the selective path (in-process p99 is
/// single-digit µs — see `docs/performance/results.md` §1 — plus handler/transport overhead),
/// the middle the broad lane (tens of µs to ms under co-location), and the top aligns with the
/// ADR-085 client deadlines (10 s read / 30 s write) — anything slower already failed client-side.
pub(crate) const LATENCY_LE: [(u64, &str); 22] = [
    (2_500, "0.0000025"),
    (5_000, "0.000005"),
    (10_000, "0.00001"),
    (25_000, "0.000025"),
    (50_000, "0.00005"),
    (100_000, "0.0001"),
    (250_000, "0.00025"),
    (500_000, "0.0005"),
    (1_000_000, "0.001"),
    (2_500_000, "0.0025"),
    (5_000_000, "0.005"),
    (10_000_000, "0.01"),
    (25_000_000, "0.025"),
    (50_000_000, "0.05"),
    (100_000_000, "0.1"),
    (250_000_000, "0.25"),
    (500_000_000, "0.5"),
    (1_000_000_000, "1"),
    (2_500_000_000, "2.5"),
    (5_000_000_000, "5"),
    (10_000_000_000, "10"),
    (30_000_000_000, "30"),
];

/// The shard-side RPCs we time — the discriminant indexes [`SHARD_RPC_LABELS`] and the per-slot
/// histogram array. Percolate and its ranked variant are split to mirror the coordinator's
/// ADR-085 per-method client labels, so `client latency − shard service latency ≈ network +
/// queueing` is computable per method. Insert/delete/flush are deliberately untimed (rare,
/// per-item, or not service-latency-shaped); adding one is one enum variant + one label.
#[derive(Clone, Copy)]
pub(crate) enum ShardRpc {
    Percolate = 0,
    PercolateRanked = 1,
    PercolateTopK = 2,
    FetchMatches = 3,
    Ingest = 4,
    PercolateTopKBatch = 5,
}

/// The `method` label value per [`ShardRpc`] discriminant.
pub(crate) const SHARD_RPC_LABELS: [&str; 6] = [
    "percolate",
    "percolate_ranked",
    "percolate_top_k",
    "fetch_matches",
    "ingest",
    "percolate_top_k_batch",
];

/// A lean fixed-bucket latency histogram: lock-free `AtomicU64`s, `Relaxed` everywhere (each
/// counter is an independent monotone total — the `transport_metrics` pattern). Buckets are
/// stored NON-cumulative and cumulated at render. `Relaxed` gives no cross-counter ordering, so
/// a concurrent scrape can observe a bucket increment whose count increment it misses (or vice
/// versa); the renderer clamps `+Inf` to `max(count, Σ buckets)` so the exposition is always a
/// well-formed histogram regardless of interleaving — see [`LatencySnapshot::total`].
pub(crate) struct LatencyHistogram {
    buckets: [AtomicU64; LATENCY_LE.len()],
    sum_nanos: AtomicU64,
    count: AtomicU64,
}

impl LatencyHistogram {
    pub(crate) fn new() -> Self {
        Self {
            buckets: [const { AtomicU64::new(0) }; LATENCY_LE.len()],
            sum_nanos: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Record one observation. `le` is inclusive (the Prometheus contract): the first bound with
    /// `nanos <= bound` wins — a linear scan over 22 consts, off the match hot path entirely.
    /// An observation beyond the last bound (> 30 s) lands in no finite bucket; it is counted by
    /// `count`/`sum` only and so surfaces in the rendered `+Inf` bucket.
    pub(crate) fn observe(&self, d: Duration) {
        let nanos = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        if let Some(i) = LATENCY_LE.iter().position(|&(bound, _)| nanos <= bound) {
            self.buckets[i].fetch_add(1, Ordering::Relaxed);
        }
        self.sum_nanos.fetch_add(nanos, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// A point-in-time copy for rendering (relaxed reads; see the type docs for why the renderer
    /// clamps rather than relying on read ordering).
    pub(crate) fn snapshot(&self) -> LatencySnapshot {
        let mut buckets = [0u64; LATENCY_LE.len()];
        for (out, b) in buckets.iter_mut().zip(&self.buckets) {
            *out = b.load(Ordering::Relaxed);
        }
        LatencySnapshot {
            buckets,
            sum_nanos: self.sum_nanos.load(Ordering::Relaxed),
            count: self.count.load(Ordering::Relaxed),
        }
    }
}

/// A rendered-side copy of one histogram: non-cumulative finite buckets + sum + count.
#[derive(Clone, Copy)]
pub(crate) struct LatencySnapshot {
    pub(crate) buckets: [u64; LATENCY_LE.len()],
    pub(crate) sum_nanos: u64,
    pub(crate) count: u64,
}

impl LatencySnapshot {
    /// The value rendered for `le="+Inf"` and `_count`: `max(count, Σ buckets)`. Under relaxed
    /// concurrent updates a torn read could otherwise render `+Inf` below the last finite
    /// cumulative bucket — a malformed histogram `histogram_quantile()` mishandles. Clamping
    /// keeps every scrape well-formed; the one-observation skew self-corrects next scrape.
    pub(crate) fn total(&self) -> u64 {
        self.count.max(self.buckets.iter().sum())
    }

    /// An all-zero snapshot — test support for hand-built render inputs (production snapshots
    /// come from [`LatencyHistogram::snapshot`]; a fresh histogram renders all-zero naturally).
    #[cfg(test)]
    pub(crate) fn zero() -> Self {
        LatencySnapshot {
            buckets: [0; LATENCY_LE.len()],
            sum_nanos: 0,
            count: 0,
        }
    }
}

/// One slot's per-RPC histograms — the field a `ShardSlot` carries (ADR-100). Living on the SLOT
/// (not the swappable `ServerState`) means an in-place `recover_from` state swap keeps the
/// series continuous; the totals reset only when the slot itself is replaced (adopt-on-empty /
/// `AddShard`) or the process restarts — both ordinary Prometheus counter resets.
pub(crate) struct SlotLatency {
    per_rpc: [LatencyHistogram; SHARD_RPC_LABELS.len()],
}

impl SlotLatency {
    pub(crate) fn new() -> Self {
        Self {
            per_rpc: std::array::from_fn(|_| LatencyHistogram::new()),
        }
    }

    pub(crate) fn observe(&self, rpc: ShardRpc, d: Duration) {
        self.per_rpc[rpc as usize].observe(d);
    }

    pub(crate) fn snapshot(&self) -> [LatencySnapshot; SHARD_RPC_LABELS.len()] {
        std::array::from_fn(|index| self.per_rpc[index].snapshot())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn le_is_inclusive_and_ladder_is_sorted() {
        // The table itself must be strictly increasing (the renderer's cumulation and the
        // observe scan both assume it).
        assert!(LATENCY_LE.windows(2).all(|w| w[0].0 < w[1].0));

        let h = LatencyHistogram::new();
        // Exactly at a bound lands IN that bucket (le is inclusive)…
        h.observe(Duration::from_nanos(2_500));
        // …one nano over lands in the next…
        h.observe(Duration::from_nanos(2_501));
        // …and beyond the last bound lands in NO finite bucket (only count/sum).
        h.observe(Duration::from_secs(31));
        let s = h.snapshot();
        assert_eq!(s.buckets[0], 1, "at-bound observation is le-inclusive");
        assert_eq!(s.buckets[1], 1, "one-over lands in the next bucket");
        assert_eq!(s.buckets.iter().sum::<u64>(), 2, "overflow is bucketless");
        assert_eq!(s.count, 3);
        assert_eq!(
            s.total(),
            3,
            "+Inf renders from count (covers the overflow)"
        );
        assert_eq!(
            s.sum_nanos,
            2_500 + 2_501 + 31_000_000_000,
            "sum accumulates raw nanos"
        );
    }

    #[test]
    fn total_clamps_a_torn_read() {
        // Simulate the torn scrape: a bucket increment visible, its count increment not.
        let mut s = LatencySnapshot::zero();
        s.buckets[3] = 5;
        s.count = 4;
        assert_eq!(s.total(), 5, "+Inf must never render below a finite bucket");
    }

    #[test]
    fn per_rpc_histograms_are_independent() {
        let sl = SlotLatency::new();
        sl.observe(ShardRpc::Percolate, Duration::from_micros(3));
        sl.observe(ShardRpc::Percolate, Duration::from_micros(3));
        sl.observe(ShardRpc::Ingest, Duration::from_millis(2));
        let [p, pr, top_k, fetch, i, batch] = sl.snapshot();
        assert_eq!(p.count, 2);
        assert_eq!(pr.count, 0);
        assert_eq!(top_k.count, 0);
        assert_eq!(fetch.count, 0);
        assert_eq!(i.count, 1);
        assert_eq!(batch.count, 0);
    }
}
