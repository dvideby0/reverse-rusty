//! Per-RPC transport metrics for the cluster gRPC client (ADR-085).
//!
//! A lean, std-only (atomic) collector the coordinator shares (`Arc`) into every
//! [`RemoteShard`](super::remote::RemoteShard); each RPC records its outcome + latency
//! through `RemoteShard`'s unified `call` seam. The in-process / RF=1 path never builds a
//! `RemoteShard`, so its metrics stay all-zero and the default behavior is byte-identical.
//!
//! Exposed point-in-time via [`TransportMetrics::snapshot`] — the pull-on-scrape pattern of
//! [`EngineMetrics`](crate::events::EngineMetrics) — which the coordinator-mode server
//! bridges to Prometheus. The collector + snapshot are lean (always compiled, since cluster
//! mode is not feature-gated); the *writer* side (`RpcMethod`/`RpcOutcome`/`record`) is only
//! reachable from the `distributed` gRPC client, so it is gated to avoid lean-build dead code.

use std::sync::atomic::{AtomicU64, Ordering};

/// Stable snake_case labels, one per counter slot, in slot order. The writer-side
/// [`RpcMethod`] enum's discriminants index these, and [`TransportMetrics::snapshot`] reads
/// them — so both sides share one ordering and cannot drift.
const METHOD_LABELS: [&str; TransportMetrics::SLOTS] = [
    "percolate",
    "percolate_ranked",
    "percolate_top_k",
    "fetch_matches",
    "num_queries",
    "class_counts",
    "ingest",
    "insert",
    "delete",
    "flush",
    "fence",
    "unfence",
    "retention_lease",
    "recover_from",
    "translog",
    "list_shards",
    "drop_shard",
    "content_fingerprint",
    "percolate_top_k_batch",
    "percolate_all",
];

#[derive(Default)]
struct MethodCounters {
    calls: AtomicU64,
    errors: AtomicU64,
    timeouts: AtomicU64,
    retries: AtomicU64,
    latency_nanos: AtomicU64,
}

/// Cumulative per-RPC transport counters, shared (`Arc`) from the coordinator into every
/// `RemoteShard`. All-zero on the in-process path (no remote RPC ever records).
pub struct TransportMetrics {
    per_method: [MethodCounters; Self::SLOTS],
}

impl Default for TransportMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl TransportMetrics {
    /// Number of distinct RPC kinds tracked (the counter-array length).
    pub(crate) const SLOTS: usize = 20;

    /// A fresh, all-zero collector.
    pub fn new() -> Self {
        TransportMetrics {
            per_method: std::array::from_fn(|_| MethodCounters::default()),
        }
    }

    /// Record one completed RPC (after any retries): which method, its final outcome, the
    /// total wall-clock latency, and how many retry attempts were spent. Lock-free; safe to
    /// call concurrently from every fan-out worker. Writer side — `distributed` only.
    #[cfg(feature = "distributed")]
    pub(crate) fn record(
        &self,
        method: RpcMethod,
        outcome: RpcOutcome,
        latency: std::time::Duration,
        retries: u32,
    ) {
        let c = &self.per_method[method as usize];
        c.calls.fetch_add(1, Ordering::Relaxed);
        c.latency_nanos
            .fetch_add(latency.as_nanos() as u64, Ordering::Relaxed);
        if retries > 0 {
            c.retries.fetch_add(u64::from(retries), Ordering::Relaxed);
        }
        match outcome {
            RpcOutcome::Ok => {}
            // A timeout is a failure too, so it bumps BOTH counters — `timeouts` is the
            // subset of `errors` that were deadline-exceeded.
            RpcOutcome::Timeout => {
                c.timeouts.fetch_add(1, Ordering::Relaxed);
                c.errors.fetch_add(1, Ordering::Relaxed);
            }
            RpcOutcome::Error => {
                c.errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// A point-in-time copy of every counter, for scraping / introspection.
    pub fn snapshot(&self) -> TransportMetricsSnapshot {
        let methods = self
            .per_method
            .iter()
            .enumerate()
            .map(|(i, c)| MethodStat {
                method: METHOD_LABELS[i],
                calls: c.calls.load(Ordering::Relaxed),
                errors: c.errors.load(Ordering::Relaxed),
                timeouts: c.timeouts.load(Ordering::Relaxed),
                retries: c.retries.load(Ordering::Relaxed),
                latency_nanos_total: c.latency_nanos.load(Ordering::Relaxed),
            })
            .collect();
        TransportMetricsSnapshot { methods }
    }
}

/// Point-in-time per-method transport stats (one row per RPC kind). Cheap to build and
/// `Serialize`-free, like [`EngineMetrics`](crate::events::EngineMetrics).
#[derive(Clone, Debug, Default)]
pub struct TransportMetricsSnapshot {
    pub methods: Vec<MethodStat>,
}

impl TransportMetricsSnapshot {
    /// Total RPCs issued across all methods.
    pub fn total_calls(&self) -> u64 {
        self.methods.iter().map(|m| m.calls).sum()
    }
    /// Total failed RPCs (includes timeouts).
    pub fn total_errors(&self) -> u64 {
        self.methods.iter().map(|m| m.errors).sum()
    }
    /// Total RPCs that exceeded their per-call deadline.
    pub fn total_timeouts(&self) -> u64 {
        self.methods.iter().map(|m| m.timeouts).sum()
    }
    /// Total retry attempts spent across all methods.
    pub fn total_retries(&self) -> u64 {
        self.methods.iter().map(|m| m.retries).sum()
    }
}

/// One RPC kind's cumulative counters.
#[derive(Clone, Debug)]
pub struct MethodStat {
    /// Stable snake_case method label (a Prometheus label value).
    pub method: &'static str,
    /// Total calls issued.
    pub calls: u64,
    /// Total failures (includes timeouts).
    pub errors: u64,
    /// Failures that were deadline-exceeded (a subset of `errors`).
    pub timeouts: u64,
    /// Retry attempts spent (0 unless a transient idempotent-read retry fired).
    pub retries: u64,
    /// Summed wall-clock latency in nanoseconds (divide by `calls` for a mean).
    pub latency_nanos_total: u64,
}

/// The cluster gRPC RPCs we label transport metrics by. Discriminants index the
/// per-method counter array (and [`METHOD_LABELS`]), so this order is that order. Writer
/// side — only the `distributed` gRPC client references it.
#[cfg(feature = "distributed")]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum RpcMethod {
    Percolate,
    PercolateRanked,
    PercolateTopK,
    FetchMatches,
    NumQueries,
    ClassCounts,
    Ingest,
    Insert,
    Delete,
    Flush,
    Fence,
    Unfence,
    RetentionLease,
    RecoverFrom,
    Translog,
    ListShards,
    DropShard,
    ContentFingerprint,
    PercolateTopKBatch,
    PercolateAll,
}

#[cfg(feature = "distributed")]
impl RpcMethod {
    /// This method's stable label — the same string the snapshot's per-method row uses.
    pub(crate) fn label(self) -> &'static str {
        METHOD_LABELS[self as usize]
    }
}

/// The final outcome of one RPC call-chain (after any retries), recorded once.
#[cfg(feature = "distributed")]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum RpcOutcome {
    Ok,
    /// The call exhausted its per-call deadline (`tokio::time::timeout` elapsed).
    Timeout,
    /// The call failed for a non-timeout reason (transport / gRPC status error).
    Error,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_has_one_row_per_method_all_zero_by_default() {
        let m = TransportMetrics::new();
        let snap = m.snapshot();
        assert_eq!(snap.methods.len(), TransportMetrics::SLOTS);
        assert_eq!(snap.total_calls(), 0);
        assert_eq!(snap.total_errors(), 0);
        // Labels are present and stable.
        assert_eq!(snap.methods[0].method, "percolate");
        assert!(snap.methods.iter().any(|r| r.method == "recover_from"));
    }

    #[cfg(feature = "distributed")]
    #[test]
    fn record_accumulates_per_method_and_classifies_outcomes() {
        use std::time::Duration;
        let m = TransportMetrics::new();
        m.record(
            RpcMethod::Percolate,
            RpcOutcome::Ok,
            Duration::from_millis(2),
            0,
        );
        m.record(
            RpcMethod::Percolate,
            RpcOutcome::Timeout,
            Duration::from_secs(10),
            2,
        );
        m.record(
            RpcMethod::Ingest,
            RpcOutcome::Error,
            Duration::from_millis(5),
            0,
        );
        let snap = m.snapshot();
        let perc = snap
            .methods
            .iter()
            .find(|r| r.method == "percolate")
            .unwrap();
        assert_eq!(perc.calls, 2);
        assert_eq!(perc.errors, 1, "timeout counts as an error");
        assert_eq!(perc.timeouts, 1);
        assert_eq!(perc.retries, 2);
        let ingest = snap.methods.iter().find(|r| r.method == "ingest").unwrap();
        assert_eq!(ingest.calls, 1);
        assert_eq!(ingest.errors, 1);
        assert_eq!(ingest.timeouts, 0);
        assert_eq!(snap.total_calls(), 3);
        assert_eq!(snap.total_timeouts(), 1);
    }
}
