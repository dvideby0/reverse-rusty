//! Fixed-cardinality ADR-110 shard delivery counters.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RankDeliverySnapshot {
    pub top_k_hits: u64,
    pub top_k_result_bytes: u64,
    pub fetch_source_bytes: u64,
    pub total_eq: u64,
    pub total_gte: u64,
    pub cancellations: u64,
    pub cap_rejections: u64,
}

pub(crate) struct SlotRankDelivery {
    top_k_hits: AtomicU64,
    top_k_result_bytes: AtomicU64,
    fetch_source_bytes: AtomicU64,
    total_eq: AtomicU64,
    total_gte: AtomicU64,
    cancellations: AtomicU64,
    cap_rejections: AtomicU64,
}

impl SlotRankDelivery {
    pub(crate) fn new() -> Self {
        Self {
            top_k_hits: AtomicU64::new(0),
            top_k_result_bytes: AtomicU64::new(0),
            fetch_source_bytes: AtomicU64::new(0),
            total_eq: AtomicU64::new(0),
            total_gte: AtomicU64::new(0),
            cancellations: AtomicU64::new(0),
            cap_rejections: AtomicU64::new(0),
        }
    }

    pub(crate) fn record_top_k(&self, hits: usize, bytes: usize, exact: bool) {
        self.top_k_hits.fetch_add(hits as u64, Ordering::Relaxed);
        self.top_k_result_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
        if exact {
            self.total_eq.fetch_add(1, Ordering::Relaxed);
        } else {
            self.total_gte.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Result bytes not attributable to one title's rows (the ADR-112 batch
    /// summary frame) — keeps the byte counter equal to the exact encoded
    /// bytes returned, matching the coordinator's every-frame sum.
    pub(crate) fn record_result_bytes(&self, bytes: usize) {
        self.top_k_result_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_fetch(&self, bytes: usize) {
        self.fetch_source_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_cancellation(&self) {
        self.cancellations.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_cap_rejection(&self) {
        self.cap_rejections.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> RankDeliverySnapshot {
        RankDeliverySnapshot {
            top_k_hits: self.top_k_hits.load(Ordering::Relaxed),
            top_k_result_bytes: self.top_k_result_bytes.load(Ordering::Relaxed),
            fetch_source_bytes: self.fetch_source_bytes.load(Ordering::Relaxed),
            total_eq: self.total_eq.load(Ordering::Relaxed),
            total_gte: self.total_gte.load(Ordering::Relaxed),
            cancellations: self.cancellations.load(Ordering::Relaxed),
            cap_rejections: self.cap_rejections.load(Ordering::Relaxed),
        }
    }
}
