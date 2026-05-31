//! `Shard` — the local↔remote seam — and `LocalShard`, its in-process implementation.
//!
//! [`Shard`] abstracts the OPERATION a coordinator performs on a shard, never the
//! shard's internal data: a remote shard has no in-process [`EngineSnapshot`], so the
//! trait exposes [`Shard::percolate`] (the matched ids + stats for one title) rather
//! than handing back a snapshot. [`LocalShard`] is the in-process impl — an owned
//! [`Engine`] (writes serialized behind a `std::sync::Mutex`) plus an
//! `ArcSwap<EngineSnapshot>` for lock-free reads, exactly the per-engine pattern the
//! HTTP server uses. It does NOT re-implement matching; `percolate` delegates to
//! [`EngineSnapshot::match_title`]. Every `LocalShard` is constructed with
//! [`Engine::with_shared`] over the coordinator's frozen normalizer + dict, and all
//! writes go through the read-only `*_extracted` paths so the shared `Arc<Dict>` is
//! never forked.
//!
//! The remote implementation (`RemoteShard`, behind the `distributed` feature) lives
//! in `super::remote` and satisfies the same trait by issuing gRPC calls.

use std::sync::{Arc, Mutex, PoisonError};

use arc_swap::ArcSwap;

use crate::compile::Extracted;
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::normalize::Normalizer;
use crate::segment::{Engine, EngineSnapshot, IngestReport, MatchScratch, MatchStats};

/// One shard, local or remote — the seam that lets a coordinator hold a mix of
/// in-process and (eventually) networked shards behind one type.
///
/// Abstracts the OPERATION, not the data: there is deliberately no `snapshot()`,
/// because a remote shard has no local [`EngineSnapshot`]. [`Shard::percolate`] IS
/// the per-shard probe (matched logical ids + [`MatchStats`]); `include_broad` is the
/// ALREADY-RESOLVED per-shard toggle — the coordinator applies the "broad lane only
/// on shard 0" rule before calling, and the shard never re-derives it.
///
/// `Send + Sync` is a supertrait because the coordinator fans probes out across rayon
/// worker threads, which borrow `&dyn Shard`. Object-safety and the `Send + Sync`
/// bound are enforced for free by `ClusterEngine.shards: Vec<Box<dyn Shard>>` plus the
/// `assert_send_sync::<ClusterEngine>()` guard in `lib.rs`.
pub(crate) trait Shard: Send + Sync {
    // ---- reads ----
    /// Probe this shard for one title; returns matched logical ids + match stats.
    fn percolate(&self, title: &str, include_broad: bool) -> (Vec<u64>, MatchStats);
    /// Physical query count held by this shard (a replicated/any-of query is counted
    /// once per local entry, so it is counted on each shard holding it).
    fn num_queries(&self) -> usize;
    /// Per-class entry tally `[A, B, C, D]` for this shard (introspection/tests).
    fn class_counts(&self) -> [u64; 4];

    // ---- writes ----
    /// Bulk-ingest a pre-extracted bucket into a new immutable base segment.
    fn ingest_extracted(&self, items: &[(u64, Extracted, String, u32)]) -> IngestReport;
    /// Insert one pre-extracted query into the memtable (live add).
    fn insert_extracted(
        &self,
        ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
    ) -> Option<u32>;
    /// Tombstone every live entry for `logical` (idempotent; a cheap no-op on a shard
    /// that doesn't hold it).
    fn delete_by_logical_id(&self, logical: u64) -> usize;
    /// Seal the memtable into an immutable base segment.
    fn flush(&self);
}

/// One in-process shard: owned engine for writes + lock-free snapshot for reads.
pub(crate) struct LocalShard {
    engine: Mutex<Engine>,
    snapshot: ArcSwap<EngineSnapshot>,
}

impl LocalShard {
    /// Build a shard sharing the coordinator's frozen normalizer + dict.
    pub(crate) fn new(norm: Arc<Normalizer>, dict: Arc<Dict>, config: EngineConfig) -> Self {
        let engine = Engine::with_shared(norm, dict, config);
        let snapshot = ArcSwap::new(Arc::new(engine.snapshot()));
        LocalShard {
            engine: Mutex::new(engine),
            snapshot,
        }
    }

    /// Lock the engine, recovering the guard if a prior writer panicked: a poisoned
    /// shard mutex must not take down the whole cluster, and the engine state behind
    /// it is still self-consistent (writes are atomic at this layer).
    fn lock(&self) -> std::sync::MutexGuard<'_, Engine> {
        self.engine.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Republish the lock-free read snapshot after a write.
    fn publish(eng: &Engine, slot: &ArcSwap<EngineSnapshot>) {
        slot.store(Arc::new(eng.snapshot()));
    }

    /// The current lock-free read snapshot (an `Arc` clone — no engine lock). Private:
    /// the seam exposes `percolate`, not the snapshot, so a remote shard need not have
    /// one.
    fn snapshot(&self) -> Arc<EngineSnapshot> {
        self.snapshot.load_full()
    }
}

impl Shard for LocalShard {
    /// Verbatim the body of the coordinator's old `query_shard`: allocate scratch,
    /// match one title against the lock-free snapshot, return ids + stats.
    fn percolate(&self, title: &str, include_broad: bool) -> (Vec<u64>, MatchStats) {
        let mut scratch = MatchScratch::new();
        let mut out = Vec::new();
        let stats = self
            .snapshot()
            .match_title(title, &mut scratch, &mut out, include_broad);
        (out, stats)
    }

    fn num_queries(&self) -> usize {
        self.snapshot.load().num_queries()
    }

    fn class_counts(&self) -> [u64; 4] {
        self.snapshot.load().class_counts()
    }

    fn ingest_extracted(&self, items: &[(u64, Extracted, String, u32)]) -> IngestReport {
        let mut eng = self.lock();
        let report = eng.ingest_extracted(items);
        Self::publish(&eng, &self.snapshot);
        report
    }

    fn insert_extracted(
        &self,
        ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
    ) -> Option<u32> {
        let mut eng = self.lock();
        let out = eng.insert_extracted(ex, logical, version, text);
        Self::publish(&eng, &self.snapshot);
        out
    }

    fn delete_by_logical_id(&self, logical: u64) -> usize {
        let mut eng = self.lock();
        // In-memory shards have no WAL, so the delete never errors; `0` on the
        // impossible error rather than panicking.
        let n = eng.delete_by_logical_id(logical).unwrap_or(0);
        Self::publish(&eng, &self.snapshot);
        n
    }

    fn flush(&self) {
        let mut eng = self.lock();
        eng.flush();
        Self::publish(&eng, &self.snapshot);
    }
}
