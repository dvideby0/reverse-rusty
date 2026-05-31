//! `Shard` — one in-process shard.
//!
//! Wraps an owned [`Engine`] (writes serialized behind a `std::sync::Mutex`) plus
//! an `ArcSwap<EngineSnapshot>` for lock-free reads — exactly the per-engine
//! pattern the HTTP server uses. It does NOT re-implement matching; reads delegate
//! to [`EngineSnapshot::match_title`]. Every shard is constructed with
//! [`Engine::with_shared`] over the coordinator's frozen normalizer + dict, and
//! all writes go through the read-only `*_extracted` paths so the shared
//! `Arc<Dict>` is never forked.

use std::sync::{Arc, Mutex, PoisonError};

use arc_swap::ArcSwap;

use crate::compile::Extracted;
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::normalize::Normalizer;
use crate::segment::{Engine, EngineSnapshot, IngestReport};

/// One shard: owned engine for writes + lock-free snapshot for reads.
pub(crate) struct Shard {
    engine: Mutex<Engine>,
    snapshot: ArcSwap<EngineSnapshot>,
}

impl Shard {
    /// Build a shard sharing the coordinator's frozen normalizer + dict.
    pub(crate) fn new(norm: Arc<Normalizer>, dict: Arc<Dict>, config: EngineConfig) -> Self {
        let engine = Engine::with_shared(norm, dict, config);
        let snapshot = ArcSwap::new(Arc::new(engine.snapshot()));
        Shard {
            engine: Mutex::new(engine),
            snapshot,
        }
    }

    /// Lock the engine, recovering the guard if a prior writer panicked: a
    /// poisoned shard mutex must not take down the whole cluster, and the engine
    /// state behind it is still self-consistent (writes are atomic at this layer).
    fn lock(&self) -> std::sync::MutexGuard<'_, Engine> {
        self.engine.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Republish the lock-free read snapshot after a write.
    fn publish(eng: &Engine, slot: &ArcSwap<EngineSnapshot>) {
        slot.store(Arc::new(eng.snapshot()));
    }

    /// Bulk-ingest a pre-extracted bucket into a new immutable base segment.
    pub(crate) fn ingest_extracted(&self, items: &[(u64, Extracted, String, u32)]) -> IngestReport {
        let mut eng = self.lock();
        let report = eng.ingest_extracted(items);
        Self::publish(&eng, &self.snapshot);
        report
    }

    /// Insert one pre-extracted query into the memtable (live add).
    pub(crate) fn insert_extracted(
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

    /// Tombstone every live entry for `logical` (idempotent; a cheap no-op on a
    /// shard that doesn't hold it). In-memory shards have no WAL, so the delete
    /// never errors; `0` is returned on the impossible error rather than panicking.
    pub(crate) fn delete_by_logical_id(&self, logical: u64) -> usize {
        let mut eng = self.lock();
        let n = eng.delete_by_logical_id(logical).unwrap_or(0);
        Self::publish(&eng, &self.snapshot);
        n
    }

    /// Seal the memtable into an immutable base segment.
    pub(crate) fn flush(&self) {
        let mut eng = self.lock();
        eng.flush();
        Self::publish(&eng, &self.snapshot);
    }

    /// The current lock-free read snapshot (an `Arc` clone — no engine lock).
    pub(crate) fn snapshot(&self) -> Arc<EngineSnapshot> {
        self.snapshot.load_full()
    }

    /// Physical query count in this shard (counts a logical id once per local
    /// entry, so a replicated/any-of query is counted on each shard holding it).
    pub(crate) fn num_queries(&self) -> usize {
        self.snapshot.load().num_queries()
    }

    /// Per-class entry tally `[A, B, C, D]` for this shard (introspection/tests).
    pub(crate) fn class_counts(&self) -> [u64; 4] {
        self.snapshot.load().class_counts()
    }
}
