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
//! Every operation returns [`Result<_, ShardError>`]: a `LocalShard` is infallible
//! (it always returns `Ok`), but a remote shard can fail on the wire. Surfacing that
//! as an error — rather than swallowing it into an empty result — is load-bearing for
//! the zero-false-negative contract: a dropped shard probe must fail the percolate,
//! not silently shrink the answer. The remote implementation (`RemoteShard`, behind
//! the `distributed` feature) lives in `super::remote` and satisfies the same trait
//! by issuing gRPC calls.

use std::sync::{Arc, Mutex, PoisonError};

use arc_swap::ArcSwap;

use crate::compile::Extracted;
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::normalize::Normalizer;
use crate::segment::{Engine, EngineSnapshot, IngestReport, MatchScratch, MatchStats};

/// An error from cluster construction or a shard operation. In-process
/// ([`LocalShard`]) *operations* are infallible and never produce this; a `RemoteShard`
/// produces [`ShardError::Remote`] on gRPC transport or status failure, and
/// [`ShardError::DictMismatch`] when a server's frozen dict diverges from the
/// coordinator's (the connect-time fingerprint handshake). Cluster *construction* (the
/// `ClusterEngine` builders and `HashRing::new`) produces [`ShardError::Config`] on an
/// invalid configuration. Kept transport-agnostic (a `String` detail, not a
/// `tonic::Status`) so it lives in the always-compiled core alongside the trait, rather
/// than dragging the gated networking stack into the lean build.
#[derive(Debug, Clone)]
pub enum ShardError {
    /// A remote shard was unreachable or returned an error status (detail included).
    Remote(String),
    /// Invalid cluster configuration / construction precondition — e.g. zero shards, or
    /// a shard/endpoint count that disagrees with the ring. Replaces the old
    /// construction-time `assert!`s so library code never panics on bad input.
    Config(String),
    /// A remote shard's frozen-dict fingerprint disagreed with the coordinator's at
    /// connect time. The cross-process shared-dict invariant is broken, so matching
    /// against that shard would *silently* drop results — fail loud instead. This is the
    /// one false-negative path the otherwise-fallible seam cannot catch (ADR-029).
    DictMismatch { expected: u64, actual: u64 },
    /// A cluster mutation could not be durably logged (the coordinator's externalized
    /// `ClusterLog`, ADR-031). The mutation is *rejected*, not applied — surfacing it
    /// rather than acknowledging an unlogged write is load-bearing for the
    /// rebuild-from-log contract (an un-logged add/remove would silently vanish on
    /// reopen). Parallels the engine's WAL-first write path (ADR-013).
    Log(String),
}

impl std::fmt::Display for ShardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShardError::Remote(m) => write!(f, "remote shard error: {m}"),
            ShardError::Config(m) => write!(f, "cluster config error: {m}"),
            ShardError::DictMismatch { expected, actual } => write!(
                f,
                "dict fingerprint mismatch: coordinator {expected:#018x} != shard \
                 {actual:#018x} (every shard must share the coordinator's frozen dict)"
            ),
            ShardError::Log(m) => write!(f, "cluster log durability error: {m}"),
        }
    }
}

impl std::error::Error for ShardError {}

/// Sink for shard-level observability events (e.g. a [`ReplicatedShard`] replica
/// dropping out of its in-sync set). The `Arc` analogue of the engine's event
/// observer; the coordinator fans its observer in via `ClusterEngine::set_observer`.
///
/// [`ReplicatedShard`]: super::replica::ReplicatedShard
pub(crate) type EventSink = Arc<dyn Fn(&crate::events::EngineEvent) + Send + Sync>;

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
    fn percolate(
        &self,
        title: &str,
        include_broad: bool,
    ) -> Result<(Vec<u64>, MatchStats), ShardError>;
    /// Physical query count held by this shard (a replicated/any-of query is counted
    /// once per local entry, so it is counted on each shard holding it).
    fn num_queries(&self) -> Result<usize, ShardError>;
    /// Per-class entry tally `[A, B, C, D]` for this shard (introspection/tests).
    fn class_counts(&self) -> Result<[u64; 4], ShardError>;

    // ---- writes ----
    /// Bulk-ingest a pre-extracted bucket into a new immutable base segment — the
    /// distributed load path ([`crate::cluster::ClusterEngine::ingest`]). NOTE:
    /// `ClusterEngine::build` does NOT use this; it ingests via the infallible inherent
    /// [`LocalShard::ingest_local`] so that constructing an in-process cluster stays
    /// infallible. This seam method is what lets the coordinator load a *remote* shard.
    fn ingest_extracted(
        &self,
        items: &[(u64, Extracted, String, u32)],
    ) -> Result<IngestReport, ShardError>;
    /// Insert one pre-extracted query into the memtable (live add).
    fn insert_extracted(
        &self,
        ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
    ) -> Result<Option<u32>, ShardError>;
    /// Tombstone every live entry for `logical` (idempotent; a cheap no-op on a shard
    /// that doesn't hold it).
    fn delete_by_logical_id(&self, logical: u64) -> Result<usize, ShardError>;
    /// Seal the memtable into an immutable base segment.
    fn flush(&self) -> Result<(), ShardError>;

    // ---- durable checkpoint (ADR-032; local shards only) ----
    /// Seal for a cluster checkpoint: flush the memtable AND re-seal any tombstoned base
    /// segment, so the ON-DISK segment set reflects every applied delete. Without the
    /// re-seal a `Remove` against a base segment lives only in the in-RAM alive overlay
    /// and would resurrect the query on reopen once its log entry is truncated.
    fn seal_for_checkpoint(&self) -> Result<(), ShardError>;
    /// This shard's live (mmap'd) base-segment filenames — the registry the coordinator
    /// commits into `cluster_manifest.bin`. `Err` (never a silent empty list) if any
    /// segment is in-memory (a write fell back), which would otherwise lose data on reopen.
    fn segment_filenames(&self) -> Result<Vec<String>, ShardError>;
    /// This shard's next segment-id counter — committed per shard so a flush after reopen
    /// never reuses a committed segment filename.
    fn next_seg_id(&self) -> Result<u64, ShardError>;

    // ---- observability (ADR-035) ----
    /// Install an event sink so this shard can surface degraded-redundancy events — e.g. a
    /// [`ReplicatedShard`](super::replica::ReplicatedShard) replica falling out of its
    /// in-sync set. Default: a no-op (a plain [`LocalShard`]/`RemoteShard` emits nothing
    /// here). The coordinator fans its observer in via `ClusterEngine::set_observer`.
    fn set_event_sink(&self, _sink: EventSink) {}
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

    /// Build a DURABLE shard (ADR-032): an engine that persists compiled segments under
    /// `config.data_dir` (the shard's directory) with no WAL and no own manifest — the
    /// coordinator's `ClusterLog` is the tail and its `cluster_manifest.bin` is the
    /// registry. Shares the coordinator's frozen normalizer + dict.
    pub(crate) fn new_durable(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        config: EngineConfig,
    ) -> Result<Self, ShardError> {
        let engine = Engine::with_shared_segments_only(norm, dict, config)
            .map_err(|e| ShardError::Log(format!("creating durable shard: {e}")))?;
        let snapshot = ArcSwap::new(Arc::new(engine.snapshot()));
        Ok(LocalShard {
            engine: Mutex::new(engine),
            snapshot,
        })
    }

    /// Reopen a durable shard by attaching its committed segment files (ADR-032) against
    /// the shared dict — attach-and-mmap, not re-ingest. `files`/`next_seg_id` come from
    /// the coordinator's `cluster_manifest.bin`; `config.data_dir` is the shard's dir.
    pub(crate) fn open_segments(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        config: EngineConfig,
        files: &[String],
        next_seg_id: u64,
    ) -> Result<Self, ShardError> {
        let engine = Engine::open_shared_segments(norm, dict, config, files, next_seg_id)
            .map_err(|e| ShardError::Log(format!("attaching shard segments: {e}")))?;
        let snapshot = ArcSwap::new(Arc::new(engine.snapshot()));
        Ok(LocalShard {
            engine: Mutex::new(engine),
            snapshot,
        })
    }

    /// Bulk-ingest, infallibly — the build path uses this directly on a concrete
    /// `LocalShard` (before boxing) so `ClusterEngine::build` stays infallible. The
    /// trait's `ingest_extracted` is the `Result`-wrapped view of the same work.
    pub(crate) fn ingest_local(&self, items: &[(u64, Extracted, String, u32)]) -> IngestReport {
        let mut eng = self.lock();
        let report = eng.ingest_extracted(items);
        Self::publish(&eng, &self.snapshot);
        report
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
    /// match one title against the lock-free snapshot, return ids + stats. Infallible
    /// — wrapped in `Ok` to satisfy the (remote-capable) trait.
    fn percolate(
        &self,
        title: &str,
        include_broad: bool,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        let mut scratch = MatchScratch::new();
        let mut out = Vec::new();
        let stats = self
            .snapshot()
            .match_title(title, &mut scratch, &mut out, include_broad);
        Ok((out, stats))
    }

    fn num_queries(&self) -> Result<usize, ShardError> {
        Ok(self.snapshot.load().num_queries())
    }

    fn class_counts(&self) -> Result<[u64; 4], ShardError> {
        Ok(self.snapshot.load().class_counts())
    }

    fn ingest_extracted(
        &self,
        items: &[(u64, Extracted, String, u32)],
    ) -> Result<IngestReport, ShardError> {
        Ok(self.ingest_local(items))
    }

    fn insert_extracted(
        &self,
        ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
    ) -> Result<Option<u32>, ShardError> {
        let mut eng = self.lock();
        let out = eng.insert_extracted(ex, logical, version, text);
        Self::publish(&eng, &self.snapshot);
        Ok(out)
    }

    fn delete_by_logical_id(&self, logical: u64) -> Result<usize, ShardError> {
        let mut eng = self.lock();
        // In-memory shards have no WAL, so the delete never errors; `0` on the
        // impossible error rather than panicking.
        let n = eng.delete_by_logical_id(logical).unwrap_or(0);
        Self::publish(&eng, &self.snapshot);
        Ok(n)
    }

    fn flush(&self) -> Result<(), ShardError> {
        let mut eng = self.lock();
        eng.flush();
        Self::publish(&eng, &self.snapshot);
        Ok(())
    }

    fn seal_for_checkpoint(&self) -> Result<(), ShardError> {
        let mut eng = self.lock();
        eng.flush(); // seal the memtable into a base segment
        eng.reseal_tombstoned_segments(); // bake base-segment tombstones onto disk
        Self::publish(&eng, &self.snapshot);
        Ok(())
    }

    fn segment_filenames(&self) -> Result<Vec<String>, ShardError> {
        self.lock()
            .segment_filenames()
            .map_err(|e| ShardError::Log(format!("collecting shard segment filenames: {e}")))
    }

    fn next_seg_id(&self) -> Result<u64, ShardError> {
        Ok(self.lock().next_seg_id())
    }
}
