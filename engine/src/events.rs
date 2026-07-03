//! Engine events and metrics — zero-dependency observability for Reverse Rusty.
//!
//! Design philosophy: the engine emits structured events through an optional
//! callback (no logging crate dependency). Callers wire these into whatever
//! observability stack they use — `tracing`, `log`, Prometheus push, or a
//! simple `Vec<EngineEvent>` in tests.
//!
//! The [`EngineMetrics`] struct is a point-in-time snapshot of engine state,
//! suitable for periodic scraping or dashboard display.

use crate::segment::CompactionReport;

/// Which durability-related operation failed. Carried in
/// [`EngineEvent::DurabilityFailure`] so an observer can label metrics and route
/// alerts by failure kind without string-matching a message.
///
/// **Severity.** Use [`is_data_at_risk`](DurabilityOp::is_data_at_risk) to
/// distinguish failures that mean *match data* may be lost or was never durably
/// committed (page these) from failures that only affect display-only source
/// text or are benign recovery housekeeping (log these). The server maps the
/// former to `error!` + a critical alert and the latter to `warn!`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityOp {
    /// Initializing the data directory or opening the WAL at startup failed; the
    /// engine is running **without durability** (writes will not survive a
    /// restart). Data at risk.
    WalInit,
    /// Appending a mutation (insert/tombstone) to the WAL failed; the mutation
    /// was *rejected* (not applied), so in-memory state stays consistent with the
    /// log — but the caller's write did not land. Data at risk.
    WalAppend,
    /// Writing the post-flush WAL checkpoint marker failed. Benign: the next
    /// recovery simply replays from an earlier point.
    WalCheckpoint,
    /// Truncating/resetting the WAL after a successful checkpoint failed. Benign:
    /// the WAL keeps already-checkpointed entries that the next recovery re-applies
    /// idempotently.
    WalReset,
    /// Writing a segment file to disk failed; the engine fell back to an
    /// in-memory segment (`build_*`/`bulk_ingest` instead roll the batch back).
    /// Data at risk.
    SegmentWrite,
    /// Mmapping a freshly written segment file failed; fell back to in-memory.
    /// Data at risk.
    SegmentMmap,
    /// A segment file referenced by the manifest was corrupt/unreadable and was
    /// skipped during recovery (`Engine::open`). Data at risk: those queries are
    /// gone until reingested.
    SegmentRecovery,
    /// Writing the manifest — the atomic commit point — failed. Data at risk: the
    /// batch is rolled back.
    ManifestWrite,
    /// Persisting query source text (`sources.dat`) failed. Display-only:
    /// `_source`/explain may be stale, but match data is unaffected.
    SourceStoreWrite,
    /// Re-mapping the freshly written source store failed (lazy mode). Display-only.
    SourceStoreRemap,
    /// Loading persisted query sources at startup failed (`Engine::open`).
    /// Display-only: `_source` is unavailable for recovered queries.
    SourceStoreLoad,
    /// The WAL tail was corrupt/torn and trailing bytes were skipped during
    /// recovery. Informational: the torn tail was never acknowledged as durable.
    WalTornTail,
    /// An ingest batch could not be durably committed and was rolled back
    /// entirely (nothing committed). Data at risk: the caller's batch did not land.
    IngestRollback,
    /// A flush, compaction, or reseal could not durably persist its new segment and
    /// was failed closed: the WAL was left intact (flush) or the merge was rolled
    /// back to its source segments (compaction/reseal), so nothing was lost. NOT
    /// data at risk — the pre-operation state is fully recoverable — but durability
    /// is degraded and the operation did not advance on disk. Distinguished from
    /// [`SegmentWrite`](Self::SegmentWrite) (a write that fell back to in-memory,
    /// leaving the data durable *only* in RAM) precisely because this path keeps a
    /// durable copy. See ADR-051.
    Compaction,
    /// A replica in a [`ReplicatedShard`](crate::cluster) group could not apply a
    /// replicated write (or failed a read probe) and was dropped from its in-sync
    /// set. NOT data at risk: the primary still holds the data and the op succeeded;
    /// the replica is flagged for peer re-recovery (clustering build-path step 4).
    /// Redundancy/availability is reduced until it recovers.
    ReplicaDesync,
    /// A cluster multi-shard mutation (a selective Add or a Remove) applied to SOME but not
    /// all of its target shards: a remote shard write failed mid-fan-out (ADR-047). The
    /// mutation is durably logged (so it WILL converge on `ClusterEngine::resync` or reopen)
    /// and the failed shards are queued for repair — but until then the query is only
    /// partially visible: a transient FALSE-NEGATIVE window on the un-applied shards. Data at
    /// risk (a missed match is this system's worst outcome). Distributed layer only; the
    /// in-process / RF=1 path never produces it (its `LocalShard` writes are infallible).
    ClusterPartialApply,
}

impl DurabilityOp {
    /// Stable snake_case identifier, suitable as a metric label value or a
    /// structured-log field. Kept in lockstep with the variant set.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            DurabilityOp::WalInit => "wal_init",
            DurabilityOp::WalAppend => "wal_append",
            DurabilityOp::WalCheckpoint => "wal_checkpoint",
            DurabilityOp::WalReset => "wal_reset",
            DurabilityOp::SegmentWrite => "segment_write",
            DurabilityOp::SegmentMmap => "segment_mmap",
            DurabilityOp::SegmentRecovery => "segment_recovery",
            DurabilityOp::ManifestWrite => "manifest_write",
            DurabilityOp::SourceStoreWrite => "source_store_write",
            DurabilityOp::SourceStoreRemap => "source_store_remap",
            DurabilityOp::SourceStoreLoad => "source_store_load",
            DurabilityOp::WalTornTail => "wal_torn_tail",
            DurabilityOp::IngestRollback => "ingest_rollback",
            DurabilityOp::Compaction => "compaction",
            DurabilityOp::ReplicaDesync => "replica_desync",
            DurabilityOp::ClusterPartialApply => "cluster_partial_apply",
        }
    }

    /// True if this failure means *match data* may be lost or was never durably
    /// committed — the operator should be paged. False for display-only
    /// (`_source`) failures and benign WAL housekeeping, which only warrant a
    /// warning. See the per-variant docs for the rationale.
    #[must_use]
    pub fn is_data_at_risk(self) -> bool {
        match self {
            DurabilityOp::WalInit
            | DurabilityOp::WalAppend
            | DurabilityOp::SegmentWrite
            | DurabilityOp::SegmentMmap
            | DurabilityOp::SegmentRecovery
            | DurabilityOp::ManifestWrite
            | DurabilityOp::IngestRollback
            | DurabilityOp::ClusterPartialApply => true,
            DurabilityOp::WalCheckpoint
            | DurabilityOp::WalReset
            | DurabilityOp::SourceStoreWrite
            | DurabilityOp::SourceStoreRemap
            | DurabilityOp::SourceStoreLoad
            | DurabilityOp::WalTornTail
            | DurabilityOp::Compaction
            | DurabilityOp::ReplicaDesync => false,
        }
    }
}

/// Why a compaction was triggered. Carried in [`EngineEvent::Compaction`] so
/// callers can distinguish policy-driven merges from explicit ones.
#[derive(Debug, Clone)]
pub enum CompactionTrigger {
    /// Triggered by `compact()` because base segment count exceeded `max_segments`.
    SegmentCount { count: usize },
    /// Triggered because a segment's tombstone fraction exceeded `holes_ratio_threshold`.
    HolesRatio { segment_idx: usize, ratio: f64 },
    /// Triggered by an explicit `compact_all()` call.
    ExplicitAll,
    /// Triggered by an explicit `compact_range(lo, hi)` call.
    ExplicitRange { lo: usize, hi: usize },
}

/// Lifecycle events emitted by the engine. Register an observer via
/// [`Engine::set_observer`](crate::segment::Engine::set_observer) to receive
/// these. All events are emitted synchronously on the calling thread; the
/// observer callback should be fast (buffer or increment counters, don't do I/O).
#[derive(Debug, Clone)]
pub enum EngineEvent {
    /// The memtable was sealed into an immutable base segment.
    Flush {
        /// Number of entries in the flushed segment.
        entries: usize,
        /// Total base segment count after the flush.
        base_segments_after: usize,
        /// Wall-clock seconds spent sealing the memtable into a base segment
        /// (covers the seal + anchor-filter build, not the subsequent WAL
        /// checkpoint or manifest save).
        duration_secs: f64,
    },

    /// A batch of queries was ingested (via `build_from_queries` or `bulk_ingest`).
    Ingest {
        ingested: usize,
        rejected_parse: usize,
        rejected_class_d: usize,
        /// Total base segment count after ingest.
        base_segments_after: usize,
    },

    /// Base segments were merged (compaction completed).
    Compaction {
        report: CompactionReport,
        trigger: CompactionTrigger,
        /// Total base segment count after compaction.
        base_segments_after: usize,
        /// Wall-clock seconds spent merging the selected range of base segments.
        duration_secs: f64,
    },

    /// A best-effort removal of a segment file failed (e.g. orphan cleanup after
    /// a write error, or stale-file cleanup after compaction). The owning
    /// operation has already succeeded or reported its own error; this only
    /// signals a leaked file on disk that may warrant manual cleanup. A missing
    /// file is *not* reported here (that is the expected, benign case).
    SegmentCleanupFailed {
        /// The file the engine tried to remove.
        path: std::path::PathBuf,
        /// The OS error encountered.
        error: String,
    },

    /// A durability/persistence operation failed. Unlike [`SegmentCleanupFailed`]
    /// (a leaked file after an otherwise-successful op), this signals that
    /// durability is *degraded*: a write was lost, fell back to in-memory, was
    /// rolled back, or could not be recovered. The owning operation has already
    /// taken its consistency-preserving action (reject the write, roll the batch
    /// back, or fall back to memory) and set the relevant health flag
    /// (`wal_healthy`/`persistence_healthy`); this event exists so an observer can
    /// log the failure through the structured stack and increment an alertable
    /// counter, rather than the engine writing to stderr where ops can't see it.
    ///
    /// Recovery-time failures (raised inside `Engine::open`/`with_config`, before
    /// an observer can be attached) are buffered and delivered when
    /// [`set_observer`](crate::segment::Engine::set_observer) is called.
    DurabilityFailure {
        /// Which operation failed — a stable, matchable discriminator. Drives the
        /// metric label and the log severity (see [`DurabilityOp::is_data_at_risk`]).
        op: DurabilityOp,
        /// Human-readable context: what was being done, the path involved, and the
        /// consequence (e.g. "segment write for …, fell back to in-memory").
        detail: String,
        /// The underlying cause — an OS error string, or a description of the
        /// condition where there is no `io::Error` (e.g. "N bytes" for a torn tail).
        error: String,
    },
}

/// Point-in-time snapshot of engine state. Obtain via
/// [`Engine::metrics()`](crate::segment::Engine::metrics).
///
/// This is cheap to construct (no heap allocation beyond the `Vec`s) and
/// suitable for periodic scraping by a monitoring system.
#[derive(Debug, Clone)]
pub struct EngineMetrics {
    /// Total queries stored across all segments + memtable.
    pub total_queries: usize,
    /// Number of sealed (immutable) base segments.
    pub base_segments: usize,
    /// Entries currently in the mutable memtable.
    pub memtable_entries: usize,
    /// Entry count per base segment (for size-distribution analysis).
    pub segment_sizes: Vec<usize>,
    /// Holes ratio per base segment (tombstoned fraction).
    pub segment_holes: Vec<f64>,
    /// Cumulative queries rejected due to parse errors.
    pub rejected_parse: u64,
    /// Cumulative queries rejected as cost-class D.
    pub rejected_class_d: u64,
    /// Observe-first hot-tier telemetry (the Broad-Query Cost Program): accepted
    /// compiles since process start whose plan kept a main-lane query whose
    /// deciding anchor's frequency is already ≥ the default hot-anchor threshold
    /// ([`DEFAULT_HOT_ANCHOR_THETA`](crate::config::DEFAULT_HOT_ANCHOR_THETA)) —
    /// the queries the hot tier will reclassify once it ships. Counts compile
    /// events (incl. WAL replay / vocab recompiles), not distinct stored queries;
    /// resets on restart (rate()-friendly).
    pub would_be_hot: u64,
    /// Number of distinct features in the shared dictionary.
    pub dict_features: usize,
    /// Heap bytes used by the exact-match SoA store.
    pub exact_bytes: usize,
    /// Heap bytes used by the candidate indexes (main + broad).
    pub index_bytes: usize,
    /// Heap bytes used by per-segment anchor filters (bloom filters).
    pub filter_bytes: usize,
    /// Segments compiled against an older vocab epoch (need reingestion).
    pub stale_segments: usize,
    /// Resident heap bytes used by the shared feature dictionary. Unlike
    /// `exact_bytes`/`index_bytes` (which report 0 for mmap'd segments because
    /// that data is file-backed/paged), the four `*_bytes` fields below are
    /// resident RAM even at scale — the structures this measures are what
    /// dominate per-node memory once the SoA and index are mmap'd.
    pub dict_bytes: usize,
    /// Resident heap bytes used by the query source store (source text held for
    /// `_source`/explain).
    pub query_store_bytes: usize,
    /// Resident heap bytes used by per-segment logical→local reverse indexes.
    pub logical_index_bytes: usize,
    /// Resident heap bytes used by per-segment liveness (alive) overlays.
    pub alive_bytes: usize,
    /// Current on-disk size of the write-ahead log in bytes (0 when running
    /// without durability). Analogous to Elasticsearch's translog
    /// `size_in_bytes` — indicates recovery cost on restart.
    pub wal_size_bytes: u64,
    /// Number of un-checkpointed WAL entries (mutations since the last flush
    /// checkpoint; 0 when running without durability). Analogous to
    /// Elasticsearch's translog `operations` — shows whether checkpointing keeps
    /// up with the write rate.
    pub wal_pending_entries: u64,
}

/// Posting-length distribution of one candidate-index lane, computed on demand
/// across every segment + the memtable (the Broad-Query Cost Program's
/// observe-first telemetry — `docs/proposals/broad-cost-program.md` §5.0).
/// Percentiles are nearest-rank over the per-signature posting lengths; all
/// zeros when the lane holds no postings. Never computed on the match path.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PostingStats {
    /// Number of postings (distinct signatures) in the lane.
    pub count: usize,
    /// Median posting length.
    pub p50: u32,
    /// 95th-percentile posting length.
    pub p95: u32,
    /// 99th-percentile posting length.
    pub p99: u32,
    /// Longest posting in the lane — the fat-anchor fingerprint the hot tier
    /// targets (a large main-lane max here with a modest p99 is the top-64
    /// rank-cliff signature ADR-104 measured).
    pub max: u32,
}

/// Per-lane [`PostingStats`] (main = the always-probed realtime lane, broad =
/// the opt-in quarantine lane). Returned by `EngineSnapshot::lane_posting_stats`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LanePostingStats {
    pub main: PostingStats,
    pub broad: PostingStats,
}

/// How a segment's payload is backed. Mirrors the engine's two sealed-segment
/// representations plus the mutable memtable, so per-segment introspection can
/// tell an operator which segments are off-heap (page cache) versus resident.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentKind {
    /// Sealed, in-memory `Segment` — its SoA + indexes are resident heap.
    Memory,
    /// Sealed, file-backed `MmapSegment` — its SoA + indexes live in the page
    /// cache (off-heap); only the liveness overlay + reverse index are resident.
    Mmap,
    /// The mutable hot delta. Always in-memory; sealed into a base segment on flush.
    Memtable,
}

impl SegmentKind {
    /// Stable lowercase identifier, suitable for a JSON value or a `_cat` table
    /// cell. Kept in lockstep with the variant set.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            SegmentKind::Memory => "memory",
            SegmentKind::Mmap => "mmap",
            SegmentKind::Memtable => "memtable",
        }
    }
}

/// Per-segment introspection record — one row of the LSM layout, including the
/// mutable memtable as the final row. Powers the server's ES-style
/// `GET /_cat/segments`, which exposes the segment-level detail that the
/// aggregate [`EngineMetrics`] flattens away: which segments carry compaction
/// pressure (`holes_ratio`), how memory is distributed (resident vs off-heap),
/// and which segments are stale against the current vocab epoch.
///
/// Like [`EngineMetrics`], this is a plain data record with no serialization
/// dependency; the server builds its own `Serialize` response type from it.
#[derive(Debug, Clone)]
pub struct SegmentInfo {
    /// Position in the LSM layout: base segments are `0..base_segments` (0 is the
    /// oldest), and the memtable is reported last at ordinal `base_segments`.
    pub ordinal: usize,
    /// How this segment's payload is backed (see [`SegmentKind`]).
    pub kind: SegmentKind,
    /// Total entries, alive + tombstoned. This is the denominator compaction
    /// scores against, so it is reported even though `alive + deleted` recovers it.
    pub entries: usize,
    /// Alive (non-tombstoned) entries — the queries this segment can still match.
    pub alive: usize,
    /// Tombstoned (logically deleted) entries awaiting reclamation by compaction.
    pub deleted: usize,
    /// Tombstone fraction in `[0.0, 1.0]` (`deleted / entries`). Drives the
    /// holes-ratio compaction trigger.
    pub holes_ratio: f64,
    /// Vocab epoch this segment's queries were compiled at.
    pub vocab_epoch: u64,
    /// True if `vocab_epoch` is behind the engine's current epoch — this segment's
    /// normalizer differs from the live one, so its queries should be reingested
    /// for consistent matching. The (empty) memtable is never flagged stale.
    pub stale: bool,
    /// Resident heap bytes for the match payload: exact SoA + candidate indexes +
    /// anchor filter. **0 for `Mmap` segments** — their payload is file-backed and
    /// paged through the OS cache, not the heap (matching the byte accounting in
    /// [`EngineMetrics`]). A 0 here is informative: the segment is off-heap.
    pub resident_bytes: usize,
    /// Resident heap bytes for the always-in-RAM overhead: the logical→local
    /// reverse index + the liveness overlay. Real for **both** kinds (an mmap'd
    /// segment still keeps these structures resident).
    pub overhead_bytes: usize,
}
