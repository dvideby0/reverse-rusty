//! Engine events and metrics — zero-dependency observability for the percolator.
//!
//! Design philosophy: the engine emits structured events through an optional
//! callback (no logging crate dependency). Callers wire these into whatever
//! observability stack they use — `tracing`, `log`, Prometheus push, or a
//! simple `Vec<EngineEvent>` in tests.
//!
//! The [`EngineMetrics`] struct is a point-in-time snapshot of engine state,
//! suitable for periodic scraping or dashboard display.

use crate::segment::CompactionReport;

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
}
