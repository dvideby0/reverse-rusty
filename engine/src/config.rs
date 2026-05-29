//! Engine configuration — runtime-tunable knobs for compaction, flush, and merge scoring.
//!
//! Design: docs/design/ingestion-and-updates.md §7 (compaction policy)
//! Invariant: None of these settings affect the correctness contract;
//!   they only govern when maintenance work triggers and how aggressive it is.
//!
//! Posting-level thresholds (INLINE_CAP, ROARING_THRESHOLD) remain compile-time
//! constants in index.rs — they affect memory layout and are stable across
//! production workloads. The knobs here are engine-level: flush cadence,
//! compaction trigger policy, and merge-score tuning.

/// Configuration for the percolator [`Engine`](crate::segment::Engine).
///
/// All fields have sensible defaults via `Default`. Pass to
/// [`Engine::with_config`](crate::segment::Engine::with_config) to override.
///
/// # Example
/// ```
/// use percolator::config::EngineConfig;
///
/// let config = EngineConfig {
///     max_segments: 6,
///     auto_compact_on_flush: true,
///     ..EngineConfig::default()
/// };
/// ```
// Config knobs are naturally a flat bag of independent flags; grouping the bools
// into sub-structs would hurt readability for no gain.
#[allow(clippy::struct_excessive_bools)]
// `Serialize` so the server can expose the live config as JSON via `GET /_settings`
// (the field names are the setting keys). Updates go through the server's flat
// JSON patch, not `Deserialize`, so the dynamic/static split can be enforced.
#[derive(Debug, Clone, serde::Serialize)]
pub struct EngineConfig {
    // ---- compaction trigger policy ----
    /// Maximum base segment count before `maybe_compact` triggers a merge.
    /// When the number of sealed base segments exceeds this, the engine picks
    /// the best merge range (score-based) and compacts it. Set to `usize::MAX`
    /// to disable segment-count-triggered compaction.
    ///
    /// Default: `8`
    pub max_segments: usize,

    /// Holes-ratio threshold: if ANY base segment's tombstone fraction exceeds
    /// this, `maybe_compact` will merge that segment (with its neighbors) even
    /// if the segment count is below `max_segments`. This reclaims dead space
    /// from heavy update workloads. Set to `1.0` to disable.
    ///
    /// Default: `0.3` (30% tombstones triggers a merge)
    pub holes_ratio_threshold: f64,

    /// Memtable entry count that triggers an automatic flush (seal the memtable
    /// into an immutable base segment). Checked after each `insert_live`. Set
    /// to `usize::MAX` to disable auto-flush (caller manages flush timing).
    ///
    /// Default: `100_000`
    pub memtable_flush_threshold: usize,

    /// Run `maybe_compact` automatically after every `flush`. When true, the
    /// engine checks the compaction policy after sealing the memtable. When
    /// false, the caller is responsible for calling `compact` / `maybe_compact`.
    ///
    /// Default: `true`
    pub auto_compact_on_flush: bool,

    /// Run `maybe_compact` automatically after every `bulk_ingest`. Same
    /// semantics as `auto_compact_on_flush`.
    ///
    /// Default: `true`
    pub auto_compact_on_ingest: bool,

    // ---- persistence ----
    /// Directory for persisting segments and WAL. When `Some`, sealed segments
    /// are written to disk and mmap'd back; the WAL records mutations for crash
    /// recovery. When `None` (default), the engine is fully in-memory.
    ///
    /// Default: `None`
    pub data_dir: Option<std::path::PathBuf>,

    /// WAL durability policy. When `false` (default), each `insert`/`tombstone`
    /// is `write(2)`-en to the WAL (reaching the OS page cache) but only
    /// `fsync`'d at the next flush checkpoint — so an acknowledged write
    /// survives a process crash but not a power loss until the next checkpoint
    /// (equivalent to RocksDB `sync=false` / SQLite `synchronous=NORMAL`).
    /// When `true`, every WAL append is `fsync`'d before the write is
    /// acknowledged, so an acknowledged write survives power loss (equivalent
    /// to SQLite `synchronous=FULL`) — at a large per-write latency cost (one
    /// device flush per mutation). Independent of error propagation: a failed
    /// WAL write is always surfaced to the caller and the mutation rejected,
    /// regardless of this setting.
    ///
    /// Default: `false`
    pub wal_sync_on_write: bool,

    /// Whether to keep every query's source text resident in RAM. When `true`
    /// (default), the source store is fully in-memory — `_source`/explain reads
    /// are instant, matching historical behavior. When `false`, source text is
    /// kept on disk (`sources.dat`, mmap'd) and fetched on demand; this trades a
    /// cold binary-search + possible page fault per `_source`/explain lookup
    /// (never the match hot path) for a large resident-memory saving at scale —
    /// at ~100M queries the source text is the single largest resident structure
    /// (see ADR-020). Mutations between flushes are held in a small in-memory
    /// overlay regardless of this setting.
    ///
    /// Default: `true`
    pub retain_source: bool,

    // ---- merge scoring ----

    // ---- query complexity limits ----
    /// Maximum query string length in bytes. Queries exceeding this are
    /// rejected at parse time with `ParseErrorKind::QueryTooLong`.
    ///
    /// Default: `10_000`
    pub max_query_length: usize,

    /// Maximum number of clauses (terms + groups) in a single query.
    /// Each term and each any-of group counts as one clause.
    ///
    /// Default: `256`
    pub max_query_clauses: usize,

    /// Maximum number of members in a single any-of group `(a,b,c,...)`.
    ///
    /// Default: `64`
    pub max_anyof_group_size: usize,

    // ---- merge scoring ----
    /// Fixed-cost bias in the ClickHouse-inspired merge score formula:
    ///   `score = (sum_size + fixed_cost * count) / (count - 1.9)`
    ///
    /// Higher values bias toward merging small segments first (cheap wins).
    /// Lower values prefer merging fewer, larger segments.
    ///
    /// Default: `1000.0`
    pub compaction_fixed_cost: f64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            max_segments: 8,
            holes_ratio_threshold: 0.3,
            memtable_flush_threshold: 100_000,
            auto_compact_on_flush: true,
            auto_compact_on_ingest: true,
            data_dir: None,
            wal_sync_on_write: false,
            retain_source: true,
            max_query_length: 10_000,
            max_query_clauses: 256,
            max_anyof_group_size: 64,
            compaction_fixed_cost: 1000.0,
        }
    }
}

impl EngineConfig {
    /// Default configuration — identical to `Default::default()` but available
    /// as a `const fn` for static initialization.
    pub fn new() -> Self {
        Self::default()
    }

    /// Validate configuration, returning a list of problems. Empty means valid.
    pub fn validate(&self) -> Vec<String> {
        let mut problems = Vec::new();
        if self.max_segments == 0 {
            problems.push("max_segments must be >= 1".into());
        }
        if self.memtable_flush_threshold == 0 {
            problems.push("memtable_flush_threshold must be >= 1".into());
        }
        if self.holes_ratio_threshold < 0.0 || self.holes_ratio_threshold > 1.0 {
            problems.push("holes_ratio_threshold must be in [0.0, 1.0]".into());
        }
        if self.max_query_length == 0 {
            problems.push("max_query_length must be >= 1".into());
        }
        if self.max_query_clauses == 0 {
            problems.push("max_query_clauses must be >= 1".into());
        }
        if self.max_anyof_group_size == 0 {
            problems.push("max_anyof_group_size must be >= 1".into());
        }
        if self.compaction_fixed_cost < 0.0 {
            problems.push("compaction_fixed_cost must be >= 0".into());
        }
        problems
    }
}
