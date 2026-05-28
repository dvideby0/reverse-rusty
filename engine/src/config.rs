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
#[derive(Debug, Clone)]
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
}
