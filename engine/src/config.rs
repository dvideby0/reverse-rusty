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

/// Configuration for the Reverse Rusty [`Engine`](crate::segment::Engine).
///
/// All fields have sensible defaults via `Default`. Pass to
/// [`Engine::with_config`](crate::segment::Engine::with_config) to override.
///
/// # Example
/// ```
/// use reverse_rusty::config::EngineConfig;
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

    /// Re-anchor drifted queries during compaction (the "improve" phase,
    /// [`ingestion-and-updates.md`](../docs/design/ingestion-and-updates.md) §7.3,
    /// ADR-056). When `true`, a merge re-derives each alive query's signature cover
    /// with the *current* feature frequencies instead of carrying the old anchors
    /// forward verbatim, so a query whose anchor drifted to a more-common feature
    /// moves onto its now-most-selective anchor — shrinking hot postings and
    /// per-title candidate fan-out. Result-preserving: re-anchoring only changes
    /// *which* posting list a query lives in (the cover stays lossless because it is
    /// rebuilt by the same optimizer the title side is matched against), never the
    /// match set, never the exact-store data — proven zero-false-negative by the
    /// differential oracle. (The cost class A/B/C *may* change — e.g. a query whose
    /// anchor drifted to high frequency escalating to a more-selective arity-2 cover —
    /// which is exactly the repair; it stays lossless by the same matched-pair argument.)
    ///
    /// Works *within* the frozen 64-hot common mask (re-ranking the hot set itself is a
    /// major-version blue/green concern, §8), so it repairs frequency-ordering drift
    /// rather than re-classifying hotness. A no-op in a cluster shard
    /// (whose shared dict is frozen, so frequencies never drift) and on a single
    /// build (no drift yet) — so the default path is byte-identical.
    ///
    /// Default: `false`
    pub compaction_reanchor: bool,

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
    // These are enforced on every ingest path (live insert, bulk, initial build)
    // via [`parse_limits`](Self::parse_limits), which the engine threads into the
    // DSL parser. They are dynamic (`PUT /_settings`); a tightened limit takes
    // effect on the next ingest. WAL replay deliberately ignores them and uses
    // the compiled-in ceiling, so a tightened limit never drops an
    // already-acknowledged write on recovery.
    /// Maximum query string length in bytes. Queries exceeding this are rejected
    /// at parse time with `ParseErrorKind::QueryTooLong`.
    ///
    /// Default: [`dsl::MAX_QUERY_LENGTH`](crate::dsl::MAX_QUERY_LENGTH).
    pub max_query_length: usize,

    /// Maximum number of clauses (terms + groups) in a single query. Each term
    /// and each any-of group counts as one clause. Exceeding it is rejected with
    /// `ParseErrorKind::TooManyClauses`.
    ///
    /// Default: [`dsl::MAX_CLAUSES`](crate::dsl::MAX_CLAUSES).
    pub max_query_clauses: usize,

    /// Maximum number of members in a single any-of group `(a,b,c,...)`.
    /// Exceeding it is rejected with `ParseErrorKind::AnyOfGroupTooLarge`.
    ///
    /// Default: [`dsl::MAX_ANY_OF_SIZE`](crate::dsl::MAX_ANY_OF_SIZE).
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

    // ---- broad-lane batch evaluation (ADR-026) ----
    // These govern the columnar broad lane used by `POST /_mpercolate`. They are
    // performance/observability knobs only: none change the match result set (the
    // batch path is byte-identical to the per-title path for every setting —
    // tests/broad_batch.rs). All four are dynamic (`PUT /_settings`).
    /// Title sub-batch / rayon chunk size for the columnar broad pass. Larger
    /// amortizes each broad posting's scan over more titles (higher throughput,
    /// higher per-request latency); smaller is the reverse. Never changes results.
    ///
    /// Default: `256`
    pub broad_batch_size: usize,

    /// Use the columnar broad evaluator (once per batch). When `false`, the
    /// batch path falls back to the original inline per-title broad probe — the
    /// provable kill-switch (byte-identical results, no amortization).
    ///
    /// Default: `true`
    pub broad_columnar: bool,

    /// Use the pure-anchor materialization fast path: broad queries whose entire
    /// semantics is their hot anchor emit directly from the anchor's title bitmap
    /// with no exact verification. When `false`, those queries go through full
    /// bitmap verification instead (identical results, slower) — a kill-switch for
    /// the optimization.
    ///
    /// Default: `true`
    pub broad_materialize: bool,

    /// Maximum number of documents accepted in a single `POST /_mpercolate` batch.
    /// Requests above this are rejected with `400` before any work is scheduled,
    /// bounding per-request memory and latency.
    ///
    /// Default: `10_000`
    pub max_percolate_batch: usize,

    // ---- the class-D always-candidate lane (ADR-068) ----
    /// Accept negation-only queries (cost class D: no required feature, no any-of
    /// group — only forbidden features) as **always-candidates**: stored in the
    /// broad lane under the universal signature, a member of every title's
    /// candidate set, forbidden features enforced only in exact verification.
    /// This is the ES/OS `query_string` match-all-except parity lane; like every
    /// broad-lane query, an always-candidate matches only when the request
    /// includes the broad lane. A query with no positives AND no forbidden
    /// features (an effectively empty query) is rejected regardless.
    ///
    /// Dynamic (`PUT /_settings`); gates **acceptance only** — already-stored
    /// entries stay matchable when toggled off, and WAL replay / the vocab
    /// recompile deliberately ignore it (an acknowledged or stored query is never
    /// dropped by a since-flipped knob).
    ///
    /// Default: `false` (negation-only queries are loudly rejected)
    pub accept_class_d: bool,

    /// Translog peer-recovery retention-lease TTL, in seconds (ADR-048). A lease pins a
    /// recovery source's un-sealed translog tail so a concurrent seal can't trim it
    /// (ADR-040); a recovery renews its lease every catch-up pass (the heartbeat). If a
    /// lease has not been renewed within this window it is presumed dead — a crashed or
    /// stalled recovering node — and is reaped at the next `seal_for_checkpoint`, so the
    /// source can reclaim its tail instead of pinning it forever.
    ///
    /// Must exceed the longest expected single-shard peer recovery (a stall this long means
    /// the recovery is effectively dead). `0` disables the TTL entirely (a lease never
    /// expires — byte-identical to ADR-040). Only affects durable cluster shards; ignored by
    /// an in-memory shard that never seals.
    ///
    /// Default: `1800` (30 minutes)
    pub retention_lease_ttl_secs: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            max_segments: 8,
            holes_ratio_threshold: 0.3,
            memtable_flush_threshold: 100_000,
            auto_compact_on_flush: true,
            auto_compact_on_ingest: true,
            compaction_reanchor: false,
            data_dir: None,
            wal_sync_on_write: false,
            retain_source: true,
            max_query_length: crate::dsl::MAX_QUERY_LENGTH,
            max_query_clauses: crate::dsl::MAX_CLAUSES,
            max_anyof_group_size: crate::dsl::MAX_ANY_OF_SIZE,
            compaction_fixed_cost: 1000.0,
            broad_batch_size: 256,
            broad_columnar: true,
            broad_materialize: true,
            max_percolate_batch: 10_000,
            accept_class_d: false,
            retention_lease_ttl_secs: 1800,
        }
    }
}

impl EngineConfig {
    /// Default configuration — identical to `Default::default()` but available
    /// as a `const fn` for static initialization.
    pub fn new() -> Self {
        Self::default()
    }

    /// The query-complexity limits to apply at parse time, derived from this
    /// config. The ingest paths pass this to
    /// [`dsl::parse_with_limits`](crate::dsl::parse_with_limits) so the
    /// configured (and runtime-tunable) limits actually govern parsing.
    pub fn parse_limits(&self) -> crate::dsl::ParseLimits {
        crate::dsl::ParseLimits {
            max_query_length: self.max_query_length,
            max_clauses: self.max_query_clauses,
            max_any_of_size: self.max_anyof_group_size,
        }
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
        if self.broad_batch_size == 0 {
            problems.push("broad_batch_size must be >= 1".into());
        }
        if self.max_percolate_batch == 0 {
            problems.push("max_percolate_batch must be >= 1".into());
        }
        problems
    }
}
