//! Command-line flags for the HTTP server (clap). The bulk of these mirror the
//! tunable [`reverse_rusty::config::EngineConfig`] knobs; `main` maps them onto a
//! config and the runtime-tunable subset is also reachable via `PUT /_settings`.

use std::path::PathBuf;

use clap::Parser;

#[allow(clippy::struct_excessive_bools)]
#[derive(Parser, Debug)]
#[command(name = "reverse-rusty-server", about = "Reverse Rusty HTTP server")]
pub(crate) struct Cli {
    /// IP address to bind. Defaults to `127.0.0.1` (loopback), so the server is NOT
    /// reachable beyond the local host unless you opt in — the REST API has no
    /// built-in authentication and exposes mutating/admin endpoints (`_doc`, `_bulk`,
    /// `_flush`, `_compact`, `_vocab`, `_settings`). Set to `0.0.0.0` to listen on all
    /// interfaces only behind a trusted network or an authenticating reverse proxy
    /// (see docs/reference/api.md). Matches the loopback default the gRPC bins use.
    #[arg(long, default_value = "127.0.0.1")]
    pub(crate) host: std::net::IpAddr,

    /// Port to listen on.
    #[arg(long, default_value_t = 9200)]
    pub(crate) port: u16,

    /// Persistence directory (segments, WAL). Omit for in-memory only.
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,

    /// Pre-load queries from a CSV or JSONL file at startup.
    #[arg(long)]
    pub(crate) load_file: Option<PathBuf>,

    /// Load vocabulary from a JSON file at startup.
    #[arg(long)]
    pub(crate) vocab_file: Option<PathBuf>,

    /// Include broad-lane queries in match results.
    #[arg(long, default_value_t = false)]
    pub(crate) include_broad: bool,

    /// Number of rayon worker threads (defaults to physical cores).
    #[arg(long)]
    pub(crate) threads: Option<usize>,

    /// Graceful shutdown drain timeout in seconds.
    #[arg(long, default_value_t = 30)]
    pub(crate) drain_timeout: u64,

    /// Log format: "json" for structured JSON, "pretty" for human-readable.
    #[arg(long, default_value = "pretty")]
    pub(crate) log_format: String,

    /// Slow-query threshold in milliseconds. Searches exceeding this are logged
    /// at warn level with diagnostic context. 0 disables.
    #[arg(long, default_value_t = 1000)]
    pub(crate) slow_query_threshold_ms: u64,

    /// Max base segments before compaction triggers.
    #[arg(long, default_value_t = 8)]
    pub(crate) max_segments: usize,

    /// Memtable entry count that triggers an automatic flush.
    #[arg(long, default_value_t = 100_000)]
    pub(crate) memtable_flush_threshold: usize,

    /// Maximum query string length in bytes.
    #[arg(long, default_value_t = reverse_rusty::dsl::MAX_QUERY_LENGTH)]
    pub(crate) max_query_length: usize,

    /// Maximum number of clauses per query.
    #[arg(long, default_value_t = reverse_rusty::dsl::MAX_CLAUSES)]
    pub(crate) max_query_clauses: usize,

    /// Maximum members in an any-of group.
    #[arg(long, default_value_t = reverse_rusty::dsl::MAX_ANY_OF_SIZE)]
    pub(crate) max_anyof_group_size: usize,

    /// Fsync the write-ahead log on every mutation before acknowledging it.
    /// When false (default), WAL appends reach the OS page cache and are
    /// fsync'd at the next flush checkpoint — an acknowledged write survives a
    /// process crash but not power loss until checkpoint (RocksDB sync=false /
    /// SQLite NORMAL). When true, every write is durable against power loss at
    /// a large per-write latency cost (SQLite FULL).
    #[arg(long, default_value_t = false)]
    pub(crate) wal_sync_on_write: bool,

    /// Keep every query's source text resident in RAM (default true — instant
    /// `_source`/explain, historical behavior). Set false to store source text on
    /// disk (`sources.dat`, mmap'd) and fetch it lazily — a large resident-memory
    /// saving at scale (the source store is the single largest resident structure
    /// at ~100M queries), at the cost of a cold binary-search + page fault per
    /// `_source`/explain lookup (never the match hot path). See ADR-020.
    #[arg(long, default_value_t = true)]
    pub(crate) retain_source: bool,

    /// Title sub-batch size for the columnar broad lane on `POST /_mpercolate`
    /// (ADR-026). Larger amortizes broad-posting scans over more titles. Dynamic
    /// via `PUT /_settings`.
    #[arg(long, default_value_t = 256)]
    pub(crate) broad_batch_size: usize,

    /// Use the columnar broad evaluator (once per batch). Set false to fall back
    /// to the inline per-title broad probe — the kill-switch (identical results,
    /// no amortization). Dynamic via `PUT /_settings`.
    #[arg(long, default_value_t = true)]
    pub(crate) broad_columnar: bool,

    /// Use the pure-anchor materialization fast path (emit pure-anchor broad
    /// queries straight from the anchor bitmap, skipping verification). Dynamic
    /// via `PUT /_settings`.
    #[arg(long, default_value_t = true)]
    pub(crate) broad_materialize: bool,

    /// Maximum documents accepted in one `POST /_mpercolate` batch; larger
    /// requests are rejected with 400. Dynamic via `PUT /_settings`.
    #[arg(long, default_value_t = 10_000)]
    pub(crate) max_percolate_batch: usize,
}
