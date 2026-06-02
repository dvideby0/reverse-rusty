//! Reverse Rusty HTTP server — Elasticsearch-inspired REST API.
//!
//! Endpoints:
//!   PUT  /_doc/{id}          Register a query (body: {"query": "..."})
//!   DELETE /_doc/{id}        Remove a stored query
//!   POST /_search            Percolate title(s) (body: {"document": {"title": "..."}} or "documents")
//!   POST /_mpercolate        Batch percolate (body: {"documents":[...]}, responses[] envelope)
//!   POST /_bulk              NDJSON bulk ingest ({action}\n{source}\n...)
//!   POST /_flush             Flush memtable to immutable segment
//!   POST /_compact           Force compaction
//!   GET  /_stats             JSON metrics snapshot
//!   GET  /_cat/stats         Human-readable metrics
//!   GET  /_cat/segments      Per-segment LSM detail (text table; ?format=json)
//!   GET  /_health            Health check
//!   GET  /_metrics           Prometheus text exposition format
//!   GET  /_vocab             Current vocabulary as JSON
//!   PUT  /_vocab             Replace vocabulary (body: Vocab JSON)
//!   POST /_vocab/learn       Learn synonyms from raw query text
//!   GET  /_settings          Engine settings as JSON (?include_defaults=true)
//!   PUT  /_settings          Update dynamic settings (body: flat JSON, e.g. {"max_segments":16})
//!
//! Usage:
//!   cargo run --release --bin server -- [--port 9200] [--data-dir ./data] [--load-file queries.csv]
//!
//! The engine uses a snapshot-based concurrency model: a `Mutex<Engine>` for
//! serialized writes and an `ArcSwap<EngineSnapshot>` for lock-free reads.
//! Search and other read endpoints load the snapshot without any lock;
//! writes acquire the mutex, mutate the engine, then atomically publish a
//! new snapshot.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use axum::{
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use parking_lot::Mutex;
use prometheus::{
    Counter, Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info, instrument, warn};

use std::cell::RefCell;

use reverse_rusty::config::EngineConfig;
use reverse_rusty::events::{EngineEvent, SegmentInfo};
use reverse_rusty::loader;
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{
    BatchMatchOptions, BroadStrategy, Engine, EngineSnapshot, IngestItemStatus, MatchScratch,
    MatchStats,
};

thread_local! {
    static SCRATCH: RefCell<MatchScratch> = RefCell::new(MatchScratch::new());
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

// CLI flags are naturally a flat bag of independent toggles (mirroring the
// EngineConfig knobs); grouping the bools into sub-structs would not help.
#[allow(clippy::struct_excessive_bools)]
#[derive(Parser, Debug)]
#[command(name = "reverse-rusty-server", about = "Reverse Rusty HTTP server")]
struct Cli {
    /// Port to listen on.
    #[arg(long, default_value_t = 9200)]
    port: u16,

    /// Persistence directory (segments, WAL). Omit for in-memory only.
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Pre-load queries from a CSV or JSONL file at startup.
    #[arg(long)]
    load_file: Option<PathBuf>,

    /// Load vocabulary from a JSON file at startup.
    #[arg(long)]
    vocab_file: Option<PathBuf>,

    /// Include broad-lane queries in match results.
    #[arg(long, default_value_t = false)]
    include_broad: bool,

    /// Number of rayon worker threads (defaults to physical cores).
    #[arg(long)]
    threads: Option<usize>,

    /// Graceful shutdown drain timeout in seconds.
    #[arg(long, default_value_t = 30)]
    drain_timeout: u64,

    /// Log format: "json" for structured JSON, "pretty" for human-readable.
    #[arg(long, default_value = "pretty")]
    log_format: String,

    /// Slow-query threshold in milliseconds. Searches exceeding this are logged
    /// at warn level with diagnostic context. 0 disables.
    #[arg(long, default_value_t = 1000)]
    slow_query_threshold_ms: u64,

    /// Max base segments before compaction triggers.
    #[arg(long, default_value_t = 8)]
    max_segments: usize,

    /// Memtable entry count that triggers an automatic flush.
    #[arg(long, default_value_t = 100_000)]
    memtable_flush_threshold: usize,

    /// Maximum query string length in bytes.
    #[arg(long, default_value_t = reverse_rusty::dsl::MAX_QUERY_LENGTH)]
    max_query_length: usize,

    /// Maximum number of clauses per query.
    #[arg(long, default_value_t = reverse_rusty::dsl::MAX_CLAUSES)]
    max_query_clauses: usize,

    /// Maximum members in an any-of group.
    #[arg(long, default_value_t = reverse_rusty::dsl::MAX_ANY_OF_SIZE)]
    max_anyof_group_size: usize,

    /// Fsync the write-ahead log on every mutation before acknowledging it.
    /// When false (default), WAL appends reach the OS page cache and are
    /// fsync'd at the next flush checkpoint — an acknowledged write survives a
    /// process crash but not power loss until checkpoint (RocksDB sync=false /
    /// SQLite NORMAL). When true, every write is durable against power loss at
    /// a large per-write latency cost (SQLite FULL).
    #[arg(long, default_value_t = false)]
    wal_sync_on_write: bool,

    /// Keep every query's source text resident in RAM (default true — instant
    /// `_source`/explain, historical behavior). Set false to store source text on
    /// disk (`sources.dat`, mmap'd) and fetch it lazily — a large resident-memory
    /// saving at scale (the source store is the single largest resident structure
    /// at ~100M queries), at the cost of a cold binary-search + page fault per
    /// `_source`/explain lookup (never the match hot path). See ADR-020.
    #[arg(long, default_value_t = true)]
    retain_source: bool,

    /// Title sub-batch size for the columnar broad lane on `POST /_mpercolate`
    /// (ADR-026). Larger amortizes broad-posting scans over more titles. Dynamic
    /// via `PUT /_settings`.
    #[arg(long, default_value_t = 256)]
    broad_batch_size: usize,

    /// Use the columnar broad evaluator (once per batch). Set false to fall back
    /// to the inline per-title broad probe — the kill-switch (identical results,
    /// no amortization). Dynamic via `PUT /_settings`.
    #[arg(long, default_value_t = true)]
    broad_columnar: bool,

    /// Use the pure-anchor materialization fast path (emit pure-anchor broad
    /// queries straight from the anchor bitmap, skipping verification). Dynamic
    /// via `PUT /_settings`.
    #[arg(long, default_value_t = true)]
    broad_materialize: bool,

    /// Maximum documents accepted in one `POST /_mpercolate` batch; larger
    /// requests are rejected with 400. Dynamic via `PUT /_settings`.
    #[arg(long, default_value_t = 10_000)]
    max_percolate_batch: usize,
}

// ---------------------------------------------------------------------------
// Prometheus metrics registry
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct PrometheusMetrics {
    registry: Registry,

    // Engine gauge metrics (scraped from EngineMetrics snapshot)
    total_queries: IntGauge,
    base_segments: IntGauge,
    memtable_entries: IntGauge,
    dict_features: IntGauge,
    memory_bytes: IntGaugeVec,
    wal_size_bytes: IntGauge,
    wal_pending_entries: IntGauge,

    // Cumulative counters (incremented via EngineEvent observer)
    flush_total: IntCounter,
    flush_entries_total: IntCounter,
    ingest_total: IntCounter,
    ingest_queries_total: IntCounter,
    ingest_rejected: IntCounterVec,
    compaction_total: IntCounter,
    compaction_tombstones_reclaimed: IntCounter,
    segment_cleanup_failures_total: IntCounter,
    /// Durability/persistence failures, labeled by `op` (e.g. `segment_write`,
    /// `manifest_write`, `wal_append`). Alert on this — a nonzero rate means
    /// durability is degraded. See `EngineEvent::DurabilityFailure`.
    durability_failures_total: IntCounterVec,
    flush_time_seconds_total: Counter,
    compaction_time_seconds_total: Counter,

    // Request metrics
    http_requests_total: IntCounterVec,
    http_request_duration: HistogramVec,
    in_flight_requests: IntGauge,

    // Match metrics
    match_candidates_per_title: Histogram,
    match_results_per_title: Histogram,

    // Broad-lane batch metrics (POST /_mpercolate columnar evaluation, ADR-026).
    // Cumulative across requests; the amortization shows as broad_postings_scanned
    // rising far slower than broad_candidates as batch size grows.
    broad_batches_total: IntCounter,
    broad_postings_scanned_total: IntCounter,
    broad_queries_evaluated_total: IntCounter,
    broad_candidates_total: IntCounter,

    // Slow query counter
    slow_queries_total: IntCounter,
}

impl PrometheusMetrics {
    fn new() -> Self {
        let registry = Registry::new_custom(Some("reverse_rusty".to_string()), None)
            .expect("failed to create prometheus registry");

        // --- Engine gauges (refreshed on each /_metrics scrape) ---

        let total_queries = IntGauge::with_opts(Opts::new(
            "total_queries",
            "Total queries stored across all segments and memtable",
        ))
        .unwrap();

        let base_segments = IntGauge::with_opts(Opts::new(
            "base_segments",
            "Number of sealed immutable base segments",
        ))
        .unwrap();

        let memtable_entries = IntGauge::with_opts(Opts::new(
            "memtable_entries",
            "Entries currently in the mutable memtable",
        ))
        .unwrap();

        let dict_features = IntGauge::with_opts(Opts::new(
            "dict_features",
            "Distinct features in the shared dictionary",
        ))
        .unwrap();

        let memory_bytes = IntGaugeVec::new(
            Opts::new("memory_bytes", "Heap memory usage by component"),
            &["component"],
        )
        .unwrap();

        let wal_size_bytes = IntGauge::with_opts(Opts::new(
            "wal_size_bytes",
            "Current on-disk size of the write-ahead log in bytes",
        ))
        .unwrap();

        let wal_pending_entries = IntGauge::with_opts(Opts::new(
            "wal_pending_entries",
            "Un-checkpointed WAL entries (mutations not yet in a sealed segment)",
        ))
        .unwrap();

        // --- Event counters ---

        let flush_total =
            IntCounter::with_opts(Opts::new("flush_total", "Total number of memtable flushes"))
                .unwrap();

        let flush_entries_total = IntCounter::with_opts(Opts::new(
            "flush_entries_total",
            "Total entries flushed across all flushes",
        ))
        .unwrap();

        let ingest_total = IntCounter::with_opts(Opts::new(
            "ingest_total",
            "Total number of bulk ingest operations",
        ))
        .unwrap();

        let ingest_queries_total = IntCounter::with_opts(Opts::new(
            "ingest_queries_total",
            "Total queries ingested successfully",
        ))
        .unwrap();

        let ingest_rejected = IntCounterVec::new(
            Opts::new("ingest_rejected_total", "Queries rejected during ingest"),
            &["reason"],
        )
        .unwrap();

        let compaction_total = IntCounter::with_opts(Opts::new(
            "compaction_total",
            "Total number of compaction operations",
        ))
        .unwrap();

        let compaction_tombstones_reclaimed = IntCounter::with_opts(Opts::new(
            "compaction_tombstones_reclaimed_total",
            "Tombstones reclaimed by compaction",
        ))
        .unwrap();

        let segment_cleanup_failures_total = IntCounter::with_opts(Opts::new(
            "segment_cleanup_failures_total",
            "Segment files that failed best-effort removal (orphan/stale cleanup)",
        ))
        .unwrap();

        let durability_failures_total = IntCounterVec::new(
            Opts::new(
                "durability_failures_total",
                "Durability/persistence failures by operation (degraded durability — alertable)",
            ),
            &["op"],
        )
        .unwrap();

        let flush_time_seconds_total = Counter::with_opts(Opts::new(
            "flush_time_seconds_total",
            "Cumulative wall-clock seconds spent flushing the memtable into segments",
        ))
        .unwrap();

        let compaction_time_seconds_total = Counter::with_opts(Opts::new(
            "compaction_time_seconds_total",
            "Cumulative wall-clock seconds spent compacting base segments",
        ))
        .unwrap();

        // --- HTTP request metrics ---

        let http_requests_total = IntCounterVec::new(
            Opts::new(
                "http_requests_total",
                "Total HTTP requests by endpoint and status",
            ),
            &["endpoint", "status"],
        )
        .unwrap();

        let http_request_duration = HistogramVec::new(
            HistogramOpts::new(
                "http_request_duration_seconds",
                "HTTP request duration in seconds",
            )
            .buckets(vec![
                0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0,
            ]),
            &["endpoint"],
        )
        .unwrap();

        let in_flight_requests = IntGauge::with_opts(Opts::new(
            "in_flight_requests",
            "HTTP requests currently being processed",
        ))
        .unwrap();

        // --- Match metrics ---

        let match_candidates_per_title = Histogram::with_opts(
            HistogramOpts::new(
                "match_candidates_per_title",
                "Candidate queries evaluated per title",
            )
            .buckets(vec![
                1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0,
            ]),
        )
        .unwrap();

        let match_results_per_title = Histogram::with_opts(
            HistogramOpts::new("match_results_per_title", "Confirmed matches per title")
                .buckets(vec![0.0, 1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0]),
        )
        .unwrap();

        let slow_queries_total = IntCounter::with_opts(Opts::new(
            "slow_queries_total",
            "Searches exceeding the slow-query threshold",
        ))
        .unwrap();

        // --- Broad-lane batch metrics (POST /_mpercolate) ---

        let broad_batches_total = IntCounter::with_opts(Opts::new(
            "broad_batches_total",
            "Broad-lane sub-batches (title chunks) evaluated columnar",
        ))
        .unwrap();

        let broad_postings_scanned_total = IntCounter::with_opts(Opts::new(
            "broad_postings_scanned_total",
            "Broad posting entries scanned (the quantity batch evaluation amortizes)",
        ))
        .unwrap();

        let broad_queries_evaluated_total = IntCounter::with_opts(Opts::new(
            "broad_queries_evaluated_total",
            "Broad queries exact-checked via bitmap evaluation (non pure-anchor)",
        ))
        .unwrap();

        let broad_candidates_total = IntCounter::with_opts(Opts::new(
            "broad_candidates_total",
            "Broad-lane candidate queries retrieved across batches",
        ))
        .unwrap();

        // Register all
        registry.register(Box::new(total_queries.clone())).unwrap();
        registry.register(Box::new(base_segments.clone())).unwrap();
        registry
            .register(Box::new(memtable_entries.clone()))
            .unwrap();
        registry.register(Box::new(dict_features.clone())).unwrap();
        registry.register(Box::new(memory_bytes.clone())).unwrap();
        registry.register(Box::new(flush_total.clone())).unwrap();
        registry
            .register(Box::new(flush_entries_total.clone()))
            .unwrap();
        registry.register(Box::new(ingest_total.clone())).unwrap();
        registry
            .register(Box::new(ingest_queries_total.clone()))
            .unwrap();
        registry
            .register(Box::new(ingest_rejected.clone()))
            .unwrap();
        registry
            .register(Box::new(compaction_total.clone()))
            .unwrap();
        registry
            .register(Box::new(compaction_tombstones_reclaimed.clone()))
            .unwrap();
        registry
            .register(Box::new(durability_failures_total.clone()))
            .unwrap();
        registry
            .register(Box::new(segment_cleanup_failures_total.clone()))
            .unwrap();
        registry
            .register(Box::new(http_requests_total.clone()))
            .unwrap();
        registry
            .register(Box::new(http_request_duration.clone()))
            .unwrap();
        registry
            .register(Box::new(match_candidates_per_title.clone()))
            .unwrap();
        registry
            .register(Box::new(match_results_per_title.clone()))
            .unwrap();
        registry
            .register(Box::new(broad_batches_total.clone()))
            .unwrap();
        registry
            .register(Box::new(broad_postings_scanned_total.clone()))
            .unwrap();
        registry
            .register(Box::new(broad_queries_evaluated_total.clone()))
            .unwrap();
        registry
            .register(Box::new(broad_candidates_total.clone()))
            .unwrap();
        registry
            .register(Box::new(slow_queries_total.clone()))
            .unwrap();
        registry.register(Box::new(wal_size_bytes.clone())).unwrap();
        registry
            .register(Box::new(wal_pending_entries.clone()))
            .unwrap();
        registry
            .register(Box::new(flush_time_seconds_total.clone()))
            .unwrap();
        registry
            .register(Box::new(compaction_time_seconds_total.clone()))
            .unwrap();
        registry
            .register(Box::new(in_flight_requests.clone()))
            .unwrap();

        Self {
            registry,
            total_queries,
            base_segments,
            memtable_entries,
            dict_features,
            memory_bytes,
            wal_size_bytes,
            wal_pending_entries,
            flush_total,
            flush_entries_total,
            ingest_total,
            ingest_queries_total,
            ingest_rejected,
            compaction_total,
            compaction_tombstones_reclaimed,
            segment_cleanup_failures_total,
            durability_failures_total,
            flush_time_seconds_total,
            compaction_time_seconds_total,
            http_requests_total,
            http_request_duration,
            in_flight_requests,
            match_candidates_per_title,
            match_results_per_title,
            broad_batches_total,
            broad_postings_scanned_total,
            broad_queries_evaluated_total,
            broad_candidates_total,
            slow_queries_total,
        }
    }

    /// Update gauge metrics from an EngineMetrics snapshot.
    fn refresh_gauges(&self, m: &reverse_rusty::events::EngineMetrics) {
        self.total_queries.set(m.total_queries as i64);
        self.base_segments.set(m.base_segments as i64);
        self.memtable_entries.set(m.memtable_entries as i64);
        self.dict_features.set(m.dict_features as i64);
        self.memory_bytes
            .with_label_values(&["exact"])
            .set(m.exact_bytes as i64);
        self.memory_bytes
            .with_label_values(&["index"])
            .set(m.index_bytes as i64);
        self.memory_bytes
            .with_label_values(&["filter"])
            .set(m.filter_bytes as i64);
        self.wal_size_bytes.set(m.wal_size_bytes as i64);
        self.wal_pending_entries.set(m.wal_pending_entries as i64);
    }

    /// Handle an EngineEvent — increment counters. Called from the observer.
    fn observe_event(&self, event: &EngineEvent) {
        match event {
            EngineEvent::Flush {
                entries,
                duration_secs,
                ..
            } => {
                self.flush_total.inc();
                self.flush_entries_total.inc_by(*entries as u64);
                self.flush_time_seconds_total.inc_by(*duration_secs);
            }
            EngineEvent::Ingest {
                ingested,
                rejected_parse,
                rejected_class_d,
                ..
            } => {
                self.ingest_total.inc();
                self.ingest_queries_total.inc_by(*ingested as u64);
                if *rejected_parse > 0 {
                    self.ingest_rejected
                        .with_label_values(&["parse"])
                        .inc_by(*rejected_parse as u64);
                }
                if *rejected_class_d > 0 {
                    self.ingest_rejected
                        .with_label_values(&["class_d"])
                        .inc_by(*rejected_class_d as u64);
                }
            }
            EngineEvent::Compaction {
                report,
                duration_secs,
                ..
            } => {
                self.compaction_total.inc();
                self.compaction_tombstones_reclaimed
                    .inc_by(report.tombstones_reclaimed as u64);
                self.compaction_time_seconds_total.inc_by(*duration_secs);
            }
            EngineEvent::SegmentCleanupFailed { .. } => {
                self.segment_cleanup_failures_total.inc();
            }
            EngineEvent::DurabilityFailure { op, .. } => {
                self.durability_failures_total
                    .with_label_values(&[op.as_str()])
                    .inc();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

struct AppState {
    engine: Mutex<Engine>,
    snapshot: ArcSwap<EngineSnapshot>,
    pool: rayon::ThreadPool,
    include_broad: bool,
    prom: PrometheusMetrics,
    slow_query_threshold_ms: u64,
}

impl AppState {
    fn publish_snapshot(&self) {
        let engine = self.engine.lock();
        self.snapshot.store(Arc::new(engine.snapshot()));
    }
}

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

// -- PUT /_doc/{id}
#[derive(Deserialize)]
struct PutDocBody {
    query: String,
    #[serde(default = "default_version")]
    version: u32,
}
fn default_version() -> u32 {
    1
}

#[derive(Serialize)]
struct PutDocResponse {
    _id: u64,
    result: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

// -- GET /_doc/{id}
#[derive(Serialize)]
struct GetDocResponse {
    _id: u64,
    found: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    _source: Option<HitSource>,
}

// -- DELETE /_doc/{id}
#[derive(Serialize)]
struct DeleteDocResponse {
    _id: u64,
    result: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    deleted_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

// -- POST /_flush
#[derive(Serialize)]
struct FlushResponse {
    took_ms: f64,
    acknowledged: bool,
    total_queries: usize,
    base_segments: usize,
}

// -- POST /_compact
#[derive(Serialize)]
struct CompactResponse {
    took_ms: f64,
    acknowledged: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    segments_merged: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    entries_before: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    entries_after: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tombstones_reclaimed: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<&'static str>,
}

// -- PUT /_vocab
#[derive(Serialize)]
struct PutVocabResponse {
    acknowledged: bool,
    /// Number of stored queries recompiled under the new normalizer so the change
    /// takes effect immediately with zero false negatives (0 if none were affected).
    recompiled: usize,
}

// -- POST /_search
#[derive(Deserialize)]
struct SearchBody {
    document: Option<DocBody>,
    documents: Option<Vec<DocBody>>,
    /// Optional per-request timeout in milliseconds (default: 30000).
    timeout_ms: Option<u64>,
    /// Maximum number of hits to return (default: 1000).
    size: Option<usize>,
    /// Offset into the result set for pagination (default: 0).
    from: Option<usize>,
    /// Include original query text in each hit (default: true).
    include_source: Option<bool>,
    /// Include per-hit explain detail showing why each query matched (default: false).
    explain: Option<bool>,
    /// Include match profile (candidate/posting stats) in the response (default: false).
    profile: Option<bool>,
}

#[derive(Deserialize)]
struct DocBody {
    title: String,
}

#[derive(Serialize)]
struct SearchResponse {
    took_ms: f64,
    hits: SearchHits,
    #[serde(skip_serializing_if = "Option::is_none")]
    slots: Option<Vec<SlotHit>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    profile: Option<StatsResponse>,
}

#[derive(Serialize)]
struct SearchHits {
    total: usize,
    hits: Vec<SearchHitItem>,
}

#[derive(Serialize)]
struct SearchHitItem {
    _id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    _source: Option<HitSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    _explanation: Option<reverse_rusty::ExplainDetail>,
}

#[derive(Serialize)]
struct HitSource {
    query: String,
}

#[derive(Serialize)]
struct SlotHit {
    slot: usize,
    total: usize,
    hits: Vec<SearchHitItem>,
    stats: StatsResponse,
}

#[derive(Serialize, Clone)]
struct StatsResponse {
    unique_candidates: u32,
    /// Broad-lane subset of `unique_candidates` — how much of the work came from
    /// quarantined broad (class-C) queries (0 unless `include_broad`).
    broad_candidates: u32,
    postings_scanned: u32,
    matches: u32,
    probes_attempted: u32,
    probes_skipped: u32,
}

impl From<MatchStats> for StatsResponse {
    fn from(s: MatchStats) -> Self {
        Self {
            unique_candidates: s.unique_candidates,
            broad_candidates: s.broad_candidates,
            postings_scanned: s.postings_scanned,
            matches: s.matches,
            probes_attempted: s.probes_attempted,
            probes_skipped: s.probes_skipped,
        }
    }
}

// -- POST /_bulk
#[derive(Serialize)]
struct BulkResponse {
    took_ms: f64,
    errors: bool,
    items: Vec<BulkItem>,
}

#[derive(Serialize)]
struct BulkItem {
    index: BulkItemInner,
}

#[derive(Serialize)]
struct BulkItemInner {
    _id: u64,
    status: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

// -- POST /_mpercolate (batch percolation; ES `_msearch`-shaped responses[])
#[derive(Deserialize)]
struct MPercolateBody {
    /// The batch of documents to percolate. Each entry is matched independently;
    /// `responses[i]` corresponds to `documents[i]`.
    documents: Option<Vec<DocBody>>,
    /// Per-request override of the server's broad-lane default. When set, controls
    /// whether class-C (broad) queries are evaluated for this batch.
    include_broad: Option<bool>,
    /// Include original query text in each hit (default: true).
    include_source: Option<bool>,
    /// Maximum hits to return per document (default: 1000).
    size: Option<usize>,
    /// Per-request timeout in milliseconds (default: 30000).
    timeout_ms: Option<u64>,
    /// Include the top-level broad-lane summary in the response (default: false).
    profile: Option<bool>,
}

#[derive(Serialize)]
struct MPercolateResponse {
    took_ms: f64,
    /// One entry per input document, in submission order.
    responses: Vec<PercolateItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    broad: Option<BroadSummary>,
}

#[derive(Serialize)]
struct PercolateItem {
    hits: SearchHits,
}

/// Top-level broad-lane summary for a `/_mpercolate` batch — surfaces the columnar
/// evaluator's amortization (see `MatchStats` / ADR-026). `broad_postings_scanned`
/// rising far slower than `broad_candidates` as `batch_size` grows IS the win.
#[derive(Serialize)]
struct BroadSummary {
    strategy: &'static str,
    batch_size: usize,
    broad_batches: u32,
    broad_postings_scanned: u32,
    broad_queries_evaluated: u32,
    broad_candidates: u32,
    total_matches: u32,
}

// -- GET /_stats
#[derive(Serialize)]
struct EngineStatsResponse {
    total_queries: usize,
    base_segments: usize,
    memtable_entries: usize,
    dict_features: usize,
    rejected_parse: u64,
    rejected_class_d: u64,
    class_counts: ClassCounts,
    segment_sizes: Vec<usize>,
    segment_holes: Vec<f64>,
    memory: MemoryStats,
}

#[derive(Serialize)]
struct ClassCounts {
    a: u64,
    b: u64,
    c: u64,
    d: u64,
}

#[derive(Serialize)]
// Field names are the serialized JSON keys (public API); the shared `_bytes`
// suffix is the contract, not an accident — don't rename it away.
#[allow(clippy::struct_field_names)]
struct MemoryStats {
    exact_bytes: usize,
    index_bytes: usize,
    filter_bytes: usize,
}

// -- GET /_cat/segments
/// Query string for the `_cat` endpoints. `?format=json` switches the default
/// text table to a JSON array (ES convention).
#[derive(Deserialize, Default)]
struct CatQuery {
    format: Option<String>,
}

/// One row of `GET /_cat/segments?format=json` — the JSON projection of an
/// engine [`SegmentInfo`]. Byte fields are raw integers (machine-readable); the
/// text table humanizes them instead.
#[derive(Serialize)]
struct SegmentRow {
    ordinal: usize,
    kind: &'static str,
    entries: usize,
    alive: usize,
    deleted: usize,
    holes_ratio: f64,
    vocab_epoch: u64,
    stale: bool,
    resident_bytes: usize,
    overhead_bytes: usize,
}

impl From<&SegmentInfo> for SegmentRow {
    fn from(s: &SegmentInfo) -> Self {
        Self {
            ordinal: s.ordinal,
            kind: s.kind.as_str(),
            entries: s.entries,
            alive: s.alive,
            deleted: s.deleted,
            holes_ratio: s.holes_ratio,
            vocab_epoch: s.vocab_epoch,
            stale: s.stale,
            resident_bytes: s.resident_bytes,
            overhead_bytes: s.overhead_bytes,
        }
    }
}

// -- GET /_health
#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    total_queries: usize,
    wal_healthy: bool,
    persistence_healthy: bool,
    skipped_segments: usize,
    stale_segments: usize,
}

// -- GET /
#[derive(Serialize)]
struct RootResponse {
    name: &'static str,
    version: &'static str,
    tagline: &'static str,
}

// -- Structured API errors
#[derive(Serialize, Debug)]
struct ApiError {
    error: ApiErrorBody,
    status: u16,
}

#[derive(Serialize, Debug)]
struct ApiErrorBody {
    #[serde(rename = "type")]
    error_type: String,
    reason: String,
}

impl ApiError {
    fn response(
        status: StatusCode,
        error_type: &str,
        reason: impl Into<String>,
    ) -> (StatusCode, Json<ApiError>) {
        let code = status.as_u16();
        (
            status,
            Json(ApiError {
                error: ApiErrorBody {
                    error_type: error_type.to_string(),
                    reason: reason.into(),
                },
                status: code,
            }),
        )
    }
}

// ---------------------------------------------------------------------------
// Request ID middleware
// ---------------------------------------------------------------------------

/// RAII guard for the in-flight request gauge: increments on construction and
/// decrements on drop, so every exit path of the request stays balanced.
struct InFlightGuard<'a>(&'a IntGauge);

impl<'a> InFlightGuard<'a> {
    fn new(gauge: &'a IntGauge) -> Self {
        gauge.inc();
        Self(gauge)
    }
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.dec();
    }
}

/// Adds a unique X-Request-Id header to every response, tracks the in-flight
/// request gauge, and includes the request ID in the tracing span for
/// correlation.
async fn request_id_middleware(
    State(state): State<Arc<AppState>>,
    request: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    let _in_flight = InFlightGuard::new(&state.prom.in_flight_requests);
    let request_id = uuid::Uuid::new_v4().to_string();
    let span = tracing::info_span!("request", request_id = %request_id);
    let _guard = span.enter();

    let mut response = next.run(request).await;
    if let Ok(val) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert("x-request-id", val);
    }
    response
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// PUT /_doc/{id} — register a single query.
#[instrument(skip(state, body), fields(query_id = id))]
async fn put_doc(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u64>,
    Json(body): Json<PutDocBody>,
) -> impl IntoResponse {
    let start = Instant::now();
    let result = {
        let mut engine = state.engine.lock();
        match engine.try_insert_live(&body.query, id, body.version) {
            Ok(reverse_rusty::segment::InsertOutcome::Inserted(_)) => {
                info!(query_id = id, "query registered");
                state
                    .prom
                    .http_requests_total
                    .with_label_values(&["put_doc", "201"])
                    .inc();
                (
                    StatusCode::CREATED,
                    Json(PutDocResponse {
                        _id: id,
                        result: "created",
                        error: None,
                    }),
                )
            }
            Ok(reverse_rusty::segment::InsertOutcome::RejectedClassD) => {
                warn!(query_id = id, "query rejected: cost class D");
                state
                    .prom
                    .http_requests_total
                    .with_label_values(&["put_doc", "400"])
                    .inc();
                (
                    StatusCode::BAD_REQUEST,
                    Json(PutDocResponse {
                        _id: id,
                        result: "rejected",
                        error: Some("query has no anchorable feature (cost class D)".into()),
                    }),
                )
            }
            Err(reverse_rusty::WriteError::Parse(e)) => {
                warn!(query_id = id, error = %e, "query parse error");
                state
                    .prom
                    .http_requests_total
                    .with_label_values(&["put_doc", "400"])
                    .inc();
                (
                    StatusCode::BAD_REQUEST,
                    Json(PutDocResponse {
                        _id: id,
                        result: "error",
                        error: Some(format!("parse error: {e}")),
                    }),
                )
            }
            Err(reverse_rusty::WriteError::Wal(e)) => {
                // Durability failure: the mutation was NOT applied. Never
                // acknowledge a write we couldn't log (see ADR-013). 503 tells
                // the client to retry — the engine state is unchanged.
                error!(query_id = id, error = %e, "WAL write failed, mutation rejected");
                state
                    .prom
                    .http_requests_total
                    .with_label_values(&["put_doc", "503"])
                    .inc();
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(PutDocResponse {
                        _id: id,
                        result: "error",
                        error: Some(format!("write-ahead log error: {e}")),
                    }),
                )
            }
        }
    };
    state.publish_snapshot();
    state
        .prom
        .http_request_duration
        .with_label_values(&["put_doc"])
        .observe(start.elapsed().as_secs_f64());
    result
}

/// GET /_doc/{id} — retrieve a stored query by logical ID.
#[instrument(skip(state), fields(query_id = id))]
async fn get_doc(State(state): State<Arc<AppState>>, Path(id): Path<u64>) -> impl IntoResponse {
    let start = Instant::now();
    let snap = state.snapshot.load();
    let result = if let Some(query_text) = snap.get_query_source(id) {
        state
            .prom
            .http_requests_total
            .with_label_values(&["get_doc", "200"])
            .inc();
        (
            StatusCode::OK,
            Json(GetDocResponse {
                _id: id,
                found: true,
                _source: Some(HitSource { query: query_text }),
            }),
        )
    } else {
        state
            .prom
            .http_requests_total
            .with_label_values(&["get_doc", "404"])
            .inc();
        (
            StatusCode::NOT_FOUND,
            Json(GetDocResponse {
                _id: id,
                found: false,
                _source: None,
            }),
        )
    };
    state
        .prom
        .http_request_duration
        .with_label_values(&["get_doc"])
        .observe(start.elapsed().as_secs_f64());
    result
}

/// DELETE /_doc/{id} — remove a stored query by logical ID.
#[instrument(skip(state), fields(query_id = id))]
async fn delete_doc(State(state): State<Arc<AppState>>, Path(id): Path<u64>) -> impl IntoResponse {
    let start = Instant::now();
    let deleted = {
        let mut engine = state.engine.lock();
        engine.delete_by_logical_id(id)
    };
    state.publish_snapshot();
    state
        .prom
        .http_request_duration
        .with_label_values(&["delete_doc"])
        .observe(start.elapsed().as_secs_f64());
    match deleted {
        Ok(n) if n > 0 => {
            info!(query_id = id, deleted = n, "query deleted");
            state
                .prom
                .http_requests_total
                .with_label_values(&["delete_doc", "200"])
                .inc();
            (
                StatusCode::OK,
                Json(DeleteDocResponse {
                    _id: id,
                    result: "deleted",
                    deleted_count: Some(n as u64),
                    error: None,
                }),
            )
        }
        Ok(_) => {
            state
                .prom
                .http_requests_total
                .with_label_values(&["delete_doc", "404"])
                .inc();
            (
                StatusCode::NOT_FOUND,
                Json(DeleteDocResponse {
                    _id: id,
                    result: "not_found",
                    deleted_count: None,
                    error: None,
                }),
            )
        }
        Err(e) => {
            // Tombstone WAL append failed: the delete was NOT applied. Reject
            // rather than acknowledge a delete we couldn't log (see ADR-013).
            error!(query_id = id, error = %e, "WAL write failed, delete rejected");
            state
                .prom
                .http_requests_total
                .with_label_values(&["delete_doc", "503"])
                .inc();
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(DeleteDocResponse {
                    _id: id,
                    result: "error",
                    deleted_count: None,
                    error: Some(format!("write-ahead log error: {e}")),
                }),
            )
        }
    }
}

/// POST /_search — percolate one or more titles.
#[instrument(skip_all)]
async fn search(
    State(state): State<Arc<AppState>>,
    Json(body): Json<SearchBody>,
) -> Result<Json<SearchResponse>, (StatusCode, Json<ApiError>)> {
    let start = Instant::now();
    let include_broad = state.include_broad;
    let include_source = body.include_source.unwrap_or(true);
    let include_explain = body.explain.unwrap_or(false);
    let include_profile = body.profile.unwrap_or(false);
    let timeout = tokio::time::Duration::from_millis(body.timeout_ms.unwrap_or(30_000));
    let page_size = body.size.unwrap_or(1000);
    let page_from = body.from.unwrap_or(0);

    let response = match (body.document, body.documents) {
        // Single document percolation.
        (Some(doc), _) => {
            let title = doc.title;
            let title_for_explain = if include_explain {
                Some(title.clone())
            } else {
                None
            };
            let prom = state.prom.clone();
            let snap = Arc::clone(&state.snapshot.load());
            let state_inner = Arc::clone(&state);

            let search_fut = tokio::task::spawn_blocking(move || {
                state_inner.pool.install(|| {
                    SCRATCH.with(|cell| {
                        let mut scratch = cell.borrow_mut();
                        let mut out = Vec::new();
                        let stats = snap.match_title(&title, &mut scratch, &mut out, include_broad);
                        (out, stats)
                    })
                })
            });

            let (ids, stats) = match tokio::time::timeout(timeout, search_fut).await {
                Ok(Ok(result)) => result,
                Ok(Err(e)) => {
                    eprintln!("search task panicked: {e}");
                    state
                        .prom
                        .http_requests_total
                        .with_label_values(&["search", "500"])
                        .inc();
                    return Err(ApiError::response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "search_error",
                        "internal search task failed",
                    ));
                }
                Err(_) => {
                    state
                        .prom
                        .http_requests_total
                        .with_label_values(&["search", "408"])
                        .inc();
                    return Err(ApiError::response(
                        StatusCode::REQUEST_TIMEOUT,
                        "timeout",
                        format!("search timed out after {}ms", timeout.as_millis()),
                    ));
                }
            };

            prom.match_candidates_per_title
                .observe(f64::from(stats.unique_candidates));
            prom.match_results_per_title.observe(ids.len() as f64);

            let took_ms = start.elapsed().as_secs_f64() * 1000.0;
            let total = ids.len();
            let paged_ids: Vec<u64> = ids.into_iter().skip(page_from).take(page_size).collect();
            let snap = state.snapshot.load();
            let hits = paged_ids
                .iter()
                .map(|&id| {
                    let source = if include_source {
                        snap.get_query_source(id).map(|q| HitSource { query: q })
                    } else {
                        None
                    };
                    let explanation = title_for_explain
                        .as_deref()
                        .and_then(|t| snap.explain_hit(id, t));
                    SearchHitItem {
                        _id: id,
                        _source: source,
                        _explanation: explanation,
                    }
                })
                .collect();
            info!(
                titles = 1,
                matches = total,
                took_ms = format!("{:.2}", took_ms),
                "search complete"
            );
            SearchResponse {
                took_ms,
                hits: SearchHits { total, hits },
                slots: None,
                profile: if include_profile {
                    Some(stats.into())
                } else {
                    None
                },
            }
        }

        // Multi-document percolation.
        (None, Some(docs)) => {
            let num_docs = docs.len();
            let titles: Vec<String> = docs.into_iter().map(|d| d.title).collect();
            let prom = state.prom.clone();
            let snap = Arc::clone(&state.snapshot.load());
            let state_inner = Arc::clone(&state);

            let search_fut = tokio::task::spawn_blocking(move || {
                state_inner
                    .pool
                    .install(|| snap.match_titles_par(&titles, include_broad))
            });

            let results = match tokio::time::timeout(timeout, search_fut).await {
                Ok(Ok(result)) => result,
                Ok(Err(e)) => {
                    eprintln!("search task panicked: {e}");
                    state
                        .prom
                        .http_requests_total
                        .with_label_values(&["search", "500"])
                        .inc();
                    return Err(ApiError::response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "search_error",
                        "internal search task failed",
                    ));
                }
                Err(_) => {
                    state
                        .prom
                        .http_requests_total
                        .with_label_values(&["search", "408"])
                        .inc();
                    return Err(ApiError::response(
                        StatusCode::REQUEST_TIMEOUT,
                        "timeout",
                        format!("search timed out after {}ms", timeout.as_millis()),
                    ));
                }
            };

            let took_ms = start.elapsed().as_secs_f64() * 1000.0;
            let mut all_ids = Vec::new();
            let mut slot_data: Vec<(usize, Vec<u64>, StatsResponse)> = Vec::new();
            for (slot, ids, stats) in results {
                prom.match_candidates_per_title
                    .observe(f64::from(stats.unique_candidates));
                prom.match_results_per_title.observe(ids.len() as f64);

                all_ids.extend_from_slice(&ids);
                slot_data.push((slot, ids, stats.into()));
            }
            all_ids.sort_unstable();
            all_ids.dedup();

            let total = all_ids.len();
            let paged_ids: Vec<u64> = all_ids
                .into_iter()
                .skip(page_from)
                .take(page_size)
                .collect();

            let snap = state.snapshot.load();
            let make_hit = |id: u64| {
                let source = if include_source {
                    snap.get_query_source(id).map(|q| HitSource { query: q })
                } else {
                    None
                };
                SearchHitItem {
                    _id: id,
                    _source: source,
                    _explanation: None,
                }
            };
            let hits: Vec<_> = paged_ids.iter().map(|&id| make_hit(id)).collect();
            let slots: Vec<_> = slot_data
                .into_iter()
                .map(|(slot, ids, stats)| {
                    let slot_hits = ids.iter().map(|&id| make_hit(id)).collect();
                    SlotHit {
                        slot,
                        total: ids.len(),
                        hits: slot_hits,
                        stats,
                    }
                })
                .collect();

            info!(
                titles = num_docs,
                matches = total,
                took_ms = format!("{:.2}", took_ms),
                "search complete"
            );
            SearchResponse {
                took_ms,
                hits: SearchHits { total, hits },
                slots: Some(slots),
                profile: None,
            }
        }

        (None, None) => {
            state
                .prom
                .http_requests_total
                .with_label_values(&["search", "400"])
                .inc();
            return Err(ApiError::response(
                StatusCode::BAD_REQUEST,
                "validation_error",
                "request must include 'document' or 'documents' field",
            ));
        }
    };

    let threshold = state.slow_query_threshold_ms;
    if threshold > 0 && response.took_ms >= threshold as f64 {
        state.prom.slow_queries_total.inc();
        warn!(
            took_ms = format!("{:.2}", response.took_ms),
            threshold_ms = threshold,
            matches = response.hits.total,
            titles = response.slots.as_ref().map_or(1, std::vec::Vec::len),
            "slow query"
        );
    }

    state
        .prom
        .http_requests_total
        .with_label_values(&["search", "200"])
        .inc();
    state
        .prom
        .http_request_duration
        .with_label_values(&["search"])
        .observe(start.elapsed().as_secs_f64());
    Ok(Json(response))
}

/// POST /_mpercolate — batch percolation (ES `_msearch`-shaped).
///
/// Percolates a batch of documents in one request, evaluating the broad lane
/// ONCE per title-batch (columnar; ADR-026) instead of once per document, so the
/// broad-posting scan amortizes across the batch. Returns a `responses[]`
/// envelope, one entry per input document in submission order. The broad lane is
/// opt-in per request (`include_broad`, falling back to the server default).
///
/// This is the throughput path; `/_search` remains the rich path. Because the
/// broad lane is amortized per batch, `/_mpercolate` does not produce per-document
/// candidate/posting stats — only an optional top-level broad summary (`profile`).
#[instrument(skip_all)]
async fn mpercolate(
    State(state): State<Arc<AppState>>,
    Json(body): Json<MPercolateBody>,
) -> Result<Json<MPercolateResponse>, (StatusCode, Json<ApiError>)> {
    let start = Instant::now();

    let Some(docs) = body.documents else {
        state
            .prom
            .http_requests_total
            .with_label_values(&["mpercolate", "400"])
            .inc();
        return Err(ApiError::response(
            StatusCode::BAD_REQUEST,
            "validation_error",
            "request must include 'documents' (an array of {\"title\": ...})",
        ));
    };

    let include_broad = body.include_broad.unwrap_or(state.include_broad);
    let include_source = body.include_source.unwrap_or(true);
    let page_size = body.size.unwrap_or(1000);
    let include_profile = body.profile.unwrap_or(false);
    let timeout = tokio::time::Duration::from_millis(body.timeout_ms.unwrap_or(30_000));

    // Empty batch: a valid no-op — return an empty responses[] without scheduling
    // any work.
    if docs.is_empty() {
        state
            .prom
            .http_requests_total
            .with_label_values(&["mpercolate", "200"])
            .inc();
        return Ok(Json(MPercolateResponse {
            took_ms: start.elapsed().as_secs_f64() * 1000.0,
            responses: Vec::new(),
            broad: None,
        }));
    }

    let num_docs = docs.len();

    // Read the live broad-lane config from the snapshot (ADR-026 dynamic knobs):
    // batch size, columnar-vs-inline kill-switch, pure-anchor materialization, and
    // the max batch size that bounds per-request work.
    let snap = Arc::clone(&state.snapshot.load());
    let cfg = snap.config();
    if num_docs > cfg.max_percolate_batch {
        state
            .prom
            .http_requests_total
            .with_label_values(&["mpercolate", "400"])
            .inc();
        return Err(ApiError::response(
            StatusCode::BAD_REQUEST,
            "validation_error",
            format!(
                "batch of {num_docs} documents exceeds max_percolate_batch ({})",
                cfg.max_percolate_batch
            ),
        ));
    }
    let opts = BatchMatchOptions {
        include_broad,
        broad_batch_size: cfg.broad_batch_size,
        broad_strategy: if cfg.broad_columnar {
            BroadStrategy::Columnar
        } else {
            BroadStrategy::Inline
        },
        broad_materialize: cfg.broad_materialize,
    };

    let titles: Vec<String> = docs.into_iter().map(|d| d.title).collect();
    let state_inner = Arc::clone(&state);
    let search_fut = tokio::task::spawn_blocking(move || {
        state_inner
            .pool
            .install(|| snap.match_titles_batch_with_stats(&titles, opts))
    });

    let (results, stats) = match tokio::time::timeout(timeout, search_fut).await {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => {
            eprintln!("mpercolate task panicked: {e}");
            state
                .prom
                .http_requests_total
                .with_label_values(&["mpercolate", "500"])
                .inc();
            return Err(ApiError::response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "search_error",
                "internal percolate task failed",
            ));
        }
        Err(_) => {
            state
                .prom
                .http_requests_total
                .with_label_values(&["mpercolate", "408"])
                .inc();
            return Err(ApiError::response(
                StatusCode::REQUEST_TIMEOUT,
                "timeout",
                format!("mpercolate timed out after {}ms", timeout.as_millis()),
            ));
        }
    };

    // Broad-lane meters (cumulative across requests).
    state
        .prom
        .broad_batches_total
        .inc_by(u64::from(stats.broad_batches));
    state
        .prom
        .broad_postings_scanned_total
        .inc_by(u64::from(stats.broad_postings_scanned));
    state
        .prom
        .broad_queries_evaluated_total
        .inc_by(u64::from(stats.broad_queries_evaluated));
    state
        .prom
        .broad_candidates_total
        .inc_by(u64::from(stats.broad_candidates));

    // Reassemble per-document results in submission order (`results` is
    // (global_index, ids) with index in 0..num_docs).
    let mut per_doc: Vec<Vec<u64>> = vec![Vec::new(); num_docs];
    for (idx, ids) in results {
        if let Some(slot) = per_doc.get_mut(idx) {
            *slot = ids;
        }
    }

    let snap = state.snapshot.load();
    let responses: Vec<PercolateItem> = per_doc
        .into_iter()
        .map(|ids| {
            let total = ids.len();
            let hits = ids
                .into_iter()
                .take(page_size)
                .map(|id| {
                    let source = if include_source {
                        snap.get_query_source(id).map(|q| HitSource { query: q })
                    } else {
                        None
                    };
                    SearchHitItem {
                        _id: id,
                        _source: source,
                        _explanation: None,
                    }
                })
                .collect();
            PercolateItem {
                hits: SearchHits { total, hits },
            }
        })
        .collect();

    let took_ms = start.elapsed().as_secs_f64() * 1000.0;
    // Build the summary lazily (only when requested) — `then_some` would build it
    // even when `profile` is false.
    let broad = if include_profile {
        Some(BroadSummary {
            strategy: if matches!(opts.broad_strategy, BroadStrategy::Columnar) {
                "columnar"
            } else {
                "inline"
            },
            batch_size: opts.broad_batch_size,
            broad_batches: stats.broad_batches,
            broad_postings_scanned: stats.broad_postings_scanned,
            broad_queries_evaluated: stats.broad_queries_evaluated,
            broad_candidates: stats.broad_candidates,
            total_matches: stats.matches,
        })
    } else {
        None
    };

    info!(
        titles = num_docs,
        matches = stats.matches,
        include_broad,
        took_ms = format!("{:.2}", took_ms),
        "mpercolate complete"
    );

    state
        .prom
        .http_requests_total
        .with_label_values(&["mpercolate", "200"])
        .inc();
    state
        .prom
        .http_request_duration
        .with_label_values(&["mpercolate"])
        .observe(start.elapsed().as_secs_f64());

    Ok(Json(MPercolateResponse {
        took_ms,
        responses,
        broad,
    }))
}

/// POST /_bulk — NDJSON bulk ingest.
///
/// Format (ES-compatible):
///   {"index": {"_id": 123}}
///   {"query": "pokemon base set"}
///   {"index": {"_id": 456}}
///   {"query": "charizard holo"}
#[instrument(skip_all)]
async fn bulk_ingest(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: String,
) -> impl IntoResponse {
    let start = Instant::now();

    if let Some(ct) = headers.get("content-type") {
        if let Ok(ct_str) = ct.to_str() {
            let ct_lower = ct_str.to_ascii_lowercase();
            if !ct_lower.starts_with("application/json")
                && !ct_lower.starts_with("application/x-ndjson")
            {
                state
                    .prom
                    .http_requests_total
                    .with_label_values(&["bulk", "415"])
                    .inc();
                return ApiError::response(
                    StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    "unsupported_media_type",
                    "Content-Type must be application/json or application/x-ndjson",
                )
                .into_response();
            }
        }
    }

    // Parse NDJSON action/source pairs.
    let lines: Vec<&str> = body.lines().filter(|l| !l.trim().is_empty()).collect();
    let mut pairs: Vec<(u64, String)> = Vec::new();
    // For each entry in `pairs`, the index of its provisional item in `items`,
    // so the engine's per-item outcome can be mapped back to the right slot.
    let mut pair_item_idx: Vec<usize> = Vec::new();
    let mut items: Vec<BulkItem> = Vec::new();
    let mut has_errors = false;

    let mut i = 0;
    while i < lines.len() {
        let action_line = lines[i];
        i += 1;

        // Parse action: {"index": {"_id": N}} or just {"_id": N, ...}
        let action: serde_json::Value = match serde_json::from_str(action_line) {
            Ok(v) => v,
            Err(e) => {
                has_errors = true;
                items.push(BulkItem {
                    index: BulkItemInner {
                        _id: 0,
                        status: 400,
                        error: Some(format!("invalid action JSON: {e}")),
                    },
                });
                // Try to skip the source line too.
                if i < lines.len() {
                    i += 1;
                }
                continue;
            }
        };

        let id = extract_bulk_id(&action);

        // Next line is the source document.
        if i >= lines.len() {
            has_errors = true;
            items.push(BulkItem {
                index: BulkItemInner {
                    _id: id.unwrap_or(0),
                    status: 400,
                    error: Some("missing source line after action".into()),
                },
            });
            break;
        }

        let source_line = lines[i];
        i += 1;

        let Some(id) = id else {
            has_errors = true;
            items.push(BulkItem {
                index: BulkItemInner {
                    _id: 0,
                    status: 400,
                    error: Some("could not extract _id from action".into()),
                },
            });
            continue;
        };

        let source: serde_json::Value = match serde_json::from_str(source_line) {
            Ok(v) => v,
            Err(e) => {
                has_errors = true;
                items.push(BulkItem {
                    index: BulkItemInner {
                        _id: id,
                        status: 400,
                        error: Some(format!("invalid source JSON: {e}")),
                    },
                });
                continue;
            }
        };

        let query = if let Some(q) = source.get("query").and_then(|v| v.as_str()) {
            q.to_string()
        } else {
            has_errors = true;
            items.push(BulkItem {
                index: BulkItemInner {
                    _id: id,
                    status: 400,
                    error: Some("missing or non-string 'query' field".into()),
                },
            });
            continue;
        };

        pairs.push((id, query));
        // Provisional success; the engine outcome (below) may downgrade this
        // item to a 400 once the batch is compiled.
        pair_item_idx.push(items.len());
        items.push(BulkItem {
            index: BulkItemInner {
                _id: id,
                status: 201,
                error: None,
            },
        });
    }

    // Ingest the valid pairs.
    if !pairs.is_empty() {
        let result = {
            let mut engine = state.engine.lock();
            engine.try_bulk_ingest_detailed(&pairs)
        };

        let (report, item_status) = match result {
            Ok(outcome) => {
                state.publish_snapshot();
                outcome
            }
            Err(e) => {
                // Durability failure: the batch was NOT committed (all-or-nothing,
                // ADR-017). 503 tells the client to retry — engine state is
                // unchanged, so no snapshot republish is needed.
                error!(error = %e, "bulk ingest persistence failed, batch rolled back");
                state
                    .prom
                    .http_requests_total
                    .with_label_values(&["bulk", "503"])
                    .inc();
                return ApiError::response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "persistence_unavailable",
                    format!("bulk ingest could not be durably persisted: {e}"),
                )
                .into_response();
            }
        };

        // Map each engine outcome back onto its provisional item. `item_status[k]`
        // describes `pairs[k]`, whose response slot is `pair_item_idx[k]`. Parse
        // and class-D rejections become per-item 400s (mirroring PUT /_doc), so a
        // caller can see exactly which queries were dropped and why.
        for (status, &slot) in item_status.iter().zip(pair_item_idx.iter()) {
            match status {
                IngestItemStatus::Ingested => {}
                IngestItemStatus::RejectedParse(e) => {
                    items[slot].index.status = 400;
                    items[slot].index.error = Some(format!("parse error: {e}"));
                    has_errors = true;
                }
                IngestItemStatus::RejectedClassD => {
                    items[slot].index.status = 400;
                    items[slot].index.error =
                        Some("query has no anchorable feature (cost class D)".into());
                    has_errors = true;
                }
            }
        }

        info!(
            ingested = report.ingested,
            rejected_parse = report.rejected_parse,
            rejected_class_d = report.rejected_class_d,
            "bulk ingest complete"
        );
    }

    let took_ms = start.elapsed().as_secs_f64() * 1000.0;
    state
        .prom
        .http_requests_total
        .with_label_values(&["bulk", "200"])
        .inc();
    state
        .prom
        .http_request_duration
        .with_label_values(&["bulk"])
        .observe(start.elapsed().as_secs_f64());
    Json(BulkResponse {
        took_ms,
        errors: has_errors,
        items,
    })
    .into_response()
}

/// Extract _id from ES-style action line.
/// Accepts: {"index": {"_id": 123}} or {"_id": 123}
fn extract_bulk_id(action: &serde_json::Value) -> Option<u64> {
    // ES style: {"index": {"_id": N}}
    if let Some(inner) = action.get("index") {
        if let Some(id) = inner.get("_id").and_then(serde_json::Value::as_u64) {
            return Some(id);
        }
    }
    // Flat style: {"_id": N}
    if let Some(id) = action.get("_id").and_then(serde_json::Value::as_u64) {
        return Some(id);
    }
    // Also try "id" without underscore.
    action.get("id").and_then(serde_json::Value::as_u64)
}

/// POST /_flush
#[instrument(skip_all)]
async fn flush(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let start = Instant::now();
    let metrics = {
        let mut engine = state.engine.lock();
        engine.flush();
        engine.metrics()
    };
    state.publish_snapshot();
    info!(
        total_queries = metrics.total_queries,
        base_segments = metrics.base_segments,
        "flush complete"
    );
    state
        .prom
        .http_requests_total
        .with_label_values(&["flush", "200"])
        .inc();
    state
        .prom
        .http_request_duration
        .with_label_values(&["flush"])
        .observe(start.elapsed().as_secs_f64());
    Json(FlushResponse {
        took_ms: start.elapsed().as_secs_f64() * 1000.0,
        acknowledged: true,
        total_queries: metrics.total_queries,
        base_segments: metrics.base_segments,
    })
}

/// POST /_compact
#[instrument(skip_all)]
async fn compact(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let start = Instant::now();
    let report = {
        let mut engine = state.engine.lock();
        engine.maybe_compact()
    };
    state.publish_snapshot();
    state
        .prom
        .http_requests_total
        .with_label_values(&["compact", "200"])
        .inc();
    state
        .prom
        .http_request_duration
        .with_label_values(&["compact"])
        .observe(start.elapsed().as_secs_f64());
    if let Some(r) = report {
        info!(
            segments_merged = r.segments_merged,
            entries_before = r.entries_before,
            entries_after = r.entries_after,
            tombstones_reclaimed = r.tombstones_reclaimed,
            "compaction complete"
        );
        Json(CompactResponse {
            took_ms: start.elapsed().as_secs_f64() * 1000.0,
            acknowledged: true,
            segments_merged: Some(r.segments_merged),
            entries_before: Some(r.entries_before),
            entries_after: Some(r.entries_after),
            tombstones_reclaimed: Some(r.tombstones_reclaimed),
            message: None,
        })
    } else {
        info!("compaction skipped: not needed");
        Json(CompactResponse {
            took_ms: start.elapsed().as_secs_f64() * 1000.0,
            acknowledged: true,
            segments_merged: None,
            entries_before: None,
            entries_after: None,
            tombstones_reclaimed: None,
            message: Some("no compaction needed"),
        })
    }
}

/// GET /_stats — JSON metrics snapshot.
async fn stats(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let snap = state.snapshot.load();
    let m = snap.metrics();
    let cc = snap.class_counts();
    Json(EngineStatsResponse {
        total_queries: m.total_queries,
        base_segments: m.base_segments,
        memtable_entries: m.memtable_entries,
        dict_features: m.dict_features,
        rejected_parse: m.rejected_parse,
        rejected_class_d: m.rejected_class_d,
        class_counts: ClassCounts {
            a: cc[0],
            b: cc[1],
            c: cc[2],
            d: cc[3],
        },
        segment_sizes: m.segment_sizes,
        segment_holes: m.segment_holes,
        memory: MemoryStats {
            exact_bytes: m.exact_bytes,
            index_bytes: m.index_bytes,
            filter_bytes: m.filter_bytes,
        },
    })
}

/// GET /_cat/stats — human-readable metrics.
async fn cat_stats(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let snap = state.snapshot.load();
    let m = snap.metrics();
    let cc = snap.class_counts();
    let total_mem = m.exact_bytes + m.index_bytes + m.filter_bytes;

    let mut out = String::new();
    out.push_str(&format!("queries          {}\n", m.total_queries));
    out.push_str(&format!(
        "segments         {} (+ memtable: {})\n",
        m.base_segments, m.memtable_entries
    ));
    out.push_str(&format!("features         {}\n", m.dict_features));
    out.push_str(&format!(
        "class A/B/C/D    {} / {} / {} / {}\n",
        cc[0], cc[1], cc[2], cc[3]
    ));
    out.push_str(&format!("rejected parse   {}\n", m.rejected_parse));
    out.push_str(&format!("rejected classD  {}\n", m.rejected_class_d));
    out.push_str(&format!(
        "memory           {} bytes (~{:.1} MB)\n",
        total_mem,
        total_mem as f64 / 1_048_576.0
    ));
    let cfg = snap.config();
    out.push_str(&format!(
        "broad lane       {} (batch_size {}, materialize {}, max_batch {})\n",
        if cfg.broad_columnar {
            "columnar"
        } else {
            "inline"
        },
        cfg.broad_batch_size,
        cfg.broad_materialize,
        cfg.max_percolate_batch,
    ));

    if !m.segment_sizes.is_empty() {
        out.push_str("\nsegment  entries  holes\n");
        for (i, (&sz, &h)) in m
            .segment_sizes
            .iter()
            .zip(m.segment_holes.iter())
            .enumerate()
        {
            out.push_str(&format!("{:<8} {:<8} {:.2}%\n", i, sz, h * 100.0));
        }
    }

    (
        StatusCode::OK,
        [("content-type", "text/plain; charset=utf-8")],
        out,
    )
}

/// GET /_cat/segments — per-segment detail of the LSM layout (one row per base
/// segment, oldest first, then the memtable). Defaults to a human-readable text
/// table like the other `_cat` endpoints; `?format=json` returns a JSON array of
/// row objects (ES `_cat?format=json` convention). Reads the lock-free snapshot.
///
/// This exposes the segment-level detail the aggregate `/_stats` flattens: which
/// segments carry compaction pressure (`holes`), how memory is distributed
/// (resident vs off-heap `mmap`), and which segments are stale against the
/// current vocab epoch.
async fn cat_segments(
    State(state): State<Arc<AppState>>,
    Query(q): Query<CatQuery>,
) -> impl IntoResponse {
    let infos = state.snapshot.load().segment_infos();
    if q.format.as_deref() == Some("json") {
        let rows: Vec<SegmentRow> = infos.iter().map(SegmentRow::from).collect();
        Json(rows).into_response()
    } else {
        (
            StatusCode::OK,
            [("content-type", "text/plain; charset=utf-8")],
            render_segments_table(&infos),
        )
            .into_response()
    }
}

/// Render the `_cat/segments` text table: a header row plus one row per segment.
/// Numbers are right-aligned, byte counts humanized; the memtable is the final
/// row (kind `memtable`). Pure so it is unit-tested without the HTTP layer.
fn render_segments_table(infos: &[SegmentInfo]) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "{:<7} {:<8} {:>9} {:>9} {:>9} {:>7} {:>6} {:>5} {:>12} {:>12}\n",
        "segment",
        "kind",
        "entries",
        "alive",
        "deleted",
        "holes",
        "epoch",
        "stale",
        "resident",
        "overhead",
    ));
    for s in infos {
        out.push_str(&format!(
            "{:<7} {:<8} {:>9} {:>9} {:>9} {:>6.2}% {:>6} {:>5} {:>12} {:>12}\n",
            s.ordinal,
            s.kind.as_str(),
            s.entries,
            s.alive,
            s.deleted,
            s.holes_ratio * 100.0,
            s.vocab_epoch,
            if s.stale { "yes" } else { "no" },
            fmt_bytes(s.resident_bytes),
            fmt_bytes(s.overhead_bytes),
        ));
    }
    out
}

/// Humanize a byte count for the `_cat` text tables (binary units, 2 dp).
/// JSON callers get the raw integer instead (see [`SegmentRow`]).
fn fmt_bytes(n: usize) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    let f = n as f64;
    if f >= GB {
        format!("{:.2} GB", f / GB)
    } else if f >= MB {
        format!("{:.2} MB", f / MB)
    } else if f >= KB {
        format!("{:.2} KB", f / KB)
    } else {
        format!("{n} B")
    }
}

/// GET /_health
async fn health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let snap = state.snapshot.load();
    let total = snap.num_queries();
    let wal_healthy = snap.wal_healthy();
    let persistence_healthy = snap.persistence_healthy();
    let skipped_segments = snap.skipped_segments();
    let stale_segments = snap.stale_segment_count();
    let status = if !wal_healthy || !persistence_healthy {
        "red"
    } else if skipped_segments > 0 || stale_segments > 0 {
        "yellow"
    } else {
        "green"
    };
    Json(HealthResponse {
        status,
        total_queries: total,
        wal_healthy,
        persistence_healthy,
        skipped_segments,
        stale_segments,
    })
}

/// GET / — API root.
async fn api_root() -> impl IntoResponse {
    Json(RootResponse {
        name: "reverse-rusty",
        version: env!("CARGO_PKG_VERSION"),
        tagline: "you know, for matching",
    })
}

/// GET /_metrics — Prometheus text exposition format.
///
/// On each scrape, refreshes gauge metrics from an EngineMetrics snapshot,
/// then encodes all registered metrics.
async fn prometheus_metrics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    // Refresh gauges from current snapshot state.
    {
        let snap = state.snapshot.load();
        let m = snap.metrics();
        state.prom.refresh_gauges(&m);
    }

    let encoder = TextEncoder::new();
    let metric_families = state.prom.registry.gather();
    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
        error!(error = %e, "failed to encode prometheus metrics");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            [("content-type", "text/plain; charset=utf-8")],
            Vec::new(),
        );
    }

    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        buffer,
    )
}

// ---------------------------------------------------------------------------
// Vocabulary management
// ---------------------------------------------------------------------------

/// GET /_vocab — return the current vocabulary as JSON. Reads the lock-free
/// `ArcSwap` snapshot (ADR-016) rather than locking the engine, so vocab reads
/// never block behind a writer — consistent with `/_search` and the other read
/// endpoints.
async fn get_vocab(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let snap = state.snapshot.load();
    let vocab = snap.vocab().cloned().unwrap_or_default();
    Json(vocab)
}

/// PUT /_vocab — replace the vocabulary. Existing compiled queries become
/// stale; the caller should reingest for consistent matching.
async fn put_vocab(
    State(state): State<Arc<AppState>>,
    Json(vocab): Json<reverse_rusty::vocab::Vocab>,
) -> impl IntoResponse {
    let result = {
        let mut engine = state.engine.lock();
        match engine.set_vocab(vocab) {
            Ok(_) => {
                // Recompile every stored query under the new normalizer so the
                // change takes effect with zero false negatives — under the same
                // lock and BEFORE the snapshot is published, so readers never see
                // the new normalizer against not-yet-recompiled segments.
                let recompiled = engine.recompile_stale_segments();
                (
                    StatusCode::OK,
                    Json(PutVocabResponse {
                        acknowledged: true,
                        recompiled,
                    }),
                )
                    .into_response()
            }
            Err(e) => ApiError::response(StatusCode::BAD_REQUEST, "vocab_error", e.to_string())
                .into_response(),
        }
    };
    state.publish_snapshot();
    result
}

#[derive(Deserialize)]
struct LearnRequest {
    queries: Vec<(u64, String)>,
    #[serde(default = "default_min_count")]
    min_count: usize,
}

fn default_min_count() -> usize {
    2
}

/// POST /_vocab/learn — learn synonyms from raw query text. Returns the
/// learned vocabulary without applying it. The caller can review, edit,
/// and then PUT /_vocab to apply.
async fn learn_vocab(Json(req): Json<LearnRequest>) -> impl IntoResponse {
    let vocab = reverse_rusty::vocab::learn_from_queries(&req.queries, req.min_count);
    Json(vocab)
}

// ---------------------------------------------------------------------------
// Settings management (ES-style /_settings)
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
struct SettingsQuery {
    /// When true, `GET /_settings` also returns the default settings (ES-style).
    #[serde(default)]
    include_defaults: bool,
}

#[derive(Serialize)]
struct GetSettingsResponse {
    settings: EngineConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    defaults: Option<EngineConfig>,
}

#[derive(Serialize)]
struct PutSettingsResponse {
    acknowledged: bool,
    /// Whether the change survives a restart. Currently always `false`: settings
    /// updates are in-memory only (the startup CLI flags are the durable source).
    /// Surfaced explicitly so clients aren't surprised after a restart.
    persistent: bool,
    settings: EngineConfig,
}

/// GET /_settings — return the live engine settings as JSON. Reads the lock-free
/// snapshot (ADR-016). `?include_defaults=true` also returns the defaults, like
/// Elasticsearch's `GET /_cluster/settings?include_defaults`.
async fn get_settings(
    State(state): State<Arc<AppState>>,
    Query(q): Query<SettingsQuery>,
) -> impl IntoResponse {
    let settings = state.snapshot.load().config().clone();
    let defaults = q.include_defaults.then(EngineConfig::default);
    Json(GetSettingsResponse { settings, defaults })
}

/// PUT /_settings — update dynamic engine settings at runtime. The body is a flat
/// JSON object of setting keys to new values, e.g. `{"max_segments": 16}`.
/// All-or-nothing: if any key is unknown, non-dynamic, the wrong type, or would
/// produce an invalid config, nothing changes and the request is rejected with an
/// ES-style reason. Changes are in-memory (not persisted across restart).
async fn put_settings(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(patch) = body.as_object() else {
        return ApiError::response(
            StatusCode::BAD_REQUEST,
            "settings_error",
            "request body must be a JSON object of settings",
        )
        .into_response();
    };
    if patch.is_empty() {
        return ApiError::response(
            StatusCode::BAD_REQUEST,
            "settings_error",
            "no settings provided",
        )
        .into_response();
    }

    let updated = {
        let mut engine = state.engine.lock();
        match apply_settings_patch(engine.config().clone(), patch) {
            Ok(cfg) => {
                engine.set_config(cfg.clone());
                cfg
            }
            Err(problems) => {
                return ApiError::response(
                    StatusCode::BAD_REQUEST,
                    "settings_error",
                    problems.join("; "),
                )
                .into_response();
            }
        }
    };
    // Republish so the lock-free snapshot (and GET /_settings) reflects the change.
    state.publish_snapshot();

    Json(PutSettingsResponse {
        acknowledged: true,
        persistent: false,
        settings: updated,
    })
    .into_response()
}

/// Apply a flat settings patch to `cfg`, enforcing the dynamic/static split, key
/// validity, value types, and the engine's own `validate()` ranges. Returns the
/// updated config, or every problem found (all keys are checked, so the caller
/// sees all errors at once — and on any error nothing is applied). Pure and
/// side-effect-free, so it is unit-tested directly without the HTTP layer.
fn apply_settings_patch(
    mut cfg: EngineConfig,
    patch: &serde_json::Map<String, serde_json::Value>,
) -> Result<EngineConfig, Vec<String>> {
    let mut errors = Vec::new();
    for (key, val) in patch {
        match key.as_str() {
            // ---- dynamic knobs (runtime-tunable) ----
            "max_segments" => set_usize(&mut cfg.max_segments, key, val, &mut errors),
            "memtable_flush_threshold" => {
                set_usize(&mut cfg.memtable_flush_threshold, key, val, &mut errors);
            }
            "max_query_length" => set_usize(&mut cfg.max_query_length, key, val, &mut errors),
            "max_query_clauses" => set_usize(&mut cfg.max_query_clauses, key, val, &mut errors),
            "max_anyof_group_size" => {
                set_usize(&mut cfg.max_anyof_group_size, key, val, &mut errors);
            }
            "holes_ratio_threshold" => {
                set_f64(&mut cfg.holes_ratio_threshold, key, val, &mut errors);
            }
            "compaction_fixed_cost" => {
                set_f64(&mut cfg.compaction_fixed_cost, key, val, &mut errors);
            }
            "auto_compact_on_flush" => {
                set_bool(&mut cfg.auto_compact_on_flush, key, val, &mut errors);
            }
            "auto_compact_on_ingest" => {
                set_bool(&mut cfg.auto_compact_on_ingest, key, val, &mut errors);
            }
            // ---- broad-lane batch knobs (ADR-026) ----
            "broad_batch_size" => set_usize(&mut cfg.broad_batch_size, key, val, &mut errors),
            "max_percolate_batch" => {
                set_usize(&mut cfg.max_percolate_batch, key, val, &mut errors);
            }
            "broad_columnar" => set_bool(&mut cfg.broad_columnar, key, val, &mut errors),
            "broad_materialize" => set_bool(&mut cfg.broad_materialize, key, val, &mut errors),
            // ---- static (bound at construction) ----
            "data_dir" | "wal_sync_on_write" | "retain_source" => errors.push(format!(
                "setting [{key}] is not dynamically updateable; set it at startup"
            )),
            // ---- unknown ----
            _ => errors.push(format!("unknown setting [{key}]")),
        }
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    // Range/sanity checks from the engine itself (thresholds > 0, ratio in [0,1], …).
    let problems = cfg.validate();
    if problems.is_empty() {
        Ok(cfg)
    } else {
        Err(problems)
    }
}

fn set_usize(slot: &mut usize, key: &str, val: &serde_json::Value, errors: &mut Vec<String>) {
    match val.as_u64() {
        Some(n) => *slot = n as usize,
        None => errors.push(format!("setting [{key}] must be a non-negative integer")),
    }
}

fn set_f64(slot: &mut f64, key: &str, val: &serde_json::Value, errors: &mut Vec<String>) {
    match val.as_f64() {
        Some(n) => *slot = n,
        None => errors.push(format!("setting [{key}] must be a number")),
    }
}

fn set_bool(slot: &mut bool, key: &str, val: &serde_json::Value, errors: &mut Vec<String>) {
    match val.as_bool() {
        Some(b) => *slot = b,
        None => errors.push(format!("setting [{key}] must be a boolean")),
    }
}

// ---------------------------------------------------------------------------
// Graceful shutdown
// ---------------------------------------------------------------------------

/// Wait for SIGINT (ctrl-c) or SIGTERM, then return.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install ctrl-c handler");
    };

    #[cfg(unix)]
    let sigterm = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let sigterm = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {
            info!("received SIGINT (ctrl-c)");
        }
        () = sigterm => {
            info!("received SIGTERM");
        }
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Initialize structured logging.
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    match cli.log_format.as_str() {
        "json" => {
            tracing_subscriber::fmt()
                .json()
                .with_target(true)
                .with_thread_ids(true)
                .with_file(false)
                .with_line_number(false)
                .with_env_filter(env_filter)
                .init();
        }
        _ => {
            tracing_subscriber::fmt()
                .with_target(false)
                .with_env_filter(env_filter)
                .init();
        }
    }

    info!(
        port = cli.port,
        data_dir = ?cli.data_dir,
        log_format = %cli.log_format,
        drain_timeout = cli.drain_timeout,
        "starting reverse-rusty server"
    );

    // Build engine config from CLI flags.
    let config = EngineConfig {
        data_dir: cli.data_dir.clone(),
        max_segments: cli.max_segments,
        memtable_flush_threshold: cli.memtable_flush_threshold,
        max_query_length: cli.max_query_length,
        max_query_clauses: cli.max_query_clauses,
        max_anyof_group_size: cli.max_anyof_group_size,
        wal_sync_on_write: cli.wal_sync_on_write,
        retain_source: cli.retain_source,
        broad_batch_size: cli.broad_batch_size,
        broad_columnar: cli.broad_columnar,
        broad_materialize: cli.broad_materialize,
        max_percolate_batch: cli.max_percolate_batch,
        ..EngineConfig::default()
    };
    let problems = config.validate();
    if !problems.is_empty() {
        for p in &problems {
            error!(problem = %p, "invalid engine config");
        }
        std::process::exit(1);
    }
    if config.data_dir.is_none() {
        warn!("no --data-dir specified: engine is in-memory only, data will not survive restarts");
    }

    // Load vocab file if provided, otherwise use minimal (domain-free) normalizer.
    let vocab = if let Some(ref path) = cli.vocab_file {
        info!(path = ?path, "loading vocabulary from file");
        let v = reverse_rusty::vocab::Vocab::load_json(path).expect("failed to read vocab file");
        info!(
            synonyms = v.synonyms().len(),
            phrases = v.phrases().len(),
            graders = v.graders().len(),
            grade_words = v.grade_words().len(),
            "vocabulary loaded"
        );
        Some(v)
    } else {
        None
    };

    let build_normalizer = |v: &Option<reverse_rusty::vocab::Vocab>| -> Normalizer {
        match v {
            Some(vocab) => vocab
                .to_normalizer()
                .expect("failed to build normalizer from vocab"),
            None => Normalizer::default_vocab().expect("failed to build normalizer"),
        }
    };

    let mut engine = if let Some(data_dir) = cli.data_dir.as_ref() {
        let norm = build_normalizer(&vocab);
        match Engine::open(norm, config.clone()) {
            Ok(mut e) => {
                info!(data_dir = ?data_dir, "recovered engine from persistence");
                if let Some(v) = vocab {
                    // The engine was opened with this vocab's normalizer, so its
                    // segments already align with it — just record the Vocab object
                    // (for GET /_vocab) without recompiling. A genuine vocabulary
                    // *change* goes through PUT /_vocab (set_vocab + recompile).
                    if let Err(err) = e.adopt_vocab(v) {
                        warn!(
                            error = %err,
                            "failed to apply vocab file to recovered engine; \
                             continuing with the recovered normalizer"
                        );
                    }
                }
                e
            }
            Err(e) => {
                // Engine::open returns Ok for a genuinely empty/new data dir, so an
                // error here is real corruption or an I/O failure — never "no data".
                // Refuse to start rather than silently overwriting recoverable data
                // with a fresh (empty) engine.
                error!(
                    data_dir = ?data_dir,
                    error = %e,
                    "failed to open existing data directory; refusing to start to \
                     avoid overwriting recoverable data"
                );
                std::process::exit(1);
            }
        }
    } else if let Some(v) = vocab {
        Engine::with_vocab(v, config).expect("failed to build engine from vocab")
    } else {
        let norm = Normalizer::default_vocab().expect("failed to build normalizer");
        Engine::with_config(norm, config)
    };

    // Create Prometheus metrics and wire the engine observer.
    let prom = PrometheusMetrics::new();
    let prom_for_observer = prom.clone();
    engine.set_observer(move |event: &EngineEvent| {
        // Increment Prometheus counters.
        prom_for_observer.observe_event(event);

        // Emit structured tracing events.
        match event {
            EngineEvent::Flush {
                entries,
                base_segments_after,
                ..
            } => {
                info!(
                    entries = entries,
                    base_segments_after = base_segments_after,
                    "engine.flush"
                );
            }
            EngineEvent::Ingest {
                ingested,
                rejected_parse,
                rejected_class_d,
                base_segments_after,
            } => {
                info!(
                    ingested = ingested,
                    rejected_parse = rejected_parse,
                    rejected_class_d = rejected_class_d,
                    base_segments_after = base_segments_after,
                    "engine.ingest"
                );
            }
            EngineEvent::Compaction {
                report,
                trigger,
                base_segments_after,
                ..
            } => {
                info!(
                    segments_merged = report.segments_merged,
                    entries_before = report.entries_before,
                    entries_after = report.entries_after,
                    tombstones_reclaimed = report.tombstones_reclaimed,
                    trigger = ?trigger,
                    base_segments_after = base_segments_after,
                    "engine.compaction"
                );
            }
            EngineEvent::SegmentCleanupFailed { path, error } => {
                warn!(
                    path = ?path,
                    error = %error,
                    "engine.segment_cleanup_failed: leaked segment file on disk"
                );
            }
            EngineEvent::DurabilityFailure { op, detail, error } => {
                // Data-at-risk failures (lost/uncommitted match data) are errors
                // worth paging on; display-only and benign-recovery failures are
                // warnings. See DurabilityOp::is_data_at_risk.
                if op.is_data_at_risk() {
                    error!(
                        op = op.as_str(),
                        detail = %detail,
                        error = %error,
                        "engine.durability_failure: durability degraded"
                    );
                } else {
                    warn!(
                        op = op.as_str(),
                        detail = %detail,
                        error = %error,
                        "engine.durability_failure"
                    );
                }
            }
        }
    });

    // Pre-load queries from file if specified.
    if let Some(ref path) = cli.load_file {
        info!(path = ?path, "loading queries from file");
        let start = Instant::now();
        let result = loader::load_file(path).expect("failed to read query file");
        if !result.errors.is_empty() {
            warn!(
                error_count = result.errors.len(),
                first_error = %result.errors.first().map(std::string::ToString::to_string).unwrap_or_default(),
                "query file had load errors"
            );
        }
        if !result.queries.is_empty() {
            // All-or-nothing: if the initial load can't be durably persisted,
            // fail fast rather than silently serve an empty/non-durable engine.
            let report = match engine.try_build_from_queries(&result.queries) {
                Ok(report) => report,
                Err(e) => {
                    error!(error = %e, "initial query load could not be durably persisted; aborting startup");
                    std::process::exit(1);
                }
            };
            let elapsed = start.elapsed();
            info!(
                ingested = report.ingested,
                rejected_parse = report.rejected_parse,
                rejected_class_d = report.rejected_class_d,
                elapsed_ms = format!("{:.1}", elapsed.as_secs_f64() * 1000.0),
                "query file loaded"
            );
        }
    }

    // Build rayon pool.
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(cli.threads.unwrap_or(0)) // 0 = default (physical cores)
        .build()
        .expect("failed to build rayon thread pool");

    let drain_timeout = cli.drain_timeout;
    let slow_threshold = cli.slow_query_threshold_ms;
    let initial_snapshot = Arc::new(engine.snapshot());
    let state = Arc::new(AppState {
        engine: Mutex::new(engine),
        snapshot: ArcSwap::new(initial_snapshot),
        pool,
        include_broad: cli.include_broad,
        prom,
        slow_query_threshold_ms: slow_threshold,
    });

    // Build router.
    let app = Router::new()
        .route("/", get(api_root))
        .route("/_doc/{id}", get(get_doc).put(put_doc).delete(delete_doc))
        .route("/_search", post(search))
        .route("/_mpercolate", post(mpercolate))
        .route("/_bulk", post(bulk_ingest))
        .route("/_flush", post(flush))
        .route("/_compact", post(compact))
        .route("/_stats", get(stats))
        .route("/_cat/stats", get(cat_stats))
        .route("/_cat/segments", get(cat_segments))
        .route("/_health", get(health))
        .route("/_metrics", get(prometheus_metrics))
        .route("/_vocab", get(get_vocab).put(put_vocab))
        .route("/_vocab/learn", post(learn_vocab))
        .route("/_settings", get(get_settings).put(put_settings))
        .layer(DefaultBodyLimit::max(100 * 1024 * 1024)) // 100MB
        .layer(tower::limit::ConcurrencyLimitLayer::new(256))
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            request_id_middleware,
        ))
        .with_state(Arc::clone(&state));

    let addr = SocketAddr::from(([0, 0, 0, 0], cli.port));
    info!(
        address = %addr,
        slow_query_threshold_ms = slow_threshold,
        endpoints = "GET /, GET/PUT/DELETE /_doc/{id}, POST /_search, POST /_mpercolate, POST /_bulk, GET /_stats, GET /_cat/stats, GET /_health, GET /_metrics, GET/PUT /_vocab, POST /_vocab/learn",
        "server listening"
    );

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind failed");

    // Graceful shutdown with drain timeout enforcement.
    // 1. Wait for SIGINT/SIGTERM.
    // 2. Tell axum to stop accepting new connections and drain in-flight requests.
    // 3. If drain doesn't complete within `drain_timeout` seconds, force through.
    let signal_received = Arc::new(tokio::sync::Notify::new());
    let signal_received2 = Arc::clone(&signal_received);

    let graceful_shutdown = async move {
        shutdown_signal().await;
        signal_received2.notify_one();
    };

    let server_fut = axum::serve(listener, app).with_graceful_shutdown(graceful_shutdown);

    // After signal fires, race the drain against the timeout.
    let drain_deadline = async {
        signal_received.notified().await;
        tokio::time::sleep(tokio::time::Duration::from_secs(drain_timeout)).await;
        warn!(drain_timeout, "drain timeout exceeded, forcing shutdown");
    };

    tokio::select! {
        result = server_fut => {
            if let Err(e) = result {
                eprintln!("server error: {e}");
            }
        }
        () = drain_deadline => {
            // Drain took too long — fall through to cleanup.
        }
    }

    info!(
        drain_timeout = drain_timeout,
        "connection drain complete, running shutdown sequence"
    );

    // Flush memtable and log final metrics.
    {
        let mut engine = state.engine.lock();
        let pre_metrics = engine.metrics();

        if pre_metrics.memtable_entries > 0 {
            info!(
                memtable_entries = pre_metrics.memtable_entries,
                "flushing memtable before shutdown"
            );
            engine.flush();
        }

        let final_metrics = engine.metrics();
        info!(
            total_queries = final_metrics.total_queries,
            base_segments = final_metrics.base_segments,
            dict_features = final_metrics.dict_features,
            exact_bytes = final_metrics.exact_bytes,
            index_bytes = final_metrics.index_bytes,
            filter_bytes = final_metrics.filter_bytes,
            "final engine state"
        );
    }

    info!("shutdown complete");
}

#[cfg(test)]
mod settings_tests {
    use super::{apply_settings_patch, EngineConfig};

    fn patch(json: &str) -> serde_json::Map<String, serde_json::Value> {
        serde_json::from_str(json).expect("test patch must be a JSON object")
    }

    #[test]
    fn applies_dynamic_settings() {
        let cfg = apply_settings_patch(
            EngineConfig::default(),
            &patch(
                r#"{"max_segments": 16, "auto_compact_on_flush": false, "holes_ratio_threshold": 0.5}"#,
            ),
        )
        .expect("valid dynamic patch");
        assert_eq!(cfg.max_segments, 16);
        assert!(!cfg.auto_compact_on_flush);
        assert!((cfg.holes_ratio_threshold - 0.5).abs() < f64::EPSILON);
        // Untouched fields keep their defaults.
        assert_eq!(cfg.memtable_flush_threshold, 100_000);
    }

    #[test]
    fn applies_broad_lane_settings() {
        let cfg = apply_settings_patch(
            EngineConfig::default(),
            &patch(
                r#"{"broad_batch_size": 512, "broad_columnar": false, "broad_materialize": false, "max_percolate_batch": 50000}"#,
            ),
        )
        .expect("valid broad-lane patch");
        assert_eq!(cfg.broad_batch_size, 512);
        assert!(!cfg.broad_columnar);
        assert!(!cfg.broad_materialize);
        assert_eq!(cfg.max_percolate_batch, 50_000);
    }

    #[test]
    fn rejects_zero_broad_batch_size() {
        let err = apply_settings_patch(
            EngineConfig::default(),
            &patch(r#"{"broad_batch_size": 0}"#),
        )
        .expect_err("broad_batch_size 0 must be rejected by validate()");
        assert!(
            err.iter()
                .any(|e| e.contains("broad_batch_size must be >= 1")),
            "{err:?}"
        );
    }

    #[test]
    fn rejects_static_settings() {
        let err = apply_settings_patch(
            EngineConfig::default(),
            &patch(r#"{"wal_sync_on_write": true}"#),
        )
        .expect_err("static setting must be rejected");
        assert!(
            err.iter().any(
                |e| e.contains("wal_sync_on_write") && e.contains("not dynamically updateable")
            ),
            "{err:?}"
        );
    }

    #[test]
    fn rejects_unknown_settings() {
        let err = apply_settings_patch(EngineConfig::default(), &patch(r#"{"bogus": 1}"#))
            .expect_err("unknown setting must be rejected");
        assert!(
            err.iter().any(|e| e.contains("unknown setting [bogus]")),
            "{err:?}"
        );
    }

    #[test]
    fn rejects_wrong_value_type() {
        let err = apply_settings_patch(
            EngineConfig::default(),
            &patch(r#"{"max_segments": "lots"}"#),
        )
        .expect_err("wrong type must be rejected");
        assert!(
            err.iter().any(|e| e.contains("non-negative integer")),
            "{err:?}"
        );
    }

    #[test]
    fn rejects_out_of_range_via_validate() {
        // 0 segments and a ratio > 1 are caught by EngineConfig::validate().
        let err = apply_settings_patch(
            EngineConfig::default(),
            &patch(r#"{"max_segments": 0, "holes_ratio_threshold": 2.0}"#),
        )
        .expect_err("invalid ranges must be rejected");
        assert!(err.iter().any(|e| e.contains("max_segments")), "{err:?}");
        assert!(
            err.iter().any(|e| e.contains("holes_ratio_threshold")),
            "{err:?}"
        );
    }

    #[test]
    fn one_bad_key_rejects_the_whole_patch() {
        // A valid key alongside a static one → the whole patch is rejected, so the
        // caller (the handler) leaves the engine config untouched.
        let err = apply_settings_patch(
            EngineConfig::default(),
            &patch(r#"{"max_segments": 12, "data_dir": "/tmp/x"}"#),
        )
        .expect_err("a static key alongside a valid one rejects the batch");
        assert!(err.iter().any(|e| e.contains("data_dir")), "{err:?}");
    }
}

#[cfg(test)]
mod cat_segments_tests {
    use super::{fmt_bytes, render_segments_table, SegmentRow};
    use reverse_rusty::events::{SegmentInfo, SegmentKind};

    fn info(ordinal: usize, kind: SegmentKind, alive: usize, deleted: usize) -> SegmentInfo {
        let entries = alive + deleted;
        SegmentInfo {
            ordinal,
            kind,
            entries,
            alive,
            deleted,
            holes_ratio: if entries == 0 {
                0.0
            } else {
                deleted as f64 / entries as f64
            },
            vocab_epoch: 3,
            stale: false,
            resident_bytes: 0,
            overhead_bytes: 0,
        }
    }

    #[test]
    fn fmt_bytes_scales_by_unit() {
        assert_eq!(fmt_bytes(0), "0 B");
        assert_eq!(fmt_bytes(512), "512 B");
        assert_eq!(fmt_bytes(1024), "1.00 KB");
        assert_eq!(fmt_bytes(1_572_864), "1.50 MB");
        assert_eq!(fmt_bytes(3 * 1024 * 1024 * 1024), "3.00 GB");
    }

    #[test]
    fn table_has_header_and_one_row_per_segment() {
        let infos = vec![
            info(0, SegmentKind::Mmap, 98_000, 2_000),
            info(1, SegmentKind::Memory, 50_000, 0),
            info(2, SegmentKind::Memtable, 1_200, 0),
        ];
        let table = render_segments_table(&infos);
        let lines: Vec<&str> = table.lines().collect();
        // 1 header + 3 data rows.
        assert_eq!(lines.len(), 4, "table:\n{table}");
        assert!(lines[0].contains("segment") && lines[0].contains("holes"));
        assert!(lines[1].contains("mmap"));
        assert!(lines[2].contains("memory"));
        assert!(lines[3].contains("memtable"));
        // 2000/100000 = 2.00% holes on the first base segment.
        assert!(lines[1].contains("2.00%"), "row:\n{}", lines[1]);
    }

    #[test]
    fn stale_flag_renders_yes_no() {
        let mut stale = info(0, SegmentKind::Memory, 10, 0);
        stale.stale = true;
        let table = render_segments_table(&[stale]);
        let row = table.lines().nth(1).expect("data row");
        assert!(row.contains("yes"), "row: {row}");

        let fresh = info(0, SegmentKind::Memory, 10, 0);
        let table = render_segments_table(&[fresh]);
        let row = table.lines().nth(1).expect("data row");
        assert!(row.contains(" no "), "row: {row}");
    }

    #[test]
    fn json_row_projects_segment_info() {
        let mut s = info(2, SegmentKind::Memtable, 1_200, 0);
        s.resident_bytes = 145_000;
        s.overhead_bytes = 18_000;
        let row = SegmentRow::from(&s);
        let json = serde_json::to_value(&row).expect("serialize");
        assert_eq!(json["kind"], "memtable");
        assert_eq!(json["ordinal"], 2);
        assert_eq!(json["alive"], 1_200);
        // Byte fields are raw integers in JSON (humanized only in the text table).
        assert_eq!(json["resident_bytes"], 145_000);
        assert_eq!(json["overhead_bytes"], 18_000);
    }
}

#[cfg(test)]
mod mpercolate_tests {
    //! Handler-level tests for POST /_mpercolate: request validation, the empty
    //! batch no-op, the responses[] envelope shape, and — the load-bearing one —
    //! that each per-document response is identical to the per-title path
    //! (`match_title`), so the batch endpoint can't silently diverge from
    //! `/_search`. The library already proves batch == scalar (tests/broad_batch);
    //! this proves the HTTP layer threads results through in order and unchanged.
    use super::{mpercolate, AppState, DocBody, MPercolateBody, PrometheusMetrics, State};
    use axum::Json;
    use reverse_rusty::gen::{generate, GenConfig};
    use reverse_rusty::segment::{Engine, MatchScratch};
    use reverse_rusty::Normalizer;
    use std::sync::Arc;

    fn corpus() -> (Engine, Vec<String>) {
        let data = generate(&GenConfig {
            num_queries: 5_000,
            num_titles: 300,
            broad_query_frac: 0.1,
            hot_skew: 2.0,
            family_size: 8,
            seed: 0x0BA7_C0DE,
            num_players: 2_000,
            num_sets: 1_000,
        });
        let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
        eng.build_from_queries(&data.queries);
        (eng, data.titles)
    }

    fn state_with(eng: Engine, include_broad: bool) -> Arc<AppState> {
        let snap = Arc::new(eng.snapshot());
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .expect("pool");
        Arc::new(AppState {
            engine: parking_lot::Mutex::new(eng),
            snapshot: arc_swap::ArcSwap::new(snap),
            pool,
            include_broad,
            prom: PrometheusMetrics::new(),
            slow_query_threshold_ms: 0,
        })
    }

    fn body(docs: Option<Vec<&str>>, include_broad: Option<bool>, profile: bool) -> MPercolateBody {
        MPercolateBody {
            documents: docs.map(|v| {
                v.into_iter()
                    .map(|t| DocBody {
                        title: t.to_string(),
                    })
                    .collect()
            }),
            include_broad,
            include_source: Some(false),
            // Large cap so no per-document truncation can mask a result mismatch.
            size: Some(1_000_000),
            timeout_ms: None,
            profile: Some(profile),
        }
    }

    #[tokio::test]
    async fn missing_documents_is_400() {
        let (eng, _) = corpus();
        let state = state_with(eng, false);
        let err = mpercolate(State(state), Json(body(None, None, false)))
            .await
            .err()
            .expect("missing documents must error");
        assert_eq!(err.0, axum::http::StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn empty_batch_is_noop() {
        let (eng, _) = corpus();
        let state = state_with(eng, true);
        let resp = mpercolate(State(state), Json(body(Some(Vec::new()), None, true)))
            .await
            .expect("empty batch is a valid no-op")
            .0;
        assert!(resp.responses.is_empty());
        assert!(resp.broad.is_none(), "no work => no broad summary");
    }

    // Reads the ES-convention `_id` field on hits (clippy::used_underscore_binding).
    #[allow(clippy::used_underscore_binding)]
    #[tokio::test]
    async fn responses_are_byte_identical_to_per_title_search() {
        let (eng, titles) = corpus();
        // Capture a snapshot of the same state for the per-title baseline before
        // the engine moves into the AppState.
        let baseline = Arc::new(eng.snapshot());
        let state = state_with(eng, true);

        let batch: Vec<&str> = titles.iter().take(150).map(String::as_str).collect();
        // include_broad=true exercises the columnar broad lane through the endpoint.
        let resp = mpercolate(
            State(Arc::clone(&state)),
            Json(body(Some(batch.clone()), Some(true), true)),
        )
        .await
        .expect("ok")
        .0;

        assert_eq!(
            resp.responses.len(),
            batch.len(),
            "one response per document"
        );

        let mut scratch = MatchScratch::new();
        let mut out = Vec::new();
        let mut summed = 0u32;
        for (i, title) in batch.iter().enumerate() {
            out.clear();
            baseline.match_title(title, &mut scratch, &mut out, true);
            let mut expected = out.clone();
            expected.sort_unstable();
            expected.dedup();

            let item = &resp.responses[i];
            let mut got: Vec<u64> = item.hits.hits.iter().map(|h| h._id).collect();
            got.sort_unstable();
            assert_eq!(
                got, expected,
                "document {i} ({title}) diverged from per-title search"
            );
            assert_eq!(item.hits.total, expected.len(), "total mismatch at {i}");
            summed += expected.len() as u32;
        }

        // Top-level broad summary present (profile=true) and internally consistent.
        let broad = resp.broad.expect("profile=true => broad summary");
        assert_eq!(broad.strategy, "columnar");
        assert_eq!(broad.batch_size, 256);
        assert_eq!(
            broad.total_matches, summed,
            "summary total must equal the per-document sum"
        );
    }
}
