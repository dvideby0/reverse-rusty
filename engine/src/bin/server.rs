//! Percolator HTTP server — Elasticsearch-inspired REST API.
//!
//! Endpoints:
//!   PUT  /_doc/{id}          Register a query (body: {"query": "..."})
//!   DELETE /_doc/{id}        Remove a stored query
//!   POST /_search            Percolate title(s) (body: {"document": {"title": "..."}} or "documents")
//!   POST /_bulk              NDJSON bulk ingest ({action}\n{source}\n...)
//!   POST /_flush             Flush memtable to immutable segment
//!   POST /_compact           Force compaction
//!   GET  /_stats             JSON metrics snapshot
//!   GET  /_cat/stats         Human-readable metrics
//!   GET  /_health            Health check
//!   GET  /_metrics           Prometheus text exposition format
//!   GET  /_vocab             Current vocabulary as JSON
//!   PUT  /_vocab             Replace vocabulary (body: Vocab JSON)
//!   POST /_vocab/learn       Learn synonyms from raw query text
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
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use parking_lot::Mutex;
use clap::Parser;
use prometheus::{
    Encoder, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Histogram,
    HistogramOpts, HistogramVec, Opts, Registry, TextEncoder,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn, instrument};

use std::cell::RefCell;

use percolator::config::EngineConfig;
use percolator::events::EngineEvent;
use percolator::loader;
use percolator::normalize::Normalizer;
use percolator::segment::{Engine, EngineSnapshot, MatchScratch, MatchStats};

thread_local! {
    static SCRATCH: RefCell<MatchScratch> = RefCell::new(MatchScratch::new());
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser, Debug)]
#[command(name = "percolator-server", about = "Percolator HTTP server")]
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
    #[arg(long, default_value_t = 10_000)]
    max_query_length: usize,

    /// Maximum number of clauses per query.
    #[arg(long, default_value_t = 256)]
    max_query_clauses: usize,

    /// Maximum members in an any-of group.
    #[arg(long, default_value_t = 64)]
    max_anyof_group_size: usize,

    /// Fsync the write-ahead log on every mutation before acknowledging it.
    /// When false (default), WAL appends reach the OS page cache and are
    /// fsync'd at the next flush checkpoint — an acknowledged write survives a
    /// process crash but not power loss until checkpoint (RocksDB sync=false /
    /// SQLite NORMAL). When true, every write is durable against power loss at
    /// a large per-write latency cost (SQLite FULL).
    #[arg(long, default_value_t = false)]
    wal_sync_on_write: bool,
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

    // Cumulative counters (incremented via EngineEvent observer)
    flush_total: IntCounter,
    flush_entries_total: IntCounter,
    ingest_total: IntCounter,
    ingest_queries_total: IntCounter,
    ingest_rejected: IntCounterVec,
    compaction_total: IntCounter,
    compaction_tombstones_reclaimed: IntCounter,

    // Request metrics
    http_requests_total: IntCounterVec,
    http_request_duration: HistogramVec,

    // Match metrics
    match_candidates_per_title: Histogram,
    match_results_per_title: Histogram,

    // Slow query counter
    slow_queries_total: IntCounter,
}

impl PrometheusMetrics {
    fn new() -> Self {
        let registry = Registry::new_custom(Some("percolator".to_string()), None)
            .expect("failed to create prometheus registry");

        // --- Engine gauges (refreshed on each /_metrics scrape) ---

        let total_queries = IntGauge::with_opts(
            Opts::new("total_queries", "Total queries stored across all segments and memtable"),
        ).unwrap();

        let base_segments = IntGauge::with_opts(
            Opts::new("base_segments", "Number of sealed immutable base segments"),
        ).unwrap();

        let memtable_entries = IntGauge::with_opts(
            Opts::new("memtable_entries", "Entries currently in the mutable memtable"),
        ).unwrap();

        let dict_features = IntGauge::with_opts(
            Opts::new("dict_features", "Distinct features in the shared dictionary"),
        ).unwrap();

        let memory_bytes = IntGaugeVec::new(
            Opts::new("memory_bytes", "Heap memory usage by component"),
            &["component"],
        ).unwrap();

        // --- Event counters ---

        let flush_total = IntCounter::with_opts(
            Opts::new("flush_total", "Total number of memtable flushes"),
        ).unwrap();

        let flush_entries_total = IntCounter::with_opts(
            Opts::new("flush_entries_total", "Total entries flushed across all flushes"),
        ).unwrap();

        let ingest_total = IntCounter::with_opts(
            Opts::new("ingest_total", "Total number of bulk ingest operations"),
        ).unwrap();

        let ingest_queries_total = IntCounter::with_opts(
            Opts::new("ingest_queries_total", "Total queries ingested successfully"),
        ).unwrap();

        let ingest_rejected = IntCounterVec::new(
            Opts::new("ingest_rejected_total", "Queries rejected during ingest"),
            &["reason"],
        ).unwrap();

        let compaction_total = IntCounter::with_opts(
            Opts::new("compaction_total", "Total number of compaction operations"),
        ).unwrap();

        let compaction_tombstones_reclaimed = IntCounter::with_opts(
            Opts::new("compaction_tombstones_reclaimed_total", "Tombstones reclaimed by compaction"),
        ).unwrap();

        // --- HTTP request metrics ---

        let http_requests_total = IntCounterVec::new(
            Opts::new("http_requests_total", "Total HTTP requests by endpoint and status"),
            &["endpoint", "status"],
        ).unwrap();

        let http_request_duration = HistogramVec::new(
            HistogramOpts::new("http_request_duration_seconds", "HTTP request duration in seconds")
                .buckets(vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
            &["endpoint"],
        ).unwrap();

        // --- Match metrics ---

        let match_candidates_per_title = Histogram::with_opts(
            HistogramOpts::new("match_candidates_per_title", "Candidate queries evaluated per title")
                .buckets(vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0]),
        ).unwrap();

        let match_results_per_title = Histogram::with_opts(
            HistogramOpts::new("match_results_per_title", "Confirmed matches per title")
                .buckets(vec![0.0, 1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0]),
        ).unwrap();

        let slow_queries_total = IntCounter::with_opts(
            Opts::new("slow_queries_total", "Searches exceeding the slow-query threshold"),
        ).unwrap();

        // Register all
        registry.register(Box::new(total_queries.clone())).unwrap();
        registry.register(Box::new(base_segments.clone())).unwrap();
        registry.register(Box::new(memtable_entries.clone())).unwrap();
        registry.register(Box::new(dict_features.clone())).unwrap();
        registry.register(Box::new(memory_bytes.clone())).unwrap();
        registry.register(Box::new(flush_total.clone())).unwrap();
        registry.register(Box::new(flush_entries_total.clone())).unwrap();
        registry.register(Box::new(ingest_total.clone())).unwrap();
        registry.register(Box::new(ingest_queries_total.clone())).unwrap();
        registry.register(Box::new(ingest_rejected.clone())).unwrap();
        registry.register(Box::new(compaction_total.clone())).unwrap();
        registry.register(Box::new(compaction_tombstones_reclaimed.clone())).unwrap();
        registry.register(Box::new(http_requests_total.clone())).unwrap();
        registry.register(Box::new(http_request_duration.clone())).unwrap();
        registry.register(Box::new(match_candidates_per_title.clone())).unwrap();
        registry.register(Box::new(match_results_per_title.clone())).unwrap();
        registry.register(Box::new(slow_queries_total.clone())).unwrap();

        Self {
            registry,
            total_queries,
            base_segments,
            memtable_entries,
            dict_features,
            memory_bytes,
            flush_total,
            flush_entries_total,
            ingest_total,
            ingest_queries_total,
            ingest_rejected,
            compaction_total,
            compaction_tombstones_reclaimed,
            http_requests_total,
            http_request_duration,
            match_candidates_per_title,
            match_results_per_title,
            slow_queries_total,
        }
    }

    /// Update gauge metrics from an EngineMetrics snapshot.
    fn refresh_gauges(&self, m: &percolator::events::EngineMetrics) {
        self.total_queries.set(m.total_queries as i64);
        self.base_segments.set(m.base_segments as i64);
        self.memtable_entries.set(m.memtable_entries as i64);
        self.dict_features.set(m.dict_features as i64);
        self.memory_bytes.with_label_values(&["exact"]).set(m.exact_bytes as i64);
        self.memory_bytes.with_label_values(&["index"]).set(m.index_bytes as i64);
        self.memory_bytes.with_label_values(&["filter"]).set(m.filter_bytes as i64);
    }

    /// Handle an EngineEvent — increment counters. Called from the observer.
    fn observe_event(&self, event: &EngineEvent) {
        match event {
            EngineEvent::Flush { entries, .. } => {
                self.flush_total.inc();
                self.flush_entries_total.inc_by(*entries as u64);
            }
            EngineEvent::Ingest { ingested, rejected_parse, rejected_class_d, .. } => {
                self.ingest_total.inc();
                self.ingest_queries_total.inc_by(*ingested as u64);
                if *rejected_parse > 0 {
                    self.ingest_rejected.with_label_values(&["parse"]).inc_by(*rejected_parse as u64);
                }
                if *rejected_class_d > 0 {
                    self.ingest_rejected.with_label_values(&["class_d"]).inc_by(*rejected_class_d as u64);
                }
            }
            EngineEvent::Compaction { report, .. } => {
                self.compaction_total.inc();
                self.compaction_tombstones_reclaimed.inc_by(report.tombstones_reclaimed as u64);
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
fn default_version() -> u32 { 1 }

#[derive(Serialize)]
struct PutDocResponse {
    _id: u64,
    result: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
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
    _explanation: Option<percolator::ExplainDetail>,
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
    postings_scanned: u32,
    matches: u32,
    probes_attempted: u32,
    probes_skipped: u32,
}

impl From<MatchStats> for StatsResponse {
    fn from(s: MatchStats) -> Self {
        Self {
            unique_candidates: s.unique_candidates,
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
struct MemoryStats {
    exact_bytes: usize,
    index_bytes: usize,
    filter_bytes: usize,
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
#[derive(Serialize)]
struct ApiError {
    error: ApiErrorBody,
    status: u16,
}

#[derive(Serialize)]
struct ApiErrorBody {
    #[serde(rename = "type")]
    error_type: String,
    reason: String,
}

impl ApiError {
    fn response(status: StatusCode, error_type: &str, reason: impl Into<String>) -> (StatusCode, Json<ApiError>) {
        let code = status.as_u16();
        (status, Json(ApiError {
            error: ApiErrorBody {
                error_type: error_type.to_string(),
                reason: reason.into(),
            },
            status: code,
        }))
    }
}

// ---------------------------------------------------------------------------
// Request ID middleware
// ---------------------------------------------------------------------------

/// Adds a unique X-Request-Id header to every response and includes it in the
/// tracing span for request correlation.
async fn request_id_middleware(
    request: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Response {
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
            Ok(percolator::segment::InsertOutcome::Inserted(_)) => {
                info!(query_id = id, "query registered");
                state.prom.http_requests_total.with_label_values(&["put_doc", "201"]).inc();
                (StatusCode::CREATED, Json(PutDocResponse { _id: id, result: "created", error: None }))
            }
            Ok(percolator::segment::InsertOutcome::RejectedClassD) => {
                warn!(query_id = id, "query rejected: cost class D");
                state.prom.http_requests_total.with_label_values(&["put_doc", "400"]).inc();
                (StatusCode::BAD_REQUEST, Json(PutDocResponse {
                    _id: id,
                    result: "rejected",
                    error: Some("query has no anchorable feature (cost class D)".into()),
                }))
            }
            Err(percolator::WriteError::Parse(e)) => {
                warn!(query_id = id, error = %e, "query parse error");
                state.prom.http_requests_total.with_label_values(&["put_doc", "400"]).inc();
                (StatusCode::BAD_REQUEST, Json(PutDocResponse {
                    _id: id,
                    result: "error",
                    error: Some(format!("parse error: {}", e)),
                }))
            }
            Err(percolator::WriteError::Wal(e)) => {
                // Durability failure: the mutation was NOT applied. Never
                // acknowledge a write we couldn't log (see ADR-013). 503 tells
                // the client to retry — the engine state is unchanged.
                error!(query_id = id, error = %e, "WAL write failed, mutation rejected");
                state.prom.http_requests_total.with_label_values(&["put_doc", "503"]).inc();
                (StatusCode::SERVICE_UNAVAILABLE, Json(PutDocResponse {
                    _id: id,
                    result: "error",
                    error: Some(format!("write-ahead log error: {}", e)),
                }))
            }
        }
    };
    state.publish_snapshot();
    state.prom.http_request_duration.with_label_values(&["put_doc"])
        .observe(start.elapsed().as_secs_f64());
    result
}

/// GET /_doc/{id} — retrieve a stored query by logical ID.
#[instrument(skip(state), fields(query_id = id))]
async fn get_doc(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u64>,
) -> impl IntoResponse {
    let start = Instant::now();
    let snap = state.snapshot.load();
    let result = match snap.get_query_source(id) {
        Some(query_text) => {
            state.prom.http_requests_total.with_label_values(&["get_doc", "200"]).inc();
            (StatusCode::OK, Json(serde_json::json!({
                "_id": id,
                "found": true,
                "_source": { "query": query_text }
            })))
        }
        None => {
            state.prom.http_requests_total.with_label_values(&["get_doc", "404"]).inc();
            (StatusCode::NOT_FOUND, Json(serde_json::json!({
                "_id": id,
                "found": false
            })))
        }
    };
    state.prom.http_request_duration.with_label_values(&["get_doc"])
        .observe(start.elapsed().as_secs_f64());
    result
}

/// DELETE /_doc/{id} — remove a stored query by logical ID.
#[instrument(skip(state), fields(query_id = id))]
async fn delete_doc(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u64>,
) -> impl IntoResponse {
    let start = Instant::now();
    let deleted = {
        let mut engine = state.engine.lock();
        engine.delete_by_logical_id(id)
    };
    state.publish_snapshot();
    state.prom.http_request_duration.with_label_values(&["delete_doc"])
        .observe(start.elapsed().as_secs_f64());
    match deleted {
        Ok(n) if n > 0 => {
            info!(query_id = id, deleted = n, "query deleted");
            state.prom.http_requests_total.with_label_values(&["delete_doc", "200"]).inc();
            (StatusCode::OK, Json(serde_json::json!({
                "_id": id,
                "result": "deleted",
                "deleted_count": n
            })))
        }
        Ok(_) => {
            state.prom.http_requests_total.with_label_values(&["delete_doc", "404"]).inc();
            (StatusCode::NOT_FOUND, Json(serde_json::json!({
                "_id": id,
                "result": "not_found"
            })))
        }
        Err(e) => {
            // Tombstone WAL append failed: the delete was NOT applied. Reject
            // rather than acknowledge a delete we couldn't log (see ADR-013).
            error!(query_id = id, error = %e, "WAL write failed, delete rejected");
            state.prom.http_requests_total.with_label_values(&["delete_doc", "503"]).inc();
            (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({
                "_id": id,
                "result": "error",
                "error": format!("write-ahead log error: {}", e)
            })))
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
            let title_for_explain = if include_explain { Some(title.clone()) } else { None };
            let prom = state.prom.clone();
            let snap = Arc::clone(&state.snapshot.load());
            let state_inner = Arc::clone(&state);

            let search_fut = tokio::task::spawn_blocking(move || {
                state_inner.pool.install(|| {
                    SCRATCH.with(|cell| {
                        let mut scratch = cell.borrow_mut();
                        let mut out = Vec::new();
                        let stats =
                            snap.match_title(&title, &mut scratch, &mut out, include_broad);
                        (out, stats)
                    })
                })
            });

            let (ids, stats) = match tokio::time::timeout(timeout, search_fut).await {
                Ok(Ok(result)) => result,
                Ok(Err(e)) => {
                    eprintln!("search task panicked: {e}");
                    state.prom.http_requests_total.with_label_values(&["search", "500"]).inc();
                    return Err(ApiError::response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "search_error",
                        "internal search task failed",
                    ));
                }
                Err(_) => {
                    state.prom.http_requests_total.with_label_values(&["search", "408"]).inc();
                    return Err(ApiError::response(
                        StatusCode::REQUEST_TIMEOUT,
                        "timeout",
                        format!("search timed out after {}ms", timeout.as_millis()),
                    ));
                }
            };

            prom.match_candidates_per_title.observe(stats.unique_candidates as f64);
            prom.match_results_per_title.observe(ids.len() as f64);

            let took_ms = start.elapsed().as_secs_f64() * 1000.0;
            let total = ids.len();
            let paged_ids: Vec<u64> = ids.into_iter().skip(page_from).take(page_size).collect();
            let snap = state.snapshot.load();
            let hits = paged_ids.iter().map(|&id| {
                let source = if include_source {
                    snap.get_query_source(id).map(|q| HitSource { query: q })
                } else {
                    None
                };
                let explanation = title_for_explain.as_deref()
                    .and_then(|t| snap.explain_hit(id, t));
                SearchHitItem { _id: id, _source: source, _explanation: explanation }
            }).collect();
            info!(titles = 1, matches = total, took_ms = format!("{:.2}", took_ms), "search complete");
            SearchResponse {
                took_ms,
                hits: SearchHits { total, hits },
                slots: None,
                profile: if include_profile { Some(stats.into()) } else { None },
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
                state_inner.pool.install(|| snap.match_titles_par(&titles, include_broad))
            });

            let results = match tokio::time::timeout(timeout, search_fut).await {
                Ok(Ok(result)) => result,
                Ok(Err(e)) => {
                    eprintln!("search task panicked: {e}");
                    state.prom.http_requests_total.with_label_values(&["search", "500"]).inc();
                    return Err(ApiError::response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "search_error",
                        "internal search task failed",
                    ));
                }
                Err(_) => {
                    state.prom.http_requests_total.with_label_values(&["search", "408"]).inc();
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
                prom.match_candidates_per_title.observe(stats.unique_candidates as f64);
                prom.match_results_per_title.observe(ids.len() as f64);

                all_ids.extend_from_slice(&ids);
                slot_data.push((slot, ids, stats.into()));
            }
            all_ids.sort_unstable();
            all_ids.dedup();

            let total = all_ids.len();
            let paged_ids: Vec<u64> = all_ids.into_iter().skip(page_from).take(page_size).collect();

            let snap = state.snapshot.load();
            let make_hit = |id: u64| {
                let source = if include_source {
                    snap.get_query_source(id).map(|q| HitSource { query: q })
                } else {
                    None
                };
                SearchHitItem { _id: id, _source: source, _explanation: None }
            };
            let hits: Vec<_> = paged_ids.iter().map(|&id| make_hit(id)).collect();
            let slots: Vec<_> = slot_data.into_iter().map(|(slot, ids, stats)| {
                let slot_hits = ids.iter().map(|&id| make_hit(id)).collect();
                SlotHit { slot, total: ids.len(), hits: slot_hits, stats }
            }).collect();

            info!(titles = num_docs, matches = total, took_ms = format!("{:.2}", took_ms), "search complete");
            SearchResponse {
                took_ms,
                hits: SearchHits { total, hits },
                slots: Some(slots),
                profile: None,
            }
        }

        (None, None) => {
            state.prom.http_requests_total.with_label_values(&["search", "400"]).inc();
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
            titles = if response.slots.is_some() {
                response.slots.as_ref().unwrap().len()
            } else {
                1
            },
            "slow query"
        );
    }

    state.prom.http_requests_total.with_label_values(&["search", "200"]).inc();
    state.prom.http_request_duration.with_label_values(&["search"])
        .observe(start.elapsed().as_secs_f64());
    Ok(Json(response))
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
                state.prom.http_requests_total.with_label_values(&["bulk", "415"]).inc();
                return ApiError::response(
                    StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    "unsupported_media_type",
                    "Content-Type must be application/json or application/x-ndjson",
                ).into_response();
            }
        }
    }

    // Parse NDJSON action/source pairs.
    let lines: Vec<&str> = body.lines().filter(|l| !l.trim().is_empty()).collect();
    let mut pairs: Vec<(u64, String)> = Vec::new();
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
                items.push(BulkItem { index: BulkItemInner {
                    _id: 0, status: 400,
                    error: Some(format!("invalid action JSON: {}", e)),
                }});
                // Try to skip the source line too.
                if i < lines.len() { i += 1; }
                continue;
            }
        };

        let id = extract_bulk_id(&action);

        // Next line is the source document.
        if i >= lines.len() {
            has_errors = true;
            items.push(BulkItem { index: BulkItemInner {
                _id: id.unwrap_or(0), status: 400,
                error: Some("missing source line after action".into()),
            }});
            break;
        }

        let source_line = lines[i];
        i += 1;

        let id = match id {
            Some(id) => id,
            None => {
                has_errors = true;
                items.push(BulkItem { index: BulkItemInner {
                    _id: 0, status: 400,
                    error: Some("could not extract _id from action".into()),
                }});
                continue;
            }
        };

        let source: serde_json::Value = match serde_json::from_str(source_line) {
            Ok(v) => v,
            Err(e) => {
                has_errors = true;
                items.push(BulkItem { index: BulkItemInner {
                    _id: id, status: 400,
                    error: Some(format!("invalid source JSON: {}", e)),
                }});
                continue;
            }
        };

        let query = match source.get("query").and_then(|v| v.as_str()) {
            Some(q) => q.to_string(),
            None => {
                has_errors = true;
                items.push(BulkItem { index: BulkItemInner {
                    _id: id, status: 400,
                    error: Some("missing or non-string 'query' field".into()),
                }});
                continue;
            }
        };

        pairs.push((id, query));
        items.push(BulkItem { index: BulkItemInner { _id: id, status: 201, error: None } });
    }

    // Ingest the valid pairs.
    if !pairs.is_empty() {
        let report = {
            let mut engine = state.engine.lock();
            engine.bulk_ingest(&pairs)
        };
        state.publish_snapshot();

        info!(
            ingested = report.ingested,
            rejected_parse = report.rejected_parse,
            rejected_class_d = report.rejected_class_d,
            "bulk ingest complete"
        );

        if report.rejected_parse > 0 || report.rejected_class_d > 0 {
            has_errors = true;
        }
    }

    let took_ms = start.elapsed().as_secs_f64() * 1000.0;
    state.prom.http_requests_total.with_label_values(&["bulk", "200"]).inc();
    state.prom.http_request_duration.with_label_values(&["bulk"])
        .observe(start.elapsed().as_secs_f64());
    Json(BulkResponse { took_ms, errors: has_errors, items }).into_response()
}

/// Extract _id from ES-style action line.
/// Accepts: {"index": {"_id": 123}} or {"_id": 123}
fn extract_bulk_id(action: &serde_json::Value) -> Option<u64> {
    // ES style: {"index": {"_id": N}}
    if let Some(inner) = action.get("index") {
        if let Some(id) = inner.get("_id").and_then(|v| v.as_u64()) {
            return Some(id);
        }
    }
    // Flat style: {"_id": N}
    if let Some(id) = action.get("_id").and_then(|v| v.as_u64()) {
        return Some(id);
    }
    // Also try "id" without underscore.
    action.get("id").and_then(|v| v.as_u64())
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
    state.prom.http_requests_total.with_label_values(&["flush", "200"]).inc();
    state.prom.http_request_duration.with_label_values(&["flush"])
        .observe(start.elapsed().as_secs_f64());
    Json(serde_json::json!({
        "acknowledged": true,
        "total_queries": metrics.total_queries,
        "base_segments": metrics.base_segments,
    }))
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
    state.prom.http_requests_total.with_label_values(&["compact", "200"]).inc();
    state.prom.http_request_duration.with_label_values(&["compact"])
        .observe(start.elapsed().as_secs_f64());
    match report {
        Some(r) => {
            info!(
                segments_merged = r.segments_merged,
                entries_before = r.entries_before,
                entries_after = r.entries_after,
                tombstones_reclaimed = r.tombstones_reclaimed,
                "compaction complete"
            );
            Json(serde_json::json!({
                "acknowledged": true,
                "segments_merged": r.segments_merged,
                "entries_before": r.entries_before,
                "entries_after": r.entries_after,
                "tombstones_reclaimed": r.tombstones_reclaimed,
            }))
        }
        None => {
            info!("compaction skipped: not needed");
            Json(serde_json::json!({
                "acknowledged": true,
                "message": "no compaction needed",
            }))
        }
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
        class_counts: ClassCounts { a: cc[0], b: cc[1], c: cc[2], d: cc[3] },
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
    out.push_str(&format!("segments         {} (+ memtable: {})\n", m.base_segments, m.memtable_entries));
    out.push_str(&format!("features         {}\n", m.dict_features));
    out.push_str(&format!("class A/B/C/D    {} / {} / {} / {}\n", cc[0], cc[1], cc[2], cc[3]));
    out.push_str(&format!("rejected parse   {}\n", m.rejected_parse));
    out.push_str(&format!("rejected classD  {}\n", m.rejected_class_d));
    out.push_str(&format!("memory           {} bytes (~{:.1} MB)\n", total_mem, total_mem as f64 / 1_048_576.0));

    if !m.segment_sizes.is_empty() {
        out.push_str("\nsegment  entries  holes\n");
        for (i, (&sz, &h)) in m.segment_sizes.iter().zip(m.segment_holes.iter()).enumerate() {
            out.push_str(&format!("{:<8} {:<8} {:.2}%\n", i, sz, h * 100.0));
        }
    }

    (StatusCode::OK, [("content-type", "text/plain; charset=utf-8")], out)
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
        name: "percolator",
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
    encoder.encode(&metric_families, &mut buffer).unwrap();

    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        buffer,
    )
}

// ---------------------------------------------------------------------------
// Vocabulary management
// ---------------------------------------------------------------------------

/// GET /_vocab — return the current vocabulary as JSON.
async fn get_vocab(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let engine = state.engine.lock();
    match engine.vocab() {
        Some(v) => Json(serde_json::to_value(v).unwrap_or_default()).into_response(),
        None => Json(serde_json::json!({
            "synonyms": [],
            "phrases": [],
            "graders": [],
            "grade_words": []
        }))
        .into_response(),
    }
}

/// PUT /_vocab — replace the vocabulary. Existing compiled queries become
/// stale; the caller should reingest for consistent matching.
async fn put_vocab(
    State(state): State<Arc<AppState>>,
    Json(vocab): Json<percolator::vocab::Vocab>,
) -> impl IntoResponse {
    let result = {
        let mut engine = state.engine.lock();
        match engine.set_vocab(vocab) {
            Ok(stale) => {
                let mut resp = serde_json::json!({"acknowledged": true, "stale_segments": stale});
                if stale > 0 {
                    resp["warning"] = serde_json::json!(
                        "normalizer changed with existing queries; reingest for consistent matching"
                    );
                }
                Ok((StatusCode::OK, Json(resp)))
            }
            Err(e) => Err((
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e.to_string()})),
            )),
        }
    };
    state.publish_snapshot();
    match result {
        Ok(r) => r,
        Err(r) => r,
    }
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
    let vocab = percolator::vocab::learn_from_queries(&req.queries, req.min_count);
    Json(serde_json::to_value(&vocab).unwrap_or_default())
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
        _ = ctrl_c => {
            info!("received SIGINT (ctrl-c)");
        }
        _ = sigterm => {
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
        "starting percolator server"
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
        let v = percolator::vocab::Vocab::load_json(path).expect("failed to read vocab file");
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

    let build_normalizer = |v: &Option<percolator::vocab::Vocab>| -> Normalizer {
        match v {
            Some(vocab) => vocab.to_normalizer().expect("failed to build normalizer from vocab"),
            None => Normalizer::default_vocab().expect("failed to build normalizer"),
        }
    };

    let mut engine = if cli.data_dir.is_some() {
        let norm = build_normalizer(&vocab);
        match Engine::open(norm, config.clone()) {
            Ok(mut e) => {
                info!(data_dir = ?cli.data_dir.as_ref().unwrap(), "recovered engine from persistence");
                if let Some(v) = vocab {
                    let _ = e.set_vocab(v);
                }
                e
            }
            Err(e) => {
                warn!(error = %e, "no existing data, starting fresh");
                match vocab {
                    Some(v) => Engine::with_vocab(v, config)
                        .expect("failed to build engine from vocab"),
                    None => Engine::with_config(
                        Normalizer::default_vocab().expect("normalizer"),
                        config,
                    ),
                }
            }
        }
    } else {
        match vocab {
            Some(v) => Engine::with_vocab(v, config).expect("failed to build engine from vocab"),
            None => {
                let norm = Normalizer::default_vocab().expect("failed to build normalizer");
                Engine::with_config(norm, config)
            }
        }
    };

    // Create Prometheus metrics and wire the engine observer.
    let prom = PrometheusMetrics::new();
    let prom_for_observer = prom.clone();
    engine.set_observer(move |event: &EngineEvent| {
        // Increment Prometheus counters.
        prom_for_observer.observe_event(event);

        // Emit structured tracing events.
        match event {
            EngineEvent::Flush { entries, base_segments_after } => {
                info!(
                    entries = entries,
                    base_segments_after = base_segments_after,
                    "engine.flush"
                );
            }
            EngineEvent::Ingest { ingested, rejected_parse, rejected_class_d, base_segments_after } => {
                info!(
                    ingested = ingested,
                    rejected_parse = rejected_parse,
                    rejected_class_d = rejected_class_d,
                    base_segments_after = base_segments_after,
                    "engine.ingest"
                );
            }
            EngineEvent::Compaction { report, trigger, base_segments_after } => {
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
                first_error = %result.errors.first().map(|e| e.to_string()).unwrap_or_default(),
                "query file had load errors"
            );
        }
        if !result.queries.is_empty() {
            let report = engine.build_from_queries(&result.queries);
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
        .route("/_bulk", post(bulk_ingest))
        .route("/_flush", post(flush))
        .route("/_compact", post(compact))
        .route("/_stats", get(stats))
        .route("/_cat/stats", get(cat_stats))
        .route("/_health", get(health))
        .route("/_metrics", get(prometheus_metrics))
        .route("/_vocab", get(get_vocab).put(put_vocab))
        .route("/_vocab/learn", post(learn_vocab))
        .layer(DefaultBodyLimit::max(100 * 1024 * 1024)) // 100MB
        .layer(tower::limit::ConcurrencyLimitLayer::new(256))
        .layer(middleware::from_fn(request_id_middleware))
        .with_state(Arc::clone(&state));

    let addr = SocketAddr::from(([0, 0, 0, 0], cli.port));
    info!(
        address = %addr,
        slow_query_threshold_ms = slow_threshold,
        endpoints = "GET /, GET/PUT/DELETE /_doc/{id}, POST /_search, POST /_bulk, GET /_stats, GET /_cat/stats, GET /_health, GET /_metrics, GET/PUT /_vocab, POST /_vocab/learn",
        "server listening"
    );

    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind failed");

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

    let server_fut = axum::serve(listener, app)
        .with_graceful_shutdown(graceful_shutdown);

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
        _ = drain_deadline => {
            // Drain took too long — fall through to cleanup.
        }
    }

    info!(drain_timeout = drain_timeout, "connection drain complete, running shutdown sequence");

    // Flush memtable and log final metrics.
    {
        let mut engine = state.engine.lock();
        let pre_metrics = engine.metrics();

        if pre_metrics.memtable_entries > 0 {
            info!(memtable_entries = pre_metrics.memtable_entries, "flushing memtable before shutdown");
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
