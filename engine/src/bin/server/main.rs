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
//!   POST /_vocab/learn       Learn synonyms from raw query text (returns them)
//!   POST /_vocab/learn_and_apply  Learn synonyms from stored queries + apply (?min_count=N)
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
//!
//! Module layout (this file is the entry point — CLI parse, engine build, router
//! wiring, graceful shutdown):
//!   * [`cli`]     — command-line flags ([`cli::Cli`]).
//!   * [`auth`]    — opt-in bearer-token gate for mutating/admin endpoints (ADR-062).
//!   * [`metrics`] — Prometheus registry + the `EngineEvent` → counter bridge.
//!   * [`state`]   — [`state::AppState`] + the request-id / in-flight middleware.
//!   * [`dto`]     — response types shared across handlers (errors, `_source`).
//!   * [`handlers`] — the endpoint handlers, grouped by family (doc/search/admin/vocab),
//!     each owning its endpoint-specific request/response DTOs.

mod auth;
mod cli;
mod dto;
mod handlers;
mod metrics;
mod state;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use axum::{
    extract::DefaultBodyLimit,
    middleware,
    routing::{get, post},
    Router,
};
use clap::Parser;
use parking_lot::Mutex;
use tracing::{error, info, warn};

use reverse_rusty::config::EngineConfig;
use reverse_rusty::events::EngineEvent;
use reverse_rusty::loader;
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::Engine;

use cli::Cli;
use handlers::{
    api_root, bulk_ingest, cat_segments, cat_stats, compact, delete_doc, flush, get_aliases,
    get_doc, get_settings, get_vocab, health, import_aliases, learn_and_apply_aliases,
    learn_and_apply_vocab, learn_vocab, mpercolate, prometheus_metrics, put_doc, put_settings,
    put_vocab, search, stats,
};
use metrics::PrometheusMetrics;
use state::{request_id_middleware, AppState};

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

    // Resolve bearer-token auth (ADR-062). Fail loud on an invalid config —
    // never fall back to silently serving open. The token itself is never
    // logged.
    let auth_config = match auth::AuthConfig::resolve(
        cli.auth_token.clone(),
        std::env::var("RR_AUTH_TOKEN"),
        cli.auth_protect_reads,
    ) {
        Ok(a) => a,
        Err(e) => {
            error!(error = %e, "invalid auth configuration");
            std::process::exit(1);
        }
    };
    match &auth_config {
        Some(a) if a.protect_reads => {
            info!("bearer-token auth enabled (all endpoints except /_health)");
        }
        Some(_) => info!("bearer-token auth enabled (mutating/admin endpoints)"),
        None => {
            if !cli.host.is_loopback() {
                warn!(
                    host = %cli.host,
                    "binding a non-loopback interface without auth: mutating endpoints are \
                     open to the network (set RR_AUTH_TOKEN/--auth-token or front with an \
                     authenticating reverse proxy)"
                );
            }
        }
    }

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
        accept_class_d: cli.accept_class_d,
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
        // A vocab-built engine opens via open_with_vocab so the vocab's equivalence groups are
        // installed BEFORE the WAL tail is replayed — the equivalence map is transient, so an
        // open + adopt_vocab sequence would recompile the recovered tail without alias
        // expansion, a recovery false negative (codex R13).
        let opened = match vocab {
            Some(v) => Engine::open_with_vocab(v, config.clone()),
            None => Engine::open(build_normalizer(&None), config.clone()),
        };
        match opened {
            Ok(e) => {
                info!(data_dir = ?data_dir, "recovered engine from persistence");
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
                    reanchored = report.reanchored,
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
        auth: auth_config,
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
        .route("/_vocab/learn_and_apply", post(learn_and_apply_vocab))
        .route("/_vocab/aliases", get(get_aliases))
        .route("/_vocab/aliases/import", post(import_aliases))
        .route(
            "/_vocab/aliases/learn_and_apply",
            post(learn_and_apply_aliases),
        )
        .route("/_settings", get(get_settings).put(put_settings))
        .layer(DefaultBodyLimit::max(100 * 1024 * 1024)) // 100MB
        .layer(tower::limit::ConcurrencyLimitLayer::new(256))
        // Auth sits OUTSIDE the concurrency limiter: an unauthenticated flood
        // is rejected by a cheap header compare without consuming the 256
        // slots that protect the engine for legitimate traffic.
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            auth::auth_middleware,
        ))
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            request_id_middleware,
        ))
        .with_state(Arc::clone(&state));

    let addr = SocketAddr::new(cli.host, cli.port);
    info!(
        address = %addr,
        slow_query_threshold_ms = slow_threshold,
        endpoints = "GET /, GET/PUT/DELETE /_doc/{id}, POST /_search, POST /_mpercolate, POST /_bulk, GET /_stats, GET /_cat/stats, GET /_health, GET /_metrics, GET/PUT /_vocab, POST /_vocab/learn, POST /_vocab/learn_and_apply, GET /_vocab/aliases, POST /_vocab/aliases/import, POST /_vocab/aliases/learn_and_apply",
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
