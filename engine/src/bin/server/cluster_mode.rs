//! Coordinator-mode startup (ADR-070): assemble a [`ClusterEngine`] from the CLI
//! (in-process build/reopen, or remote connect under the `distributed` feature),
//! wire the observer → Prometheus bridge, build the cluster router over the shared
//! middleware stack, serve, and run the durability shutdown sequence (flush +
//! checkpoint).
//!
//! Durability model by mode (recorded in ADR-070): an in-process `--data-dir`
//! cluster is the ADR-031/032 story (log-first writes, manifest commit at
//! checkpoint, attach-and-mmap reopen). A remote cluster's coordinator is
//! STATELESS — durability lives on the shard nodes (per-shard translog +
//! checkpoint sidecar, ADR-039); a coordinator restart reconnects to the same
//! endpoints and re-ships the deterministically re-minted dict.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::DefaultBodyLimit,
    middleware,
    routing::{get, post},
    Router,
};
use parking_lot::{Mutex, RwLock};
use tracing::{error, info, warn};

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, ShardError};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::events::EngineEvent;
use reverse_rusty::loader;
use reverse_rusty::normalize::Normalizer;

use crate::auth::AuthConfig;
use crate::cli::Cli;
use crate::handlers::{
    cluster_bulk, cluster_cat_segments, cluster_cat_shards, cluster_cat_stats, cluster_checkpoint,
    cluster_compact, cluster_delete_doc, cluster_deregister_node, cluster_flush,
    cluster_get_aliases, cluster_get_doc, cluster_get_settings, cluster_get_vocab, cluster_health,
    cluster_import_aliases, cluster_learn_aliases, cluster_learn_and_apply_vocab,
    cluster_learn_vocab, cluster_metrics, cluster_mpercolate, cluster_put_doc,
    cluster_put_settings, cluster_put_vocab, cluster_rebalance, cluster_register_node,
    cluster_resync, cluster_root, cluster_search, cluster_state, cluster_stats,
};
use crate::metrics::PrometheusMetrics;
use crate::state::{request_id_middleware, ClusterAppState};
use crate::{auth, shutdown_signal};

/// Run the server in coordinator mode. Mirrors `main`'s single-node flow: build
/// the cluster, wire observability, serve, shut down cleanly.
pub(crate) async fn run(cli: Cli, auth_config: Option<AuthConfig>) {
    // Per-shard engine config from the same flags single-node mode maps; the
    // coordinator derives each shard's data dir itself (ADR-032), so data_dir
    // stays unset here.
    let per_shard = EngineConfig {
        data_dir: None,
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
    let problems = per_shard.validate();
    if !problems.is_empty() {
        for p in &problems {
            error!(problem = %p, "invalid engine config");
        }
        std::process::exit(1);
    }
    if cli.accept_class_d {
        warn!(
            "--accept-class-d is inert in cluster mode: the coordinator rejects \
             negation-only queries at placement (the cluster always-candidate lane is \
             ADR-065 criterion 8)"
        );
    }

    let remote_groups: Vec<String> = cli.shard_endpoint.clone();
    let in_process = remote_groups.is_empty();
    if !in_process && cli.data_dir.is_some() {
        error!(
            "--data-dir cannot be combined with --shard-endpoint: a remote coordinator \
             is stateless — durability lives on the shard nodes (shardserver --data-dir)"
        );
        std::process::exit(1);
    }
    if in_process && cli.data_dir.is_none() {
        warn!("no --data-dir specified: cluster is in-memory only, data will not survive restarts");
    }

    let num_shards = if in_process {
        cli.shards
    } else {
        remote_groups.len()
    };
    let cluster_config = ClusterConfig {
        num_shards,
        replication_factor: cli.replication_factor,
        per_shard,
        include_broad: cli.include_broad,
        data_dir: if in_process {
            cli.data_dir.clone()
        } else {
            None
        },
        wal_sync_on_write: cli.wal_sync_on_write,
        ..ClusterConfig::default()
    };

    // Vocabulary → normalizer (the same vocab-file flow as single-node mode).
    let vocab = cli.vocab_file.as_ref().map(|path| {
        info!(path = ?path, "loading vocabulary from file");
        reverse_rusty::vocab::Vocab::load_json(path).expect("failed to read vocab file")
    });
    let norm = match &vocab {
        Some(v) => v
            .to_normalizer()
            .expect("failed to build normalizer from vocab"),
        None => Normalizer::default_vocab().expect("failed to build normalizer"),
    };

    // Pre-load corpus (used by build / ingest; skipped on a populated reopen).
    let load_start = Instant::now();
    let queries: Vec<(u64, String)> = match &cli.load_file {
        Some(path) => {
            info!(path = ?path, "loading queries from file");
            let result = loader::load_file(path).expect("failed to read query file");
            if !result.errors.is_empty() {
                warn!(
                    error_count = result.errors.len(),
                    first_error = %result.errors.first().map(std::string::ToString::to_string).unwrap_or_default(),
                    "query file had load errors"
                );
            }
            result.queries
        }
        None => Vec::new(),
    };

    // Assemble the cluster OFF the runtime workers: build/open are plain sync work,
    // and the gRPC connect path's sync→async bridge must not run on a runtime
    // worker thread (it would nest `block_on`).
    let handle = tokio::runtime::Handle::current();
    let data_dir = cluster_config.data_dir.clone();
    let cfg = cluster_config.clone();
    let assemble = tokio::task::spawn_blocking(move || {
        assemble_cluster(
            in_process,
            &remote_groups,
            data_dir,
            &cfg,
            norm,
            &queries,
            &handle,
        )
    });
    let cluster = match assemble.await.expect("cluster assembly task panicked") {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "failed to assemble cluster; refusing to start");
            std::process::exit(1);
        }
    };
    info!(
        shards = cluster.num_shards(),
        replication_factor = cluster.replication_factor(),
        durable = cluster.is_durable(),
        elapsed_ms = format!("{:.1}", load_start.elapsed().as_secs_f64() * 1000.0),
        "cluster assembled"
    );

    // Prometheus + the observer bridge (the cluster emits Ingest/DurabilityFailure
    // events through the same EngineEvent enum).
    let prom = PrometheusMetrics::new();
    let prom_for_observer = prom.clone();
    cluster.set_observer(Arc::new(move |event: &EngineEvent| {
        prom_for_observer.observe_event(event);
        if let EngineEvent::DurabilityFailure { op, detail, error } = event {
            if op.is_data_at_risk() {
                error!(op = op.as_str(), detail = %detail, error = %error,
                    "cluster.durability_failure: durability degraded");
            } else {
                warn!(op = op.as_str(), detail = %detail, error = %error,
                    "cluster.durability_failure");
            }
        }
    }));

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(cli.threads.unwrap_or(0))
        .build()
        .expect("failed to build rayon thread pool");

    let state = Arc::new(ClusterAppState {
        cluster: RwLock::new(cluster),
        write_serial: Mutex::new(()),
        pool,
        include_broad: cli.include_broad,
        prom,
        slow_query_threshold_ms: cli.slow_query_threshold_ms,
        auth: auth_config,
    });

    let app = Router::new()
        .route("/", get(cluster_root))
        .route(
            "/_doc/{id}",
            get(cluster_get_doc)
                .put(cluster_put_doc)
                .delete(cluster_delete_doc),
        )
        .route("/_search", post(cluster_search))
        .route("/_mpercolate", post(cluster_mpercolate))
        .route("/_bulk", post(cluster_bulk))
        .route("/_flush", post(cluster_flush))
        .route("/_checkpoint", post(cluster_checkpoint))
        .route("/_compact", post(cluster_compact))
        .route("/_stats", get(cluster_stats))
        .route("/_cat/shards", get(cluster_cat_shards))
        .route("/_cat/stats", get(cluster_cat_stats))
        .route("/_cat/segments", get(cluster_cat_segments))
        .route("/_health", get(cluster_health))
        .route("/_metrics", get(cluster_metrics))
        .route("/_vocab", get(cluster_get_vocab).put(cluster_put_vocab))
        .route("/_vocab/learn", post(cluster_learn_vocab))
        .route(
            "/_vocab/learn_and_apply",
            post(cluster_learn_and_apply_vocab),
        )
        .route("/_vocab/aliases", get(cluster_get_aliases))
        .route("/_vocab/aliases/import", post(cluster_import_aliases))
        .route(
            "/_vocab/aliases/learn_and_apply",
            post(cluster_learn_aliases),
        )
        .route(
            "/_settings",
            get(cluster_get_settings).put(cluster_put_settings),
        )
        .route("/_cluster/state", get(cluster_state))
        .route("/_cluster/nodes", post(cluster_register_node))
        .route(
            "/_cluster/nodes/{id}",
            axum::routing::delete(cluster_deregister_node),
        )
        .route("/_cluster/rebalance", post(cluster_rebalance))
        .route("/_cluster/resync", post(cluster_resync))
        .layer(DefaultBodyLimit::max(100 * 1024 * 1024)) // 100MB
        .layer(tower::limit::ConcurrencyLimitLayer::new(256))
        // Auth outside the limiter, exactly as in single-node mode (ADR-062).
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            auth::auth_middleware::<ClusterAppState>,
        ))
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            request_id_middleware::<ClusterAppState>,
        ))
        .with_state(Arc::clone(&state));

    let addr = SocketAddr::new(cli.host, cli.port);
    info!(address = %addr, mode = "cluster", "server listening");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind failed");

    let signal_received = Arc::new(tokio::sync::Notify::new());
    let signal_received2 = Arc::clone(&signal_received);
    let graceful_shutdown = async move {
        shutdown_signal().await;
        signal_received2.notify_one();
    };
    let server_fut = axum::serve(listener, app).with_graceful_shutdown(graceful_shutdown);
    let drain_timeout = cli.drain_timeout;
    let drain_deadline = async {
        signal_received.notified().await;
        tokio::time::sleep(tokio::time::Duration::from_secs(drain_timeout)).await;
        warn!(drain_timeout, "drain timeout exceeded, forcing shutdown");
    };

    tokio::select! {
        result = server_fut => {
            if let Err(e) = result {
                error!(error = %e, "server error");
            }
        }
        () = drain_deadline => {}
    }

    info!("connection drain complete, running cluster shutdown sequence");

    // Durability shutdown: flush + checkpoint (the manifest commit), so reopen
    // attaches segments instead of replaying a long log tail. In-memory clusters
    // flush only (checkpoint is a no-op there anyway).
    {
        let _w = state.write_serial.lock();
        let cluster = state.cluster.read();
        if let Err(e) = cluster.flush() {
            error!(error = %e, "shutdown flush failed");
        }
        if cluster.is_durable() {
            match cluster.checkpoint() {
                Ok(()) => info!(epoch = cluster.epoch(), "shutdown checkpoint committed"),
                Err(e) => error!(error = %e, "shutdown checkpoint failed"),
            }
        }
    }
    info!("shutdown complete");
}

/// Assemble the `ClusterEngine` for the chosen backend: reopen an existing durable
/// in-process cluster, build a fresh one, or (under the `distributed` feature)
/// connect remote shard endpoints, ship the frozen feature space, and bulk-load.
fn assemble_cluster(
    in_process: bool,
    remote_groups: &[String],
    data_dir: Option<PathBuf>,
    cfg: &ClusterConfig,
    norm: Normalizer,
    queries: &[(u64, String)],
    handle: &tokio::runtime::Handle,
) -> Result<ClusterEngine, ShardError> {
    if in_process {
        let _ = handle; // only the remote path connects on the runtime
        if let Some(dir) = data_dir.filter(|d| ClusterEngine::cluster_exists(d)) {
            info!(data_dir = ?dir, "reopening durable cluster from manifest");
            let cluster = ClusterEngine::open(dir, norm, Some(cfg))?;
            if !queries.is_empty() {
                match cluster.num_queries()? {
                    0 => cluster.ingest(queries)?,
                    n => warn!(
                        existing = n,
                        "skipping --load-file: the reopened cluster is already populated"
                    ),
                }
            }
            return Ok(cluster);
        }
        return ClusterEngine::build(norm, cfg, queries);
    }

    #[cfg(feature = "distributed")]
    {
        connect_remote_cluster(remote_groups, cfg, norm, queries, handle)
    }
    #[cfg(not(feature = "distributed"))]
    {
        let _ = (remote_groups, handle);
        Err(ShardError::Config(
            "--shard-endpoint requires a server built with --features distributed \
             (the gRPC RemoteShard transport is compiled out of this binary)"
                .into(),
        ))
    }
}

/// Connect a coordinator over remote `shardserver` endpoints: mint + freeze the
/// feature space over the load corpus (pass A of `build`, so a restart re-mints the
/// identical dict and the ADR-034 fingerprint handshake holds), connect (the dict +
/// tag space ship at connect), then bulk-load an empty cluster.
#[cfg(feature = "distributed")]
fn connect_remote_cluster(
    remote_groups: &[String],
    cfg: &ClusterConfig,
    norm: Normalizer,
    queries: &[(u64, String)],
    handle: &tokio::runtime::Handle,
) -> Result<ClusterEngine, ShardError> {
    use reverse_rusty::cluster::ShardGroup;

    let groups: Vec<ShardGroup> = remote_groups
        .iter()
        .map(|g| {
            let mut parts = g.split(',').map(str::trim).map(str::to_string);
            let primary = parts.next().unwrap_or_default();
            ShardGroup {
                primary,
                replicas: parts.collect(),
            }
        })
        .collect();
    if groups.iter().any(|g| g.primary.is_empty()) {
        return Err(ShardError::Config(
            "every --shard-endpoint needs a primary endpoint (got an empty one)".into(),
        ));
    }

    let (dict, tag_dict) = ClusterEngine::freeze_feature_space(&norm, queries, &[]);
    let norm = Arc::new(norm);
    let dict = Arc::new(dict);
    let tag_dict = Arc::new(tag_dict);

    let plain = groups.iter().all(|g| g.replicas.is_empty());
    let cluster = if plain && cfg.replication_factor == 1 {
        let endpoints: Vec<String> = groups.into_iter().map(|g| g.primary).collect();
        ClusterEngine::connect_remote(norm, dict, tag_dict, cfg, &endpoints, handle)?
    } else {
        ClusterEngine::connect_replicated(norm, dict, tag_dict, cfg, &groups, handle)?
    };

    if !queries.is_empty() {
        match cluster.num_queries()? {
            0 => cluster.ingest(queries)?,
            n => warn!(
                existing = n,
                "skipping --load-file: the remote cluster is already populated"
            ),
        }
    }
    Ok(cluster)
}
