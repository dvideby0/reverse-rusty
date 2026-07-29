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
    routing::{any, get, post, put},
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
    alias_discover_method_not_allowed, alias_discover_record_method_not_allowed,
    alias_feedback_apply_method_not_allowed, alias_feedback_read_method_not_allowed,
    alias_feedback_reset_method_not_allowed, alias_import_method_not_allowed,
    alias_learn_apply_method_not_allowed, alias_read_method_not_allowed, cluster_backup,
    cluster_bulk_route, cluster_cancel_job, cluster_cat_segments, cluster_cat_shards,
    cluster_cat_stats, cluster_checkpoint, cluster_compact, cluster_create_job_route,
    cluster_delete_doc, cluster_deregister_node, cluster_discover_aliases,
    cluster_discover_and_record_aliases, cluster_flush_route, cluster_gc,
    cluster_get_alias_feedback, cluster_get_aliases, cluster_get_doc, cluster_get_job,
    cluster_get_job_stream, cluster_get_settings, cluster_get_vocab, cluster_handoff,
    cluster_health, cluster_import_aliases, cluster_learn_aliases, cluster_learn_and_apply_vocab,
    cluster_learn_vocab, cluster_metrics, cluster_mpercolate_route, cluster_put_doc,
    cluster_put_settings, cluster_put_vocab, cluster_reassign, cluster_rebalance,
    cluster_reconcile, cluster_register_node, cluster_reset_alias_feedback, cluster_resize,
    cluster_resync, cluster_root, cluster_search_route, cluster_state, cluster_stats,
    cluster_v2_mpercolate_route, cluster_v2_search_route, cluster_validate_and_apply_feedback,
    settings_method_not_allowed, vocab_learn_apply_method_not_allowed,
    vocab_learn_method_not_allowed, vocab_method_not_allowed, ALIAS_DISCOVER_BODY_LIMIT,
    ALIAS_DISCOVER_RECORD_BODY_LIMIT, ALIAS_FEEDBACK_APPLY_BODY_LIMIT,
    ALIAS_FEEDBACK_READ_BODY_LIMIT, ALIAS_FEEDBACK_RESET_BODY_LIMIT, ALIAS_IMPORT_BODY_LIMIT,
    ALIAS_LEARN_APPLY_BODY_LIMIT, ALIAS_READ_BODY_LIMIT, BACKUP_BODY_LIMIT,
    CAT_SEGMENTS_BODY_LIMIT, CAT_SHARDS_BODY_LIMIT, CHECKPOINT_BODY_LIMIT,
    CLUSTER_NODE_DEREGISTER_BODY_LIMIT, CLUSTER_NODE_REGISTER_BODY_LIMIT, CLUSTER_STATE_BODY_LIMIT,
    EXHAUSTIVE_JOB_BODY_LIMIT, HEALTH_BODY_LIMIT, METRICS_BODY_LIMIT, PIT_BODY_LIMIT,
    SETTINGS_READ_BODY_LIMIT, SETTINGS_WRITE_BODY_LIMIT, STATS_BODY_LIMIT,
    VOCAB_LEARN_APPLY_BODY_LIMIT, VOCAB_LEARN_BODY_LIMIT, VOCAB_READ_BODY_LIMIT,
    VOCAB_WRITE_BODY_LIMIT,
};
use crate::metrics::PrometheusMetrics;
use crate::state::{request_id_middleware, ClusterAppState};
use crate::{auth, shutdown_signal};

/// Remote-coordinator assembly (connect + control-plane attach + route-by-assignments), split out to
/// keep this file within the module-size budget (ADR-086).
#[cfg(feature = "distributed")]
mod remote_connect;

/// The unattended re-point reconcile loop (ADR-092), split out to keep this file within the
/// module-size budget. `distributed`-gated: it drives the data-moving reconcile.
#[cfg(feature = "distributed")]
mod reconcile_loop;

/// Run the server in coordinator mode. Mirrors `main`'s single-node flow: build
/// the cluster, wire observability, serve, shut down cleanly.
pub(crate) async fn run(
    cli: Cli,
    auth_config: Option<AuthConfig>,
    rank_profiles: Arc<reverse_rusty::RankProfiles>,
) {
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
        max_tags: cli.max_tags,
        wal_sync_on_write: cli.wal_sync_on_write,
        retain_source: cli.retain_source,
        broad_batch_size: cli.broad_batch_size,
        hot_anchor_threshold: cli.hot_anchor_threshold,
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
    let remote_groups: Vec<String> = cli.shard_endpoint.clone();
    // A coordinator routing by committed assignments with ONLY --control-endpoint (no --shard-endpoint)
    // is a REMOTE cluster that resolves its shard endpoints from the durable quorum (ADR-086
    // resolve-only boot — the quorum must already be seeded). Otherwise remote mode is defined by the
    // presence of --shard-endpoint.
    let resolve_only =
        cli.route_by_assignments && remote_groups.is_empty() && !cli.control_endpoint.is_empty();
    let in_process = remote_groups.is_empty() && !resolve_only;
    // accept_class_d drives the cluster always-candidate lane (ADR-080): the coordinator places
    // class-D on the broad lane (replicated to every shard). The COORDINATOR is the SOLE gate — a
    // remote `ShardServer` is coordinator-gated storage (`LocalShard` forces accept_class_d on
    // every shard it builds, so it stores whatever the coordinator places), and therefore needs no
    // flag of its own. (An earlier warning here told operators to set a nonexistent `shardserver
    // --accept-class-d`, describing a drop that LocalShard makes impossible — see the
    // cluster_grpc_oracle class-D test, which proves a default-config shard still serves class-D.)
    if in_process
        && (cli.grpc_tls_ca.is_some()
            || cli.grpc_tls_domain.is_some()
            || cli.cluster_token.is_some())
    {
        error!(
            "--grpc-tls-ca/--grpc-tls-domain/--cluster-token apply to the gRPC mesh links              and require --shard-endpoint (remote mode)"
        );
        std::process::exit(1);
    }
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
    // The hot tier (ADR-105) is classified SHARD-SIDE in remote mode: each shardserver's
    // own θ decides whether a coordinator-placed query lands in its realtime lane or its
    // hot tier. Divergence is cost-only (both lanes always-visible; placement θ-invariant)
    // but silently defeats the quarantine — remind the operator of the contract.
    if !in_process && cli.hot_anchor_threshold != 0 {
        warn!(
            theta = cli.hot_anchor_threshold,
            "--hot-anchor-threshold in remote cluster mode: ensure every shardserver runs              the same --hot-anchor-threshold (divergence is cost-only, never correctness)"
        );
    }
    // --control-endpoint attaches the coordinator to a durable control-plane quorum (ADR-083). It is
    // only meaningful for a REMOTE cluster: an in-process cluster owns the one logical node, so its
    // in-memory control plane already IS the source of truth. Fail loud rather than silently ignore.
    if in_process && !cli.control_endpoint.is_empty() {
        error!(
            "--control-endpoint requires --shard-endpoint (remote mode): an in-process cluster uses \
             its own in-memory control plane"
        );
        std::process::exit(1);
    }
    // --route-by-assignments makes the committed quorum the topology source of truth (ADR-086), so it
    // requires a control plane to read. With --shard-endpoint it seeds + routes; with only
    // --control-endpoint it resolves the topology from the quorum (resolve-only boot).
    if cli.route_by_assignments && cli.control_endpoint.is_empty() {
        error!(
            "--route-by-assignments requires --control-endpoint: the committed quorum is the \
             topology source of truth (ADR-086)"
        );
        std::process::exit(1);
    }
    // --reconcile-interval-secs runs the unattended reconciler (ADR-092), which re-points routing by
    // MOVING data to the committed map's owner. It is only safe + meaningful when the coordinator
    // actually ROUTES by that committed map — otherwise a converged map would not change routing.
    // Require --route-by-assignments (which itself requires --control-endpoint), so a misconfiguration
    // refuses startup rather than running a loop that moves data the coordinator then ignores.
    if cli.reconcile_interval_secs.is_some() && !cli.route_by_assignments {
        error!(
            "--reconcile-interval-secs requires --route-by-assignments: the reconciler converges the \
             committed shard→node map the coordinator routes by (ADR-092/086)"
        );
        std::process::exit(1);
    }

    // The ring size: --shards for an in-process OR a resolve-only-boot cluster (validated against the
    // quorum's committed num_shards on attach), else the --shard-endpoint count.
    let num_shards = if remote_groups.is_empty() {
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

    // Mesh client security for the remote links (ADR-071), resolved fail-loud HERE so a
    // misconfiguration refuses startup. Kept as plain bytes — the typed ClientSecurity is
    // built inside the distributed-gated connect path.
    let mesh = MeshClientParts {
        ca: cli.grpc_tls_ca.as_ref().map(|p| {
            std::fs::read(p).unwrap_or_else(|e| {
                error!(path = ?p, error = %e, "cannot read --grpc-tls-ca");
                std::process::exit(1);
            })
        }),
        domain: cli.grpc_tls_domain.clone(),
        token: match crate::auth::AuthConfig::resolve(
            cli.cluster_token.clone(),
            std::env::var("RR_CLUSTER_TOKEN"),
            false,
        ) {
            Ok(t) => t.map(|a| a.token_bytes().to_vec()),
            Err(e) => {
                error!(error = %e, "invalid mesh cluster token");
                std::process::exit(1);
            }
        },
        connect_timeout_secs: cli.grpc_connect_timeout_secs,
        read_timeout_secs: cli.grpc_read_timeout_secs,
        write_timeout_secs: cli.grpc_write_timeout_secs,
        keepalive_secs: cli.grpc_keepalive_secs,
        read_retries: cli.grpc_read_retries,
    };
    if mesh.token.is_some() && mesh.ca.is_none() {
        warn!(
            "--cluster-token without --grpc-tls-ca: the mesh secret crosses the wire in              cleartext; configure mesh TLS (ADR-071)"
        );
    }

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
    let control_endpoints: Vec<String> = cli.control_endpoint.clone();
    let route_by_assignments = cli.route_by_assignments;
    let assemble = tokio::task::spawn_blocking(move || {
        assemble_cluster(
            in_process,
            &remote_groups,
            data_dir,
            &cfg,
            norm,
            vocab,
            &queries,
            &handle,
            mesh,
            &control_endpoints,
            route_by_assignments,
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
    let ranked_workers = pool.current_num_threads().max(1);
    let exhaustive_jobs = crate::jobs::ExhaustiveJobs::new(
        crate::jobs::ExhaustiveJobConfig {
            threads: cli.exhaustive_threads,
            max_concurrent: cli.max_concurrent_exhaustive_jobs,
            chunk_size: cli.exhaustive_chunk_size,
            channel_depth: cli.exhaustive_channel_depth,
            max_timeout: std::time::Duration::from_secs(cli.exhaustive_job_timeout_secs),
            max_retained: cli.max_retained_exhaustive_jobs,
        },
        prom.clone(),
    )
    .unwrap_or_else(|reason| {
        error!(%reason, "invalid exhaustive-job configuration");
        std::process::exit(1);
    });

    let state = Arc::new(ClusterAppState {
        cluster: RwLock::new(cluster),
        topology_guard: RwLock::new(()),
        write_serial: Mutex::new(()),
        flush_serial: Mutex::new(()),
        durability_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_CONCURRENT_CLUSTER_DURABILITY_OPERATIONS,
        )),
        health_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_CONCURRENT_HEALTH_REQUESTS,
        )),
        stats_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_CONCURRENT_STATS,
        )),
        pool,
        search_permits: (cli.max_concurrent_searches > 0)
            .then(|| std::sync::Arc::new(tokio::sync::Semaphore::new(cli.max_concurrent_searches))),
        ranked_search_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(ranked_workers)),
        exhaustive_jobs,
        rank_profiles,
        max_ranked_enrichment_bytes: cli.max_ranked_enrichment_bytes,
        include_broad: cli.include_broad,
        prom,
        slow_query_threshold_ms: cli.slow_query_threshold_ms,
        auth: auth_config,
        pit_tokens: crate::pit::PitTokens::generate(),
        pit_config: reverse_rusty::PitConfig {
            default_keep_alive: std::time::Duration::from_secs(cli.pit_default_keep_alive_secs),
            max_keep_alive: std::time::Duration::from_secs(cli.pit_max_keep_alive_secs),
            max_open: cli.max_open_pits,
        },
    });

    let app = Router::new()
        .route("/", get(cluster_root))
        .route(
            "/_doc/{id}",
            get(cluster_get_doc)
                .put(cluster_put_doc)
                .delete(cluster_delete_doc),
        )
        .route(
            "/_search",
            get(cluster_search_route).post(cluster_search_route),
        )
        .route("/v2/_search", post(cluster_v2_search_route))
        .route("/v2/_mpercolate", post(cluster_v2_mpercolate_route))
        .route(
            "/v2/_pit",
            post(crate::handlers::cluster_open_pit_route)
                .delete(crate::handlers::cluster_close_pit_route)
                .layer(DefaultBodyLimit::max(PIT_BODY_LIMIT)),
        )
        .route(
            "/_percolate/jobs",
            post(cluster_create_job_route).layer(DefaultBodyLimit::max(EXHAUSTIVE_JOB_BODY_LIMIT)),
        )
        .route(
            "/_percolate/jobs/{id}",
            get(cluster_get_job).delete(cluster_cancel_job),
        )
        .route("/_percolate/jobs/{id}/stream", any(cluster_get_job_stream))
        .route("/_mpercolate", post(cluster_mpercolate_route))
        .route("/_bulk", post(cluster_bulk_route))
        .route("/_flush", any(cluster_flush_route))
        .route(
            "/_checkpoint",
            any(cluster_checkpoint).layer(DefaultBodyLimit::max(CHECKPOINT_BODY_LIMIT)),
        )
        .route(
            "/_backup",
            any(cluster_backup).layer(DefaultBodyLimit::max(BACKUP_BODY_LIMIT)),
        )
        .route("/_compact", post(cluster_compact))
        .route("/_forcemerge", post(cluster_compact))
        .route(
            "/_stats",
            any(cluster_stats).layer(DefaultBodyLimit::max(STATS_BODY_LIMIT)),
        )
        .route(
            "/_cat/shards",
            any(cluster_cat_shards).layer(DefaultBodyLimit::max(CAT_SHARDS_BODY_LIMIT)),
        )
        .route("/_cat/stats", get(cluster_cat_stats))
        .route(
            "/_cat/segments",
            any(cluster_cat_segments).layer(DefaultBodyLimit::max(CAT_SEGMENTS_BODY_LIMIT)),
        )
        .route(
            "/_health",
            any(cluster_health).layer(DefaultBodyLimit::max(HEALTH_BODY_LIMIT)),
        )
        .route(
            "/_metrics",
            any(cluster_metrics).layer(DefaultBodyLimit::max(METRICS_BODY_LIMIT)),
        )
        .route(
            "/_vocab",
            get(cluster_get_vocab)
                .layer(DefaultBodyLimit::max(VOCAB_READ_BODY_LIMIT))
                .merge(put(cluster_put_vocab).layer(DefaultBodyLimit::max(VOCAB_WRITE_BODY_LIMIT)))
                .fallback(vocab_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/learn",
            post(cluster_learn_vocab)
                .layer(DefaultBodyLimit::max(VOCAB_LEARN_BODY_LIMIT))
                .fallback(vocab_learn_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/learn_and_apply",
            post(cluster_learn_and_apply_vocab)
                .layer(DefaultBodyLimit::max(VOCAB_LEARN_APPLY_BODY_LIMIT))
                .fallback(vocab_learn_apply_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/aliases",
            get(cluster_get_aliases)
                .layer(DefaultBodyLimit::max(ALIAS_READ_BODY_LIMIT))
                .fallback(alias_read_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/aliases/import",
            post(cluster_import_aliases)
                .layer(DefaultBodyLimit::max(ALIAS_IMPORT_BODY_LIMIT))
                .fallback(alias_import_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/aliases/learn_and_apply",
            post(cluster_learn_aliases)
                .layer(DefaultBodyLimit::max(ALIAS_LEARN_APPLY_BODY_LIMIT))
                .fallback(alias_learn_apply_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/aliases/discover",
            post(cluster_discover_aliases)
                .layer(DefaultBodyLimit::max(ALIAS_DISCOVER_BODY_LIMIT))
                .fallback(alias_discover_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/aliases/discover_and_record",
            post(cluster_discover_and_record_aliases)
                .layer(DefaultBodyLimit::max(ALIAS_DISCOVER_RECORD_BODY_LIMIT))
                .fallback(alias_discover_record_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/aliases/feedback",
            get(cluster_get_alias_feedback)
                .layer(DefaultBodyLimit::max(ALIAS_FEEDBACK_READ_BODY_LIMIT))
                .fallback(alias_feedback_read_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/aliases/feedback/reset",
            post(cluster_reset_alias_feedback)
                .layer(DefaultBodyLimit::max(ALIAS_FEEDBACK_RESET_BODY_LIMIT))
                .fallback(alias_feedback_reset_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/aliases/validate_and_apply",
            post(cluster_validate_and_apply_feedback)
                .layer(DefaultBodyLimit::max(ALIAS_FEEDBACK_APPLY_BODY_LIMIT))
                .fallback(alias_feedback_apply_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_settings",
            get(cluster_get_settings)
                .layer(DefaultBodyLimit::max(SETTINGS_READ_BODY_LIMIT))
                .merge(
                    put(cluster_put_settings)
                        .layer(DefaultBodyLimit::max(SETTINGS_WRITE_BODY_LIMIT)),
                )
                .fallback(settings_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_cluster/state",
            any(cluster_state).layer(DefaultBodyLimit::max(CLUSTER_STATE_BODY_LIMIT)),
        )
        .route(
            "/_cluster/state/{metric}",
            any(cluster_state).layer(DefaultBodyLimit::max(CLUSTER_STATE_BODY_LIMIT)),
        )
        .route(
            "/_cluster/state/{metric}/{target}",
            any(cluster_state).layer(DefaultBodyLimit::max(CLUSTER_STATE_BODY_LIMIT)),
        )
        .route(
            "/_cluster/nodes",
            any(cluster_register_node)
                .layer(DefaultBodyLimit::max(CLUSTER_NODE_REGISTER_BODY_LIMIT)),
        )
        .route(
            "/_cluster/nodes/{id}",
            any(cluster_deregister_node)
                .layer(DefaultBodyLimit::max(CLUSTER_NODE_DEREGISTER_BODY_LIMIT)),
        )
        .route("/_cluster/rebalance", post(cluster_rebalance))
        .route("/_cluster/reassign", post(cluster_reassign))
        .route("/_cluster/reconcile", post(cluster_reconcile))
        .route("/_cluster/gc", post(cluster_gc))
        .route("/_cluster/resize", post(cluster_resize))
        .route("/_cluster/resync", post(cluster_resync))
        .route("/_cluster/handoff", post(cluster_handoff))
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

    // ADR-092: the opt-in unattended reconcile loop (distributed-only — it drives the data-moving
    // reconcile). Spawned only when --reconcile-interval-secs is set (the guard above already required
    // --route-by-assignments); held so it can be aborted at the start of the shutdown sequence, before
    // the durability flush, so a pass never starts racing the checkpoint. Default (unset) ⇒ never
    // spawned ⇒ byte-identical.
    #[cfg(feature = "distributed")]
    let reconcile_task = cli.reconcile_interval_secs.map(|secs| {
        let cfg = reverse_rusty::cluster::ReconcileConfig {
            enabled: true,
            rf: cli.replication_factor,
            min_interval: std::time::Duration::from_secs(secs.max(1)),
            max_parallel_moves: cli.reconcile_max_parallel.max(1),
            gc_orphans: cli.reconcile_gc_orphans,
        };
        reconcile_loop::spawn_reconcile_loop(Arc::clone(&state), &cfg)
    });

    tokio::select! {
        result = server_fut => {
            if let Err(e) = result {
                error!(error = %e, "server error");
            }
        }
        () = drain_deadline => {}
    }

    let cancelled_jobs = state.exhaustive_jobs.cancel_all();
    if cancelled_jobs > 0 {
        info!(
            cancelled_jobs,
            "cancelled exhaustive jobs before cluster shutdown cleanup"
        );
    }

    // Stop the reconcile loop before the durability flush: a pass already on the blocking pool finishes
    // its current move-then-commit safely (handoff tolerates concurrent flushes, ADR-044), but no new
    // pass starts racing the checkpoint.
    #[cfg(feature = "distributed")]
    if let Some(task) = reconcile_task {
        info!("stopping reconcile loop");
        task.abort();
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

mod assemble;

use assemble::{assemble_cluster, MeshClientParts};
