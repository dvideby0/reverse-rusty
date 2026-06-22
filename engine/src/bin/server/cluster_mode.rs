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
    cluster_backup, cluster_bulk, cluster_cat_segments, cluster_cat_shards, cluster_cat_stats,
    cluster_checkpoint, cluster_compact, cluster_delete_doc, cluster_deregister_node,
    cluster_flush, cluster_get_aliases, cluster_get_doc, cluster_get_settings, cluster_get_vocab,
    cluster_handoff, cluster_health, cluster_import_aliases, cluster_learn_aliases,
    cluster_learn_and_apply_vocab, cluster_learn_vocab, cluster_metrics, cluster_mpercolate,
    cluster_put_doc, cluster_put_settings, cluster_put_vocab, cluster_rebalance,
    cluster_register_node, cluster_resize, cluster_resync, cluster_root, cluster_search,
    cluster_state, cluster_stats,
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
        max_tags: cli.max_tags,
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
    let remote_groups: Vec<String> = cli.shard_endpoint.clone();
    let in_process = remote_groups.is_empty();
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
        .route("/_backup", post(cluster_backup))
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

/// The mesh client-security pieces as plain bytes (ADR-071) — typed
/// `ClientSecurity` is built inside the distributed-gated connect path, so the
/// default (non-distributed) build never names the gated types.
struct MeshClientParts {
    ca: Option<Vec<u8>>,
    /// Consumed only by the distributed-gated connect path, hence the gated allowance.
    #[cfg_attr(not(feature = "distributed"), allow(dead_code))]
    domain: Option<String>,
    token: Option<Vec<u8>>,
}

/// Assemble the `ClusterEngine` for the chosen backend: reopen an existing durable
/// in-process cluster, build a fresh one, or (under the `distributed` feature)
/// connect remote shard endpoints, ship the frozen feature space, and bulk-load.
#[allow(clippy::too_many_arguments)]
fn assemble_cluster(
    in_process: bool,
    remote_groups: &[String],
    data_dir: Option<PathBuf>,
    cfg: &ClusterConfig,
    norm: Normalizer,
    vocab: Option<reverse_rusty::vocab::Vocab>,
    queries: &[(u64, String)],
    handle: &tokio::runtime::Handle,
    mesh: MeshClientParts,
    control_endpoints: &[String],
) -> Result<ClusterEngine, ShardError> {
    if in_process {
        let _ = (handle, mesh, control_endpoints); // only the remote path connects on the runtime
        if let Some(dir) = data_dir.filter(|d| ClusterEngine::cluster_exists(d)) {
            info!(data_dir = ?dir, "reopening durable cluster from manifest");
            // The manifest's persisted vocab is authoritative on a reopen (it matches
            // the committed segments); the file-supplied one only derived `norm`.
            let mut cluster = ClusterEngine::open(dir, norm, Some(cfg))?;
            if let Some(v) = vocab {
                if cluster.vocab().is_some() {
                    info!(
                        "--vocab-file ignored on reopen: the manifest's persisted \
                         vocabulary is authoritative (change it via PUT /_vocab)"
                    );
                } else if cluster.num_queries()? == 0 {
                    // A bare manifest (no persisted vocab) + an EMPTY corpus: activate
                    // the file vocab so this reopen behaves exactly like a fresh
                    // `build_with_vocab` — `set_vocab` installs the equivalence/alias
                    // machinery and its own durable checkpoint persists the vocab,
                    // BEFORE any --load-file ingest below (codex: this path used to
                    // ingest with the rules silently inert and the next reopen lost
                    // the file's vocabulary entirely).
                    info!("activating the vocab file on the empty reopened cluster");
                    cluster.set_vocab(v)?;
                } else {
                    warn!(
                        "--vocab-file NOT applied: this reopened cluster is populated \
                         and its manifest carries no vocabulary, so the file's \
                         equivalence/alias rules stay inactive (only its \
                         normalizer-level rules derived `norm`). Apply it explicitly \
                         via PUT /_vocab (a full blue/green rebuild)."
                    );
                }
            }
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
        // A vocab FILE must fully activate (ADR-076): `build_with_vocab` installs the
        // equivalence/alias machinery on the minted dict (a bare-normalizer build
        // would leave declared equivalences + registry aliases silently inert) and
        // persists the vocab in the manifest from the first durable commit.
        return match vocab {
            Some(v) => ClusterEngine::build_with_vocab(v, cfg, queries),
            None => ClusterEngine::build(norm, cfg, queries),
        };
    }
    // Remote shard servers run the STOCK normalizer (`shardserver` has no vocab flag)
    // and `AdoptDict` ships only the frozen dict — NO mechanism ships a normalizer
    // across processes. ANY vocab file would therefore split the feature space:
    // equivalence-driven rules would be silently inert, and even normalizer-level
    // rules (synonyms/phrases/punctuation/number-context) would have the coordinator
    // extracting queries and routing under a normalizer the shards' title side does
    // not run — cross-process query/title normalizer divergence, silent cross-form
    // false negatives (codex review broadened this from the equivalence-only check).
    // ADR-076 records the refusal: vocabulary on a remote cluster is deploy-time
    // configuration, and v1 ships no mechanism to deploy one.
    if vocab.is_some() {
        return Err(ShardError::Config(
            "a --vocab-file cannot apply to a REMOTE cluster (ADR-076): remote shard \
             servers run the stock normalizer and are not shipped vocabulary, so \
             queries and titles would normalize differently across processes (silent \
             false negatives — even for plain synonyms/phrases/punctuation). Remove \
             the vocab file or run the cluster in-process (--shards K)."
                .into(),
        ));
    }

    #[cfg(feature = "distributed")]
    {
        let security = reverse_rusty::cluster::ClientSecurity {
            tls: mesh
                .ca
                .map(|ca_pem| reverse_rusty::cluster::TlsClientConfig {
                    ca_pem,
                    domain: mesh.domain,
                }),
            token: mesh.token,
        };
        connect_remote_cluster(
            remote_groups,
            cfg,
            norm,
            queries,
            handle,
            security,
            control_endpoints,
        )
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
    security: reverse_rusty::cluster::ClientSecurity,
    control_endpoints: &[String],
) -> Result<ClusterEngine, ShardError> {
    use reverse_rusty::cluster::{RemoteControlPlane, ShardGroup};

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
        ClusterEngine::connect_remote_with_security(
            norm,
            dict,
            tag_dict,
            cfg,
            &endpoints,
            handle,
            security.clone(),
        )?
    } else {
        ClusterEngine::connect_replicated_with_security(
            norm,
            dict,
            tag_dict,
            cfg,
            &groups,
            handle,
            security.clone(),
        )?
    };

    // Attach the durable control-plane quorum (ADR-083) BEFORE any ingest: the coordinator becomes a
    // thin `ControlService` client, so membership/assignment/resize decisions go through the quorum
    // (durable + HA across coordinator restarts) instead of the in-memory backend. The control plane
    // is off the matching hot path, so this never affects a percolate's result (zero FN risk).
    let cluster = match control_endpoints.first() {
        Some(endpoint) => {
            info!(endpoint = %endpoint, "attaching coordinator to durable control-plane quorum");
            let rcp = RemoteControlPlane::connect(endpoint, handle.clone(), security)
                .map_err(|e| {
                    ShardError::ControlPlane(format!("connect control plane {endpoint}: {e}"))
                })?;
            cluster.with_control_plane(Box::new(rcp))
        }
        None => cluster,
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
