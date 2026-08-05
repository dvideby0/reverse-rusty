//! Handler tests for coordinator mode (ADR-070): drive the cluster router with
//! tower `oneshot` requests over a real in-process multi-shard `ClusterEngine`.

use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::http::{HeaderMap, Request, StatusCode};
use axum::routing::{any, get, post};
use axum::Router;
use parking_lot::{Mutex, RwLock};
use tower::ServiceExt;

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, ShardError};
use reverse_rusty::Normalizer;

use crate::metrics::PrometheusMetrics;
use crate::state::ClusterAppState;

use super::*;

fn test_state(queries: &[(u64, String)]) -> Arc<ClusterAppState> {
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..Default::default()
    };
    let cluster = ClusterEngine::build(Normalizer::default_vocab().expect("vocab"), &cfg, queries)
        .expect("cluster builds");
    state_from_cluster(cluster)
}

fn state_from_cluster(cluster: ClusterEngine) -> Arc<ClusterAppState> {
    state_from_cluster_with_rebalance_topology(
        cluster,
        crate::state::ClusterRebalanceTopology::InProcess,
    )
}

fn state_from_cluster_with_rebalance_topology(
    cluster: ClusterEngine,
    rebalance_topology: crate::state::ClusterRebalanceTopology,
) -> Arc<ClusterAppState> {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .expect("pool");
    let prom = PrometheusMetrics::new();
    Arc::new(ClusterAppState {
        cluster: RwLock::new(cluster),
        topology_guard: RwLock::new(()),
        write_serial: Mutex::new(()),
        flush_serial: Mutex::new(()),
        durability_permits: Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_CONCURRENT_CLUSTER_DURABILITY_OPERATIONS,
        )),
        rebalance_permits: Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_CONCURRENT_CLUSTER_REBALANCES,
        )),
        rebalance_topology,
        health_permits: Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_CONCURRENT_HEALTH_REQUESTS,
        )),
        stats_permits: Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_CONCURRENT_STATS,
        )),
        pool,
        search_permits: None,
        ranked_search_permits: Arc::new(tokio::sync::Semaphore::new(2)),
        exhaustive_jobs: crate::jobs::ExhaustiveJobs::for_tests(prom.clone()),
        rank_profiles: Arc::new(reverse_rusty::RankProfiles::default()),
        max_ranked_enrichment_bytes: crate::state::DEFAULT_MAX_RANKED_ENRICHMENT_BYTES,
        include_broad: true,
        prom,
        slow_query_threshold_ms: 0,
        auth: None,
        pit_tokens: crate::pit::PitTokens::generate(),
        pit_config: reverse_rusty::PitConfig::default(),
    })
}

fn router(state: &Arc<ClusterAppState>) -> Router {
    Router::new()
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
        .route(
            "/v2/_search",
            post(crate::handlers::cluster_v2_search_route),
        )
        .route(
            "/v2/_mpercolate",
            post(crate::handlers::cluster_v2_mpercolate_route),
        )
        .route(
            "/v2/_pit",
            post(crate::handlers::cluster_open_pit_route)
                .delete(crate::handlers::cluster_close_pit_route)
                .layer(axum::extract::DefaultBodyLimit::max(
                    crate::handlers::PIT_BODY_LIMIT,
                )),
        )
        .route(
            "/_percolate/jobs",
            post(crate::handlers::cluster_create_job_route).layer(
                axum::extract::DefaultBodyLimit::max(crate::handlers::EXHAUSTIVE_JOB_BODY_LIMIT),
            ),
        )
        .route(
            "/_percolate/jobs/{id}",
            get(crate::handlers::cluster_get_job).delete(crate::handlers::cluster_cancel_job),
        )
        .route(
            "/_percolate/jobs/{id}/stream",
            any(crate::handlers::cluster_get_job_stream),
        )
        .route("/_mpercolate", post(cluster_mpercolate_route))
        .route("/_bulk", post(cluster_bulk_route))
        .route("/_flush", any(cluster_flush_route))
        .route(
            "/_checkpoint",
            any(cluster_checkpoint)
                .layer(axum::extract::DefaultBodyLimit::max(CHECKPOINT_BODY_LIMIT)),
        )
        .route(
            "/_backup",
            any(cluster_backup).layer(axum::extract::DefaultBodyLimit::max(
                crate::handlers::BACKUP_BODY_LIMIT,
            )),
        )
        .route("/_compact", post(cluster_compact))
        .route("/_forcemerge", post(cluster_compact))
        .route(
            "/_stats",
            any(cluster_stats).layer(axum::extract::DefaultBodyLimit::max(
                crate::handlers::STATS_BODY_LIMIT,
            )),
        )
        .route(
            "/_cat/shards",
            any(cluster_cat_shards).layer(axum::extract::DefaultBodyLimit::max(
                crate::handlers::CAT_SHARDS_BODY_LIMIT,
            )),
        )
        .route(
            "/_cat/segments",
            any(cluster_cat_segments).layer(axum::extract::DefaultBodyLimit::max(
                crate::handlers::CAT_SEGMENTS_BODY_LIMIT,
            )),
        )
        .route(
            "/_health",
            any(cluster_health).layer(axum::extract::DefaultBodyLimit::max(
                crate::handlers::HEALTH_BODY_LIMIT,
            )),
        )
        .route(
            "/_metrics",
            any(cluster_metrics).layer(axum::extract::DefaultBodyLimit::max(
                crate::handlers::METRICS_BODY_LIMIT,
            )),
        )
        .route(
            "/_vocab",
            get(cluster_get_vocab)
                .layer(axum::extract::DefaultBodyLimit::max(
                    crate::handlers::VOCAB_READ_BODY_LIMIT,
                ))
                .merge(axum::routing::put(cluster_put_vocab).layer(
                    axum::extract::DefaultBodyLimit::max(crate::handlers::VOCAB_WRITE_BODY_LIMIT),
                ))
                .fallback(crate::handlers::vocab_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/learn",
            post(cluster_learn_vocab)
                .layer(axum::extract::DefaultBodyLimit::max(
                    crate::handlers::VOCAB_LEARN_BODY_LIMIT,
                ))
                .fallback(crate::handlers::vocab_learn_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/learn_and_apply",
            post(cluster_learn_and_apply_vocab)
                .layer(axum::extract::DefaultBodyLimit::max(
                    crate::handlers::VOCAB_LEARN_APPLY_BODY_LIMIT,
                ))
                .fallback(crate::handlers::vocab_learn_apply_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/aliases",
            get(cluster_get_aliases)
                .layer(axum::extract::DefaultBodyLimit::max(
                    crate::handlers::ALIAS_READ_BODY_LIMIT,
                ))
                .fallback(crate::handlers::alias_read_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/aliases/import",
            post(cluster_import_aliases)
                .layer(axum::extract::DefaultBodyLimit::max(
                    crate::handlers::ALIAS_IMPORT_BODY_LIMIT,
                ))
                .fallback(crate::handlers::alias_import_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/aliases/learn_and_apply",
            post(cluster_learn_aliases)
                .layer(axum::extract::DefaultBodyLimit::max(
                    crate::handlers::ALIAS_LEARN_APPLY_BODY_LIMIT,
                ))
                .fallback(crate::handlers::alias_learn_apply_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/aliases/discover",
            post(cluster_discover_aliases)
                .layer(axum::extract::DefaultBodyLimit::max(
                    crate::handlers::ALIAS_DISCOVER_BODY_LIMIT,
                ))
                .fallback(crate::handlers::alias_discover_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_vocab/aliases/discover_and_record",
            post(cluster_discover_and_record_aliases)
                .layer(axum::extract::DefaultBodyLimit::max(
                    crate::handlers::ALIAS_DISCOVER_RECORD_BODY_LIMIT,
                ))
                .fallback(
                    crate::handlers::alias_discover_record_method_not_allowed::<ClusterAppState>,
                ),
        )
        .route(
            "/_vocab/aliases/feedback",
            get(cluster_get_alias_feedback)
                .layer(axum::extract::DefaultBodyLimit::max(
                    crate::handlers::ALIAS_FEEDBACK_READ_BODY_LIMIT,
                ))
                .fallback(
                    crate::handlers::alias_feedback_read_method_not_allowed::<ClusterAppState>,
                ),
        )
        .route(
            "/_vocab/aliases/feedback/reset",
            post(cluster_reset_alias_feedback)
                .layer(axum::extract::DefaultBodyLimit::max(
                    crate::handlers::ALIAS_FEEDBACK_RESET_BODY_LIMIT,
                ))
                .fallback(
                    crate::handlers::alias_feedback_reset_method_not_allowed::<ClusterAppState>,
                ),
        )
        .route(
            "/_vocab/aliases/validate_and_apply",
            post(cluster_validate_and_apply_feedback)
                .layer(axum::extract::DefaultBodyLimit::max(
                    crate::handlers::ALIAS_FEEDBACK_APPLY_BODY_LIMIT,
                ))
                .fallback(
                    crate::handlers::alias_feedback_apply_method_not_allowed::<ClusterAppState>,
                ),
        )
        .route(
            "/_settings",
            get(cluster_get_settings)
                .layer(axum::extract::DefaultBodyLimit::max(
                    crate::handlers::SETTINGS_READ_BODY_LIMIT,
                ))
                .merge(axum::routing::put(cluster_put_settings).layer(
                    axum::extract::DefaultBodyLimit::max(
                        crate::handlers::SETTINGS_WRITE_BODY_LIMIT,
                    ),
                ))
                .fallback(crate::handlers::settings_method_not_allowed::<ClusterAppState>),
        )
        .route(
            "/_cluster/state",
            any(cluster_state).layer(axum::extract::DefaultBodyLimit::max(
                CLUSTER_STATE_BODY_LIMIT,
            )),
        )
        .route(
            "/_cluster/state/{metric}",
            any(cluster_state).layer(axum::extract::DefaultBodyLimit::max(
                CLUSTER_STATE_BODY_LIMIT,
            )),
        )
        .route(
            "/_cluster/state/{metric}/{target}",
            any(cluster_state).layer(axum::extract::DefaultBodyLimit::max(
                CLUSTER_STATE_BODY_LIMIT,
            )),
        )
        .route(
            "/_cluster/nodes",
            any(cluster_register_node).layer(axum::extract::DefaultBodyLimit::max(
                CLUSTER_NODE_REGISTER_BODY_LIMIT,
            )),
        )
        .route(
            "/_cluster/nodes/{id}",
            any(cluster_deregister_node).layer(axum::extract::DefaultBodyLimit::max(
                CLUSTER_NODE_DEREGISTER_BODY_LIMIT,
            )),
        )
        .route(
            "/_cluster/rebalance",
            any(cluster_rebalance).layer(axum::extract::DefaultBodyLimit::max(
                CLUSTER_REBALANCE_BODY_LIMIT,
            )),
        )
        .route(
            "/_cluster/resize",
            any(cluster_resize).layer(axum::extract::DefaultBodyLimit::max(
                CLUSTER_RESIZE_BODY_LIMIT,
            )),
        )
        .route("/_cluster/resync", post(cluster_resync))
        .with_state(Arc::clone(state))
}

fn req(method: &str, path: &str, body: &serde_json::Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("request")
}

fn req_empty(method: &str, path: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path)
        .body(Body::empty())
        .expect("request")
}

async fn send(state: &Arc<ClusterAppState>, r: Request<Body>) -> (StatusCode, serde_json::Value) {
    let (status, _, bytes) = send_raw(state, r).await;
    let json = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    };
    (status, json)
}

async fn send_raw(
    state: &Arc<ClusterAppState>,
    r: Request<Body>,
) -> (StatusCode, HeaderMap, Bytes) {
    let resp = router(state).oneshot(r).await.expect("router response");
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("body");
    (status, headers, bytes)
}

fn seed() -> Vec<(u64, String)> {
    vec![
        (1, "1994 acme".to_string()),
        (2, "1995 vertex".to_string()),
        (3, "(rarezza,uniquor)".to_string()),
    ]
}

mod admin;
mod alias_discover;
mod alias_discover_record;
mod alias_feedback_read;
mod alias_feedback_reset;
mod alias_feedback_validate;
mod alias_import;
mod alias_learn_apply;
mod backup;
mod bulk;
mod cat_shards;
mod checkpoint;
mod crud;
mod flush;
mod health;
mod jobs;
mod metrics;
mod node_deregister;
mod node_register;
mod pit;
mod ranked;
mod rebalance;
mod resize;
mod settings_read;
mod settings_write;
mod state_read;
mod v2;
mod vocab;
