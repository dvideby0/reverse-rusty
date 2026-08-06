//! Cluster-mode admin handlers (ADR-070): read-only introspection (root, stats,
//! `_cat/shards`, health, metrics) + flush/backup durability operations + the
//! single-node-only `_cat`/compact stubs. The strict `POST /_checkpoint`
//! boundary lives in the sibling `checkpoint` module.
//!
//! The strict reconcile, handoff, reassign, rebalance, resync, and resize
//! boundaries live in their named modules. Strict
//! cluster-state reads and node-descriptor
//! mutations live in sibling modules; orphan-slot GC lives in [`gc`] (ADR-096).

mod cat_shards;
mod gc;
mod handoff;
mod health;
mod metrics;
mod reassign;
mod rebalance;
mod reconcile;
mod resize;
mod resync;

pub(crate) use cat_shards::{cluster_cat_shards, CAT_SHARDS_BODY_LIMIT};
pub(crate) use gc::cluster_gc;
#[cfg(test)]
pub(crate) use handoff::CLUSTER_HANDOFF_BODY_TIMEOUT;
pub(crate) use handoff::{cluster_handoff, CLUSTER_HANDOFF_BODY_LIMIT};
pub(crate) use health::cluster_health;
pub(crate) use metrics::cluster_metrics;
#[cfg(test)]
pub(crate) use reassign::CLUSTER_REASSIGN_BODY_TIMEOUT;
pub(crate) use reassign::{cluster_reassign, CLUSTER_REASSIGN_BODY_LIMIT};
#[cfg(test)]
pub(crate) use rebalance::CLUSTER_REBALANCE_BODY_TIMEOUT;
pub(crate) use rebalance::{cluster_rebalance, CLUSTER_REBALANCE_BODY_LIMIT};
#[cfg(test)]
pub(crate) use reconcile::CLUSTER_RECONCILE_BODY_TIMEOUT;
pub(crate) use reconcile::{cluster_reconcile, CLUSTER_RECONCILE_BODY_LIMIT};
#[cfg(test)]
pub(crate) use resize::CLUSTER_RESIZE_BODY_TIMEOUT;
pub(crate) use resize::{cluster_resize, CLUSTER_RESIZE_BODY_LIMIT};
#[cfg(test)]
pub(crate) use resync::CLUSTER_RESYNC_BODY_TIMEOUT;
pub(crate) use resync::{cluster_resync, CLUSTER_RESYNC_BODY_LIMIT};

use std::sync::Arc;
use std::time::Instant;

use axum::{
    body::Bytes,
    extract::{
        rejection::{BytesRejection, QueryRejection},
        Query, RawQuery, State,
    },
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use tracing::{error, info, instrument};

use crate::dto::ApiVersion;
use crate::handlers::admin::{
    acquire_flush, finish_cat_segments_response, finish_stats_response, stats_rejection,
    validate_cat_segments_method, validate_cat_segments_request, validate_flush_method,
    validate_flush_request, validate_stats_method, validate_stats_request, CatSegmentsParams,
    FlushParams, FlushResponse, StatsShards,
};
use crate::handlers::backup::{
    acquire_backup_permit, backup_error_response, backup_rejection, validate_backup_method,
    validate_backup_request, BackupResponse,
};
use crate::state::ClusterAppState;

use super::{not_in_cluster_mode, shard_error_response, shard_error_status};

#[derive(Serialize)]
struct ClusterRootResponse {
    name: &'static str,
    cluster_name: &'static str,
    cluster_uuid: &'static str,
    version: ApiVersion,
    mode: &'static str,
    shards: usize,
    replication_factor: usize,
    durable: bool,
    tagline: &'static str,
}

/// GET / — cluster-mode root.
pub(crate) async fn cluster_root(State(state): State<Arc<ClusterAppState>>) -> impl IntoResponse {
    let cluster = state.cluster.read();
    Json(ClusterRootResponse {
        name: "reverse-rusty",
        cluster_name: "reverse-rusty",
        cluster_uuid: "_na_",
        version: ApiVersion::current(),
        mode: "cluster",
        shards: cluster.num_shards(),
        replication_factor: cluster.replication_factor(),
        durable: cluster.is_durable(),
        tagline: "you know, for matching",
    })
}

#[derive(Serialize)]
struct ClusterStatsResponse {
    took: u64,
    took_ms: f64,
    #[serde(rename = "_shards")]
    shard_result: StatsShards,
    mode: &'static str,
    shards: usize,
    replication_factor: usize,
    include_broad: bool,
    durable: bool,
    /// Physical entries across shards (a replicated/any-of query counts once per
    /// holding shard; includes tombstoned entries, like single-node `total_queries`).
    total_queries: usize,
    shard_queries: Vec<usize>,
    class_counts: ClassCounts,
    /// Checkpoint generation (bumped by `POST /_checkpoint`).
    epoch: u64,
    /// Mutations queued for partial-apply repair (ADR-047) — 0 on a healthy cluster.
    pending_repairs: usize,
    /// Whether any stored query carries tags (the `set_vocab` refusal condition).
    has_tagged_queries: bool,
}

impl ClusterStatsResponse {
    fn set_took(&mut self, took_ms: f64) {
        self.took = took_ms.floor() as u64;
        self.took_ms = took_ms;
    }
}

#[derive(Serialize)]
struct ClassCounts {
    a: u64,
    b: u64,
    c: u64,
    d: u64,
    /// The hot tier (class H, ADR-105) — 0 while `hot_anchor_threshold` is off.
    h: u64,
}

/// GET /_stats — cluster-wide counts.
#[instrument(skip_all)]
pub(crate) async fn cluster_stats(
    State(state): State<Arc<ClusterAppState>>,
    method: Method,
    raw_query: RawQuery,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let _duration = state
        .prom
        .http_request_duration
        .with_label_values(&["stats"])
        .start_timer();
    let started = Instant::now();
    if let Err(response) = validate_stats_method(&state.prom, &method) {
        return *response;
    }
    if let Err(response) = validate_stats_request(&state.prom, raw_query, body) {
        return *response;
    }
    let Ok(permit) = Arc::clone(&state.stats_permits).acquire_owned().await else {
        return stats_rejection(
            &state.prom,
            StatusCode::SERVICE_UNAVAILABLE,
            "stats_unavailable",
            "stats admission is closed",
        );
    };
    let worker_state = Arc::clone(&state);
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let cluster = worker_state.cluster.read();
        // One count pass is enough: the aggregate is the sum of the returned
        // per-position rows. The old path called every shard twice.
        let per_shard = cluster.shard_query_counts()?;
        let total = per_shard
            .iter()
            .copied()
            .fold(0usize, usize::saturating_add);
        let cc = cluster.class_counts()?;
        let shards = cluster.num_shards();
        Ok::<_, reverse_rusty::cluster::ShardError>(ClusterStatsResponse {
            took: 0,
            took_ms: 0.0,
            shard_result: StatsShards {
                total: shards,
                successful: shards,
                failed: 0,
            },
            mode: "cluster",
            shards,
            replication_factor: cluster.replication_factor(),
            include_broad: worker_state.include_broad,
            durable: cluster.is_durable(),
            total_queries: total,
            shard_queries: per_shard,
            class_counts: ClassCounts {
                a: cc[0],
                b: cc[1],
                c: cc[2],
                d: cc[3],
                h: cc[4],
            },
            epoch: cluster.epoch(),
            pending_repairs: cluster.pending_repairs(),
            has_tagged_queries: cluster.has_tagged_queries(),
        })
    });
    match worker.await {
        Ok(Ok(mut stats)) => {
            stats.set_took(started.elapsed().as_secs_f64() * 1000.0);
            finish_stats_response(&state.prom, Json(stats).into_response())
        }
        Ok(Err(error)) => finish_stats_response(
            &state.prom,
            shard_error_response("stats unavailable", &error),
        ),
        Err(join_error) => {
            error!(error = %join_error, "cluster stats worker failed");
            stats_rejection(
                &state.prom,
                StatusCode::INTERNAL_SERVER_ERROR,
                "stats_unavailable",
                "cluster stats worker failed",
            )
        }
    }
}

/// GET/POST `/_flush` — seal every shard's memtable into an immutable segment.
#[instrument(skip_all)]
pub(crate) async fn cluster_flush_route(
    State(state): State<Arc<ClusterAppState>>,
    method: Method,
    params: Result<Query<FlushParams>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let _duration = state
        .prom
        .http_request_duration
        .with_label_values(&["flush"])
        .start_timer();
    let started = Instant::now();
    if let Err(response) = validate_flush_method(&state.prom, &method) {
        return *response;
    }
    let params = match validate_flush_request(&state.prom, params, body) {
        Ok(params) => params,
        Err(response) => return *response,
    };
    let force = params.force_requested();
    let _flush = match acquire_flush(&state.flush_serial, params, &state.prom) {
        Ok(guard) => guard,
        Err(response) => return *response,
    };
    let (shards, result) = {
        let _w = state.write_serial.lock();
        let cluster = state.cluster.read();
        (cluster.num_shards(), cluster.flush())
    };
    match result {
        Ok(()) => {
            let took_ms = started.elapsed().as_secs_f64() * 1_000.0;
            info!(force, shards, took_ms, "cluster flush complete");
            state
                .prom
                .http_requests_total
                .with_label_values(&["flush", "200"])
                .inc();
            Json(FlushResponse::new(
                took_ms, true, shards, shards, None, None,
            ))
            .into_response()
        }
        Err(e) => {
            let status = shard_error_status(&e);
            error!(force, error = %e, "cluster flush failed");
            state
                .prom
                .http_requests_total
                .with_label_values(&["flush", status.as_str()])
                .inc();
            shard_error_response("flush failed", &e)
        }
    }
}

/// POST /_backup — snapshot the cluster's durable state into `dest`, a server-side
/// path that must not already exist (ADR-079): checkpoint, then copy the coordinator
/// manifest + per-shard segments + `sources.dat` + the coordinator log. Restore by
/// pointing a fresh coordinator at the copy via `--data-dir`. Replicas are rebuilt on
/// open, so they are not copied.
///
/// Holds the writer-serialization mutex + the cluster READ lock across the checkpoint
/// AND the copy (mirroring `cluster_checkpoint`), so no concurrent mutation or shard
/// compaction runs during the snapshot; reads keep flowing off the shard snapshots.
/// An in-memory cluster (no `--data-dir`) is a 400.
#[instrument(skip_all)]
pub(crate) async fn cluster_backup(
    State(state): State<Arc<ClusterAppState>>,
    method: Method,
    raw_query: RawQuery,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let _duration = state
        .prom
        .http_request_duration
        .with_label_values(&["backup"])
        .start_timer();
    let started = Instant::now();
    if let Err(response) = validate_backup_method(&state.prom, &method) {
        return *response;
    }
    let prepared = match validate_backup_request(&state.prom, raw_query, &headers, body) {
        Ok(prepared) => prepared,
        Err(response) => return *response,
    };
    let Ok(permit) = acquire_backup_permit(&state.durability_permits).await else {
        error!("cluster backup admission unexpectedly closed");
        return backup_rejection(
            &state.prom,
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "backup admission unavailable",
        );
    };
    let work_state = Arc::clone(&state);
    let dest = prepared.path;
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let _writer = work_state.write_serial.lock();
        let cluster = work_state.cluster.read();
        cluster.backup_to(&dest).map(|()| cluster.epoch())
    });
    let prom = state.prom.clone();
    let dest_label = prepared.dest.clone();
    let reporter = tokio::spawn(async move {
        let result = match worker.await {
            Ok(result) => result,
            Err(join_error) => {
                error!(error = %join_error, "cluster backup worker failed");
                prom.http_requests_total
                    .with_label_values(&["backup", "500"])
                    .inc();
                return Err(join_error);
            }
        };
        let took_ms = started.elapsed().as_secs_f64() * 1_000.0;
        match &result {
            Ok(epoch) => {
                info!(
                    dest = %dest_label,
                    took_ms,
                    epoch,
                    "cluster backup complete"
                );
                prom.http_requests_total
                    .with_label_values(&["backup", "200"])
                    .inc();
            }
            Err(error) => {
                let status = shard_error_status(error);
                error!(dest = %dest_label, error = %error, "cluster backup failed");
                prom.http_requests_total
                    .with_label_values(&["backup", status.as_str()])
                    .inc();
            }
        }
        Ok((result, took_ms))
    });
    let (result, took_ms) = match reporter.await {
        Ok(Ok(completion)) => completion,
        Ok(Err(join_error)) => {
            return backup_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                format!("cluster backup worker failed: {join_error}"),
            );
        }
        Err(join_error) => {
            error!(error = %join_error, "cluster backup completion reporter failed");
            return backup_rejection(
                &state.prom,
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "cluster backup completion reporter failed",
            );
        }
    };
    match result {
        Ok(epoch) => Json(BackupResponse::new(took_ms, prepared.dest, Some(epoch))).into_response(),
        Err(error) => shard_error_response("backup failed", &error),
    }
}

/// GET /_cat/stats — single-node only; the cluster summary is `GET /_stats`.
pub(crate) async fn cluster_cat_stats() -> Response {
    not_in_cluster_mode("GET /_cat/stats", "use GET /_stats or GET /_cat/shards")
}

/// GET /_cat/segments — single-node only (per-shard LSM detail is shard-internal).
#[instrument(skip_all)]
pub(crate) async fn cluster_cat_segments(
    State(state): State<Arc<ClusterAppState>>,
    method: Method,
    params: Result<Query<CatSegmentsParams>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let _duration = state
        .prom
        .http_request_duration
        .with_label_values(&["cat_segments"])
        .start_timer();
    if let Err(response) = validate_cat_segments_method(&state.prom, &method) {
        return *response;
    }
    if let Err(response) = validate_cat_segments_request(&state.prom, params, body) {
        return *response;
    }
    finish_cat_segments_response(
        &state.prom,
        not_in_cluster_mode(
            "GET /_cat/segments",
            "per-shard segment detail is shard-internal; use GET /_cat/shards for \
             per-shard counts",
        ),
    )
}

/// POST /_compact or /_forcemerge — standalone only; per-shard compaction runs
/// under each shard's own engine policy.
pub(crate) async fn cluster_compact() -> Response {
    not_in_cluster_mode(
        "POST /_compact or /_forcemerge",
        "per-shard compaction follows each shard engine's policy; use POST /_checkpoint \
         for the cluster durability commit",
    )
}
