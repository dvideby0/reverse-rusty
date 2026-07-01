//! Cluster-mode admin handlers (ADR-070): read-only introspection (root, stats,
//! `_cat/shards`, health, metrics) + the durability commit points (flush,
//! `POST /_checkpoint`, `POST /_backup`) + the single-node-only `_cat`/compact stubs.
//!
//! The `_cluster/*` control-plane operations (state, node register/deregister,
//! rebalance, handoff, reassign, resize, resync) live in the [`ops`] submodule and are
//! re-exported below, so callers keep resolving them as `admin::cluster_*`.

mod ops;

pub(crate) use ops::{
    cluster_deregister_node, cluster_handoff, cluster_reassign, cluster_rebalance,
    cluster_reconcile, cluster_register_node, cluster_resize, cluster_resync, cluster_state,
};

use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use prometheus::{Encoder, TextEncoder};
use serde::{Deserialize, Serialize};
use tracing::{error, info, instrument};

use crate::state::ClusterAppState;

use super::{not_in_cluster_mode, shard_error_response};

#[derive(Serialize)]
struct ClusterRootResponse {
    name: &'static str,
    version: &'static str,
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
        version: env!("CARGO_PKG_VERSION"),
        mode: "cluster",
        shards: cluster.num_shards(),
        replication_factor: cluster.replication_factor(),
        durable: cluster.is_durable(),
        tagline: "you know, for matching",
    })
}

#[derive(Serialize)]
struct ClusterStatsResponse {
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

#[derive(Serialize)]
struct ClassCounts {
    a: u64,
    b: u64,
    c: u64,
    d: u64,
}

/// GET /_stats — cluster-wide counts.
#[instrument(skip_all)]
pub(crate) async fn cluster_stats(State(state): State<Arc<ClusterAppState>>) -> Response {
    let cluster = state.cluster.read();
    let (total, per_shard, cc) = match (
        cluster.num_queries(),
        cluster.shard_query_counts(),
        cluster.class_counts(),
    ) {
        (Ok(t), Ok(p), Ok(c)) => (t, p, c),
        (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => {
            return shard_error_response("stats unavailable", &e)
        }
    };
    Json(ClusterStatsResponse {
        mode: "cluster",
        shards: cluster.num_shards(),
        replication_factor: cluster.replication_factor(),
        include_broad: state.include_broad,
        durable: cluster.is_durable(),
        total_queries: total,
        shard_queries: per_shard,
        class_counts: ClassCounts {
            a: cc[0],
            b: cc[1],
            c: cc[2],
            d: cc[3],
        },
        epoch: cluster.epoch(),
        pending_repairs: cluster.pending_repairs(),
        has_tagged_queries: cluster.has_tagged_queries(),
    })
    .into_response()
}

/// GET /_cat/shards — per-shard text table (`?format=json` for the JSON shape).
#[instrument(skip_all)]
pub(crate) async fn cluster_cat_shards(
    State(state): State<Arc<ClusterAppState>>,
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Response {
    let cluster = state.cluster.read();
    let counts = match cluster.shard_query_counts() {
        Ok(c) => c,
        Err(e) => return shard_error_response("shard counts unavailable", &e),
    };
    let assignments = cluster
        .control_state()
        .map(|s| s.assignments)
        .unwrap_or_default();
    let node_of = |pos: usize| -> String {
        assignments
            .iter()
            .find(|a| a.position as usize == pos)
            .map_or_else(
                || "-".to_string(),
                |a| {
                    let mut s = a.primary.0.to_string();
                    if !a.replicas.is_empty() {
                        s.push('+');
                        s.push_str(
                            &a.replicas
                                .iter()
                                .map(|r| r.0.to_string())
                                .collect::<Vec<_>>()
                                .join("+"),
                        );
                    }
                    s
                },
            )
    };

    if q.get("format").map(String::as_str) == Some("json") {
        #[derive(Serialize)]
        struct ShardRow {
            shard: usize,
            queries: usize,
            nodes: String,
        }
        let rows: Vec<ShardRow> = counts
            .iter()
            .enumerate()
            .map(|(i, &n)| ShardRow {
                shard: i,
                queries: n,
                nodes: node_of(i),
            })
            .collect();
        return Json(rows).into_response();
    }

    let mut out = String::from("shard queries nodes\n");
    for (i, n) in counts.iter().enumerate() {
        out.push_str(&format!("{i:>5} {n:>7} {}\n", node_of(i)));
    }
    (StatusCode::OK, out).into_response()
}

#[derive(Serialize)]
struct ClusterHealthResponse {
    status: &'static str,
    mode: &'static str,
    shards: usize,
    pending_repairs: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

/// GET /_health — green (all shards answer, no queued repairs), yellow (repairs
/// queued — converging), red (a shard probe fails).
#[instrument(skip_all)]
pub(crate) async fn cluster_health(State(state): State<Arc<ClusterAppState>>) -> Response {
    let cluster = state.cluster.read();
    let shards = cluster.num_shards();
    match cluster.num_queries() {
        Ok(_) => {
            let pending = cluster.pending_repairs();
            let (status, code) = if pending > 0 {
                ("yellow", StatusCode::OK)
            } else {
                ("green", StatusCode::OK)
            };
            (
                code,
                Json(ClusterHealthResponse {
                    status,
                    mode: "cluster",
                    shards,
                    pending_repairs: pending,
                    reason: (pending > 0)
                        .then(|| "partial applies queued; resync converges them".to_string()),
                }),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ClusterHealthResponse {
                status: "red",
                mode: "cluster",
                shards,
                pending_repairs: cluster.pending_repairs(),
                reason: Some(format!("a shard probe failed: {e}")),
            }),
        )
            .into_response(),
    }
}

/// GET /_metrics — Prometheus text exposition. The HTTP/event counters are wired
/// through the observer bridge exactly as in single-node mode; the engine gauges
/// that exist at the cluster level (total queries) refresh on scrape.
#[instrument(skip_all)]
pub(crate) async fn cluster_metrics(State(state): State<Arc<ClusterAppState>>) -> Response {
    {
        let cluster = state.cluster.read();
        if let Ok(n) = cluster.num_queries() {
            state.prom.total_queries.set(n as i64);
        }
        // Cluster gRPC transport metrics (ADR-085) — all-zero for an in-process cluster.
        state.prom.observe_transport(&cluster.transport_metrics());
        // Per-shard stored-query distribution (ADR-091) — best-effort; a transient shard error
        // (e.g. a remote shard mid-handoff) just leaves the prior gauge values in place.
        if let Ok(counts) = cluster.shard_query_counts() {
            state.prom.observe_shard_queries(&counts);
        }
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
        )
            .into_response();
    }
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        buffer,
    )
        .into_response()
}

/// POST /_flush — seal every shard's memtable into an immutable segment.
#[instrument(skip_all)]
pub(crate) async fn cluster_flush(State(state): State<Arc<ClusterAppState>>) -> Response {
    let result = {
        let _w = state.write_serial.lock();
        let cluster = state.cluster.read();
        cluster.flush()
    };
    match result {
        Ok(()) => {
            info!("cluster flush complete");
            Json(serde_json::json!({"acknowledged": true})).into_response()
        }
        Err(e) => shard_error_response("flush failed", &e),
    }
}

/// POST /_checkpoint — the cluster durability commit point (ADR-031/032): seal
/// shards, commit the coordinator manifest, truncate the log. A no-op (still 200)
/// on an in-memory cluster.
#[instrument(skip_all)]
pub(crate) async fn cluster_checkpoint(State(state): State<Arc<ClusterAppState>>) -> Response {
    let start = Instant::now();
    let result = {
        let _w = state.write_serial.lock();
        let cluster = state.cluster.read();
        cluster.checkpoint().map(|()| cluster.epoch())
    };
    match result {
        Ok(epoch) => {
            info!(
                epoch,
                took_ms = start.elapsed().as_millis() as u64,
                "cluster checkpoint complete"
            );
            Json(serde_json::json!({"acknowledged": true, "epoch": epoch})).into_response()
        }
        Err(e) => {
            error!(error = %e, "cluster checkpoint failed");
            shard_error_response("checkpoint failed", &e)
        }
    }
}

#[derive(Deserialize)]
pub(crate) struct BackupBody {
    /// Server-side destination directory for the snapshot. Must not already exist.
    dest: String,
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
    Json(body): Json<BackupBody>,
) -> Response {
    let start = Instant::now();
    let dest = std::path::PathBuf::from(&body.dest);
    let result = {
        let _w = state.write_serial.lock();
        let cluster = state.cluster.read();
        cluster.backup_to(&dest)
    };
    match result {
        Ok(()) => {
            info!(
                dest = %body.dest,
                took_ms = start.elapsed().as_millis() as u64,
                "cluster backup complete"
            );
            Json(serde_json::json!({"acknowledged": true, "dest": body.dest})).into_response()
        }
        Err(e) => {
            error!(dest = %body.dest, error = %e, "cluster backup failed");
            shard_error_response("backup failed", &e)
        }
    }
}

/// GET /_cat/stats — single-node only; the cluster summary is `GET /_stats`.
pub(crate) async fn cluster_cat_stats() -> Response {
    not_in_cluster_mode("GET /_cat/stats", "use GET /_stats or GET /_cat/shards")
}

/// GET /_cat/segments — single-node only (per-shard LSM detail is shard-internal).
pub(crate) async fn cluster_cat_segments() -> Response {
    not_in_cluster_mode(
        "GET /_cat/segments",
        "per-shard segment detail is shard-internal; use GET /_cat/shards for \
         per-shard counts",
    )
}

/// POST /_compact — single-node only; per-shard compaction runs under each shard's
/// own engine policy.
pub(crate) async fn cluster_compact() -> Response {
    not_in_cluster_mode(
        "POST /_compact",
        "per-shard compaction follows each shard engine's policy; use POST /_checkpoint \
         for the cluster durability commit",
    )
}
