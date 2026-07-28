//! Operational endpoints: flush/compact, the JSON and `_cat` stats views, health,
//! the API root, and the Prometheus exposition. These project engine introspection
//! ([`reverse_rusty::events`] metrics + per-segment info) into the REST surface.

pub(crate) mod cat_table;
mod compact;
mod flush;
mod segments;
mod stats;

pub(crate) use compact::{compact_route, force_merge_route};
pub(crate) use flush::{
    acquire_flush, flush_route, validate_flush_method, validate_flush_request, FlushParams,
    FlushResponse,
};
pub(crate) use segments::{
    cat_segments, finish_cat_segments_response, validate_cat_segments_method,
    validate_cat_segments_request, CatSegmentsParams, CAT_SEGMENTS_BODY_LIMIT,
};
pub(crate) use stats::{
    cat_stats, finish_stats_response, stats, stats_rejection, validate_stats_method,
    validate_stats_request, StatsShards, STATS_BODY_LIMIT,
};

use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use prometheus::{Encoder, TextEncoder};
use serde::Serialize;
use tracing::error;

use crate::dto::ApiVersion;
use crate::state::AppState;

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
    cluster_name: &'static str,
    cluster_uuid: &'static str,
    version: ApiVersion,
    tagline: &'static str,
}

/// GET /_health
pub(crate) async fn health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
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
pub(crate) async fn api_root() -> impl IntoResponse {
    Json(RootResponse {
        name: "reverse-rusty",
        cluster_name: "reverse-rusty",
        // Reverse Rusty does not yet persist a cluster identity. `_na_` is the
        // established ES sentinel and is more honest than a UUID that changes
        // at every restart.
        cluster_uuid: "_na_",
        version: ApiVersion::current(),
        tagline: "you know, for matching",
    })
}

/// GET /_metrics — Prometheus text exposition format.
///
/// On each scrape, refreshes gauge metrics from an EngineMetrics snapshot,
/// then encodes all registered metrics.
pub(crate) async fn prometheus_metrics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
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

#[cfg(test)]
mod root_tests;

#[cfg(test)]
mod flush_tests;

#[cfg(test)]
mod compact_tests;
