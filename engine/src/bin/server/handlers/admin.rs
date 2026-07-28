//! Operational endpoints: flush/compact, the JSON and `_cat` stats views, health,
//! the API root, and the Prometheus exposition. These project engine introspection
//! ([`reverse_rusty::events`] metrics + per-segment info) into the REST surface.

pub(crate) mod cat_table;
mod compact;
mod flush;
mod health;
mod metrics;
mod segments;
mod stats;

pub(crate) use compact::{compact_route, force_merge_route};
pub(crate) use flush::{
    acquire_flush, flush_route, validate_flush_method, validate_flush_request, FlushParams,
    FlushResponse,
};
pub(crate) use health::{
    finish_health_response, health, health_rejection, validate_health_request, wait_delay,
    HealthParams, HealthStatus, HealthTransport, HEALTH_BODY_LIMIT,
};
pub(crate) use metrics::{
    encode_metrics, finish_metrics_response, metrics_rejection, prometheus_metrics,
    MetricsTransport, METRICS_BODY_LIMIT,
};
pub(crate) use segments::{
    cat_segments, finish_cat_segments_response, validate_cat_segments_method,
    validate_cat_segments_request, CatSegmentsParams, CAT_SEGMENTS_BODY_LIMIT,
};
pub(crate) use stats::{
    cat_stats, finish_stats_response, stats, stats_rejection, validate_stats_method,
    validate_stats_request, StatsShards, STATS_BODY_LIMIT,
};

use axum::{response::IntoResponse, Json};
use serde::Serialize;

use crate::dto::ApiVersion;

// -- GET /
#[derive(Serialize)]
struct RootResponse {
    name: &'static str,
    cluster_name: &'static str,
    cluster_uuid: &'static str,
    version: ApiVersion,
    tagline: &'static str,
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

#[cfg(test)]
mod root_tests;

#[cfg(test)]
mod flush_tests;

#[cfg(test)]
mod compact_tests;

#[cfg(test)]
mod health_tests;

#[cfg(test)]
mod metrics_tests;
