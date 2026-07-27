//! Strict native `GET /_stats` transport and bounded collection.

use std::sync::Arc;
use std::time::Instant;

use axum::{
    body::Bytes,
    extract::{rejection::BytesRejection, RawQuery, State},
    http::{header, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use tracing::{error, instrument};

use reverse_rusty::segment::EngineSnapshot;

use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::AppState;

#[derive(Serialize)]
struct EngineStatsResponse {
    took: u64,
    took_ms: f64,
    #[serde(rename = "_shards")]
    shards: StatsShards,
    mode: &'static str,
    /// Physical stored rows, including tombstoned copies.
    total_queries: usize,
    /// Live rows after applying the liveness overlay.
    live_queries: usize,
    /// Physical rows retained as tombstones until compaction.
    tombstoned_queries: usize,
    base_segments: usize,
    memtable_entries: usize,
    dict_features: usize,
    rejected_parse: u64,
    rejected_class_d: u64,
    /// Observe-first hot-tier telemetry (Broad-Query Cost Program): accepted
    /// compiles since process start that would reclassify to the hot tier under
    /// the default hot-anchor threshold.
    would_be_hot: u64,
    /// Canonical-body dedup telemetry (Stage A): accepted compiles, how many
    /// joined an existing per-segment body group, and a linear-counting
    /// estimate of DISTINCT bodies seen (global — the cross-segment potential).
    dedup: DedupStats,
    class_counts: ClassCounts,
    /// Posting-length percentiles per candidate-index lane (nearest-rank; a fat
    /// main `max` against a modest `p99` is the top-64 rank-cliff fingerprint).
    postings: PostingLanes,
    segment_sizes: Vec<usize>,
    segment_holes: Vec<f64>,
    memory: MemoryStats,
    /// ES/OpenSearch-familiar projection of the native WAL backlog.
    translog: TranslogStats,
}

impl EngineStatsResponse {
    fn set_took(&mut self, took_ms: f64) {
        self.took = took_ms.floor() as u64;
        self.took_ms = took_ms;
    }
}

#[derive(Serialize)]
pub(crate) struct StatsShards {
    pub(crate) total: usize,
    pub(crate) successful: usize,
    pub(crate) failed: usize,
}

#[derive(Serialize)]
struct DedupStats {
    bodies_total: u64,
    dup_joined: u64,
    distinct_bodies_est: u64,
}

#[derive(Serialize)]
struct ClassCounts {
    a: u64,
    b: u64,
    c: u64,
    d: u64,
    /// The hot tier (class H, ADR-105) — θ-hot-anchored, always-visible,
    /// columnar-evaluated. 0 while `hot_anchor_threshold` is off.
    h: u64,
}

#[derive(Serialize)]
struct PostingLanes {
    main: PostingLaneStats,
    broad: PostingLaneStats,
    hot: PostingLaneStats,
}

#[derive(Serialize)]
struct PostingLaneStats {
    count: usize,
    p50: u32,
    p95: u32,
    p99: u32,
    max: u32,
}

impl From<reverse_rusty::events::PostingStats> for PostingLaneStats {
    fn from(s: reverse_rusty::events::PostingStats) -> Self {
        Self {
            count: s.count,
            p50: s.p50,
            p95: s.p95,
            p99: s.p99,
            max: s.max,
        }
    }
}

#[derive(Serialize)]
// Field names are the serialized JSON keys (public API); the shared `_bytes`
// suffix is the contract, not an accident — don't rename it away.
#[allow(clippy::struct_field_names)]
struct MemoryStats {
    exact_bytes: usize,
    index_bytes: usize,
    filter_bytes: usize,
    dict_bytes: usize,
    query_store_bytes: usize,
    logical_index_bytes: usize,
    alive_bytes: usize,
    total_resident_bytes: usize,
}

#[derive(Serialize)]
struct TranslogStats {
    operations: u64,
    size_in_bytes: u64,
}

/// Stats is a native operational snapshot despite sharing an ES/OpenSearch path.
/// Unsupported index-stat controls fail loudly instead of being ignored.
pub(crate) const STATS_BODY_LIMIT: usize = 64 * 1024;

/// GET /_stats — JSON metrics snapshot.
///
/// Class columns and posting lengths are corpus-wide scans, so execution is
/// admitted one-at-a-time and moved off the Tokio request worker.
#[instrument(skip_all)]
pub(crate) async fn stats(
    State(state): State<Arc<AppState>>,
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
    let snapshot = state.snapshot.load_full();
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        collect_engine_stats(&snapshot)
    });
    match worker.await {
        Ok(mut stats) => {
            stats.set_took(started.elapsed().as_secs_f64() * 1000.0);
            finish_stats_response(&state.prom, Json(stats).into_response())
        }
        Err(join_error) => {
            error!(error = %join_error, "stats worker failed");
            stats_rejection(
                &state.prom,
                StatusCode::INTERNAL_SERVER_ERROR,
                "stats_unavailable",
                "stats worker failed",
            )
        }
    }
}

fn collect_engine_stats(snap: &EngineSnapshot) -> EngineStatsResponse {
    let m = snap.metrics();
    let cc = snap.class_counts();
    let lanes = snap.lane_posting_stats();
    let infos = snap.segment_infos();
    let live_queries = infos.iter().map(|segment| segment.alive).sum();
    let tombstoned_queries = infos.iter().map(|segment| segment.deleted).sum();
    let total_resident_bytes = [
        m.exact_bytes,
        m.index_bytes,
        m.filter_bytes,
        m.dict_bytes,
        m.query_store_bytes,
        m.logical_index_bytes,
        m.alive_bytes,
    ]
    .into_iter()
    .fold(0usize, usize::saturating_add);
    EngineStatsResponse {
        took: 0,
        took_ms: 0.0,
        shards: StatsShards {
            total: 1,
            successful: 1,
            failed: 0,
        },
        mode: "standalone",
        total_queries: m.total_queries,
        live_queries,
        tombstoned_queries,
        base_segments: m.base_segments,
        memtable_entries: m.memtable_entries,
        dict_features: m.dict_features,
        rejected_parse: m.rejected_parse,
        rejected_class_d: m.rejected_class_d,
        would_be_hot: m.would_be_hot,
        dedup: DedupStats {
            bodies_total: m.bodies_total,
            dup_joined: m.dup_joined,
            distinct_bodies_est: m.distinct_bodies_est,
        },
        class_counts: ClassCounts {
            a: cc[0],
            b: cc[1],
            c: cc[2],
            d: cc[3],
            h: cc[4],
        },
        postings: PostingLanes {
            main: lanes.main.into(),
            broad: lanes.broad.into(),
            hot: lanes.hot.into(),
        },
        segment_sizes: m.segment_sizes,
        segment_holes: m.segment_holes,
        memory: MemoryStats {
            exact_bytes: m.exact_bytes,
            index_bytes: m.index_bytes,
            filter_bytes: m.filter_bytes,
            dict_bytes: m.dict_bytes,
            query_store_bytes: m.query_store_bytes,
            logical_index_bytes: m.logical_index_bytes,
            alive_bytes: m.alive_bytes,
            total_resident_bytes,
        },
        translog: TranslogStats {
            operations: m.wal_pending_entries,
            size_in_bytes: m.wal_size_bytes,
        },
    }
}

pub(crate) fn validate_stats_method(
    prom: &PrometheusMetrics,
    method: &Method,
) -> Result<(), Box<Response>> {
    if *method == Method::GET {
        return Ok(());
    }
    let mut response = Box::new(stats_rejection(
        prom,
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        "GET is the only supported /_stats method",
    ));
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("GET"));
    Err(response)
}

pub(crate) fn validate_stats_request(
    prom: &PrometheusMetrics,
    RawQuery(raw_query): RawQuery,
    body: Result<Bytes, BytesRejection>,
) -> Result<(), Box<Response>> {
    if raw_query.as_deref().is_some_and(|query| !query.is_empty()) {
        return Err(Box::new(stats_rejection(
            prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            "GET /_stats does not accept query parameters",
        )));
    }
    let body = body.map_err(|error| {
        let status = error.status();
        let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
            "payload_too_large"
        } else {
            "validation_error"
        };
        Box::new(stats_rejection(
            prom,
            status,
            error_type,
            format!("invalid stats body: {error}"),
        ))
    })?;
    if !body.is_empty() {
        return Err(Box::new(stats_rejection(
            prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            "GET /_stats does not accept a request body",
        )));
    }
    Ok(())
}

pub(crate) fn finish_stats_response(prom: &PrometheusMetrics, mut response: Response) -> Response {
    prom.http_requests_total
        .with_label_values(&["stats", response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

pub(crate) fn stats_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    finish_stats_response(
        prom,
        ApiError::response(status, error_type, reason).into_response(),
    )
}

#[cfg(test)]
mod tests;
