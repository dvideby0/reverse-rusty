//! Operational endpoints: flush/compact, the JSON and `_cat` stats views, health,
//! the API root, and the Prometheus exposition. These project engine introspection
//! ([`reverse_rusty::events`] metrics + per-segment info) into the REST surface.

mod compact;
mod flush;

pub(crate) use compact::{compact_route, force_merge_route};
pub(crate) use flush::{
    acquire_flush, flush_route, validate_flush_method, validate_flush_request, FlushParams,
    FlushResponse,
};

use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use prometheus::{Encoder, TextEncoder};
use serde::{Deserialize, Serialize};
use tracing::error;

use reverse_rusty::events::SegmentInfo;

use crate::dto::ApiVersion;
use crate::state::AppState;

// -- GET /_stats
#[derive(Serialize)]
struct EngineStatsResponse {
    total_queries: usize,
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
}

// -- GET /_cat/segments
/// Query string for the `_cat` endpoints. `?format=json` switches the default
/// text table to a JSON array (ES convention).
#[derive(Deserialize, Default)]
pub(crate) struct CatQuery {
    format: Option<String>,
}

/// One row of `GET /_cat/segments?format=json` — the JSON projection of an
/// engine [`SegmentInfo`]. Byte fields are raw integers (machine-readable); the
/// text table humanizes them instead.
#[derive(Serialize)]
struct SegmentRow {
    ordinal: usize,
    kind: &'static str,
    entries: usize,
    alive: usize,
    deleted: usize,
    holes_ratio: f64,
    vocab_epoch: u64,
    stale: bool,
    resident_bytes: usize,
    overhead_bytes: usize,
}

impl From<&SegmentInfo> for SegmentRow {
    fn from(s: &SegmentInfo) -> Self {
        Self {
            ordinal: s.ordinal,
            kind: s.kind.as_str(),
            entries: s.entries,
            alive: s.alive,
            deleted: s.deleted,
            holes_ratio: s.holes_ratio,
            vocab_epoch: s.vocab_epoch,
            stale: s.stale,
            resident_bytes: s.resident_bytes,
            overhead_bytes: s.overhead_bytes,
        }
    }
}

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

/// GET /_stats — JSON metrics snapshot.
pub(crate) async fn stats(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let snap = state.snapshot.load();
    let m = snap.metrics();
    let cc = snap.class_counts();
    let lanes = snap.lane_posting_stats();
    Json(EngineStatsResponse {
        total_queries: m.total_queries,
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
        },
    })
}

/// GET /_cat/stats — human-readable metrics.
pub(crate) async fn cat_stats(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let snap = state.snapshot.load();
    let m = snap.metrics();
    let cc = snap.class_counts();
    let total_mem = m.exact_bytes + m.index_bytes + m.filter_bytes;

    let mut out = String::new();
    out.push_str(&format!("queries          {}\n", m.total_queries));
    out.push_str(&format!(
        "segments         {} (+ memtable: {})\n",
        m.base_segments, m.memtable_entries
    ));
    out.push_str(&format!("features         {}\n", m.dict_features));
    out.push_str(&format!(
        "class A/B/C/D/H  {} / {} / {} / {} / {}\n",
        cc[0], cc[1], cc[2], cc[3], cc[4]
    ));
    out.push_str(&format!("rejected parse   {}\n", m.rejected_parse));
    out.push_str(&format!("rejected classD  {}\n", m.rejected_class_d));
    out.push_str(&format!("would-be hot     {}\n", m.would_be_hot));
    out.push_str(&format!(
        "dedup            {} joined / {} bodies (distinct est {})\n",
        m.dup_joined, m.bodies_total, m.distinct_bodies_est
    ));
    let lanes = snap.lane_posting_stats();
    out.push_str(&format!(
        "postings main    {} sigs (p50 {} p95 {} p99 {} max {})\n",
        lanes.main.count, lanes.main.p50, lanes.main.p95, lanes.main.p99, lanes.main.max
    ));
    out.push_str(&format!(
        "postings broad   {} sigs (p50 {} p95 {} p99 {} max {})\n",
        lanes.broad.count, lanes.broad.p50, lanes.broad.p95, lanes.broad.p99, lanes.broad.max
    ));
    out.push_str(&format!(
        "postings hot     {} sigs (p50 {} p95 {} p99 {} max {})\n",
        lanes.hot.count, lanes.hot.p50, lanes.hot.p95, lanes.hot.p99, lanes.hot.max
    ));
    out.push_str(&format!(
        "memory           {} bytes (~{:.1} MB)\n",
        total_mem,
        total_mem as f64 / 1_048_576.0
    ));
    let cfg = snap.config();
    out.push_str(&format!(
        "broad lane       {} (batch_size {}, materialize {}, prefilter {}, max_batch {})\n",
        if cfg.broad_columnar {
            "columnar"
        } else {
            "inline"
        },
        cfg.broad_batch_size,
        cfg.broad_materialize,
        cfg.broad_prefilter,
        cfg.max_percolate_batch,
    ));

    if !m.segment_sizes.is_empty() {
        out.push_str("\nsegment  entries  holes\n");
        for (i, (&sz, &h)) in m
            .segment_sizes
            .iter()
            .zip(m.segment_holes.iter())
            .enumerate()
        {
            out.push_str(&format!("{:<8} {:<8} {:.2}%\n", i, sz, h * 100.0));
        }
    }

    (
        StatusCode::OK,
        [("content-type", "text/plain; charset=utf-8")],
        out,
    )
}

/// GET /_cat/segments — per-segment detail of the LSM layout (one row per base
/// segment, oldest first, then the memtable). Defaults to a human-readable text
/// table like the other `_cat` endpoints; `?format=json` returns a JSON array of
/// row objects (ES `_cat?format=json` convention). Reads the lock-free snapshot.
///
/// This exposes the segment-level detail the aggregate `/_stats` flattens: which
/// segments carry compaction pressure (`holes`), how memory is distributed
/// (resident vs off-heap `mmap`), and which segments are stale against the
/// current vocab epoch.
pub(crate) async fn cat_segments(
    State(state): State<Arc<AppState>>,
    Query(q): Query<CatQuery>,
) -> impl IntoResponse {
    let infos = state.snapshot.load().segment_infos();
    if q.format.as_deref() == Some("json") {
        let rows: Vec<SegmentRow> = infos.iter().map(SegmentRow::from).collect();
        Json(rows).into_response()
    } else {
        (
            StatusCode::OK,
            [("content-type", "text/plain; charset=utf-8")],
            render_segments_table(&infos),
        )
            .into_response()
    }
}

/// Render the `_cat/segments` text table: a header row plus one row per segment.
/// Numbers are right-aligned, byte counts humanized; the memtable is the final
/// row (kind `memtable`). Pure so it is unit-tested without the HTTP layer.
fn render_segments_table(infos: &[SegmentInfo]) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "{:<7} {:<8} {:>9} {:>9} {:>9} {:>7} {:>6} {:>5} {:>12} {:>12}\n",
        "segment",
        "kind",
        "entries",
        "alive",
        "deleted",
        "holes",
        "epoch",
        "stale",
        "resident",
        "overhead",
    ));
    for s in infos {
        out.push_str(&format!(
            "{:<7} {:<8} {:>9} {:>9} {:>9} {:>6.2}% {:>6} {:>5} {:>12} {:>12}\n",
            s.ordinal,
            s.kind.as_str(),
            s.entries,
            s.alive,
            s.deleted,
            s.holes_ratio * 100.0,
            s.vocab_epoch,
            if s.stale { "yes" } else { "no" },
            fmt_bytes(s.resident_bytes),
            fmt_bytes(s.overhead_bytes),
        ));
    }
    out
}

/// Humanize a byte count for the `_cat` text tables (binary units, 2 dp).
/// JSON callers get the raw integer instead (see [`SegmentRow`]).
fn fmt_bytes(n: usize) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    let f = n as f64;
    if f >= GB {
        format!("{:.2} GB", f / GB)
    } else if f >= MB {
        format!("{:.2} MB", f / MB)
    } else if f >= KB {
        format!("{:.2} KB", f / KB)
    } else {
        format!("{n} B")
    }
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
mod cat_segments_tests;

#[cfg(test)]
mod flush_tests;

#[cfg(test)]
mod compact_tests;
