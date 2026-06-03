//! Operational endpoints: flush/compact, the JSON and `_cat` stats views, health,
//! the API root, and the Prometheus exposition. These project engine introspection
//! ([`reverse_rusty::events`] metrics + per-segment info) into the REST surface.

use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use prometheus::{Encoder, TextEncoder};
use serde::{Deserialize, Serialize};
use tracing::{error, info, instrument};

use reverse_rusty::events::SegmentInfo;

use crate::state::AppState;

// -- POST /_flush
#[derive(Serialize)]
struct FlushResponse {
    took_ms: f64,
    acknowledged: bool,
    total_queries: usize,
    base_segments: usize,
}

// -- POST /_compact
#[derive(Serialize)]
struct CompactResponse {
    took_ms: f64,
    acknowledged: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    segments_merged: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    entries_before: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    entries_after: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tombstones_reclaimed: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<&'static str>,
}

// -- GET /_stats
#[derive(Serialize)]
struct EngineStatsResponse {
    total_queries: usize,
    base_segments: usize,
    memtable_entries: usize,
    dict_features: usize,
    rejected_parse: u64,
    rejected_class_d: u64,
    class_counts: ClassCounts,
    segment_sizes: Vec<usize>,
    segment_holes: Vec<f64>,
    memory: MemoryStats,
}

#[derive(Serialize)]
struct ClassCounts {
    a: u64,
    b: u64,
    c: u64,
    d: u64,
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
    version: &'static str,
    tagline: &'static str,
}

/// POST /_flush
#[instrument(skip_all)]
pub(crate) async fn flush(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let start = Instant::now();
    let metrics = {
        let mut engine = state.engine.lock();
        engine.flush();
        engine.metrics()
    };
    state.publish_snapshot();
    info!(
        total_queries = metrics.total_queries,
        base_segments = metrics.base_segments,
        "flush complete"
    );
    state
        .prom
        .http_requests_total
        .with_label_values(&["flush", "200"])
        .inc();
    state
        .prom
        .http_request_duration
        .with_label_values(&["flush"])
        .observe(start.elapsed().as_secs_f64());
    Json(FlushResponse {
        took_ms: start.elapsed().as_secs_f64() * 1000.0,
        acknowledged: true,
        total_queries: metrics.total_queries,
        base_segments: metrics.base_segments,
    })
}

/// POST /_compact
#[instrument(skip_all)]
pub(crate) async fn compact(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let start = Instant::now();
    let report = {
        let mut engine = state.engine.lock();
        engine.maybe_compact()
    };
    state.publish_snapshot();
    state
        .prom
        .http_requests_total
        .with_label_values(&["compact", "200"])
        .inc();
    state
        .prom
        .http_request_duration
        .with_label_values(&["compact"])
        .observe(start.elapsed().as_secs_f64());
    if let Some(r) = report {
        info!(
            segments_merged = r.segments_merged,
            entries_before = r.entries_before,
            entries_after = r.entries_after,
            tombstones_reclaimed = r.tombstones_reclaimed,
            "compaction complete"
        );
        Json(CompactResponse {
            took_ms: start.elapsed().as_secs_f64() * 1000.0,
            acknowledged: true,
            segments_merged: Some(r.segments_merged),
            entries_before: Some(r.entries_before),
            entries_after: Some(r.entries_after),
            tombstones_reclaimed: Some(r.tombstones_reclaimed),
            message: None,
        })
    } else {
        info!("compaction skipped: not needed");
        Json(CompactResponse {
            took_ms: start.elapsed().as_secs_f64() * 1000.0,
            acknowledged: true,
            segments_merged: None,
            entries_before: None,
            entries_after: None,
            tombstones_reclaimed: None,
            message: Some("no compaction needed"),
        })
    }
}

/// GET /_stats — JSON metrics snapshot.
pub(crate) async fn stats(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let snap = state.snapshot.load();
    let m = snap.metrics();
    let cc = snap.class_counts();
    Json(EngineStatsResponse {
        total_queries: m.total_queries,
        base_segments: m.base_segments,
        memtable_entries: m.memtable_entries,
        dict_features: m.dict_features,
        rejected_parse: m.rejected_parse,
        rejected_class_d: m.rejected_class_d,
        class_counts: ClassCounts {
            a: cc[0],
            b: cc[1],
            c: cc[2],
            d: cc[3],
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
        "class A/B/C/D    {} / {} / {} / {}\n",
        cc[0], cc[1], cc[2], cc[3]
    ));
    out.push_str(&format!("rejected parse   {}\n", m.rejected_parse));
    out.push_str(&format!("rejected classD  {}\n", m.rejected_class_d));
    out.push_str(&format!(
        "memory           {} bytes (~{:.1} MB)\n",
        total_mem,
        total_mem as f64 / 1_048_576.0
    ));
    let cfg = snap.config();
    out.push_str(&format!(
        "broad lane       {} (batch_size {}, materialize {}, max_batch {})\n",
        if cfg.broad_columnar {
            "columnar"
        } else {
            "inline"
        },
        cfg.broad_batch_size,
        cfg.broad_materialize,
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
        version: env!("CARGO_PKG_VERSION"),
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
mod cat_segments_tests {
    use super::{fmt_bytes, render_segments_table, SegmentRow};
    use reverse_rusty::events::{SegmentInfo, SegmentKind};

    fn info(ordinal: usize, kind: SegmentKind, alive: usize, deleted: usize) -> SegmentInfo {
        let entries = alive + deleted;
        SegmentInfo {
            ordinal,
            kind,
            entries,
            alive,
            deleted,
            holes_ratio: if entries == 0 {
                0.0
            } else {
                deleted as f64 / entries as f64
            },
            vocab_epoch: 3,
            stale: false,
            resident_bytes: 0,
            overhead_bytes: 0,
        }
    }

    #[test]
    fn fmt_bytes_scales_by_unit() {
        assert_eq!(fmt_bytes(0), "0 B");
        assert_eq!(fmt_bytes(512), "512 B");
        assert_eq!(fmt_bytes(1024), "1.00 KB");
        assert_eq!(fmt_bytes(1_572_864), "1.50 MB");
        assert_eq!(fmt_bytes(3 * 1024 * 1024 * 1024), "3.00 GB");
    }

    #[test]
    fn table_has_header_and_one_row_per_segment() {
        let infos = vec![
            info(0, SegmentKind::Mmap, 98_000, 2_000),
            info(1, SegmentKind::Memory, 50_000, 0),
            info(2, SegmentKind::Memtable, 1_200, 0),
        ];
        let table = render_segments_table(&infos);
        let lines: Vec<&str> = table.lines().collect();
        // 1 header + 3 data rows.
        assert_eq!(lines.len(), 4, "table:\n{table}");
        assert!(lines[0].contains("segment") && lines[0].contains("holes"));
        assert!(lines[1].contains("mmap"));
        assert!(lines[2].contains("memory"));
        assert!(lines[3].contains("memtable"));
        // 2000/100000 = 2.00% holes on the first base segment.
        assert!(lines[1].contains("2.00%"), "row:\n{}", lines[1]);
    }

    #[test]
    fn stale_flag_renders_yes_no() {
        let mut stale = info(0, SegmentKind::Memory, 10, 0);
        stale.stale = true;
        let table = render_segments_table(&[stale]);
        let row = table.lines().nth(1).expect("data row");
        assert!(row.contains("yes"), "row: {row}");

        let fresh = info(0, SegmentKind::Memory, 10, 0);
        let table = render_segments_table(&[fresh]);
        let row = table.lines().nth(1).expect("data row");
        assert!(row.contains(" no "), "row: {row}");
    }

    #[test]
    fn json_row_projects_segment_info() {
        let mut s = info(2, SegmentKind::Memtable, 1_200, 0);
        s.resident_bytes = 145_000;
        s.overhead_bytes = 18_000;
        let row = SegmentRow::from(&s);
        let json = serde_json::to_value(&row).expect("serialize");
        assert_eq!(json["kind"], "memtable");
        assert_eq!(json["ordinal"], 2);
        assert_eq!(json["alive"], 1_200);
        // Byte fields are raw integers in JSON (humanized only in the text table).
        assert_eq!(json["resident_bytes"], 145_000);
        assert_eq!(json["overhead_bytes"], 18_000);
    }
}
