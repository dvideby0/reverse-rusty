//! Native `GET /_cat/stats` table over the bounded stats collector.
//!
//! Elasticsearch and OpenSearch do not define this path. The transport still
//! follows their common CAT conventions where they map exactly: text by
//! default, `format=json`, `v`, `h`, `help`, and `s`.

use std::sync::Arc;
use std::time::Instant;

use axum::{
    body::Bytes,
    extract::{
        rejection::{BytesRejection, QueryRejection},
        Query, State,
    },
    http::{header, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use tracing::{error, instrument};

use reverse_rusty::segment::EngineSnapshot;

use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::AppState;

use super::super::cat_table::{self, CatAlignment, CatCell, CatColumn, CatRequest, CatRow};
use super::collect_engine_stats;

const ENDPOINT: &str = "cat_stats";

const COLUMNS: [CatColumn; 2] = [
    CatColumn::new(
        "metric",
        &["m"],
        "native Reverse Rusty statistic name",
        CatAlignment::Left,
    ),
    CatColumn::new(
        "value",
        &["v"],
        "statistic value (byte fields use raw bytes)",
        CatAlignment::Left,
    ),
];

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CatStatsParams {
    format: Option<String>,
    v: Option<String>,
    h: Option<String>,
    help: Option<String>,
    s: Option<String>,
}

impl CatStatsParams {
    fn resolve(self) -> Result<CatRequest, String> {
        cat_table::resolve_request(
            "CAT stats",
            &COLUMNS,
            self.format.as_deref(),
            self.v.as_deref(),
            self.h.as_deref(),
            self.help.as_deref(),
            self.s.as_deref(),
        )
    }
}

macro_rules! cat_row {
    ($metric:expr, $value:expr $(,)?) => {
        CatRow::new([CatCell::text($metric), CatCell::text($value.to_string())])
    };
}

/// Native human-readable stats with ES/OpenSearch-familiar CAT controls.
///
/// The class-column and posting-length scans are the same corpus-wide work as
/// `GET /_stats`, so both endpoints share one admission semaphore.
#[instrument(skip_all)]
pub(crate) async fn cat_stats(
    State(state): State<Arc<AppState>>,
    method: Method,
    params: Result<Query<CatStatsParams>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let _duration = state
        .prom
        .http_request_duration
        .with_label_values(&[ENDPOINT])
        .start_timer();
    let started = Instant::now();
    if method != Method::GET {
        return method_rejection(&state.prom);
    }
    let request = match validate_request(&state.prom, params, body) {
        Ok(request) => request,
        Err(response) => return *response,
    };
    if request.is_help() {
        return finish_response(&state.prom, cat_table::render_help(&request, &COLUMNS));
    }

    let Ok(permit) = Arc::clone(&state.stats_permits).acquire_owned().await else {
        return rejection(
            &state.prom,
            StatusCode::SERVICE_UNAVAILABLE,
            "cat_stats_unavailable",
            "CAT stats admission is closed",
        );
    };
    let snapshot = state.snapshot.load_full();
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        collect_rows(&snapshot)
    });
    match worker.await {
        Ok(mut rows) => {
            rows.insert(
                0,
                cat_row!(
                    "took_ms",
                    format!("{:.3}", started.elapsed().as_secs_f64() * 1_000.0),
                ),
            );
            let response = cat_table::render_rows(&mut rows, &request, &COLUMNS);
            finish_response(&state.prom, response)
        }
        Err(join_error) => {
            error!(error = %join_error, "CAT stats worker failed");
            rejection(
                &state.prom,
                StatusCode::INTERNAL_SERVER_ERROR,
                "cat_stats_unavailable",
                "CAT stats worker failed",
            )
        }
    }
}

fn validate_request(
    prom: &PrometheusMetrics,
    params: Result<Query<CatStatsParams>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<CatRequest, Box<Response>> {
    let Query(params) = params.map_err(|error| {
        Box::new(rejection(
            prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            format!("invalid CAT stats query parameters: {error}"),
        ))
    })?;
    let body = body.map_err(|error| {
        let status = error.status();
        let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
            "payload_too_large"
        } else {
            "validation_error"
        };
        Box::new(rejection(
            prom,
            status,
            error_type,
            format!("invalid CAT stats body: {error}"),
        ))
    })?;
    if !body.is_empty() {
        return Err(Box::new(rejection(
            prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            "GET /_cat/stats does not accept a request body",
        )));
    }

    params.resolve().map_err(|reason| {
        Box::new(rejection(
            prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            reason,
        ))
    })
}

fn collect_rows(snapshot: &EngineSnapshot) -> Vec<CatRow> {
    let stats = collect_engine_stats(snapshot);
    let config = snapshot.config();
    let mut rows = vec![
        cat_row!("mode", stats.mode),
        cat_row!("queries.physical", stats.total_queries),
        cat_row!("queries.live", stats.live_queries),
        cat_row!("queries.tombstoned", stats.tombstoned_queries),
        cat_row!("segments.base", stats.base_segments),
        cat_row!("memtable.entries", stats.memtable_entries),
        cat_row!("features", stats.dict_features),
        cat_row!("class.a", stats.class_counts.a),
        cat_row!("class.b", stats.class_counts.b),
        cat_row!("class.c", stats.class_counts.c),
        cat_row!("class.d", stats.class_counts.d),
        cat_row!("class.h", stats.class_counts.h),
        cat_row!("rejected.parse", stats.rejected_parse),
        cat_row!("rejected.class_d", stats.rejected_class_d),
        cat_row!("would_be_hot", stats.would_be_hot),
        cat_row!("dedup.bodies_total", stats.dedup.bodies_total),
        cat_row!("dedup.joined", stats.dedup.dup_joined),
        cat_row!("dedup.distinct_bodies_est", stats.dedup.distinct_bodies_est),
    ];
    push_posting_rows(&mut rows, "main", &stats.postings.main);
    push_posting_rows(&mut rows, "broad", &stats.postings.broad);
    push_posting_rows(&mut rows, "hot", &stats.postings.hot);
    rows.extend([
        cat_row!("memory.exact_bytes", stats.memory.exact_bytes),
        cat_row!("memory.index_bytes", stats.memory.index_bytes),
        cat_row!("memory.filter_bytes", stats.memory.filter_bytes),
        cat_row!("memory.dict_bytes", stats.memory.dict_bytes),
        cat_row!("memory.query_store_bytes", stats.memory.query_store_bytes),
        cat_row!(
            "memory.logical_index_bytes",
            stats.memory.logical_index_bytes,
        ),
        cat_row!("memory.alive_bytes", stats.memory.alive_bytes),
        cat_row!(
            "memory.total_resident_bytes",
            stats.memory.total_resident_bytes,
        ),
        cat_row!("translog.operations", stats.translog.operations),
        cat_row!("translog.size_in_bytes", stats.translog.size_in_bytes),
        cat_row!(
            "broad.mode",
            if config.broad_columnar {
                "columnar"
            } else {
                "inline"
            },
        ),
        cat_row!("broad.batch_size", config.broad_batch_size),
        cat_row!("broad.materialize", config.broad_materialize),
        cat_row!("broad.prefilter", config.broad_prefilter),
        cat_row!("batch.max", config.max_percolate_batch),
    ]);
    for (ordinal, (&entries, &holes)) in stats
        .segment_sizes
        .iter()
        .zip(&stats.segment_holes)
        .enumerate()
    {
        rows.push(cat_row!(format!("segment.{ordinal}.entries"), entries));
        rows.push(cat_row!(
            format!("segment.{ordinal}.holes_percent"),
            format!("{:.2}", holes * 100.0),
        ));
    }
    rows
}

fn push_posting_rows(rows: &mut Vec<CatRow>, lane: &str, stats: &super::PostingLaneStats) {
    rows.extend([
        cat_row!(format!("postings.{lane}.count"), stats.count),
        cat_row!(format!("postings.{lane}.p50"), stats.p50),
        cat_row!(format!("postings.{lane}.p95"), stats.p95),
        cat_row!(format!("postings.{lane}.p99"), stats.p99),
        cat_row!(format!("postings.{lane}.max"), stats.max),
    ]);
}

fn method_rejection(prom: &PrometheusMetrics) -> Response {
    let mut response = rejection(
        prom,
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        "GET is the only supported /_cat/stats method",
    );
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("GET"));
    response
}

fn rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    finish_response(
        prom,
        ApiError::response(status, error_type, reason).into_response(),
    )
}

fn finish_response(prom: &PrometheusMetrics, mut response: Response) -> Response {
    prom.http_requests_total
        .with_label_values(&[ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
