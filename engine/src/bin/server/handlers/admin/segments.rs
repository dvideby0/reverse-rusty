//! Strict native segment introspection with familiar CAT mechanics.
//!
//! Reverse Rusty's LSM rows are not Lucene index-shard rows, so the table keeps
//! native storage semantics while using Elasticsearch/OpenSearch column names
//! where they are exact (`docs.count`, `docs.deleted`, and `size.memory`).

use std::sync::Arc;

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
use tracing::instrument;

use reverse_rusty::events::SegmentInfo;

use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::AppState;

use super::cat_table::{self, CatAlignment, CatCell, CatColumn, CatRequest, CatRow};

const ENDPOINT: &str = "cat_segments";

/// CAT routes do not accept bodies. The explicit ceiling prevents a body-bearing
/// GET from inheriting the server's much larger bulk-ingest limit.
pub(crate) const CAT_SEGMENTS_BODY_LIMIT: usize = 64 * 1024;

const COLUMNS: [CatColumn; 11] = [
    CatColumn::new(
        "segment",
        &["ordinal", "seg"],
        "native LSM ordinal (oldest base first, memtable last)",
        CatAlignment::Right,
    ),
    CatColumn::new(
        "kind",
        &["k"],
        "storage kind: memory, mmap, or memtable",
        CatAlignment::Left,
    ),
    CatColumn::new(
        "entries",
        &["e"],
        "physical rows (live plus tombstoned)",
        CatAlignment::Right,
    ),
    CatColumn::new(
        "docs.count",
        &["alive", "dc"],
        "live query rows",
        CatAlignment::Right,
    ),
    CatColumn::new(
        "docs.deleted",
        &["deleted", "dd"],
        "tombstoned rows awaiting compaction",
        CatAlignment::Right,
    ),
    CatColumn::new(
        "holes.percent",
        &["holes", "holes_ratio", "hp"],
        "tombstoned percentage of physical rows",
        CatAlignment::Right,
    ),
    CatColumn::new(
        "vocab.epoch",
        &["epoch", "vocab_epoch", "ve"],
        "vocabulary epoch used to compile the segment",
        CatAlignment::Right,
    ),
    CatColumn::new(
        "stale",
        &["st"],
        "whether the segment predates the live vocabulary epoch",
        CatAlignment::Left,
    ),
    CatColumn::new(
        "size.memory",
        &["memory", "sm"],
        "total attributed resident heap bytes",
        CatAlignment::Right,
    ),
    CatColumn::new(
        "memory.payload",
        &["resident", "resident_bytes", "mp"],
        "resident match-payload bytes (zero for mmap payloads)",
        CatAlignment::Right,
    ),
    CatColumn::new(
        "memory.overhead",
        &["overhead", "overhead_bytes", "mo"],
        "resident logical-index and liveness-overlay bytes",
        CatAlignment::Right,
    ),
];

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CatSegmentsParams {
    format: Option<String>,
    v: Option<String>,
    h: Option<String>,
    help: Option<String>,
    s: Option<String>,
    bytes: Option<String>,
}

pub(crate) struct CatSegmentsRequest {
    table: CatRequest,
    bytes: ByteUnit,
}

impl CatSegmentsRequest {
    fn is_help(&self) -> bool {
        self.table.is_help()
    }
}

impl CatSegmentsParams {
    fn resolve(self) -> Result<CatSegmentsRequest, String> {
        let table = cat_table::resolve_request(
            "CAT segments",
            &COLUMNS,
            self.format.as_deref(),
            self.v.as_deref(),
            self.h.as_deref(),
            self.help.as_deref(),
            self.s.as_deref(),
        )?;
        let bytes = ByteUnit::parse(self.bytes.as_deref())?;
        if table.is_help() && self.bytes.is_some() {
            return Err("CAT segments help cannot be combined with bytes".to_string());
        }
        Ok(CatSegmentsRequest { table, bytes })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ByteUnit {
    Auto,
    Bytes,
    Kibibytes,
    Mebibytes,
    Gibibytes,
    Tebibytes,
    Pebibytes,
}

impl ByteUnit {
    fn parse(raw: Option<&str>) -> Result<Self, String> {
        match raw {
            None => Ok(Self::Auto),
            Some("b") => Ok(Self::Bytes),
            Some("k" | "kb") => Ok(Self::Kibibytes),
            Some("m" | "mb") => Ok(Self::Mebibytes),
            Some("g" | "gb") => Ok(Self::Gibibytes),
            Some("t" | "tb") => Ok(Self::Tebibytes),
            Some("p" | "pb") => Ok(Self::Pebibytes),
            Some(other) => Err(format!(
                "unsupported CAT segments byte unit `{other}`; supported: \
                 b, kb, k, mb, m, gb, g, tb, t, pb, p"
            )),
        }
    }

    fn render(self, bytes: usize) -> String {
        let bytes = bytes as u64;
        match self {
            Self::Auto => human_bytes(bytes),
            Self::Bytes => bytes.to_string(),
            Self::Kibibytes => (bytes / 1024).to_string(),
            Self::Mebibytes => (bytes / 1024u64.pow(2)).to_string(),
            Self::Gibibytes => (bytes / 1024u64.pow(3)).to_string(),
            Self::Tebibytes => (bytes / 1024u64.pow(4)).to_string(),
            Self::Pebibytes => (bytes / 1024u64.pow(5)).to_string(),
        }
    }
}

/// `GET /_cat/segments` — base segments oldest-first followed by the active
/// memtable. Collection is O(number of segments) from one lock-free snapshot.
#[instrument(skip_all)]
pub(crate) async fn cat_segments(
    State(state): State<Arc<AppState>>,
    method: Method,
    params: Result<Query<CatSegmentsParams>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let _duration = state
        .prom
        .http_request_duration
        .with_label_values(&[ENDPOINT])
        .start_timer();
    if let Err(response) = validate_cat_segments_method(&state.prom, &method) {
        return *response;
    }
    let request = match validate_cat_segments_request(&state.prom, params, body) {
        Ok(request) => request,
        Err(response) => return *response,
    };
    if request.is_help() {
        return finish_cat_segments_response(
            &state.prom,
            cat_table::render_help(&request.table, &COLUMNS),
        );
    }

    let snapshot = state.snapshot.load();
    let mut rows: Vec<CatRow> = snapshot
        .segment_infos()
        .iter()
        .map(|info| segment_row(info, request.bytes))
        .collect();
    finish_cat_segments_response(
        &state.prom,
        cat_table::render_rows(&mut rows, &request.table, &COLUMNS),
    )
}

pub(crate) fn validate_cat_segments_method(
    prom: &PrometheusMetrics,
    method: &Method,
) -> Result<(), Box<Response>> {
    if method == Method::GET {
        return Ok(());
    }
    let mut response = cat_segments_rejection(
        prom,
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        "GET is the only supported /_cat/segments method",
    );
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("GET"));
    Err(Box::new(response))
}

pub(crate) fn validate_cat_segments_request(
    prom: &PrometheusMetrics,
    params: Result<Query<CatSegmentsParams>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<CatSegmentsRequest, Box<Response>> {
    let Query(params) = params.map_err(|error| {
        Box::new(cat_segments_rejection(
            prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            format!("invalid CAT segments query parameters: {error}"),
        ))
    })?;
    let body = body.map_err(|error| {
        let status = error.status();
        let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
            "payload_too_large"
        } else {
            "validation_error"
        };
        Box::new(cat_segments_rejection(
            prom,
            status,
            error_type,
            format!("invalid CAT segments body: {error}"),
        ))
    })?;
    if !body.is_empty() {
        return Err(Box::new(cat_segments_rejection(
            prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            "GET /_cat/segments does not accept a request body",
        )));
    }
    params.resolve().map_err(|reason| {
        Box::new(cat_segments_rejection(
            prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            reason,
        ))
    })
}

pub(crate) fn cat_segments_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    finish_cat_segments_response(
        prom,
        ApiError::response(status, error_type, reason).into_response(),
    )
}

pub(crate) fn finish_cat_segments_response(
    prom: &PrometheusMetrics,
    mut response: Response,
) -> Response {
    prom.http_requests_total
        .with_label_values(&[ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn segment_row(info: &SegmentInfo, bytes: ByteUnit) -> CatRow {
    let memory_total = info.resident_bytes.saturating_add(info.overhead_bytes);
    CatRow::new([
        CatCell::unsigned(info.ordinal as u64),
        CatCell::text(info.kind.as_str()),
        CatCell::unsigned(info.entries as u64),
        CatCell::unsigned(info.alive as u64),
        CatCell::unsigned(info.deleted as u64),
        CatCell::decimal(
            format!("{:.2}%", info.holes_ratio * 100.0),
            info.holes_ratio,
        ),
        CatCell::unsigned(info.vocab_epoch),
        CatCell::boolean(info.stale),
        CatCell::unsigned_display(bytes.render(memory_total), memory_total as u64),
        CatCell::unsigned_display(
            bytes.render(info.resident_bytes),
            info.resident_bytes as u64,
        ),
        CatCell::unsigned_display(
            bytes.render(info.overhead_bytes),
            info.overhead_bytes as u64,
        ),
    ])
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [(&str, u64); 6] = [
        ("pb", 1024u64.pow(5)),
        ("tb", 1024u64.pow(4)),
        ("gb", 1024u64.pow(3)),
        ("mb", 1024u64.pow(2)),
        ("kb", 1024),
        ("b", 1),
    ];
    let &(suffix, divisor) = UNITS
        .iter()
        .find(|(_, divisor)| bytes >= *divisor)
        .unwrap_or(&("b", 1));
    if divisor == 1 {
        return format!("{bytes}b");
    }
    let mut value = format!("{:.2}", bytes as f64 / divisor as f64);
    while value.ends_with('0') {
        value.pop();
    }
    if value.ends_with('.') {
        value.pop();
    }
    format!("{value}{suffix}")
}

#[cfg(test)]
#[path = "cat_segments_tests.rs"]
mod tests;
