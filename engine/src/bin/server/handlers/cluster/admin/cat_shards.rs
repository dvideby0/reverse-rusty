//! Strict coordinator `GET /_cat/shards` projection.
//!
//! Reverse Rusty exposes logical shard positions rather than Elasticsearch or
//! OpenSearch index-shard copies. The table therefore keeps native `queries`
//! and `nodes` semantics while sharing their familiar CAT transport mechanics.

use std::collections::BTreeMap;
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
use tracing::{error, instrument};

use reverse_rusty::cluster::{ClusterEngine, ShardAssignment, ShardError};

use crate::dto::ApiError;
use crate::handlers::admin::cat_table::{
    self, CatAlignment, CatCell, CatColumn, CatRequest, CatRow,
};
use crate::metrics::PrometheusMetrics;
use crate::state::ClusterAppState;

use super::super::shard_error_response;

const ENDPOINT: &str = "cat_shards";

/// CAT routes do not accept bodies. The route-local ceiling prevents a rejected
/// body from inheriting the server's 100 MiB bulk-ingest limit.
pub(crate) const CAT_SHARDS_BODY_LIMIT: usize = 64 * 1024;

const COLUMNS: [CatColumn; 3] = [
    CatColumn::new(
        "shard",
        &["s", "sh", "position"],
        "native logical shard position",
        CatAlignment::Right,
    ),
    CatColumn::new(
        "queries",
        &["q", "count"],
        "physical stored-query rows, including tombstones",
        CatAlignment::Right,
    ),
    CatColumn::new(
        "nodes",
        &["n", "assignment"],
        "committed primary+replica logical node ids",
        CatAlignment::Left,
    ),
];

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CatShardsParams {
    format: Option<String>,
    v: Option<String>,
    h: Option<String>,
    help: Option<String>,
    s: Option<String>,
}

impl CatShardsParams {
    fn resolve(self) -> Result<CatRequest, String> {
        cat_table::resolve_request(
            "CAT shards",
            &COLUMNS,
            self.format.as_deref(),
            self.v.as_deref(),
            self.h.as_deref(),
            self.help.as_deref(),
            self.s.as_deref(),
        )
    }
}

/// `GET /_cat/shards` — one native row per logical shard position.
///
/// Shard and control-plane probes can cross the network. They share stats
/// admission and run on a blocking worker instead of occupying a Tokio request
/// thread. Any failed shard or topology read fails the whole response.
#[instrument(skip_all)]
pub(crate) async fn cluster_cat_shards(
    State(state): State<Arc<ClusterAppState>>,
    method: Method,
    params: Result<Query<CatShardsParams>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let _duration = state
        .prom
        .http_request_duration
        .with_label_values(&[ENDPOINT])
        .start_timer();
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
            "cat_shards_unavailable",
            "CAT shards admission is closed",
        );
    };
    let worker_state = Arc::clone(&state);
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let cluster = worker_state.cluster.read();
        collect_rows(&cluster)
    });
    match worker.await {
        Ok(Ok(mut rows)) => finish_response(
            &state.prom,
            cat_table::render_rows(&mut rows, &request, &COLUMNS),
        ),
        Ok(Err(error)) => finish_response(
            &state.prom,
            shard_error_response("CAT shards unavailable", &error),
        ),
        Err(join_error) => {
            error!(error = %join_error, "CAT shards worker failed");
            rejection(
                &state.prom,
                StatusCode::INTERNAL_SERVER_ERROR,
                "cat_shards_unavailable",
                "CAT shards worker failed",
            )
        }
    }
}

fn validate_request(
    prom: &PrometheusMetrics,
    params: Result<Query<CatShardsParams>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<CatRequest, Box<Response>> {
    let Query(params) = params.map_err(|error| {
        Box::new(rejection(
            prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            format!("invalid CAT shards query parameters: {error}"),
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
            format!("invalid CAT shards body: {error}"),
        ))
    })?;
    if !body.is_empty() {
        return Err(Box::new(rejection(
            prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            "GET /_cat/shards does not accept a request body",
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

fn collect_rows(cluster: &ClusterEngine) -> Result<Vec<CatRow>, ShardError> {
    let control = cluster.control_state()?;
    let counts = cluster.shard_query_counts()?;
    if control.num_shards as usize != counts.len() {
        return Err(ShardError::ControlPlane(format!(
            "committed shard count {} does not match the serving ring count {}",
            control.num_shards,
            counts.len()
        )));
    }

    let mut assignments: BTreeMap<usize, ShardAssignment> = BTreeMap::new();
    for assignment in control.assignments {
        let position = assignment.position as usize;
        if position >= counts.len() {
            return Err(ShardError::ControlPlane(format!(
                "committed assignment names out-of-range shard position {position}"
            )));
        }
        if assignments.insert(position, assignment).is_some() {
            return Err(ShardError::ControlPlane(format!(
                "committed topology contains duplicate shard position {position}"
            )));
        }
    }

    counts
        .into_iter()
        .enumerate()
        .map(|(position, queries)| {
            let assignment = assignments.remove(&position).ok_or_else(|| {
                ShardError::ControlPlane(format!(
                    "no committed node assignment for shard position {position}"
                ))
            })?;
            Ok(CatRow::new([
                CatCell::unsigned(position as u64),
                CatCell::unsigned(queries as u64),
                CatCell::text(render_nodes(&assignment)),
            ]))
        })
        .collect()
}

fn render_nodes(assignment: &ShardAssignment) -> String {
    let mut nodes = assignment.primary.0.to_string();
    for replica in &assignment.replicas {
        nodes.push('+');
        nodes.push_str(&replica.0.to_string());
    }
    nodes
}

fn method_rejection(prom: &PrometheusMetrics) -> Response {
    let mut response = rejection(
        prom,
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        "GET is the only supported /_cat/shards method",
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
