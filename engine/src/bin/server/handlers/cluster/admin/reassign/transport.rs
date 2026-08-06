//! Bounded request extraction for the native cluster-reassign route.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    extract::{FromRequest, Json, Query, Request},
    http::{header, HeaderValue, Method, StatusCode},
    response::Response,
};
use prometheus::HistogramTimer;
use serde::{de, Deserialize, Deserializer};

use crate::handlers::search::parse_named_time_value;
use crate::state::ClusterAppState;

use super::{cluster_reassign_rejection, CLUSTER_REASSIGN_BODY_TIMEOUT, CLUSTER_REASSIGN_ENDPOINT};

const DEFAULT_CLUSTER_REASSIGN_MANAGER_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CLUSTER_REASSIGN_MANAGER_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterReassignParams {
    /// OpenSearch-inclusive spelling.
    cluster_manager_timeout: Option<String>,
    /// Elasticsearch and legacy OpenSearch spelling.
    master_timeout: Option<String>,
}

impl ClusterReassignParams {
    fn manager_timeout(self) -> Result<Duration, String> {
        if self.cluster_manager_timeout.is_some() && self.master_timeout.is_some() {
            return Err(
                "`cluster_manager_timeout` and `master_timeout` are aliases; specify exactly one"
                    .to_string(),
            );
        }
        let timeout = self
            .cluster_manager_timeout
            .or(self.master_timeout)
            .as_deref()
            .map(parse_cluster_reassign_manager_timeout)
            .transpose()?
            .unwrap_or(DEFAULT_CLUSTER_REASSIGN_MANAGER_TIMEOUT);
        if timeout > MAX_CLUSTER_REASSIGN_MANAGER_TIMEOUT {
            return Err("reassign manager timeout must not exceed 30s".to_string());
        }
        Ok(timeout)
    }
}

fn parse_cluster_reassign_manager_timeout(raw: &str) -> Result<Duration, String> {
    if raw == "0" {
        return Ok(Duration::ZERO);
    }
    parse_named_time_value("cluster_manager_timeout/master_timeout", raw)
}

#[derive(Deserialize)]
#[serde(untagged)]
enum NodeIdInput {
    Number(u64),
    String(String),
}

fn deserialize_node_id<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    match NodeIdInput::deserialize(deserializer)? {
        NodeIdInput::Number(node) => Ok(node),
        NodeIdInput::String(node) => node.parse::<u64>().map_err(|_| {
            de::Error::custom("`node`/`to_node` must be a numeric Reverse Rusty node id")
        }),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(not(feature = "distributed"), allow(dead_code))]
pub(super) struct ClusterReassignBody {
    /// Global logical shard position. `shard` is the familiar additive
    /// Elasticsearch/OpenSearch spelling; this remains a native single-index
    /// operation rather than a `/_cluster/reroute` alias.
    #[serde(alias = "shard")]
    pub(super) position: u32,
    /// Target membership id. `to_node` accepts either a JSON integer or its
    /// decimal-string representation for client ergonomics.
    #[serde(alias = "to_node", deserialize_with = "deserialize_node_id")]
    pub(super) node: u64,
}

/// Method/query validation plus bounded strict-JSON extraction. The route
/// timer begins before every transport check.
pub(crate) struct ClusterReassignTransport {
    duration: HistogramTimer,
    started: Instant,
    manager_timeout: Duration,
    body: ClusterReassignBody,
}

impl ClusterReassignTransport {
    pub(super) fn into_parts(self) -> (HistogramTimer, Instant, Duration, ClusterReassignBody) {
        (self.duration, self.started, self.manager_timeout, self.body)
    }
}

impl FromRequest<Arc<ClusterAppState>> for ClusterReassignTransport {
    type Rejection = Response;

    async fn from_request(
        request: Request,
        state: &Arc<ClusterAppState>,
    ) -> Result<Self, Self::Rejection> {
        let started = Instant::now();
        let duration = state
            .prom
            .http_request_duration
            .with_label_values(&[CLUSTER_REASSIGN_ENDPOINT])
            .start_timer();
        if request.method() != Method::POST {
            let mut response = cluster_reassign_rejection(
                &state.prom,
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "POST is the only supported /_cluster/reassign method",
            );
            response
                .headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static("POST"));
            return Err(response);
        }

        let Query(params) =
            Query::<ClusterReassignParams>::try_from_uri(request.uri()).map_err(|source| {
                cluster_reassign_rejection(
                    &state.prom,
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    format!("invalid reassign query parameters: {source}"),
                )
            })?;
        let manager_timeout = params.manager_timeout().map_err(|reason| {
            cluster_reassign_rejection(
                &state.prom,
                StatusCode::BAD_REQUEST,
                "validation_error",
                reason,
            )
        })?;

        let body_deadline = Instant::now()
            .checked_add(CLUSTER_REASSIGN_BODY_TIMEOUT)
            .unwrap_or_else(Instant::now);
        let body = tokio::time::timeout(
            CLUSTER_REASSIGN_BODY_TIMEOUT,
            Json::<ClusterReassignBody>::from_request(request, state),
        )
        .await
        .map_err(|_| {
            cluster_reassign_rejection(
                &state.prom,
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "reassign request body did not complete within 250ms",
            )
        })?;
        if Instant::now() >= body_deadline {
            return Err(cluster_reassign_rejection(
                &state.prom,
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "reassign request body did not complete within 250ms",
            ));
        }
        let Json(body) = body.map_err(|source| {
            let source_status = source.status();
            let status = match source_status {
                StatusCode::PAYLOAD_TOO_LARGE | StatusCode::UNSUPPORTED_MEDIA_TYPE => source_status,
                _ => StatusCode::BAD_REQUEST,
            };
            let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload_too_large"
            } else if status == StatusCode::UNSUPPORTED_MEDIA_TYPE {
                "unsupported_media_type"
            } else {
                "validation_error"
            };
            cluster_reassign_rejection(
                &state.prom,
                status,
                error_type,
                format!("invalid reassign body: {source}"),
            )
        })?;

        Ok(Self {
            duration,
            started,
            manager_timeout,
            body,
        })
    }
}
