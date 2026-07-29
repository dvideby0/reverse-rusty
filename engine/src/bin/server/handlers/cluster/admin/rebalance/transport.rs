//! Request extraction and validation for `POST /_cluster/rebalance`.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::Bytes,
    extract::{FromRequest, Query, Request},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::Response,
};
use prometheus::HistogramTimer;
use serde::{Deserialize, Deserializer};

use crate::handlers::search::parse_named_time_value;
use crate::state::ClusterAppState;

use super::{
    cluster_rebalance_rejection, CLUSTER_REBALANCE_BODY_TIMEOUT, CLUSTER_REBALANCE_ENDPOINT,
    DEFAULT_CLUSTER_REBALANCE_MANAGER_TIMEOUT, MAX_CLUSTER_REBALANCE_MANAGER_TIMEOUT,
};

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterRebalanceParams {
    /// OpenSearch-inclusive spelling.
    cluster_manager_timeout: Option<String>,
    /// Elasticsearch and legacy OpenSearch spelling.
    master_timeout: Option<String>,
}

impl ClusterRebalanceParams {
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
            .map(parse_cluster_rebalance_manager_timeout)
            .transpose()?
            .unwrap_or(DEFAULT_CLUSTER_REBALANCE_MANAGER_TIMEOUT);
        if timeout > MAX_CLUSTER_REBALANCE_MANAGER_TIMEOUT {
            return Err("rebalance manager timeout must not exceed 30s".to_string());
        }
        Ok(timeout)
    }
}

fn parse_cluster_rebalance_manager_timeout(raw: &str) -> Result<Duration, String> {
    if raw == "0" {
        return Ok(Duration::ZERO);
    }
    parse_named_time_value("cluster_manager_timeout/master_timeout", raw)
}

/// Deserialize a present value as `T`, while leaving field absence to
/// `#[serde(default)]`. Unlike `Option<T>`'s normal deserializer, JSON `null`
/// is rejected instead of being conflated with an omitted control.
fn present_value<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ClusterRebalanceBody {
    /// `None` selects the topology-safe default: physical movement for a
    /// remote cluster and a map-only commit for an in-process cluster.
    #[serde(default, rename = "move", deserialize_with = "present_value")]
    pub(super) do_move: Option<bool>,
    /// Conflict-free wave width for a remote data-moving pass.
    #[serde(default, deserialize_with = "present_value")]
    pub(super) max_parallel: Option<NonZeroUsize>,
}

impl ClusterRebalanceBody {
    fn validate(self) -> Result<Self, String> {
        if self.do_move == Some(false) && self.max_parallel.is_some() {
            return Err(
                "`max_parallel` cannot be specified when `move` is explicitly false".to_string(),
            );
        }
        Ok(self)
    }
}

pub(crate) struct ClusterRebalanceTransport {
    duration: HistogramTimer,
    manager_timeout: Duration,
    body: ClusterRebalanceBody,
}

impl ClusterRebalanceTransport {
    pub(super) fn into_parts(self) -> (HistogramTimer, Duration, ClusterRebalanceBody) {
        (self.duration, self.manager_timeout, self.body)
    }
}

impl FromRequest<Arc<ClusterAppState>> for ClusterRebalanceTransport {
    type Rejection = Response;

    async fn from_request(
        request: Request,
        state: &Arc<ClusterAppState>,
    ) -> Result<Self, Self::Rejection> {
        let duration = state
            .prom
            .http_request_duration
            .with_label_values(&[CLUSTER_REBALANCE_ENDPOINT])
            .start_timer();
        if request.method() != Method::POST {
            let mut response = cluster_rebalance_rejection(
                &state.prom,
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "POST is the only supported /_cluster/rebalance method",
            );
            response
                .headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static("POST"));
            return Err(response);
        }

        let Query(params) =
            Query::<ClusterRebalanceParams>::try_from_uri(request.uri()).map_err(|source| {
                cluster_rebalance_rejection(
                    &state.prom,
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    format!("invalid rebalance query parameters: {source}"),
                )
            })?;
        let manager_timeout = params.manager_timeout().map_err(|reason| {
            cluster_rebalance_rejection(
                &state.prom,
                StatusCode::BAD_REQUEST,
                "validation_error",
                reason,
            )
        })?;

        let has_json_content_type = is_json_content_type(request.headers());
        let body_deadline = Instant::now()
            .checked_add(CLUSTER_REBALANCE_BODY_TIMEOUT)
            .unwrap_or_else(Instant::now);
        let bytes = tokio::time::timeout(
            CLUSTER_REBALANCE_BODY_TIMEOUT,
            Bytes::from_request(request, state),
        )
        .await
        .map_err(|_| {
            cluster_rebalance_rejection(
                &state.prom,
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "rebalance body did not complete within 250ms",
            )
        })?;
        if Instant::now() >= body_deadline {
            return Err(cluster_rebalance_rejection(
                &state.prom,
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "rebalance body did not complete within 250ms",
            ));
        }
        let bytes = bytes.map_err(|source| {
            let status = source.status();
            let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload_too_large"
            } else {
                "validation_error"
            };
            cluster_rebalance_rejection(
                &state.prom,
                status,
                error_type,
                format!("invalid rebalance body: {source}"),
            )
        })?;

        let body = if bytes.is_empty() {
            ClusterRebalanceBody::default()
        } else {
            if !has_json_content_type {
                return Err(cluster_rebalance_rejection(
                    &state.prom,
                    StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    "unsupported_media_type",
                    "a non-empty POST /_cluster/rebalance body requires Content-Type: \
                     application/json",
                ));
            }
            if bytes
                .iter()
                .find(|byte| !byte.is_ascii_whitespace())
                .copied()
                != Some(b'{')
            {
                return Err(cluster_rebalance_rejection(
                    &state.prom,
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    "the rebalance JSON body must be an object",
                ));
            }
            serde_json::from_slice::<ClusterRebalanceBody>(&bytes)
                .map_err(|source| {
                    cluster_rebalance_rejection(
                        &state.prom,
                        StatusCode::BAD_REQUEST,
                        "validation_error",
                        format!("invalid rebalance JSON body: {source}"),
                    )
                })?
                .validate()
                .map_err(|reason| {
                    cluster_rebalance_rejection(
                        &state.prom,
                        StatusCode::BAD_REQUEST,
                        "validation_error",
                        reason,
                    )
                })?
        };

        Ok(Self {
            duration,
            manager_timeout,
            body,
        })
    }
}

fn is_json_content_type(headers: &HeaderMap) -> bool {
    let Some(value) = headers.get(header::CONTENT_TYPE) else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };
    let media_type = value
        .split_once(';')
        .map_or(value, |(media_type, _)| media_type)
        .trim()
        .to_ascii_lowercase();
    media_type == "application/json"
        || media_type
            .strip_prefix("application/")
            .is_some_and(|subtype| subtype.ends_with("+json"))
}
