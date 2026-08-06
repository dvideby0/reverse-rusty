//! Request extraction and validation for `POST /_cluster/reconcile`.

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
    cluster_reconcile_rejection, CLUSTER_RECONCILE_BODY_TIMEOUT, CLUSTER_RECONCILE_ENDPOINT,
    DEFAULT_CLUSTER_RECONCILE_MANAGER_TIMEOUT, MAX_CLUSTER_RECONCILE_MANAGER_TIMEOUT,
};

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterReconcileParams {
    cluster_manager_timeout: Option<String>,
    master_timeout: Option<String>,
}

impl ClusterReconcileParams {
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
            .map(parse_cluster_reconcile_manager_timeout)
            .transpose()?
            .unwrap_or(DEFAULT_CLUSTER_RECONCILE_MANAGER_TIMEOUT);
        if timeout > MAX_CLUSTER_RECONCILE_MANAGER_TIMEOUT {
            return Err("reconcile manager timeout must not exceed 30s".to_string());
        }
        Ok(timeout)
    }
}

fn parse_cluster_reconcile_manager_timeout(raw: &str) -> Result<Duration, String> {
    if raw == "0" {
        return Ok(Duration::ZERO);
    }
    parse_named_time_value("cluster_manager_timeout/master_timeout", raw)
}

fn present_value<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterReconcileBodyWire {
    #[serde(default, deserialize_with = "present_value")]
    max_parallel: Option<NonZeroUsize>,
}

#[derive(Clone, Copy)]
pub(super) struct ClusterReconcileBody {
    pub(super) max_parallel: usize,
}

impl Default for ClusterReconcileBody {
    fn default() -> Self {
        Self { max_parallel: 1 }
    }
}

pub(crate) struct ClusterReconcileTransport {
    duration: HistogramTimer,
    manager_timeout: Duration,
    body: ClusterReconcileBody,
}

impl ClusterReconcileTransport {
    pub(super) fn into_parts(self) -> (HistogramTimer, Duration, ClusterReconcileBody) {
        (self.duration, self.manager_timeout, self.body)
    }
}

impl FromRequest<Arc<ClusterAppState>> for ClusterReconcileTransport {
    type Rejection = Response;

    async fn from_request(
        request: Request,
        state: &Arc<ClusterAppState>,
    ) -> Result<Self, Self::Rejection> {
        let duration = state
            .prom
            .http_request_duration
            .with_label_values(&[CLUSTER_RECONCILE_ENDPOINT])
            .start_timer();
        if request.method() != Method::POST {
            let mut response = cluster_reconcile_rejection(
                &state.prom,
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "POST is the only supported /_cluster/reconcile method",
            );
            response
                .headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static("POST"));
            return Err(response);
        }

        let Query(params) =
            Query::<ClusterReconcileParams>::try_from_uri(request.uri()).map_err(|source| {
                cluster_reconcile_rejection(
                    &state.prom,
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    format!("invalid reconcile query parameters: {source}"),
                )
            })?;
        let manager_timeout = params.manager_timeout().map_err(|reason| {
            cluster_reconcile_rejection(
                &state.prom,
                StatusCode::BAD_REQUEST,
                "validation_error",
                reason,
            )
        })?;

        let has_json_content_type = is_json_content_type(request.headers());
        let body_deadline = Instant::now()
            .checked_add(CLUSTER_RECONCILE_BODY_TIMEOUT)
            .unwrap_or_else(Instant::now);
        let bytes = tokio::time::timeout(
            CLUSTER_RECONCILE_BODY_TIMEOUT,
            Bytes::from_request(request, state),
        )
        .await
        .map_err(|_| {
            cluster_reconcile_rejection(
                &state.prom,
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "reconcile body did not complete within 250ms",
            )
        })?;
        if Instant::now() >= body_deadline {
            return Err(cluster_reconcile_rejection(
                &state.prom,
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "reconcile body did not complete within 250ms",
            ));
        }
        let bytes = bytes.map_err(|source| {
            let status = source.status();
            let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload_too_large"
            } else {
                "validation_error"
            };
            cluster_reconcile_rejection(
                &state.prom,
                status,
                error_type,
                format!("invalid reconcile body: {source}"),
            )
        })?;

        let body = if bytes.is_empty() {
            ClusterReconcileBody::default()
        } else {
            if !has_json_content_type {
                return Err(cluster_reconcile_rejection(
                    &state.prom,
                    StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    "unsupported_media_type",
                    "a non-empty POST /_cluster/reconcile body requires Content-Type: \
                     application/json",
                ));
            }
            if bytes
                .iter()
                .find(|byte| !byte.is_ascii_whitespace())
                .copied()
                != Some(b'{')
            {
                return Err(cluster_reconcile_rejection(
                    &state.prom,
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    "the reconcile JSON body must be an object",
                ));
            }
            let wire =
                serde_json::from_slice::<ClusterReconcileBodyWire>(&bytes).map_err(|source| {
                    cluster_reconcile_rejection(
                        &state.prom,
                        StatusCode::BAD_REQUEST,
                        "validation_error",
                        format!("invalid reconcile JSON body: {source}"),
                    )
                })?;
            ClusterReconcileBody {
                max_parallel: wire.max_parallel.map_or(1, NonZeroUsize::get),
            }
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
