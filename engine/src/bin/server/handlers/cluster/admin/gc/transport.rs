//! Request extraction and validation for bodyless `POST /_cluster/gc`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::Bytes,
    extract::{FromRequest, Query, Request},
    http::{header, HeaderValue, Method, StatusCode},
    response::Response,
};
use prometheus::HistogramTimer;
use serde::Deserialize;

use crate::handlers::search::parse_named_time_value;
use crate::state::ClusterAppState;

use super::{
    cluster_gc_rejection, CLUSTER_GC_BODY_TIMEOUT, CLUSTER_GC_ENDPOINT,
    DEFAULT_CLUSTER_GC_MANAGER_TIMEOUT, MAX_CLUSTER_GC_MANAGER_TIMEOUT,
};

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterGcParams {
    cluster_manager_timeout: Option<String>,
    master_timeout: Option<String>,
}

impl ClusterGcParams {
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
            .map(parse_cluster_gc_manager_timeout)
            .transpose()?
            .unwrap_or(DEFAULT_CLUSTER_GC_MANAGER_TIMEOUT);
        if timeout > MAX_CLUSTER_GC_MANAGER_TIMEOUT {
            return Err("GC manager timeout must not exceed 30s".to_string());
        }
        Ok(timeout)
    }
}

fn parse_cluster_gc_manager_timeout(raw: &str) -> Result<Duration, String> {
    if raw == "0" {
        return Ok(Duration::ZERO);
    }
    parse_named_time_value("cluster_manager_timeout/master_timeout", raw)
}

pub(crate) struct ClusterGcTransport {
    duration: HistogramTimer,
    manager_timeout: Duration,
}

impl ClusterGcTransport {
    pub(super) fn into_parts(self) -> (HistogramTimer, Duration) {
        (self.duration, self.manager_timeout)
    }
}

impl FromRequest<Arc<ClusterAppState>> for ClusterGcTransport {
    type Rejection = Response;

    async fn from_request(
        request: Request,
        state: &Arc<ClusterAppState>,
    ) -> Result<Self, Self::Rejection> {
        let duration = state
            .prom
            .http_request_duration
            .with_label_values(&[CLUSTER_GC_ENDPOINT])
            .start_timer();
        if request.method() != Method::POST {
            let mut response = cluster_gc_rejection(
                &state.prom,
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "POST is the only supported /_cluster/gc method",
            );
            response
                .headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static("POST"));
            return Err(response);
        }

        let Query(params) =
            Query::<ClusterGcParams>::try_from_uri(request.uri()).map_err(|source| {
                cluster_gc_rejection(
                    &state.prom,
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    format!("invalid GC query parameters: {source}"),
                )
            })?;
        let manager_timeout = params.manager_timeout().map_err(|reason| {
            cluster_gc_rejection(
                &state.prom,
                StatusCode::BAD_REQUEST,
                "validation_error",
                reason,
            )
        })?;

        let body_deadline = Instant::now()
            .checked_add(CLUSTER_GC_BODY_TIMEOUT)
            .unwrap_or_else(Instant::now);
        let body =
            tokio::time::timeout(CLUSTER_GC_BODY_TIMEOUT, Bytes::from_request(request, state))
                .await
                .map_err(|_| {
                    cluster_gc_rejection(
                        &state.prom,
                        StatusCode::REQUEST_TIMEOUT,
                        "request_timeout",
                        "GC request body did not complete within 250ms",
                    )
                })?;
        // Tokio polls a ready body before its timeout timer. Preserve the
        // absolute boundary even for a late-ready extraction error.
        if Instant::now() >= body_deadline {
            return Err(cluster_gc_rejection(
                &state.prom,
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "GC request body did not complete within 250ms",
            ));
        }
        let body = body.map_err(|source| {
            let status = source.status();
            let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload_too_large"
            } else {
                "validation_error"
            };
            cluster_gc_rejection(
                &state.prom,
                status,
                error_type,
                format!("invalid GC body: {source}"),
            )
        })?;
        if !body.is_empty() {
            return Err(cluster_gc_rejection(
                &state.prom,
                StatusCode::BAD_REQUEST,
                "validation_error",
                "POST /_cluster/gc does not accept a request body",
            ));
        }

        Ok(Self {
            duration,
            manager_timeout,
        })
    }
}
