//! Bounded transport extraction for the raw cluster-handoff route.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    extract::{FromRequest, Json, Query, Request},
    http::{header, HeaderValue, Method, StatusCode, Uri},
    response::Response,
};
use prometheus::HistogramTimer;
use serde::Deserialize;

use crate::handlers::search::parse_named_time_value;
use crate::state::ClusterAppState;

use super::{cluster_handoff_rejection, CLUSTER_HANDOFF_BODY_TIMEOUT, CLUSTER_HANDOFF_ENDPOINT};

const DEFAULT_CLUSTER_HANDOFF_MANAGER_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CLUSTER_HANDOFF_MANAGER_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_HANDOFF_ENDPOINT_BYTES: usize = 2_048;

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterHandoffParams {
    /// OpenSearch-inclusive spelling.
    cluster_manager_timeout: Option<String>,
    /// Elasticsearch and legacy OpenSearch spelling.
    master_timeout: Option<String>,
}

impl ClusterHandoffParams {
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
            .map(parse_cluster_handoff_manager_timeout)
            .transpose()?
            .unwrap_or(DEFAULT_CLUSTER_HANDOFF_MANAGER_TIMEOUT);
        if timeout > MAX_CLUSTER_HANDOFF_MANAGER_TIMEOUT {
            return Err("handoff manager timeout must not exceed 30s".to_string());
        }
        Ok(timeout)
    }
}

fn parse_cluster_handoff_manager_timeout(raw: &str) -> Result<Duration, String> {
    if raw == "0" {
        return Ok(Duration::ZERO);
    }
    parse_named_time_value("cluster_manager_timeout/master_timeout", raw)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(not(feature = "distributed"), allow(dead_code))]
pub(super) struct ClusterHandoffBody {
    /// Global shard position. `shard` is an additive ES/OpenSearch-shaped
    /// spelling for clients that already use shard terminology.
    #[serde(alias = "shard")]
    pub(super) position: u32,
    /// Current live primary gRPC endpoint.
    pub(super) source: String,
    /// Fresh target gRPC endpoint, outside the live replica set.
    pub(super) target: String,
    /// Required acknowledgement that this low-level primitive does not update
    /// the durable assignment map. Production placement changes use reassign.
    allow_uncommitted: bool,
}

impl ClusterHandoffBody {
    fn validate(self) -> Result<Self, String> {
        if !self.allow_uncommitted {
            return Err(
                "raw handoff changes only live routing; set `allow_uncommitted` to true or use \
                 POST /_cluster/reassign for a durable assignment change"
                    .to_string(),
            );
        }
        validate_handoff_endpoint("source", &self.source)?;
        validate_handoff_endpoint("target", &self.target)?;
        if normalized_endpoint(&self.source) == normalized_endpoint(&self.target) {
            return Err("`source` and `target` must name different endpoints".to_string());
        }
        Ok(self)
    }
}

fn validate_handoff_endpoint(name: &str, raw: &str) -> Result<(), String> {
    if raw.len() > MAX_HANDOFF_ENDPOINT_BYTES {
        return Err(format!(
            "`{name}` must not exceed {MAX_HANDOFF_ENDPOINT_BYTES} bytes"
        ));
    }
    let uri = raw
        .parse::<Uri>()
        .map_err(|_| format!("`{name}` must be an absolute http:// or https:// endpoint"))?;
    let Some(authority) = uri.authority() else {
        return Err(format!(
            "`{name}` must be an absolute http:// or https:// endpoint"
        ));
    };
    if !matches!(uri.scheme_str(), Some("http" | "https")) {
        return Err(format!(
            "`{name}` must be an absolute http:// or https:// endpoint"
        ));
    }
    if uri.host().is_none_or(str::is_empty) {
        return Err(format!("`{name}` must include a non-empty endpoint host"));
    }
    if authority.as_str().contains('@') {
        return Err(format!("`{name}` must not contain user information"));
    }
    if uri.query().is_some() || !matches!(uri.path(), "" | "/") {
        return Err(format!(
            "`{name}` must identify an endpoint authority without a path or query"
        ));
    }
    Ok(())
}

fn normalized_endpoint(endpoint: &str) -> String {
    endpoint.trim_end_matches('/').to_ascii_lowercase()
}

/// Method/query validation plus bounded strict-JSON extraction. The route
/// timer begins before every transport check.
pub(crate) struct ClusterHandoffTransport {
    duration: HistogramTimer,
    started: Instant,
    manager_timeout: Duration,
    body: ClusterHandoffBody,
}

impl ClusterHandoffTransport {
    pub(super) fn into_parts(self) -> (HistogramTimer, Instant, Duration, ClusterHandoffBody) {
        (self.duration, self.started, self.manager_timeout, self.body)
    }
}

impl FromRequest<Arc<ClusterAppState>> for ClusterHandoffTransport {
    type Rejection = Response;

    async fn from_request(
        request: Request,
        state: &Arc<ClusterAppState>,
    ) -> Result<Self, Self::Rejection> {
        let started = Instant::now();
        let duration = state
            .prom
            .http_request_duration
            .with_label_values(&[CLUSTER_HANDOFF_ENDPOINT])
            .start_timer();
        if request.method() != Method::POST {
            let mut response = cluster_handoff_rejection(
                &state.prom,
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "POST is the only supported /_cluster/handoff method",
            );
            response
                .headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static("POST"));
            return Err(response);
        }

        let Query(params) =
            Query::<ClusterHandoffParams>::try_from_uri(request.uri()).map_err(|source| {
                cluster_handoff_rejection(
                    &state.prom,
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    format!("invalid handoff query parameters: {source}"),
                )
            })?;
        let manager_timeout = params.manager_timeout().map_err(|reason| {
            cluster_handoff_rejection(
                &state.prom,
                StatusCode::BAD_REQUEST,
                "validation_error",
                reason,
            )
        })?;

        let body_deadline = Instant::now()
            .checked_add(CLUSTER_HANDOFF_BODY_TIMEOUT)
            .unwrap_or_else(Instant::now);
        let body = tokio::time::timeout(
            CLUSTER_HANDOFF_BODY_TIMEOUT,
            Json::<ClusterHandoffBody>::from_request(request, state),
        )
        .await
        .map_err(|_| {
            cluster_handoff_rejection(
                &state.prom,
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "handoff request body did not complete within 250ms",
            )
        })?;
        if Instant::now() >= body_deadline {
            return Err(cluster_handoff_rejection(
                &state.prom,
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "handoff request body did not complete within 250ms",
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
            } else {
                "validation_error"
            };
            cluster_handoff_rejection(
                &state.prom,
                status,
                error_type,
                format!("invalid handoff body: {source}"),
            )
        })?;
        let body = body.validate().map_err(|reason| {
            cluster_handoff_rejection(
                &state.prom,
                StatusCode::BAD_REQUEST,
                "validation_error",
                reason,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_validation_is_authority_only() {
        for endpoint in [
            "http://node:50051",
            "https://node:50051/",
            "HTTPS://NODE:50051",
        ] {
            validate_handoff_endpoint("source", endpoint).expect("valid endpoint");
        }
        for endpoint in [
            "node:50051",
            "https://:50051",
            "ftp://node:50051",
            "https://user@node:50051",
            "https://node:50051/path",
            "https://node:50051/?query=true",
        ] {
            assert!(
                validate_handoff_endpoint("source", endpoint).is_err(),
                "{endpoint}"
            );
        }
    }
}
