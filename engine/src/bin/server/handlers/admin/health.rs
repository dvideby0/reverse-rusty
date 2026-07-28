//! Strict native `GET`/`HEAD /_health` readiness contract.
//!
//! Reverse Rusty's colors describe serving and durability dependencies rather
//! than Elasticsearch/OpenSearch index-shard allocation. The endpoint keeps the
//! native path and payload, while adopting the familiar `wait_for_status` and
//! `timeout` controls whose behavior maps exactly.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::{Body, Bytes},
    extract::{
        rejection::{BytesRejection, QueryRejection},
        FromRequest, Query, Request, State,
    },
    http::{header, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::dto::ApiError;
use crate::handlers::search::parse_named_time_value;
use crate::metrics::PrometheusMetrics;
use crate::state::{AppState, RequestCtx};

pub(crate) const HEALTH_ENDPOINT: &str = "health";
pub(crate) const HEALTH_BODY_LIMIT: usize = 64 * 1024;
pub(crate) const HEALTH_BODY_READ_TIMEOUT: Duration = Duration::from_millis(250);
pub(crate) const HEALTH_POLL_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_HEALTH_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum HealthStatus {
    Red,
    Yellow,
    Green,
}

impl HealthStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Red => "red",
            Self::Yellow => "yellow",
            Self::Green => "green",
        }
    }

    fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "red" => Ok(Self::Red),
            "yellow" => Ok(Self::Yellow),
            "green" => Ok(Self::Green),
            other => Err(format!(
                "`wait_for_status` must be red, yellow, or green (got `{other}`)"
            )),
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HealthParams {
    wait_for_status: Option<String>,
    timeout: Option<String>,
    level: Option<String>,
}

pub(crate) struct HealthRequest {
    wait_for_status: Option<HealthStatus>,
    timeout: Duration,
}

impl HealthRequest {
    fn resolve(params: &HealthParams) -> Result<Self, String> {
        if let Some(level) = params.level.as_deref() {
            if level != "cluster" {
                return Err(format!(
                    "`level` must be `cluster`; native /_health has no index or shard detail \
                     (got `{level}`)"
                ));
            }
        }
        let wait_for_status = params
            .wait_for_status
            .as_deref()
            .map(HealthStatus::parse)
            .transpose()?;
        let timeout = params
            .timeout
            .as_deref()
            .map(|raw| parse_named_time_value("timeout", raw))
            .transpose()?
            .unwrap_or(DEFAULT_HEALTH_TIMEOUT);
        Ok(Self {
            wait_for_status,
            timeout,
        })
    }

    pub(crate) fn satisfied_by(&self, status: HealthStatus) -> bool {
        self.wait_for_status
            .is_none_or(|requested| status >= requested)
    }

    pub(crate) const fn waits_for_status(&self) -> bool {
        self.wait_for_status.is_some()
    }

    pub(crate) fn deadline(&self) -> Result<Instant, String> {
        Instant::now()
            .checked_add(self.timeout)
            .ok_or_else(|| "`timeout` is too large for this platform".to_string())
    }
}

#[derive(Clone, Copy)]
struct StandaloneHealth {
    total_queries: usize,
    wal_healthy: bool,
    persistence_healthy: bool,
    skipped_segments: usize,
    stale_segments: usize,
}

impl StandaloneHealth {
    const fn status(self) -> HealthStatus {
        if !self.wal_healthy || !self.persistence_healthy {
            HealthStatus::Red
        } else if self.skipped_segments > 0 || self.stale_segments > 0 {
            HealthStatus::Yellow
        } else {
            HealthStatus::Green
        }
    }
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    mode: &'static str,
    timed_out: bool,
    total_queries: usize,
    wal_healthy: bool,
    persistence_healthy: bool,
    skipped_segments: usize,
    stale_segments: usize,
}

/// Method validation, admission, and bounded body extraction for `/_health`.
///
/// `/_health` intentionally bypasses read authentication, so its independent
/// cap must be held before buffering an untrusted body. Body extraction itself
/// has a short deadline so a handful of slow clients cannot retain every permit
/// indefinitely. The permit then remains held until the response is complete.
pub(crate) struct HealthTransport {
    permit: OwnedSemaphorePermit,
    head: bool,
    body: Result<Bytes, BytesRejection>,
}

impl HealthTransport {
    pub(crate) fn into_parts(self) -> (OwnedSemaphorePermit, bool, Result<Bytes, BytesRejection>) {
        (self.permit, self.head, self.body)
    }
}

impl<S> FromRequest<Arc<S>> for HealthTransport
where
    S: RequestCtx,
{
    type Rejection = Response;

    async fn from_request(request: Request, state: &Arc<S>) -> Result<Self, Self::Rejection> {
        let head =
            validate_health_method(state.prom(), request.method()).map_err(|response| *response)?;
        let permit = try_acquire_health_work(state.health_permits(), state.prom(), head)
            .map_err(|response| *response)?;
        let body = tokio::time::timeout(
            HEALTH_BODY_READ_TIMEOUT,
            Bytes::from_request(request, state),
        )
        .await
        .map_err(|_| {
            health_rejection(
                state.prom(),
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "health request body did not complete within 250ms",
                head,
            )
        })?;
        Ok(Self { permit, head, body })
    }
}

/// Native readiness and durability health.
///
/// Snapshot reads are lock-free, so a waiting request polls without consuming a
/// blocking worker. Red is a 503; an unmet `wait_for_status` is a familiar 408
/// with `timed_out=true`.
pub(crate) async fn health(
    State(state): State<Arc<AppState>>,
    params: Result<Query<HealthParams>, QueryRejection>,
    transport: HealthTransport,
) -> Response {
    let _duration = state
        .prom
        .http_request_duration
        .with_label_values(&[HEALTH_ENDPOINT])
        .start_timer();
    let (_permit, head, body) = transport.into_parts();
    let request = match validate_health_request(&state.prom, params, body, head) {
        Ok(request) => request,
        Err(response) => return *response,
    };
    let deadline = match request.deadline() {
        Ok(deadline) => deadline,
        Err(reason) => {
            return health_rejection(
                &state.prom,
                StatusCode::BAD_REQUEST,
                "validation_error",
                reason,
                head,
            )
        }
    };

    let mut waited = false;
    loop {
        let current = {
            let snapshot = state.snapshot.load();
            StandaloneHealth {
                total_queries: snapshot.num_queries(),
                wal_healthy: snapshot.wal_healthy(),
                persistence_healthy: snapshot.persistence_healthy(),
                skipped_segments: snapshot.skipped_segments(),
                stale_segments: snapshot.stale_segment_count(),
            }
        };
        if waited && Instant::now() >= deadline {
            return finish_health_response(&state.prom, standalone_response(current, true), head);
        }
        if request.satisfied_by(current.status()) {
            return finish_health_response(&state.prom, standalone_response(current, false), head);
        }
        let Some(delay) = wait_delay(deadline) else {
            return finish_health_response(&state.prom, standalone_response(current, true), head);
        };
        tokio::time::sleep(delay).await;
        waited = true;
    }
}

pub(crate) fn validate_health_method(
    prom: &PrometheusMetrics,
    method: &Method,
) -> Result<bool, Box<Response>> {
    if method == Method::GET {
        return Ok(false);
    }
    if method == Method::HEAD {
        return Ok(true);
    }
    let mut response = health_rejection(
        prom,
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        "GET and HEAD are the only supported /_health methods",
        false,
    );
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("GET, HEAD"));
    Err(Box::new(response))
}

pub(crate) fn validate_health_request(
    prom: &PrometheusMetrics,
    params: Result<Query<HealthParams>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
    head: bool,
) -> Result<HealthRequest, Box<Response>> {
    let Query(params) = params.map_err(|error| {
        Box::new(health_rejection(
            prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            format!("invalid health query parameters: {error}"),
            head,
        ))
    })?;
    let body = body.map_err(|error| {
        let status = error.status();
        let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
            "payload_too_large"
        } else {
            "validation_error"
        };
        Box::new(health_rejection(
            prom,
            status,
            error_type,
            format!("invalid health body: {error}"),
            head,
        ))
    })?;
    if !body.is_empty() {
        return Err(Box::new(health_rejection(
            prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            "GET/HEAD /_health does not accept a request body",
            head,
        )));
    }
    HealthRequest::resolve(&params).map_err(|reason| {
        Box::new(health_rejection(
            prom,
            StatusCode::BAD_REQUEST,
            "validation_error",
            reason,
            head,
        ))
    })
}

pub(crate) fn wait_delay(deadline: Instant) -> Option<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .map(|remaining| remaining.min(HEALTH_POLL_INTERVAL))
}

fn try_acquire_health_work(
    permits: &Arc<Semaphore>,
    prom: &PrometheusMetrics,
    head: bool,
) -> Result<OwnedSemaphorePermit, Box<Response>> {
    try_acquire_health_work_from(permits, prom, head)
}

fn try_acquire_health_work_from(
    permits: &Arc<Semaphore>,
    prom: &PrometheusMetrics,
    head: bool,
) -> Result<OwnedSemaphorePermit, Box<Response>> {
    Arc::clone(permits).try_acquire_owned().map_err(|_| {
        let mut response = health_rejection(
            prom,
            StatusCode::TOO_MANY_REQUESTS,
            "rejected_execution_exception",
            "too many concurrent /_health probes or status waits",
            head,
        );
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        Box::new(response)
    })
}

pub(crate) fn health_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
    head: bool,
) -> Response {
    finish_health_response(
        prom,
        ApiError::response(status, error_type, reason).into_response(),
        head,
    )
}

pub(crate) fn finish_health_response(
    prom: &PrometheusMetrics,
    mut response: Response,
    head: bool,
) -> Response {
    prom.http_requests_total
        .with_label_values(&[HEALTH_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if head {
        *response.body_mut() = Body::empty();
    }
    response
}

fn standalone_response(current: StandaloneHealth, timed_out: bool) -> Response {
    let status = if timed_out {
        StatusCode::REQUEST_TIMEOUT
    } else if current.status() == HealthStatus::Red {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };
    (
        status,
        Json(HealthResponse {
            status: current.status().as_str(),
            mode: "standalone",
            timed_out,
            total_queries: current.total_queries,
            wal_healthy: current.wal_healthy,
            persistence_healthy: current.persistence_healthy,
            skipped_segments: current.skipped_segments,
            stale_segments: current.stale_segments,
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_order_and_http_mapping_are_fail_loud() {
        assert!(HealthStatus::Green > HealthStatus::Yellow);
        assert!(HealthStatus::Yellow > HealthStatus::Red);
        let red = StandaloneHealth {
            total_queries: 0,
            wal_healthy: false,
            persistence_healthy: true,
            skipped_segments: 0,
            stale_segments: 0,
        };
        assert_eq!(
            standalone_response(red, false).status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            standalone_response(red, true).status(),
            StatusCode::REQUEST_TIMEOUT
        );
    }

    #[test]
    fn familiar_controls_are_strict() {
        let request = HealthRequest::resolve(&HealthParams {
            wait_for_status: Some("yellow".to_string()),
            timeout: Some("250ms".to_string()),
            level: Some("cluster".to_string()),
        })
        .expect("supported controls");
        assert!(request.satisfied_by(HealthStatus::Green));
        assert!(request.satisfied_by(HealthStatus::Yellow));
        assert!(!request.satisfied_by(HealthStatus::Red));
        assert_eq!(request.timeout, Duration::from_millis(250));

        assert!(HealthRequest::resolve(&HealthParams {
            wait_for_status: Some("blue".to_string()),
            ..HealthParams::default()
        })
        .is_err());
        assert!(HealthRequest::resolve(&HealthParams {
            level: Some("shards".to_string()),
            ..HealthParams::default()
        })
        .is_err());
    }

    #[test]
    fn unauthenticated_health_work_admission_is_independently_bounded() {
        let prom = PrometheusMetrics::new();
        let admission = Arc::new(Semaphore::new(2));
        let permits = (0..2)
            .map(|_| {
                try_acquire_health_work_from(&admission, &prom, false).expect("bounded permit")
            })
            .collect::<Vec<_>>();
        let response = try_acquire_health_work_from(&admission, &prom, false)
            .expect_err("work beyond the cap is rejected");
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response.headers().get(header::RETRY_AFTER).expect("retry"),
            "1"
        );
        assert_eq!(
            response
                .headers()
                .get(header::CACHE_CONTROL)
                .expect("cache"),
            "no-store"
        );
        drop(permits);
    }
}
