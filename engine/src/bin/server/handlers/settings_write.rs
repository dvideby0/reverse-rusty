//! Strict native `PUT /_settings` runtime-configuration update.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::Bytes,
    extract::{FromRequest, Query, Request, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
};
use prometheus::HistogramTimer;
use serde::{
    de::{self, MapAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tracing::{error, info};

use reverse_rusty::config::EngineConfig;

use super::search::parse_named_time_value;
use crate::dto::ApiError;
use crate::metrics::PrometheusMetrics;
use crate::state::{AppState, RequestCtx};

pub(crate) const SETTINGS_WRITE_BODY_LIMIT: usize = 64 * 1024;
pub(crate) const SETTINGS_WRITE_BODY_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const SETTINGS_WRITE_DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const SETTINGS_WRITE_MAX_TIMEOUT: Duration = Duration::from_secs(30);
const SETTINGS_WRITE_RESPONSE_LIMIT: usize = 64 * 1024;
const SETTINGS_WRITE_ENDPOINT: &str = "settings_put";

#[derive(Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct SettingsWriteParams {
    /// Familiar ES/OpenSearch representation control. Native keys and the
    /// response are already flat, so either value is representation-identical.
    #[serde(default, rename = "flat_settings")]
    _flat_settings: bool,
    timeout: Option<String>,
}

/// A duplicate-safe top-level settings object.
///
/// `serde_json::Value` silently keeps only the last duplicate object key. That
/// is unsafe for an operator mutation, so this visitor rejects ambiguity before
/// the patch is validated or applied.
pub(crate) struct SettingsPatch(serde_json::Map<String, serde_json::Value>);

impl SettingsPatch {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn into_inner(self) -> serde_json::Map<String, serde_json::Value> {
        self.0
    }
}

impl<'de> Deserialize<'de> for SettingsPatch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SettingsPatchVisitor;

        impl<'de> Visitor<'de> for SettingsPatchVisitor {
            type Value = SettingsPatch;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON object of native setting keys")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut patch = serde_json::Map::new();
                while let Some(key) = map.next_key::<String>()? {
                    if patch.contains_key(&key) {
                        return Err(de::Error::custom(format!("duplicate setting key [{key}]")));
                    }
                    let value = map.next_value()?;
                    patch.insert(key, value);
                }
                Ok(SettingsPatch(patch))
            }
        }

        deserializer.deserialize_map(SettingsPatchVisitor)
    }
}

/// Strict query/media/body transport shared by standalone and coordinator mode.
pub(crate) struct SettingsWriteTransport {
    duration: HistogramTimer,
    timeout: Duration,
    patch: SettingsPatch,
}

impl SettingsWriteTransport {
    pub(crate) fn into_parts(self) -> (HistogramTimer, Duration, SettingsPatch) {
        (self.duration, self.timeout, self.patch)
    }
}

impl<S> FromRequest<Arc<S>> for SettingsWriteTransport
where
    S: RequestCtx,
{
    type Rejection = Response;

    async fn from_request(request: Request, state: &Arc<S>) -> Result<Self, Self::Rejection> {
        let duration = state
            .prom()
            .http_request_duration
            .with_label_values(&[SETTINGS_WRITE_ENDPOINT])
            .start_timer();
        if request.method() != Method::PUT {
            return Err(settings_write_rejection(
                state.prom(),
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "PUT is the settings update method supported by /_settings",
            ));
        }

        let Query(params) =
            Query::<SettingsWriteParams>::try_from_uri(request.uri()).map_err(|source| {
                settings_write_rejection(
                    state.prom(),
                    StatusCode::BAD_REQUEST,
                    "validation_error",
                    format!("invalid settings query parameters: {source}"),
                )
            })?;
        let timeout = parse_settings_timeout(params.timeout.as_deref()).map_err(|reason| {
            settings_write_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "validation_error",
                reason,
            )
        })?;
        if !is_json_content_type(request.headers()) {
            return Err(settings_write_rejection(
                state.prom(),
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                "PUT /_settings requires Content-Type: application/json",
            ));
        }

        let body = tokio::time::timeout(
            SETTINGS_WRITE_BODY_TIMEOUT,
            Bytes::from_request(request, state),
        )
        .await
        .map_err(|_| {
            settings_write_rejection(
                state.prom(),
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "settings write body did not complete within 5s",
            )
        })?
        .map_err(|source| {
            let status = source.status();
            let error_type = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload_too_large"
            } else {
                "validation_error"
            };
            settings_write_rejection(
                state.prom(),
                status,
                error_type,
                format!("invalid settings write body: {source}"),
            )
        })?;
        let patch: SettingsPatch = serde_json::from_slice(&body).map_err(|source| {
            settings_write_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "validation_error",
                format!("invalid settings JSON body: {source}"),
            )
        })?;
        if patch.is_empty() {
            return Err(settings_write_rejection(
                state.prom(),
                StatusCode::BAD_REQUEST,
                "settings_error",
                "no settings provided",
            ));
        }

        Ok(Self {
            duration,
            timeout,
            patch,
        })
    }
}

fn parse_settings_timeout(raw: Option<&str>) -> Result<Duration, String> {
    let timeout = match raw {
        Some("0") => Duration::ZERO,
        Some(raw) => parse_named_time_value("timeout", raw)?,
        None => SETTINGS_WRITE_DEFAULT_TIMEOUT,
    };
    if timeout > SETTINGS_WRITE_MAX_TIMEOUT {
        return Err("`timeout` must be no greater than 30s".to_string());
    }
    Ok(timeout)
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

#[derive(Serialize)]
struct PutSettingsResponse<'a> {
    acknowledged: bool,
    /// Updates are live-only; startup configuration remains the durable source.
    persistent: bool,
    settings: &'a EngineConfig,
}

#[derive(Debug)]
enum SettingsWriteWorkerError {
    Serialization(serde_json::Error),
    ResponseTooLarge(usize),
}

enum SettingsWriteOutcome {
    Applied(Vec<u8>),
    Invalid(Vec<String>),
    TimedOut,
    Failed(SettingsWriteWorkerError),
}

fn serialize_settings_response(config: &EngineConfig) -> Result<Vec<u8>, SettingsWriteWorkerError> {
    let encoded = serde_json::to_vec(&PutSettingsResponse {
        acknowledged: true,
        persistent: false,
        settings: config,
    })
    .map_err(SettingsWriteWorkerError::Serialization)?;
    if encoded.len() > SETTINGS_WRITE_RESPONSE_LIMIT {
        return Err(SettingsWriteWorkerError::ResponseTooLarge(encoded.len()));
    }
    Ok(encoded)
}

pub(crate) async fn acquire_settings_write_permit(
    permits: &Arc<Semaphore>,
    prom: &PrometheusMetrics,
    deadline: Instant,
) -> Result<OwnedSemaphorePermit, Response> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Arc::clone(permits).try_acquire_owned().map_err(|source| {
            let (status, error_type, reason) = match source {
                TryAcquireError::NoPermits => (
                    StatusCode::REQUEST_TIMEOUT,
                    "request_timeout",
                    "settings update timed out while waiting for administrative admission",
                ),
                TryAcquireError::Closed => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "settings_unavailable",
                    "settings write admission is closed",
                ),
            };
            settings_write_rejection(prom, status, error_type, reason)
        });
    }
    match tokio::time::timeout(remaining, Arc::clone(permits).acquire_owned()).await {
        Ok(Ok(permit)) => Ok(permit),
        Ok(Err(_)) => Err(settings_write_rejection(
            prom,
            StatusCode::SERVICE_UNAVAILABLE,
            "settings_unavailable",
            "settings write admission is closed",
        )),
        Err(_) => Err(settings_write_rejection(
            prom,
            StatusCode::REQUEST_TIMEOUT,
            "request_timeout",
            "settings update timed out while waiting for administrative admission",
        )),
    }
}

/// Update the live config on a bounded blocking worker. The worker owns
/// admission through mutation, coherent snapshot publication, and response
/// serialization, so cancellation cannot publish a partial or hidden update.
pub(crate) async fn put_settings(
    State(state): State<Arc<AppState>>,
    transport: SettingsWriteTransport,
) -> Response {
    let (_duration, timeout, patch) = transport.into_parts();
    let deadline = Instant::now()
        .checked_add(timeout)
        .unwrap_or_else(Instant::now);
    let permit =
        match acquire_settings_write_permit(&state.stats_permits, &state.prom, deadline).await {
            Ok(permit) => permit,
            Err(response) => return response,
        };
    let work_state = Arc::clone(&state);
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let remaining = deadline.saturating_duration_since(Instant::now());
        let Some(mut engine) = (if remaining.is_zero() {
            work_state.engine.try_lock()
        } else {
            work_state.engine.try_lock_for(remaining)
        }) else {
            return SettingsWriteOutcome::TimedOut;
        };
        let updated = match apply_settings_patch(engine.config().clone(), &patch.into_inner()) {
            Ok(updated) => updated,
            Err(problems) => return SettingsWriteOutcome::Invalid(problems),
        };
        let encoded = match serialize_settings_response(&updated) {
            Ok(encoded) => encoded,
            Err(source) => return SettingsWriteOutcome::Failed(source),
        };

        engine.set_config(updated);
        // Publish from the same engine guard as the config mutation. This makes
        // the acknowledgement and lock-free GET view one coherent commit.
        work_state.publish_snapshot_from_locked_engine(&engine);
        SettingsWriteOutcome::Applied(encoded)
    });

    let response = match worker.await {
        Ok(SettingsWriteOutcome::Applied(encoded)) => {
            info!("runtime settings updated");
            (
                StatusCode::OK,
                [(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                )],
                encoded,
            )
                .into_response()
        }
        Ok(SettingsWriteOutcome::Invalid(problems)) => settings_write_error_response(
            StatusCode::BAD_REQUEST,
            "settings_error",
            problems.join("; "),
        ),
        Ok(SettingsWriteOutcome::TimedOut) => settings_write_error_response(
            StatusCode::REQUEST_TIMEOUT,
            "request_timeout",
            "settings update timed out before the engine lock became available",
        ),
        Ok(SettingsWriteOutcome::Failed(SettingsWriteWorkerError::Serialization(source))) => {
            error!(error = %source, "failed to serialize settings write response");
            settings_write_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "settings_unavailable",
                "settings response serialization failed",
            )
        }
        Ok(SettingsWriteOutcome::Failed(SettingsWriteWorkerError::ResponseTooLarge(bytes))) => {
            error!(bytes, "settings write response exceeded its fixed ceiling");
            settings_write_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "settings_unavailable",
                "settings response exceeded the fixed response limit",
            )
        }
        Err(join_error) => {
            error!(error = %join_error, "settings write worker failed");
            settings_write_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "settings_unavailable",
                "settings write worker failed",
            )
        }
    };
    finish_settings_write_response(&state.prom, response)
}

/// Apply a flat settings patch to `cfg`, enforcing the dynamic/static split,
/// value types, and the engine's range validation. Every key is checked and any
/// error rejects the complete patch.
pub(crate) fn apply_settings_patch(
    mut cfg: EngineConfig,
    patch: &serde_json::Map<String, serde_json::Value>,
) -> Result<EngineConfig, Vec<String>> {
    let mut errors = Vec::new();
    for (key, val) in patch {
        match key.as_str() {
            "max_segments" => set_usize(&mut cfg.max_segments, key, val, &mut errors),
            "memtable_flush_threshold" => {
                set_usize(&mut cfg.memtable_flush_threshold, key, val, &mut errors);
            }
            "max_query_length" => set_usize(&mut cfg.max_query_length, key, val, &mut errors),
            "max_query_clauses" => set_usize(&mut cfg.max_query_clauses, key, val, &mut errors),
            "max_anyof_group_size" => {
                set_usize(&mut cfg.max_anyof_group_size, key, val, &mut errors);
            }
            "max_tags" => set_usize(&mut cfg.max_tags, key, val, &mut errors),
            "holes_ratio_threshold" => {
                set_f64(&mut cfg.holes_ratio_threshold, key, val, &mut errors);
            }
            "compaction_fixed_cost" => {
                set_f64(&mut cfg.compaction_fixed_cost, key, val, &mut errors);
            }
            "auto_compact_on_flush" => {
                set_bool(&mut cfg.auto_compact_on_flush, key, val, &mut errors);
            }
            "auto_compact_on_ingest" => {
                set_bool(&mut cfg.auto_compact_on_ingest, key, val, &mut errors);
            }
            "compaction_reanchor" => {
                set_bool(&mut cfg.compaction_reanchor, key, val, &mut errors);
            }
            "broad_batch_size" => set_usize(&mut cfg.broad_batch_size, key, val, &mut errors),
            "max_percolate_batch" => {
                set_usize(&mut cfg.max_percolate_batch, key, val, &mut errors);
            }
            "broad_columnar" => set_bool(&mut cfg.broad_columnar, key, val, &mut errors),
            "broad_materialize" => set_bool(&mut cfg.broad_materialize, key, val, &mut errors),
            "broad_prefilter" => set_bool(&mut cfg.broad_prefilter, key, val, &mut errors),
            "dedup_bodies" => set_bool(&mut cfg.dedup_bodies, key, val, &mut errors),
            "hot_anchor_threshold" => {
                set_u32(&mut cfg.hot_anchor_threshold, key, val, &mut errors);
            }
            "hot_migration_max_moves" => {
                set_usize(&mut cfg.hot_migration_max_moves, key, val, &mut errors);
            }
            "cooperative_cancel" => set_bool(&mut cfg.cooperative_cancel, key, val, &mut errors),
            "alias_feedback_capture" => {
                set_bool(&mut cfg.alias_feedback_capture, key, val, &mut errors);
            }
            "alias_feedback_max_pairs" => {
                set_usize(&mut cfg.alias_feedback_max_pairs, key, val, &mut errors);
            }
            "accept_class_d" => set_bool(&mut cfg.accept_class_d, key, val, &mut errors),
            "data_dir" | "wal_sync_on_write" | "retain_source" | "retention_lease_ttl_secs" => {
                errors.push(format!(
                    "setting [{key}] is not dynamically updateable; set it at startup"
                ));
            }
            "persistent" => errors.push(
                "persistent settings are unsupported; PUT /_settings updates live in-memory \
                 settings only"
                    .to_string(),
            ),
            "transient" | "settings" => errors.push(format!(
                "the [{key}] wrapper is unsupported; send native setting keys at the top level"
            )),
            _ => errors.push(format!("unknown setting [{key}]")),
        }
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    let problems = cfg.validate();
    if problems.is_empty() {
        Ok(cfg)
    } else {
        Err(problems)
    }
}

fn set_usize(slot: &mut usize, key: &str, val: &serde_json::Value, errors: &mut Vec<String>) {
    match val.as_u64().and_then(|value| usize::try_from(value).ok()) {
        Some(value) => *slot = value,
        None => errors.push(format!(
            "setting [{key}] must be a non-negative integer fitting usize"
        )),
    }
}

fn set_f64(slot: &mut f64, key: &str, val: &serde_json::Value, errors: &mut Vec<String>) {
    match val.as_f64() {
        Some(value) => *slot = value,
        None => errors.push(format!("setting [{key}] must be a number")),
    }
}

fn set_u32(slot: &mut u32, key: &str, val: &serde_json::Value, errors: &mut Vec<String>) {
    match val.as_u64().and_then(|value| u32::try_from(value).ok()) {
        Some(value) => *slot = value,
        None => errors.push(format!(
            "setting [{key}] must be a non-negative integer fitting u32"
        )),
    }
}

fn set_bool(slot: &mut bool, key: &str, val: &serde_json::Value, errors: &mut Vec<String>) {
    match val.as_bool() {
        Some(value) => *slot = value,
        None => errors.push(format!("setting [{key}] must be a boolean")),
    }
}

pub(crate) fn settings_write_rejection(
    prom: &PrometheusMetrics,
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    finish_settings_write_response(
        prom,
        settings_write_error_response(status, error_type, reason),
    )
}

pub(crate) fn settings_write_error_response(
    status: StatusCode,
    error_type: &'static str,
    reason: impl Into<String>,
) -> Response {
    ApiError::response(status, error_type, reason).into_response()
}

pub(crate) fn finish_settings_write_response(
    prom: &PrometheusMetrics,
    mut response: Response,
) -> Response {
    prom.http_requests_total
        .with_label_values(&[SETTINGS_WRITE_ENDPOINT, response.status().as_str()])
        .inc();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
