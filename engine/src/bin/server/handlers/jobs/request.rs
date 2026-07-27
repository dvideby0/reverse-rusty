//! Exhaustive-job request DTOs, validation, and idempotency canonicalization.
//!
//! This module resolves every semantic default and set-shaped collection before
//! admission. The HTTP adapter receives one prepared request and cannot invent
//! a second fingerprinting interpretation.

use std::time::Duration;

use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::dto::ApiError;
use crate::jobs::ExhaustiveJobs;

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DocumentBody {
    pub(super) title: String,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BoostBody {
    pub(super) key: String,
    pub(super) value: String,
    pub(super) boost: i64,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RankBody {
    #[serde(default, deserialize_with = "deserialize_non_null")]
    pub(super) priority_field: Option<String>,
    #[serde(default)]
    pub(super) boosts: Vec<BoostBody>,
}

impl RankBody {
    fn into_spec(self) -> reverse_rusty::RankProgramSpec {
        reverse_rusty::RankProgramSpec {
            priority_field: Some(
                self.priority_field
                    .unwrap_or_else(|| "priority".to_string()),
            ),
            boosts: self
                .boosts
                .into_iter()
                .map(|boost| (boost.key, boost.value, boost.boost))
                .collect(),
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SinkBody {
    #[serde(rename = "type")]
    pub(super) kind: String,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateJobBody {
    #[serde(default, deserialize_with = "deserialize_non_null")]
    pub(super) event_id: Option<String>,
    #[serde(default, deserialize_with = "deserialize_non_null")]
    pub(super) document: Option<DocumentBody>,
    #[serde(default, deserialize_with = "deserialize_non_null")]
    pub(super) filter: Option<serde_json::Value>,
    #[serde(default, deserialize_with = "deserialize_non_null")]
    pub(super) result_mode: Option<reverse_rusty::ResultMode>,
    #[serde(default, deserialize_with = "deserialize_non_null")]
    pub(super) query_scope: Option<reverse_rusty::QueryScope>,
    #[serde(default, deserialize_with = "deserialize_non_null")]
    pub(super) rank: Option<RankBody>,
    #[serde(default, deserialize_with = "deserialize_non_null")]
    pub(super) sink: Option<SinkBody>,
    #[serde(default, deserialize_with = "deserialize_non_null")]
    pub(super) timeout_ms: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_non_null")]
    pub(super) timeout: Option<String>,
    #[serde(default, deserialize_with = "deserialize_non_null")]
    pub(super) allow_partial_results: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_non_null")]
    pub(super) allow_partial_search_results: Option<bool>,
    /// ES/OpenSearch async-search retention controls do not map to this
    /// in-memory, single-consumer stream. They are parsed only to fail loud.
    #[serde(default, deserialize_with = "deserialize_non_null")]
    wait_for_completion_timeout: Option<String>,
    #[serde(default, deserialize_with = "deserialize_non_null")]
    keep_alive: Option<String>,
    #[serde(default, deserialize_with = "deserialize_non_null")]
    keep_on_completion: Option<bool>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateJobParams {
    #[serde(default, deserialize_with = "deserialize_non_null")]
    timeout_ms: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_non_null")]
    timeout: Option<String>,
    #[serde(default, deserialize_with = "deserialize_non_null")]
    allow_partial_results: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_non_null")]
    allow_partial_search_results: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_non_null")]
    wait_for_completion_timeout: Option<String>,
    #[serde(default, deserialize_with = "deserialize_non_null")]
    keep_alive: Option<String>,
    #[serde(default, deserialize_with = "deserialize_non_null")]
    keep_on_completion: Option<bool>,
}

pub(super) fn validation(reason: impl Into<String>) -> (StatusCode, Json<ApiError>) {
    ApiError::response(StatusCode::BAD_REQUEST, "validation_error", reason)
}

/// Omission is handled by the field default; an explicit JSON/query null must
/// not silently become the same request as omission.
fn deserialize_non_null<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

pub(super) struct PreparedJob {
    pub(super) event_id: String,
    pub(super) title: String,
    pub(super) filter: Vec<(String, Vec<String>)>,
    pub(super) scope: reverse_rusty::QueryScope,
    pub(super) rank: Option<reverse_rusty::RankProgramSpec>,
    pub(super) timeout: Duration,
}

/// Hash the execution semantics after defaults and unordered collections have
/// been canonicalized. `event_id` is the lookup key itself, while the fixed
/// `result_mode`/sink/partial-result fields have only one admitted meaning and
/// therefore do not need a second representation in the digest.
pub(super) fn request_fingerprint(
    title: &str,
    filter: &[(String, Vec<String>)],
    scope: reverse_rusty::QueryScope,
    rank: Option<&reverse_rusty::RankProgramSpec>,
    timeout: Duration,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    let mut piece = |bytes: &[u8]| {
        hasher.update((bytes.len() as u64).to_le_bytes());
        hasher.update(bytes);
    };

    piece(b"reverse-rusty/exhaustive-job/v2");
    piece(title.as_bytes());
    piece(&[match scope {
        reverse_rusty::QueryScope::Standard => 0,
        reverse_rusty::QueryScope::WithBroad => 1,
    }]);
    piece(&timeout.as_secs().to_le_bytes());
    piece(&timeout.subsec_nanos().to_le_bytes());

    match rank {
        None => piece(&[0]),
        Some(rank) => {
            piece(&[1]);
            piece(rank.priority_field.as_deref().unwrap_or("").as_bytes());
            // Canonicalize the raw semantic key rather than the compiled
            // TagId. A standalone TagDict grows as writes intern new tags, so
            // the same retained POST can resolve from a synthetic id to a
            // dense id later even though its request semantics did not change.
            // Compilation is last-write-wins for repeats of one raw pair.
            let mut boosts: std::collections::BTreeMap<(&str, &str), i64> =
                std::collections::BTreeMap::new();
            for (key, value, weight) in &rank.boosts {
                boosts.insert((key.as_str(), value.as_str()), *weight);
            }
            piece(&(boosts.len() as u64).to_le_bytes());
            for ((key, value), weight) in boosts {
                piece(key.as_bytes());
                piece(value.as_bytes());
                piece(&weight.to_le_bytes());
            }
        }
    }

    // Filtering is AND across groups and OR within a group. Preserve the raw
    // key/value structure so the fingerprint is independent of TagDict growth;
    // order and repeats within these set-shaped collections are irrelevant.
    let mut canonical_filter = filter.to_vec();
    for (_, values) in &mut canonical_filter {
        values.sort();
        values.dedup();
    }
    canonical_filter.sort();
    canonical_filter.dedup();
    piece(&(canonical_filter.len() as u64).to_le_bytes());
    for (key, values) in canonical_filter {
        piece(key.as_bytes());
        piece(&(values.len() as u64).to_le_bytes());
        for value in values {
            piece(value.as_bytes());
        }
    }
    hasher.finalize().into()
}

/// A synthetic `TagId` collision between two DISTINCT raw boost pairs makes
/// the compiled map order-sensitive even though boost collection is otherwise
/// set-shaped. Reject that ambiguous request before idempotency lookup; repeats
/// of the SAME raw pair remain valid last-write-wins input.
pub(super) fn validate_resolved_boosts(
    raw: &reverse_rusty::RankProgramSpec,
    compiled: &reverse_rusty::CompiledRankProgram,
) -> Result<(), (StatusCode, Json<ApiError>)> {
    let distinct_raw: std::collections::BTreeSet<(&str, &str)> = raw
        .boosts
        .iter()
        .map(|(key, value, _)| (key.as_str(), value.as_str()))
        .collect();
    if distinct_raw.len() != compiled.boosts().count() {
        return Err(validation(
            "rank boosts contain distinct tags that resolve to the same tag id",
        ));
    }
    Ok(())
}

fn one_alias<T>(
    left: Option<T>,
    right: Option<T>,
    left_name: &str,
    right_name: &str,
) -> Result<Option<T>, String> {
    match (left, right) {
        (Some(_), Some(_)) => Err(format!(
            "`{left_name}` and `{right_name}` are aliases; specify exactly one of them"
        )),
        (left, right) => Ok(left.or(right)),
    }
}

fn one_location<T>(body: Option<T>, query: Option<T>, name: &str) -> Result<Option<T>, String> {
    match (body, query) {
        (Some(_), Some(_)) => Err(format!(
            "`{name}` must be specified in either the request body or query string, not both"
        )),
        (body, query) => Ok(body.or(query)),
    }
}

fn location_timeout(
    timeout_ms: Option<u64>,
    timeout: Option<String>,
    location: &str,
) -> Result<Option<Duration>, String> {
    match (timeout_ms, timeout) {
        (Some(_), Some(_)) => Err(format!(
            "`timeout_ms` and `timeout` are aliases; specify exactly one in the {location}"
        )),
        (Some(ms), None) => Ok(Some(Duration::from_millis(ms))),
        (None, Some(value)) => {
            super::super::search::parse_named_time_value("timeout", &value).map(Some)
        }
        (None, None) => Ok(None),
    }
}

fn reject_unsupported_async_controls(
    body: &CreateJobBody,
    params: &CreateJobParams,
) -> Result<(), (StatusCode, Json<ApiError>)> {
    if body.wait_for_completion_timeout.is_some()
        || params.wait_for_completion_timeout.is_some()
        || body.keep_alive.is_some()
        || params.keep_alive.is_some()
        || body.keep_on_completion.is_some()
        || params.keep_on_completion.is_some()
    {
        return Err(validation(
            "`wait_for_completion_timeout`, `keep_alive`, and `keep_on_completion` are not \
             supported: exhaustive jobs always return immediately and retain in-memory status \
             until bounded registry pruning; use `timeout` for the execution deadline",
        ));
    }
    Ok(())
}

pub(super) fn prepare(
    jobs: &ExhaustiveJobs,
    body: CreateJobBody,
    params: CreateJobParams,
) -> Result<PreparedJob, (StatusCode, Json<ApiError>)> {
    reject_unsupported_async_controls(&body, &params)?;
    let event_id = body
        .event_id
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    if event_id.is_empty() || event_id.len() > 512 {
        return Err(validation("event_id must contain 1..=512 bytes"));
    }
    if body.result_mode.unwrap_or(reverse_rusty::ResultMode::All) != reverse_rusty::ResultMode::All
    {
        return Err(ApiError::response(
            StatusCode::BAD_REQUEST,
            "unsupported_result_mode",
            "exhaustive jobs support result_mode=\"all\" only",
        ));
    }
    let body_partial = one_alias(
        body.allow_partial_results,
        body.allow_partial_search_results,
        "allow_partial_results",
        "allow_partial_search_results",
    )
    .map_err(validation)?;
    let query_partial = one_alias(
        params.allow_partial_results,
        params.allow_partial_search_results,
        "allow_partial_results",
        "allow_partial_search_results",
    )
    .map_err(validation)?;
    let partial = one_location(
        body_partial,
        query_partial,
        "allow_partial_results/allow_partial_search_results",
    )
    .map_err(validation)?;
    if partial == Some(true) {
        return Err(validation(
            "partial search results are incompatible with exhaustive exact delivery",
        ));
    }
    if let Some(sink) = body.sink {
        if !matches!(sink.kind.as_str(), "grpc_stream" | "ndjson_stream") {
            return Err(validation(
                "sink.type must be \"grpc_stream\" or \"ndjson_stream\"",
            ));
        }
    }
    let document = body
        .document
        .ok_or_else(|| validation("request must include one document"))?;
    let (_, _, filter) = super::super::search::resolve_percolate(
        Some(super::super::search::DocBody {
            title: document.title.clone(),
        }),
        None,
        body.filter,
        None,
    )
    .map_err(validation)?;
    let body_timeout =
        location_timeout(body.timeout_ms, body.timeout, "request body").map_err(validation)?;
    let query_timeout =
        location_timeout(params.timeout_ms, params.timeout, "query string").map_err(validation)?;
    let requested_timeout =
        one_location(body_timeout, query_timeout, "timeout").map_err(validation)?;
    let timeout = jobs.bounded_timeout(requested_timeout).map_err(|()| {
        validation("timeout must be non-zero and no larger than the server job timeout")
    })?;
    let scope = body.query_scope.unwrap_or_default();
    let rank = body.rank.map(RankBody::into_spec);
    Ok(PreparedJob {
        event_id,
        title: document.title,
        filter,
        scope,
        rank,
        timeout,
    })
}
