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
pub(super) struct DocumentBody {
    pub(super) title: String,
}

#[derive(Clone, Deserialize, Serialize)]
pub(super) struct BoostBody {
    pub(super) key: String,
    pub(super) value: String,
    pub(super) boost: i64,
}

#[derive(Clone, Deserialize, Serialize)]
pub(super) struct RankBody {
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
pub(super) struct SinkBody {
    #[serde(rename = "type")]
    pub(super) kind: String,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct CreateJobBody {
    pub(super) event_id: String,
    pub(super) document: Option<DocumentBody>,
    pub(super) filter: Option<serde_json::Value>,
    pub(super) result_mode: Option<reverse_rusty::ResultMode>,
    pub(super) query_scope: Option<reverse_rusty::QueryScope>,
    pub(super) rank: Option<RankBody>,
    pub(super) sink: Option<SinkBody>,
    pub(super) timeout_ms: Option<u64>,
    pub(super) allow_partial_results: Option<bool>,
}

pub(super) fn validation(reason: impl Into<String>) -> (StatusCode, Json<ApiError>) {
    ApiError::response(StatusCode::BAD_REQUEST, "validation_error", reason)
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

pub(super) fn prepare(
    jobs: &ExhaustiveJobs,
    body: CreateJobBody,
) -> Result<PreparedJob, (StatusCode, Json<ApiError>)> {
    if body.event_id.is_empty() || body.event_id.len() > 512 {
        return Err(validation("event_id must contain 1..=512 bytes"));
    }
    if body.result_mode != Some(reverse_rusty::ResultMode::All) {
        return Err(validation(
            "exhaustive jobs require explicit result_mode=\"all\"",
        ));
    }
    if body.allow_partial_results == Some(true) {
        return Err(validation(
            "allow_partial_results=true is incompatible with exhaustive exact delivery",
        ));
    }
    let sink = body
        .sink
        .ok_or_else(|| validation("sink.type=\"grpc_stream\" is required"))?;
    if !matches!(sink.kind.as_str(), "grpc_stream" | "ndjson_stream") {
        return Err(validation(
            "sink.type must be \"grpc_stream\" (\"ndjson_stream\" is accepted for the HTTP reference sink)",
        ));
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
    let requested_timeout = body.timeout_ms.map(Duration::from_millis);
    let timeout = jobs.bounded_timeout(requested_timeout).map_err(|()| {
        validation("timeout_ms must be non-zero and no larger than the server job timeout")
    })?;
    let scope = body.query_scope.unwrap_or_default();
    let rank = body.rank.map(RankBody::into_spec);
    Ok(PreparedJob {
        event_id: body.event_id,
        title: document.title,
        filter,
        scope,
        rank,
        timeout,
    })
}
