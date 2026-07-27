//! `_doc` CRUD and `_bulk` ingest handlers: register, fetch, delete, and bulk-load
//! stored queries, plus the per-query metadata-tag extraction shared by the single
//! and bulk write paths (ADR-049).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use axum::{
    body::Bytes,
    extract::{
        rejection::{BytesRejection, QueryRejection},
        Path, Query, State,
    },
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info, instrument, warn};

use reverse_rusty::segment::IngestItemStatus;

use crate::dto::ApiError;
use crate::state::AppState;

/// The class-D rejection body, shared by the single-doc and bulk paths. Names the
/// opt-in lane (ADR-068) so an operator hitting the reject knows the way out.
pub(crate) const CLASS_D_REJECT_MSG: &str = "query has no anchorable feature (cost class D); \
     negation-only queries are stored as always-candidates when the accept_class_d \
     setting is enabled";

#[derive(Deserialize)]
pub(crate) struct PutDocBody {
    pub(crate) query: String,
    #[serde(default = "default_version")]
    pub(crate) version: u32,
    /// Per-query metadata tags (ADR-049): a canonical `tags` object plus any ES-style
    /// sibling fields (everything not named `query`/`version`/`tags`). See
    /// [`parsing::extract_ingest_tags`].
    #[serde(flatten)]
    pub(crate) rest: serde_json::Map<String, serde_json::Value>,
}
fn default_version() -> u32 {
    1
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct PutDocParams {
    /// Reverse Rusty publishes every accepted mutation before replying, so all
    /// three ES/OS refresh policies share the same (stronger) immediate-visibility
    /// behavior. Keeping the typed field makes invalid values fail loudly.
    refresh: Option<RefreshPolicy>,
    #[serde(default)]
    op_type: PutDocOpType,
}

impl PutDocParams {
    pub(crate) fn create_only(&self) -> bool {
        self.op_type == PutDocOpType::Create
    }

    pub(crate) fn acknowledge_refresh_policy(&self) {
        // The value has already been validated by serde. Deliberately consume it
        // here to make the compatibility behavior explicit: no policy can weaken
        // the engine's publish-before-response guarantee.
        let _ = self.refresh;
    }
}

#[derive(Deserialize, Clone, Copy)]
pub(crate) enum RefreshPolicy {
    #[serde(rename = "false")]
    Deferred,
    #[serde(rename = "true")]
    Immediate,
    #[serde(rename = "wait_for")]
    WaitFor,
}

#[derive(Deserialize, Default, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum PutDocOpType {
    #[default]
    Index,
    Create,
}

#[derive(Serialize)]
pub(crate) struct PutDocResponse {
    pub(crate) _index: &'static str,
    pub(crate) _id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) _version: Option<u32>,
    pub(crate) result: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
}

enum PutEngineOutcome {
    Created,
    Updated { replaced: usize },
    RejectedClassD,
}

// -- GET /_doc/{id}
pub(crate) const QUERY_INDEX: &str = "queries";

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct GetDocParams {
    #[serde(rename = "_source")]
    source: Option<bool>,
    #[serde(rename = "_source_includes", alias = "_source_include", default)]
    source_includes: Option<String>,
    #[serde(rename = "_source_excludes", alias = "_source_exclude", default)]
    source_excludes: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct GetDocResponse {
    _index: &'static str,
    _id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    _version: Option<u32>,
    found: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    _source: Option<GetDocSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    _source_metadata: Option<GetDocSourceMetadata>,
}

#[derive(Serialize)]
struct GetDocSource {
    #[serde(skip_serializing_if = "Option::is_none")]
    query: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tags: Option<BTreeMap<String, serde_json::Value>>,
}

#[derive(Serialize)]
struct GetDocSourceMetadata {
    complete: bool,
    reason: &'static str,
}

impl GetDocResponse {
    pub(crate) fn found(
        id: u64,
        document: &reverse_rusty::storage::StoredSource,
        params: &GetDocParams,
    ) -> Self {
        let metadata = (!document.tags_known()).then_some(GetDocSourceMetadata {
            complete: false,
            reason: "tag metadata predates the source metadata footer; re-PUT this document to \
                     materialize it",
        });
        Self {
            _index: QUERY_INDEX,
            _id: id,
            _version: Some(document.version()),
            found: true,
            _source: project_source(document, params),
            _source_metadata: metadata,
        }
    }

    pub(crate) const fn missing(id: u64) -> Self {
        Self {
            _index: QUERY_INDEX,
            _id: id,
            _version: None,
            found: false,
            _source: None,
            _source_metadata: None,
        }
    }
}

fn project_source(
    document: &reverse_rusty::storage::StoredSource,
    params: &GetDocParams,
) -> Option<GetDocSource> {
    if params.source == Some(false) {
        return None;
    }
    let includes = source_patterns(params.source_includes.as_deref());
    let excludes = source_patterns(params.source_excludes.as_deref());
    let include_query = field_selected("query", &includes, &excludes);
    let mut grouped: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (key, value) in document.tags() {
        let field = format!("tags.{key}");
        if nested_field_selected("tags", &field, &includes, &excludes) {
            grouped.entry(key.clone()).or_default().push(value.clone());
        }
    }
    let tags = (!grouped.is_empty()).then(|| {
        grouped
            .into_iter()
            .map(|(key, mut values)| {
                values.sort_unstable();
                values.dedup();
                let value = if values.len() == 1 {
                    serde_json::Value::String(values.pop().unwrap_or_default())
                } else {
                    serde_json::Value::Array(
                        values.into_iter().map(serde_json::Value::String).collect(),
                    )
                };
                (key, value)
            })
            .collect()
    });
    Some(GetDocSource {
        query: include_query.then(|| document.query().to_owned()),
        tags,
    })
}

fn source_patterns(raw: Option<&str>) -> Vec<&str> {
    raw.into_iter()
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .collect()
}

fn field_selected(field: &str, includes: &[&str], excludes: &[&str]) -> bool {
    let included = includes.is_empty()
        || includes
            .iter()
            .any(|pattern| source_pattern_matches(pattern, field));
    included
        && !excludes
            .iter()
            .any(|pattern| source_pattern_matches(pattern, field))
}

fn nested_field_selected(parent: &str, field: &str, includes: &[&str], excludes: &[&str]) -> bool {
    let matches = |patterns: &[&str]| {
        patterns.iter().any(|pattern| {
            source_pattern_matches(pattern, parent) || source_pattern_matches(pattern, field)
        })
    };
    (includes.is_empty() || matches(includes)) && !matches(excludes)
}

/// Small ES-style source-filter glob (`*` and `?`), kept local to this
/// off-hot-path point read so no regex dependency reaches the core.
fn source_pattern_matches(pattern: &str, value: &str) -> bool {
    // ES/OS wildcard `?` consumes one Unicode scalar value, not one UTF-8 byte.
    // This is off the hot path, so small temporary char vectors keep the greedy
    // matcher simple and make tag keys such as `é` behave as one character.
    let pattern: Vec<char> = pattern.chars().collect();
    let value: Vec<char> = value.chars().collect();
    let (mut p, mut v, mut star, mut retry_v) = (0usize, 0usize, None, 0usize);
    while v < value.len() {
        if p < pattern.len() && (pattern[p] == '?' || pattern[p] == value[v]) {
            p += 1;
            v += 1;
        } else if p < pattern.len() && pattern[p] == '*' {
            star = Some(p);
            p += 1;
            retry_v = v;
        } else if let Some(star_at) = star {
            p = star_at + 1;
            retry_v += 1;
            v = retry_v;
        } else {
            return false;
        }
    }
    while p < pattern.len() && pattern[p] == '*' {
        p += 1;
    }
    p == pattern.len()
}

// -- DELETE /_doc/{id}
#[derive(Serialize)]
pub(crate) struct DeleteDocResponse {
    pub(crate) _index: &'static str,
    pub(crate) _id: u64,
    pub(crate) result: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) deleted_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeleteDocParams {
    /// As with PUT, every completed delete is published before the response.
    /// All three ES/OS policies therefore receive immediate visibility.
    refresh: Option<RefreshPolicy>,
}

impl DeleteDocParams {
    pub(crate) fn acknowledge_refresh_policy(&self) {
        let _ = self.refresh;
    }
}

// -- POST /_bulk
#[derive(Serialize)]
pub(crate) struct BulkResponse {
    pub(crate) took: u64,
    pub(crate) took_ms: f64,
    pub(crate) errors: bool,
    pub(crate) items: Vec<BulkItem>,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BulkItem {
    Index(BulkItemInner),
    Create(BulkItemInner),
}

#[derive(Serialize)]
pub(crate) struct BulkItemInner {
    #[serde(rename = "_index")]
    pub(crate) index: &'static str,
    #[serde(rename = "_id")]
    pub(crate) id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "_version")]
    pub(crate) version: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) result: Option<&'static str>,
    pub(crate) status: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<BulkItemError>,
}

#[derive(Serialize)]
pub(crate) struct BulkItemError {
    #[serde(rename = "type")]
    pub(crate) error_type: &'static str,
    pub(crate) reason: String,
}

#[cfg(test)]
mod tests;

/// Reserved top-level fields on an ingest body that are NOT metadata tags.
const RESERVED_INGEST_FIELDS: [&str; 4] = ["query", "version", "tags", "rank_fields"];

mod bulk;
mod delete;
mod get;
mod parsing;
mod put;

pub(crate) use bulk::{
    bulk_body_rejection, bulk_query_rejection, bulk_rejection, bulk_route, error_item, fail_item,
    item_inner_mut, parse_bulk_request, pending_item, succeed_item, BulkActionKind, BulkParams,
    ParsedBulkItem,
};
pub(crate) use delete::delete_doc;
pub(crate) use get::get_doc;
pub(crate) use parsing::{coerce_tag_scalar, extract_ranked_ingest, json_type_name};
pub(crate) use put::put_doc;

#[cfg(test)]
pub(crate) use parsing::extract_ingest_tags;
