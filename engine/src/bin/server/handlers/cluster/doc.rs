//! Cluster-mode `_doc` CRUD + `_bulk` (ADR-070). `PUT /_doc/{id}` is the
//! cluster-atomic index operation: the default uses ONE `ClusterMutation::Upsert`
//! log frame to replace every prior live copy and insert the new version (ES `index`
//! semantics, the ADR-067 contract at the coordinator), while `op_type=create` uses
//! the insert-only `Add` funnel. `_bulk` maps each index action onto the upsert path,
//! one per-item status each.

use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{rejection::QueryRejection, Path, Query, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use tracing::{error, info, instrument, warn};

use reverse_rusty::cluster::{AddOutcome, ShardError};

use crate::dto::ApiError;
use crate::handlers::doc::{
    extract_bulk_id, extract_ranked_ingest, GetDocParams, GetDocResponse, PutDocBody, PutDocParams,
    PutDocResponse, CLASS_D_REJECT_MSG, QUERY_INDEX,
};
use crate::state::ClusterAppState;

use super::{shard_error_response, shard_error_status};

#[derive(Serialize)]
struct ClusterDeleteDocResponse {
    _id: u64,
    result: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    deleted_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct ClusterBulkResponse {
    took_ms: f64,
    errors: bool,
    items: Vec<ClusterBulkItem>,
}

#[derive(Serialize)]
struct ClusterBulkItem {
    index: ClusterBulkItemInner,
}

#[derive(Serialize)]
struct ClusterBulkItemInner {
    _id: u64,
    status: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Render one upsert outcome as the PUT /_doc response. Shared with the per-item
/// bulk mapping so single and bulk writes can never drift.
fn upsert_status(
    removed: usize,
    outcome: &AddOutcome,
) -> (StatusCode, &'static str, Option<String>) {
    match outcome {
        AddOutcome::Placed { .. } | AddOutcome::Replicated => {
            if removed > 0 {
                (StatusCode::OK, "updated", None)
            } else {
                (StatusCode::CREATED, "created", None)
            }
        }
        AddOutcome::RejectedClassD => (
            StatusCode::BAD_REQUEST,
            "rejected",
            Some(format!(
                "{CLASS_D_REJECT_MSG}; in cluster mode class-D queries are rejected at \
                 placement (the cluster always-candidate lane is ADR-065 criterion 8)"
            )),
        ),
        AddOutcome::RejectedParse(e) => (
            StatusCode::BAD_REQUEST,
            "error",
            Some(format!("parse error: {e}")),
        ),
    }
}

mod bulk;
mod delete;
mod get;
mod put;

pub(crate) use bulk::cluster_bulk;
pub(crate) use delete::cluster_delete_doc;
pub(crate) use get::cluster_get_doc;
pub(crate) use put::cluster_put_doc;
