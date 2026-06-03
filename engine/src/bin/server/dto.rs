//! Response DTOs shared across handler modules.
//!
//! [`ApiError`] is the structured error envelope every handler returns on failure;
//! [`HitSource`] is the `_source` projection of a stored query, emitted by both the
//! `_doc` read path and the percolate/search hits. Endpoint-specific request/response
//! shapes live with their handler in [`crate::handlers`].

use axum::{http::StatusCode, Json};
use serde::Serialize;

// -- Structured API errors
#[derive(Serialize, Debug)]
pub(crate) struct ApiError {
    error: ApiErrorBody,
    status: u16,
}

#[derive(Serialize, Debug)]
struct ApiErrorBody {
    #[serde(rename = "type")]
    error_type: String,
    reason: String,
}

impl ApiError {
    pub(crate) fn response(
        status: StatusCode,
        error_type: &str,
        reason: impl Into<String>,
    ) -> (StatusCode, Json<ApiError>) {
        let code = status.as_u16();
        (
            status,
            Json(ApiError {
                error: ApiErrorBody {
                    error_type: error_type.to_string(),
                    reason: reason.into(),
                },
                status: code,
            }),
        )
    }
}

/// The `_source` of a stored query — its original DSL text. Shared by the `_doc`
/// read response and every percolate/search hit.
#[derive(Serialize)]
pub(crate) struct HitSource {
    pub(crate) query: String,
}
