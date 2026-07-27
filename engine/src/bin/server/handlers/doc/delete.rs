use super::{
    error, info, instrument, warn, ApiError, AppState, Arc, DeleteDocParams, DeleteDocResponse,
    Instant, IntoResponse, Json, Path, Query, QueryRejection, Response, State, StatusCode,
    QUERY_INDEX,
};

/// DELETE /_doc/{id} — remove a stored query by logical ID.
#[instrument(skip(state, params), fields(query_id = id))]
pub(crate) async fn delete_doc(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u64>,
    params: Result<Query<DeleteDocParams>, QueryRejection>,
) -> Response {
    let start = Instant::now();
    let params = match params {
        Ok(Query(params)) => params,
        Err(e) => {
            warn!(query_id = id, error = %e, "invalid delete-document query parameters");
            state
                .prom
                .http_requests_total
                .with_label_values(&["delete_doc", "400"])
                .inc();
            state
                .prom
                .http_request_duration
                .with_label_values(&["delete_doc"])
                .observe(start.elapsed().as_secs_f64());
            return ApiError::response(
                StatusCode::BAD_REQUEST,
                "illegal_argument_exception",
                format!("invalid delete-document query parameters: {e}"),
            )
            .into_response();
        }
    };
    params.acknowledge_refresh_policy();
    let deleted = {
        let mut engine = state.engine.lock();
        engine.delete_by_logical_id(id)
    };
    state.publish_snapshot();
    state
        .prom
        .http_request_duration
        .with_label_values(&["delete_doc"])
        .observe(start.elapsed().as_secs_f64());
    match deleted {
        Ok(n) if n > 0 => {
            info!(query_id = id, deleted = n, "query deleted");
            state
                .prom
                .http_requests_total
                .with_label_values(&["delete_doc", "200"])
                .inc();
            (
                StatusCode::OK,
                Json(DeleteDocResponse {
                    _index: QUERY_INDEX,
                    _id: id,
                    result: "deleted",
                    // The REST resource is one logical document; `n` may count
                    // historical physical rows in a legacy segment layout.
                    deleted_count: Some(1),
                    error: None,
                }),
            )
                .into_response()
        }
        Ok(_) => {
            state
                .prom
                .http_requests_total
                .with_label_values(&["delete_doc", "404"])
                .inc();
            (
                StatusCode::NOT_FOUND,
                Json(DeleteDocResponse {
                    _index: QUERY_INDEX,
                    _id: id,
                    result: "not_found",
                    deleted_count: None,
                    error: None,
                }),
            )
                .into_response()
        }
        Err(e) => {
            // Tombstone WAL append failed: the delete was NOT applied. Reject
            // rather than acknowledge a delete we couldn't log (see ADR-013).
            error!(query_id = id, error = %e, "WAL write failed, delete rejected");
            state
                .prom
                .http_requests_total
                .with_label_values(&["delete_doc", "503"])
                .inc();
            ApiError::response(
                StatusCode::SERVICE_UNAVAILABLE,
                "durability_unavailable",
                format!("delete rejected: write-ahead log error: {e}"),
            )
            .into_response()
        }
    }
}
