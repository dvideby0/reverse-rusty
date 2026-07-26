use super::{
    error, info, instrument, AppState, Arc, DeleteDocResponse, Instant, IntoResponse, Json, Path,
    State, StatusCode,
};

/// DELETE /_doc/{id} — remove a stored query by logical ID.
#[instrument(skip(state), fields(query_id = id))]
pub(crate) async fn delete_doc(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u64>,
) -> impl IntoResponse {
    let start = Instant::now();
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
                    _id: id,
                    result: "deleted",
                    deleted_count: Some(n as u64),
                    error: None,
                }),
            )
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
                    _id: id,
                    result: "not_found",
                    deleted_count: None,
                    error: None,
                }),
            )
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
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(DeleteDocResponse {
                    _id: id,
                    result: "error",
                    deleted_count: None,
                    error: Some(format!("write-ahead log error: {e}")),
                }),
            )
        }
    }
}
