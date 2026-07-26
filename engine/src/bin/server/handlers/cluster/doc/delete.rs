use super::{
    error, info, instrument, shard_error_response, Arc, ClusterAppState, ClusterDeleteDocResponse,
    IntoResponse, Json, Path, Response, State, StatusCode,
};

/// DELETE /_doc/{id} — remove a stored query everywhere (idempotent fan-out).
#[instrument(skip(state), fields(query_id = id))]
pub(crate) async fn cluster_delete_doc(
    State(state): State<Arc<ClusterAppState>>,
    Path(id): Path<u64>,
) -> Response {
    let result = {
        let _w = state.write_serial.lock();
        let cluster = state.cluster.read();
        cluster.remove_query(id)
    };
    match result {
        Ok(n) if n > 0 => {
            info!(query_id = id, deleted = n, "query deleted");
            state
                .prom
                .http_requests_total
                .with_label_values(&["delete_doc", "200"])
                .inc();
            (
                StatusCode::OK,
                Json(ClusterDeleteDocResponse {
                    _id: id,
                    result: "deleted",
                    deleted_count: Some(n as u64),
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
                Json(ClusterDeleteDocResponse {
                    _id: id,
                    result: "not_found",
                    deleted_count: None,
                    error: None,
                }),
            )
                .into_response()
        }
        Err(e) => {
            error!(query_id = id, error = %e, "cluster delete failed");
            state
                .prom
                .http_requests_total
                .with_label_values(&["delete_doc", "503"])
                .inc();
            shard_error_response("delete rejected", &e)
        }
    }
}
