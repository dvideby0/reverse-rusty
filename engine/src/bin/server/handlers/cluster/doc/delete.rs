use super::{
    error, info, instrument, shard_error_response, shard_error_status, warn, ApiError, Arc,
    ClusterAppState, DeleteDocParams, DeleteDocResponse, Instant, IntoResponse, Json, Path, Query,
    QueryRejection, Response, ShardError, State, StatusCode, QUERY_INDEX,
};

/// DELETE /_doc/{id} — remove a stored query everywhere (idempotent fan-out).
#[instrument(skip(state, params), fields(query_id = id))]
pub(crate) async fn cluster_delete_doc(
    State(state): State<Arc<ClusterAppState>>,
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

    let result = {
        let _w = state.write_serial.lock();
        let cluster = state.cluster.read();
        cluster.remove_query(id)
    };
    let (status, response) = render_delete_result(id, result);
    state
        .prom
        .http_requests_total
        .with_label_values(&["delete_doc", status.as_str()])
        .inc();
    state
        .prom
        .http_request_duration
        .with_label_values(&["delete_doc"])
        .observe(start.elapsed().as_secs_f64());
    response
}

/// Keep the response status, body, and metrics classification on one seam. In
/// particular, `PartiallyApplied` is a durably-owned 200 outcome whose repair
/// path is `/_cluster/resync`, not a retryable generic error.
fn render_delete_result(id: u64, result: Result<usize, ShardError>) -> (StatusCode, Response) {
    match result {
        Ok(n) if n > 0 => {
            info!(query_id = id, deleted = n, "query deleted");
            (
                StatusCode::OK,
                (
                    StatusCode::OK,
                    Json(DeleteDocResponse {
                        _index: QUERY_INDEX,
                        _id: id,
                        result: "deleted",
                        // `n` counts physical rows across placements/replicas.
                        // The REST resource is one logical document.
                        deleted_count: Some(1),
                        error: None,
                    }),
                )
                    .into_response(),
            )
        }
        Ok(_) => (
            StatusCode::NOT_FOUND,
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
                .into_response(),
        ),
        Err(ShardError::PartiallyApplied {
            ref applied,
            ref failed,
            ..
        }) => {
            warn!(
                query_id = id,
                ?applied,
                ?failed,
                "delete partially applied; queued for repair"
            );
            (
                StatusCode::OK,
                (
                    StatusCode::OK,
                    Json(DeleteDocResponse {
                        _index: QUERY_INDEX,
                        _id: id,
                        result: "partial",
                        deleted_count: None,
                        error: Some(format!(
                            "applied on shards {applied:?}, pending on {failed:?}; durably \
                             logged — POST /_cluster/resync (or reopen) converges it"
                        )),
                    }),
                )
                    .into_response(),
            )
        }
        Err(e) => {
            error!(query_id = id, error = %e, "cluster delete failed");
            let status = shard_error_status(&e);
            (status, shard_error_response("delete rejected", &e))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn partial_delete_is_an_explicit_non_retryable_result() {
        let (status, response) = render_delete_result(
            7,
            Err(ShardError::PartiallyApplied {
                logical: 7,
                applied: vec![0],
                failed: vec![1],
                detail: "fault injected".into(),
            }),
        );
        assert_eq!(status, StatusCode::OK);
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("response JSON");
        assert_eq!(body["_index"], QUERY_INDEX);
        assert_eq!(body["_id"], 7);
        assert_eq!(body["result"], "partial");
        assert!(body.get("deleted_count").is_none());
        assert!(body["error"]
            .as_str()
            .expect("repair guidance")
            .contains("/_cluster/resync"));
    }
}
