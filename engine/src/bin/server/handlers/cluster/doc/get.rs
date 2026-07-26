use super::{
    instrument, shard_error_response, ApiError, Arc, ClusterAppState, GetDocParams, GetDocResponse,
    Instant, IntoResponse, Json, Method, Path, Query, Response, ShardError, State, StatusCode,
};

/// GET /_doc/{id} — retrieve a stored query's source, probing each shard's source
/// store (local clusters). On a remote cluster the lookup fails loud with 501
/// rather than reporting a false `found: false`.
#[instrument(skip(state, params), fields(query_id = id))]
pub(crate) async fn cluster_get_doc(
    State(state): State<Arc<ClusterAppState>>,
    Path(id): Path<u64>,
    Query(params): Query<GetDocParams>,
    method: Method,
) -> Response {
    let start = Instant::now();
    let response = if method == Method::HEAD {
        let result = {
            let cluster = state.cluster.read();
            cluster.document_exists(id)
        };
        match result {
            Ok(exists) => {
                let status = if exists {
                    StatusCode::OK
                } else {
                    StatusCode::NOT_FOUND
                };
                state
                    .prom
                    .http_requests_total
                    .with_label_values(&["get_doc", status.as_str()])
                    .inc();
                status.into_response()
            }
            Err(ShardError::Config(e)) => {
                state
                    .prom
                    .http_requests_total
                    .with_label_values(&["get_doc", "501"])
                    .inc();
                ApiError::response(
                    StatusCode::NOT_IMPLEMENTED,
                    "not_supported_in_cluster_mode",
                    format!("index liveness lookup is not available on this cluster: {e}"),
                )
                .into_response()
            }
            Err(e) => {
                let (status, _) = e.write_http_class();
                state
                    .prom
                    .http_requests_total
                    .with_label_values(&["get_doc", &status.to_string()])
                    .inc();
                shard_error_response("index liveness lookup failed", &e)
            }
        }
    } else {
        let result = {
            let cluster = state.cluster.read();
            cluster.get_document(id)
        };
        match result {
            Ok(Some(document)) => {
                state
                    .prom
                    .http_requests_total
                    .with_label_values(&["get_doc", "200"])
                    .inc();
                (
                    StatusCode::OK,
                    Json(GetDocResponse::found(id, &document, &params)),
                )
                    .into_response()
            }
            Ok(None) => {
                state
                    .prom
                    .http_requests_total
                    .with_label_values(&["get_doc", "404"])
                    .inc();
                (StatusCode::NOT_FOUND, Json(GetDocResponse::missing(id))).into_response()
            }
            Err(ShardError::Config(e)) => {
                state
                    .prom
                    .http_requests_total
                    .with_label_values(&["get_doc", "501"])
                    .inc();
                ApiError::response(
                    StatusCode::NOT_IMPLEMENTED,
                    "not_supported_in_cluster_mode",
                    format!("source lookup is not available on this cluster: {e}"),
                )
                .into_response()
            }
            Err(e) => {
                let (status, _) = e.write_http_class();
                state
                    .prom
                    .http_requests_total
                    .with_label_values(&["get_doc", &status.to_string()])
                    .inc();
                shard_error_response("source lookup failed", &e)
            }
        }
    };
    state
        .prom
        .http_request_duration
        .with_label_values(&["get_doc"])
        .observe(start.elapsed().as_secs_f64());
    response
}
