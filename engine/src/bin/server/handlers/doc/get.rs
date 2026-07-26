use super::{
    instrument, ApiError, AppState, Arc, GetDocParams, GetDocResponse, Instant, IntoResponse, Json,
    Method, Path, Query, Response, State, StatusCode,
};

/// GET /_doc/{id} — retrieve a stored query by logical ID.
#[instrument(skip(state, params), fields(query_id = id))]
pub(crate) async fn get_doc(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u64>,
    Query(params): Query<GetDocParams>,
    method: Method,
) -> Response {
    let start = Instant::now();
    let snap = state.snapshot.load();
    let (status, response) = if method == Method::HEAD {
        // HEAD is an existence probe over the live exact index. It must not
        // materialize `_source` or turn a damaged display-only sidecar into a
        // false "missing"/500 result.
        let status = if snap.has_live_query(id) {
            StatusCode::OK
        } else {
            StatusCode::NOT_FOUND
        };
        (status, status.into_response())
    } else {
        match snap.get_query_document(id) {
            Some(document) => (
                StatusCode::OK,
                (
                    StatusCode::OK,
                    Json(GetDocResponse::found(id, &document, &params)),
                )
                    .into_response(),
            ),
            None if snap.has_live_query(id) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                ApiError::response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "source_unavailable",
                    format!(
                        "query {id} is live but its stored source is unavailable; repair or \
                         restore sources.dat"
                    ),
                )
                .into_response(),
            ),
            None => (
                StatusCode::NOT_FOUND,
                (StatusCode::NOT_FOUND, Json(GetDocResponse::missing(id))).into_response(),
            ),
        }
    };
    state
        .prom
        .http_requests_total
        .with_label_values(&["get_doc", status.as_str()])
        .inc();
    state
        .prom
        .http_request_duration
        .with_label_values(&["get_doc"])
        .observe(start.elapsed().as_secs_f64());
    response
}
