use super::{
    error, extract_ranked_ingest, info, instrument, shard_error_response, shard_error_status,
    upsert_status, warn, ApiError, Arc, ClusterAppState, Instant, IntoResponse, Json, Path,
    PutDocBody, PutDocParams, PutDocResponse, Query, QueryRejection, Response, ShardError, State,
    StatusCode, QUERY_INDEX,
};

/// PUT /_doc/{id} — cluster-atomic index/create operation (ADR-117). The default
/// upsert replaces by id under ONE coordinator log frame; `op_type=create` uses
/// the insert-only `Add` funnel and conflicts without logging when the id is live.
/// A partial multi-shard apply (remote clusters only) answers 200 `partial`: the
/// mutation IS durably logged and queued for repair — re-PUTting would double-log
/// (`POST /_cluster/resync` converges it).
#[instrument(skip(state, params, body), fields(query_id = id))]
pub(crate) async fn cluster_put_doc(
    State(state): State<Arc<ClusterAppState>>,
    Path(id): Path<u64>,
    params: Result<Query<PutDocParams>, QueryRejection>,
    Json(body): Json<PutDocBody>,
) -> Response {
    let start = Instant::now();
    let params = match params {
        Ok(Query(params)) => params,
        Err(e) => {
            warn!(query_id = id, error = %e, "invalid index-document query parameters");
            state
                .prom
                .http_requests_total
                .with_label_values(&["put_doc", "400"])
                .inc();
            state
                .prom
                .http_request_duration
                .with_label_values(&["put_doc"])
                .observe(start.elapsed().as_secs_f64());
            return ApiError::response(
                StatusCode::BAD_REQUEST,
                "illegal_argument_exception",
                format!("invalid index-document query parameters: {e}"),
            )
            .into_response();
        }
    };
    params.acknowledge_refresh_policy();
    // A malformed tag value is a caller error: 400 before any coordinator work
    // (ADR-073 — never silently drop a tag the caller asked for).
    let tags = match extract_ranked_ingest(&body.rest) {
        Ok((tags, _rank)) => tags,
        Err((error_type, msg)) => {
            warn!(query_id = id, error = %msg, "invalid tag value");
            state
                .prom
                .http_requests_total
                .with_label_values(&["put_doc", "400"])
                .inc();
            // Keep the latency histogram complete (mirrors the single-node
            // handler — every other exit records a duration).
            state
                .prom
                .http_request_duration
                .with_label_values(&["put_doc"])
                .observe(start.elapsed().as_secs_f64());
            if error_type == "invalid_tag_value" {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(PutDocResponse {
                        _index: QUERY_INDEX,
                        _id: id,
                        _version: None,
                        result: "error",
                        error: Some(msg),
                    }),
                )
                    .into_response();
            }
            return ApiError::response(StatusCode::BAD_REQUEST, error_type, msg).into_response();
        }
    };
    let result = {
        let _w = state.write_serial.lock();
        let cluster = state.cluster.read();
        if params.create_only() {
            cluster
                .create_query_with_tags(id, &body.query, body.version, &tags)
                .map(|outcome| (0, outcome))
        } else {
            cluster.upsert_query_with_tags(id, &body.query, body.version, &tags)
        }
    };
    let response = match result {
        Ok((removed, outcome)) => {
            let (status, result, error) = upsert_status(removed, &outcome);
            match status {
                StatusCode::CREATED => info!(query_id = id, "query registered"),
                StatusCode::OK => info!(query_id = id, removed, "query replaced"),
                _ => warn!(query_id = id, result, "query rejected"),
            }
            state
                .prom
                .http_requests_total
                .with_label_values(&["put_doc", status.as_str()])
                .inc();
            (
                status,
                Json(PutDocResponse {
                    _index: QUERY_INDEX,
                    _id: id,
                    _version: status.is_success().then_some(body.version),
                    result,
                    error,
                }),
            )
                .into_response()
        }
        Err(ShardError::PartiallyApplied {
            ref applied,
            ref failed,
            ..
        }) => {
            // Durably logged + queued for repair: tell the caller precisely, with a
            // 200 (NOT a retry signal — a re-PUT would double-log; resync converges).
            warn!(
                query_id = id,
                ?applied,
                ?failed,
                "upsert partially applied; queued for repair"
            );
            state
                .prom
                .http_requests_total
                .with_label_values(&["put_doc", "200"])
                .inc();
            (
                StatusCode::OK,
                Json(PutDocResponse {
                    _index: QUERY_INDEX,
                    _id: id,
                    _version: Some(body.version),
                    result: "partial",
                    error: Some(format!(
                        "applied on shards {applied:?}, pending on {failed:?}; durably \
                         logged — POST /_cluster/resync (or reopen) converges it"
                    )),
                }),
            )
                .into_response()
        }
        Err(ShardError::DuplicateLogicalId(_)) if params.create_only() => {
            warn!(
                query_id = id,
                "create-only write conflicts with a live document"
            );
            state
                .prom
                .http_requests_total
                .with_label_values(&["put_doc", "409"])
                .inc();
            ApiError::response(
                StatusCode::CONFLICT,
                "version_conflict_engine_exception",
                format!("document {id} already exists; op_type=create requires a missing id"),
            )
            .into_response()
        }
        Err(e) => {
            error!(query_id = id, error = %e, "cluster document write failed");
            let status = shard_error_status(&e);
            state
                .prom
                .http_requests_total
                .with_label_values(&["put_doc", status.as_str()])
                .inc();
            shard_error_response("document write rejected", &e)
        }
    };
    state
        .prom
        .http_request_duration
        .with_label_values(&["put_doc"])
        .observe(start.elapsed().as_secs_f64());
    response
}
