use super::{
    error, extract_ranked_ingest, info, instrument, warn, ApiError, AppState, Arc, Instant,
    IntoResponse, Json, Path, PutDocBody, PutDocParams, PutDocResponse, PutEngineOutcome, Query,
    QueryRejection, Response, State, StatusCode, CLASS_D_REJECT_MSG, QUERY_INDEX,
};

/// PUT /_doc/{id} — register or replace a single query. ES `index` semantics
/// (ADR-067): an atomic upsert — the new version is inserted and every prior
/// live copy of the id is tombstoned in ONE writer critical section, ONE WAL
/// frame, and ONE snapshot publish. A fresh id answers 201 `created`; a
/// replacement answers 200 `updated` (the ES status split). A rejected new
/// version (parse error or class D) leaves the prior version live and matchable.
#[instrument(skip(state, params, body), fields(query_id = id))]
pub(crate) async fn put_doc(
    State(state): State<Arc<AppState>>,
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
    // A malformed tag value is a caller error: 400 before any engine work
    // (ADR-073 — never silently drop a tag the caller asked for).
    let (tags, rank) = match extract_ranked_ingest(&body.rest) {
        Ok(value) => value,
        Err((error_type, msg)) => {
            warn!(query_id = id, error = %msg, "invalid tag value");
            state
                .prom
                .http_requests_total
                .with_label_values(&["put_doc", "400"])
                .inc();
            // Keep the latency histogram complete: every other put_doc exit
            // records a duration (review catch — a counted-but-unobserved
            // request skews the percentiles' denominator).
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
    let response = {
        let mut engine = state.engine.lock();
        if params.create_only() && engine.snapshot().has_live_query(id) {
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
        } else {
            let write = if params.create_only() {
                engine
                    .try_insert_live_ranked(&body.query, id, body.version, &tags, rank)
                    .map(|outcome| match outcome {
                        reverse_rusty::segment::InsertOutcome::Inserted(_) => {
                            PutEngineOutcome::Created
                        }
                        reverse_rusty::segment::InsertOutcome::RejectedClassD => {
                            PutEngineOutcome::RejectedClassD
                        }
                    })
            } else {
                engine
                    .try_upsert_live_ranked(&body.query, id, body.version, &tags, rank)
                    .map(|outcome| match outcome {
                        reverse_rusty::segment::UpsertOutcome::Created(_) => {
                            PutEngineOutcome::Created
                        }
                        reverse_rusty::segment::UpsertOutcome::Updated { replaced, .. } => {
                            PutEngineOutcome::Updated { replaced }
                        }
                        reverse_rusty::segment::UpsertOutcome::RejectedClassD => {
                            PutEngineOutcome::RejectedClassD
                        }
                    })
            };
            match write {
                Ok(PutEngineOutcome::Created) => {
                    info!(query_id = id, "query registered");
                    state
                        .prom
                        .http_requests_total
                        .with_label_values(&["put_doc", "201"])
                        .inc();
                    (
                        StatusCode::CREATED,
                        Json(PutDocResponse {
                            _index: QUERY_INDEX,
                            _id: id,
                            _version: Some(body.version),
                            result: "created",
                            error: None,
                        }),
                    )
                        .into_response()
                }
                Ok(PutEngineOutcome::Updated { replaced }) => {
                    info!(query_id = id, replaced, "query replaced");
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
                            result: "updated",
                            error: None,
                        }),
                    )
                        .into_response()
                }
                Ok(PutEngineOutcome::RejectedClassD) => {
                    warn!(query_id = id, "query rejected: cost class D");
                    state
                        .prom
                        .http_requests_total
                        .with_label_values(&["put_doc", "400"])
                        .inc();
                    (
                        StatusCode::BAD_REQUEST,
                        Json(PutDocResponse {
                            _index: QUERY_INDEX,
                            _id: id,
                            _version: None,
                            result: "rejected",
                            error: Some(CLASS_D_REJECT_MSG.into()),
                        }),
                    )
                        .into_response()
                }
                Err(reverse_rusty::WriteError::Parse(e)) => {
                    warn!(query_id = id, error = %e, "query parse error");
                    state
                        .prom
                        .http_requests_total
                        .with_label_values(&["put_doc", "400"])
                        .inc();
                    (
                        StatusCode::BAD_REQUEST,
                        Json(PutDocResponse {
                            _index: QUERY_INDEX,
                            _id: id,
                            _version: None,
                            result: "error",
                            error: Some(format!("parse error: {e}")),
                        }),
                    )
                        .into_response()
                }
                Err(reverse_rusty::WriteError::Wal(e)) => {
                    // Durability failure: the mutation was NOT applied. Never
                    // acknowledge a write we couldn't log (see ADR-013). 503 tells
                    // the client to retry — the engine state is unchanged.
                    error!(query_id = id, error = %e, "WAL write failed, mutation rejected");
                    state
                        .prom
                        .http_requests_total
                        .with_label_values(&["put_doc", "503"])
                        .inc();
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(PutDocResponse {
                            _index: QUERY_INDEX,
                            _id: id,
                            _version: None,
                            result: "error",
                            error: Some(format!("write-ahead log error: {e}")),
                        }),
                    )
                        .into_response()
                }
            }
        }
    };
    state.publish_snapshot();
    state
        .prom
        .http_request_duration
        .with_label_values(&["put_doc"])
        .observe(start.elapsed().as_secs_f64());
    response
}
