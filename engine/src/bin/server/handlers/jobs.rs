//! ADR-114 exhaustive job HTTP surface.
//!
//! Request semantics live in `request`; this module only adapts prepared jobs
//! to standalone/cluster execution and HTTP status/stream/cancel responses.

mod request;
#[cfg(test)]
mod tests;

use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes};
use axum::extract::{
    rejection::{JsonRejection, QueryRejection},
    Path, Query, State,
};
use axum::http::{header, Method, Response, StatusCode};
use axum::Json;
use serde::{Deserialize, Serialize};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use crate::dto::ApiError;
use crate::jobs::{DeleteOutcome, ExhaustiveJobs, JobView, StartError, StreamError};
use crate::state::{AppState, ClusterAppState};
#[cfg(test)]
use request::DocumentBody;
use request::{
    prepare, request_fingerprint, validate_resolved_boosts, validation, CreateJobBody,
    CreateJobParams,
};

/// Exhaustive creation contains one document and optional small filter/rank
/// programs. Bound it before JSON deserialization so a client cannot use the
/// server-wide bulk allowance to amplify parser memory on this control route.
pub(crate) const EXHAUSTIVE_JOB_BODY_LIMIT: usize = 1024 * 1024;

#[derive(Debug, Serialize)]
pub(crate) struct CreateJobResponse {
    id: String,
    job_id: String,
    event_id: String,
    state: crate::jobs::JobPhase,
    is_running: bool,
    is_partial: bool,
    start_time_in_millis: u64,
    snapshot_generation: u64,
    status_url: String,
    stream_url: String,
    reused: bool,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GetJobParams {
    wait_for_completion_timeout: Option<String>,
    keep_alive: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeleteJobParams {}

#[derive(Serialize)]
pub(crate) struct GetJobResponse {
    id: String,
    job_id: String,
    event_id: String,
    state: crate::jobs::JobPhase,
    is_running: bool,
    is_partial: bool,
    query_scope: reverse_rusty::QueryScope,
    snapshot_generation: u64,
    start_time_in_millis: u64,
    created_unix_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    completion_time_in_millis: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    completed_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exact_total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    chunk_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checksum: Option<reverse_rusty::DeliveryChecksum>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<GetJobError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure: Option<String>,
}

#[derive(Serialize)]
struct GetJobError {
    #[serde(rename = "type")]
    error_type: &'static str,
    reason: String,
}

#[derive(Serialize)]
pub(crate) struct DeleteJobResponse {
    acknowledged: bool,
    deleted: bool,
    id: String,
    #[serde(flatten)]
    job: JobView,
}

impl From<DeleteOutcome> for DeleteJobResponse {
    fn from(outcome: DeleteOutcome) -> Self {
        Self {
            acknowledged: true,
            deleted: outcome.deleted,
            id: outcome.job.job_id.clone(),
            job: outcome.job,
        }
    }
}

impl From<JobView> for GetJobResponse {
    fn from(job: JobView) -> Self {
        let is_running = job.state == crate::jobs::JobPhase::Running;
        let is_partial = job.state != crate::jobs::JobPhase::Completed;
        let error = match job.state {
            crate::jobs::JobPhase::Failed => Some(GetJobError {
                error_type: "exhaustive_job_failed",
                reason: job
                    .failure
                    .clone()
                    .unwrap_or_else(|| "exhaustive job failed".to_string()),
            }),
            crate::jobs::JobPhase::Cancelled => Some(GetJobError {
                error_type: "exhaustive_job_cancelled",
                reason: job
                    .failure
                    .clone()
                    .unwrap_or_else(|| "exhaustive job was cancelled".to_string()),
            }),
            crate::jobs::JobPhase::Running | crate::jobs::JobPhase::Completed => None,
        };
        Self {
            id: job.job_id.clone(),
            job_id: job.job_id,
            event_id: job.event_id,
            state: job.state,
            is_running,
            is_partial,
            query_scope: job.query_scope,
            snapshot_generation: job.snapshot_generation,
            start_time_in_millis: job.created_unix_ms,
            created_unix_ms: job.created_unix_ms,
            completion_time_in_millis: job.completed_unix_ms,
            completed_unix_ms: job.completed_unix_ms,
            exact_total: job.exact_total,
            chunk_count: job.chunk_count,
            checksum: job.checksum,
            error,
            failure: job.failure,
        }
    }
}

fn start_response(outcome: crate::jobs::StartOutcome) -> (StatusCode, Json<CreateJobResponse>) {
    let job = outcome.job;
    let base = format!("/_percolate/jobs/{}", job.job_id);
    let is_running = job.state == crate::jobs::JobPhase::Running;
    (
        StatusCode::ACCEPTED,
        Json(CreateJobResponse {
            id: job.job_id.clone(),
            job_id: job.job_id,
            event_id: job.event_id,
            state: job.state,
            is_running,
            // An exact result set exists only after the completion frame has
            // committed the terminal summary.
            is_partial: job.state != crate::jobs::JobPhase::Completed,
            start_time_in_millis: job.created_unix_ms,
            snapshot_generation: job.snapshot_generation,
            status_url: base.clone(),
            stream_url: format!("{base}/stream"),
            reused: outcome.reused,
        }),
    )
}

fn start_error(error: StartError) -> (StatusCode, Json<ApiError>) {
    match error {
        StartError::Busy => ApiError::response(
            StatusCode::SERVICE_UNAVAILABLE,
            "exhaustive_capacity",
            "all dedicated exhaustive-job permits are in use",
        ),
        StartError::Capacity => ApiError::response(
            StatusCode::TOO_MANY_REQUESTS,
            "exhaustive_registry_full",
            "the bounded job registry is full of active jobs",
        ),
        StartError::EventConflict => ApiError::response(
            StatusCode::CONFLICT,
            "event_id_conflict",
            "event_id already names a different retained exhaustive request",
        ),
        StartError::InvalidTimeout => ApiError::response(
            StatusCode::BAD_REQUEST,
            "invalid_timeout",
            "exhaustive timeout must be positive, within the configured maximum, and representable",
        ),
    }
}

fn query_rejection(error: &QueryRejection) -> (StatusCode, Json<ApiError>) {
    validation(format!("invalid exhaustive-job query parameters: {error}"))
}

fn body_rejection(error: &JsonRejection) -> (StatusCode, Json<ApiError>) {
    let rejection_status = error.status();
    let error_type = if rejection_status == StatusCode::PAYLOAD_TOO_LARGE {
        "payload_too_large"
    } else if rejection_status == StatusCode::UNSUPPORTED_MEDIA_TYPE {
        "unsupported_media_type"
    } else {
        "validation_error"
    };
    let status = if rejection_status == StatusCode::PAYLOAD_TOO_LARGE
        || rejection_status == StatusCode::UNSUPPORTED_MEDIA_TYPE
    {
        rejection_status
    } else {
        StatusCode::BAD_REQUEST
    };
    ApiError::response(
        status,
        error_type,
        format!("invalid exhaustive-job body: {error}"),
    )
}

/// Strict production boundary for exhaustive-job creation.
#[allow(clippy::unused_async)] // Axum handlers are asynchronous entry points.
pub(crate) async fn create_job_route(
    State(state): State<Arc<AppState>>,
    params: Result<Query<CreateJobParams>, QueryRejection>,
    body: Result<Json<CreateJobBody>, JsonRejection>,
) -> Result<(StatusCode, Json<CreateJobResponse>), (StatusCode, Json<ApiError>)> {
    let Query(params) = params.map_err(|error| query_rejection(&error))?;
    let Json(body) = body.map_err(|error| body_rejection(&error))?;
    create_job_inner(&state, body, params)
}

/// Direct lifecycle seam; HTTP parsing is covered by route tests.
#[cfg(test)]
#[allow(clippy::unused_async)] // Keeps the seam congruent with the Axum handler.
pub(crate) async fn create_job(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateJobBody>,
) -> Result<(StatusCode, Json<CreateJobResponse>), (StatusCode, Json<ApiError>)> {
    create_job_inner(&state, body, CreateJobParams::default())
}

fn create_job_inner(
    state: &AppState,
    body: CreateJobBody,
    params: CreateJobParams,
) -> Result<(StatusCode, Json<CreateJobResponse>), (StatusCode, Json<ApiError>)> {
    let prepared = prepare(&state.exhaustive_jobs, body, params)?;
    let snapshot = state.snapshot.load_full();
    let pred = snapshot.compile_tag_predicate(&prepared.filter);
    let program = match prepared.rank.as_ref() {
        Some(spec) => {
            let compiled = snapshot
                .compile_rank_program(spec)
                .map_err(|error| validation(format!("invalid rank program: {error}")))?;
            validate_resolved_boosts(spec, &compiled)?;
            Some(compiled)
        }
        None => None,
    };
    let fingerprint = request_fingerprint(
        &prepared.title,
        &prepared.filter,
        prepared.scope,
        prepared.rank.as_ref(),
        prepared.timeout,
    );
    let title = prepared.title;
    let scope = prepared.scope;
    let chunk_size = state.exhaustive_jobs.chunk_size();
    state
        .exhaustive_jobs
        .start(
            prepared.event_id,
            fingerprint,
            scope,
            prepared.timeout,
            move |sink, deadline| {
                snapshot
                    .try_match_title_chunks(
                        &title,
                        reverse_rusty::ExhaustiveOptions {
                            query_scope: scope,
                            chunk_size,
                        },
                        program.as_ref(),
                        &pred,
                        &mut reverse_rusty::segment::MatchScratch::new(),
                        Some(deadline),
                        sink,
                    )
                    .map(|result| result.summary)
                    .map_err(|error| error.to_string())
            },
        )
        .map(start_response)
        .map_err(start_error)
}

#[allow(clippy::unused_async)] // Axum handlers are asynchronous entry points.
pub(crate) async fn cluster_create_job_route(
    State(state): State<Arc<ClusterAppState>>,
    params: Result<Query<CreateJobParams>, QueryRejection>,
    body: Result<Json<CreateJobBody>, JsonRejection>,
) -> Result<(StatusCode, Json<CreateJobResponse>), (StatusCode, Json<ApiError>)> {
    let Query(params) = params.map_err(|error| query_rejection(&error))?;
    let Json(body) = body.map_err(|error| body_rejection(&error))?;
    cluster_create_job_inner(&state, body, params)
}

fn cluster_create_job_inner(
    state: &Arc<ClusterAppState>,
    body: CreateJobBody,
    params: CreateJobParams,
) -> Result<(StatusCode, Json<CreateJobResponse>), (StatusCode, Json<ApiError>)> {
    let prepared = prepare(&state.exhaustive_jobs, body, params)?;
    let program = {
        let cluster = state.cluster.read();
        match prepared.rank.as_ref() {
            Some(spec) => {
                let compiled = cluster
                    .compile_rank_program(spec)
                    .map_err(|error| validation(format!("invalid rank program: {error}")))?;
                validate_resolved_boosts(spec, &compiled)?;
                Some(compiled)
            }
            None => None,
        }
    };
    let fingerprint = request_fingerprint(
        &prepared.title,
        &prepared.filter,
        prepared.scope,
        prepared.rank.as_ref(),
        prepared.timeout,
    );
    let state_for_job = Arc::clone(state);
    let title = prepared.title;
    let filter = prepared.filter;
    let scope = prepared.scope;
    let chunk_size = state.exhaustive_jobs.chunk_size();
    state
        .exhaustive_jobs
        .start(
            prepared.event_id,
            fingerprint,
            scope,
            prepared.timeout,
            move |sink, deadline| {
                // Freeze coordinator writes and placement for the complete
                // shard sequence, yielding one coherent execution view.
                let _writes = lock_cluster_writes(&state_for_job.write_serial, sink, deadline)?;
                let cluster = state_for_job.cluster.read();
                cluster
                    .try_percolate_filtered_all(
                        &title,
                        &filter,
                        scope,
                        program.as_ref(),
                        chunk_size,
                        Some(deadline),
                        sink,
                    )
                    .map(|result| result.summary)
                    .map_err(|error| error.to_string())
            },
        )
        .map(start_response)
        .map_err(start_error)
}

fn lock_cluster_writes<'a>(
    lock: &'a parking_lot::Mutex<()>,
    sink: &mut dyn reverse_rusty::ChunkSink,
    deadline: Instant,
) -> Result<parking_lot::MutexGuard<'a, ()>, String> {
    const POLL: Duration = Duration::from_millis(10);
    loop {
        sink.check_cancelled().map_err(|error| error.to_string())?;
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| "job deadline exceeded while waiting for cluster writes".to_string())?;
        if let Some(guard) = lock.try_lock_for(remaining.min(POLL)) {
            return Ok(guard);
        }
    }
}

type GetJobReply = (
    [(axum::http::HeaderName, &'static str); 1],
    Json<GetJobResponse>,
);

fn status_wait(
    jobs: &ExhaustiveJobs,
    params: GetJobParams,
) -> Result<Duration, (StatusCode, Json<ApiError>)> {
    if params.keep_alive.is_some() {
        return Err(validation(
            "`keep_alive` is not supported: exhaustive-job status is retained in memory until \
             bounded count-based registry pruning, not a client-selected expiration",
        ));
    }
    let Some(raw) = params.wait_for_completion_timeout else {
        return Ok(Duration::ZERO);
    };
    let wait = super::search::parse_named_time_value("wait_for_completion_timeout", &raw)
        .map_err(validation)?;
    if !wait.is_zero() && jobs.bounded_timeout(Some(wait)).is_err() {
        return Err(validation(
            "`wait_for_completion_timeout` must not exceed the configured exhaustive-job maximum",
        ));
    }
    Ok(wait)
}

async fn status(
    jobs: &ExhaustiveJobs,
    id: &str,
    params: GetJobParams,
) -> Result<GetJobReply, (StatusCode, Json<ApiError>)> {
    let wait = status_wait(jobs, params)?;
    jobs.wait_status(id, wait)
        .await
        .map(|job| {
            (
                [(header::CACHE_CONTROL, "no-store")],
                Json(GetJobResponse::from(job)),
            )
        })
        .ok_or_else(|| {
            ApiError::response(
                StatusCode::NOT_FOUND,
                "job_not_found",
                format!("exhaustive job {id} is not retained"),
            )
        })
}

pub(crate) async fn get_job(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    params: Result<Query<GetJobParams>, QueryRejection>,
) -> Result<GetJobReply, (StatusCode, Json<ApiError>)> {
    let Query(params) = params.map_err(|error| query_rejection(&error))?;
    status(&state.exhaustive_jobs, &id, params).await
}

pub(crate) async fn cluster_get_job(
    State(state): State<Arc<ClusterAppState>>,
    Path(id): Path<String>,
    params: Result<Query<GetJobParams>, QueryRejection>,
) -> Result<GetJobReply, (StatusCode, Json<ApiError>)> {
    let Query(params) = params.map_err(|error| query_rejection(&error))?;
    status(&state.exhaustive_jobs, &id, params).await
}

fn stream(jobs: &ExhaustiveJobs, id: &str) -> Result<Response<Body>, (StatusCode, Json<ApiError>)> {
    let receiver = jobs.take_stream(id).map_err(|error| match error {
        StreamError::NotFound => ApiError::response(
            StatusCode::NOT_FOUND,
            "job_not_found",
            format!("exhaustive job {id} is not retained"),
        ),
        StreamError::AlreadyTaken => ApiError::response(
            StatusCode::CONFLICT,
            "stream_already_claimed",
            "an exhaustive job stream has exactly one consumer",
        ),
    })?;
    let body_stream = ReceiverStream::new(receiver)
        .filter_map(crate::jobs::JobFrame::into_bytes)
        .map(|bytes| Ok::<Bytes, Infallible>(Bytes::from(bytes)));
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from_stream(body_stream))
        .map_err(|error| {
            ApiError::response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "stream_error",
                error.to_string(),
            )
        })
}

pub(crate) async fn get_job_stream(
    method: Method,
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Response<Body>, (StatusCode, Json<ApiError>)> {
    reject_non_get_stream_method(&method)?;
    stream(&state.exhaustive_jobs, &id)
}

pub(crate) async fn cluster_get_job_stream(
    method: Method,
    State(state): State<Arc<ClusterAppState>>,
    Path(id): Path<String>,
) -> Result<Response<Body>, (StatusCode, Json<ApiError>)> {
    reject_non_get_stream_method(&method)?;
    stream(&state.exhaustive_jobs, &id)
}

fn reject_non_get_stream_method(method: &Method) -> Result<(), (StatusCode, Json<ApiError>)> {
    if method == Method::GET {
        Ok(())
    } else {
        Err(ApiError::response(
            StatusCode::METHOD_NOT_ALLOWED,
            "method_not_allowed",
            "an exhaustive stream is claimed only by GET; HEAD is not supported",
        ))
    }
}

type DeleteJobReply = (
    [(axum::http::HeaderName, &'static str); 1],
    Json<DeleteJobResponse>,
);

fn delete(jobs: &ExhaustiveJobs, id: &str) -> Result<DeleteJobReply, (StatusCode, Json<ApiError>)> {
    jobs.delete(id)
        .map(|outcome| {
            (
                [(header::CACHE_CONTROL, "no-store")],
                Json(DeleteJobResponse::from(outcome)),
            )
        })
        .ok_or_else(|| {
            ApiError::response(
                StatusCode::NOT_FOUND,
                "job_not_found",
                format!("exhaustive job {id} is not retained"),
            )
        })
}

pub(crate) async fn cancel_job(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    params: Result<Query<DeleteJobParams>, QueryRejection>,
) -> Result<DeleteJobReply, (StatusCode, Json<ApiError>)> {
    let Query(_) = params.map_err(|error| query_rejection(&error))?;
    delete(&state.exhaustive_jobs, &id)
}

pub(crate) async fn cluster_cancel_job(
    State(state): State<Arc<ClusterAppState>>,
    Path(id): Path<String>,
    params: Result<Query<DeleteJobParams>, QueryRejection>,
) -> Result<DeleteJobReply, (StatusCode, Json<ApiError>)> {
    let Query(_) = params.map_err(|error| query_rejection(&error))?;
    delete(&state.exhaustive_jobs, &id)
}
