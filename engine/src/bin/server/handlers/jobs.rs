//! ADR-114 exhaustive job HTTP surface.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{header, Method, Response, StatusCode};
use axum::Json;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use crate::dto::ApiError;
use crate::jobs::{ExhaustiveJobs, JobView, StartError, StreamError};
use crate::state::{AppState, ClusterAppState};

#[derive(Clone, Deserialize, Serialize)]
struct DocumentBody {
    title: String,
}

#[derive(Clone, Deserialize, Serialize)]
struct BoostBody {
    key: String,
    value: String,
    boost: i64,
}

#[derive(Clone, Deserialize, Serialize)]
struct RankBody {
    priority_field: Option<String>,
    #[serde(default)]
    boosts: Vec<BoostBody>,
}

impl RankBody {
    fn into_spec(self) -> reverse_rusty::RankProgramSpec {
        reverse_rusty::RankProgramSpec {
            priority_field: Some(
                self.priority_field
                    .unwrap_or_else(|| "priority".to_string()),
            ),
            boosts: self
                .boosts
                .into_iter()
                .map(|boost| (boost.key, boost.value, boost.boost))
                .collect(),
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
struct SinkBody {
    #[serde(rename = "type")]
    kind: String,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct CreateJobBody {
    event_id: String,
    document: Option<DocumentBody>,
    filter: Option<serde_json::Value>,
    result_mode: Option<reverse_rusty::ResultMode>,
    query_scope: Option<reverse_rusty::QueryScope>,
    rank: Option<RankBody>,
    sink: Option<SinkBody>,
    timeout_ms: Option<u64>,
    allow_partial_results: Option<bool>,
}

#[derive(Debug, Serialize)]
pub(crate) struct CreateJobResponse {
    job_id: String,
    event_id: String,
    state: crate::jobs::JobPhase,
    snapshot_generation: u64,
    status_url: String,
    stream_url: String,
    reused: bool,
}

fn validation(reason: impl Into<String>) -> (StatusCode, Json<ApiError>) {
    ApiError::response(StatusCode::BAD_REQUEST, "validation_error", reason)
}

struct PreparedJob {
    event_id: String,
    title: String,
    filter: Vec<(String, Vec<String>)>,
    scope: reverse_rusty::QueryScope,
    rank: Option<reverse_rusty::RankProgramSpec>,
    timeout: Duration,
}

/// Hash the execution semantics after defaults and unordered collections have
/// been canonicalized. `event_id` is the lookup key itself, while the fixed
/// `result_mode`/sink/partial-result fields have only one admitted meaning and
/// therefore do not need a second representation in the digest.
fn request_fingerprint(
    title: &str,
    filter: &[(String, Vec<String>)],
    scope: reverse_rusty::QueryScope,
    rank: Option<&reverse_rusty::RankProgramSpec>,
    timeout: Duration,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    let mut piece = |bytes: &[u8]| {
        hasher.update((bytes.len() as u64).to_le_bytes());
        hasher.update(bytes);
    };

    piece(b"reverse-rusty/exhaustive-job/v2");
    piece(title.as_bytes());
    piece(&[match scope {
        reverse_rusty::QueryScope::Standard => 0,
        reverse_rusty::QueryScope::WithBroad => 1,
    }]);
    piece(&timeout.as_secs().to_le_bytes());
    piece(&timeout.subsec_nanos().to_le_bytes());

    match rank {
        None => piece(&[0]),
        Some(rank) => {
            piece(&[1]);
            piece(rank.priority_field.as_deref().unwrap_or("").as_bytes());
            // Canonicalize the raw semantic key rather than the compiled
            // TagId. A standalone TagDict grows as writes intern new tags, so
            // the same retained POST can resolve from a synthetic id to a
            // dense id later even though its request semantics did not change.
            // Compilation is last-write-wins for repeats of one raw pair.
            let mut boosts: std::collections::BTreeMap<(&str, &str), i64> =
                std::collections::BTreeMap::new();
            for (key, value, weight) in &rank.boosts {
                boosts.insert((key.as_str(), value.as_str()), *weight);
            }
            piece(&(boosts.len() as u64).to_le_bytes());
            for ((key, value), weight) in boosts {
                piece(key.as_bytes());
                piece(value.as_bytes());
                piece(&weight.to_le_bytes());
            }
        }
    }

    // Filtering is AND across groups and OR within a group. Preserve the raw
    // key/value structure so the fingerprint is independent of TagDict growth;
    // order and repeats within these set-shaped collections are irrelevant.
    let mut canonical_filter = filter.to_vec();
    for (_, values) in &mut canonical_filter {
        values.sort();
        values.dedup();
    }
    canonical_filter.sort();
    canonical_filter.dedup();
    piece(&(canonical_filter.len() as u64).to_le_bytes());
    for (key, values) in canonical_filter {
        piece(key.as_bytes());
        piece(&(values.len() as u64).to_le_bytes());
        for value in values {
            piece(value.as_bytes());
        }
    }
    hasher.finalize().into()
}

/// A synthetic `TagId` collision between two DISTINCT raw boost pairs makes
/// the compiled map order-sensitive even though boost collection is otherwise
/// set-shaped. Reject that ambiguous request before idempotency lookup; repeats
/// of the SAME raw pair remain valid last-write-wins input.
fn validate_resolved_boosts(
    raw: &reverse_rusty::RankProgramSpec,
    compiled: &reverse_rusty::CompiledRankProgram,
) -> Result<(), (StatusCode, Json<ApiError>)> {
    let distinct_raw: std::collections::BTreeSet<(&str, &str)> = raw
        .boosts
        .iter()
        .map(|(key, value, _)| (key.as_str(), value.as_str()))
        .collect();
    if distinct_raw.len() != compiled.boosts().count() {
        return Err(validation(
            "rank boosts contain distinct tags that resolve to the same tag id",
        ));
    }
    Ok(())
}

fn prepare(
    jobs: &ExhaustiveJobs,
    body: CreateJobBody,
) -> Result<PreparedJob, (StatusCode, Json<ApiError>)> {
    if body.event_id.is_empty() || body.event_id.len() > 512 {
        return Err(validation("event_id must contain 1..=512 bytes"));
    }
    if body.result_mode != Some(reverse_rusty::ResultMode::All) {
        return Err(validation(
            "exhaustive jobs require explicit result_mode=\"all\"",
        ));
    }
    if body.allow_partial_results == Some(true) {
        return Err(validation(
            "allow_partial_results=true is incompatible with exhaustive exact delivery",
        ));
    }
    let sink = body
        .sink
        .ok_or_else(|| validation("sink.type=\"grpc_stream\" is required"))?;
    if !matches!(sink.kind.as_str(), "grpc_stream" | "ndjson_stream") {
        return Err(validation(
            "sink.type must be \"grpc_stream\" (\"ndjson_stream\" is accepted for the HTTP reference sink)",
        ));
    }
    let document = body
        .document
        .ok_or_else(|| validation("request must include one document"))?;
    let (_, _, filter) = super::search::resolve_percolate(
        Some(super::search::DocBody {
            title: document.title.clone(),
        }),
        None,
        body.filter,
        None,
    )
    .map_err(validation)?;
    let requested_timeout = body.timeout_ms.map(Duration::from_millis);
    let timeout = jobs.bounded_timeout(requested_timeout).map_err(|()| {
        validation("timeout_ms must be non-zero and no larger than the server job timeout")
    })?;
    let scope = body.query_scope.unwrap_or_default();
    let rank = body.rank.map(RankBody::into_spec);
    Ok(PreparedJob {
        event_id: body.event_id,
        title: document.title,
        filter,
        scope,
        rank,
        timeout,
    })
}

fn start_response(outcome: crate::jobs::StartOutcome) -> (StatusCode, Json<CreateJobResponse>) {
    let job = outcome.job;
    let base = format!("/_percolate/jobs/{}", job.job_id);
    (
        StatusCode::ACCEPTED,
        Json(CreateJobResponse {
            job_id: job.job_id,
            event_id: job.event_id,
            state: job.state,
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

pub(crate) async fn create_job(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateJobBody>,
) -> Result<(StatusCode, Json<CreateJobResponse>), (StatusCode, Json<ApiError>)> {
    let prepared = prepare(&state.exhaustive_jobs, body)?;
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

pub(crate) async fn cluster_create_job(
    State(state): State<Arc<ClusterAppState>>,
    Json(body): Json<CreateJobBody>,
) -> Result<(StatusCode, Json<CreateJobResponse>), (StatusCode, Json<ApiError>)> {
    let prepared = prepare(&state.exhaustive_jobs, body)?;
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
    let state_for_job = Arc::clone(&state);
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

fn status(jobs: &ExhaustiveJobs, id: &str) -> Result<Json<JobView>, (StatusCode, Json<ApiError>)> {
    jobs.status(id).map(Json).ok_or_else(|| {
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
) -> Result<Json<JobView>, (StatusCode, Json<ApiError>)> {
    status(&state.exhaustive_jobs, &id)
}

pub(crate) async fn cluster_get_job(
    State(state): State<Arc<ClusterAppState>>,
    Path(id): Path<String>,
) -> Result<Json<JobView>, (StatusCode, Json<ApiError>)> {
    status(&state.exhaustive_jobs, &id)
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

fn cancel(jobs: &ExhaustiveJobs, id: &str) -> Result<Json<JobView>, (StatusCode, Json<ApiError>)> {
    jobs.cancel(id).map(Json).ok_or_else(|| {
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
) -> Result<Json<JobView>, (StatusCode, Json<ApiError>)> {
    cancel(&state.exhaustive_jobs, &id)
}

pub(crate) async fn cluster_cancel_job(
    State(state): State<Arc<ClusterAppState>>,
    Path(id): Path<String>,
) -> Result<Json<JobView>, (StatusCode, Json<ApiError>)> {
    cancel(&state.exhaustive_jobs, &id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arc_swap::ArcSwap;
    use axum::body::to_bytes;
    use axum::extract::State;
    use axum::http::Request;
    use axum::routing::get;
    use axum::Router;
    use parking_lot::Mutex;
    use reverse_rusty::segment::Engine;
    use reverse_rusty::Normalizer;
    use tower::ServiceExt;

    struct CancelWhileWaiting {
        checks: usize,
    }

    impl reverse_rusty::ChunkSink for CancelWhileWaiting {
        fn send_chunk(
            &mut self,
            _chunk: &reverse_rusty::MatchChunk,
        ) -> Result<(), reverse_rusty::ChunkSinkError> {
            panic!("lock-wait cancellation test never emits")
        }

        fn check_cancelled(&mut self) -> Result<(), reverse_rusty::ChunkSinkError> {
            self.checks += 1;
            if self.checks >= 3 {
                Err(reverse_rusty::ChunkSinkError::new("cancelled"))
            } else {
                Ok(())
            }
        }
    }

    fn state(query_count: u64, channel_depth: usize) -> Arc<AppState> {
        let mut engine = Engine::new(Normalizer::default_vocab().expect("normalizer"));
        for id in 0..query_count {
            engine
                .try_insert_live("deliveryneedle", id, 1)
                .expect("insert");
        }
        let snapshot = Arc::new(engine.snapshot());
        let prom = crate::metrics::PrometheusMetrics::new();
        let exhaustive_jobs = crate::jobs::ExhaustiveJobs::new(
            crate::jobs::ExhaustiveJobConfig {
                threads: 1,
                max_concurrent: 1,
                chunk_size: 2,
                channel_depth,
                max_timeout: Duration::from_secs(5),
                max_retained: 32,
            },
            prom.clone(),
        )
        .expect("jobs");
        Arc::new(AppState {
            engine: Mutex::new(engine),
            snapshot: ArcSwap::new(snapshot),
            pool: rayon::ThreadPoolBuilder::new()
                .num_threads(1)
                .build()
                .expect("search pool"),
            search_permits: None,
            ranked_search_permits: Arc::new(tokio::sync::Semaphore::new(1)),
            exhaustive_jobs,
            max_ranked_enrichment_bytes: crate::state::DEFAULT_MAX_RANKED_ENRICHMENT_BYTES,
            include_broad: false,
            prom,
            slow_query_threshold_ms: 0,
            auth: None,
            feedback: Mutex::new(reverse_rusty::vocab::AliasFeedback::default()),
            pit_tokens: crate::pit::PitTokens::generate(),
            pits: Mutex::new(reverse_rusty::PitRegistry::new()),
            pit_config: reverse_rusty::PitConfig::default(),
        })
    }

    fn request(event_id: &str) -> CreateJobBody {
        serde_json::from_value(serde_json::json!({
            "event_id": event_id,
            "document": {"title": "deliveryneedle"},
            "result_mode": "all",
            "query_scope": "standard",
            "sink": {"type": "grpc_stream"}
        }))
        .expect("request")
    }

    async fn wait_terminal(state: &AppState, id: &str) -> JobView {
        for _ in 0..200 {
            let view = state.exhaustive_jobs.status(id).expect("retained");
            if view.state != crate::jobs::JobPhase::Running {
                return view;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        panic!("job did not terminate");
    }

    #[tokio::test]
    async fn stream_ends_in_exact_completion_and_post_is_idempotent() {
        let state = state(5, 8);
        let body = request("event-complete");
        let (_, Json(created)) = create_job(State(Arc::clone(&state)), Json(body.clone()))
            .await
            .expect("accepted");
        let response = get_job_stream(
            Method::GET,
            State(Arc::clone(&state)),
            Path(created.job_id.clone()),
        )
        .await
        .expect("stream");
        let bytes = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("stream body");
        let frames: Vec<serde_json::Value> = std::str::from_utf8(&bytes)
            .expect("utf8")
            .lines()
            .map(|line| serde_json::from_str(line).expect("frame"))
            .collect();
        assert_eq!(
            frames.last().and_then(|f| f["type"].as_str()),
            Some("completion")
        );
        assert_eq!(frames.last().unwrap()["exact_total"], 5);
        let chunks: Vec<&serde_json::Value> = frames
            .iter()
            .filter(|frame| frame["type"] == "match_chunk")
            .collect();
        assert_eq!(
            chunks
                .iter()
                .map(|frame| frame["sequence"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        let keys: Vec<&str> = chunks
            .iter()
            .flat_map(|frame| frame["members"].as_array().unwrap())
            .map(|member| member["idempotency_key"].as_str().unwrap())
            .collect();
        assert_eq!(keys.len(), 5);
        assert!(keys.iter().all(|key| key.len() == 64));
        assert_eq!(
            wait_terminal(&state, &created.job_id).await.state,
            crate::jobs::JobPhase::Completed
        );

        let (_, Json(reused)) = create_job(State(Arc::clone(&state)), Json(body))
            .await
            .expect("idempotent replay");
        assert!(reused.reused);
        assert_eq!(reused.job_id, created.job_id);
        assert_eq!(reused.snapshot_generation, created.snapshot_generation);
    }

    #[tokio::test]
    async fn semantic_defaults_and_collection_order_share_one_idempotency_fingerprint() {
        let state = state(0, 8);
        let first: CreateJobBody = serde_json::from_value(serde_json::json!({
            "event_id": "event-canonical",
            "document": {"title": "deliveryneedle"},
            "filter": {"tier": ["silver", "gold", "gold"]},
            "result_mode": "all",
            "rank": {
                "boosts": [
                    {"key": "tier", "value": "gold", "boost": 10},
                    {"key": "channel", "value": "web", "boost": 3}
                ]
            },
            "sink": {"type": "grpc_stream"}
        }))
        .expect("first request");
        let second: CreateJobBody = serde_json::from_value(serde_json::json!({
            "event_id": "event-canonical",
            "document": {"title": "deliveryneedle"},
            "filter": {"tier": ["gold", "silver"]},
            "result_mode": "all",
            "query_scope": "standard",
            "rank": {
                "priority_field": "priority",
                "boosts": [
                    {"key": "channel", "value": "web", "boost": 3},
                    {"key": "tier", "value": "gold", "boost": 10}
                ]
            },
            "sink": {"type": "ndjson_stream"},
            "timeout_ms": 5000,
            "allow_partial_results": false
        }))
        .expect("second request");

        let (_, Json(created)) = create_job(State(Arc::clone(&state)), Json(first))
            .await
            .expect("accepted");
        let (_, Json(reused)) = create_job(State(Arc::clone(&state)), Json(second))
            .await
            .expect("semantic retry");
        assert!(reused.reused);
        assert_eq!(reused.job_id, created.job_id);
        assert_eq!(reused.snapshot_generation, created.snapshot_generation);

        let mut changed = request("event-canonical");
        changed.document = Some(DocumentBody {
            title: "different title".into(),
        });
        let error = create_job(State(Arc::clone(&state)), Json(changed))
            .await
            .expect_err("different semantics must conflict");
        assert_eq!(error.0, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn colliding_synthetic_boosts_are_rejected_as_ambiguous() {
        // These two raw tags are a pinned collision in the documented 31-bit
        // synthetic TagId space. Allowing both would make the compiled program
        // order-sensitive while the stable raw idempotency key is set-shaped.
        assert_eq!(
            reverse_rusty::tagdict::synthetic_tag_id("k", "v23943"),
            reverse_rusty::tagdict::synthetic_tag_id("k", "v83758")
        );
        let state = state(0, 8);
        let first: CreateJobBody = serde_json::from_value(serde_json::json!({
            "event_id": "event-synthetic-collision",
            "document": {"title": "deliveryneedle"},
            "result_mode": "all",
            "rank": {
                "boosts": [
                    {"key": "k", "value": "v23943", "boost": 10},
                    {"key": "k", "value": "v83758", "boost": 20}
                ]
            },
            "sink": {"type": "grpc_stream"}
        }))
        .expect("first collision request");
        let second: CreateJobBody = serde_json::from_value(serde_json::json!({
            "event_id": "event-synthetic-collision",
            "document": {"title": "deliveryneedle"},
            "result_mode": "all",
            "rank": {
                "boosts": [
                    {"key": "k", "value": "v83758", "boost": 20},
                    {"key": "k", "value": "v23943", "boost": 10}
                ]
            },
            "sink": {"type": "grpc_stream"}
        }))
        .expect("second collision request");

        let first_error = create_job(State(Arc::clone(&state)), Json(first))
            .await
            .expect_err("ambiguous collision must be rejected");
        assert_eq!(first_error.0, StatusCode::BAD_REQUEST);
        let second_error = create_job(State(Arc::clone(&state)), Json(second))
            .await
            .expect_err("reversing the ambiguous collision is still invalid");
        assert_eq!(second_error.0, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn retained_event_fingerprint_survives_tag_dict_growth() {
        let state = state(0, 8);
        let body: CreateJobBody = serde_json::from_value(serde_json::json!({
            "event_id": "event-tag-growth",
            "document": {"title": "deliveryneedle"},
            "filter": {"tenant": ["acme"]},
            "result_mode": "all",
            "rank": {
                "boosts": [
                    {"key": "tenant", "value": "acme", "boost": 10}
                ]
            },
            "sink": {"type": "grpc_stream"}
        }))
        .expect("tag-growth request");

        let (_, Json(created)) = create_job(State(Arc::clone(&state)), Json(body.clone()))
            .await
            .expect("initial synthetic-id request");
        let response = get_job_stream(
            Method::GET,
            State(Arc::clone(&state)),
            Path(created.job_id.clone()),
        )
        .await
        .expect("claim initial stream");
        let bytes = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("drain initial stream");
        assert_eq!(
            std::str::from_utf8(&bytes)
                .expect("utf8")
                .lines()
                .last()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("frame"))
                .as_ref()
                .and_then(|frame| frame["type"].as_str()),
            Some("completion")
        );
        wait_terminal(&state, &created.job_id).await;

        {
            let mut engine = state.engine.lock();
            engine
                .try_insert_live_with_tags(
                    "deliveryneedle",
                    99,
                    1,
                    &[("tenant".into(), "acme".into())],
                )
                .expect("tagged insert");
        }
        state.publish_snapshot();

        let (_, Json(reused)) = create_job(State(Arc::clone(&state)), Json(body))
            .await
            .expect("identical retained request after tag interning");
        assert!(reused.reused);
        assert_eq!(reused.job_id, created.job_id);
        assert_eq!(reused.snapshot_generation, created.snapshot_generation);
    }

    #[tokio::test]
    async fn head_never_claims_the_single_consumer_stream() {
        let state = state(5, 8);
        let (_, Json(created)) = create_job(State(Arc::clone(&state)), Json(request("event-head")))
            .await
            .expect("accepted");
        let path = format!("/_percolate/jobs/{}/stream", created.job_id);
        let app = Router::new()
            .route("/_percolate/jobs/{id}/stream", get(get_job_stream))
            .with_state(Arc::clone(&state));

        let head = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::HEAD)
                    .uri(&path)
                    .body(Body::empty())
                    .expect("HEAD request"),
            )
            .await
            .expect("HEAD response");
        assert_eq!(head.status(), StatusCode::METHOD_NOT_ALLOWED);

        let get = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(path)
                    .body(Body::empty())
                    .expect("GET request"),
            )
            .await
            .expect("GET response");
        assert_eq!(get.status(), StatusCode::OK);
        let bytes = to_bytes(get.into_body(), 64 * 1024)
            .await
            .expect("stream body");
        let completion = std::str::from_utf8(&bytes)
            .expect("utf8")
            .lines()
            .last()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("frame"))
            .expect("terminal frame");
        assert_eq!(completion["type"], "completion");
    }

    #[tokio::test]
    async fn disconnected_consumer_fails_without_a_completion() {
        let state = state(20, 1);
        let (_, Json(created)) = create_job(State(Arc::clone(&state)), Json(request("event-drop")))
            .await
            .expect("accepted");
        let mut receiver = state
            .exhaustive_jobs
            .take_stream(&created.job_id)
            .expect("claim stream");
        let first = receiver
            .recv()
            .await
            .expect("first chunk")
            .into_bytes()
            .expect("non-terminal chunk");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&first).unwrap()["type"],
            "match_chunk"
        );
        drop(receiver);

        let terminal = wait_terminal(&state, &created.job_id).await;
        assert_eq!(terminal.state, crate::jobs::JobPhase::Failed);
        assert!(terminal.exact_total.is_none());
        assert!(terminal.checksum.is_none());
    }

    #[test]
    fn cancellation_interrupts_cluster_write_barrier_wait() {
        let lock = parking_lot::Mutex::new(());
        let held = lock.lock();
        let mut sink = CancelWhileWaiting { checks: 0 };
        let started = Instant::now();
        let result = lock_cluster_writes(&lock, &mut sink, started + Duration::from_secs(1));
        assert!(result.is_err());
        assert_eq!(sink.checks, 3);
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "cancelled lock wait lasted {:?}",
            started.elapsed()
        );
        drop(held);
    }
}
