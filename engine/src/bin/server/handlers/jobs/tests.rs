use super::*;
use arc_swap::ArcSwap;
use axum::body::to_bytes;
use axum::extract::State;
use axum::http::Request;
use axum::routing::{any, get};
use axum::Router;
use parking_lot::Mutex;
use reverse_rusty::segment::Engine;
use reverse_rusty::Normalizer;
use tower::ServiceExt;

mod create_route;
mod delete_route;
mod status_route;
mod stream_route;

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
        flush_serial: Mutex::new(()),
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
        RawQuery(None),
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
    assert_eq!(reused.id, reused.job_id);
    assert_eq!(reused.job_id, created.job_id);
    assert_eq!(reused.snapshot_generation, created.snapshot_generation);
    assert!(!reused.is_running);
    assert!(!reused.is_partial);
    assert_eq!(reused.start_time_in_millis, created.start_time_in_millis);
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
        RawQuery(None),
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
            .try_insert_live_with_tags("deliveryneedle", 99, 1, &[("tenant".into(), "acme".into())])
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
