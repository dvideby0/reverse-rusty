use std::convert::Infallible;
use std::time::Duration;

use super::*;
use axum::http::header;
use tower::ServiceExt;

fn durable_state(tag: &str) -> (Arc<ClusterAppState>, std::path::PathBuf) {
    let root = std::env::temp_dir().join(format!(
        "rr-cluster-checkpoint-api-{tag}-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&root).expect("create root");
    let config = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        data_dir: Some(root.join("data")),
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(
        Normalizer::default_vocab().expect("vocab"),
        &config,
        &seed(),
    )
    .expect("durable cluster");
    (state_from_cluster(cluster), root)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_reports_whether_it_created_durable_shard_state() {
    let in_memory = test_state(&seed());
    let (status, headers, bytes) = send_raw(&in_memory, req_empty("POST", "/_checkpoint")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON");
    assert!(body["took"].is_u64(), "{body}");
    assert!(body["took_ms"].is_f64(), "{body}");
    assert_eq!(body["acknowledged"], true);
    assert_eq!(body["durable"], false);
    assert_eq!(body["epoch"], 0);
    assert_eq!(body["shards_checkpointed"], 0);
    assert!(body["message"]
        .as_str()
        .expect("nondurable explanation")
        .contains("no data directory"));

    let (durable, root) = durable_state("success");
    let epoch_before = durable.cluster.read().epoch();
    let (status, headers, bytes) = send_raw(&durable, req_empty("POST", "/_checkpoint")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON");
    assert_eq!(body["acknowledged"], true);
    assert_eq!(body["durable"], true);
    assert_eq!(body["shards_checkpointed"], 3);
    assert!(body["epoch"].as_u64().expect("epoch") > epoch_before);
    assert!(body.get("message").is_none(), "{body}");
    assert_eq!(
        durable
            .prom
            .http_requests_total
            .with_label_values(&["checkpoint", "200"])
            .get(),
        1
    );
    assert_eq!(
        durable
            .prom
            .http_request_duration
            .with_label_values(&["checkpoint"])
            .get_sample_count(),
        1
    );

    drop(in_memory);
    drop(durable);
    std::fs::remove_dir_all(root).expect("cleanup");
}

#[tokio::test]
async fn checkpoint_transport_is_strict_bounded_and_uncacheable() {
    let state = test_state(&seed());
    let (status, headers, bytes) =
        send_raw(&state, req_empty("POST", "/_checkpoint?wait=true")).await;
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");

    let request = Request::post("/_checkpoint")
        .body(Body::from("{}"))
        .expect("request");
    let (status, headers, bytes) = send_raw(&state, request).await;
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");

    let request = Request::post("/_checkpoint")
        .body(Body::from(vec![b'x'; CHECKPOINT_BODY_LIMIT + 1]))
        .expect("request");
    let (status, headers, bytes) = send_raw(&state, request).await;
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert_error(
        status,
        &bytes,
        StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
    );

    let (status, headers, bytes) = send_raw(&state, req_empty("GET", "/_checkpoint")).await;
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "POST");
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert_error(
        status,
        &bytes,
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
    );

    let pending = Body::from_stream(tokio_stream::pending::<Result<Bytes, Infallible>>());
    let request = Request::post("/_checkpoint")
        .body(pending)
        .expect("request");
    let (status, headers, bytes) =
        tokio::time::timeout(Duration::from_secs(1), send_raw(&state, request))
            .await
            .expect("checkpoint body deadline");
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert_error(
        status,
        &bytes,
        StatusCode::REQUEST_TIMEOUT,
        "request_timeout",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn checkpoint_waits_off_runtime_and_detached_work_keeps_shared_admission() {
    let (state, root) = durable_state("detached");
    let backup_dest = root.join("backup-that-must-not-run");
    let epoch_before = state.cluster.read().epoch();
    let held_state = Arc::clone(&state);
    let (locked_tx, locked_rx) = std::sync::mpsc::sync_channel(0);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
    let holder = std::thread::spawn(move || {
        let _writer = held_state.write_serial.lock();
        locked_tx.send(()).expect("announce writer lock");
        release_rx.recv().expect("release writer lock");
    });
    locked_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("writer lock held");

    let mut checkpoint = Box::pin(router(&state).oneshot(req_empty("POST", "/_checkpoint")));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), checkpoint.as_mut())
            .await
            .is_err(),
        "checkpoint should wait for the writer lock on a blocking worker"
    );
    tokio::time::timeout(
        Duration::from_millis(100),
        tokio::time::sleep(Duration::from_millis(5)),
    )
    .await
    .expect("Tokio worker remained responsive");
    assert_eq!(state.durability_permits.available_permits(), 0);

    let mut backup = Box::pin(router(&state).oneshot(req(
        "POST",
        "/_backup",
        &serde_json::json!({"dest": backup_dest}),
    )));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), backup.as_mut())
            .await
            .is_err(),
        "backup must share checkpoint durability admission"
    );

    drop(backup);
    drop(checkpoint);
    release_tx.send(()).expect("release writer lock");
    holder.join().expect("writer holder");

    tokio::time::timeout(Duration::from_secs(2), async {
        while state.durability_permits.available_permits() != 1
            || state.cluster.read().epoch() == epoch_before
            || state
                .prom
                .http_requests_total
                .with_label_values(&["checkpoint", "200"])
                .get()
                != 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached checkpoint completes and is reported");
    assert!(!backup_dest.exists(), "cancelled backup must never start");

    state.durability_permits.close();
    let (status, headers, bytes) = send_raw(&state, req_empty("POST", "/_checkpoint")).await;
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert_error(
        status,
        &bytes,
        StatusCode::SERVICE_UNAVAILABLE,
        "checkpoint_unavailable",
    );

    drop(state);
    std::fs::remove_dir_all(root).expect("cleanup");
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_fails_loud_when_a_shard_cannot_persist() {
    use std::os::unix::fs::PermissionsExt;

    let (state, root) = durable_state("persistence-failure");
    state
        .cluster
        .read()
        .add_query(99, "checkpoint failure sentinel")
        .expect("WAL-backed live add");
    let epoch_before = state.cluster.read().epoch();
    let mut original = Vec::new();
    for shard in 0..3 {
        let segments = root.join("data").join(format!("shard_{shard:03}/segments"));
        let permissions = std::fs::metadata(&segments)
            .expect("segments directory")
            .permissions();
        std::fs::set_permissions(&segments, std::fs::Permissions::from_mode(0o555))
            .expect("make segments read-only");
        original.push((segments, permissions));
    }

    let (status, headers, bytes) = send_raw(&state, req_empty("POST", "/_checkpoint")).await;
    for (segments, permissions) in original {
        std::fs::set_permissions(segments, permissions).expect("restore permissions");
    }

    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert_error(
        status,
        &bytes,
        StatusCode::SERVICE_UNAVAILABLE,
        "durability_unavailable",
    );
    assert_eq!(
        state.cluster.read().epoch(),
        epoch_before,
        "a failed manifest commit must not advance the checkpoint generation"
    );
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["checkpoint", "503"])
            .get(),
        1
    );

    drop(state);
    std::fs::remove_dir_all(root).expect("cleanup");
}

fn assert_error(status: StatusCode, bytes: &Bytes, expected: StatusCode, kind: &str) {
    assert_eq!(status, expected);
    let body: serde_json::Value = serde_json::from_slice(bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], kind, "{body}");
}
