use super::*;
use tower::ServiceExt;

fn durable_state(tag: &str) -> (Arc<ClusterAppState>, std::path::PathBuf) {
    let root = std::env::temp_dir().join(format!(
        "rr-cluster-backup-api-{tag}-{}",
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

#[tokio::test]
async fn cluster_backup_uses_shared_contract_and_reports_checkpoint_epoch() {
    let (state, root) = durable_state("success");
    let dest = root.join("backup");
    let epoch_before = state.cluster.read().epoch();
    let (status, body) = send(
        &state,
        req("POST", "/_backup", &serde_json::json!({"dest": dest})),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["took"].is_u64(), "{body}");
    assert!(body["took_ms"].is_f64(), "{body}");
    assert_eq!(body["acknowledged"], true);
    assert_eq!(body["dest"], dest.to_string_lossy().as_ref());
    assert!(body["epoch"].as_u64().expect("epoch") > epoch_before);
    reverse_rusty::storage::verify_cluster_backup(&dest).expect("cluster backup verifies");

    drop(state);
    std::fs::remove_dir_all(root).expect("cleanup");
}

#[tokio::test]
async fn cluster_backup_rejects_invalid_or_nondurable_requests_before_checkpoint() {
    let (state, root) = durable_state("strict");
    let epoch_before = state.cluster.read().epoch();
    let dest = root.join("backup");
    let (status, body) = send(
        &state,
        req(
            "POST",
            "/_backup?unknown=true",
            &serde_json::json!({"dest": dest}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "validation_error", "{body}");
    assert_eq!(state.cluster.read().epoch(), epoch_before);
    assert!(!dest.exists());

    let in_memory = test_state(&seed());
    let (status, body) = send(
        &in_memory,
        req(
            "POST",
            "/_backup",
            &serde_json::json!({"dest": root.join("in-memory")}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    drop(state);
    drop(in_memory);
    std::fs::remove_dir_all(root).expect("cleanup");
}

#[tokio::test]
async fn dropped_cluster_request_keeps_admission_until_blocking_backup_finishes() {
    let (state, root) = durable_state("dropped");
    let dest = root.join("backup");
    let queued_dest = root.join("queued-backup");
    let epoch_before = state.cluster.read().epoch();
    let held_state = Arc::clone(&state);
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        let _writer = held_state.write_serial.lock();
        locked_tx.send(()).expect("report held writer");
        release_rx.recv().expect("release held writer");
    });
    locked_rx.recv().expect("wait for held writer");

    let mut admitted = Box::pin(router(&state).oneshot(req(
        "POST",
        "/_backup",
        &serde_json::json!({"dest": dest}),
    )));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), admitted.as_mut())
            .await
            .is_err(),
        "the cluster backup worker should wait off the async runtime"
    );
    assert_eq!(state.backup_permits.available_permits(), 0);

    let mut queued = Box::pin(router(&state).oneshot(req(
        "POST",
        "/_backup",
        &serde_json::json!({"dest": queued_dest}),
    )));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), queued.as_mut())
            .await
            .is_err(),
        "the second cluster backup must wait for admission before spawning"
    );
    drop(queued);
    drop(admitted);
    release_tx.send(()).expect("release writer");
    holder.join().expect("writer holder");

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !dest.exists() || state.backup_permits.available_permits() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached cluster backup completes and releases admission");
    reverse_rusty::storage::verify_cluster_backup(&dest).expect("cluster backup verifies");
    assert!(state.cluster.read().epoch() > epoch_before);
    assert!(!queued_dest.exists());

    drop(state);
    std::fs::remove_dir_all(root).expect("cleanup");
}
