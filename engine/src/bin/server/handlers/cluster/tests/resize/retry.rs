//! Failure-after-swap retry coverage for the resize terminal contract.

use super::*;

struct FailResizeProposals {
    inner: InMemoryControlPlane,
    remaining: AtomicUsize,
}

impl ControlPlane for FailResizeProposals {
    fn cluster_state(&self) -> Result<Arc<ClusterState>, ControlError> {
        self.inner.cluster_state()
    }

    fn version(&self) -> Result<StateVersion, ControlError> {
        self.inner.version()
    }

    fn propose(&self, change: ClusterStateChange) -> Result<StateVersion, ControlError> {
        if matches!(change, ClusterStateChange::SetShardCount { .. })
            && self
                .remaining
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        {
            return Err(ControlError::Backend(
                "secret control-plane transport detail".to_string(),
            ));
        }
        self.inner.propose(change)
    }

    fn change_membership(&self, voters: Vec<NodeId>) -> Result<StateVersion, ControlError> {
        self.inner.change_membership(voters)
    }

    fn leader(&self) -> Result<Option<NodeId>, ControlError> {
        self.inner.leader()
    }
}

#[tokio::test]
async fn retry_repairs_a_post_swap_control_failure_before_acknowledging() {
    let base = test_state(&seed());
    let initial = base.cluster.read().control_state().expect("state");
    drop(base);
    let state = state_with_control(Box::new(FailResizeProposals {
        inner: InMemoryControlPlane::new(initial),
        remaining: AtomicUsize::new(1),
    }));

    let (status, _, bytes) = send_raw(
        &state,
        resize_request("/_cluster/resize", r#"{"num_shards":4}"#),
    )
    .await;
    assert_error(
        status,
        &bytes,
        StatusCode::SERVICE_UNAVAILABLE,
        "control_plane_error",
    );
    assert!(
        !String::from_utf8_lossy(&bytes).contains("secret control-plane"),
        "backend detail must remain server-side: {bytes:?}"
    );
    assert_eq!(
        state.cluster.read().num_shards(),
        4,
        "the live swap occurred"
    );
    assert_eq!(
        state
            .cluster
            .read()
            .control_state()
            .expect("stale state")
            .num_shards,
        3,
        "the first control proposal failed"
    );

    let (status, body) = send(
        &state,
        resize_request("/_cluster/resize", r#"{"num_shards":4}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["acknowledged"], true, "{body}");
    assert_eq!(body["shards_acknowledged"], true, "{body}");
    assert_eq!(body["old_num_shards"], 4, "{body}");
    assert_eq!(body["num_shards"], 4, "{body}");
    assert_eq!(body["rebuilt"], 0, "{body}");
    let cluster = state.cluster.read();
    let control = cluster.control_state().expect("repaired state");
    assert_eq!(control.num_shards, 4);
    assert_eq!(
        control.placement_generation,
        cluster.placement_generation().0,
        "same-count retry must repair the terminal control attestation"
    );
}

#[tokio::test]
async fn different_target_retry_repairs_before_advancing_generation() {
    let base = test_state(&seed());
    let initial = base.cluster.read().control_state().expect("state");
    let initial_generation = initial.placement_generation;
    drop(base);
    let state = state_with_control(Box::new(FailResizeProposals {
        inner: InMemoryControlPlane::new(initial),
        remaining: AtomicUsize::new(2),
    }));

    let (status, _) = send(
        &state,
        resize_request("/_cluster/resize", r#"{"num_shards":4}"#),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(state.cluster.read().num_shards(), 4, "first live swap");

    let (status, _) = send(
        &state,
        resize_request("/_cluster/resize", r#"{"num_shards":5}"#),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        state.cluster.read().num_shards(),
        4,
        "a failed predecessor repair must block the next rebuild"
    );
    assert_eq!(
        state
            .cluster
            .read()
            .control_state()
            .expect("still-stale control")
            .placement_generation,
        initial_generation
    );

    let (status, body) = send(
        &state,
        resize_request("/_cluster/resize", r#"{"num_shards":5}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let cluster = state.cluster.read();
    let control = cluster.control_state().expect("terminal control");
    assert_eq!(cluster.num_shards(), 5);
    assert_eq!(control.num_shards, 5);
    assert_eq!(
        control.placement_generation,
        cluster.placement_generation().0,
        "success must attest the exact serving placement generation"
    );
}
