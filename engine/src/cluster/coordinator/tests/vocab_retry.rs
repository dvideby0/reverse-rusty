use super::*;

use crate::cluster::control::{
    ClusterState, ClusterStateChange, ControlError, ControlPlane, InMemoryControlPlane, NodeId,
    StateVersion,
};

struct FailFirstProposal {
    inner: InMemoryControlPlane,
    fail_next: AtomicBool,
}

impl FailFirstProposal {
    fn new(state: ClusterState) -> Self {
        Self {
            inner: InMemoryControlPlane::new(state),
            fail_next: AtomicBool::new(true),
        }
    }
}

impl ControlPlane for FailFirstProposal {
    fn cluster_state(&self) -> Result<Arc<ClusterState>, ControlError> {
        self.inner.cluster_state()
    }

    fn version(&self) -> Result<StateVersion, ControlError> {
        self.inner.version()
    }

    fn propose(&self, change: ClusterStateChange) -> Result<StateVersion, ControlError> {
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err(ControlError::Backend(
                "injected first proposal failure".into(),
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

#[test]
fn identical_alias_retry_repairs_a_failed_control_transition() {
    let cfg = ClusterConfig {
        num_shards: 3,
        ..Default::default()
    };
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &[(1, "package adapter".into())])
        .expect("in-memory cluster");
    let initial = cluster.control_state().expect("initial control state");
    cluster = cluster.with_control_plane(Box::new(FailFirstProposal::new(initial)));

    let first = cluster.import_alias_synonyms("package, pkg");
    assert!(
        matches!(first, Err(ShardError::ControlPlane(_))),
        "first proposal must fail after the live rebuild: {first:?}"
    );
    assert!(cluster.percolate("pkg adapter").unwrap().contains(&1));

    let retry = cluster
        .import_alias_synonyms("package, pkg")
        .expect("identical retry must repair the model transition");
    assert!(!retry.applied, "the live registry was already installed");
    assert_eq!(retry.recompiled, 0);

    let repaired = cluster.control_state().expect("repaired control state");
    assert_eq!(
        repaired.placement_generation,
        cluster.placement_generation().0
    );
    assert_eq!(repaired.dict_fingerprint, cluster.dict.fingerprint());
}
