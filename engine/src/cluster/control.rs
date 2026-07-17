//! `ControlPlane` — the coordinator's quorum-replicated CLUSTER-STATE seam (clustering
//! build-path step 5a / ADR-037).
//!
//! Design: docs/design/clustering-and-scaling.md §4.3 (control plane), §10 step 5.
//!
//! [`ControlPlane`] is to cluster *state* what [`ClusterLog`](super::clog::ClusterLog) is
//! to the mutation *log*: a sync, fallible, `Send + Sync` seam that abstracts the
//! OPERATION, so the dependency-free [`InMemoryControlPlane`] shipped here can later be
//! swapped for an openraft-backed one *without touching the coordinator*. The crucial
//! difference from `ClusterLog`: a consensus library (openraft) owns its OWN log
//! internally, so this is NOT a log-append seam — it is a **document-mutation +
//! linearizable-read** seam. The intended mapping onto openraft (step 5b, `distributed`):
//!   - [`ControlPlane::cluster_state`]     → `ensure_linearizable` then read the state machine,
//!   - [`ControlPlane::propose`]           → `Raft::client_write` (commit on a quorum),
//!   - [`ControlPlane::change_membership`] → `Raft::change_membership` (joint consensus —
//!     deliberately NOT folded into `propose`, because changing the voter set is special
//!     in Raft and must stay a distinct operation),
//!   - [`ClusterState`]                    → the replicated state-machine document (snapshot),
//!   - [`ClusterStateChange`]              → the Raft log-entry payload.
//!
//! That mapping is why a few shape choices are load-bearing NOW even though the in-memory
//! backend doesn't need them: reads are a snapshot *pull* (openraft has no watch of *your*
//! document), membership is split from `propose`, and [`ControlError`] carries a
//! [`ForwardToLeader`](ControlError::ForwardToLeader) variant from day one (a follower's
//! `client_write` returns it) so adding the real backend later changes no call site.
//!
//! ## What it holds — and what it must NOT (the boundary invariant, ADR-037)
//! Consensus holds the SMALL, LOW-RATE cluster-state document: membership + the
//! shard→node map + ring params + the feature-model version + an epoch. It must NEVER
//! carry the high-rate query mutations (those stay on [`ClusterLog`](super::clog) + the
//! per-shard primary→replica path) nor the per-shard segment registry (that stays in the
//! LOCAL [`ClusterManifest`](crate::storage::ClusterManifest)). Routing ~750k/sec query
//! adds through one consensus group would cap throughput at commit latency and defeat the
//! content-routed design.
//!
//! ## Two distinct epochs
//! [`ClusterState::epoch`] is an APP-level counter bumped on each committed transition. It
//! is deliberately distinct from (a) openraft's term / `LogId` later and (b) the LOCAL
//! checkpoint generation in [`ClusterManifest`](crate::storage::ClusterManifest). Do not
//! conflate the three.
//!
//! ## Lean core
//! Dependency-free (std + `serde`, both already core): the seam + the in-memory backend
//! compile under `--no-default-features`, exactly like
//! [`NullClusterLog`](super::clog::NullClusterLog). The openraft backend (step 5b) is a
//! separate `distributed`-gated module; openraft never enters the lean core.

use std::sync::{Arc, Mutex, PoisonError};

use serde::{Deserialize, Serialize};

use super::shard::ShardError;

/// Logical node identity — the concept the in-process clustering core never had (placement
/// was purely `FeatureId → ring → shard INDEX`). New-typed so it can't be confused with a
/// shard index/position (both bare integers); cf. [`LogPos`](super::clog) /
/// [`FeatureId`](crate::dict::FeatureId). The address + role live in [`NodeDescriptor`].
/// `0` is the conventional id of the single logical node in an in-process cluster.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct NodeId(pub u64);

/// What a node is *eligible* to be — orthogonal to what data it holds (that is encoded by
/// its appearance as a primary/replica in [`ShardAssignment`]s). `Manager` = cluster-manager
/// (Raft-voter) eligible; the currently-voting subset is [`ClusterState::voters`]. Inert in
/// step 5a (single node); meaningful once the openraft backend lands.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum NodeRole {
    /// Holds shard data (primary/replica). The only role exercised in step 5a.
    Data,
    /// Cluster-manager-eligible (can be elected / vote).
    Manager,
}

/// One cluster member: identity + transport address + role. `addr` is the gRPC endpoint
/// string the remote transport already passes around (e.g. `"http://127.0.0.1:50051"`);
/// `None` for an in-process logical node, which has no socket.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct NodeDescriptor {
    pub id: NodeId,
    pub addr: Option<String>,
    pub role: NodeRole,
}

/// One shard POSITION's placement across logical nodes. `position` is the shard INDEX the
/// ring produces (`0..num_shards`); `primary`/`replicas` are the nodes that host it.
/// Replication factor for the position is `1 + replicas.len()`. Composed with the ring
/// (`FeatureId → position`), this gives the coordinator `FeatureId → position → node → addr`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ShardAssignment {
    pub position: u32,
    pub primary: NodeId,
    pub replicas: Vec<NodeId>,
}

/// Monotonic committed version of the cluster-state document — the value a successful
/// [`ControlPlane::propose`] returns, and openraft's commit index/term mirror later.
/// New-typed (callers can't do arithmetic); `StateVersion(0)` = "genesis, nothing
/// committed beyond the initial document".
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
pub struct StateVersion(pub u64);

/// The committed cluster-state document the control plane holds consensus over — the
/// node-level analogue of what [`ClusterManifest`](crate::storage::ClusterManifest) is for
/// *local* durability. Small and low-rate by construction (see the boundary invariant in
/// the module docs). Self-contained + `serde`-serializable: it is the future Raft snapshot
/// payload, so it must hold no engine handles / `Arc<Dict>`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ClusterState {
    /// APP-level term, bumped on every committed transition. Distinct from openraft's
    /// term/`LogId` (later) AND from [`ClusterManifest::epoch`](crate::storage::ClusterManifest)
    /// (the local checkpoint generation).
    pub epoch: u64,
    /// Cluster membership (incl. data nodes + their addresses), kept sorted by id.
    pub nodes: Vec<NodeDescriptor>,
    /// The current Raft VOTER set (manager nodes), kept sorted + deduped. Managed by
    /// [`ControlPlane::change_membership`] (→ openraft joint consensus), NOT by `propose`.
    pub voters: Vec<NodeId>,
    /// The shard→node map, one entry per position, kept sorted by position.
    pub assignments: Vec<ShardAssignment>,
    /// Ring parameters, so any node re-derives a byte-identical [`HashRing`](super::ring::HashRing).
    /// Mirrors the same two fields in [`ClusterManifest`](crate::storage::ClusterManifest).
    pub num_shards: u32,
    pub vnodes: u32,
    /// Feature-model version. `dict_fingerprint` is the frozen-dict identity (matches the
    /// manifest); `model_version` is a dense counter the deferred new-vocabulary epoch
    /// handshake will coordinate on.
    pub dict_fingerprint: u64,
    pub model_version: u64,
    /// ADR-109 logical placement identity. Bumped only by model/ring blue-green
    /// rebuild transitions, never by physical assignment or checkpoint changes.
    pub placement_generation: u64,
}

/// One atomic transition the control plane commits — the [`ClusterMutation`](super::clog)
/// analogue, and the openraft log-entry payload later. Coarse-grained + low-rate by
/// construction (membership/placement/model changes, never query writes). Applying the
/// ordered change stream reproduces the document deterministically (live ≡ replay).
///
/// Note: `AddNode`/`RemoveNode` are APP-level node *registration* (a node enters/leaves the
/// cluster document with an address + role). Changing the Raft *voter set* is the separate
/// [`ControlPlane::change_membership`] operation — see the module docs.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum ClusterStateChange {
    /// Register (or replace, by [`NodeId`]) a cluster member.
    AddNode(NodeDescriptor),
    /// Deregister a member by id (idempotent). Pruning it from `voters`/`assignments` is
    /// the caller's separate responsibility (`change_membership` / a reassignment), exactly
    /// as removing a voter is distinct from removing a node in Raft.
    RemoveNode(NodeId),
    /// Place a shard position on nodes (replaces the entry for that position).
    AssignShard(ShardAssignment),
    /// Advance the feature-model version (sets the fingerprint, bumps `model_version`).
    BumpModelVersion { dict_fingerprint: u64 },
    /// Resize the cluster to `num_shards` positions (ADR-078): set the count, add a default
    /// single-node assignment (`primary NodeId(0)`, no replicas) for each new position on grow,
    /// and prune assignments for positions ≥ `num_shards` on shrink. The ring itself is
    /// re-derived by the coordinator from the new count; this keeps the cluster-state document
    /// (and thus `collect_load` / `assignment_for`) consistent. On a multi-node cluster a
    /// follow-up `rebalance` spreads the new positions across nodes.
    SetShardCount { num_shards: u32 },
}

/// Why a control-plane operation could not commit. Typed (not stringly) so callers can act
/// on it — chiefly [`ForwardToLeader`](Self::ForwardToLeader), which a follower's
/// `client_write` returns under openraft. The in-memory single-node backend never returns
/// any of these (it is always its own leader with a trivial quorum); they exist so the
/// openraft backend drops in behind the seam without changing the error shape.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlError {
    /// This node is not the leader; retry against `leader` (at `addr`, if known).
    ForwardToLeader {
        leader: Option<NodeId>,
        addr: Option<String>,
    },
    /// This node is not the leader and the leader is presently unknown.
    NotLeader,
    /// The proposal could not be committed on a quorum (e.g. lost majority).
    NoQuorum,
    /// A backend/transport error (I/O, storage, RPC) with detail.
    Backend(String),
}

impl std::fmt::Display for ControlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ControlError::ForwardToLeader { leader, addr } => write!(
                f,
                "not the control-plane leader; forward to {leader:?} at {addr:?}"
            ),
            ControlError::NotLeader => {
                write!(
                    f,
                    "this node is not the control-plane leader (leader unknown)"
                )
            }
            ControlError::NoQuorum => {
                write!(f, "control-plane proposal could not reach a quorum")
            }
            ControlError::Backend(m) => write!(f, "control-plane backend error: {m}"),
        }
    }
}

impl std::error::Error for ControlError {}

/// Fold a control-plane error into the cluster's shared [`ShardError`] at the coordinator
/// boundary, so coordinator methods that already return `Result<_, ShardError>` can `?` it.
/// The structured detail is preserved in the message (the typed variant stays available to
/// callers that hold a [`ControlError`] directly).
impl From<ControlError> for ShardError {
    fn from(e: ControlError) -> Self {
        ShardError::ControlPlane(e.to_string())
    }
}

/// The cluster-state consensus seam — sync, fallible (`Result<_, ControlError>`),
/// `Send + Sync`, exactly like [`Shard`](super::shard::Shard) / [`ClusterLog`](super::clog).
/// `Send + Sync` is mandatory: a `Box<dyn ControlPlane>` lives in `ClusterEngine`, which is
/// asserted `Send + Sync` in `lib.rs`. Surfacing errors (never swallowing a stale/blind read
/// of the assignment map) is load-bearing — a silently-wrong map routes a title to the wrong
/// node, a shard-sized false negative.
pub trait ControlPlane: Send + Sync {
    /// A linearizable read of the committed cluster-state document. Cheap (an `Arc` clone),
    /// so a caller can hold a stable snapshot while it resolves a route. (Snapshot *pull*,
    /// not a watch: openraft offers no watch of an application document.)
    fn cluster_state(&self) -> Result<Arc<ClusterState>, ControlError>;

    /// The committed version (`ClusterState::epoch`) without cloning the whole document —
    /// lets a caller cheaply detect "did the map move?". `StateVersion(0)` before any commit.
    fn version(&self) -> Result<StateVersion, ControlError>;

    /// Propose ONE non-membership transition; returns the committed version once it is
    /// durable on a quorum (immediately, for the in-memory backend). Fail-closed: a rejected
    /// proposal returns `Err` and does NOT mutate the document — the control-plane analogue
    /// of the log-first write path.
    fn propose(&self, change: ClusterStateChange) -> Result<StateVersion, ControlError>;

    /// Change the Raft VOTER set — DISTINCT from [`propose`](Self::propose) because joint
    /// consensus is special in Raft (maps to `Raft::change_membership`, not `client_write`).
    fn change_membership(&self, voters: Vec<NodeId>) -> Result<StateVersion, ControlError>;

    /// The current leader, if known — drives forward-to-leader. The single-node in-memory
    /// backend is always its own leader.
    fn leader(&self) -> Result<Option<NodeId>, ControlError>;

    /// Test-only fault injection: make subsequent `propose`/`change_membership` calls fail,
    /// so a coordinator test can prove the fail-closed contract through a `Box<dyn
    /// ControlPlane>`. Default no-op (mirrors [`ClusterLog::break_writes_for_test`](super::clog)).
    #[cfg(test)]
    fn break_proposals_for_test(&self) {}
}

/// Apply one transition to the document in place (NOT the epoch — the caller bumps that).
/// Canonicalizing (sorted `nodes`/`assignments`) so the committed document is order-
/// independent: replaying the same change set in log order yields a byte-identical
/// `ClusterState`, which is what makes the two-backend differential meaningful.
///
/// `pub(super)` so the openraft state machine (`control_raft.rs`, ADR-038) applies a
/// committed `Normal` log entry through the SAME funnel as [`InMemoryControlPlane`] — live
/// ≡ replay across both backends, the property the differential oracle relies on.
pub(super) fn apply(state: &mut ClusterState, change: ClusterStateChange) {
    match change {
        ClusterStateChange::AddNode(node) => {
            state.nodes.retain(|n| n.id != node.id);
            state.nodes.push(node);
            state.nodes.sort_by_key(|n| n.id.0);
        }
        ClusterStateChange::RemoveNode(id) => state.nodes.retain(|n| n.id != id),
        ClusterStateChange::AssignShard(a) => {
            state.assignments.retain(|x| x.position != a.position);
            state.assignments.push(a);
            state.assignments.sort_by_key(|x| x.position);
        }
        ClusterStateChange::BumpModelVersion { dict_fingerprint } => {
            state.dict_fingerprint = dict_fingerprint;
            state.model_version += 1;
            state.placement_generation = state.placement_generation.saturating_add(1);
        }
        ClusterStateChange::SetShardCount { num_shards } => {
            state.num_shards = num_shards;
            state.placement_generation = state.placement_generation.saturating_add(1);
            // Shrink: drop assignments for positions that no longer exist.
            state.assignments.retain(|a| a.position < num_shards);
            // Grow: add a default single-node assignment for each new position. A multi-node
            // caller follows with `rebalance` to spread the new positions across nodes.
            for position in 0..num_shards {
                if !state.assignments.iter().any(|a| a.position == position) {
                    state.assignments.push(ShardAssignment {
                        position,
                        primary: NodeId(0),
                        replicas: Vec::new(),
                    });
                }
            }
            state.assignments.sort_by_key(|x| x.position);
        }
    }
}

/// The canonical single-logical-node cluster-state document: one `NodeId(0)`
/// (`Manager`-eligible, the sole voter, no socket), identity assignments
/// (`position i → primary NodeId(0)`, no replicas), and the build's ring params + dict
/// fingerprint. Shared by [`InMemoryControlPlane::single_node`] AND the openraft state
/// machine's genesis seed (`control_raft.rs`, ADR-038), so the two backends start from a
/// byte-identical document — the precondition that makes their differential meaningful.
pub(super) fn single_node_state(
    num_shards: u32,
    vnodes: u32,
    dict_fingerprint: u64,
) -> ClusterState {
    ClusterState {
        epoch: 0,
        nodes: vec![NodeDescriptor {
            id: NodeId(0),
            addr: None,
            role: NodeRole::Manager,
        }],
        voters: vec![NodeId(0)],
        assignments: (0..num_shards)
            .map(|position| ShardAssignment {
                position,
                primary: NodeId(0),
                replicas: Vec::new(),
            })
            .collect(),
        num_shards,
        vnodes,
        dict_fingerprint,
        model_version: 0,
        placement_generation: crate::ownership::PlacementGeneration::INITIAL.0,
    }
}

/// The dependency-free, single-node control plane: applies every proposal immediately to an
/// in-RAM document and is always `Ok` (a single node trivially has a quorum) — the
/// [`NullClusterLog`](super::clog::NullClusterLog) analogue. It is BOTH the behavior of an
/// in-process cluster (one logical node owns every shard, so the default path is
/// byte-identical to the pre-ADR-037 cluster) AND the fast backend the differential oracle
/// runs the openraft backend against later.
pub struct InMemoryControlPlane {
    /// `Arc` inside the `Mutex` so a read clones an `Arc` handle (O(1)) and a write swaps in
    /// a fresh document — the in-memory mirror of openraft's `ArcSwap`-over-the-state-machine.
    state: Mutex<Arc<ClusterState>>,
    /// Test-only fault flag (see [`ControlPlane::break_proposals_for_test`]). Gated so a
    /// non-test build carries no unused field.
    #[cfg(test)]
    broken: std::sync::atomic::AtomicBool,
}

impl InMemoryControlPlane {
    /// Genesis from an explicit document (the openraft bootstrap analogue).
    pub fn new(initial: ClusterState) -> Self {
        InMemoryControlPlane {
            state: Mutex::new(Arc::new(initial)),
            #[cfg(test)]
            broken: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// The DEFAULT single-logical-node control plane the coordinator uses when none is
    /// supplied: the [`single_node_state`] document wrapped in an in-memory backend. Every
    /// shard is "assigned" to the one node, so the RF=1 in-process path is byte-identical to
    /// before ADR-037.
    pub fn single_node(num_shards: u32, vnodes: u32, dict_fingerprint: u64) -> Self {
        InMemoryControlPlane::new(single_node_state(num_shards, vnodes, dict_fingerprint))
    }

    pub(crate) fn single_node_with_generation(
        num_shards: u32,
        vnodes: u32,
        dict_fingerprint: u64,
        generation: crate::ownership::PlacementGeneration,
    ) -> Self {
        let mut state = single_node_state(num_shards, vnodes, dict_fingerprint);
        state.placement_generation = generation.0;
        InMemoryControlPlane::new(state)
    }

    /// Lock the document, recovering a poisoned guard rather than panicking (a prior writer
    /// panic must not take down the cluster; the document is always whole). Matches the
    /// `PoisonError::into_inner` convention used across the cluster module.
    fn lock(&self) -> std::sync::MutexGuard<'_, Arc<ClusterState>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Commit a freshly-mutated document and return its version. Shared by `propose` and
    /// `change_membership`.
    fn commit(&self, next: ClusterState) -> StateVersion {
        let version = StateVersion(next.epoch);
        *self.lock() = Arc::new(next);
        version
    }
}

impl ControlPlane for InMemoryControlPlane {
    fn cluster_state(&self) -> Result<Arc<ClusterState>, ControlError> {
        Ok(Arc::clone(&self.lock()))
    }

    fn version(&self) -> Result<StateVersion, ControlError> {
        Ok(StateVersion(self.lock().epoch))
    }

    fn propose(&self, change: ClusterStateChange) -> Result<StateVersion, ControlError> {
        #[cfg(test)]
        if self.broken.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(ControlError::Backend(
                "proposals broken (test fault injection)".into(),
            ));
        }
        let mut next = (*self.cluster_state()?).clone();
        apply(&mut next, change);
        next.epoch += 1;
        Ok(self.commit(next))
    }

    fn change_membership(&self, mut voters: Vec<NodeId>) -> Result<StateVersion, ControlError> {
        #[cfg(test)]
        if self.broken.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(ControlError::Backend(
                "proposals broken (test fault injection)".into(),
            ));
        }
        voters.sort_unstable();
        voters.dedup();
        let mut next = (*self.cluster_state()?).clone();
        next.voters = voters;
        next.epoch += 1;
        Ok(self.commit(next))
    }

    fn leader(&self) -> Result<Option<NodeId>, ControlError> {
        Ok(self.lock().voters.first().copied())
    }

    #[cfg(test)]
    fn break_proposals_for_test(&self) {
        self.broken
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u64, role: NodeRole) -> NodeDescriptor {
        NodeDescriptor {
            id: NodeId(id),
            addr: Some(format!("http://127.0.0.1:{}", 50050 + id)),
            role,
        }
    }

    #[test]
    fn single_node_is_one_manager_owning_every_position() {
        let cp = InMemoryControlPlane::single_node(4, 128, 0xABCD);
        let st = cp.cluster_state().unwrap();
        assert_eq!(st.epoch, 0);
        assert_eq!(st.num_shards, 4);
        assert_eq!(st.nodes.len(), 1);
        assert_eq!(st.voters, vec![NodeId(0)]);
        assert_eq!(st.assignments.len(), 4);
        assert!(st
            .assignments
            .iter()
            .all(|a| a.primary == NodeId(0) && a.replicas.is_empty()));
        assert_eq!(cp.leader().unwrap(), Some(NodeId(0)));
    }

    #[test]
    fn add_node_is_idempotent_and_sorted_and_bumps_version() {
        let cp = InMemoryControlPlane::single_node(1, 64, 0);
        let v0 = cp.version().unwrap();
        cp.propose(ClusterStateChange::AddNode(node(2, NodeRole::Data)))
            .unwrap();
        let v1 = cp
            .propose(ClusterStateChange::AddNode(node(1, NodeRole::Data)))
            .unwrap();
        assert!(v1 > v0, "each commit advances the version");
        // Re-adding the same id replaces, never duplicates.
        cp.propose(ClusterStateChange::AddNode(node(1, NodeRole::Manager)))
            .unwrap();
        let st = cp.cluster_state().unwrap();
        let ids: Vec<u64> = st.nodes.iter().map(|n| n.id.0).collect();
        assert_eq!(ids, vec![0, 1, 2], "no dups, sorted by id");
        assert_eq!(
            st.nodes.iter().find(|n| n.id == NodeId(1)).unwrap().role,
            NodeRole::Manager,
            "the last add wins"
        );
    }

    #[test]
    fn remove_node_is_idempotent() {
        let cp = InMemoryControlPlane::single_node(1, 64, 0);
        cp.propose(ClusterStateChange::AddNode(node(5, NodeRole::Data)))
            .unwrap();
        cp.propose(ClusterStateChange::RemoveNode(NodeId(5)))
            .unwrap();
        cp.propose(ClusterStateChange::RemoveNode(NodeId(5)))
            .unwrap(); // no-op, no panic
        assert!(cp
            .cluster_state()
            .unwrap()
            .nodes
            .iter()
            .all(|n| n.id != NodeId(5)));
    }

    #[test]
    fn assign_shard_replaces_position_kept_sorted() {
        let cp = InMemoryControlPlane::single_node(3, 64, 0);
        cp.propose(ClusterStateChange::AssignShard(ShardAssignment {
            position: 2,
            primary: NodeId(7),
            replicas: vec![NodeId(8)],
        }))
        .unwrap();
        // Replace the same position rather than appending a second entry for it.
        cp.propose(ClusterStateChange::AssignShard(ShardAssignment {
            position: 2,
            primary: NodeId(9),
            replicas: vec![],
        }))
        .unwrap();
        let st = cp.cluster_state().unwrap();
        let positions: Vec<u32> = st.assignments.iter().map(|a| a.position).collect();
        assert_eq!(positions, vec![0, 1, 2], "one entry per position, sorted");
        let p2 = st.assignments.iter().find(|a| a.position == 2).unwrap();
        assert_eq!(p2.primary, NodeId(9));
        assert!(p2.replicas.is_empty(), "the last assignment wins");
    }

    #[test]
    fn bump_model_version_advances_fingerprint_and_counter() {
        let cp = InMemoryControlPlane::single_node(1, 64, 0x1111);
        cp.propose(ClusterStateChange::BumpModelVersion {
            dict_fingerprint: 0x2222,
        })
        .unwrap();
        let st = cp.cluster_state().unwrap();
        assert_eq!(st.dict_fingerprint, 0x2222);
        assert_eq!(st.model_version, 1);
    }

    #[test]
    fn change_membership_sorts_dedups_and_is_distinct_from_propose() {
        let cp = InMemoryControlPlane::single_node(1, 64, 0);
        cp.change_membership(vec![NodeId(3), NodeId(1), NodeId(3), NodeId(2)])
            .unwrap();
        assert_eq!(
            cp.cluster_state().unwrap().voters,
            vec![NodeId(1), NodeId(2), NodeId(3)]
        );
        // Leader is the first voter.
        assert_eq!(cp.leader().unwrap(), Some(NodeId(1)));
    }

    #[test]
    fn proposals_are_deterministic_regardless_of_order() {
        // Two backends fed the same change SET in different orders converge to the same
        // canonical document — the property the two-backend differential relies on.
        let mk = || InMemoryControlPlane::single_node(2, 64, 0);
        let (a, b) = (mk(), mk());
        let changes = [
            ClusterStateChange::AddNode(node(3, NodeRole::Data)),
            ClusterStateChange::AddNode(node(1, NodeRole::Data)),
            ClusterStateChange::AssignShard(ShardAssignment {
                position: 1,
                primary: NodeId(3),
                replicas: vec![NodeId(1)],
            }),
        ];
        for c in &changes {
            a.propose(c.clone()).unwrap();
        }
        for c in changes.iter().rev() {
            b.propose(c.clone()).unwrap();
        }
        // Same membership + assignments (epoch differs only if order changed counts — here
        // both applied 3 changes, so epochs match too).
        let (sa, sb) = (a.cluster_state().unwrap(), b.cluster_state().unwrap());
        assert_eq!(sa.nodes, sb.nodes);
        assert_eq!(sa.assignments, sb.assignments);
        assert_eq!(sa.epoch, sb.epoch);
    }

    #[test]
    fn broken_backend_fails_closed() {
        let cp = InMemoryControlPlane::single_node(1, 64, 0);
        let before = cp.cluster_state().unwrap();
        cp.break_proposals_for_test();
        assert!(matches!(
            cp.propose(ClusterStateChange::AddNode(node(1, NodeRole::Data))),
            Err(ControlError::Backend(_))
        ));
        // State is unchanged — fail-closed (no partial mutation).
        assert_eq!(*cp.cluster_state().unwrap(), *before);
    }

    #[test]
    fn control_error_folds_into_shard_error() {
        let e: ShardError = ControlError::NoQuorum.into();
        assert!(matches!(e, ShardError::ControlPlane(_)));
    }
}
