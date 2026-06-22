//! `RaftControlPlane` — the openraft-backed [`ControlPlane`] backend (clustering
//! build-path step 5b / ADR-038).
//!
//! Design: docs/design/clustering-and-scaling.md §4.3 (control plane), §10 step 5b.
//!
//! Step 5a (ADR-037) shipped the dependency-free [`ControlPlane`] seam + an in-memory
//! backend. This module drops the real consensus engine in behind that *unchanged* seam:
//! a [`RaftControlPlane`] over openraft's [`Raft`], so the coordinator (which holds a
//! `Box<dyn ControlPlane>`) is byte-identical whether it carries the in-memory backend or
//! this one. openraft is `distributed`-gated, so the lean core never compiles a consensus
//! engine.
//!
//! ## The mapping onto openraft (the shape ADR-037 froze, now realized)
//!   - [`ControlPlane::propose`]           → [`Raft::client_write`] (commit a `Normal` entry on a quorum),
//!   - [`ControlPlane::change_membership`] → [`Raft::change_membership`] (joint consensus — deliberately
//!     distinct from `propose`, exactly as the seam anticipated),
//!   - [`ControlPlane::cluster_state`]     → [`Raft::ensure_linearizable`] then read the state machine,
//!   - [`ControlError::ForwardToLeader`]   ← openraft's `ForwardToLeader` (a follower's `client_write` /
//!     `ensure_linearizable` returns it; mapped 1:1 so no call site changes),
//!   - [`ClusterState`]                    = the replicated state-machine document (the Raft snapshot payload),
//!   - [`ClusterStateChange`]              = the Raft log-entry payload (`C::D`).
//!
//! The seam is **synchronous**; openraft is async. We bridge with `handle.block_on`, exactly
//! like [`RemoteShard`](super::remote::RemoteShard) — safe because the caller runs off the
//! tokio worker threads (the control plane is touched at assembly/introspection time, never on
//! the per-title hot path).
//!
//! ## The state machine reuses the ONE apply funnel
//! [`RaftStateMachine::apply`](openraft::storage::RaftStateMachine::apply) routes a committed `Normal(ClusterStateChange)` entry through
//! [`super::control::apply`] — the SAME function [`InMemoryControlPlane`] uses — so the two
//! backends are live ≡ replay by construction (the property the differential oracle relies on).
//! A `Membership` entry derives [`ClusterState::voters`] from the Raft voter set (the faithful
//! mapping of `change_membership`); a `Blank` leader-marker entry is a no-op.
//!
//! ## Two distinct epochs (still)
//! [`ClusterState::epoch`] counts *semantic* transitions (`Normal` + `Membership` applies) — it
//! is NOT openraft's term / `LogId`, and NOT [`ClusterManifest::epoch`](crate::storage::ClusterManifest)
//! (the local checkpoint generation). Because openraft commits its own `Blank`/`Membership`
//! entries during election + bootstrap, a raft node's epoch is NOT comparable to the in-memory
//! backend's under the same logical script — the differential asserts the converged
//! voters/nodes/assignments/model, not a literal epoch match.
//!
//! ## Scope (ADR-038)
//! The log store here is **in-memory** — sufficient to prove consensus convergence; a durable
//! CRC-framed store (reusing `storage::crc32` + `durable_rename`) is the deferred follow-on. The
//! cross-process gRPC `ControlService` + a tonic [`RaftNetwork`](openraft::network::RaftNetwork) are step 5b-2 (a
//! sibling module); the [`in_process_cluster`] builder here uses a direct-dispatch
//! [`RaftNetwork`](openraft::network::RaftNetwork) over a registry
//! of in-process [`Raft`] handles, which proves the backend end-to-end with no sockets.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::io::Cursor;
use std::sync::Arc;

use openraft::error::{CheckIsLeaderError, ClientWriteError, InitializeError, RaftError};
use openraft::{BasicNode, Raft, ServerState};
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;

use super::control::{
    ClusterState, ClusterStateChange, ControlError, ControlPlane, NodeId, StateVersion,
};

mod builders;
mod log_store;
mod network;
mod state_machine;

// Public entry points live in `builders`; the opaque-envelope codec + snapshot type that the
// sibling `control_server` re-uses live in `network`. Re-export both at the module root so the
// external paths (`cluster::control_raft::{start_grpc_node, …, decode, encode, SnapshotEnvelope}`)
// are unchanged by the split.
pub use builders::{
    durable_single_node, in_process_cluster, start_grpc_node, start_grpc_node_with_security,
};
pub(crate) use network::{decode, encode, SnapshotEnvelope};
use state_machine::StateMachine;

/// The application response a committed proposal yields: the post-apply control-plane version
/// (= [`ClusterState::epoch`]). Tiny + `serde` — it is openraft's `C::R` (`AppDataResponse`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterStateResponse {
    pub version: u64,
}

openraft::declare_raft_types!(
    /// The openraft type configuration for the cluster-state control plane. The committed log
    /// entry is a [`ClusterStateChange`] (ADR-037's "future log-entry payload", now realized) and
    /// the response a [`ClusterStateResponse`]; nodes are `BasicNode`s keyed by `u64` (the gRPC
    /// endpoint string lives in `BasicNode::addr`), snapshot data is an in-memory byte cursor.
    pub TypeConfig:
        D = ClusterStateChange,
        R = ClusterStateResponse,
);

pub struct RaftControlPlane {
    raft: Raft<TypeConfig>,
    sm: StateMachine,
    handle: Handle,
}

impl RaftControlPlane {
    /// The underlying Raft handle (cheap clone) — used by the gRPC `ControlServer` (step 5b-2) to
    /// dispatch incoming Vote/AppendEntries/Snapshot RPCs to this node's consensus engine.
    pub fn raft(&self) -> Raft<TypeConfig> {
        self.raft.clone()
    }

    /// The current leader as this node sees it (`None` until one is elected).
    pub fn current_leader(&self) -> Option<u64> {
        self.raft.metrics().borrow().current_leader
    }

    /// This node's Raft server state (Leader / Follower / Candidate / Learner / Shutdown).
    pub fn server_state(&self) -> ServerState {
        self.raft.metrics().borrow().state
    }

    /// A direct, NON-linearizable read of this node's committed document — for asserting follower
    /// convergence, where the linearizable [`ControlPlane::cluster_state`] would forward to the leader.
    pub fn local_state(&self) -> ClusterState {
        self.sm.local_state()
    }

    /// Bootstrap this node as the genesis of a cluster with `members` (`id → endpoint`). Must be
    /// called on exactly one node; idempotent on an already-formed cluster (a `NotAllowed` result
    /// is the goal state and is ignored, per openraft).
    pub fn initialize(&self, members: &[(u64, String)]) -> Result<(), ControlError> {
        let map: BTreeMap<u64, BasicNode> = members
            .iter()
            .map(|(id, addr)| (*id, BasicNode::new(addr)))
            .collect();
        match self.handle.block_on(self.raft.initialize(map)) {
            Ok(()) | Err(RaftError::APIError(InitializeError::NotAllowed(_))) => Ok(()),
            Err(e) => Err(ControlError::Backend(format!("initialize: {e}"))),
        }
    }

    /// Cleanly stop this node's Raft core (await the background task's join) — releases the durable
    /// files so a restart from the same dir (ADR-041) does not race a still-running core, and the
    /// graceful path for a manager node shutting down. Best-effort: a join error (already stopped)
    /// is benign.
    pub fn shutdown(&self) {
        self.handle.block_on(self.raft.shutdown()).ok();
    }
}

impl ControlPlane for RaftControlPlane {
    fn cluster_state(&self) -> Result<Arc<ClusterState>, ControlError> {
        // Linearizable read: confirm leadership + wait for the state machine to catch up, then
        // read the committed document directly (the SM is shared with this handle).
        self.handle
            .block_on(self.raft.ensure_linearizable())
            .map_err(map_check_leader)?;
        Ok(Arc::new(self.sm.local_state()))
    }

    fn version(&self) -> Result<StateVersion, ControlError> {
        Ok(StateVersion(self.sm.local_state().epoch))
    }

    fn propose(&self, change: ClusterStateChange) -> Result<StateVersion, ControlError> {
        let resp = self
            .handle
            .block_on(self.raft.client_write(change))
            .map_err(map_client_write)?;
        Ok(StateVersion(resp.data.version))
    }

    fn change_membership(&self, voters: Vec<NodeId>) -> Result<StateVersion, ControlError> {
        let set: BTreeSet<u64> = voters.iter().map(|n| n.0).collect();
        let resp = self
            .handle
            .block_on(self.raft.change_membership(set, false))
            .map_err(map_client_write)?;
        Ok(StateVersion(resp.data.version))
    }

    fn leader(&self) -> Result<Option<NodeId>, ControlError> {
        Ok(self.current_leader().map(NodeId))
    }
}

/// Map a `client_write` / `change_membership` error onto [`ControlError`]; `ForwardToLeader` is
/// preserved 1:1 (the variant ADR-037 baked in so this backend changes no call site).
pub(crate) fn map_client_write(e: RaftError<u64, ClientWriteError<u64, BasicNode>>) -> ControlError {
    match e {
        RaftError::APIError(ClientWriteError::ForwardToLeader(f)) => {
            ControlError::ForwardToLeader {
                leader: f.leader_id.map(NodeId),
                addr: f.leader_node.map(|n| n.addr),
            }
        }
        RaftError::APIError(ClientWriteError::ChangeMembershipError(e)) => {
            ControlError::Backend(format!("change membership: {e}"))
        }
        RaftError::Fatal(f) => ControlError::Backend(format!("raft fatal: {f}")),
    }
}

/// Map an `ensure_linearizable` error onto [`ControlError`] (`ForwardToLeader` / `NoQuorum`).
pub(crate) fn map_check_leader(e: RaftError<u64, CheckIsLeaderError<u64, BasicNode>>) -> ControlError {
    match e {
        RaftError::APIError(CheckIsLeaderError::ForwardToLeader(f)) => {
            ControlError::ForwardToLeader {
                leader: f.leader_id.map(NodeId),
                addr: f.leader_node.map(|n| n.addr),
            }
        }
        RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(_)) => ControlError::NoQuorum,
        RaftError::Fatal(f) => ControlError::Backend(format!("raft fatal: {f}")),
    }
}
