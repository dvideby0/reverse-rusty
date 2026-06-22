//! Wire payloads for the client-facing `ControlService::ClientControl` RPC (ADR-083).
//!
//! A coordinator's [`RemoteControlPlane`](super::remote_control::RemoteControlPlane) — a thin
//! client, NOT a Raft member — calls `ClientControl` to read/propose against the durable quorum.
//! The request/reply are serde-encoded into the opaque `RaftEnvelope.data` (the same byte-pipe
//! pattern as the Raft RPCs), so protobuf carries no control-plane schema. The server maps each
//! request onto its local [`RaftControlPlane`](super::control_raft::RaftControlPlane), and a
//! [`ControlError::ForwardToLeader`] is preserved 1:1 so the client can redial the leader.

use serde::{Deserialize, Serialize};

use super::control::{ClusterState, ClusterStateChange, ControlError, NodeId};

/// One client-facing control-plane operation — the [`ControlPlane`](super::control::ControlPlane)
/// trait, projected onto the wire. `distributed`-gated like the rest of the gRPC transport. The
/// client borrows it across the original call + a single `ForwardToLeader` retry (no clone needed).
#[derive(Serialize, Deserialize)]
pub(crate) enum ClientControlRequest {
    /// Linearizable read of the committed cluster-state document.
    GetState,
    /// The committed version (`ClusterState::epoch`) without cloning the document.
    Version,
    /// Commit one non-membership transition.
    Propose(ClusterStateChange),
    /// Change the Raft voter set (joint consensus).
    ChangeMembership(Vec<NodeId>),
    /// The current leader as the serving node sees it.
    Leader,
}

/// The reply: the typed success payload for the request, or a [`WireControlError`] (incl. the
/// leader redirect the client follows). `Committed` carries the post-commit `StateVersion`.
#[derive(Serialize, Deserialize)]
pub(crate) enum ClientControlReply {
    State(Box<ClusterState>),
    Version(u64),
    Committed(u64),
    Leader(Option<NodeId>),
    Err(WireControlError),
}

/// Serializable mirror of [`ControlError`] (which stays serde-free in the lean core). Carried in a
/// reply so the client reconstructs the exact typed error — chiefly `ForwardToLeader`, which it
/// acts on by redialing the named leader.
#[derive(Serialize, Deserialize)]
pub(crate) enum WireControlError {
    ForwardToLeader {
        leader: Option<NodeId>,
        addr: Option<String>,
    },
    NotLeader,
    NoQuorum,
    Backend(String),
}

impl From<&ControlError> for WireControlError {
    fn from(e: &ControlError) -> Self {
        match e {
            ControlError::ForwardToLeader { leader, addr } => WireControlError::ForwardToLeader {
                leader: *leader,
                addr: addr.clone(),
            },
            ControlError::NotLeader => WireControlError::NotLeader,
            ControlError::NoQuorum => WireControlError::NoQuorum,
            ControlError::Backend(m) => WireControlError::Backend(m.clone()),
        }
    }
}

impl From<WireControlError> for ControlError {
    fn from(e: WireControlError) -> Self {
        match e {
            WireControlError::ForwardToLeader { leader, addr } => {
                ControlError::ForwardToLeader { leader, addr }
            }
            WireControlError::NotLeader => ControlError::NotLeader,
            WireControlError::NoQuorum => ControlError::NoQuorum,
            WireControlError::Backend(m) => ControlError::Backend(m),
        }
    }
}
