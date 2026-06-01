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
//! [`RaftStateMachine::apply`] routes a committed `Normal(ClusterStateChange)` entry through
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
//! cross-process gRPC `ControlService` + a tonic [`RaftNetwork`] are step 5b-2 (a sibling module);
//! the [`in_process_cluster`] builder here uses a direct-dispatch [`RaftNetwork`] over a registry
//! of in-process [`Raft`] handles, which proves the backend end-to-end with no sockets.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::future::Future;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use openraft::error::{
    CheckIsLeaderError, ClientWriteError, Fatal, InitializeError, NetworkError, RPCError,
    RaftError, RemoteError, ReplicationClosed, StreamingError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine};
use openraft::{
    BasicNode, Config, Entry, EntryPayload, LogId, LogState, OptionalSend, Raft, RaftLogReader,
    RaftSnapshotBuilder, ServerState, Snapshot, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use tonic::transport::Channel;

use super::control::{
    single_node_state, ClusterState, ClusterStateChange, ControlError, ControlPlane, NodeId,
    StateVersion,
};
use super::proto;
use super::proto::control_service_client::ControlServiceClient;

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

// ===========================================================================
// Log storage — in-memory (ADR-038 step 5b-1). Entries are stored as-is (no
// serialization), so there are no holes and reads are clones.
// ===========================================================================

#[derive(Default)]
struct LogStoreInner {
    /// Log entries keyed by index — consecutive, no holes (the Raft correctness requirement).
    log: BTreeMap<u64, Entry<TypeConfig>>,
    last_purged: Option<LogId<u64>>,
    vote: Option<Vote<u64>>,
    committed: Option<LogId<u64>>,
}

/// In-memory [`RaftLogStorage`]. `Arc`-shared so `get_log_reader` hands out a cheap clone.
#[derive(Clone, Default)]
struct LogStore {
    inner: Arc<Mutex<LogStoreInner>>,
}

impl LogStore {
    fn lock(&self) -> MutexGuard<'_, LogStoreInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        Ok(self
            .lock()
            .log
            .range(range)
            .map(|(_, e)| e.clone())
            .collect())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let inner = self.lock();
        let last = inner.log.values().next_back().map(|e| e.log_id);
        let last_purged = inner.last_purged;
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last.or(last_purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.lock().vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.lock().vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        self.lock().committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.lock().committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        {
            let mut inner = self.lock();
            for entry in entries {
                inner.log.insert(entry.log_id.index, entry);
            }
        }
        // In-memory: the entries are readable the instant they are in the map, so signal the
        // flush complete immediately. (A durable store would fsync the appended run first.)
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.lock();
        let keys: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.lock();
        inner.last_purged = Some(log_id);
        let keys: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        Ok(())
    }
}

// ===========================================================================
// State machine — the ClusterState document, applied through control::apply.
// ===========================================================================

/// A built snapshot: the serialized [`ClusterState`] + its meta.
#[derive(Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<u64, BasicNode>,
    data: Vec<u8>,
}

struct SmInner {
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    /// The committed cluster-state document (ADR-037).
    state: ClusterState,
    snapshot: Option<StoredSnapshot>,
    /// Monotonic counter for unique snapshot ids (no wall-clock in library code).
    snapshot_idx: u64,
}

/// In-memory [`RaftStateMachine`] over the cluster-state document. `Arc`-shared so the owning
/// [`RaftControlPlane`] can read the committed document directly (after a linearizable check),
/// and so `get_snapshot_builder` hands out a cheap clone.
#[derive(Clone)]
struct StateMachine {
    inner: Arc<Mutex<SmInner>>,
}

impl StateMachine {
    /// Genesis from a seed document — the openraft-side analogue of
    /// [`InMemoryControlPlane::single_node`](super::control::InMemoryControlPlane::single_node),
    /// so both backends start from a byte-identical document.
    fn new(genesis: ClusterState) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SmInner {
                last_applied: None,
                last_membership: StoredMembership::default(),
                state: genesis,
                snapshot: None,
                snapshot_idx: 0,
            })),
        }
    }

    fn lock(&self) -> MutexGuard<'_, SmInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A direct, NON-linearizable read of the committed document — used for follower-convergence
    /// assertions, where a linearizable read would forward to the leader.
    fn local_state(&self) -> ClusterState {
        self.lock().state.clone()
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let mut inner = self.lock();
        let data =
            serde_json::to_vec(&inner.state).map_err(|e| StorageIOError::read_state_machine(&e))?;
        inner.snapshot_idx += 1;
        let snapshot_id = match inner.last_applied {
            Some(log_id) => format!("{}-{}", log_id.index, inner.snapshot_idx),
            None => format!("none-{}", inner.snapshot_idx),
        };
        let meta = SnapshotMeta {
            last_log_id: inner.last_applied,
            last_membership: inner.last_membership.clone(),
            snapshot_id,
        };
        inner.snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let inner = self.lock();
        Ok((inner.last_applied, inner.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ClusterStateResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut inner = self.lock();
        let mut responses = Vec::new();
        for entry in entries {
            inner.last_applied = Some(entry.log_id);
            match entry.payload {
                // A new leader's no-op marker — does NOT advance the semantic epoch.
                EntryPayload::Blank => {}
                // The ONE apply funnel shared with InMemoryControlPlane (live ≡ replay).
                EntryPayload::Normal(change) => {
                    super::control::apply(&mut inner.state, change);
                    inner.state.epoch += 1;
                }
                // Raft membership ⇒ the app voter set (the faithful change_membership mapping).
                EntryPayload::Membership(m) => {
                    let mut voters: Vec<NodeId> = m.voter_ids().map(NodeId).collect();
                    voters.sort_unstable();
                    voters.dedup();
                    inner.state.voters = voters;
                    inner.last_membership = StoredMembership::new(Some(entry.log_id), m);
                    inner.state.epoch += 1;
                }
            }
            let version = inner.state.epoch;
            responses.push(ClusterStateResponse { version });
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let data = snapshot.into_inner();
        let state: ClusterState = serde_json::from_slice(&data)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        let mut inner = self.lock();
        inner.state = state;
        inner.last_applied = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();
        inner.snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        Ok(self.lock().snapshot.as_ref().map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Box::new(Cursor::new(s.data.clone())),
        }))
    }
}

// ===========================================================================
// In-process network — direct dispatch over a registry of Raft handles. This
// proves the backend end-to-end in ONE process (the 5b-1 convergence oracle);
// the cross-process gRPC RaftNetwork is step 5b-2 (a sibling module).
// ===========================================================================

/// Shared map `node id → its Raft handle`, populated as the in-process cluster is built. A peer
/// not yet present is reported [`Unreachable`] (Raft backs off and retries), exactly as a real
/// transport would treat a node that has not started listening.
type Registry = Arc<Mutex<BTreeMap<u64, Raft<TypeConfig>>>>;

#[derive(Clone)]
struct InProcFactory {
    registry: Registry,
}

struct InProcNetwork {
    target: u64,
    registry: Registry,
}

impl InProcNetwork {
    fn peer(&self) -> Option<Raft<TypeConfig>> {
        self.registry
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&self.target)
            .cloned()
    }
}

/// A peer not yet registered: temporarily unreachable (Raft backs off + retries).
fn unreachable(target: u64) -> Unreachable {
    Unreachable::new(&std::io::Error::new(
        std::io::ErrorKind::NotConnected,
        format!("in-process peer {target} not registered"),
    ))
}

impl RaftNetwork<TypeConfig> for InProcNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let peer = self
            .peer()
            .ok_or_else(|| RPCError::Unreachable(unreachable(self.target)))?;
        peer.append_entries(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let peer = self
            .peer()
            .ok_or_else(|| RPCError::Unreachable(unreachable(self.target)))?;
        peer.vote(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<u64>,
        snapshot: Snapshot<TypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<u64>, StreamingError<TypeConfig, Fatal<u64>>> {
        let peer = self
            .peer()
            .ok_or_else(|| StreamingError::Unreachable(unreachable(self.target)))?;
        peer.install_full_snapshot(vote, snapshot)
            .await
            .map_err(|e| StreamingError::RemoteError(RemoteError::new(self.target, e)))
    }
}

impl RaftNetworkFactory<TypeConfig> for InProcFactory {
    type Network = InProcNetwork;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        InProcNetwork {
            target,
            registry: Arc::clone(&self.registry),
        }
    }
}

// ===========================================================================
// gRPC network — the cross-process transport (ADR-038 step 5b-2). Each Raft
// RPC is a serde-encoded OPAQUE envelope over the proto `ControlService`: the
// consensus engine owns its own schema, the proto stays a dumb byte pipe.
// ===========================================================================

/// serde-encode an openraft message into the opaque wire envelope. `pub(super)` so the
/// [`ControlServer`](super::control_server::ControlServer) re-uses the SAME codec the client does.
pub(super) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, ControlError> {
    serde_json::to_vec(value).map_err(|e| ControlError::Backend(format!("raft encode: {e}")))
}

/// serde-decode an opaque wire envelope back into an openraft message.
pub(super) fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, ControlError> {
    serde_json::from_slice(bytes).map_err(|e| ControlError::Backend(format!("raft decode: {e}")))
}

/// The whole snapshot shipped in one `full_snapshot` RPC — the cluster-state document is tiny, so
/// chunked streaming buys nothing (the `generic-snapshot-data` feature lets us send it whole).
#[derive(Serialize, Deserialize)]
pub(crate) struct SnapshotEnvelope {
    pub vote: Vote<u64>,
    pub meta: SnapshotMeta<u64, BasicNode>,
    pub data: Vec<u8>,
}

/// A [`RaftNetworkFactory`] over the gRPC `ControlService`. `new_client` builds a *lazily*
/// connecting client per target (the openraft contract: do not connect eagerly).
#[derive(Clone, Default)]
pub(crate) struct GrpcControlNetworkFactory;

/// A [`RaftNetwork`] to one peer over gRPC. `client` is `None` if the peer address is malformed,
/// so every RPC reports the peer unreachable instead of panicking at construction.
pub(crate) struct GrpcControlNetwork {
    target: u64,
    client: Option<ControlServiceClient<Channel>>,
}

impl GrpcControlNetwork {
    fn client(&self) -> Option<ControlServiceClient<Channel>> {
        self.client.clone()
    }
}

/// Map a tonic transport status onto an RPC error: an unavailable peer should back off
/// (`Unreachable`); anything else retries immediately (`Network`).
fn rpc_status<E: std::error::Error>(
    target: u64,
    status: &tonic::Status,
) -> RPCError<u64, BasicNode, E> {
    if status.code() == tonic::Code::Unavailable {
        RPCError::Unreachable(unreachable(target))
    } else {
        RPCError::Network(NetworkError::new(status))
    }
}

impl RaftNetwork<TypeConfig> for GrpcControlNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let mut client = self
            .client()
            .ok_or_else(|| RPCError::Unreachable(unreachable(self.target)))?;
        let data = encode(&rpc).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let reply = client
            .append_entries(tonic::Request::new(proto::RaftEnvelope { data }))
            .await
            .map_err(|s| rpc_status(self.target, &s))?;
        let res: Result<AppendEntriesResponse<u64>, RaftError<u64>> =
            decode(&reply.into_inner().data)
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        res.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let mut client = self
            .client()
            .ok_or_else(|| RPCError::Unreachable(unreachable(self.target)))?;
        let data = encode(&rpc).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let reply = client
            .vote(tonic::Request::new(proto::RaftEnvelope { data }))
            .await
            .map_err(|s| rpc_status(self.target, &s))?;
        let res: Result<VoteResponse<u64>, RaftError<u64>> = decode(&reply.into_inner().data)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        res.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<u64>,
        snapshot: Snapshot<TypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<u64>, StreamingError<TypeConfig, Fatal<u64>>> {
        let mut client = self
            .client()
            .ok_or_else(|| StreamingError::Unreachable(unreachable(self.target)))?;
        let env = SnapshotEnvelope {
            vote,
            meta: snapshot.meta,
            data: snapshot.snapshot.into_inner(),
        };
        let data = encode(&env).map_err(|e| StreamingError::Network(NetworkError::new(&e)))?;
        let reply = client
            .snapshot(tonic::Request::new(proto::RaftEnvelope { data }))
            .await
            .map_err(|s| {
                if s.code() == tonic::Code::Unavailable {
                    StreamingError::Unreachable(unreachable(self.target))
                } else {
                    StreamingError::Network(NetworkError::new(&s))
                }
            })?;
        let res: Result<SnapshotResponse<u64>, Fatal<u64>> = decode(&reply.into_inner().data)
            .map_err(|e| StreamingError::Network(NetworkError::new(&e)))?;
        res.map_err(|e| StreamingError::RemoteError(RemoteError::new(self.target, e)))
    }
}

impl RaftNetworkFactory<TypeConfig> for GrpcControlNetworkFactory {
    type Network = GrpcControlNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        let client = Channel::from_shared(node.addr.clone())
            .ok()
            .map(|ep| ControlServiceClient::new(ep.connect_lazy()));
        GrpcControlNetwork { target, client }
    }
}

// ===========================================================================
// RaftControlPlane — the trait ControlPlane backend over a local Raft<C>.
// ===========================================================================

/// A [`ControlPlane`] backed by a local openraft [`Raft`] node (ADR-038). Holds the Raft handle,
/// a clone of its state machine (for linearizable reads), and a tokio [`Handle`] to bridge the
/// sync seam onto async Raft calls (`block_on`, off the runtime's worker threads).
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
fn map_client_write(e: RaftError<u64, ClientWriteError<u64, BasicNode>>) -> ControlError {
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
fn map_check_leader(e: RaftError<u64, CheckIsLeaderError<u64, BasicNode>>) -> ControlError {
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

// ===========================================================================
// Builders.
// ===========================================================================

/// A Raft [`Config`] tuned for the control plane: fast heartbeats/elections (the cluster-state
/// document is small + low-rate, so quick failover matters more than minimizing chatter).
fn control_config() -> Result<Config, ControlError> {
    Config {
        cluster_name: "reverse-rusty-control".to_string(),
        heartbeat_interval: 150,
        election_timeout_min: 300,
        election_timeout_max: 600,
        ..Default::default()
    }
    .validate()
    .map_err(|e| ControlError::Backend(format!("invalid raft config: {e}")))
}

/// Build a [`RaftControlPlane`] over an explicit network factory + genesis document. Shared by
/// [`in_process_cluster`] (in-process registry network) and the gRPC manager node (step 5b-2).
fn build_node<N>(
    node_id: u64,
    genesis: ClusterState,
    network: N,
    handle: &Handle,
) -> Result<RaftControlPlane, ControlError>
where
    N: RaftNetworkFactory<TypeConfig>,
{
    let config = Arc::new(control_config()?);
    let log = LogStore::default();
    let sm = StateMachine::new(genesis);
    let raft = handle
        .block_on(Raft::new(node_id, config, network, log, sm.clone()))
        .map_err(|e| ControlError::Backend(format!("Raft::new({node_id}): {e}")))?;
    Ok(RaftControlPlane {
        raft,
        sm,
        handle: handle.clone(),
    })
}

/// Build a cluster-manager node that talks to its peers over the gRPC `ControlService` (ADR-038
/// step 5b-2). Seeds the [`single_node_state`] genesis (every manager starts from the same
/// document); the caller serves a [`ControlServer`](super::control_server::ControlServer) over
/// `node.raft()` and, on exactly ONE node, calls [`RaftControlPlane::initialize`] with the manager
/// addresses once all peers are listening. Returns the node handle (not yet serving).
pub fn start_grpc_node(
    node_id: u64,
    num_shards: u32,
    vnodes: u32,
    dict_fingerprint: u64,
    handle: &Handle,
) -> Result<RaftControlPlane, ControlError> {
    let genesis = single_node_state(num_shards, vnodes, dict_fingerprint);
    build_node(node_id, genesis, GrpcControlNetworkFactory, handle)
}

/// Build an in-process multi-node control-plane cluster: `ids.len()` real [`Raft`] nodes wired by
/// a direct-dispatch registry network, all seeded with the [`single_node_state`] genesis
/// (`num_shards`/`vnodes`/`dict_fingerprint`) so the committed document is comparable to
/// [`InMemoryControlPlane::single_node`](super::control::InMemoryControlPlane::single_node). Node
/// `ids[0]` bootstraps the cluster; the call blocks until a leader is elected.
///
/// This runs genuine elections + log replication + quorum commit in ONE process — the acceptance
/// vehicle for the openraft backend (ADR-038 step 5b-1). It is `distributed`-gated and intended for
/// the oracle / single-process embedding; multi-process deployment uses the gRPC `ControlService`
/// (step 5b-2).
pub fn in_process_cluster(
    ids: &[u64],
    num_shards: u32,
    vnodes: u32,
    dict_fingerprint: u64,
    handle: &Handle,
) -> Result<Vec<RaftControlPlane>, ControlError> {
    let Some(&first_id) = ids.first() else {
        return Err(ControlError::Backend(
            "in_process_cluster needs at least one node".into(),
        ));
    };
    let registry: Registry = Arc::new(Mutex::new(BTreeMap::new()));
    let genesis = single_node_state(num_shards, vnodes, dict_fingerprint);

    let mut planes = Vec::with_capacity(ids.len());
    for &id in ids {
        let factory = InProcFactory {
            registry: Arc::clone(&registry),
        };
        let plane = build_node(id, genesis.clone(), factory, handle)?;
        // Register the handle so peers can reach it. Raft tolerates the brief window before all
        // handles are present (uninitialized nodes do not campaign, so no RPCs fly until bootstrap).
        registry
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(id, plane.raft());
        planes.push(plane);
    }

    // Bootstrap from the first node with ALL members as genesis voters (in-process addresses are
    // placeholders — the registry routes by id, not address).
    let members: Vec<(u64, String)> = ids
        .iter()
        .map(|&id| (id, format!("inproc://{id}")))
        .collect();
    if let Some(first) = planes.first() {
        first.initialize(&members)?;
    }
    wait_for_leader(&planes, first_id, Duration::from_secs(10))?;
    Ok(planes)
}

/// Poll until some node reports an elected leader, or `timeout` elapses (fail-closed — a silent
/// "no leader" would hang the caller). Returns the elected leader id.
fn wait_for_leader(
    planes: &[RaftControlPlane],
    _bootstrap_id: u64,
    timeout: Duration,
) -> Result<u64, ControlError> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(leader) = planes.first().and_then(RaftControlPlane::current_leader) {
            return Ok(leader);
        }
        if Instant::now() >= deadline {
            return Err(ControlError::Backend(
                "no control-plane leader elected within timeout".into(),
            ));
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}
