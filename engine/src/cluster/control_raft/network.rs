//! openraft `RaftNetwork` implementations: an in-process direct-dispatch network (`InProcFactory`,
//! for tests) and a gRPC `ControlService` client network with an opaque serde envelope (ADR-038).

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};

use openraft::error::{
    Fatal, NetworkError, RPCError, RaftError, RemoteError, ReplicationClosed, StreamingError,
    Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{BasicNode, OptionalSend, Raft, Snapshot, SnapshotMeta, Vote};
use serde::{Deserialize, Serialize};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

use crate::cluster::control::ControlError;
use crate::cluster::proto;
use crate::cluster::proto::control_service_client::ControlServiceClient;
use crate::cluster::security::{configure_endpoint, ClientSecurity, MeshAuthInject};

use super::TypeConfig;

pub(in crate::cluster::control_raft) type Registry = Arc<Mutex<BTreeMap<u64, Raft<TypeConfig>>>>;

#[derive(Clone)]
pub(in crate::cluster::control_raft) struct InProcFactory {
    pub(in crate::cluster::control_raft) registry: Registry,
}

pub(in crate::cluster::control_raft) struct InProcNetwork {
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
/// [`ControlServer`](crate::cluster::control_server::ControlServer) re-uses the SAME codec the client does.
pub(crate) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, ControlError> {
    serde_json::to_vec(value).map_err(|e| ControlError::Backend(format!("raft encode: {e}")))
}

/// serde-decode an opaque wire envelope back into an openraft message.
pub(crate) fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, ControlError> {
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
/// connecting client per target (the openraft contract: do not connect eagerly). Carries
/// the mesh security config (ADR-071): TLS + the cluster token applied to every peer link
/// — manager nodes are clients of each other, so the client half lives here.
#[derive(Clone, Default)]
pub(crate) struct GrpcControlNetworkFactory {
    pub(crate) security: ClientSecurity,
}

/// The mesh-aware control client (ADR-071): every RPC flows through the token-injecting
/// interceptor (a no-op with no token), so secured and plaintext share one type.
type ControlMeshClient = ControlServiceClient<InterceptedService<Channel, MeshAuthInject>>;

/// A [`RaftNetwork`] to one peer over gRPC. `client` is `None` if the peer address or the
/// TLS/token config is malformed, so every RPC reports the peer unreachable instead of
/// panicking at construction.
pub(crate) struct GrpcControlNetwork {
    target: u64,
    client: Option<ControlMeshClient>,
}

impl GrpcControlNetwork {
    fn client(&self) -> Option<ControlMeshClient> {
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
        // Lazy connect per the openraft contract; the TLS config and token interceptor
        // are applied here so every peer link is secured identically (ADR-071).
        let client = configure_endpoint(
            &node.addr,
            self.security.tls.as_ref(),
            &self.security.transport,
        )
        .ok()
        .and_then(|ep| {
            let inject = MeshAuthInject::new(self.security.token.as_deref()).ok()?;
            Some(ControlServiceClient::with_interceptor(
                ep.connect_lazy(),
                inject,
            ))
        });
        GrpcControlNetwork { target, client }
    }
}
