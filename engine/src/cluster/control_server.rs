//! `ControlServer` — serves the gRPC `ControlService` over ONE cluster-manager's openraft node
//! (clustering build-path step 5b-2 / ADR-038), plus the client-facing `ClientControl` op (ADR-083).
//!
//! The three Raft RPCs are dumb relays: decode the opaque envelope into an openraft request, hand it
//! to the LOCAL [`Raft`](openraft::Raft) handler (`append_entries` / `vote` / `install_full_snapshot`),
//! and encode the result back. `ClientControl` (ADR-083) is the coordinator-facing surface: a
//! [`RemoteControlPlane`](super::remote_control::RemoteControlPlane) reads/proposes against this node's
//! [`RaftControlPlane`](super::control_raft::RaftControlPlane) WITHOUT joining consensus. The server
//! holds the whole `RaftControlPlane` (not just its `Raft` handle) so it can both relay Raft RPCs
//! (`plane.raft()`) and serve client ops against the committed document.

use std::collections::BTreeSet;
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;

use openraft::raft::{AppendEntriesRequest, VoteRequest};
use openraft::Snapshot;
use tonic::{Request, Response, Status};

use super::control::NodeId;
use super::control_raft::{
    decode, encode, map_check_leader, map_client_write, RaftControlPlane, SnapshotEnvelope,
    TypeConfig,
};
use super::control_wire::{ClientControlReply, ClientControlRequest, WireControlError};
use super::proto;
use super::proto::control_service_server::{ControlService, ControlServiceServer};
use super::security::{MeshAuthVerify, ServerSecurity};
use super::server::server_tls_config;

/// Serves `ControlService` over a single manager node's [`RaftControlPlane`].
pub struct ControlServer {
    /// Shared (`Arc`) so the caller keeps the plane to `initialize` the cluster after peers listen.
    plane: Arc<RaftControlPlane>,
    /// Mesh security (ADR-071): TLS identity + expected cluster token. Default (none) ⇒ the
    /// historical plaintext/open behavior.
    security: ServerSecurity,
}

impl ControlServer {
    /// Wrap a manager node's control plane as a gRPC server.
    pub fn new(plane: Arc<RaftControlPlane>) -> Self {
        Self {
            plane,
            security: ServerSecurity::default(),
        }
    }

    /// Install mesh security (ADR-071), applied by every `serve*` method. Unset ⇒ byte-identical
    /// plaintext/open behavior.
    #[must_use]
    pub fn with_security(mut self, security: ServerSecurity) -> Self {
        self.security = security;
        self
    }

    /// Build the tonic server (TLS when configured) + token-verified service — one assembly shared
    /// by every `serve*` flavor (mirrors `ShardServer`).
    fn secured_router(self) -> Result<tonic::transport::server::Router, tonic::transport::Error> {
        let security = self.security.clone();
        let mut builder = tonic::transport::Server::builder();
        if let Some(tls) = &security.tls {
            builder = builder.tls_config(server_tls_config(tls))?;
        }
        let verify = MeshAuthVerify::new(security.token);
        Ok(builder.add_service(ControlServiceServer::with_interceptor(self, verify)))
    }

    /// Serve `ControlService` on `addr` until the returned future completes.
    pub async fn serve(self, addr: SocketAddr) -> Result<(), tonic::transport::Error> {
        self.secured_router()?.serve(addr).await
    }

    /// Serve with a graceful-shutdown `signal` future — used by tests to stop cleanly.
    pub async fn serve_with_shutdown<F>(
        self,
        addr: SocketAddr,
        signal: F,
    ) -> Result<(), tonic::transport::Error>
    where
        F: std::future::Future<Output = ()>,
    {
        self.secured_router()?
            .serve_with_shutdown(addr, signal)
            .await
    }

    /// Serve on an already-bound `incoming` listener (no rebind) — the port-race-safe path.
    pub async fn serve_with_incoming(
        self,
        incoming: tonic::transport::server::TcpIncoming,
    ) -> Result<(), tonic::transport::Error> {
        self.secured_router()?.serve_with_incoming(incoming).await
    }
}

#[tonic::async_trait]
impl ControlService for ControlServer {
    async fn append_entries(
        &self,
        request: Request<proto::RaftEnvelope>,
    ) -> Result<Response<proto::RaftEnvelope>, Status> {
        let req: AppendEntriesRequest<TypeConfig> = decode(&request.into_inner().data)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let res = self.plane.raft().append_entries(req).await;
        let data = encode(&res).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(proto::RaftEnvelope { data }))
    }

    async fn vote(
        &self,
        request: Request<proto::RaftEnvelope>,
    ) -> Result<Response<proto::RaftEnvelope>, Status> {
        let req: VoteRequest<u64> = decode(&request.into_inner().data)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let res = self.plane.raft().vote(req).await;
        let data = encode(&res).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(proto::RaftEnvelope { data }))
    }

    async fn snapshot(
        &self,
        request: Request<proto::RaftEnvelope>,
    ) -> Result<Response<proto::RaftEnvelope>, Status> {
        let env: SnapshotEnvelope = decode(&request.into_inner().data)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let snapshot = Snapshot {
            meta: env.meta,
            snapshot: Box::new(Cursor::new(env.data)),
        };
        let res = self
            .plane
            .raft()
            .install_full_snapshot(env.vote, snapshot)
            .await;
        let data = encode(&res).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(proto::RaftEnvelope { data }))
    }

    /// Client-facing control-plane op (ADR-083): a coordinator's `RemoteControlPlane` reads/proposes
    /// against this node WITHOUT joining consensus. Done as native async (the sync `ControlPlane`
    /// methods `block_on` internally, which would nest on a gRPC worker), reusing the SAME openraft
    /// calls + error mapping `RaftControlPlane` uses — so the remote path is live ≡ the embedded
    /// backend. A `ForwardToLeader` is preserved on the wire so the client redials the leader.
    async fn client_control(
        &self,
        request: Request<proto::RaftEnvelope>,
    ) -> Result<Response<proto::RaftEnvelope>, Status> {
        let req: ClientControlRequest = decode(&request.into_inner().data)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let raft = self.plane.raft();
        let reply = match req {
            ClientControlRequest::GetState => match raft.ensure_linearizable().await {
                Ok(_) => ClientControlReply::State(Box::new(self.plane.local_state())),
                Err(e) => ClientControlReply::Err(WireControlError::from(&map_check_leader(e))),
            },
            ClientControlRequest::Version => {
                ClientControlReply::Version(self.plane.local_state().epoch)
            }
            ClientControlRequest::Propose(change) => match raft.client_write(change).await {
                Ok(r) => ClientControlReply::Committed(r.data.version),
                Err(e) => ClientControlReply::Err(WireControlError::from(&map_client_write(e))),
            },
            ClientControlRequest::ChangeMembership(voters) => {
                let set: BTreeSet<u64> = voters.iter().map(|n| n.0).collect();
                match raft.change_membership(set, false).await {
                    Ok(r) => ClientControlReply::Committed(r.data.version),
                    Err(e) => ClientControlReply::Err(WireControlError::from(&map_client_write(e))),
                }
            }
            ClientControlRequest::Leader => {
                ClientControlReply::Leader(self.plane.current_leader().map(NodeId))
            }
        };
        let data = encode(&reply).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(proto::RaftEnvelope { data }))
    }
}
