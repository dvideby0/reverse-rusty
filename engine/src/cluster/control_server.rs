//! `ControlServer` — serves the gRPC `ControlService` over ONE cluster-manager's openraft node
//! (clustering build-path step 5b-2 / ADR-038).
//!
//! Each RPC is a dumb relay: decode the opaque [`RaftEnvelope`](super::proto::RaftEnvelope) into an
//! openraft request, hand it to the LOCAL [`Raft`] handler (`append_entries` / `vote` /
//! `install_full_snapshot`), and encode the handler's `Result` straight back into the reply
//! envelope. The consensus engine owns the schema; this server never inspects it. It is the
//! manager-side analogue of [`ShardServer`](super::server::ShardServer) (the data path) and is
//! served via the same port-race-safe `serve_with_incoming` pattern.

use std::io::Cursor;
use std::net::SocketAddr;

use openraft::raft::{AppendEntriesRequest, VoteRequest};
use openraft::{Raft, Snapshot};
use tonic::{Request, Response, Status};

use super::control_raft::{decode, encode, SnapshotEnvelope, TypeConfig};
use super::proto;
use super::proto::control_service_server::{ControlService, ControlServiceServer};

/// Serves `ControlService` over a single manager node's [`Raft`] handle (obtained from
/// [`RaftControlPlane::raft`](super::control_raft::RaftControlPlane::raft)).
pub struct ControlServer {
    raft: Raft<TypeConfig>,
}

impl ControlServer {
    /// Wrap a manager node's Raft handle as a gRPC server.
    pub fn new(raft: Raft<TypeConfig>) -> Self {
        Self { raft }
    }

    /// Serve `ControlService` on `addr` until the returned future completes.
    pub async fn serve(self, addr: SocketAddr) -> Result<(), tonic::transport::Error> {
        tonic::transport::Server::builder()
            .add_service(ControlServiceServer::new(self))
            .serve(addr)
            .await
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
        tonic::transport::Server::builder()
            .add_service(ControlServiceServer::new(self))
            .serve_with_shutdown(addr, signal)
            .await
    }

    /// Serve on an already-bound `incoming` listener (no rebind) — the port-race-safe path: bind
    /// the socket first, learn its port (an ephemeral `:0` for tests), then serve, with no
    /// bind→drop→rebind gap. Mirrors [`ShardServer::serve_with_incoming`](super::server::ShardServer).
    pub async fn serve_with_incoming(
        self,
        incoming: tonic::transport::server::TcpIncoming,
    ) -> Result<(), tonic::transport::Error> {
        tonic::transport::Server::builder()
            .add_service(ControlServiceServer::new(self))
            .serve_with_incoming(incoming)
            .await
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
        let res = self.raft.append_entries(req).await;
        let data = encode(&res).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(proto::RaftEnvelope { data }))
    }

    async fn vote(
        &self,
        request: Request<proto::RaftEnvelope>,
    ) -> Result<Response<proto::RaftEnvelope>, Status> {
        let req: VoteRequest<u64> = decode(&request.into_inner().data)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let res = self.raft.vote(req).await;
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
        let res = self.raft.install_full_snapshot(env.vote, snapshot).await;
        let data = encode(&res).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(proto::RaftEnvelope { data }))
    }
}
