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
use openraft::{Raft, Snapshot};
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

/// Serves `ControlService` over a single manager node's [`Raft`] handle, plus — when a control plane
/// is attached via [`with_client_plane`](Self::with_client_plane) — the coordinator-facing
/// `ClientControl` op (ADR-083).
pub struct ControlServer {
    raft: Raft<TypeConfig>,
    /// `Some` ⇒ serve the client-facing `ClientControl` op against this plane (ADR-083); `None` ⇒
    /// a Raft-only server (the historical ADR-038 shape), where `ClientControl` returns
    /// `unimplemented` rather than a silently-wrong reply. The `Arc` is shared with the caller, who
    /// keeps it to `initialize` the cluster after the peers are listening.
    plane: Option<Arc<RaftControlPlane>>,
    /// Mesh security (ADR-071): TLS identity + expected cluster token. Default (none) ⇒ the
    /// historical plaintext/open behavior.
    security: ServerSecurity,
    /// `Some` ⇒ also serve the standard `grpc.health.v1.Health` service on this SEPARATE
    /// plaintext port for Kubernetes probes (ADR-084). `None` (default) ⇒ no second
    /// listener, byte-identical to the historical single-port behavior.
    health_addr: Option<SocketAddr>,
}

impl ControlServer {
    /// Wrap a manager node's Raft handle as a gRPC server (Raft RPCs only). Attach a control plane
    /// via [`with_client_plane`](Self::with_client_plane) to also serve the `ClientControl` op.
    pub fn new(raft: Raft<TypeConfig>) -> Self {
        Self {
            raft,
            plane: None,
            security: ServerSecurity::default(),
            health_addr: None,
        }
    }

    /// Enable the coordinator-facing `ClientControl` op (ADR-083) by attaching this node's control
    /// plane — the same node the `raft` handle belongs to. Without it, `ClientControl` returns
    /// `unimplemented` (a deployed `controlserver` always attaches it; the Raft-only oracle does not).
    #[must_use]
    pub fn with_client_plane(mut self, plane: Arc<RaftControlPlane>) -> Self {
        self.plane = Some(plane);
        self
    }

    /// Install mesh security (ADR-071), applied by every `serve*` method. Unset ⇒ byte-identical
    /// plaintext/open behavior.
    #[must_use]
    pub fn with_security(mut self, security: ServerSecurity) -> Self {
        self.security = security;
        self
    }

    /// Also serve the standard `grpc.health.v1.Health` service on `addr` — a SEPARATE
    /// plaintext port for Kubernetes liveness/readiness probes (ADR-084). Liveness
    /// (`Check("")`) is SERVING once the gRPC server is up; readiness (`Check("ready")`)
    /// tracks whether this raft node currently sees a leader (consensus is live + this node
    /// participates). Unset ⇒ no second listener, byte-identical to the historical behavior.
    #[must_use]
    pub fn with_health_addr(mut self, addr: SocketAddr) -> Self {
        self.health_addr = Some(addr);
        self
    }

    /// A cloneable handle that renders this control node's `/_metrics` body on demand (ADR-091). The
    /// deploy bin captures it BEFORE `serve` consumes the server, then hands it to
    /// [`serve_metrics`](super::node_metrics::serve_metrics) on the plaintext `--metrics-addr` port.
    /// It holds a cheap-clone Raft handle, so it reports live consensus state (term / leader / log
    /// indices) and never touches a hot path.
    pub fn metrics_source(&self) -> ControlMetricsSource {
        ControlMetricsSource {
            raft: self.raft.clone(),
        }
    }

    /// Build the tonic server (TLS when configured) + token-verified service — one assembly shared
    /// by every `serve*` flavor (mirrors `ShardServer`).
    fn secured_router(self) -> Result<tonic::transport::server::Router, tonic::transport::Error> {
        let security = self.security.clone();
        // Server-side HTTP/2 keepalive (ADR-085): reclaim dead/half-open client connections
        // instead of leaking them. Off any hot path; default-on via `ServerSecurity::default`.
        let mut builder = tonic::transport::Server::builder()
            .http2_keepalive_interval(Some(security.keepalive_interval))
            .http2_keepalive_timeout(Some(security.keepalive_timeout));
        if let Some(tls) = &security.tls {
            builder = builder.tls_config(server_tls_config(tls))?;
        }
        let verify = MeshAuthVerify::new(security.token);
        Ok(builder.add_service(ControlServiceServer::with_interceptor(self, verify)))
    }

    /// Serve `ControlService` on `addr` until the returned future completes. When a
    /// `--health-addr` was configured ([`with_health_addr`](Self::with_health_addr)), the
    /// plaintext health service runs concurrently on its own port and a watcher tracks
    /// readiness (this node sees a leader); the two servers are joined fail-loud (ADR-084).
    pub async fn serve(self, addr: SocketAddr) -> Result<(), tonic::transport::Error> {
        let Some(health_addr) = self.health_addr else {
            return self.secured_router()?.serve(addr).await;
        };
        // Clone the (cheap, `Clone`) raft handle BEFORE `secured_router` consumes `self`;
        // the watcher reads its metrics — no handler is touched.
        let reporter = super::health::HealthReporter::serving();
        let raft = self.raft.clone();
        super::health::spawn_readiness_watcher(reporter.clone(), move || {
            raft.metrics().borrow().current_leader.is_some()
        });
        let data = self.secured_router()?.serve(addr);
        let health = super::health::serve_health(health_addr, reporter);
        tokio::try_join!(data, health).map(|_| ())
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

/// A render handle for a [`ControlServer`]'s `/_metrics` endpoint (ADR-091). Holds a cheap-clone
/// Raft handle so it renders live consensus metrics and outlives the `serve` call that consumes the
/// server. `Send + 'static` so the deploy bin can move it into the metrics listener's render closure.
pub struct ControlMetricsSource {
    raft: Raft<TypeConfig>,
}

impl ControlMetricsSource {
    /// Render the current Prometheus exposition body for this control node from its live
    /// `RaftMetrics` (term, server state, leader, log indices, membership).
    pub fn render(&self) -> String {
        let metrics = self.raft.metrics();
        let view = super::node_metrics::control_view(&metrics.borrow());
        super::node_metrics::render_control(&view)
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

    /// Client-facing control-plane op (ADR-083): a coordinator's `RemoteControlPlane` reads/proposes
    /// against this node WITHOUT joining consensus. Done as native async (the sync `ControlPlane`
    /// methods `block_on` internally, which would nest on a gRPC worker), reusing the SAME openraft
    /// calls + error mapping `RaftControlPlane` uses — so the remote path is live ≡ the embedded
    /// backend. A `ForwardToLeader` is preserved on the wire so the client redials the leader.
    async fn client_control(
        &self,
        request: Request<proto::RaftEnvelope>,
    ) -> Result<Response<proto::RaftEnvelope>, Status> {
        let Some(plane) = &self.plane else {
            return Err(Status::unimplemented(
                "ClientControl is not enabled on this control server (Raft-only)",
            ));
        };
        let req: ClientControlRequest = decode(&request.into_inner().data)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let reply = match req {
            ClientControlRequest::GetState => match self.raft.ensure_linearizable().await {
                Ok(_) => ClientControlReply::State(Box::new(plane.local_state())),
                Err(e) => ClientControlReply::Err(WireControlError::from(&map_check_leader(e))),
            },
            // Linearizable like GetState (codex): a follower forwards to the leader, so a `version()`
            // right after a leader-forwarded `propose()` reflects the commit instead of the follower's
            // possibly-stale local epoch.
            ClientControlRequest::Version => match self.raft.ensure_linearizable().await {
                Ok(_) => ClientControlReply::Version(plane.local_state().epoch),
                Err(e) => ClientControlReply::Err(WireControlError::from(&map_check_leader(e))),
            },
            ClientControlRequest::Propose(change) => match self.raft.client_write(change).await {
                Ok(r) => ClientControlReply::Committed(r.data.version),
                Err(e) => ClientControlReply::Err(WireControlError::from(&map_client_write(e))),
            },
            ClientControlRequest::ChangeMembership(voters) => {
                let set: BTreeSet<u64> = voters.iter().map(|n| n.0).collect();
                match self.raft.change_membership(set, false).await {
                    Ok(r) => ClientControlReply::Committed(r.data.version),
                    Err(e) => ClientControlReply::Err(WireControlError::from(&map_client_write(e))),
                }
            }
            ClientControlRequest::Leader => {
                ClientControlReply::Leader(plane.current_leader().map(NodeId))
            }
        };
        let data = encode(&reply).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(proto::RaftEnvelope { data }))
    }
}
