//! `RemoteControlPlane` — a [`ControlPlane`] backed by a gRPC `ControlService` client (ADR-083).
//!
//! The coordinator-side counterpart of [`ControlServer`](super::control_server::ControlServer)'s
//! `ClientControl` handler. It lets a coordinator (the cluster-mode `server` binary) read + propose
//! against a DURABLE openraft quorum **as a thin client** — it does NOT join consensus, so the
//! coordinator stays stateless (the model in `cluster_mode.rs`). It implements the existing sync
//! [`ControlPlane`] trait by blocking on its async tonic client via a [`tokio::runtime::Handle`],
//! exactly like [`RemoteShard`](super::remote::RemoteShard) — so swapping the in-memory backend for
//! this one changes no coordinator call site (the seam ADR-037 designed for).
//!
//! Reads (`cluster_state`) hit the leader (the server runs `ensure_linearizable`), and a follower's
//! `ForwardToLeader` is followed transparently: the client redials the named leader and retries the
//! request ONCE. Any RPC/transport failure surfaces as [`ControlError::Backend`] — never a swallowed
//! stale read of the assignment map, which would route a title to the wrong node (a shard-sized FN).

use std::sync::Arc;

use tokio::runtime::Handle;

use super::control::{
    ClusterState, ClusterStateChange, ControlError, ControlPlane, NodeId, StateVersion,
};
use super::control_raft::{decode, encode};
use super::control_wire::{ClientControlReply, ClientControlRequest, WireControlError};
use super::proto;
use super::proto::control_service_client::ControlServiceClient;
use super::remote::{block_on_in_context, MeshChannel};
use super::security::{configure_endpoint, ClientSecurity, MeshAuthInject};
use super::shard::ShardError;

/// Async mesh connect for the control plane (ADR-071/083): configure the endpoint (TLS when the
/// security config carries it), eagerly connect, wrap with the token interceptor — the
/// `ControlService` analogue of [`super::remote::connect_mesh`].
async fn connect_control_mesh(
    endpoint: &str,
    security: &ClientSecurity,
) -> Result<ControlServiceClient<MeshChannel>, ShardError> {
    let ep = configure_endpoint(endpoint, security.tls.as_ref(), &security.transport)?;
    let channel = ep
        .connect()
        .await
        .map_err(|e| ShardError::Remote(format!("control connect: {e}")))?;
    let inject = MeshAuthInject::new(security.token.as_deref())?;
    Ok(ControlServiceClient::with_interceptor(channel, inject))
}

/// A [`ControlPlane`] served by a remote `ControlService` quorum node.
pub struct RemoteControlPlane {
    /// The eagerly-connected primary client (the first reachable endpoint at connect time) — the
    /// happy path for every call.
    client: ControlServiceClient<MeshChannel>,
    /// The full configured endpoint list, retained for per-call failover when the primary's node is
    /// unreachable (ADR-086): a control op redials the remaining endpoints in order.
    endpoints: Vec<String>,
    handle: Handle,
    /// Retained so a `ForwardToLeader` redirect (or a failover redial) can reconnect over the same
    /// mesh security.
    security: ClientSecurity,
}

impl RemoteControlPlane {
    /// Connect to a single `ControlService` at `endpoint` (e.g. `"https://control0:50061"`). A
    /// one-element [`Self::connect_failover`]; the coordinator-mode binary passes the whole
    /// `--control-endpoint` list to `connect_failover` for multi-endpoint failover (ADR-086).
    pub fn connect(
        endpoint: &str,
        handle: Handle,
        security: ClientSecurity,
    ) -> Result<Self, ControlError> {
        let endpoints = [endpoint.to_string()];
        Self::connect_failover(&endpoints, handle, security)
    }

    /// Connect to the first reachable endpoint in `endpoints`, retaining the full list for per-call
    /// failover (ADR-086). Tries each in order; the first that dials becomes the primary client. All
    /// endpoints unreachable ⇒ fail loud (the coordinator refuses to boot against a dead quorum,
    /// like a dead shard connect). A bad endpoint/handshake fails here, not on the first op.
    pub fn connect_failover(
        endpoints: &[String],
        handle: Handle,
        security: ClientSecurity,
    ) -> Result<Self, ControlError> {
        if endpoints.is_empty() {
            return Err(ControlError::Backend(
                "connect_failover: no control-plane endpoints given".into(),
            ));
        }
        let mut last_err = String::new();
        for endpoint in endpoints {
            match block_on_in_context(&handle, connect_control_mesh(endpoint, &security)) {
                Ok(client) => {
                    return Ok(Self {
                        client,
                        endpoints: endpoints.to_vec(),
                        handle,
                        security,
                    });
                }
                Err(e) => last_err = format!("connect {endpoint}: {e}"),
            }
        }
        Err(ControlError::Backend(format!(
            "all {} control-plane endpoints unreachable (last: {last_err})",
            endpoints.len(),
        )))
    }

    /// Drive ONE `ClientControl` RPC over `client`, returning the decoded reply.
    fn call_once(
        &self,
        client: &ControlServiceClient<MeshChannel>,
        req: &ClientControlRequest,
    ) -> Result<ClientControlReply, ControlError> {
        let data =
            encode(req).map_err(|e| ControlError::Backend(format!("encode request: {e}")))?;
        let mut client = client.clone();
        let env = block_on_in_context(&self.handle, async move {
            client.client_control(proto::RaftEnvelope { data }).await
        })
        .map_err(|e| ControlError::Backend(format!("client_control rpc: {e}")))?
        .into_inner();
        decode(&env.data).map_err(|e| ControlError::Backend(format!("decode reply: {e}")))
    }

    /// Drive a request over one specific client, following a single `ForwardToLeader` redirect: if
    /// the contacted node is a follower it returns the leader's address, and we redial + retry there
    /// once. A second forward (e.g. an election in flight) surfaces as the error rather than looping.
    fn call_via(
        &self,
        client: &ControlServiceClient<MeshChannel>,
        req: &ClientControlRequest,
    ) -> Result<ClientControlReply, ControlError> {
        let reply = self.call_once(client, req)?;
        if let ClientControlReply::Err(WireControlError::ForwardToLeader {
            addr: Some(leader_addr),
            ..
        }) = &reply
        {
            let leader = block_on_in_context(
                &self.handle,
                connect_control_mesh(leader_addr, &self.security),
            )
            .map_err(|e| ControlError::Backend(format!("redial leader {leader_addr}: {e}")))?;
            return self.call_once(&leader, req);
        }
        Ok(reply)
    }

    /// Call the control plane. Try the primary client; within that attempt a follower's
    /// `ForwardToLeader` is followed once ([`Self::call_via`]). On a transport/backend failure,
    /// **idempotent reads** then fail over to the remaining configured endpoints in order, each
    /// redialed fresh (ADR-086) — failover finds a *reachable* node, ForwardToLeader finds the
    /// *leader* among reachable nodes; bounded to one try per endpoint per call.
    ///
    /// **Writes (`Propose`/`ChangeMembership`) never fail over.** A write that reached the leader may
    /// have COMMITTED before a transport error swallowed the response; resubmitting it to another
    /// endpoint could double-apply a non-idempotent op (e.g. `BumpModelVersion` increments the model
    /// version on every commit). So a failed write surfaces loud and converges via an
    /// operator/restart retry — the same "writes never retry" stance as ADR-085's shard transport.
    /// The single `ForwardToLeader` follow inside `call_via` stays safe: a follower redirects WITHOUT
    /// applying, so the leader sees the first application.
    fn call(&self, req: &ClientControlRequest) -> Result<ClientControlReply, ControlError> {
        let primary = self.call_via(&self.client, req);
        if matches!(
            req,
            ClientControlRequest::Propose(_) | ClientControlRequest::ChangeMembership(_)
        ) {
            return primary; // a non-idempotent write must not be resubmitted to a fallback endpoint
        }
        let primary_err = match primary {
            Ok(reply) => return Ok(reply),
            Err(e) => e,
        };
        for endpoint in &self.endpoints {
            let Ok(client) =
                block_on_in_context(&self.handle, connect_control_mesh(endpoint, &self.security))
            else {
                continue; // this endpoint is down too — try the next
            };
            if let Ok(reply) = self.call_via(&client, req) {
                return Ok(reply);
            }
        }
        Err(primary_err)
    }
}

/// Extract the typed success payload from a reply, mapping a wire error (incl. a residual
/// `ForwardToLeader` the single retry did not resolve) back to the typed [`ControlError`]. A reply
/// of the wrong variant is a protocol violation, surfaced loud rather than swallowed.
fn unexpected(op: &str) -> ControlError {
    ControlError::Backend(format!("unexpected control-plane reply to {op}"))
}

impl ControlPlane for RemoteControlPlane {
    fn cluster_state(&self) -> Result<Arc<ClusterState>, ControlError> {
        match self.call(&ClientControlRequest::GetState)? {
            ClientControlReply::State(s) => Ok(Arc::new(*s)),
            ClientControlReply::Err(e) => Err(e.into()),
            _ => Err(unexpected("GetState")),
        }
    }

    fn version(&self) -> Result<StateVersion, ControlError> {
        match self.call(&ClientControlRequest::Version)? {
            ClientControlReply::Version(v) => Ok(StateVersion(v)),
            ClientControlReply::Err(e) => Err(e.into()),
            _ => Err(unexpected("Version")),
        }
    }

    fn propose(&self, change: ClusterStateChange) -> Result<StateVersion, ControlError> {
        match self.call(&ClientControlRequest::Propose(change))? {
            ClientControlReply::Committed(v) => Ok(StateVersion(v)),
            ClientControlReply::Err(e) => Err(e.into()),
            _ => Err(unexpected("Propose")),
        }
    }

    fn change_membership(&self, voters: Vec<NodeId>) -> Result<StateVersion, ControlError> {
        match self.call(&ClientControlRequest::ChangeMembership(voters))? {
            ClientControlReply::Committed(v) => Ok(StateVersion(v)),
            ClientControlReply::Err(e) => Err(e.into()),
            _ => Err(unexpected("ChangeMembership")),
        }
    }

    fn leader(&self) -> Result<Option<NodeId>, ControlError> {
        match self.call(&ClientControlRequest::Leader)? {
            ClientControlReply::Leader(l) => Ok(l),
            ClientControlReply::Err(e) => Err(e.into()),
            _ => Err(unexpected("Leader")),
        }
    }
}
