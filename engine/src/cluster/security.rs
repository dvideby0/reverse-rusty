//! Mesh security for the gRPC transports (ADR-071, Distributed-v1 criterion 2):
//! TLS configuration shapes + the shared bearer-token (mesh secret) machinery used
//! by BOTH transports (shard + control plane) on both sides of the wire — one
//! implementation, so the two planes cannot drift.
//!
//! Two independent, opt-in knobs (unset ⇒ byte-identical to the plaintext paths):
//! - **TLS**: the server presents an operator-provided PEM identity; the client
//!   verifies it against an operator-provided CA (tonic's rustls integration, the
//!   `tls-ring` feature). Server authentication + wire privacy/integrity.
//! - **Mesh token**: ONE shared cluster secret attached to every RPC as standard
//!   `authorization: Bearer <token>` metadata by [`MeshAuthInject`] and verified
//!   constant-time by [`MeshAuthVerify`] BEFORE any handler runs — default-deny by
//!   construction (the interceptor wraps the whole service, so a future RPC is
//!   covered without being listed anywhere).
//!
//! Trust model (ADR-071): the token admits a node to the mesh; TLS authenticates
//! servers and protects the wire. mTLS / per-RPC authorization tiers are post-v1.

mod coordinator;
mod mesh;

pub(crate) use coordinator::{fresh_coordinator_id, CoordinatorLease, CoordinatorLeaseService};
pub(crate) use mesh::{
    claim_coordinator, configure_endpoint, coordinator_claim_requested, request_coordinator_id,
    MeshAuthInject, MeshAuthVerify,
};
pub use mesh::{
    resolve_mesh_token, ClientSecurity, MeshTransport, ServerSecurity, TlsClientConfig,
    TlsServerIdentity,
};

#[cfg(test)]
use coordinator::{
    CoordinatorAdmission, CoordinatorClaimHandshake, LeaseTrackedBody, COORDINATOR_CLAIM_HEADER,
    COORDINATOR_ID_HEADER,
};
#[cfg(test)]
mod tests;
