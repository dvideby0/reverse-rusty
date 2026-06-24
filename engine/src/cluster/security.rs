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

use std::sync::Arc;
use std::time::Duration;

use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};
use tonic::{Request, Status};

use super::shard::ShardError;

/// A server's TLS identity: PEM-encoded certificate (chain) + private key, as read
/// from the operator's `--tls-cert` / `--tls-key` files.
#[derive(Clone)]
pub struct TlsServerIdentity {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

/// A client's TLS verification config: the PEM CA bundle that must have signed the
/// server certificate, plus an optional verification/SNI domain override (needed
/// when endpoints are raw IPs but the certificate names a DNS SAN).
#[derive(Clone)]
pub struct TlsClientConfig {
    pub ca_pem: Vec<u8>,
    pub domain: Option<String>,
}

/// Transport-resilience knobs for a mesh CLIENT connection (ADR-085), applied at two
/// layers so a hung/dead remote peer can never block a caller forever:
/// - [`configure_endpoint`] sets the channel-level `connect_timeout` + HTTP/2 keepalive
///   (a dead/half-open peer is detected and the connection broken, so in-flight RPCs
///   error out fail-loud), shared by the shard, control-plane, and Raft-peer clients.
/// - [`RemoteShard`](super::remote::RemoteShard) wraps each UNARY RPC in a per-call
///   deadline (`read_timeout`/`write_timeout`) and does bounded fail-loud retry of
///   IDEMPOTENT reads (`read_retries`) on transient errors. Streaming / long-pull
///   recovery RPCs opt out of the per-call deadline and lean on keepalive instead.
///
/// [`Default`] is the conservative always-on profile; the historical (pre-ADR-085)
/// behavior was no timeouts and no keepalive at all. A timeout only ever turns a hang
/// into a loud `ShardError` — it never drops a shard from a percolate union (that would
/// be a false negative); the cross-shard merge still fails closed.
#[derive(Clone, Debug)]
pub struct MeshTransport {
    /// Max time for the TCP + TLS dial before failing the connect.
    pub connect_timeout: Duration,
    /// HTTP/2 (and TCP) keepalive PING interval — detects a dead/idle peer mid-call.
    pub keepalive_interval: Duration,
    /// How long to wait for a keepalive PING ack before dropping the connection.
    pub keepalive_timeout: Duration,
    /// Per-call deadline for unary READ RPCs (percolate, counts, fingerprint).
    pub read_timeout: Duration,
    /// Per-call deadline for unary WRITE RPCs (ingest, insert, delete, flush, fence, lease).
    pub write_timeout: Duration,
    /// Bounded retry attempts for IDEMPOTENT reads on a transient error (total tries =
    /// `read_retries + 1`); `0` disables retry. Writes are never retried (non-idempotent).
    pub read_retries: u32,
}

impl MeshTransport {
    /// Default dial bound.
    pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
    /// Default keepalive PING interval.
    pub const DEFAULT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
    /// Default keepalive PING-ack timeout.
    pub const DEFAULT_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);
    /// Default per-call deadline for unary reads (the latency-sensitive percolate path).
    pub const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(10);
    /// Default per-call deadline for unary writes (generous — a bulk ingest batch is large).
    pub const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
    /// Default bounded read-retry count (3 total tries).
    pub const DEFAULT_READ_RETRIES: u32 = 2;
}

impl Default for MeshTransport {
    fn default() -> Self {
        MeshTransport {
            connect_timeout: Self::DEFAULT_CONNECT_TIMEOUT,
            keepalive_interval: Self::DEFAULT_KEEPALIVE_INTERVAL,
            keepalive_timeout: Self::DEFAULT_KEEPALIVE_TIMEOUT,
            read_timeout: Self::DEFAULT_READ_TIMEOUT,
            write_timeout: Self::DEFAULT_WRITE_TIMEOUT,
            read_retries: Self::DEFAULT_READ_RETRIES,
        }
    }
}

/// Server-side mesh security: TLS identity + the expected mesh token, plus server-side
/// HTTP/2 keepalive (ADR-085) so a dead/half-open CLIENT connection is reclaimed instead
/// of leaked. `Default` (no TLS, no token) is the historical plaintext/unauthenticated
/// behavior; the keepalive defaults are conservative and off any hot path.
#[derive(Clone)]
pub struct ServerSecurity {
    pub tls: Option<TlsServerIdentity>,
    pub token: Option<Vec<u8>>,
    /// HTTP/2 keepalive PING interval the server applies to client connections.
    pub keepalive_interval: Duration,
    /// How long the server waits for a PING ack before dropping a client connection.
    pub keepalive_timeout: Duration,
}

impl Default for ServerSecurity {
    fn default() -> Self {
        ServerSecurity {
            tls: None,
            token: None,
            keepalive_interval: MeshTransport::DEFAULT_KEEPALIVE_INTERVAL,
            keepalive_timeout: MeshTransport::DEFAULT_KEEPALIVE_TIMEOUT,
        }
    }
}

/// Client-side mesh security: TLS verification + the mesh token to present + the
/// [`MeshTransport`] resilience knobs (ADR-085). `Default` is the historical plaintext
/// behavior for TLS/token, now with the always-on transport timeouts + keepalive.
#[derive(Clone, Default)]
pub struct ClientSecurity {
    pub tls: Option<TlsClientConfig>,
    pub token: Option<Vec<u8>>,
    pub transport: MeshTransport,
}

/// Resolve the mesh token from a CLI flag and the `RR_CLUSTER_TOKEN` environment
/// variable — flag wins; the ADR-062 validation rules verbatim (non-empty, visible
/// ASCII, no spaces), fail-loud on a malformed value rather than silently serving
/// open. `Ok(None)` ⇔ no token configured.
pub fn resolve_mesh_token(
    flag: Option<String>,
    env: Result<String, std::env::VarError>,
) -> Result<Option<Vec<u8>>, String> {
    let raw = match (flag, env) {
        // Flag wins over env; both funnel into the same validation below.
        (Some(t), _) | (None, Ok(t)) => t,
        (None, Err(std::env::VarError::NotPresent)) => return Ok(None),
        (None, Err(std::env::VarError::NotUnicode(_))) => {
            // Set-but-undecodable must refuse startup, never silently disable auth
            // (the ADR-062 fail-open fix, kept here).
            return Err("RR_CLUSTER_TOKEN is set but not valid UTF-8".into());
        }
    };
    if raw.is_empty() {
        return Err("mesh token must not be empty".into());
    }
    if !raw.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
        return Err("mesh token must be visible ASCII with no spaces".into());
    }
    Ok(Some(raw.into_bytes()))
}

/// Build the (possibly TLS-wrapped) endpoint for a mesh client connection. With
/// `tls`, the endpoint should use an `https://` URI; the CA is installed and the
/// optional domain override applied. A malformed endpoint or TLS config fails loud.
pub(crate) fn configure_endpoint(
    endpoint: &str,
    tls: Option<&TlsClientConfig>,
    transport: &MeshTransport,
) -> Result<Endpoint, ShardError> {
    let ep = Endpoint::from_shared(endpoint.to_string())
        .map_err(|e| ShardError::Remote(format!("invalid endpoint {endpoint:?}: {e}")))?;
    // Transport resilience (ADR-085): bound the dial and enable HTTP/2 keepalive so a
    // dead/half-open peer is detected — the connection is broken and in-flight RPCs error
    // out fail-loud — instead of a call hanging forever. This is the SHARED endpoint
    // builder for the shard client, the control-plane client, AND the Raft peer network,
    // so all three harden in one place; it also covers the long streaming RPCs that opt
    // out of the per-call deadline (they lean on keepalive to notice a dead peer).
    let ep = ep
        .connect_timeout(transport.connect_timeout)
        .tcp_keepalive(Some(transport.keepalive_interval))
        .http2_keep_alive_interval(transport.keepalive_interval)
        .keep_alive_timeout(transport.keepalive_timeout)
        .keep_alive_while_idle(true);
    match tls {
        // An https endpoint with NO client TLS config would die inside tonic as an
        // opaque "transport error" — name the misconfiguration instead (the node is
        // missing its --tls-ca / client security half).
        None if endpoint.starts_with("https://") => Err(ShardError::Remote(format!(
            "endpoint {endpoint:?} is https but no client TLS config is set (this node \
             needs its CA — e.g. --tls-ca / --grpc-tls-ca — to dial a TLS mesh)"
        ))),
        None => Ok(ep),
        Some(cfg) => {
            let mut tls_cfg =
                ClientTlsConfig::new().ca_certificate(Certificate::from_pem(&cfg.ca_pem));
            if let Some(domain) = &cfg.domain {
                tls_cfg = tls_cfg.domain_name(domain.clone());
            }
            ep.tls_config(tls_cfg)
                .map_err(|e| ShardError::Remote(format!("TLS config for {endpoint:?}: {e}")))
        }
    }
}

/// Client-side interceptor: inject the mesh token (when configured) into every
/// outgoing RPC as `authorization: Bearer <token>`. With no token it is a no-op, so
/// the secured and plaintext paths share one client type.
#[derive(Clone)]
pub(crate) struct MeshAuthInject {
    header: Option<MetadataValue<Ascii>>,
}

impl MeshAuthInject {
    /// Build from the resolved token. The header value is pre-built once — token
    /// bytes are validated visible-ASCII at resolve time, so this cannot fail for a
    /// token that passed [`resolve_mesh_token`]; a value that somehow doesn't parse
    /// fails loud here rather than silently sending unauthenticated RPCs.
    pub(crate) fn new(token: Option<&[u8]>) -> Result<Self, ShardError> {
        let header = match token {
            None => None,
            Some(t) => {
                let v = format!("Bearer {}", String::from_utf8_lossy(t));
                Some(v.parse::<MetadataValue<Ascii>>().map_err(|e| {
                    ShardError::Remote(format!("mesh token is not a valid header value: {e}"))
                })?)
            }
        };
        Ok(MeshAuthInject { header })
    }
}

impl Interceptor for MeshAuthInject {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(h) = &self.header {
            request.metadata_mut().insert("authorization", h.clone());
        }
        Ok(request)
    }
}

/// Server-side interceptor: verify the mesh token on every incoming RPC BEFORE the
/// handler runs. With no expected token it is a pass-through (the historical open
/// behavior); with one, a missing/wrong token answers `UNAUTHENTICATED`. The
/// comparison is constant-time (no data-dependent branches).
#[derive(Clone)]
pub(crate) struct MeshAuthVerify {
    expected: Option<Arc<[u8]>>,
}

impl MeshAuthVerify {
    pub(crate) fn new(token: Option<Vec<u8>>) -> Self {
        MeshAuthVerify {
            expected: token.map(Arc::from),
        }
    }
}

impl Interceptor for MeshAuthVerify {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let Some(expected) = &self.expected else {
            return Ok(request);
        };
        let presented = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(bearer_token);
        match presented {
            Some(t) if ct_eq(t.as_bytes(), expected) => Ok(request),
            Some(_) => Err(Status::unauthenticated("invalid mesh token")),
            None => Err(Status::unauthenticated(
                "missing mesh token (authorization: Bearer <token>)",
            )),
        }
    }
}

/// Extract the token from a `Bearer <token>` value — scheme case-insensitive,
/// extra spaces tolerated (RFC 6750, mirroring the HTTP gate).
fn bearer_token(value: &str) -> Option<&str> {
    let (scheme, rest) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = rest.trim_start_matches(' ');
    (!token.is_empty()).then_some(token)
}

/// Constant-time byte equality (the ADR-062 comparator, duplicated here because the
/// HTTP gate lives in the server *binary*, not the library): the fold has no
/// data-dependent branch, so a wrong token costs the same as a nearly-right one.
/// Only the token's *length* is observable, which is not secret.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn verify(expected: Option<&str>, presented: Option<&str>) -> Result<(), Status> {
        let mut v = MeshAuthVerify::new(expected.map(|t| t.as_bytes().to_vec()));
        let mut req = Request::new(());
        if let Some(p) = presented {
            req.metadata_mut()
                .insert("authorization", p.parse().expect("header"));
        }
        v.call(req).map(|_| ())
    }

    #[test]
    fn resolve_validates_like_the_http_gate() {
        assert_eq!(
            resolve_mesh_token(None, Err(std::env::VarError::NotPresent)),
            Ok(None)
        );
        assert_eq!(
            resolve_mesh_token(Some("s3cret".into()), Err(std::env::VarError::NotPresent)),
            Ok(Some(b"s3cret".to_vec()))
        );
        // Flag wins over env.
        assert_eq!(
            resolve_mesh_token(Some("flag".into()), Ok("env".into())),
            Ok(Some(b"flag".to_vec()))
        );
        assert!(
            resolve_mesh_token(Some(String::new()), Err(std::env::VarError::NotPresent)).is_err()
        );
        assert!(resolve_mesh_token(
            Some("has space".into()),
            Err(std::env::VarError::NotPresent)
        )
        .is_err());
        assert!(
            resolve_mesh_token(Some("ünïcode".into()), Err(std::env::VarError::NotPresent))
                .is_err()
        );
    }

    #[test]
    fn verifier_gates_only_when_configured() {
        // No expected token ⇒ pass-through (the historical open behavior).
        assert!(verify(None, None).is_ok());
        assert!(verify(None, Some("Bearer anything")).is_ok());
        // Expected token ⇒ exact match required; missing/wrong are UNAUTHENTICATED.
        assert!(verify(Some("tok"), Some("Bearer tok")).is_ok());
        assert!(
            verify(Some("tok"), Some("bearer tok")).is_ok(),
            "scheme case-insensitive"
        );
        assert!(verify(Some("tok"), None).is_err());
        assert!(verify(Some("tok"), Some("Bearer wrong")).is_err());
        assert!(verify(Some("tok"), Some("Basic tok")).is_err());
    }

    #[test]
    fn injector_attaches_the_bearer_header() {
        let mut inj = MeshAuthInject::new(Some(b"tok")).expect("inject");
        let req = inj.call(Request::new(())).expect("call");
        assert_eq!(
            req.metadata()
                .get("authorization")
                .and_then(|v| v.to_str().ok()),
            Some("Bearer tok")
        );
        // No token ⇒ no header (byte-identical plaintext path).
        let mut inj = MeshAuthInject::new(None).expect("inject");
        let req = inj.call(Request::new(())).expect("call");
        assert!(req.metadata().get("authorization").is_none());
    }

    #[test]
    fn round_trip_inject_then_verify() {
        let mut inj = MeshAuthInject::new(Some(b"mesh-secret-1")).expect("inject");
        let req = inj.call(Request::new(())).expect("inject call");
        let mut v = MeshAuthVerify::new(Some(b"mesh-secret-1".to_vec()));
        assert!(v.call(req).is_ok());
    }
}
