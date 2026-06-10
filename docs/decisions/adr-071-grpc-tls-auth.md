# ADR-071: TLS + mesh auth on the gRPC transports (Distributed-v1 criterion 2)

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted

- **Context.** [ADR-065](adr-065-distributed-v1-graduation.md) criterion 2. Both gRPC surfaces — the
  shard transport (`ShardService`: percolate/ingest/recovery RPCs, ADR-029) and the control plane
  (`ControlService`: Raft vote/append/snapshot, ADR-038) — are **plaintext and unauthenticated**: any
  host that can reach the port can read queries on the wire, ingest/delete stored queries, stream a
  shard's segments (`FetchSegments`), or vote in the control plane. That is the single sharpest
  "experimental" caveat on the distributed layers. The HTTP surface solved its half in ADR-062
  (opt-in bearer token, default-deny, fail-loud config); the gRPC mesh needs the analogous rails plus
  wire privacy, because — unlike the HTTP API — it cannot hide behind one TLS-terminating proxy (the
  mesh is many node-to-node links, including node-initiated recovery pulls).

- **Decision.** Two independent, composable knobs on every gRPC link, both **opt-in and
  byte-identical when unset** (the ADR-062 posture):

  1. **TLS** (tonic's rustls integration, the **`tls-ring`** feature — pure-library build; aws-lc's
     cmake/C toolchain buys nothing here). The server presents an operator-provided PEM identity
     (`--tls-cert` + `--tls-key`); the client verifies it against an operator-provided CA
     (`--grpc-tls-ca`, with `--grpc-tls-domain` overriding the SNI/verification name when endpoints
     are raw IPs). Server authentication + wire privacy/integrity; **mTLS (client certificates) is
     deferred** — node admission at v1 is the token's job, and one shared secret is operationally
     honest for a single-operator mesh (the deployment model v1 targets).
  2. **Mesh auth token** — ONE shared cluster secret (`--cluster-token` / `RR_CLUSTER_TOKEN`,
     flag-wins precedence, the ADR-062 validation rules: non-empty visible ASCII, fail-loud on
     malformed). Clients attach it to **every RPC** as standard gRPC metadata
     (`authorization: Bearer <token>`); servers verify with a constant-time compare **before any
     handler runs** and answer `UNAUTHENTICATED` otherwise. Default-deny by construction: the check
     wraps the whole service, so a future RPC is covered without anyone remembering to list it.

  Configuration is **fail-loud**: an unreadable/malformed cert, key, or CA refuses startup; a token
  that fails validation refuses startup; a token configured **without TLS** starts but logs a loud
  warning naming the risk (the secret crosses the wire base64-ish-plaintext — same stance as
  ADR-062's HTTP-without-proxy warning). TLS-without-token works (wire privacy only).

- **Trust model (documented, not aspirational).** The mesh secret admits a node to the cluster
  (write/recovery/control RPCs); TLS authenticates *servers* to *clients* and protects the wire. A
  compromised node holding the secret is inside the trust boundary — v1 explicitly does not attempt
  per-node identity or authorization tiers (that is mTLS + per-RPC ACLs, a post-v1 hardening). The
  coordinator-mode HTTP server keeps its **separate** ADR-062 token: client-facing REST and the
  node mesh are different audiences with different rotation stories; sharing one secret across both
  would force rotating the public edge and the internal mesh together.

- **Mechanics.**
  - A `distributed`-gated `cluster::security` module owns the shapes: `TlsServerIdentity`
    (cert+key PEM), `TlsClientConfig` (CA PEM + optional domain), `MeshToken` (resolve + validate +
    constant-time verify + the metadata header builder/checker) — one implementation shared by both
    transports and both bins, so the shard and control planes cannot drift.
  - **Servers** (`ShardServer`, `ControlServer`): a `with_security(...)` builder; `serve*` applies
    `ServerTlsConfig` when present and wraps the service in the token interceptor when configured.
  - **Clients**: `RemoteShard::connect*` and the Raft `GrpcControlNetworkFactory` build their
    channels through one shared helper (`security::client_channel`) that applies `ClientTlsConfig`;
    the token rides per-request injection (one helper per client, called at every RPC site — no
    interceptor-generic type churn through the stored client fields).
  - **Builders/bins**: additive `connect_remote_with_security` / `connect_replicated_with_security`
    (the existing constructors delegate with `None` — byte-identical); `shardserver`/`controlserver`
    gain `--tls-cert`/`--tls-key`/`--cluster-token`; the coordinator-mode server (ADR-070) gains the
    client half (`--grpc-tls-ca`/`--grpc-tls-domain`/`--cluster-token`).

- **Why this is safe.** Security wrapping touches no matching or placement code: the interceptor
  runs before any handler, TLS is below the RPC layer, and every transport-level rejection surfaces
  through the existing fallible seam (`ShardError::Remote` — a failed probe fails the percolate
  loudly rather than shrinking the union, the ADR-029 posture). Unset ⇒ no interceptor, no TLS
  config ⇒ the wire bytes and code paths are exactly today's (every existing oracle unchanged).

- **Testing.** The gRPC oracle gains a secured variant: real `ShardServer`s on localhost behind TLS
  (an in-test `rcgen` self-signed CA + server cert — dev-dependency only; no key material checked
  in) + the mesh token, driven through `connect_remote_with_security` — secured cluster ≡ single-node
  ≡ brute, plus a live add/percolate over the secured link. Negative paths: a wrong/missing token is
  `UNAUTHENTICATED` (surfaced as `ShardError::Remote`, never an empty result); a plaintext client
  against a TLS server fails loud; a TLS client against a plaintext server fails loud. The control
  plane gets the same secured round trip (vote/append over TLS+token) via a secured 3-node cluster
  reaching a committed document. Config validation: malformed PEM/token refuse startup (unit tests
  on the security module).

- **Scope / explicitly deferred.** mTLS (client certs) + per-RPC authorization tiers; certificate
  rotation/reload without restart (operators restart nodes — serve-then-drop makes this cheap);
  TLS on the HTTP surface (ADR-062's documented reverse-proxy stance stands).

- **See also:** ADR-062 (the HTTP token gate this mirrors), ADR-029/034 (the shard transport +
  dict-shipping handshake the security wraps), ADR-038/041 (the control plane), ADR-065
  (the program; criterion 2), [`reference/api.md`](../reference/api.md) +
  [`clustering-and-scaling.md`](../design/clustering-and-scaling.md) (operator docs).
