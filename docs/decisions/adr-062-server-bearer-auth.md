# ADR-062: Opt-in bearer-token auth for the HTTP server's mutating/admin endpoints

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted

- **Context.** The external-review hardening pass (ADR-052) moved the HTTP server to a loopback bind
  by default but deliberately deferred authentication: the REST API exposed its mutating/admin
  endpoints (`_doc` writes, `_bulk`, `_flush`, `_compact`, `_vocab` writes, `_settings`) to anyone who
  could reach the port. Serving any network beyond the local host therefore *required* a trusted
  network or an authenticating reverse proxy — the roadmap tracked "an opt-in `RR_AUTH_TOKEN`-style
  gate on `_doc`/`_bulk`/`_flush`/`_compact`/`_vocab`/`_settings`" as the deferred production-hardening
  item. This ADR builds that item. (TLS, and auth on the *gRPC* transports, remain the separate Tier-3
  distributed item — this is the single-node HTTP surface, the part that is actually deployed today.)

- **Decision.** A single static bearer token, opt-in, enforced by one axum middleware
  (`src/bin/server/auth.rs`):
  - **Configuration.** `--auth-token <token>` or the `RR_AUTH_TOKEN` environment variable (the flag
    wins; the env var is preferred in production since flag values appear in process listings). Unset ⇒
    auth disabled ⇒ the server behaves **byte-identically** to before — strictly opt-in, like every
    other recent knob. Resolution **fails loud at startup** (refuses to boot) on an empty or
    non-printable token, or on `--auth-protect-reads` without a token — never silently serving open.
    The token value is never logged.
  - **Protected set — default-deny.** With a token configured, every request whose method is not
    GET/HEAD requires `Authorization: Bearer <token>`, **except** the read-via-POST percolate
    endpoints (`POST /_search`, `POST /_mpercolate`). The rule is method-shaped rather than an
    endpoint allowlist so a *future* mutating endpoint is protected without anyone remembering to
    register it; the two POST reads are the only (stable) exemptions. All non-GET `/_vocab*` verbs are
    protected, including the compute-only `/_vocab/learn` — it is operator surface.
  - **Optional read protection.** `--auth-protect-reads` extends the gate to read endpoints too —
    stored queries are the customer's data (`GET /_doc`, `/_vocab`, `_cat` would otherwise leak the
    corpus on an exposed port). Only `GET /_health` stays open: Kubernetes-style liveness probes can't
    send credentials, and the endpoint reveals nothing.
  - **Failure shape.** 401 with the standard error envelope (ES-style `"type": "security_exception"`)
    and an RFC 6750 `WWW-Authenticate: Bearer` challenge (`error="invalid_token"` when a token was
    presented but wrong, so clients can distinguish misconfiguration from missing credentials). Token
    comparison is constant-time (a branch-free XOR fold — only the token's *length* is observable,
    which is not secret). Failures increment `auth_failures_total{reason="missing"|"invalid"}` and log
    a structured `warn` (method + path + reason, never the presented token).
  - **Layering.** The auth middleware sits *outside* the concurrency limiter: an unauthenticated flood
    is rejected by a cheap header compare without consuming the 256 in-flight slots that protect the
    engine for legitimate traffic. It sits *inside* the request-id middleware, so 401s still carry
    `x-request-id` for correlation. Startup now also warns when binding a non-loopback interface
    *without* auth (the inverse of the ADR-052 loopback default).

- **Why a single static token (and not more).** This is the Meilisearch/Qdrant-class deployment
  shape, which matches Reverse Rusty's: a single-binary engine, machine-to-machine clients, a handful
  of trusted callers. One shared secret over one middleware is auditable in a screenful and adds zero
  dependencies (the constant-time compare is ~5 lines; no `subtle`, no token store). ES/OpenSearch-style
  users/roles/API-key management is a security *subsystem* — far more surface than the lean dependency
  philosophy (ADR-028) tolerates, and unneeded for the workload. TLS stays out: the engine should not
  grow a TLS stack when a reverse proxy or service mesh terminates it better; a bearer token over
  plaintext HTTP is for trusted-network use, and the docs say so explicitly.

- **Alternatives.** (1) *Reverse proxy only (status quo)* — still fully supported, but "you must deploy
  nginx to safely bind 0.0.0.0" is a sharp default for the simplest real deployment; an in-process
  gate removes the footgun. (2) *Per-route allowlist of protected endpoints* — rejected: the failure
  mode is forgetting to list the next admin endpoint, which fails open. Default-deny on non-GET fails
  closed. (3) *mTLS / API-key sets with roles* — disproportionate (see above). (4) *Protect reads
  always* — rejected: percolation is the service's purpose; forcing credentials onto the high-volume
  read path by default would break the byte-identical opt-in contract. It's one flag away.

- **Testing.** `auth.rs` unit tests pin the route table (every mutating endpoint requires auth; reads
  and both POST-read percolate endpoints don't; default-deny covers an unknown future endpoint;
  `protect_reads` gates everything but `/_health`), token resolution (flag-over-env precedence, the
  three fail-loud rejections), RFC 6750 header parsing (case-insensitive scheme, multi-space, wrong
  scheme, empty token), and the constant-time compare. Router-level tests (`tower::ServiceExt::oneshot`
  through the real middleware) prove: no-token-configured is a pass-through, missing ⇒ 401 +
  `WWW-Authenticate` + the error envelope, wrong ⇒ 401 + `error="invalid_token"`, right ⇒ handler
  runs, reads stay open, `/_health` always open, and both failure-reason counters increment. Verified
  live end-to-end (curl against a running server, including the Prometheus counter and the structured
  warn logs).

- **Consequences.** The server can now bind a non-loopback interface safely on a trusted network with
  one environment variable, closing the deferred ADR-052 item; the no-auth default remains
  byte-identical. The remaining (tracked, unchanged) residue: TLS/auth on the **gRPC** shard/control
  transports (Tier-3 distributed hardening), and cooperative cancellation on the match path (the other
  ADR-052 deferral). The `--version` flag (a one-line `clap` attribute) rode along, closing that
  ops-ergonomics backlog item for the server bin.
