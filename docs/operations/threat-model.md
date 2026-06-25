# Threat model

A basic threat model for Reverse Rusty: the trust boundaries, the assets, the adversaries we defend
against (and the ones we explicitly do not, at v1), and how each boundary's controls map to the code.
This is the [Phase 0](../roadmap.md) item-5 deliverable — the security-review companion to the deploy
checklist ([`build-and-smoke.md`](build-and-smoke.md)) and the deployment runbooks
([`cluster-deployment.md`](cluster-deployment.md), [`kubernetes-deployment.md`](kubernetes-deployment.md)).
Decisions behind the controls: bearer auth → [ADR-062](../decisions/adr-062-server-bearer-auth.md), mesh
TLS + token → [ADR-071](../decisions/adr-071-grpc-tls-auth.md), transport hardening →
[ADR-085](../decisions/adr-085-grpc-transport-hardening.md), backup →
[ADR-079](../decisions/adr-079-backup-restore.md), dependency policy →
[ADR-028](../decisions/adr-028-lean-core-feature-gate.md).

## What we are protecting

- **Availability of the matcher** — the cardinal product property is **zero false negatives**; a
  *silent* loss of a real match is worse than an outage. So the security posture is built so that a
  failure is **loud** (a 401/502/typed error), never a silently-truncated result set.
- **Integrity of the stored query corpus** — no unauthorized insert/delete/upsert, no corruption of
  the durable segments / WAL / manifest.
- **Confidentiality of the corpus + titles in transit** — stored queries and incoming titles can be
  commercially sensitive; on an untrusted network they must not be observable on the wire.
- **The host the node runs on** — no unauthorized write outside the node's data directory, no
  privilege escalation from the service account.

## Trust boundaries

```
                 (untrusted)                       (trusted mesh)
   client  ──HTTP──▶  server / coordinator  ──gRPC──▶  shardserver(s)
                          │                              controlserver(s)
                          ▼
                    data_dir (local disk)
```

1. **Client ↔ server (REST).** The single-node server and the cluster **coordinator** expose an
   ES-style REST API. This is the primary boundary: the client is untrusted.
2. **Coordinator ↔ shard / control mesh (gRPC).** In the distributed topology the coordinator, the
   shard data nodes, and the Raft control nodes talk over gRPC. Mesh membership is the trust boundary.
3. **Process ↔ host.** The node reads/writes a local `data_dir` and (for `_backup`) an
   operator-named destination path. The process's filesystem permissions bound the blast radius.
4. **Build ↔ supply chain.** Third-party crates + the container base image.

## Controls by boundary

### 1. REST API — opt-in bearer auth, default-deny on mutation (ADR-062)

- **Auth model.** A single bearer token (`--auth-token` / `RR_AUTH_TOKEN`) gates **every non-GET/HEAD
  request except the read-via-POST percolate endpoints** (`/_search`, `/_mpercolate`). The protected
  set is **default-deny** (`requires_auth` in `bin/server/auth.rs`): a future mutating endpoint is
  covered without anyone listing it. `--auth-protect-reads` extends the gate to everything except the
  `/_health` liveness probe.
- **Fail-loud, never fail-open.** `AuthConfig::resolve` (`auth.rs`) refuses to start on an empty,
  non-printable, or **set-but-not-UTF-8** `RR_AUTH_TOKEN` (the latter was a real fail-open bug, fixed
  in ADR-062) — the server never silently serves open when a token was intended.
- **Constant-time comparison.** `ct_eq` (`auth.rs`) compares with no data-dependent branch, so only
  the token *length* is observable. The token is never logged (failures log `reason=missing|invalid`
  + a metric, not the value).
- **Network exposure.** Both Compose and Helm **bind the REST port to loopback by default**; widening
  to a routable interface is an explicit operator step and is documented to require `RR_AUTH_TOKEN`.
- **Threats addressed:** unauthorized writes/admin (insert/delete/`_bulk`/`_flush`/`_compact`/`_backup`/
  `_settings`/vocab), credential brute-force timing, accidental open-by-default.
- **Residual:** a single shared token (no per-principal identity, no scopes); reads are open unless
  `--auth-protect-reads`. Mitigate by front-ending with a reverse proxy / mTLS gateway when
  per-principal auth is required.

### 2. Mesh gRPC — TLS + a shared cluster token (ADR-071, ADR-085)

- **Two independent, opt-in knobs**, both byte-identical to the plaintext path when unset
  (`cluster/security.rs`):
  - **TLS** (`--tls-cert`/`--tls-key` server, `--tls-ca`/`--tls-domain` client) — server
    authentication + wire confidentiality/integrity (tonic + rustls `tls-ring`).
  - **Mesh token** (`--cluster-token` / `RR_CLUSTER_TOKEN`) — one shared secret injected as
    `authorization: Bearer …` on **every** RPC by `MeshAuthInject` and verified constant-time by
    `MeshAuthVerify` *before any handler runs* (default-deny by construction — the interceptor wraps
    the whole service). Same fail-loud validation rules as the REST token (`resolve_mesh_token`).
- **Transport hardening (ADR-085).** Connect timeout + HTTP/2 keepalive + per-call deadlines, and a
  bounded fail-loud retry of **idempotent reads only** — writes never retry (non-idempotent; converge
  via the durable log + `resync`). A hung peer becomes a loud `ShardError`, never a silently dropped
  shard in the percolate union (which would be a false negative). An `https://` endpoint with no client
  TLS config is named as a misconfiguration rather than dying opaquely.
- **Threats addressed:** an unauthorized node joining the mesh, on-path eavesdropping/tampering of the
  corpus + titles, a dead/malicious peer hanging the coordinator.
- **Residual (explicit non-goals at v1):** **no mTLS** (clients are not authenticated by certificate —
  the shared token admits them) and **no per-RPC authorization tiers** (any mesh member can call any
  RPC). The trust model is "token-admitted node = full mesh peer." Run the mesh on a private network /
  service mesh; rotate the token + certs out of band. The distributed layers are **experimental /
  localhost-proven** (see [`STATUS.md`](../STATUS.md)), not yet hardened for a hostile multi-tenant
  network.

### 3. Backup write surface — authenticated, operator-named path (ADR-079)

- `POST /_backup {"dest": "<path>"}` snapshots the durable state to a **server-side** directory. It is
  **gated by the default-deny auth** (a non-GET mutation), staged into a sibling `<dest>.backup.tmp` +
  atomically renamed, **refuses a pre-existing dest**, and verifies every segment before commit
  (`storage/backup.rs`).
- **Finding — no path normalization.** The handler converts the client-supplied `dest` string straight
  to a `PathBuf` (`bin/server/handlers/backup.rs`) with no canonicalization or jail. An authenticated
  caller can therefore write a backup tree anywhere the **service account** can write (uid 10001 in the
  container image). This is **by design** — the backup location is operator-chosen at request time —
  but it is a privilege the auth token confers, so it must be treated as such.
  - **Disposition (operator responsibility, documented here):** the `_backup` endpoint is admin-grade;
    only expose it to trusted callers (it already requires `RR_AUTH_TOKEN`), keep the REST port
    loopback-bound or behind a proxy, and run the process as a **non-root** account with write access
    limited to its data + backup volumes (the shipped image already runs as uid 10001 with only `/data`
    writable). Do not run the node as root.
  - **Deferred hardening (optional):** a config-driven allowlist / jail root for `dest` (reject paths
    that escape a configured backup root). Not shipped because a hard jail would break the legitimate
    operator-chosen-destination use case; tracked as a follow-on.
- **Threats addressed:** unauthorized backup (auth-gated), silent overwrite of an existing backup
  (refused), a torn backup corrupting the source (the source is read-only during the copy; ADR-079).

### 4. Process ↔ host (the container)

- The shipped image (`deploy/Dockerfile`) runs as a **non-root** user (uid 10001) on
  `debian:trixie-slim`, carrying only the three binaries + `curl`/`ca-certificates`. The data volume is
  pre-created rr-owned so a fresh named volume is writable without granting root.
- Mesh private keys are mounted read-only; the cert generator (`deploy/gen-mesh-certs.sh`) refuses to
  clobber and the runbook flags the 0644 key-permission trade-off (bind-mount readability vs. Docker
  secrets for multi-tenant hosts).
- **Container image scan.** The image is scanned with **Trivy** (`deploy/scan-image.sh`); see
  [Container scan baseline](#container-scan-baseline) for the dated result + triage and
  [Running the scans](#running-the-scans) for the command. HIGH/CRITICAL OS-package findings are
  re-triaged on each image refresh.
- **Residual:** OS-package CVEs in the Debian base accrue between rebuilds — rebuild on a cadence; the
  binaries are not built `cargo auditable`, so the language-level dependency scan relies on
  `cargo audit`/`deny` at build time (below) rather than post-hoc binary scanning. A more minimal base
  (distroless / curl-free) would drop most of the base-image surface — a deploy-image trade-off
  (the Compose healthcheck currently shells out to `curl`).

### 5. Supply chain (dependencies)

- A **deliberately lean** dependency tree (ADR-028): the lean core is seven crates. Every full
  `check.sh` runs **`cargo audit`** (RustSec advisories, deny yanked) and **`cargo deny --all-features
  check`** (advisories + a permissive-only license allowlist + source/ban policy over the *distributed*
  graph too) — so a vulnerable or wrongly-licensed transitive dep fails the gate, in CI on every PR.
  `deny.toml` is the policy. (Precedent: the `prometheus` protobuf feature is disabled, and a memmap2
  RUSTSEC advisory was caught + bumped by this lane.)
- **Threats addressed:** a known-vulnerable crate shipping; a copyleft/unknown-source dep slipping in.
- **Residual:** advisory-DB coverage is not exhaustive; pin + review dependency bumps (the distributed
  stack is pinned exact).

## Adversary model

- **In scope:** an unauthenticated network client hitting the REST API; an on-path attacker on an
  untrusted segment between mesh nodes; a misconfigured/compromised client with the REST token trying to
  write outside the data dir; a malicious/hung mesh peer; a known-vulnerable dependency or base-image
  CVE.
- **Out of scope at v1 (documented non-goals):** a hostile node *inside* the mesh (token-admitted ⇒
  full peer — no per-RPC authz, no mTLS); a multi-tenant host where the service account is shared;
  side-channel / physical attacks; power-loss durability beyond `wal_sync_on_write=true` (page-cache
  loss is a durability concern, covered by the WAL fsync policy + the torn-tail tests, not a security
  control); secrets management beyond "supply tokens/keys via env/files/Secrets" (no built-in vault
  integration or rotation automation).

## Container scan baseline

Trivy `image` scan of `reverse-rusty:latest` (base `debian:trixie-slim`, 13.5), **2026-06-25**:

> **Total: 234 (CRITICAL: 2, HIGH: 14, MEDIUM: 58, LOW: 97, UNKNOWN: 63)** — all in **OS packages**
> of the Debian base; zero in the three Rust binaries.

**Triage:** the two CRITICAL (and most of the HIGH) are in **`perl-base`** (perl-Archive-Tar path
traversal `CVE-2026-42496`, perl-IO-Compress RCE `CVE-2026-48962`) plus `curl` (SMB wrong-transfer
`CVE-2026-5773`), `util-linux` (mount TOCTOU), and `bzip2`/`glibc` — and are predominantly Debian
`fix_deferred` / `affected` (no upstream fix released yet). **None are reachable by the service:** the
matcher is a Rust gRPC/HTTP binary that never runs perl, extracts tar archives, performs SMB transfers,
or mounts loop devices; `curl` is present only for the container healthcheck (not an SMB client). So
these are **base-image hygiene**, not an exploitable path in the running matcher.

**Disposition:** track on a rebuild cadence (a rebuild picks up Debian's fixes as they land); re-run
`deploy/scan-image.sh` on each image refresh. The structural reduction — a distroless / static, or at
least `curl`-free, base image — is recorded as a deploy-image follow-on (it would drop perl + most of
the base surface; the trade-off is the Compose healthcheck's `curl`). The application's own dependency
surface is covered separately by `cargo audit` + `cargo deny` in the gate (clean as of this date).

## Running the scans

```bash
# Dependency advisories + license/source policy (also part of the full check.sh gate):
cd engine && cargo audit && cargo deny --all-features check

# Container image scan (Trivy via its official image; needs the built image):
docker build -f deploy/Dockerfile -t reverse-rusty:latest .
deploy/scan-image.sh reverse-rusty:latest          # HIGH/CRITICAL, fails non-zero on findings
deploy/scan-image.sh reverse-rusty:latest --full   # full table, all severities
```

## Operator security checklist

1. Set a strong `RR_AUTH_TOKEN` (`openssl rand -hex 32`); the server refuses to start on an empty one.
2. Keep the REST port loopback-bound (the default) or behind an authenticating reverse proxy; only
   widen with a token set.
3. On any non-trusted network, enable the mesh **TLS + token** (both knobs) for every node; rotate the
   secret + certs out of band.
4. Run the node as **non-root** with write access limited to its data + backup volumes; never as root.
5. Treat `_backup` as admin-grade — trusted callers only, and a non-root account so its arbitrary
   `dest` is bounded.
6. Rebuild + re-scan the image on a cadence; keep `cargo audit`/`deny` green (the gate enforces it).
