# Cluster deployment & operations

Operational runbook for standing up and running a multi-node Reverse Rusty cluster from the container
image. Design rationale: [ADR-081](../decisions/adr-081-deployment-packaging-runbook.md). Backing up a
running cluster has its own doc: [backup-restore.md](backup-restore.md).

> **TL;DR** — [`deploy/compose.cluster.yml`](../../deploy/compose.cluster.yml) brings up a K-shard +
> control-plane cluster from one image ([`deploy/Dockerfile`](../../deploy/Dockerfile)). Bring-up order
> is **control plane → shards → coordinator**. Mint the mesh identity with
> [`deploy/gen-mesh-certs.sh`](../../deploy/gen-mesh-certs.sh), set `RR_CLUSTER_TOKEN` + `RR_AUTH_TOKEN`,
> and keep the REST port loopback-bound until both are in place.

> **At v1 the control-plane quorum is durable but not yet load-bearing.** A remote coordinator runs its
> own *in-memory* control plane and re-derives shard placement deterministically from the frozen dict +
> ring on every start; it does not yet consult the `controlserver` quorum (no wiring flag exists —
> [ADR-072](../decisions/adr-072-multi-machine-harness.md) §Scope, ADR-081). Ship the quorum so the
> durable placement tier is production-shaped, or omit `control0..2` at v1 with no behavioral change.

## 1. Topology

One image, three roles chosen by command (ADR-072):

| Role | Binary | Port | Durable? | Notes |
|---|---|---|---|---|
| **Coordinator** | `server --cluster` | 9200 (HTTP) | No — **stateless** | The REST API over the cluster (ADR-070). In remote mode it holds no data; durability lives on the shards. A restart reconnects and re-ships the re-minted dict. |
| **Shard** | `shardserver` | 50051 (gRPC) | Yes (`--data-dir`) | A data node. `--pending` starts dict-less and adopts the coordinator's dict at connect. |
| **Control** | `controlserver` | 50061 (gRPC) | Yes (`--data-dir`) | The openraft placement-state quorum (ADR-038/041). Present-but-idle at v1 (see above). |

The shipped compose is **K=3 shards, RF=1**. Scaling K and RF: [§5](#5-scaling).

## 2. Prerequisites

### 2.1 Build the image

```sh
docker build -f deploy/Dockerfile -t reverse-rusty:latest .   # from the repo root
```

### 2.2 Mint the mesh identity (TLS)

The mesh links (coordinator↔shard, control↔control) use TLS (ADR-071). `gen-mesh-certs.sh` writes one
self-signed EC cert whose SANs cover **every service DNS name**, served by every node and trusted as the
CA by every client:

```sh
deploy/gen-mesh-certs.sh                 # ./deploy/certs, SANs = the compose service names
deploy/gen-mesh-certs.sh /etc/rr/certs shard0 shard1 shard2 coordinator control0 control1 control2
```

- The SAN list **must** cover every name a client dials. The coordinator dials `https://shardN:50051`,
  so the cert needs `DNS:shardN`; add a SAN when you add a shard. A missing SAN fails the TLS handshake
  loud, never silently.
- `CA:FALSE` is load-bearing — webpki rejects a CA-marked cert presented as the end-entity. The helper
  sets it; don't hand-roll a cert without it.
- This is a **bootstrap** identity (one shared cert). For stronger isolation, issue per-node certs from
  a real CA and point `--tls-ca` at that CA bundle — the bins accept any CA the same way.

### 2.3 The two tokens

Two independent secrets gate two different audiences — set both:

| Variable | Audience | Gates | Decision |
|---|---|---|---|
| `RR_CLUSTER_TOKEN` | the node **mesh** (gRPC) | every coordinator↔shard / control RPC | ADR-071 |
| `RR_AUTH_TOKEN` | the **REST** client | mutating/admin HTTP endpoints (`_doc`, `_bulk`, `_flush`, `_compact`, `_vocab`, `_settings`, `_backup`) | ADR-062 |

Both are read from the **environment** (never passed as flags, which leak in process listings).
Generate strong values: `openssl rand -hex 32`. Mesh TLS + token are **opt-in** — enable both on any
network you don't fully trust.

### 2.4 The env file

```sh
cp deploy/cluster.env.example deploy/cluster.env
$EDITOR deploy/cluster.env          # fill in RR_CLUSTER_TOKEN and RR_AUTH_TOKEN
```

## 3. Bootstrap & startup ordering

Every `docker compose` command below needs the same `--project-directory`, `-f`, and `--env-file`
arguments (run from the repo root, so `RR_CERT_DIR=./deploy/certs` resolves correctly). Define a wrapper
once per shell and use it throughout:

```sh
rrc() { docker compose --project-directory . -f deploy/compose.cluster.yml \
          --env-file deploy/cluster.env "$@"; }

rrc up -d --wait          # start the cluster; --wait blocks until every service is healthy
```

`--wait` blocks until every service is healthy. The dependency order the compose encodes:

1. **Control plane** comes up first; node 0 (`--bootstrap`) forms the cluster from its `--peer` members.
2. **Shards** start in `--pending` mode and open their gRPC listener immediately (the listener is up
   before the node has a dict — that is when the coordinator may begin dialing).
3. **Coordinator** waits on `depends_on: service_healthy` for every shard, then connects, ships its
   frozen dict (`AdoptDict`), and only then serves `/_health` green.

**The connect race.** If the coordinator ever dials before a shard's first listen, the connect fails and
the coordinator exits non-zero; `restart: unless-stopped` brings it straight back (the shard healthcheck
makes this rare). This is why a cold start can show one coordinator restart in the logs — expected, not a
fault.

Load data after the cluster is green — there is no baked corpus (unlike the test harness):

```sh
curl -fsS -XPUT http://127.0.0.1:9200/_doc/1 -H "authorization: Bearer $RR_AUTH_TOKEN" \
  -H 'content-type: application/json' -d '{"query":"1990 topps griffey"}'
# bulk: POST /_bulk (newline-delimited) — see docs/reference/api.md
```

**Negation-only (class-D) queries.** To accept queries that are purely exclusions (e.g. `-reprint` —
"match any title *without* reprint"), add `--accept-class-d` to the **coordinator** command (the
`server …` service). The coordinator is the sole gate: a remote shard is coordinator-gated storage and
accepts whatever the coordinator places, so there is no per-shard flag to set. Like every broad-lane
query, an always-candidate is returned only when the request includes the broad lane (`include_broad`).

## 4. Health & readiness

| Check | Endpoint | Meaning |
|---|---|---|
| Liveness/readiness | `GET /_health` | `green` = all shards reachable; `red`/`503` = a shard is unreachable (the cluster fails loud, never silently truncates). |
| Corpus + segments | `GET /_stats` | `total_queries`, per-shard counts. |
| Per-shard view | `GET /_cat/shards` | shard → state. |

`GET /_health` is the only endpoint that never requires the bearer token, so container/orchestrator
probes work without credentials.

## 5. Scaling

**The in-process `--data-dir` cluster** (a single box running K shards in one coordinator process) can
resize live:

```sh
curl -fsS -XPOST http://127.0.0.1:9200/_cluster/resize -H "authorization: Bearer $RR_AUTH_TOKEN" \
  -H 'content-type: application/json' -d '{"num_shards": 12}'
```

This is an in-process blue/green rebuild under a fresh ring (ADR-078) — correct and durable, but
**in-process only**. `recommended_shard_count` (the autoscaler's load-based advisory) is a library/auto
driver concept, not a REST knob; pick `num_shards` yourself, optionally guided by `/_stats`.

**The remote topology** (this compose — shards on separate nodes) has **no online resize**: changing K
re-keys the ring, and a coordinator restarted at the new K routes on the new ring while the existing data
is still placed under the old one — searches in that window silently miss queries. So scale by
**blue/green**, never in place:

1. Stand up a **separate** green cluster at the new K (new project name + volumes + a `--shard-endpoint`
   per new shard + SANs).
2. Re-ingest the full corpus into the green coordinator and validate it.
3. Cut traffic over (swap the published port / proxy upstream), then decommission blue.

Do **not** add a shard to the live cluster and re-ingest in place. Cross-process / online resize is a
deferred follow-on (ADR-078, ADR-081 §Deferred).

## 6. Recovery

The cluster is shard-authoritative and **fails loud** — a degraded read returns `502`, never a silently
short result (ADR-072).

| Event | What happens | Action |
|---|---|---|
| **A shard crashes/restarts** | Durable self-restore from its `--data-dir` (segments + translog, ADR-039); reads that route to it return `502` until it's back. | `rrc restart shardN` (or let `unless-stopped` do it). Matches resume automatically. |
| **Rolling shard restart** | One shard at a time; the others keep serving (reads to the down shard fail loud meanwhile). | `rrc restart shardN` sequentially; wait for `/_health` green between each. |
| **Coordinator restart** | Stateless: reconnects to the same endpoints, re-mints + re-ships the dict, re-derives placement. No data loss. | `rrc restart coordinator`; wait for green. |
| **Control-plane restart** | Each node resumes from its durable Raft log/vote (ADR-041). | Restart control nodes; quorum re-forms. (Idle at v1 — no data-path impact.) |
| **Replica failover** (RF>1) | Reads fail over to an in-sync replica; the primary stays authoritative for writes (ADR-035). | None — automatic. |
| **Replica replacement** (RF>1) | A replacement reusing the **same durable volume** self-restores from its own segments + translog. A **fresh-volume** replica simply listed in the endpoint group is assembled as *in-sync without recovery* — reads could then serve it empty (silent FN). | Prefer same-volume restart. A fresh replica must complete an explicit peer recovery (`RecoverFrom`, ADR-036) **before** it serves reads — not a plain "start it"; treat fresh-volume replica replacement as a care-needed v1 operation. |

The lifecycle invariants above are exercised end-to-end by the multi-machine harness
([`deploy/harness.sh`](../../deploy/harness.sh), ADR-072): kill-and-recover, rolling restart, coordinator
restart, live handoff under load, control-plane restart.

## 7. Backup & restore

Backup depends on topology, because the durable state lives in different places:

- **In-process `--data-dir` cluster** (one `server --cluster --data-dir` process owns the data): use the
  engine-driven `POST /_backup` — a consistent, self-contained snapshot taken under the engine's write
  lock. This is the path with a real consistency barrier. Full procedure:
  [backup-restore.md](backup-restore.md).
- **Remote topology** (this compose — the coordinator is **stateless**): `POST /_backup` and
  `POST /_checkpoint` do **not** seal the remote shards — they no-op on a coordinator that has no
  `data_dir`. The durable state is each node's own `--data-dir` volume (`shardN-data`, `controlN-data`),
  fsync'd by that node per its WAL policy. **There is no coordinator-driven cross-shard consistency
  barrier in v1**, so for a globally consistent backup you must **quiesce writes**:
  1. Stop the ingest source (no `_doc`/`_bulk` writes in flight).
  2. Snapshot each `shardN-data` and `controlN-data` volume at the filesystem layer (ZFS/LVM/EBS/GCP disk
     snapshot — instantaneous; see [backup-restore.md](backup-restore.md#zero-write-stall-backups-large-deployments)).
  3. Resume writes.

  Restore = mount the snapshots back into the same `--data-dir` volumes and restart the nodes; each shard
  recovers its own segments + translog. Without quiescence, each shard's snapshot is still individually
  crash-consistent (its translog replays on restart), but shards may capture slightly different points in
  time — acceptable only if your ingest can re-drive recent writes. For an engine-driven consistent backup
  *without* quiescing, use the in-process `--data-dir` cluster above.

## 8. Vocabulary

**A custom vocabulary is not supported on the remote topology in v1** (ADR-076). There is no cross-process
normalizer shipping: each `shardserver` always builds `Normalizer::default_vocab()` (`AdoptDict` ships the
frozen *dict*, not the normalizer), and the coordinator *fails startup* if given a `--vocab-file` against
remote shards — precisely to avoid a coordinator/shard normalizer mismatch. So **a remote cluster runs the
default vocabulary on every node, full stop.** A live `set_vocab` is likewise refused.

To run — or change — a **custom** vocabulary, use the **in-process `--data-dir` cluster**
(`server --cluster --data-dir … --vocab-file vocab.json --shards K`, no `--shard-endpoint`), where the
coordinator owns the in-process shards' normalizer. Change it **blue/green**: stand up a parallel
in-process cluster built with the new `--vocab-file`, validate (percolate your golden titles), cut traffic
over (swap the published port / proxy upstream), decommission the old one.

This is the deployment-level realization of ADR-076's "vocab is deploy-time" decision. Background:
[research/dynamic-vocabulary.md](../research/dynamic-vocabulary.md).

## 9. Monitoring & observability

`GET /_metrics` on the **coordinator** exposes Prometheus text with the `reverse_rusty_` prefix. (The
`shardserver` is gRPC-only — it has **no HTTP `/_metrics`** in v1, so shard-local engine metrics aren't
scrapable; watch shard liveness via the coordinator's `/_health`, which fails loud on an unreachable
shard, and the shard containers' structured logs.) High-signal coordinator alerts:

| Metric | Alert when |
|---|---|
| `reverse_rusty_durability_failures_total` | `> 0` — a write could not be made durable; investigate disk/space before it compounds. |
| `reverse_rusty_auth_failures_total` | rising — rejected bearer tokens (misconfig or probing). |
| `reverse_rusty_slow_queries_total` | rising — searches past `--slow-query-threshold-ms`. |

Logs are structured (`--log-format json` for machine parsing); a degraded-shard `502` is logged with the
unreachable endpoint.

## 10. Security checklist

- [ ] **Mesh TLS + token on** (`--tls-*` + `RR_CLUSTER_TOKEN`) — opt-in (ADR-071); enable on any
      untrusted network. Plaintext mesh is for a single trusted host only.
- [ ] **`RR_AUTH_TOKEN` set** (ADR-062) — required to start; the coordinator refuses an empty token. To
      run without REST auth on a trusted single host, delete the `RR_AUTH_TOKEN` line from the coordinator
      service (an absent var disables auth; an empty one is rejected, never read as "off").
- [ ] **REST port loopback-bound** (`RR_PORT=127.0.0.1:9200`) unless behind a firewall/authenticating
      proxy. Widen to `0.0.0.0:9200` only with the bearer token set.
- [ ] **Mesh private key not world-readable on a shared host** — `gen-mesh-certs.sh` writes `node.key`
      0644 so the container user (uid 10001) can read it through the bind mount; on a multi-tenant host
      use Docker secrets (mounted 0400 to the container user) instead of bind-mounting the key.
- [ ] **Cert SANs cover every service name**; rotate by re-running `gen-mesh-certs.sh` (remove the old
      certs first) and redeploying every node together.

## 11. Not covered in v1 (see ADR-081)

- **Control-plane↔coordinator wiring (ADR-083/086) — wired by default.** Pass `--control-endpoint <URL>`
  (repeatable — list **all** quorum members) to attach the coordinator's cluster-state control plane to
  the durable `controlserver` quorum as a **thin client** (it does NOT join consensus — stays stateless).
  The client tries the endpoints in order and follows a follower's `ForwardToLeader`, **failing over**
  across the list (ADR-086) if a member is down; all-down fails loud. Failover covers idempotent
  **reads** (routing decisions stay available); admin **writes** are not resubmitted on failover (a
  committed-but-lost write must not double-apply), so while a coordinator's primary control node is down
  an admin write fails loud until the coordinator restarts onto a live endpoint. Add `--route-by-assignments`
  (ADR-086) to make the committed shard→node assignments the **topology source of truth**: the coordinator
  seeds the quorum position-preservingly from its `--shard-endpoint` list on first boot, then resolves its
  shard topology from the durable document (so a coordinator can boot without `--shard-endpoint`, sizing
  the ring from `--shards` and re-minting its dict from `--load-file`); a
  fail-loud guard refuses a committed map that is not position-preserving. Absent both flags, the in-memory
  backend is used (byte-identical). The bootstrap control node must advertise a routable self-URL via
  `--advertise-url` (ADR-082), committed at the *first* bootstrap only (Raft `initialize` is idempotent —
  an existing deployment whose quorum already bootstrapped a wildcard URL resets its idle `controlN-data`
  volumes to adopt the new one). **Still deferred (ADR-086):** *data-moving* reassignment — a committed
  assignment change does NOT yet move data + re-point routing LIVE while the coordinator runs, so a
  non-data-moving HRW `rebalance` must **not** be used to re-point routing on a populated cluster (that
  needs live handoff). The default `compose.cluster.yml` now wires `--control-endpoint` +
  `--route-by-assignments`.
- **Kubernetes / Helm** — shipped (ADR-084): `deploy/helm/reverse-rusty/` (shard + control StatefulSets, a
  stateless coordinator Deployment wiring `--control-endpoint` + `--route-by-assignments`, native gRPC
  health probes). Compose remains the simplest single-host unit; Helm is the k8s analogue.
- **Online / cross-process resize** — `/_cluster/resize` is in-process only; the remote topology scales
  by redeploy ([§5](#5-scaling)).
- **Custom vocabulary on the remote topology** — unsupported; remote shards run the default normalizer.
  Custom vocab is an in-process `--data-dir` cluster capability ([§8](#8-vocabulary)).
