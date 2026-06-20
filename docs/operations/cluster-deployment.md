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

```sh
# from the repo root — --project-directory . makes relative paths (RR_CERT_DIR) repo-root-relative
docker compose --project-directory . -f deploy/compose.cluster.yml \
  --env-file deploy/cluster.env up -d --wait
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

**The remote topology** (this compose — shards on separate nodes) scales by **redeploy**, not online
resize: add a `shardN` service + a matching `--shard-endpoint` on the coordinator + the new SAN, then
re-place data by re-ingesting the corpus into the resized coordinator. The compose carries the exact
template inline. Cross-process / online resize is a deferred follow-on (ADR-078, ADR-081 §Deferred).

## 6. Recovery

The cluster is shard-authoritative and **fails loud** — a degraded read returns `502`, never a silently
short result (ADR-072).

| Event | What happens | Action |
|---|---|---|
| **A shard crashes/restarts** | Durable self-restore from its `--data-dir` (segments + translog, ADR-039); reads that route to it return `502` until it's back. | `docker compose restart shardN` (or let `unless-stopped` do it). Matches resume automatically. |
| **Rolling shard restart** | One shard at a time; the others keep serving (reads to the down shard fail loud meanwhile). | Restart shards sequentially; wait for `/_health` green between each. |
| **Coordinator restart** | Stateless: reconnects to the same endpoints, re-mints + re-ships the dict, re-derives placement. No data loss. | `docker compose restart coordinator`; wait for green. |
| **Control-plane restart** | Each node resumes from its durable Raft log/vote (ADR-041). | Restart control nodes; quorum re-forms. (Idle at v1 — no data-path impact.) |
| **Replica failover / peer recovery** (RF>1) | Reads fail over to an in-sync replica; a fresh replica rebuilds from its primary over `FetchSegments`/`RecoverFrom` without quiescing writes (ADR-035/036). | Bring up the replacement node; it catches up automatically. |

The lifecycle invariants above are exercised end-to-end by the multi-machine harness
([`deploy/harness.sh`](../../deploy/harness.sh), ADR-072): kill-and-recover, rolling restart, coordinator
restart, live handoff under load, control-plane restart.

## 7. Backup & restore

Backup depends on topology — the durable state lives in different places:

- **In-process `--data-dir` cluster** (coordinator owns the data dir): use the engine-driven
  `POST /_backup` — a consistent, self-contained snapshot. Full procedure:
  [backup-restore.md](backup-restore.md).
- **Remote topology** (this compose — the coordinator is stateless): `POST /_backup` does **not** apply;
  the durable state is the per-node `--data-dir` volumes (`shardN-data`, `controlN-data`). Back them up
  at the filesystem layer — `POST /_checkpoint` to commit a consistent on-disk state, then snapshot each
  volume (ZFS/LVM/EBS/GCP disk snapshot — the zero-write-stall path in
  [backup-restore.md](backup-restore.md#zero-write-stall-backups-large-deployments)). Restore = mount the
  snapshots back into the same `--data-dir` volumes and restart the shard nodes.

## 8. Vocabulary redeploy

**Vocabulary is deploy-time configuration on a remote cluster, not a live operation** (ADR-076). A remote
cluster *refuses* a live `set_vocab`, and the coordinator *fails startup* if given a `--vocab-file`
against remote shards — by design: the `shardserver` runs the stock normalizer, and there is no
cross-process normalizer shipping in v1. To change vocabulary, **redeploy blue/green**:

1. Stand up a **parallel** cluster (a second compose project, e.g. `-p rr-green`, on its own ports/
   volumes) whose corpus is rebuilt under the new vocabulary. For an **in-process** `--data-dir`
   coordinator, pass the new `--vocab-file` at build; for the **remote** topology, build the new vocab
   into the data by re-ingesting the corpus into the green coordinator.
2. Validate the green cluster (percolate your golden titles).
3. Cut traffic over — swap the published port or the reverse-proxy upstream from blue to green.
4. Decommission blue.

This is the deployment-level realization of ADR-076's "vocab is deploy-time" decision. Background:
[research/dynamic-vocabulary.md](../research/dynamic-vocabulary.md).

## 9. Monitoring & observability

`GET /_metrics` exposes Prometheus text with the `reverse_rusty_` prefix. Scrape the coordinator (and
each shard server, which exposes its own engine metrics). High-signal alerts:

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
- [ ] **`RR_AUTH_TOKEN` set** before the REST port is reachable off-box (ADR-062). The server warns at
      startup if it binds beyond loopback without it.
- [ ] **REST port loopback-bound** (`RR_PORT=127.0.0.1:9200`) unless behind a firewall/authenticating
      proxy. Widen to `0.0.0.0:9200` only with the bearer token set.
- [ ] **Cert SANs cover every service name**; rotate by re-running `gen-mesh-certs.sh` (remove the old
      certs first) and redeploying every node together.

## 11. Not covered in v1 (see ADR-081)

- **Control-plane↔coordinator wiring** — the coordinator runs an in-memory control plane; the durable
  `controlserver` quorum is present but not yet consulted for routing (the follow-on is a
  `--control-endpoint` flag attaching the coordinator's `ControlPlane` to a `RaftControlPlane` client).
- **Kubernetes manifests / Helm** — deferred; the deployment unit is Compose at v1. The shape is sketched
  in ADR-081 (StatefulSets for shards/control, a Deployment for the stateless coordinator).
- **Online / cross-process resize** — `/_cluster/resize` is in-process only; the remote topology scales
  by redeploy ([§5](#5-scaling)).
- **Live remote vocabulary shipping** — vocab is deploy-time on a remote cluster ([§8](#8-vocabulary-redeploy)).
