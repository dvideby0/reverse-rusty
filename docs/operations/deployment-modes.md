# Deployment modes — the supported matrix

The canonical "known-supported deployment" contract (roadmap Tier 5 M0, ADR-098): the four
supported modes, the exact bring-up command for each, the REST surface they all guarantee, the
auth posture, and — in one place — the **v1 non-goals** as named constraints. This page is the
*contract*; the fresh-clone *acceptance recipe* that proves a checkout is green stays
[`build-and-smoke.md`](build-and-smoke.md), and the per-mode *operational runbooks* stay
[`cluster-deployment.md`](cluster-deployment.md) (Compose) and
[`kubernetes-deployment.md`](kubernetes-deployment.md) (Helm).

> TL;DR: four supported modes. The two **local** modes are CI-gated on every PR
> (`deploy/local-smoke.sh` runs inside the required `gate + benchmarks` job); the two **remote**
> modes are smoke-gated by their own scripts (`cluster-smoke.sh`, `k8s-smoke.sh`) and the
> lifecycle harness. "Deployable feature complete" here is deliberately distinct from the scale
> proof — the ≥20M-query soak stays open as Tier 3 criterion 12 (ADR-065).

## 1. The matrix

| Mode | Build / image | Bring-up (exact command) | Durability | Proven by |
|---|---|---|---|---|
| **Single-node** | `cd engine && cargo build --release` | `server --port 9200 --data-dir ./data` | WAL + segments; restart-reopen; `POST /_backup` → restore by `--data-dir` | [`local-smoke.sh`](../../deploy/local-smoke.sh) — **CI, every PR** |
| **In-process cluster** | same binary | `server --cluster --shards K --data-dir ./data` | coordinator log + manifest + per-shard segments; checkpoint + reopen; `POST /_backup` | [`local-smoke.sh`](../../deploy/local-smoke.sh) — **CI, every PR** |
| **Remote Compose** (K=3, RF=1) | `deploy/Dockerfile` (`--features distributed`) | `deploy/gen-mesh-certs.sh` + env (`RR_CLUSTER_TOKEN`, `RR_AUTH_TOKEN`) + `docker compose -f deploy/compose.cluster.yml up -d` — full procedure: [runbook §2](cluster-deployment.md) | per-shard translog + segments on named volumes; durable Raft control plane | [`cluster-smoke.sh`](../../deploy/cluster-smoke.sh) + the 6-leg [`harness.sh`](../../deploy/harness.sh) — **harness in CI, every PR** |
| **Remote Helm** (K=3, RF=1) | same image | Secrets (TLS + tokens) + `helm install rr deploy/helm/reverse-rusty` — full procedure: [runbook §3](kubernetes-deployment.md) | per-pod PVCs (shards + control); stateless coordinator Deployment | [`k8s-smoke.sh`](../../deploy/k8s-smoke.sh) (kind) + `helm lint`/`kubeconform` — **static validation in CI, every PR** |

The two local modes need only the Rust toolchain, `curl`, and `jq`. The two remote modes are the
same topology expressed twice (Compose for a single host, Helm for Kubernetes) — one image, three
binaries, role chosen by command.

## 2. The supported REST surface (the M1 contract)

Every mode serves, and `local-smoke.sh` asserts end-to-end on every PR:

```
PUT/GET/DELETE /_doc/{id}      (ids are numeric u64 — a non-numeric id is a 400)
POST /_bulk                    (NDJSON, ES-shaped)
POST /_search                  (single-document percolation; `include_broad` per request)
POST /_mpercolate              (batch percolation, ES _msearch-shaped responses[])
GET  /_health   /_stats   /_metrics   (unauthenticated reads; Prometheus text on /_metrics)
POST /_backup {"dest": ...}    (server-side snapshot; restore = open the copy via --data-dir)
+ restart-reopen               (every acknowledged write survives an operational restart)
```

Per-mode extras (single-node `/_settings`, `/_vocab*`; cluster `/_cluster/*` ops, `/_cat/shards`)
are in the API reference — [`../reference/api.md`](../reference/api.md). Endpoints that exist in
only one mode return **501 with the supported alternative** in the other, never a silent no-op.

**A micro-corpus classification note** (visible in any tiny demo, asserted in the smoke): cost
classification is frequency-based, and a *bulk* batch finalizes the 64-bit common mask from its
own corpus — so on a corpus of a handful of queries every term is "hot" and a bulk-ingested
any-of query can land in the quarantined broad lane (class C, served only with
`include_broad: true`). This is by design (frequency data is degenerate at that size; see
[`../design/matching.md`](../design/matching.md) §4); realistic corpora classify normally.

## 3. Auth posture (ADR-062 / ADR-071)

- **REST**: with `RR_AUTH_TOKEN` set, every mutating/admin endpoint requires the bearer
  (default-deny on non-GET/HEAD except `/_search` and `/_mpercolate`); reads stay open. An
  **empty** token refuses startup (never read as "off"); an **absent** token disables the gate —
  the server logs a loud warning if you bind a non-loopback interface that way. Default bind is
  loopback.
- **Mesh** (remote modes): gRPC TLS + `RR_CLUSTER_TOKEN` admission are **opt-in** (ADR-071) —
  enable both outside a trusted network; the shipped Compose/Helm topologies enable them by
  default. Details + operator checklist: [`threat-model.md`](threat-model.md).

## 4. v1 non-goals — the named constraints

Each is a deliberate, recorded decision — not an oversight. If your deployment needs one, the ADR
is where the trade-off and the follow-on path live.

| Constraint | Operational meaning | Decided in |
|---|---|---|
| **RF>1 in the Helm chart** | the chart models RF=1; `replicationFactor` is documentation-only (the *engine's* replication is built — Compose can run RF=2 by hand, [runbook §5](cluster-deployment.md)) | [ADR-084](../decisions/adr-084-kubernetes-helm-health.md); roadmap M4 |
| **Online / cross-process resize** | `/_cluster/resize` is in-process blue/green only; the remote topology changes shard count by redeploy | [ADR-078](../decisions/adr-078-cluster-resize.md) |
| **Custom vocabulary on the remote topology** | vocab is deploy-time configuration for remote clusters (live `set_vocab` is an in-process capability) | [ADR-076](../decisions/adr-076-cluster-multiword-aliases-vocab-shipping.md) |
| **Cross-shard backup barrier** | a remote (stateless-coordinator) cluster has per-shard-consistent backups, no global barrier; consistent whole-cluster backup requires quiescence | [ADR-079](../decisions/adr-079-backup-restore.md); [backup-restore.md](backup-restore.md) |
| **Scale proof at target (≥20M)** | deployable ≠ scale-proven: the largest soak to date is 10M single-node; the multi-shard 20M+ proof + real-corpus audit stay open | [ADR-065](../decisions/adr-065-distributed-v1-graduation.md) criterion 12 |
| **mTLS / per-RPC authz** | the mesh uses one shared token + server TLS; mutual TLS and per-RPC authorization are post-v1 | [ADR-071](../decisions/adr-071-grpc-tls-auth.md); [threat-model.md](threat-model.md) |
| **Power-loss durability by default** | `wal_sync_on_write` defaults **false**: an acked write survives a process crash (WAL replay), not necessarily power loss — flip the knob for fsync-per-write | [ADR-013](../decisions/adr-013-write-ahead-log.md); [ADR-088](../decisions/adr-088-crash-injection-harness.md) |

## 5. Choosing a mode

Start **single-node** — one binary, one data dir, the full API. Move to the **in-process cluster**
when one snapshot's match latency needs sharding but one machine still fits the corpus (same
binary, one flag). Go **remote** (Compose first, Helm when you already run Kubernetes) when you
need node-level fault isolation, replicas, or more machines — accepting the named constraints
above.
