# Kubernetes deployment (Helm)

Operational runbook for the Helm chart [`deploy/helm/reverse-rusty`](../../deploy/helm/reverse-rusty/).
Design rationale: [ADR-084](../decisions/adr-084-kubernetes-helm-health.md). This is the Kubernetes
analogue of the Docker Compose topology — for the conceptual model (roles, bootstrap order, the
shared mesh identity, the v1 limits) read [cluster-deployment.md](cluster-deployment.md) first; this
doc covers only what is k8s-specific. Backup/restore: [backup-restore.md](backup-restore.md).

> **TL;DR** — `StatefulSet`s for shards + control (stable DNS + per-pod PVC), a `Deployment` for the
> stateless coordinator, native **gRPC health probes** (ADR-084). Provide three things before
> `helm install`: the container image, a **mesh TLS Secret whose SANs cover the pod FQDNs**, and the
> cluster + REST token Secrets. Then `helm install rr deploy/helm/reverse-rusty -n rr`.

## 1. Prerequisites

- The image (`deploy/Dockerfile`) pushed to a registry your cluster can pull; set `image.repository`
  / `image.tag`.
- A default `StorageClass` (or set `persistence.storageClass`) — every shard + control pod gets a PVC.
- A Kubernetes version with the built-in `grpc` probe GA (**1.27+**).
- The three Secrets in §2.

## 2. Secrets (operator-provided)

The chart **references** Secrets by name; it never templates the mesh TLS (too sensitive), and only
templates the tokens when `*.create=true` (dev/CI only — the value lands in the release manifest).

**Mesh TLS** (`tls.secretName`, default `reverse-rusty-mesh-tls`) — a `kubernetes.io/tls` Secret with
an extra `ca.crt` key. **The cert SANs must cover every pod FQDN**, because every mesh dial verifies
the peer cert against the DNS name it dialed. For the default release name `rr` in namespace `rr`,
with `shardCount=3` + 3 control nodes, the SANs are:

```
rr-reverse-rusty-shard-0.rr-reverse-rusty-shard.rr.svc.cluster.local
rr-reverse-rusty-shard-1.rr-reverse-rusty-shard.rr.svc.cluster.local
rr-reverse-rusty-shard-2.rr-reverse-rusty-shard.rr.svc.cluster.local
rr-reverse-rusty-control-0.rr-reverse-rusty-control.rr.svc.cluster.local
rr-reverse-rusty-control-1.rr-reverse-rusty-control.rr.svc.cluster.local
rr-reverse-rusty-control-2.rr-reverse-rusty-control.rr.svc.cluster.local
```

Easiest path is **cert-manager** with a wildcard `*.rr-reverse-rusty-shard.rr.svc.cluster.local` +
`*.rr-reverse-rusty-control.rr.svc.cluster.local` (and the cluster-internal CA as issuer). For a
quick test you can adapt [`deploy/gen-mesh-certs.sh`](../../deploy/gen-mesh-certs.sh) to emit those
SANs and create the Secret with `kubectl create secret tls … --cert … --key …` plus `--from-file`
for `ca.crt`. (Disable TLS only for a throwaway cluster: `--set tls.enabled=false` — the mesh token
then crosses the wire in cleartext.)

**Cluster token** (`clusterToken.secretName`, key `RR_CLUSTER_TOKEN`) and **REST auth token**
(`auth.secretName`, key `RR_AUTH_TOKEN`):

```sh
kubectl -n rr create secret generic reverse-rusty-cluster-token --from-literal=RR_CLUSTER_TOKEN="$(openssl rand -hex 32)"
kubectl -n rr create secret generic reverse-rusty-auth-token    --from-literal=RR_AUTH_TOKEN="$(openssl rand -hex 32)"
```

## 3. Install

```sh
helm install rr deploy/helm/reverse-rusty -n rr --create-namespace \
  --set image.repository=YOUR_REGISTRY/reverse-rusty --set image.tag=0.1.0
```

Bring-up is automatic and order-independent: control pods elect a leader; shards start **pending**
(dict-less, live-but-not-ready); the coordinator's `wait-for-mesh` initContainer blocks until every
shard's gRPC port is open and (when `controlPlane.wireToCoordinator`) the control quorum is reachable,
then connects and ships the frozen dict via `AdoptDict` — which flips each shard's readiness to
SERVING. A `Ready` coordinator pod means the cluster is serving. Load data over REST (`/_bulk`,
`/_doc`); see [api.md](../reference/api.md).

## 4. Health & monitoring (ADR-084)

Each shard + control pod serves the standard `grpc.health.v1.Health` service on a **separate
plaintext** port (`ports.shardHealth` / `ports.controlHealth`) — the chart wires native probes:

| Probe | Service key | SERVING means |
|---|---|---|
| livenessProbe (shard/control) | `""` | the gRPC server is up |
| readinessProbe (shard) | `ready` | a dict has been adopted (a pending shard is **not** ready) |
| readinessProbe (control) | `ready` | this node currently sees an elected leader |
| liveness+readiness (coordinator) | — | HTTP `GET /_health` (200) |

`kubectl get pods -n rr` readiness reflects real serving state. Each shard / control pod ALSO serves
its own Prometheus `/_metrics` on a plaintext `--metrics-addr` port (ADR-091, closing ADR-084 deferral
b); with `metrics.enabled` (default on) the chart sets `prometheus.io/scrape|port|path` pod annotations
(shard port `ports.shardMetrics`, control `ports.controlMetrics`) so a Prometheus running pod-annotation
discovery scrapes them directly — per-shard query count / memory / compaction backlog / cost-class, and
per-control Raft term/leader/log/membership. The coordinator's `/_metrics` still carries the
cluster-level counters + a per-shard `reverse_rusty_cluster_shard_queries{shard="N"}` gauge.

## 5. Scaling, recovery, backup

- **Scale shards — blue/green only, never in place.** Do **not** `helm upgrade --set shardCount=N` on
  a live release: it re-keys the ring while the shard PVCs still hold data placed under the old ring,
  the durable control quorum keeps its old `num_shards` (re-bootstrap is idempotent — the coordinator
  then fails the ring-mismatch check, or with control wiring off routes against mis-placed data), and
  `ingest` refuses a non-empty cluster. Instead, like [cluster-deployment.md §5](cluster-deployment.md):
  1. `helm install` a **separate release** at the new `shardCount` (new name ⇒ fresh PVCs + DNS;
     **mint certs whose SANs cover the new pod FQDNs** first).
  2. Re-ingest the full corpus into the green coordinator and validate it.
  3. Cut traffic over (swap the Service/Ingress upstream), then `helm uninstall` blue.

  Cross-process / online resize is a deferred follow-on (ADR-078).
- **RF>1:** not modeled by this chart at v1 (`replicationFactor` is documentation-only). A replica per
  position needs a second StatefulSet per shard + the coordinator's `--replication-factor`; see
  [cluster-deployment.md §5](cluster-deployment.md).
- **Recovery:** a restarted shard/control pod self-restores from its PVC (durable `--data-dir`,
  ADR-036/041). Don't wipe the PVC on restart.
- **Backup:** the remote/stateless coordinator has no cross-shard backup barrier — back up each shard's
  PVC (a volume snapshot per `data` PVC) per [backup-restore.md](backup-restore.md).

## 6. Control-plane wiring & limits

With `controlPlane.enabled` + `controlPlane.wireToCoordinator` (both default true) the coordinator
attaches to the durable quorum via `--control-endpoint` (ADR-083), so the cluster-state document is
durable + HA (all members listed for failover, ADR-086). With `coordinator.routeByAssignments` (default
true) it **routes by the committed shard→node assignments** (ADR-086) — seeded position-preservingly from
the StatefulSet ordinals on first boot, so for a fixed `shardCount` the placement equals the ordinal
order while the durable document becomes the source of truth; *data-moving* live re-pointing is still
deferred (don't HRW-`rebalance` a populated cluster expecting routing to follow). Set `controlPlane.enabled=false`
for the stateless-coordinator topology (placement re-derived from the frozen dict + ring on every
start). Other v1 limits (remote vocab is deploy-time, no online resize, no gRPC `SIGTERM` drain) are
unchanged — see [cluster-deployment.md](cluster-deployment.md) and ADR-084.

## 7. Smoke test

With a cluster + `helm` + `kubectl`: [`deploy/k8s-smoke.sh`](../../deploy/k8s-smoke.sh) installs the
chart (dev secrets, TLS off), waits for readiness, ingests one query over REST, percolates, asserts
the match, and tears down. CI validates the chart structurally (`helm lint` + `helm template` +
`kubeconform -strict`) plus the compose↔chart **topology-parity tripwire** on every PR, and
**behaviorally in the release gate** (ADR-098): `release.yml` runs this script on a kind cluster
against the exact candidate image before anything is published to GHCR.
