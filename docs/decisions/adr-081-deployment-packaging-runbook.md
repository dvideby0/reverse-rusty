# ADR-081: Deployment packaging + operations runbook

**Status:** Accepted (2026-06-20)

**Context.** ADR-065 criterion 10 — *"Dockerfile / compose for a K-shard + control-plane cluster; an
operations doc (start/stop/scale/recover/back up), incl. the ADR-076 vocab-redeploy procedure."* The
image and a compose topology already exist, but only as a **test** asset: [ADR-072](adr-072-multi-machine-harness.md)
shipped [`deploy/Dockerfile`](../../deploy/Dockerfile) (one image, role-by-command — `server` /
`shardserver` / `controlserver`) and [`deploy/compose.harness.yml`](../../deploy/compose.harness.yml) +
`harness.sh`, which bake a corpus, a spare handoff node, and aggressive 2s probes and are explicitly
*not* a production reference. The only operations doc is [`backup-restore.md`](../operations/backup-restore.md)
(ADR-079). What was missing: a production compose and an operations runbook. This is the last criterion
but one (criterion 12, the ≥20M scale proof + real-corpus audit, remains) before the distributed layers
shed the "experimental" label.

**Decision.** Ship the operator packaging as **deployment files + docs only — no engine/library code** —
layered over already-shipped, oracle-proven surfaces (ADR-070 REST coordinator, ADR-071 mesh TLS+token,
ADR-078 resize, ADR-079 backup, ADR-080 broad lane):

- **[`deploy/compose.cluster.yml`](../../deploy/compose.cluster.yml)** — the production topology
  (distinct from, and not replacing, `compose.harness.yml`). Default **K=3 shards, RF=1**, mirroring the
  harness's proven patterns minus the test-only bits: durable named volumes, `restart: unless-stopped`,
  conservative health probes, the REST port **loopback-bound by default**, no baked corpus. Shards are
  **explicit services** (Compose has no stable-identity scale primitive), with an inline template for
  adding a shard and a commented RF=2 expansion. Required secrets fail loud at `up` time
  (`${RR_CLUSTER_TOKEN:?…}`).
- **[`deploy/cluster.env.example`](../../deploy/cluster.env.example)** — the documented env surface
  (`RR_IMAGE`, `RR_CLUSTER_TOKEN`, `RR_AUTH_TOKEN`, `RR_CERT_DIR`, `RR_PORT`) with safe defaults.
- **[`deploy/gen-mesh-certs.sh`](../../deploy/gen-mesh-certs.sh)** — a mesh-identity helper generalizing
  the harness's cert recipe (the load-bearing `CA:FALSE`, SANs covering every service name), with a
  refuse-to-clobber guard so re-running it can't silently rotate a live mesh's identity.
- **[`deploy/cluster-smoke.sh`](../../deploy/cluster-smoke.sh)** — a minimal "comes up green and serves a
  match" check for the production compose (the full lifecycle stays `harness.sh`'s job).
- **[`docs/operations/cluster-deployment.md`](../operations/cluster-deployment.md)** — the runbook:
  topology, prerequisites (certs + the two tokens), bootstrap/startup ordering, health, scaling, recovery,
  backup, the ADR-076 vocab redeploy, monitoring, a security checklist, and an explicit "not covered in
  v1" section. It **links** backup-restore.md rather than duplicating it.

**Explicit deferrals (recorded as decisions, not silent gaps):**

- **(a) Control-plane↔coordinator wiring stays deferred.** A remote coordinator runs its *own*
  `InMemoryControlPlane` and re-derives placement deterministically from the frozen dict + ring on every
  start (the stateless-coordinator model, ADR-070); there is **no flag** to attach it to a `controlserver`
  quorum (confirmed: no `--control-endpoint` in `engine/src/bin/server/`). The production compose still
  ships the durable 3-node quorum so the placement-state tier is production-shaped and the eventual wiring
  PR is a drop-in, but the runbook states **loudly** that at v1 the quorum is durable-but-idle. The
  follow-on is a `--control-endpoint` flag wiring the coordinator's `ControlPlane` to a `RaftControlPlane`
  client over the existing gRPC `ControlService` (ADR-038). Shipping a misleading "HA control plane" that
  the data path ignores would be worse than naming the gap.
- **(b) Kubernetes manifests deferred.** Criterion 10 requires only "Dockerfile / compose," and a k8s
  `StatefulSet` for a control plane the coordinator ignores (deferral a) would imply HA placement the v1
  cluster doesn't have. The shape, for when wiring lands: `StatefulSet`s for shards + control (stable DNS
  + per-pod PVC), a `Deployment` for the stateless coordinator, headless `Service`s for shard DNS,
  `Secret`s for the token + certs. Tracked as a Tier-3 follow-on in [`roadmap.md`](../roadmap.md).
- **(c) Remote add-a-shard is a redeploy, not an online resize.** `POST /_cluster/resize` is the
  in-process blue/green rebuild (ADR-078); the remote topology scales by adding a shard service + endpoint
  + re-ingest. Cross-process/online resize is the ADR-078 follow-on.
- **(d) Custom vocabulary is an in-process-cluster capability.** ADR-076 decided there is no cross-process
  normalizer shipping — remote shards always run `Normalizer::default_vocab()` and the coordinator refuses
  `--vocab-file` against remote shards — so a remote cluster runs the **default vocabulary only**. Custom
  or changed vocabulary uses the in-process `--data-dir` cluster (`--vocab-file`), redeployed blue/green;
  the runbook documents this (and that remote custom vocab is unsupported in v1).

The runbook also surfaces three honest v1 limitations of the remote/stateless path (none new — all are
properties of the already-shipped distributed layers, found in the ADR-081 review): the stateless
coordinator has **no cross-shard backup consistency barrier** (`POST /_backup`/`/_checkpoint` no-op
without a `data_dir`), so a consistent remote backup quiesces writes then snapshots each shard's
`--data-dir`; **class-D queries need the in-process `--data-dir` cluster** (`shardserver` exposes no
`--accept-class-d`, so a class-D-accepting coordinator would acknowledge writes every shard drops —
exposing the shard flag is a tracked follow-on); and `shardserver` is **gRPC-only with no HTTP
`/_metrics`**, so shard liveness is watched via the coordinator's fail-loud `/_health`. The production
compose also makes the **HTTP bearer token required** (the server rejects an empty `RR_AUTH_TOKEN`; the
documented opt-out is to omit the env line on a trusted host) rather than injecting an empty value that
would crash-loop the coordinator.

**Why this is safe.** It adds **no code on any path** — pure deployment artifacts + documentation over
surfaces that are already oracle-proven and harness-proven. The compose is the *same image, bins, and
mesh security* the CI harness already exercises across a real container boundary (ADR-072), with the
test-only corpus/spare-node/aggressive-probes removed and production defaults (loopback publish, auth
expected, durable `unless-stopped`) substituted. The remote coordinator is stateless (`--data-dir` is
refused with `--shard-endpoint`), so the compose ships no coordinator data volume and the runbook routes
backup to the per-node `--data-dir` snapshots — the honest backup story for this topology.

**Validation.** `docker compose config` resolves the production compose clean (anchors, env interpolation,
volume/network refs), with the REST port confirmed `host_ip: 127.0.0.1` and the coordinator confirmed to
carry no `--data-dir`; the required-secret guard refuses to resolve without `RR_CLUSTER_TOKEN` (fail-loud
before any container starts). `gen-mesh-certs.sh` and `cluster-smoke.sh` pass `bash -n`. The full bring-up
is `cluster-smoke.sh` (and the CI harness for the lifecycle), runnable wherever a Docker daemon is
available. A `docker compose config` lint is the CI-safe, daemon-independent gate for this file; a second
full bring-up in CI is not added — the harness already proves the identical image + security + lifecycle.

**See also:** ADR-072 (the shared image + the test harness this builds on), ADR-070 (the coordinator REST
surface), ADR-071/062 (mesh TLS+token / HTTP bearer — the two audiences), ADR-076 (vocab is deploy-time),
ADR-078 (in-process resize), ADR-079 (backup, linked from the runbook), ADR-080 (the broad/class-D
operator contract the runbook surfaces), ADR-065 criterion 10 (the requirement).
