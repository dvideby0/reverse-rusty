# ADR-084: Kubernetes/Helm packaging + gRPC health endpoints

**Status:** Accepted (2026-06-23)

**Context.** [ADR-081](adr-081-deployment-packaging-runbook.md) shipped the production compose +
operations runbook but **deferred Kubernetes manifests**, for one stated reason: "a k8s `StatefulSet`
for a control plane the coordinator ignores (deferral a) would imply HA placement the v1 cluster
doesn't have." [ADR-083](adr-083-control-plane-coordinator-wiring.md) then removed that blocker — the
coordinator can now attach to the durable openraft quorum via `--control-endpoint` — so a k8s
control-plane `StatefulSet` can be *load-bearing*, and the manifests can reference a real quorum
rather than an idle one. The remaining gap was operational: the gRPC data/control binaries had **no
real health surface**, so `compose.cluster.yml` probes them with a crude `exec 3<>/dev/tcp` TCP check
that a dict-less (`--pending`) shard passes despite being unable to serve. Kubernetes liveness/
readiness gating needs a meaningful signal that distinguishes "process up" from "ready to serve."

**Decision.** Ship the Kubernetes packaging as a Helm chart, plus the engine-side gRPC health
endpoints it depends on.

- **gRPC health endpoints (engine, `distributed`-gated).** Add the standard `grpc.health.v1.Health`
  service to `ShardServer` and `ControlServer`, served on an **optional, separate, plaintext**
  `--health-addr` port. Default unset ⇒ no second listener, **byte-identical** to the prior
  single-port behavior (preserves the lean / in-process invariant; the whole `distributed` oracle is
  unchanged). Two keys: `""` (overall) → SERVING once the server is up = **liveness**; `"ready"` →
  **readiness** — a shard reflects dict-adoption (`state.load().is_some()`; a `--pending` shard is
  live-but-not-ready until `AdoptDict`), a control node reflects `raft.metrics().current_leader.is_some()`.
  A 250 ms poll watcher updates readiness off any hot path.
  - **Why a separate plaintext port, not health-on-the-TLS-data-port:** the Kubernetes built-in
    `grpc` probe is plaintext-only (no CA field), and the mesh data port is TLS + token-gated (a
    kubelet has neither). Health status is non-sensitive (SERVING/NOT_SERVING) and pod-local, so a
    plaintext sidecar port is safe and needs no extra image binary. *Alternative (documented, not
    chosen):* health on the data port outside the auth interceptor + an exec probe using
    `grpc_health_probe -tls` — heavier image, fiddlier probe config.
  - **Why hand-roll the proto, not add `tonic-health`:** consistent with the existing protox-codegen
    pattern (`grpc/build.rs`) and adds **zero dependencies** (ADR-028 lean ethos + the `cargo deny`
    gate). k8s + `grpc_health_probe` call only `Check` (unary); `Watch` returns `unimplemented`.
  - **Readiness without touching any RPC handler:** `ShardServer.state` became
    `Arc<ArcSwapOption<ServerState>>`, so the watcher shares the same cell `AdoptDict` stores into —
    the only edits are the constructor wraps; the control side clones the (cheap) `Raft` handle.
- **Helm chart ([`deploy/helm/reverse-rusty/`](../../deploy/helm/reverse-rusty/)).** The Kubernetes
  analogue of `compose.cluster.yml` (same image, bins, mesh TLS+token, flags): a **shard
  `StatefulSet`** (per-pod PVC, `--pending --data-dir --health-addr`, native gRPC liveness/readiness
  probes), a headless **Service** with `publishNotReadyAddresses: true` (the coordinator must dial a
  *pending* shard to `AdoptDict` it), a **control `StatefulSet`** with the standard ordinal-0
  bootstrap dance (a mounted entrypoint derives the ordinal from `$HOSTNAME`, builds `--peer` +
  `--advertise-url` per ADR-082), a **coordinator `Deployment`** (`--shard-endpoint` per shard +
  `--control-endpoint` to the quorum when `controlPlane.wireToCoordinator`, with a `wait-for-mesh`
  initContainer standing in for compose's `depends_on`), `Secret`-referenced mesh TLS + tokens, and
  an optional `Ingress`. Defaults encode the same secure posture as the compose (TLS on, REST auth
  required).

**Explicit deferrals (recorded, not silent):**

- **(a) Routing by committed shard→node assignments.** The coordinator still routes by its
  `--shard-endpoint` list (the chart's fixed StatefulSet ordinals), not the committed control-plane
  assignments — the ADR-083 residue, unchanged here. Fine for a fixed StatefulSet shard set.
- **(b) Shard/control metrics.** ADR-084 adds *health*, not *metrics* — `shardserver`/`controlserver`
  remain gRPC-only with no Prometheus `/_metrics` (the ADR-081 limitation). Shard observability is
  still via the coordinator's `/_health`; a metrics surface is a tracked follow-on.
- **(c) gRPC graceful shutdown.** The gRPC servers do not drain on `SIGTERM` (the durable
  `--data-dir` + recovery cover an abrupt stop). `terminationGracePeriodSeconds` + the durable PVC
  handle a rolling restart; a graceful gRPC drain is a follow-on.
- **(d) RF>1, online resize, remote custom vocab** — unchanged from ADR-080/078/076; the chart models
  RF=1 and a fixed `shardCount`, documented in the runbook.

**Why this is safe.** The engine change is off every match hot path and behind the opt-in
`--health-addr` (unset ⇒ byte-identical; the full `distributed` oracle stays green, proving the
`Arc<ArcSwapOption>` refactor changed nothing). The chart adds **no code on any path** — declarative
manifests over the same image + mesh security ADR-072's harness already exercises across a real
container boundary. The zero-false-negatives contract is untouched.

**Validation.** Engine: `cargo build --no-default-features` (lean core unaffected) +
`cargo test --features distributed` (new health transition tests — shard liveness/readiness/unknown
+ readiness-flips-after-adopt over real gRPC, control readiness-on-leader-election — plus the whole
prior oracle green) + `engine/check.sh` (fmt + clippy incl. the lean lane + audit + **deny with no
new dependency**). Chart: `helm lint`, `helm template` across a value matrix (shardCount, controlPlane
on/off, tls/auth on/off, dev-secret creation), and `kubeconform -strict` against the real Kubernetes
1.29 schemas — wired as a CI job (`.github/workflows/ci.yml`, `helm-chart`). A live `kind` bring-up is
not run in CI (the container harness already proves the identical image + mesh + lifecycle); a
`deploy/k8s-smoke.sh` is provided for operators with a cluster.

**See also:** ADR-081 (compose packaging + runbook this extends), ADR-083 (the control-plane wiring
that unblocked load-bearing k8s manifests), ADR-082 (the advertise-URL the control StatefulSet
bootstrap uses), ADR-071/062 (mesh TLS+token / HTTP bearer the chart references), ADR-028 (the
lean-dependency stance behind the hand-rolled health proto), ADR-065 criterion 10 (the packaging
requirement this follows on).
