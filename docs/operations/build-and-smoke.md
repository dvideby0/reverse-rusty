# Build & deploy smoke — the fresh-clone checklist

The reproducible-from-zero verification recipe: from a clean checkout, prove the engine **builds**,
**passes the gate**, and **deploys + serves** across every shipped topology. This is the Phase 0
"fresh-clone build & deploy smoke" item — a checklist, not new engine code: each leg below is an
existing script or gate, listed with the exact command and what it proves.

The **supported-deployment contract** (the mode matrix, the guaranteed REST surface, the auth
posture, and the consolidated v1 non-goals) is [`deployment-modes.md`](deployment-modes.md)
(Tier 5 M0, ADR-098); the *operational* procedures (production bring-up, recovery, scaling) are the
runbooks: [`cluster-deployment.md`](cluster-deployment.md) (Compose) and
[`kubernetes-deployment.md`](kubernetes-deployment.md) (Helm). This page is the *acceptance* recipe
that proves a clone is green before you follow them.

## Prerequisites

| Tool | Used by | Verified with |
|---|---|---|
| Rust toolchain (pinned) | build + gate | rustc/cargo **1.95.0** (from [`engine/rust-toolchain.toml`](../../engine/rust-toolchain.toml)) |
| `cargo-audit`, `cargo-deny` | gate (advisories + license/ban policy) | `cargo-audit` 0.22.1, `cargo-deny` 0.19.8 |
| Docker + Compose v2 | image + Compose/harness legs | Docker **29.5.2**, Compose **v5.1.4** |
| `curl`, `jq`, `openssl` | smoke + harness scripts | system |
| `helm` ≥ 3.16, `kubeconform` ≥ 0.6.7 | Helm static validation | helm 4.2.2, kubeconform 0.8.0 |

The gate and the Helm render are **daemon-independent** (no Docker needed); the image, Compose smoke,
and harness need a running Docker daemon. The Compose/harness images build `linux/<host-arch>` from
source, so they run on both x86-64 and Apple-silicon hosts.

## The six legs

Run from the repo root. Each leg is independent; a green result is noted from the **last verified**
run (see the footer).

### 1. Build + gate (daemon-independent)

```bash
cd engine
export CARGO_TARGET_DIR=/tmp/reverse-rusty-target
cargo build --release
./check.sh                       # the full local gate (CI runs this same script)
```

Proves the lean + default + `distributed` builds compile, lints are clean, and **all suites pass**.
`./check.sh` runs nine lanes: `rustfmt`, `clippy` (default / lean-core / distributed), `cargo test`
(default + distributed — the cluster + gRPC oracles), `cargo audit`, `cargo deny --all-features`, and
the `ref-matcher independence` check (ADR-087). ✅ **All checks passed.** (`check.sh` also prints a
non-failing file-size advisory — informational, never blocks the gate.)

### 2. Local deploy smoke (daemon-independent) — the two local modes

```bash
deploy/local-smoke.sh            # cargo-builds the server bin; or --prebuilt <bindir>
```

For **single-node** and the **in-process cluster** (`--cluster --shards 3`), each on a fresh
`--data-dir`: start → assert an unauthenticated write is **401** (the ADR-062 posture) → ingest
over `PUT /_doc` + `POST /_bulk` → `_search`/`_mpercolate` (a match, a MUST_NOT suppression, an
any-of match) → `_stats`/`_metrics` → `POST /_backup` → **SIGTERM-restart on the same data dir and
re-assert** → **open the backup copy and re-assert** (restore = open, ADR-079). The Tier 5 **M1
acceptance gate** (ADR-098): proves the documented deployable surface + restart-reopen for the two
modes a `cargo build` user runs, with no containers. ✅ **PASS (both modes).**

### 3. Build the node image

```bash
docker build -f deploy/Dockerfile -t reverse-rusty:latest .
```

Proves the multi-stage image builds: the builder compiles `--features distributed` (server,
shardserver, controlserver); the runtime carries the three binaries on `debian:trixie-slim` as a
non-root user (uid 10001). ✅ **Image built.** (Legs 4 and 5 reuse the cached compile layers, so build
this once.)

### 4. Production-compose smoke (the shipped K=3 / RF=1 remote topology)

```bash
RR_IMAGE=reverse-rusty:latest deploy/cluster-smoke.sh
```

Stands up [`compose.cluster.yml`](../../deploy/compose.cluster.yml) (3 durable shards + stateless
coordinator + 3-node control plane, mesh TLS + tokens), waits for green, then does **one auth-gated
ingest** and asserts the title percolates back to it — then tears down. Proves the *production* compose
comes up healthy and serves a match end-to-end. ✅ **PASS** (`1 query, hits=[1]`).

### 5. Multi-machine lifecycle harness (ADR-072)

```bash
deploy/harness.sh                # builds from source; or: deploy/harness.sh --prebuilt <bindir>
```

Drives [`compose.harness.yml`](../../deploy/compose.harness.yml) through six legs over the secured
mesh, asserting through REST that every event preserves the percolate baseline and a degraded cluster
**fails loud** (a dead shard is a 502, never a silently truncated result). ✅ **All legs green:**

| Leg | Proves |
|---|---|
| 0 — baseline | corpus loads; live write matchable (13 probes, 319 queries) |
| 1 — kill a shard | degraded percolates fail loud (13/13 → 502); successes ≡ baseline |
| 1b — restart node | durable self-restore + channel reconnect ≡ baseline |
| 2 — rolling restart | every shard restarts and recovers ≡ baseline |
| 3 — coordinator restart | stateless re-mint reconnects to authoritative shards ≡ baseline |
| 4 — live handoff under load | 200 writes accepted during a position move, **zero false negatives** |
| 5 — control-plane restart | all three Raft managers resume from durable state |

### 6. Helm static validation (daemon-independent)

```bash
helm lint deploy/helm/reverse-rusty
helm template rr deploy/helm/reverse-rusty -n rrns [VALUES] \
  | kubeconform -strict -summary -kubernetes-version 1.29.0
```

Renders the chart across the CI value matrix (default, `shardCount=5`, `controlPlane.enabled=false`,
`tls.enabled=false auth.enabled=false`, and the created-secrets variant) and validates every manifest
against the real Kubernetes 1.29 schemas (`-strict` rejects unknown fields). ✅ **Green** — lint clean
(only the cosmetic "icon recommended" INFO); all five renders valid (7 / 7 / 4 / 7 / 9 resources, 0
invalid). A *real-cluster* apply (`deploy/k8s-smoke.sh` against a live cluster) is the Phase 0 item-4
acceptance run and needs a real cluster + corpus — out of scope for this checklist.

## What CI already enforces

[`.github/workflows/ci.yml`](../../.github/workflows/ci.yml) runs legs 1, 2, 5, and 6 on every PR
and push: the `gate` job runs `check.sh`, then the **local deploy smoke** (`local-smoke.sh
--prebuilt`, the M1 gate — ADR-098), then benchmarks (the 10M soak is on-demand via `run_soak`);
the `harness` job lints `compose.cluster.yml` and runs `harness.sh --prebuilt`; and the `helm
chart` job runs the lint + kubeconform matrix. So a green CI ≈ legs 1/2/5/6; **legs 3 and 4 (image
build + the production-compose smoke) are the parts a fresh-clone operator should run locally**
before a first deploy. They were previously unproven end-to-end — see the finding below.

## Findings from the verification run (2026-06-25)

- **`deploy/cluster-smoke.sh` used a non-numeric document id** (`PUT /_doc/smoke1`), which the REST
  router rejects with **400**: the `_doc/{id}` route extracts `Path<u64>` in **both** single-node and
  cluster mode (document ids are u64 logical ids by design — consistent, not a product bug). The
  script had only ever been validated with `docker compose config` (the Docker daemon was down when
  ADR-081 shipped), never run end-to-end, so the latent bug shipped; CI stayed green because
  `harness.sh` uses numeric ids. **Fixed** to use a numeric id and assert `hits=[1]`. This is exactly
  the class of drift this checklist exists to surface.

## Last verified

2026-06-25 · macOS (arm64) · Docker 29.5.2 · legs 1/3–6 green (five at the time); leg 2
(`local-smoke.sh`) added + verified green 2026-07-02. Re-run this checklist on a toolchain
or dependency bump, and after any change under `deploy/`.
