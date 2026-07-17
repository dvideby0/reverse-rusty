# Rolling upgrade

The version-upgrade procedure for the two remote topologies (roadmap Tier 5 M3). One page because
the procedure is ~80% mode-independent; the Compose and Helm legs differ only in the restart
mechanics. Local modes (single-node / in-process cluster) upgrade by stopping the process and
starting the new binary on the same `--data-dir` — the durable formats do the rest.

> TL;DR — upgrade to a **released, smoke-gated tag** only; **back up first**; roll **control →
> shards → coordinator**, health-gating every step; roll back by redeploying the previous tag on
> the same volumes — and if the old binary *refuses* the newer on-disk format, that refusal is the
> fence working: restore the pre-upgrade backup instead.

## 1. Preflight checklist

- [ ] **Target a release, not a branch:** released images
      (`ghcr.io/<owner>/reverse-rusty:vX.Y.Z`) are smoke-gated against the exact candidate
      (compose + kind) before publish (ADR-098); there is no `:latest`.
- [ ] **Read the release notes for format changes** (segment / manifest / WAL versions — see §2).
      A release that bumps a durable format documents it.
- [ ] **Take a backup / snapshot set first** ([`backup-restore.md`](backup-restore.md)): for the
      remote topologies that means the **quiesce-writes → snapshot every `shardN-data` +
      `controlN-data` volume → resume** procedure ([runbook §7](cluster-deployment.md) — a
      stateless coordinator's `POST /_checkpoint`/`/_backup` no-op, there is no cross-shard
      barrier in v1). This is the rollback path if the new version writes a format the old one
      refuses.
- [ ] **Version sanity:** `deploy/check-versions.sh vX.Y.Z` asserts the tag matches the crate +
      chart `appVersion` you are deploying (the same tripwire the release pipeline runs).
- [ ] Upgrade at **low write traffic** if you can — the windows below are smaller and the
      coordinator restart's write outage cheaper.

## 2. The compatibility-fence contract

Durable formats are **versioned and fail loud, never corrupt silently** — an incompatible open is
a refused open with a versioned error, and a refusal is always recoverable by restoring the
pre-upgrade backup:

- **Segments** (`.seg` v3–v7) and **manifests** (engine v3–v5, cluster v4–v6): newer minor
  formats read older files back; an *older* binary refuses a *newer* file it cannot honor. The
  "fence" versions exist precisely to make a semantic change loud — e.g. a
  class-D-bearing segment is written v4 so a pre-ADR-068 binary refuses it rather than silently
  mis-serving; a hot-tier-bearing segment is written **v5** (its hot-index section would be
  silently un-probed by an older binary — ADR-105; the engine manifest bumps in lockstep);
  a replicate-broad-to-all cluster writes manifest v5 as a **two-way** fence
  (ADR-080: the old binary refuses v5; the new binary refuses a v<5 shard-0-only-broad layout).
  The *cluster* manifest deliberately does NOT bump for the hot tier: cluster shards attach
  their registered segments fail-loud, so the per-shard `.seg` v5 word alone fences an old
  binary (ADR-105). ADR-108 adds `.seg` v6 priority columns. ADR-109 adds `.seg` v7 ownership
  columns and cluster-manifest v6; standalone `.seg` v1–v6 remains readable by the new binary,
  but clustered manifests v1–v5 are intentionally rebuild-only because they cannot identify a
  unique emission owner.
- **Same-θ contract (ADR-105):** in remote cluster mode, run every `shardserver` (and the
  coordinator) with the same `--hot-anchor-threshold`. Divergence can never drop a match —
  class A and class H are both always-visible and place identically — it only decides which
  node re-inherits the un-quarantined fat-posting scans; the coordinator warns at startup when
  θ is set in remote mode.
- **Logs:** the standalone WAL is v6 (ADR-108). ADR-109 advances the coordinator log and per-shard
  translog to v4; clustered v1–v3 logs are rebuild-only because their writes lack placement
  identity.
- **Adopted shard state:** ADR-109 adopted feature-space v2 records placement generation and shard
  count. Legacy adopted data-node state must be wiped and reseeded.
- **The mesh wire:** ADR-109 fields are protobuf-additive syntactically, but semantically mandatory.
  Missing/stale placement configuration or `ownership_applied` attestation fails closed. Do not run
  an ADR-109 coordinator against pre-ADR-109 shard peers.
- **ADR-110 ranked delivery:** no durable format changes. `PercolateTopK` and `FetchMatches` are
  additive RPCs, so compatibility percolation continues during a mixed-version roll, but cluster
  `/v2/_search` against an old shard fails closed (`UNIMPLEMENTED` → 502) with no partial hits. Enable
  or route v2 traffic only after every shard is upgraded; keep each shard's
  `--max-grpc-result-bytes` at or below 4 MiB.

An ADR-109 upgrade is therefore **not a normal rolling mixed-version upgrade**. Back up, stop the
cluster, rebuild clustered data under the new binary (or wipe/reseed remote shard volumes from the
authoritative corpus), then restart a version-homogeneous mesh. Ordinary later upgrades may use the
rolling procedure only when their release notes preserve the ADR-109 format/wire contract.

Operationally: **rollback within the same formats is a plain redeploy; rollback across a format
bump is a restore.** The fence tells you which case you are in — by refusing.

## 3. Roll order: control → shards → coordinator

Same order as bring-up ([runbook §3](cluster-deployment.md)), for the same reasons:

1. **Control plane first, one node at a time.** The Raft members exchange versioned envelopes —
   upgrade the quorum among itself before anything that talks to it. After each node: wait for
   its health `ready` (leader known) before the next; the quorum holds throughout (minority
   restart). The coordinator's thin client fails **reads** over to live members meanwhile; admin
   **writes** through a restarting member fail loud until it returns (ADR-085/086) — quiet on the
   admin surface during this step.
2. **Shards next, one at a time.** Each restart is a durable self-restore from its volume
   (ADR-039); reads routing to the restarting shard `502` until it is back (fail-loud). Gate on:
   the shard's gRPC readiness (`service: ready` — dict re-adopted) AND coordinator `/_health`
   green, then move to the next.
3. **Coordinator last.** It is stateless — the restart re-connects, re-ships the dict, re-resolves
   routing (ADR-086). Its restart is the API outage window (seconds); doing it last means it
   starts its new version against an already-upgraded mesh.

## 4. Compose procedure

```sh
cd deploy
# 0. preflight (§1), incl. the backup.
# 1. pin the new image and pull it:
sed -i'' -e 's/^RR_IMAGE=.*/RR_IMAGE=ghcr.io\/<owner>\/reverse-rusty:vX.Y.Z/' cluster.env
rrc pull                       # rrc = docker compose --env-file cluster.env -f compose.cluster.yml
# 2. control plane, one at a time, health-gated:
for n in control1 control2 control3; do
  rrc up -d --no-deps "$n"
  until rrc ps "$n" | grep -q healthy; do sleep 2; done
done
# 3. shards, one at a time, gated on health + cluster green:
for n in shard1 shard2 shard3; do
  rrc up -d --no-deps "$n"
  until rrc ps "$n" | grep -q healthy; do sleep 2; done
  until curl -fsS localhost:9200/_health | grep -q '"green"'; do sleep 2; done
done
# 4. coordinator last:
rrc up -d --no-deps coordinator
until curl -fsS localhost:9200/_health | grep -q '"green"'; do sleep 2; done
```

Verify (§6), and you are done. **Rollback:** set `RR_IMAGE` back to the previous tag and repeat
the same loop; if a service then logs a versioned refusal on open (§2), stop and restore the
pre-upgrade snapshot set into the volumes instead ([`disaster-recovery.md` §3.3](disaster-recovery.md)).

## 5. Helm procedure

```sh
# 0. preflight (§1), incl. per-PVC snapshots.
helm upgrade rr deploy/helm/reverse-rusty --reuse-values --set image.tag=vX.Y.Z
kubectl rollout status statefulset/rr-reverse-rusty-control --timeout=10m
kubectl rollout status statefulset/rr-reverse-rusty-shard   --timeout=10m
kubectl rollout status deployment/rr-reverse-rusty-coordinator --timeout=10m
```

What the chart guarantees while that runs:

- Both StatefulSets set `updateStrategy: RollingUpdate` explicitly: pods restart **one at a time,
  in reverse ordinal order, each gated on Ready** before the next — regardless of
  `podManagementPolicy: Parallel`, which affects scale-up/creation only, never updates. Readiness
  is the real gate: a shard is Ready only once it serves with a dict re-adopted; a control node
  only once it sees a leader (ADR-084).
- The **PodDisruptionBudgets** (`maxUnavailable: 1` for shards and control) protect the same
  invariant against *node drains* racing the rollout — an eviction can never take a second shard
  or break the control quorum while one member is already down.
- The three workloads roll **concurrently with each other** (Helm applies all templates at once) —
  a bounded mixed-version window the wire contract tolerates (§2). **Strict §3 cross-workload
  ordering is not available with this chart** (one shared `image.tag`; and `kubectl rollout
  pause` applies to Deployments only, never StatefulSets). If you need it, gate each StatefulSet
  manually with `updateStrategy.rollingUpdate.partition` (`kubectl patch` the partition down as
  each workload finishes) — otherwise rely on the readiness gates + the fail-loud fences, which
  is what the concurrent roll is designed around.
- A stuck pod (readiness never true on the new version) **halts the rollout** at that ordinal —
  the remaining replicas keep serving the old version. `kubectl rollout undo` (or `helm rollback
  rr`) rolls back; the format-fence caveat from §4 applies unchanged.

## 6. Post-upgrade verification

- [ ] `/_health` green; every shard `reverse_rusty_shard_ready 1`; control `/_metrics` reports
      exactly one leader ([`alerting.md`](alerting.md) has the expressions).
- [ ] `GET /_stats` count unchanged from preflight.
- [ ] The golden-titles probe matches its recorded output
      ([`backup-restore.md` rehearsal](backup-restore.md#rehearsal--prove-you-can-restore)).
- [ ] A sentinel write round-trips (`PUT /_doc` → `_search` → `DELETE`).
- [ ] Watch the dashboards for one soak interval: no `durability_failures_total` increase, no
      transport error/timeout rate, percolate p99 unchanged (ADR-100 histogram).
