# Roadmap — what's next

The prioritized roadmap: **open work only.** When an item ships it is **deleted from this file** —
its ADR becomes the permanent record and [`STATUS.md`](STATUS.md) gets a one-line entry (the
editing rule lives there). Tier numbers are stable — ADRs and PRs reference "Tier N" — so a
completed tier keeps its heading and one line. Decision rationale → [`DECISIONS.md`](DECISIONS.md);
component design → [`design/`](design/README.md).

Priority follows the bottleneck analysis ([`performance/results.md`](performance/results.md) §9):
the selective path is far past the spec target with a flat ~54 candidates/title, so the leverage is
in the broad lane, memory/footprint, and the durability + scale story — not in shaving the
selective candidate count further. **But before any tier work, Phase 0 (the reality / adversarial
audit) runs first — it is the current top priority.**

## Phase 0 — Reality / adversarial audit (do this first)

**Top priority — precedes every tier below.** The engine is oracle-proven, but the in-tree differential
oracle *shares the front-end* (normalizer, parser, extractor, dict) with the engine, so a front-end bug
is structurally invisible to it (the reference-free `tests/adversarial.rs` only partly covers this —
ADR-063). This phase proves which parts are real — under an *independent* check and under real failure —
before more is built on top. Goal: separate what's real from plausible-looking scaffolding. Tier work
resumes once it passes.

1. **Fresh-clone build & deploy smoke — ✅ shipped
   ([`operations/build-and-smoke.md`](operations/build-and-smoke.md)).** The reproducible-from-zero
   checklist: `cargo build --release` + `./check.sh` (the full gate) + the Docker image + the
   production-compose smoke (`deploy/cluster-smoke.sh`) + the multi-machine harness
   (`deploy/harness.sh`, ADR-072) + Helm lint/kubeconform — every leg with the exact command and what
   it proves (the Tier 5 M0 "deploy-truth" acceptance recipe). The run surfaced + fixed one real drift:
   `cluster-smoke.sh` used a non-numeric `_doc/{id}` (the route extracts `Path<u64>` in both modes → a
   400) and had never been run end-to-end. A *real-cluster* deploy proof stays item 4 (needs a real
   cluster + corpus).
2. **Independent correctness oracle — ✅ shipped ([ADR-087](decisions/adr-087-independent-correctness-oracle.md)).**
   A std-only, zero-dependency reference matcher (`reverse-rusty-ref-matcher`) reimplements the whole
   front end (parser/normalizer/extractor/predicate) from the spec, reusing none of the engine
   (independence enforced by a `check.sh` `cargo tree` lane); the engine is diffed against it
   (`tests/independent_oracle/`) over default/populated/alias corpora + a hand-written gotcha table +
   the env-gated `RR_ORACLE_CORPUS` real-corpus hook. Closes the ADR-050/063 shared-front-end blind
   spot for the covered paths — zero FN/FP, no engine front-end bug found.
3. **Durability torture (crash injection) — ✅ shipped
   ([ADR-088](decisions/adr-088-crash-injection-harness.md)).** A `crashwriter` lean-core bin + the
   `tests/crash_injection/` suite spawn a real process, deliver a real external SIGKILL mid
   durable-operation (WAL append / flush / compaction / backup / churn), reopen in-process, and diff
   the recovered engine against the front-end-independent oracle (ADR-087) — zero false negatives on
   every acked write, no resurrection/corruption. `#[ignore]`d behind a new `check.sh` crash lane
   (`RR_CRASH_ITERS`); mutation-validated. The three originally-deferred legs are now **shipped** (ADR-088
   follow-up): the **`upsert`** atomic-replace scenario (race-immune both-version construction), the
   **`watermark`** multi-reopen `ensure_seq_after` scenario (proven non-redundant vs the single-reopen
   churn), and the **cluster** kill-*mid-write* leg (`deploy/harness.sh` leg 3b: every 2xx-acked write
   matchable after restart + `/_cluster/resync`). *Still deferred:* a cluster-coordinator (not shard)
   mid-write kill, and a power-loss leg (SIGKILL cannot drop the page cache — the torn-tail/CRC
   simulations keep that domain).
4. **Deployment proof on real Kubernetes.** Deploy to a real cluster (not localhost Compose), ingest a
   **real corpus**, then restart every pod type, delete a shard pod, fill the disk, rotate secrets, and
   restore from backup — proving **no silent misses** at each step. This is the adversarial acceptance
   run for Tier 5 M2 + Tier 3 criterion 12 (real corpus).
5. **Security review — ✅ shipped ([ADR-089](decisions/adr-089-security-review.md)).** A
   [threat-model doc](operations/threat-model.md) (the 4 trust boundaries, assets, adversary model,
   controls mapped to code, the explicit v1 non-goals + operator checklist), a **container image scan**
   (`deploy/scan-image.sh`, Trivy) with a triaged baseline (234 base-image CVEs, 2 CRITICAL / 14 HIGH —
   all Debian-base, none reachable by the service; the app deps are `cargo audit`/`deny`-clean), and the
   `_backup` client-`dest` path-traversal finding dispositioned (auth-gated + non-root operator
   responsibility; an optional config jail deferred). Docs + tooling only; no code-level vuln found.
   *Deferred:* a distroless/curl-free base, the `_backup` jail, mTLS + per-RPC authz (ADR-071 post-v1).

### Tier 0 — Cluster v1 acceptance gate — ✅ complete

Dynamic vocabulary (ADR-046), the named cluster oracle gate ([`testing.md`](testing.md)),
`clusterbench` fan-out invariants, and the v1/experimental reframe — all shipped; see
[`STATUS.md`](STATUS.md).

### Tier 1 — measured bottlenecks — ✅ complete

Broad-lane batch/columnar evaluation (ADR-026) and resident-memory reduction (ADR-020) — shipped.

### Tier 2 — feature-model quality & self-tuning

Shipped: NPMI phrases (ADR-053), equivalence expansion (ADR-054), compaction re-anchoring
(ADR-056), alias governance + multi-word activation (ADR-060/061). Open:

- **Distributional alias discovery** — context-similarity candidates feeding the shipped
  ADR-054/060 seam. Noisy (conflates substitutes with co-hyponyms) ⇒ review-first, never
  auto-active.
- **Match-feedback alias validation** — the highest-precision *automated* alias signal; needs an
  operational title→query feedback loop.
- **The rest of the "improve" menu** ([`design/ingestion-and-updates.md`](design/ingestion-and-updates.md)
  §7): candidate-survival telemetry, `recommended_shard_count`/`recommended_arity`, feature-ID
  re-ranking for locality, re-running the corpus learner per range.
- **Vocab consolidation on compaction** — background re-materialize of hashed terms / learned
  synonyms into the dict (the ADR-046 deferral; distinct from ADR-056 re-anchoring).

### Tier 3 — scale & production maturity

- **Distributed v1 ([ADR-065](decisions/adr-065-distributed-v1-graduation.md)) — open criteria**
  (1–11 shipped: ADR-070/071/072/074/075/076/077/078/079/080/081, + follow-ons ADR-082/083/084; see
  [`STATUS.md`](STATUS.md)):
  - **Criterion 12 — scale proof at target:** a multi-shard load test at ≥20M stored queries on
    real hardware (largest soak to date: 10M single-node), plus the **real-corpus FN/throughput
    audit** owed in [`STATUS.md`](STATUS.md) "Current limitations".
  - *Criterion 7 follow-ons (deferred, [ADR-078](decisions/adr-078-cluster-resize.md)):* always-on
    autoscaler-driven resize (needs hysteresis to avoid thrash, since a resize is non-idempotent +
    `O(corpus)`) + a cross-process / online resize (ship the re-keyed data to remote shards over the
    live-handoff machinery; the v1 resize is in-process blue/green).
  - *Live-handoff follow-on (the [ADR-086](decisions/adr-086-control-plane-routing-and-failover.md)
    deferral — data-moving reassignment **shipped** in
    [ADR-090](decisions/adr-090-data-moving-reassignment.md); the multi-shard-per-node foundation
    **shipped** as [ADR-093](decisions/adr-093-multi-shard-per-node.md) Stages 1–3, making HRW
    rebalancing collision-safe; and the unattended **assignment-watch → re-point controller** in
    [ADR-092](decisions/adr-092-unattended-reconciler.md), rebased onto that foundation as Stage 4:
    `reconcile` + the opt-in `--reconcile-interval-secs` loop converge the committed map to the
    HRW-desired placement by moving data, automatically + idempotently + zero-FN — proven on the
    packed K&gt;N multi-shard topology that was the original clobber bug — and the autoscaler's
    membership-drift arm is now data-moving on a remote cluster too; **RF&gt;1 data-moving
    reconciliation shipped** as [ADR-094](decisions/adr-094-replicated-group-reassignment.md) — a
    replicated position's whole GROUP moves via `reassign_group_and_move`, closing the ADR-090 RF&gt;1
    deferral; and **parallel multi-position moves shipped** as
    [ADR-095](decisions/adr-095-parallel-multi-position-moves.md) — the busy-endpoint move ledger +
    conflict-free waves, opt-in via `max_parallel_moves`/`--reconcile-max-parallel`, default
    sequential byte-identical; **orphan-slot GC shipped** as
    [ADR-096](decisions/adr-096-orphan-slot-gc.md) — `ListShards`/`DropShard` + the ledger-reserved
    coordinator sweep, opt-in via `--reconcile-gc-orphans`/`POST /_cluster/gc`; and the
    **content-fingerprint skip shipped** as
    [ADR-097](decisions/adr-097-content-fingerprint-skip.md) — a provably-complete retained member
    keeps its data, collapsing a pure promotion's fence window):* the remaining open work is the
    last ADR-094 cost deferral — server-side staged recovery (shadow install, atomic promote) out
    of the fence window, now valuable only for the rare genuinely-desynced member
    (ADR-097 §Consequences). (k8s/Helm manifests + gRPC health/readiness probes shipped —
    [ADR-084](decisions/adr-084-kubernetes-helm-health.md); ADR-082 closed the advertise-URL; the
    `shardserver --accept-class-d` item was a phantom — remote shards force-accept class-D, the
    coordinator is the sole gate.)
- **Feature-model versioning + blue/green re-materialize** — frozen common-mask across minor
  versions; a major model change replays the log into a parallel index, then an atomic epoch swap.
- **Aspects-first ingestion** — use eBay structured item-specifics as features instead of relying
  only on title parsing; higher feature quality, larger domain integration.
- **Memory headroom at scale (the documented production changes)** — two items the bottleneck
  analysis ([`performance/results.md`](performance/results.md) §9) still flags open: **dictionary
  string interning** (the dict retains per-feature `String`s, inflating bytes/query — interning +
  segment mmap is the named fix) and **memory-bandwidth mitigation** as the index leaves cache
  (tighter SoA packing; sharding already buys cache residency). Gates an honest 100M memory
  extrapolation; pairs with criterion 12's real-corpus audit.

### Tier 4 — ES/OS percolator parity — small residue

The parity program is **complete** — tags + filtered percolation (ADR-049/055), punctuation
folding (ADR-058), ranking (ADR-059/075), alias evolution (ADR-060/061), and the ADR-064 drop-in
work package (ADR-067/068/069/073); workload mapping →
[`research/percolator-workload.md`](research/percolator-workload.md). Open refinements, each
behind a shipped seam:

- **Component-conjunction alternative on alias activation** — keep the scattered-components
  reading when a multi-word alias activates, via CNF distributivity (recall-only widening,
  bounded; ADR-061 §semantics-of-activation).
- **Additive punctuation fold** — emit the joined form AND the split components (à la Lucene's
  `WordDelimiterGraphFilter`); pure recall gain behind the ADR-058 `PunctClass` seam.
- **Columnar two-view broad lane** — the broad lane drops to the inline path while multi-word
  aliases are active; a perf follow-on, not correctness (ADR-061).

### Tier 5 — deployability & operational maturity

The engine + distributed layers are built and oracle-proven; this tier is the **named, documented
deployable contract** distinct from the scale proof, plus the operational hardening above it
(source: external deployability review, 2026-06-24; the review positioned M0–M2 as the highest-ROI
next work, ahead of more algorithm work). M0–M2 all shipped as
[ADR-098](decisions/adr-098-deployable-gate-and-release-pipeline.md) — the contract page is
[`operations/deployment-modes.md`](operations/deployment-modes.md), the local smoke gates every PR,
and `release.yml` publishes the smoke-gated GHCR image per `v*` tag.

- **M0 — deploy-truth — ✅ shipped ([ADR-098](decisions/adr-098-deployable-gate-and-release-pipeline.md)).**
  [`operations/deployment-modes.md`](operations/deployment-modes.md) is the canonical
  supported-deployment contract: the four-mode matrix (single-node · in-process cluster · remote
  Compose · remote Helm) with exact bring-up commands, the guaranteed REST surface, the auth
  posture, and the **v1 non-goals consolidated into one named-constraints table** (RF>1 in Helm,
  online/remote resize, remote custom vocab, cross-shard backup barrier, scale proof, mTLS,
  power-loss default — each with its deciding ADR). The runbooks' scattered copies now point there.
- **M1 — Deployable Feature Complete: single-node + in-process cluster — ✅ shipped (ADR-098).**
  [`deploy/local-smoke.sh`](../deploy/local-smoke.sh) runs both local modes end-to-end (start →
  401-auth probe → `_doc`/`_bulk` ingest → `_search`/`_mpercolate` incl. a MUST_NOT suppression →
  `_stats`/`_metrics` → `_backup` → SIGTERM-restart-reopen → restore-the-backup) **inside the
  required `gate + benchmarks` CI job** — the M1 acceptance gate on every PR, no containers.
- **M2 — Deployable Feature Complete: remote static cluster — ✅ shipped (ADR-098).**
  [`release.yml`](../.github/workflows/release.yml): on a `v*` tag — version preflight (tag ==
  crate == chart appVersion, `deploy/check-versions.sh`) → build → **smoke the exact candidate
  image** (Compose `cluster-smoke.sh` + kind `k8s-smoke.sh`, now fixed + passing end-to-end, +
  `deploy/check-topology-parity.sh`) → publish `ghcr.io/<owner>/reverse-rusty:{vX.Y.Z, X.Y.Z,
  sha-<short>}` (**the image is the only published artifact; no `:latest`** — ADR-098);
  `workflow_dispatch` = the no-publish rehearsal. Per-PR: the compose smoke rides the harness
  job's image; the parity + version tripwires ride the helm-chart job. (Scale proof stays Tier 3
  criterion 12, not a blocker here.)
- **M3 — production hardening (safe with on-call ownership).** **Shard/control-local Prometheus
  metrics shipped** ([ADR-091](decisions/adr-091-shard-control-metrics.md), closing ADR-084 deferral
  b): per-node `/_metrics` on `shardserver`/`controlserver` (stored-query count, memory, compaction
  backlog, cost-class; per-control Raft term/leader/log/membership) + a coordinator per-shard query
  gauge — the autoscaling-signal prerequisite. *Residual:* **per-shard p95/p99 latency** (needs a
  hot-path timing hook; the coordinator already has RPC latency via ADR-085) and per-shard broad-lane
  cost stay open. Plus the operational docs above the shipped ADR-081 runbook: **DR runbook,
  rolling-upgrade procedure, resource-sizing guide, alert examples, a backup/restore rehearsal**.
  Promote **cooperative cancellation / bounded concurrency** (the ADR-052 deferral now in the
  Robustness backlog) here.
- **M4 — commercial-service operations (API-driven, not runbook-driven).** The bar past "cloud
  deployable": backups, scaling, restore, and rollout become controllers/APIs, not manual procedures.
  Larger, later, and partly **in tension with the shared-nothing / no-object-store stance
  ([ADR-033](decisions/adr-033-shared-nothing-storage.md))** — resolve that first.
  - **Backup-as-a-service** — a `POST /_cluster/backup|restore` API + a coordinator-driven cross-shard
    **consistency barrier** (the no-quiesce backup the v1 runbook lacks), scheduled backups, retention,
    manifest + checksums, automated restore verification, stated RPO/RTO. *Object-storage (S3/GCS)
    targets revisit ADR-033 — decide whether to relax shared-nothing for backup export only.*
  - **Automated blue/green resize controller** — drive the existing manual blue/green path from
    metrics + `recommended_shard_count` with hysteresis (green cluster → rehydrate → validate → cut
    traffic → GC blue). True online/data-moving resize stays the deferred ADR-078/086 follow-on in
    Tier 3.
  - **Kubernetes Operator / CRD** (`ReverseRustyCluster`) — owns StatefulSets, backup schedules,
    restore jobs, blue/green resizes, coordinator HPA, rollout safety: the lifecycle layer above Helm.
  - **RF>1 Helm topology** — a replica StatefulSet per position + the coordinator's
    `--replication-factor` (the engine's RF is built; the chart value is documentation-only today).

### Evaluated & declined

- **Query-family / shared-prefix DAG** — optimizes a non-bottleneck at high format/rebuild cost;
  see [ADR-019](decisions/adr-019-query-family-factoring-declined.md). Reversible.

---

## Nice-to-have / operational polish backlog

Low-priority polish and micro-optimizations — none are production blockers.

**API / ops ergonomics**
- **CORS headers** — browser-based tools can't hit the API; add `tower-http::CorsLayer`.
- **Thread-pool introspection** (`/_cat/thread_pool` equivalent).
- **Per-segment filter FP rate in `/_cat/segments`** (deferred from ADR-023) — needs
  `SegmentFilter` to retain its inserted-key count and `MmapSegment` to expose block count, then a
  `filter_fp_pct` column.
- **`_cat` `?v`/`?h`/`?help` flags** — ES-style verbose/column-selection; listed for completeness.
- **`took_ms` uses raw f64** (`0.003284000000000001`) — integer ms or round to 2 dp.
- **No pre-warming** for mmap'd segments on cold start.
- **No measured restart/reopen time** at ≥1M queries (ADR-064 item 7) — the design implies
  sub-second-to-seconds; capture a number.
- **Tags are write-only over REST** — no endpoint returns a stored query's tags; small read-back
  addition for metadata audits (ADR-064 item 7).
- **Class-C ingest warnings / rewrite suggestions** — surface "this query landed in the broad
  lane" at ingest with a rewrite hint (the ADR-026 follow-up).

**Memory / hot-path micro-optimizations**
- **`alive: Vec<bool>`** — 8× the memory of a bitvec.
- **`seg_lens` Vec allocated on the match hot path** — could be a fixed-size array.
- **WAL `append_insert` allocates a Vec per write** — pre-allocated write buffers.
- **Byte-at-a-time CRC-32 for manifest writes** — table-based is ~10× faster.
- **SIMD intersection** for medium/large (mostly broad-lane) roaring postings (the ADR-026
  follow-up).

**Test-infrastructure follow-ons (ADR-063 audit)**
- **Extend the parse-union oracle's fuzz alphabet** with `#`/`/`/`pop` markers, 4-digit years, and
  fused graders *inside phrase patterns* (needs the reference `emit_parse` to learn the
  marker/year rules).
- **A cross-seam integration harness** (recovery×vocab, adopt-on-fresh vs adopt-on-recovered,
  `set_vocab` guard matrix) — ~22% of historical review-caught escapes were cross-seam; point
  regression tests exist, the *combinations* don't.
- **Occasional targeted `cargo-mutants` runs** on `normalize`/`compile`/`exact` after major
  matcher changes (declined as a per-PR gate in ADR-063 for wall-clock cost).
- **Messy variants of the cluster oracles** — thread `messify_dataset` through
  `tests/cluster_oracle` + the durability oracle (the harnesses already take a `Dataset`).

**Robustness**
- **Durable-ingest segment-write failures surface as `ingest_rollback`, not `segment_write`** —
  emit `SegmentWrite`/`SegmentMmap` from inside `build_durable_base` for symmetric labeling (the
  OS error is already visible; low priority).
- **Cooperative cancellation on the match path** (ADR-052 deferral) — `timeout_ms` is a response
  deadline only; timed-out work runs to completion. Weigh a coarse per-segment deadline check
  against simply bounding concurrency.
