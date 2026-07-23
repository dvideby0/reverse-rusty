# Status — what's built, at what scope

The canonical home for **implemented vs design-only**. This file is a scannable inventory — one
line per capability, each naming its ADR — not a narrative: **the full story of any item (context,
mechanism, scope boundaries, proof) lives in its one ADR file under [`decisions/`](decisions/)**
(index: [`DECISIONS.md`](DECISIONS.md)). What's *next* lives in [`roadmap.md`](roadmap.md). The full
suite (`cargo test --release`) and the `check.sh` gate run green on every PR; how-we-test →
[`testing.md`](testing.md).

> **Editing rule (keeps this file small):** when something ships, add or extend **one line** here,
> **delete** the roadmap item, and let the ADR carry the narrative. Never paste ADR detail into
> this file.

## Scope frame — read this first

**Cluster v1 is the shippable milestone** — the in-process multi-shard core + durable reopen +
dynamic vocabulary — **built and oracle-proven, zero false negatives** (Tier 0, complete). The
**distributed multi-node layers** (gRPC transport, replication, Raft control plane, translog
recovery, live handoff, autoscaler) are **built and oracle-proven in-process / on localhost, but
experimental** — graduating to release-candidate via the **Distributed-v1 program
([ADR-065](decisions/adr-065-distributed-v1-graduation.md))**: criteria **1–11 shipped**
(ADR-070/071/072/074/075/076/077/078/079/080/081); **12 open** ([`roadmap.md`](roadmap.md) Tier 3).
Everything `distributed`-gated is off by default; the lean / in-process path is byte-identical.

## Built

### Core matching engine

- **DSL parser → AST** (`dsl.rs`) — compile-time only; typed errors (ADR-005); complexity limits
  enforced in the parser (ADR-025).
- **Shared query/title normalizer** (`normalize.rs`) — one automaton for both sides; configurable
  punctuation classes (ADR-058); number-context word list incl. the parity mode (ADR-069);
  alias-mode phrases + the dual title views for multi-word aliases (ADR-061).
- **Feature dictionary** (`dict.rs`) — dense ids + 64-hot common mask; synthetic ids for
  post-freeze terms (ADR-046); transient `EquivMap` for equivalence expansion (ADR-054).
- **Signature-cover optimizer + cost classes A–D** (`compile.rs`) — the lossless cover (ADR-001,
  ADR-003); forbidden features structurally invisible to gating (ADR-006); `anchor_plan` is the
  cluster-placement SSOT (ADR-027).
- **Candidate index** (`index.rs`) — three-tier adaptive postings (inline → Vec → roaring).
- **Integer-only exact verification** (`exact.rs`) — SoA + common-mask gate (ADR-002); columnar
  batch transpose `eval_batch` (ADR-026); tag column + `TagPredicate` (ADR-049); `TitleView`
  P(T)/N(T) (ADR-061).
- **Broad lane** — class-C quarantine (ADR-003) + batch/columnar evaluation amortizing broad
  postings ~1/batch_size, exposed as `/_mpercolate` (ADR-026); + the lever-5a **count-gate
  pre-reject** (`broad_prefilter`) and the observe-first Broad-Query Cost Program telemetry
  (`would_be_hot`, per-lane posting percentiles).
- **The hot tier (class H)** — θ-reclassification under the two-axis placement rule: a
  θ-hot non-top-64 anchor moves the query to a third per-segment index, probed on every
  request (always-visible) but columnar-evaluated; `.seg`/manifest v5 fences, ring placement
  in the cluster, margin-gated compaction migration; default off (`hot_anchor_threshold` = 0)
  ⇒ byte-identical (ADR-105 — built + oracle-proven; the recovery measurement runs paired
  with dedup Stage A per the roadmap).
- **Canonical-body dedup, Stage A** — queries with identical semantic bodies share one posting
  entry per in-memory segment (verified once, emitted per member under per-member
  aliveness/tags); flush expands so the on-disk format is untouched; compaction regroups
  cross-segment; `dedup_bodies` default ON + the global distinct-bodies sketch that sizes
  Stage B (ADR-106 — built + oracle-proven).
- **Class-D always-candidate lane** — opt-in `accept_class_d` stores negation-only queries under
  the universal signature; default off = the loud reject (ADR-068).
- **Per-query tags + filtered percolation** — verify-stage filter, never gates (ADR-049); threaded
  end-to-end through the cluster (ADR-055).
- **Ranking + pagination** — opt-in post-match `Σ boosts + priority-tag value`, `from`/`size`
  (ADR-059); cluster rank-at-shard with compile-once-fan (ADR-075).
- **Typed bounded ranking** — signed `i64` priority column, newest-live integer scoring, bounded
  `TopKCollector`, and single-document `POST /v2/_search` with honest thresholded totals + cooperative
  deadlines in both single-node and cluster coordinator modes (ADR-107/108/110).
- **Deterministic distributed emission ownership** — placement generation + compact placement modes
  select exactly one routed shard position after exact/member checks; unfiltered, filtered, and
  compatibility-ranked cluster results are unchanged while cross-shard duplicate emissions are zero.
  Placement identity survives every durable/replicated lifecycle and stale data/peers fail closed
  (ADR-109).
- **Exact distributed top-K + query-then-fetch** — ownership is applied before each shard's bounded
  collector; the coordinator validates/merges at most K sorted disjoint rows per routed position,
  then fetches current source only for global winners and compiles explanations centrally. One
  deadline, no partial results, static HTTP/gRPC byte caps, cluster `POST /v2/_search`, and bounded
  delivery metrics; no durable-format change (ADR-110).
- **Distributed ranked title batching** — per-title ownership + bounded collectors through the
  columnar batch kernel; the streamed `PercolateTopKBatch` wire (capped per-title frames + a
  completeness summary); one-call-per-shard coordinator fan with the shared per-title exact merge
  and a one-credit union winner fetch; `POST /v2/_mpercolate` in both modes; `size × titles`
  heap-budget admission; no durable-format change (ADR-112).
- **PIT + cursor pagination** — `POST/DELETE /v2/_pit` pins the immutable engine snapshot (local)
  or every position's snapshot (in-process cluster, placement-identity-gated) under a renew-on-use
  TTL; `search_after` boundary pages under the one ranked order with page-invariant totals and
  HMAC-signed opaque cursors (fingerprint-checked resends); stale shapes are the one deliberate
  read-surface 409; enrichment stays current-view fail-closed; wire PIT/batch cursors deferred;
  no durable-format change (ADR-113).
- **Exhaustive job/stream delivery** — `POST /_percolate/jobs` runs exact `result_mode=all` on a
  dedicated bounded pool and returns a single-consumer NDJSON stream of fixed-size provisional
  chunks followed by one exact completion checksum. Per-member idempotency keys, fail-closed
  deadline/cancellation/sink semantics, bounded-memory legacy logical-id dedup, ownership-disjoint
  cluster fan-in with fail-closed partial-repair refusal plus a full-fan-out mutation barrier,
  pre-setup/zero-chunk/candidate/dedup-member/legacy-duplicate and ranked-metadata scan cancellation,
  deadline-bounded HTTP/gRPC backpressure,
  barrier-first mutation/repair lock ordering, score-presence-safe checksum attestation,
  post-final-send cancellation polling, non-runtime-worker-safe gRPC polling, tag-growth-stable raw
  request idempotency with ambiguous synthetic-boost collision rejection, non-destructive HTTP
  admission, completion-backpressure cancellation classification, pre-spawn shard-node admission,
  a server-owned shard-stream deadline ceiling,
  terminal-dequeue-gated completed status, populated-remote-reattach and partial-initial-bulk-load
  convergence refusal,
  boot-namespaced durable member keys, and the
  server-streaming `PercolateAll` shard RPC with fail-closed broad-evaluator validation are built;
  external broker durability remains an operator adapter over the reference at-least-once
  publisher. No persistence-format change (ADR-114).
- **Exact competitive pruning deliberately absent** — Increment 8's profiling gate did not show
  exact verification dominating delivery/orchestration, so no score-bound storage or
  high-score-first candidate visitation was added. Revisit only with phase-attributed workload
  evidence and the full exhaustive differential proof (ADR-115).
- **Explain** (`explain.rs`) — first-class; structured `ExplainDetail` over REST.

### Durability & storage

- **LSM write path** (`segment/`) — memtable + immutable segments + tombstones + score-based
  compaction with auto-triggers (ADR-004, ADR-009).
- **mmap'd `.seg` format** (v3–v7) + frozen hash tables (ADR-012/105/108/109); flat mmap'd logical-index
  columns + lazy on-disk source store → resident ~4.5 B/query (ADR-020, ADR-014).
- **WAL** (v6) — CRC-framed, crash recovery, configurable fsync (ADR-013/108); address-free logical
  deletes + per-segment dead-locals bitmaps make tombstones durable at the commit point (ADR-066);
  atomic upsert `PUT` (ADR-067); class-D op codes (ADR-068).
- **Durable bulk ingest** — segment = artifact, manifest = commit (ADR-017); per-item outcomes
  (ADR-018).
- **Fail-closed flush / compaction / reseal / recompile** — build the replacement durable before
  destroying what it replaces (ADR-051).
- **Versioned binary formats** — `RDCT`/`RTGD` headers, fail-loud decode, legacy blobs still read
  (ADR-057).
- **Compaction re-anchoring** — opt-in: a merge re-derives drifted covers; cluster no-op (ADR-056).

### Runtime, server & observability

- **Snapshot reads** — lock-free `ArcSwap<EngineSnapshot>` incl. vocab reads (ADR-016).
- **Per-segment skip filter** — cache-line blocked bloom (ADR-011).
- **Runtime config** — `EngineConfig` + ES-style `/_settings` dynamic subset (ADR-022).
- **Observability** — `EngineEvent`/`EngineMetrics`; durability failures are structured events →
  logs + Prometheus (ADR-021); per-segment introspection `/_cat/segments` (ADR-023).
- **HTTP server** (`bin/server/`) — ES-style REST ([`reference/api.md`](reference/api.md));
  production hardening (ADR-052); tag-value coercion + `maybe_flush` on PUT + per-request
  `include_broad` (ADR-073); opt-in bearer auth, default-deny on mutations (ADR-062); **cluster
  coordinator mode** `--cluster`, in-process or remote, cluster-atomic upsert (ADR-070);
  **cooperative match cancellation + bounded search concurrency** — an explicit `timeout_ms`
  stops the work at coarse boundaries, `--max-concurrent-searches` bounds pool occupancy, both
  defaults byte-identical (ADR-099); local and cluster v2 winner enrichment share
  `--max-ranked-enrichment-bytes` (16 MiB default, ADR-110); exhaustive jobs have their own
  worker pool, non-queuing concurrency quota, bounded channel, timeout, and retained-record cap;
  shard nodes separately bound direct `PercolateAll` workers before spawn and reject caller
  deadlines above a node-owned ceiling (ADR-114).
- **Gate & CI** — `check.sh` is the one gate, CI runs it (ADR-024); lean-core feature gate
  (ADR-028).
- **Security review** (Phase 0 item 5, ADR-089) — a [threat model](operations/threat-model.md) (trust
  boundaries, assets, adversary model, controls mapped to code, explicit v1 non-goals) + a Trivy
  container scan (`deploy/scan-image.sh`, triaged baseline: base-image CVEs only, none service-reachable)
  + the `_backup` client-`dest` finding dispositioned (auth-gated, non-root operator responsibility). The
  app deps stay `cargo audit`/`deny`-clean; no code-level vuln found.
- **M3 operational docs** (Tier 5 M3, no ADR — runbook extensions): the
  [DR runbook](operations/disaster-recovery.md) (RPO/RTO by mode + volume/quorum/whole-cluster
  loss flows), the [rolling-upgrade procedure](operations/rolling-upgrade.md) (compatibility-fence
  contract; the chart sets explicit `updateStrategy` + ships PodDisruptionBudgets), the
  [sizing guide](operations/sizing.md), [alerting](operations/alerting.md) + the shipped
  promtool-validated [`deploy/prometheus-alerts.yml`](../deploy/prometheus-alerts.yml), and the
  backup-restore Rehearsal drill.

### Vocabulary & aliases

- **Runtime vocab learning** + epoch staleness tracking (ADR-015); **NPMI corpus phrase
  induction**, additive, opt-in (ADR-053).
- **Equivalence (alias) expansion** — required → any-of, structurally FN-safe (ADR-054).
- **AliasRegistry governance** — provenance/kind/status, conservative single-token
  auto-activation, Solr import, the alias-ID-stability fix (ADR-060).
- **Distributional alias discovery** — PPMI-cosine context-similarity candidates over the stored
  queries, review-first (`LearnedDistributional` never auto-activates); metadata-only recording
  (no recompile), `POST /_vocab/aliases/discover[_and_record]` (ADR-102).
- **Match-feedback alias validation** — opt-in passive capture of the live match stream into
  per-candidate-pair behavioral evidence (bottom-k sketches + degenerate-evidence exclusion);
  `GET /_vocab/aliases/feedback` + `validate_and_apply` (explicit `activate=true`) (ADR-103).
- **Multi-word aliases** — two title-side views P(T)/N(T) (ADR-061); on a cluster via P(T)-aware
  routing + `build_with_vocab` (live cross-process vocab shipping decided-refused: deploy-time
  config) (ADR-076).
- **Dynamic vocabulary** — feature-hashing for post-freeze terms + blue/green vocab rebuild
  (ADR-046); works on a tagged cluster via TagId carry-through (ADR-074).

### Cluster v1 (built + oracle-proven, zero FN — the shippable milestone)

- **In-process multi-shard core** — one shared frozen dict, anchor ring, content routing with ~2–5
  shard fan-out (ADR-027).
- **Durable coordinator log + per-shard segments** — attach-and-mmap reopen, coordinator manifest
  = the atomic commit point (ADR-031, ADR-032); shared-nothing storage model (ADR-033).
- The third v1 pillar — **dynamic vocabulary** (ADR-046) — is listed under Vocabulary & aliases
  above.

### Distributed layers (experimental, localhost-proven; gRPC parts `distributed`-gated)

- **In-process replication** — `ReplicatedShard` primary + N replicas, in-sync-only failover, peer
  recovery (lean core, RF=1 default byte-identical; ADR-035).
- **Control-plane seam + allocator + autoscaler policy** — in-memory backend default (ADR-037);
  rendezvous shard→node map + minimal-movement `rebalance` (ADR-042); tick-driven policy, disabled
  by default (ADR-045). All lean core; the openraft backend below is gated.
- **Runtime cluster resize** — `num_shards` no longer fixed at construction: a blue/green rebuild
  re-places every live query under a fresh ring (the `set_vocab` machinery), in-process, durable
  (no manifest format bump), vocab/tags + dict fingerprint preserved; `recommended_shard_count` +
  `resize_to_recommended` + `POST /_cluster/resize`; the autoscaler split advisory now points at a
  real mechanism (ADR-078).
- **gRPC transport** — `ShardServer`/`RemoteShard` (ADR-029); dict fingerprint handshake
  (ADR-030); dict + tag-dict shipping at connect (ADR-034, ADR-055); tag-dict fingerprint on all
  six recovery RPCs (ADR-077); placement generation/configuration on every data/recovery boundary
  plus ownership-applied read attestation (ADR-109); bounded `PercolateTopK` and streaming
  `FetchMatches`, each under the request's absolute deadline and exact result cap (ADR-110);
  server-streaming `PercolateAll` validates bounded contiguous chunks plus a terminal ownership,
  total, and checksum attestation and has independent node-local worker admission plus a hard
  server-owned stream-duration ceiling (ADR-114).
- **gRPC transport resilience** — client connect-timeout + per-call deadlines + HTTP/2 keepalive
  (shared dial helper, so shard + control + Raft-peer links harden together), bounded fail-loud
  retry of idempotent reads on a transient error, and per-RPC transport metrics on cluster-mode
  `/_metrics`; a hung remote shard now fails the percolate loud (fail-closed, zero FN) instead of
  blocking the coordinator's fan-out forever (ADR-085).
- **Per-node Prometheus metrics** — opt-in `--metrics-addr` plaintext `/_metrics` on `shardserver` /
  `controlserver` (a lean std-only renderer + listener, no new dep): per-shard query count / memory /
  compaction backlog / cost-class, per-control Raft term/leader/log/membership; the coordinator adds a
  per-shard query gauge. Reads the lock-free snapshot / Raft handle ⇒ off every hot path; default-off
  ⇒ byte-identical. Helm + Compose wired (ADR-091). Plus **per-shard RPC latency histograms**
  (ADR-100): `reverse_rusty_shard_rpc_duration_seconds{shard,method,le}` timed at the gRPC handler
  boundary (percolate / percolate_ranked / ingest) — p95/p99 via `histogram_quantile()`. Plus
  **per-shard broad-lane cost counters** (ADR-101): the coordinator's
  `reverse_rusty_broad_*_total` names, `{shard}`-labeled, accumulated from each percolate's
  `MatchStats` at the same handler boundary. ADR-110 extends the histogram with top-K/fetch and adds
  fixed-cardinality shard hit/result-byte, source-byte, total-relation, cancellation, and cap-rejection
  counters; coordinator metrics record shard rows/bytes and enrichment rejections.
- **Replication + recovery over gRPC** — `FetchSegments`/`RecoverFrom` (ADR-036); per-shard
  translog, no-quiesce peer recovery, durable self-restart (ADR-039); retention leases + finalize
  (ADR-040) with lease TTL reaping (ADR-048).
- **openraft control plane** — gRPC `ControlService`, survives leader kill (ADR-038); durable Raft
  log + restart recovery (ADR-041); coordinator-facing `ClientControl` op + a thin stateless
  `RemoteControlPlane` client (`server --control-endpoint`, ADR-083 — off the matching hot path).
- **Coordinator routing by committed assignments + control failover** — opt-in
  `--route-by-assignments` makes the quorum the topology source of truth (position-preserving seed +
  fail-loud guard; resolve-only boot with just `--control-endpoint`); the control client fails over
  across the whole `--control-endpoint` list (ADR-086).
- **Live data-moving handoff** — swappable shard backing (ADR-043); peer-recover → fence → drain →
  flip under concurrent writes (ADR-044); auto-unfence-on-abort + autoscaler-driven (ADR-048).
- **Data-moving reassignment** — `reassign_and_move`/`rebalance_and_move` move a shard's data via
  `execute_handoff` then commit the new owner (move-then-commit), so a reassignment moves data and
  routing follows live + across a resolve-only restart; REST `POST /_cluster/reassign` +
  `rebalance {move:true}` (ADR-090). Closes the ADR-086 deferral.
- **Unattended re-point reconciler** — `reconcile` (idempotent, data-moving, continue-past-failure)
  converges the committed shard→node map to the HRW-desired placement automatically; the autoscaler's
  membership-drift arm drives the data-moving rebalance on a remote cluster (closing a latent ADR-086
  false negative); opt-in `--reconcile-interval-secs` loop + REST `POST /_cluster/reconcile`;
  default-off ⇒ byte-identical (ADR-092). **RF>1 group reconciliation** — a replicated
  position's whole group moves via `reassign_group_and_move` (fence the primary → freeze → re-establish
  every non-source member from the frozen source → swap the group composite → commit), dispatched by
  shape from `reconcile`/`rebalance_and_move` (ADR-094; the de-replication trap is oracle-proven dead).
  **Parallel multi-position moves** — the busy-endpoint move ledger (replacing the global
  `reassign_serial` mutex) + conflict-free waves; opt-in `max_parallel_moves` /
  `--reconcile-max-parallel`, default 1 = the sequential path byte-identically; also guards the raw
  REST handoff (ADR-095). **Orphan-slot GC** — `ListShards`/`DropShard` (fence-armed CAS +
  lease-guarded; rename-to-trash disk reclaim) + the ledger-reserved coordinator sweep with the
  committed-map + live-routing keep-set; opt-in `--reconcile-gc-orphans` / `POST /_cluster/gc`,
  default off (ADR-096).
- **Partial-apply repair** — typed `PartiallyApplied` + live `resync` (ADR-047).
- **Mesh security** — opt-in TLS + shared cluster token, constant-time default-deny interceptor on
  both planes (ADR-071).
- **Multi-machine harness** — compose-based kill-and-recover / rolling restarts / coordinator
  restart / live handoff under load, fully secured, runs in CI (`deploy/`, ADR-072).

### Correctness & test infrastructure

- **Differential oracles** — engine ≡ brute force across single-node, cluster (K×RF), gRPC,
  durability, control-plane, allocator, and autoscaler suites; zero FN/FP. Suite map →
  [`../CLAUDE.md`](../CLAUDE.md) + [`testing.md`](testing.md).
- **Deterministic generation + messy mode** (ADR-008, ADR-063); golden front-end pins (ADR-050);
  reference-free adversarial property suites (ADR-063).
- **Front-end-independent oracle** (Phase 0 item 2, ADR-087) — a std-only, zero-dependency reference
  matcher (`reverse-rusty-ref-matcher`) reimplements the parser/normalizer/extractor/predicate from
  the SPEC, reusing none of the engine (independence enforced by a `check.sh` `cargo tree` lane); the
  engine is diffed against it (`tests/independent_oracle/`) over default/populated/alias corpora + a
  gotcha table + an env-gated real corpus. Closes the ADR-050 shared-front-end blind spot for the
  covered paths; zero FN/FP, no engine front-end bug found.
- **Real-process SIGKILL crash injection** (Phase 0 item 3, ADR-088) — a `crashwriter` lean-core bin +
  `tests/crash_injection/` spawn a real process and deliver a real external SIGKILL mid
  durable-operation (WAL append / flush / compaction / backup / churn / **upsert** / **watermark**),
  reopen in-process, and diff the recovered engine against the ADR-087 independent oracle: zero false
  negatives on every acked write, no resurrection/corruption. The `upsert` leg proves ADR-067 atomic
  replace (race-immune both-version construction); the `watermark` leg proves the ADR-066
  `ensure_seq_after` re-pin across a SECOND reopen (non-redundant vs the single-reopen churn). Its
  **cluster** analogue is `deploy/harness.sh` leg 3b (kill a `shardserver` mid-write-loop; every 2xx-acked
  write matchable after restart + `/_cluster/resync`, ADR-047). Closes the real-kill-mid-syscall gap the
  chmod/torn-tail/CRC simulations cannot reach; `#[ignore]`d behind a `check.sh` crash lane
  (`RR_CRASH_ITERS`); mutation-validated.
- **Drop-in parity audit** — empirical PoC against the documented reference workload: zero FN
  under the parity configuration (ADR-064; workload →
  [`research/percolator-workload.md`](research/percolator-workload.md)).

## Measured

Headline figures only. Full tables, p99s, and the 100M extrapolation are the canonical record in
[`performance/results.md`](performance/results.md); the machine-independent regression invariants
live in [`performance/benchmark-results.txt`](performance/benchmark-results.txt).

- Selective path **~158k–710k titles/sec/core** (1M–5M queries; ~256 B/query), **~3.8× on 4 threads**.
- Flat **~54 candidates/title**, independent of corpus size.
- **~750k updates/sec/core** with immediate (epoch) visibility; build **~650k queries/sec/core**.
- LSM read-amplification stays bounded as segments grow (1→8) — table in
  [`performance/results.md`](performance/results.md) §7.
- **Resident memory:** ~148 → **~4.5 B/query** with `retain_source=false` (ADR-020).
- **20M-query multi-shard scale soak** (K=8, in-process, durable — ADR-104): cluster ≡
  single-node over 50k titles (0 mismatches, pre- and post-mutation), 0 sentinel misses / ghosts,
  checkpoint → reopen byte-identical; fan-out avg ~3.2, p99 5, max 7 of 8 — the bounded-fan-out +
  zero-FN claims hold at 20M ([`performance/benchmark-results.txt`](performance/benchmark-results.txt)).

## Roadmap at a glance

The prioritized roadmap (open work only) is **[`roadmap.md`](roadmap.md)**. **Current top
priority: the Broad-Query Cost Program** (adopted 2026-07-03; spec →
[`proposals/broad-cost-program.md`](proposals/broad-cost-program.md) — threshold reclassification +
the always-visible hot tier first, then staged duplicate interning). Tiers: **0** Cluster-v1
gate (✅ complete) · **1** measured bottlenecks (✅ complete) · **2** feature-model self-tuning
(open: the "improve" menu) · **3** scale & production maturity (open:
ADR-065 criterion 12, model versioning, aspects-first ingestion) · **4** percolator
parity (✅ program complete; small deferred refinements) · **5** deployability & operational
maturity (M0 deploy-truth + M1 local-smoke CI gate + M2 release pipeline ✅ shipped, ADR-098 — the
supported-deployment contract is [`operations/deployment-modes.md`](operations/deployment-modes.md),
releases are smoke-gated GHCR images; M3 hardening complete; open: M4 commercial ops) · the operational-polish backlog.

## Current limitations

*(The consolidated v1 non-goals — each with its deciding ADR — are the named-constraints table in
[`operations/deployment-modes.md`](operations/deployment-modes.md) §4, ADR-098.)*

- **Not yet a hardened multi-machine deployment.** The distributed layers are oracle-proven
  in-process / on localhost / in the containerized harness, but the Distributed-v1 graduation
  (ADR-065) is incomplete — one open criterion remnant: the **real-corpus FN/throughput audit**
  (the ≥20M multi-shard scale proof is shipped, ADR-104; deployment packaging +
  operations runbook shipped, ADR-081; Kubernetes/Helm chart + native gRPC health/readiness probes,
  ADR-084; replicate-broad-to-all + the cluster class-D lane, ADR-080; backup/restore, ADR-079). The
  shard + control gRPC servers now expose the standard `grpc.health.v1.Health` service on an opt-in
  plaintext `--health-addr` port for k8s probes (ADR-084). The coordinator attaches to the durable
  `controlserver` quorum via `--control-endpoint` (ADR-083 — the cluster-state document becomes
  durable + HA across coordinator restarts), fails over across the whole endpoint list, and (opt-in
  `--route-by-assignments`, ADR-086) routes by the committed shard→node assignments instead of its
  static `--shard-endpoint` list — making the quorum the topology source of truth. **Data-moving
  reassignment is built** (ADR-090): `POST /_cluster/reassign` (or `rebalance` with `{move:true}`)
  moves a shard's data + re-points routing live (move-then-commit); the bare map-only HRW `rebalance`
  stays map-only and must not be used alone to re-point a populated remote cluster. Mesh TLS + token
  auth are
  **opt-in** (ADR-071) — enable both outside a trusted network. Remote-cluster vocabulary is
  deploy-time configuration, not live-shipped (decided, ADR-076).
- **Multi-shard-per-node: all four ADR-093 stages BUILT.** A code review found the HRW data-moving
  *rebalance* (`rebalance_and_move`, ADR-090) and the unattended *reconciler* (ADR-092) silently
  overwrite — HRW packs several positions onto one node, but a one-shard server could hold only one.
  [ADR-093](decisions/adr-093-multi-shard-per-node.md) is the staged fix.
  **Stage 1 (foundation):** the transport carries a `shard_id`, a `ShardServer` is a shard-keyed slot map,
  and fence/recovery/storage (`shard_<id>/`) are **per-shard** — fixing the codex P1. **Stage 2
  (co-location):** the `AddShard` RPC + a per-endpoint adoption dedup in `connect_remote` let several
  positions share one endpoint (fewer pods than shards) without re-shipping the dict. **Stage 3 (per-shard
  relocation + RF>1 failover):** the relocation mechanics were already threaded by Stages 1–2, so Stage 3
  adds the `connect_replicated` co-location dedup + per-node `/_metrics` slot-aggregation, and — the
  load-bearing part — the gRPC oracle proving it: relocating one co-located shard leaves the node's others
  byte-identical + zero-FN (+ resolve-only and `open_durable` restarts), a full HRW `rebalance_and_move`
  over a packed K=6/N=3 topology converges (no slot lost, fixpoint), and RF>1 cross-replication (2 slots
  per node) survives a whole-node loss + peer-recovers onto a fresh node. **General HRW rebalance is now
  collision-safe.** **Stage 4 (the reconciler, ADR-092):** the parked unattended controller landed on
  this foundation, with the packed-K>N gRPC oracle proving the *reconciler itself* converges the exact
  topology that parked it — no slot lost, zero-FN, epoch-invariant idempotence, restart routes zero-FN.
  RF>1 data-moving reconciliation shipped as ADR-094 (the group move); parallel multi-position
  moves shipped as ADR-095 (the busy-endpoint ledger + waves, default sequential); orphan-slot GC
  shipped as ADR-096 (the moved-away slots are reclaimed, opt-in); the retained-member re-copy is
  fingerprint-skipped when provably complete as ADR-097 (staged recovery stays the one open
  fence-window deferral).
- **Empty default vocabulary.** `default_vocab()` ships no domain terms; vocabulary arrives at
  runtime via `Vocab`/`NormalizerBuilder` (learning: ADR-015/053; aliases: ADR-054/060/061).
- **Validated on synthetic + pinned-pair data, not a real corpus.** The oracle and benchmarks run
  the seeded adversarial generator (ADR-008/063); the ADR-064 PoC proved zero FN on pinned pairs
  under the parity configuration; the 20M multi-shard soak (ADR-104) proved the scale story on the
  synthetic corpus. The full **real-corpus false-negative / throughput audit** is still owed — the
  highest-leverage credibility step, and the open remnant of Distributed-v1 criterion 12 (ADR-065);
  the intake is the ADR-087 `RR_ORACLE_CORPUS` hook.
