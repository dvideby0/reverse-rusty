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
  postings ~1/batch_size, exposed as `/_mpercolate` (ADR-026).
- **Class-D always-candidate lane** — opt-in `accept_class_d` stores negation-only queries under
  the universal signature; default off = the loud reject (ADR-068).
- **Per-query tags + filtered percolation** — verify-stage filter, never gates (ADR-049); threaded
  end-to-end through the cluster (ADR-055).
- **Ranking + pagination** — opt-in post-match `Σ boosts + priority-tag value`, `from`/`size`
  (ADR-059); cluster rank-at-shard with compile-once-fan (ADR-075).
- **Explain** (`explain.rs`) — first-class; structured `ExplainDetail` over REST.

### Durability & storage

- **LSM write path** (`segment/`) — memtable + immutable segments + tombstones + score-based
  compaction with auto-triggers (ADR-004, ADR-009).
- **mmap'd `.seg` format** (v3/v4) + frozen hash tables (ADR-012); flat mmap'd logical-index
  columns + lazy on-disk source store → resident ~4.5 B/query (ADR-020, ADR-014).
- **WAL** (v5) — CRC-framed, crash recovery, configurable fsync (ADR-013); address-free logical
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
  coordinator mode** `--cluster`, in-process or remote, cluster-atomic upsert (ADR-070).
- **Gate & CI** — `check.sh` is the one gate, CI runs it (ADR-024); lean-core feature gate
  (ADR-028).

### Vocabulary & aliases

- **Runtime vocab learning** + epoch staleness tracking (ADR-015); **NPMI corpus phrase
  induction**, additive, opt-in (ADR-053).
- **Equivalence (alias) expansion** — required → any-of, structurally FN-safe (ADR-054).
- **AliasRegistry governance** — provenance/kind/status, conservative single-token
  auto-activation, Solr import, the alias-ID-stability fix (ADR-060).
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
  six recovery RPCs (ADR-077).
- **gRPC transport resilience** — client connect-timeout + per-call deadlines + HTTP/2 keepalive
  (shared dial helper, so shard + control + Raft-peer links harden together), bounded fail-loud
  retry of idempotent reads on a transient error, and per-RPC transport metrics on cluster-mode
  `/_metrics`; a hung remote shard now fails the percolate loud (fail-closed, zero FN) instead of
  blocking the coordinator's fan-out forever (ADR-085).
- **Replication + recovery over gRPC** — `FetchSegments`/`RecoverFrom` (ADR-036); per-shard
  translog, no-quiesce peer recovery, durable self-restart (ADR-039); retention leases + finalize
  (ADR-040) with lease TTL reaping (ADR-048).
- **openraft control plane** — gRPC `ControlService`, survives leader kill (ADR-038); durable Raft
  log + restart recovery (ADR-041); coordinator-facing `ClientControl` op + a thin stateless
  `RemoteControlPlane` client (`server --control-endpoint`, ADR-083 — off the matching hot path).
- **Coordinator routing by committed assignments + control failover** — opt-in
  `--route-by-assignments` makes the quorum the topology source of truth (position-preserving seed +
  fail-loud guard; resolve-only boot with just `--control-endpoint`); the control client fails over
  across the whole `--control-endpoint` list (ADR-086). Data-moving live re-pointing deferred.
- **Live data-moving handoff** — swappable shard backing (ADR-043); peer-recover → fence → drain →
  flip under concurrent writes (ADR-044); auto-unfence-on-abort + autoscaler-driven (ADR-048).
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

## Roadmap at a glance

The prioritized roadmap (open work only) is **[`roadmap.md`](roadmap.md)**. Tiers: **0** Cluster-v1
gate (✅ complete) · **1** measured bottlenecks (✅ complete) · **2** feature-model self-tuning
(open: alias-discovery sources, the "improve" menu) · **3** scale & production maturity (open:
ADR-065 criterion 12, model versioning, aspects-first ingestion) · **4** percolator
parity (✅ program complete; small deferred refinements) · the operational-polish backlog.

## Current limitations

- **Not yet a hardened multi-machine deployment.** The distributed layers are oracle-proven
  in-process / on localhost / in the containerized harness, but the Distributed-v1 graduation
  (ADR-065) is incomplete — one open criterion: the ≥20M scale proof (deployment packaging +
  operations runbook shipped, ADR-081; Kubernetes/Helm chart + native gRPC health/readiness probes,
  ADR-084; replicate-broad-to-all + the cluster class-D lane, ADR-080; backup/restore, ADR-079). The
  shard + control gRPC servers now expose the standard `grpc.health.v1.Health` service on an opt-in
  plaintext `--health-addr` port for k8s probes (ADR-084). The coordinator attaches to the durable
  `controlserver` quorum via `--control-endpoint` (ADR-083 — the cluster-state document becomes
  durable + HA across coordinator restarts), fails over across the whole endpoint list, and (opt-in
  `--route-by-assignments`, ADR-086) routes by the committed shard→node assignments instead of its
  static `--shard-endpoint` list — making the quorum the topology source of truth. **Data-moving
  reassignment is still deferred:** a non-data-moving HRW `rebalance` must not be used to re-point
  routing on a populated cluster (that needs live handoff). Mesh TLS + token auth are
  **opt-in** (ADR-071) — enable both outside a trusted network. Remote-cluster vocabulary is
  deploy-time configuration, not live-shipped (decided, ADR-076).
- **Empty default vocabulary.** `default_vocab()` ships no domain terms; vocabulary arrives at
  runtime via `Vocab`/`NormalizerBuilder` (learning: ADR-015/053; aliases: ADR-054/060/061).
- **Validated on synthetic + pinned-pair data, not a real corpus.** The oracle and benchmarks run
  the seeded adversarial generator (ADR-008/063); the ADR-064 PoC proved zero FN on pinned pairs
  under the parity configuration. The full **real-corpus false-negative / throughput audit** is
  still owed — the highest-leverage credibility step, and Distributed-v1 criterion 12 (ADR-065).
