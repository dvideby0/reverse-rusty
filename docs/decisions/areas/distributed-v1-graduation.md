# Distributed v1 graduation decisions

> [Architecture decision hub](../../DECISIONS.md)

These decisions make up the ADR-065 graduation program and its later
distributed-v1 hardening work. This page is a navigation catalog; the linked
ADRs remain canonical for rationale, trade-offs, implementation details, and
validation evidence.

## Cluster foundations

- [ADR-070 — Cluster REST surface](../adr-070-cluster-rest-surface.md) — Accepted.
  Adds coordinator mode to the existing REST server, including cluster-atomic
  upserts and fail-loud unsupported features.
- [ADR-071 — TLS and mesh authentication](../adr-071-grpc-tls-auth.md) — Accepted.
  Secures shard and control gRPC with optional TLS and constant-time shared-token
  authentication.
- [ADR-072 — Multi-machine test harness](../adr-072-multi-machine-harness.md) —
  Accepted. Exercises secure multi-node recovery, restart, and live handoff
  through public surfaces in containers.
- [ADR-074 — Tagged-cluster vocabulary changes](../adr-074-tagged-cluster-vocab-change.md)
  — Accepted. Carries `TagId`s through vocabulary rebuilds and closes tagged
  source-durability gaps.
- [ADR-075 — Cluster ranking](../adr-075-cluster-ranking.md) — Accepted. Ranks
  within each shard, then deduplicates and merges scored results at the
  coordinator.
- [ADR-076 — Multi-word aliases and vocabulary shipping](../adr-076-cluster-multiword-aliases-vocab-shipping.md)
  — Accepted. Makes routing positive-view-aware for multi-word aliases and keeps
  remote vocabulary changes deploy-time only.
- [ADR-077 — Tag-dictionary recovery fingerprints](../adr-077-tagdict-recovery-fingerprint.md)
  — Accepted. Attests tag spaces during recovery and fencing so divergence
  fails closed.
- [ADR-078 — Cluster resize](../adr-078-cluster-resize.md) — Accepted. Resizes
  in-process clusters through full blue/green re-placement under a new ring.
- [ADR-079 — Backup and restore](../adr-079-backup-restore.md) — Accepted.
  Provides engine-driven consistent backups, verification, and
  restore-through-open.
- [ADR-080 — Replicate broad queries to all shards](../adr-080-cluster-replicate-broad-to-all.md)
  — Accepted. Replicates broad and class-D queries to every shard while
  evaluating that lane on one shard per title.

## Deployment and control-plane hardening

- [ADR-081 — Deployment packaging and operations runbook](../adr-081-deployment-packaging-runbook.md)
  — Accepted. Defines production images, compose topology, operator procedures,
  and smoke workflows.
- [ADR-082 — Packaging deploy-correctness follow-up](../adr-082-packaging-deploy-correctness.md)
  — Accepted. Adds control-plane advertise URLs and coordinator gating for
  class-D configuration.
- [ADR-083 — Control-plane coordinator wiring](../adr-083-control-plane-coordinator-wiring.md)
  — Accepted. Connects the coordinator to the control quorum without making it
  a Raft participant.
- [ADR-084 — Kubernetes, Helm, and health endpoints](../adr-084-kubernetes-helm-health.md)
  — Accepted. Adds Helm packaging and separate gRPC liveness and readiness
  endpoints.
- [ADR-085 — gRPC transport hardening](../adr-085-grpc-transport-hardening.md) —
  Accepted. Adds deadlines, keepalive, bounded retries, and transport metrics.
- [ADR-086 — Control-plane routing and failover](../adr-086-control-plane-routing-and-failover.md)
  — Accepted. Routes by committed shard assignments and fails over across
  control endpoints.
- [ADR-087 — Independent correctness oracle](../adr-087-independent-correctness-oracle.md)
  — Accepted. Adds an independent parser, normalizer, and matcher to catch
  shared-implementation errors.
- [ADR-088 — Crash-injection harness](../adr-088-crash-injection-harness.md) —
  Accepted. Exercises real-process crash points and recovery with `SIGKILL`.
- [ADR-089 — Security review](../adr-089-security-review.md) — Accepted.
  Establishes the threat model and container-image security scanning.
- [ADR-091 — Shard and control metrics](../adr-091-shard-control-metrics.md) —
  Accepted. Exports lean per-node Prometheus metrics for shard and control
  processes.
- [ADR-092 — Unattended reconciler](../adr-092-unattended-reconciler.md) —
  Accepted. Converges committed placement after membership drift.
- [ADR-093 — Multiple shards per node](../adr-093-multi-shard-per-node.md) —
  Accepted. Lets one shard server host multiple slots under a shared adopted
  feature space.
- [ADR-094 — Replicated-group reassignment](../adr-094-replicated-group-reassignment.md)
  — Accepted. Moves complete replicated shard groups while preserving RF>1
  correctness.
- [ADR-095 — Parallel multi-position moves](../adr-095-parallel-multi-position-moves.md)
  — Accepted. Runs non-conflicting moves in parallel under an endpoint
  reservation ledger.
- [ADR-096 — Orphan-slot garbage collection](../adr-096-orphan-slot-gc.md) —
  Accepted. Lists and safely removes orphaned shard slots after reassignment.
- [ADR-097 — Content-fingerprint copy skipping](../adr-097-content-fingerprint-skip.md)
  — Accepted. Skips retained-member copies only when fingerprints prove the
  destination complete.
- [ADR-098 — Deployable gate and release pipeline](../adr-098-deployable-gate-and-release-pipeline.md)
  — Accepted. Defines the deployment matrix, smoke gates, and versioned release
  publishing.

## Runtime performance and observability

- [ADR-099 — Cooperative cancellation and bounded concurrency](../adr-099-cooperative-cancellation-bounded-concurrency.md)
  — Done. Adds cooperative deadlines and bounded search admission without
  changing the unarmed hot path.
- [ADR-100 — Shard RPC latency histograms](../adr-100-shard-rpc-latency-histogram.md)
  — Done. Measures per-shard RPC latency with lean Prometheus histograms.
- [ADR-101 — Broad-lane cost counters](../adr-101-shard-broad-lane-cost-counters.md)
  — Done. Exports per-shard broad-lane work counters at the gRPC boundary.
- [ADR-102 — Distributional alias discovery](../adr-102-distributional-alias-discovery.md)
  — Done. Discovers review-first alias candidates from distributional evidence.
- [ADR-103 — Match-feedback alias validation](../adr-103-match-feedback-alias-validation.md)
  — Done. Validates alias candidates from behavioral overlap before optional
  activation.
- [ADR-104 — Multi-shard scale soak](../adr-104-cluster-scale-soak.md) — Done.
  Exercises 20 million queries across eight durable shards, live mutations, and
  reopen.
- [ADR-105 — Always-visible hot tier](../adr-105-hot-tier-two-axis-placement.md)
  — Done. Adds columnar hot-query evaluation while keeping visibility and
  scheduling independent.
- [ADR-106 — Canonical-body deduplication](../adr-106-canonical-body-dedup-stage-a.md)
  — Done. Shares postings for identical semantic bodies and regroups them during
  compaction.

## Ranked delivery and query semantics

- [ADR-107 — Ranked result contract](../adr-107-ranked-percolation-result-contract.md)
  — Done. Separates exact match truth from ranked delivery modes, totals, and
  termination.
- [ADR-108 — Typed priority and local bounded ranking](../adr-108-typed-priority-local-bounded-ranking.md)
  — Done. Adds typed priority and bounded local top-K ranked percolation.
- [ADR-109 — Deterministic distributed emission ownership](../adr-109-deterministic-distributed-emission-ownership.md)
  — Done. Selects one emitting shard per logical match to eliminate duplicates.
- [ADR-110 — Distributed top-K and query-then-fetch](../adr-110-distributed-top-k-query-then-fetch.md)
  — Done. Adds bounded global top-K merging and winner-only source fetch.
- [ADR-111 — Typed ranked wire errors](../adr-111-typed-ranked-wire-errors.md) —
  Done. Carries typed ranked errors in gRPC metadata with a legacy fallback.
- [ADR-112 — Distributed title batching](../adr-112-distributed-title-batching.md)
  — Done. Batches per-title top-K work and deduplicates winner fetches across
  titles.
- [ADR-113 — PIT and cursor pagination](../adr-113-pit-cursor-pagination.md) —
  Done. Pins snapshots and signs cursor state with generation checks.
- [ADR-114 — Exhaustive job and stream delivery](../adr-114-exhaustive-job-stream-delivery.md)
  — Done. Adds bounded exhaustive delivery with idempotent chunks and terminal
  checksums.
- [ADR-115 — Competitive pruning deferred](../adr-115-competitive-pruning-deferred.md)
  — Declined. Defers score-bound pruning because profiling did not justify its
  complexity.
- [ADR-116 — Document source readback](../adr-116-get-document-source-readback.md)
  — Accepted. Persists source metadata and adds honest `GET` and `HEAD`
  document readback.
- [ADR-117 — PUT document contract](../adr-117-put-document-index-contract.md) —
  Accepted. Defines strict create/index semantics, refresh parsing, conflicts,
  and response metadata.
- [ADR-118 — Clause-boundary compiler semantics](../adr-118-clause-boundary-compiler-semantics.md)
  — Accepted. Compiles positive clauses in separate runs and safely rebuilds
  legacy materializations.
- [ADR-119 — Multi-token any-of semantics](../adr-119-multi-token-anyof-member-semantics.md)
  — Accepted. Preserves OR-of-AND semantics for multi-token any-of members.
- [ADR-120 — Quoted-phrase token graphs](../adr-120-quoted-phrase-token-graph-semantics.md)
  — Accepted. Implements analyzed token-graph adjacency for required and
  forbidden phrases.

## Follow-up hardening

- [ADR-123 — Bounded in-segment cancellation](../adr-123-bounded-in-segment-cancellation.md)
  — Accepted. Bounds cancellation latency inside long segment scans.
- [ADR-124 — Variance-tolerant performance gate](../adr-124-variance-tolerant-performance-gate.md)
  — Accepted. Uses variance-aware regression gating with scheduled soak
  coverage.

---

Implementation status belongs in [STATUS.md](../../STATUS.md). Documentation
placement rules belong in [the documentation hub](../../README.md).
