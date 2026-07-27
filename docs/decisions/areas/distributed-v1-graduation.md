# Distributed v1 — graduation program decisions

> [Architecture decision hub](../../DECISIONS.md)

The reliability, deployment, security, operability, ranking, scale, and semantic hardening work
that followed the ADR-065 graduation program.

| ADR | Decision | Summary | Status |
|---|---|---|---|
| [070](../adr-070-cluster-rest-surface.md) | Cluster REST surface | Adds coordinator mode, cluster-atomic upserts, and fail-loud handling for unsupported request features. | Accepted |
| [071](../adr-071-grpc-tls-auth.md) | TLS + mesh authentication | Secures shard and control gRPC with optional TLS and constant-time shared-token authentication. | Accepted |
| [072](../adr-072-multi-machine-harness.md) | Container lifecycle harness | Exercises secure recovery, restart, and live handoff through public surfaces across a multi-process, single-host container network. | Accepted |
| [074](../adr-074-tagged-cluster-vocab-change.md) | Tagged-cluster vocabulary changes | Carries `TagId`s through vocabulary rebuilds and closes tagged source-durability gaps. | Accepted |
| [075](../adr-075-cluster-ranking.md) | Cluster ranking | Ranks within each shard, then deduplicates and merges scored results at the coordinator. | Accepted |
| [076](../adr-076-cluster-multiword-aliases-vocab-shipping.md) | Multi-word aliases + vocabulary boundary | Makes cluster routing positive-view-aware; remote shards remain stock-vocabulary-only because the wire does not ship normalizers. | Accepted |
| [077](../adr-077-tagdict-recovery-fingerprint.md) | Tag-dictionary recovery fingerprints | Attests tag spaces during recovery and fencing so divergence fails closed. | Accepted |
| [078](../adr-078-cluster-resize.md) | Cluster resize | Resizes in-process clusters through full blue/green re-placement under a new ring. | Accepted |
| [079](../adr-079-backup-restore.md) | Backup and restore | Provides consistent backups and restore-through-open for durable engines and in-process clusters. | Accepted |
| [080](../adr-080-cluster-replicate-broad-to-all.md) | Replicate broad queries to all shards | Replicates broad and class-D queries while evaluating the broad lane on one shard per title. | Accepted |
| [081](../adr-081-deployment-packaging-runbook.md) | Deployment packaging + runbook | Defines release images, compose topology, operator procedures, and smoke workflows. | Accepted |
| [082](../adr-082-packaging-deploy-correctness.md) | Packaging correctness follow-up | Adds control-plane advertise URLs and coordinator gating for class-D configuration. | Accepted |
| [083](../adr-083-control-plane-coordinator-wiring.md) | Control-plane coordinator wiring | Connects the coordinator to the control quorum without making it a Raft participant. | Accepted |
| [084](../adr-084-kubernetes-helm-health.md) | Kubernetes, Helm, and health | Adds Helm packaging and separate gRPC liveness and readiness endpoints. | Accepted |
| [085](../adr-085-grpc-transport-hardening.md) | gRPC transport hardening | Adds deadlines, keepalive, bounded read retries, and transport metrics. | Accepted |
| [086](../adr-086-control-plane-routing-and-failover.md) | Control routing + failover | Routes by committed assignments and fails over across control endpoints. | Accepted |
| [087](../adr-087-independent-correctness-oracle.md) | Independent correctness oracle | Adds an independent parser, normalizer, and matcher to catch shared-implementation errors. | Accepted |
| [088](../adr-088-crash-injection-harness.md) | Crash-injection harness | Exercises real-process crash points and recovery with external `SIGKILL`. | Accepted |
| [089](../adr-089-security-review.md) | Security review | Establishes the threat model and container-image security scanning. | Accepted |
| [091](../adr-091-shard-control-metrics.md) | Shard + control metrics | Exports lean per-node Prometheus metrics for shard and control processes. | Accepted |
| [092](../adr-092-unattended-reconciler.md) | Unattended reconciler | Converges committed placement after membership drift through data-moving repair. | Accepted |
| [093](../adr-093-multi-shard-per-node.md) | Multiple shards per node | Lets one shard server host multiple slots under a shared adopted feature space. | Accepted |
| [094](../adr-094-replicated-group-reassignment.md) | Replicated-group reassignment | Moves complete replicated shard groups while preserving RF>1 correctness. | Accepted |
| [095](../adr-095-parallel-multi-position-moves.md) | Parallel multi-position moves | Runs non-conflicting moves in parallel under an endpoint reservation ledger. | Accepted |
| [096](../adr-096-orphan-slot-gc.md) | Orphan-slot garbage collection | Lists and safely removes orphaned shard slots after reassignment. | Accepted |
| [097](../adr-097-content-fingerprint-skip.md) | Content-fingerprint copy skipping | Skips retained-member copies only when fingerprints prove the destination complete. | Accepted |
| [098](../adr-098-deployable-gate-and-release-pipeline.md) | Deployable gate + release pipeline | Defines the deployment matrix, smoke gates, and versioned release publishing. | Accepted |
| [099](../adr-099-cooperative-cancellation-bounded-concurrency.md) | Cooperative cancellation + bounded concurrency | Adds cooperative deadlines and bounded search admission without changing the unarmed hot path. | Done |
| [100](../adr-100-shard-rpc-latency-histogram.md) | Shard RPC latency histograms | Measures per-shard RPC latency with lean Prometheus histograms. | Done |
| [101](../adr-101-shard-broad-lane-cost-counters.md) | Broad-lane cost counters | Exports per-shard broad-lane work counters at the gRPC boundary. | Done |
| [102](../adr-102-distributional-alias-discovery.md) | Distributional alias discovery | Discovers review-first alias candidates from distributional evidence. | Accepted |
| [103](../adr-103-match-feedback-alias-validation.md) | Match-feedback alias validation | Validates alias candidates from behavioral overlap before optional activation. | Accepted |
| [104](../adr-104-cluster-scale-soak.md) | Multi-shard scale soak | Exercises 20 million queries across eight durable shards, live mutations, and reopen. | Done |
| [105](../adr-105-hot-tier-two-axis-placement.md) | Always-visible hot tier | Adds columnar hot-query evaluation while keeping visibility and scheduling independent. | Accepted |
| [106](../adr-106-canonical-body-dedup-stage-a.md) | Canonical-body deduplication | Shares postings for identical semantic bodies and regroups them during compaction. | Accepted |
| [107](../adr-107-ranked-percolation-result-contract.md) | Ranked result contract | Separates exact match truth from ranked delivery modes, totals, and termination. | Accepted |
| [108](../adr-108-typed-priority-local-bounded-ranking.md) | Typed priority + local bounded ranking | Adds typed priority and bounded local top-K ranked percolation. | Accepted |
| [109](../adr-109-deterministic-distributed-emission-ownership.md) | Deterministic emission ownership | Selects one emitting shard per logical match to eliminate duplicates. | Accepted |
| [110](../adr-110-distributed-top-k-query-then-fetch.md) | Distributed top-K + query-then-fetch | Adds bounded global top-K merging and winner-only source fetch. | Accepted |
| [111](../adr-111-typed-ranked-wire-errors.md) | Typed ranked wire errors | Carries typed ranked errors in gRPC metadata with a legacy fallback. | Accepted |
| [112](../adr-112-distributed-title-batching.md) | Distributed title batching | Batches per-title top-K work and deduplicates winner fetches across titles. | Accepted |
| [113](../adr-113-pit-cursor-pagination.md) | PIT + cursor pagination | Pins snapshots and signs cursor state with generation checks. | Accepted |
| [114](../adr-114-exhaustive-job-stream-delivery.md) | Exhaustive job + stream delivery | Adds bounded exhaustive delivery with idempotent chunks and terminal checksums. | Accepted |
| [115](../adr-115-competitive-pruning-deferred.md) | Competitive pruning deferred | Defers score-bound pruning because profiling did not justify its complexity. | **Declined** |
| [116](../adr-116-get-document-source-readback.md) | Document source readback | Persists source metadata and adds honest `GET` and `HEAD` document readback. | Accepted |
| [117](../adr-117-put-document-index-contract.md) | PUT document contract | Defines strict create/index semantics, refresh parsing, conflicts, and response metadata. | Accepted |
| [118](../adr-118-clause-boundary-compiler-semantics.md) | Clause-boundary compiler semantics | Compiles positive clauses separately and safely rebuilds legacy materializations. | Accepted |
| [119](../adr-119-multi-token-anyof-member-semantics.md) | Multi-token any-of semantics | Preserves OR-of-AND semantics for multi-token any-of members. | Accepted |
| [120](../adr-120-quoted-phrase-token-graph-semantics.md) | Quoted-phrase token graphs | Implements analyzed token-graph adjacency for required and forbidden phrases. | Accepted |
| [123](../adr-123-bounded-in-segment-cancellation.md) | Bounded in-segment cancellation | Bounds cancellation latency inside long segment scans. | Accepted |
| [124](../adr-124-variance-tolerant-performance-gate.md) | Variance-tolerant performance gate | Uses variance-aware regression gating with scheduled soak coverage. | Done |
| [125](../adr-125-delete-document-contract.md) | DELETE document contract | Defines strict refresh parsing, honest delete metadata, logical counts, and partial repair. | Accepted |

---

Shipped changes are recorded in [CHANGELOG.md](../../CHANGELOG.md); unfinished work belongs in
[roadmap.md](../../roadmap.md). Documentation placement rules live in
[the documentation hub](../../README.md).
