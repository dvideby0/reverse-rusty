# Changelog

This is the chronological record of material changes shipped in Reverse Rusty. Entries are
reverse chronological and describe outcomes, not the current architecture or future plans.

- Current design → [design documentation](design/README.md)
- Current API and DSL → [reference documentation](reference/api.md)
- Decision rationale and proof → [ADR hub](DECISIONS.md)
- Unfinished ideas and priorities → [roadmap](roadmap.md)
- Exact performance captures → [performance results](performance/results.md)

## 2026-08-05 — Dependency-gate qualification

- Qualified `RUSTSEC-2026-0235` as an inactive optional `rkyv` lockfile edge, retained the
  lockfile-wide RustSec scan, and added an all-feature/all-target graph guard that fails before any
  `rkyv` version can enter a shipped build under the exception
  ([ADR-168](decisions/adr-168-inactive-rkyv-advisory.md)).

## 2026-07-26 to 2026-07-29 — Documentation, module boundaries, and API parity

- Replaced the monolithic ADR index with an area hub, nine compact catalogs, and one canonical
  page per ADR.
- Split large Rust implementation and test files along existing responsibility boundaries without
  changing public behavior.
- Standardized every ADR area catalog on the same four-column table and reduced its summary cells
  to short outcome statements.
- Reframed project tracking: this changelog owns shipped history, while the roadmap owns unfinished
  work and its full proposal text.
- Added startup-loaded, fingerprinted `static_v1`, linear, and quantized-tree CPU ranking profiles
  for native bounded/exhaustive delivery, with deterministic post-match scoring, strict model
  bounds, title-dependent batch support, pre-dedup semantic feature persistence plus source-driven
  legacy migration, benchmark selection, and fail-loud remote-wire refusal
  ([ADR-162](decisions/adr-162-versioned-cpu-ranking-profiles.md)).
- Extended named CPU profiles across remote top-K, batch, and exhaustive gRPC delivery with
  request/terminal fingerprint attestation, fail-closed version skew, shared Compose mounts, and
  Helm ConfigMap or generic PVC/CSI-capable volume sources
  ([ADR-163](decisions/adr-163-distributed-ranking-profile-attestation.md)).
- Aligned document deletion with the ES/OpenSearch shape and refresh controls, made logical delete
  counts placement-independent, and exposed the existing remote partial-repair contract accurately
  ([ADR-125](decisions/adr-125-delete-document-contract.md)).
- Hardened compatibility `GET`/`POST /_search` with strict native/ES request parsing, supported
  ES/OS controls and response identity, snapshot-generation-safe enrichment, and complete
  multi-document profile semantics ([ADR-126](decisions/adr-126-search-api-contract.md)).
- Hardened exact bounded `POST /v2/_search` with strict request parsing, honest ES/OS control
  aliases and timing fields, structured extractor failures, and mutation-fenced cluster winner
  enrichment ([ADR-127](decisions/adr-127-v2-search-api-contract.md)).
- Hardened exact bounded `POST /v2/_mpercolate` with a strict shared-options envelope, truthful
  ES/OS control aliases and batch timing/status fields, structured extractor failures, and
  mutation-fenced cluster union enrichment
  ([ADR-128](decisions/adr-128-v2-mpercolate-api-contract.md)).
- Hardened `POST /v2/_pit` with strict body/query controls, ES/OpenSearch keep-alive and
  fail-loud partial-creation aliases, structured extractor failures, and a dual-dialect response
  carrying both token names, creation time, and truthful shard counts
  ([ADR-129](decisions/adr-129-v2-open-pit-api-contract.md)).
- Hardened `DELETE /v2/_pit` with strict ES/OpenSearch/native scalar and batch identities,
  pre-decode body bounds, all-token pre-validation, structured extractor failures, and a truthful
  response carrying aggregate, per-PIT, and logical-context release results
  ([ADR-130](decisions/adr-130-v2-close-pit-api-contract.md)).
- Hardened `POST /_percolate/jobs` with strict bounded input, optional server-generated identity,
  native and ES/OpenSearch execution-control aliases, fail-loud unsupported async controls, and
  familiar async identity/status fields without weakening exact terminal delivery
  ([ADR-131](decisions/adr-131-exhaustive-job-create-api-contract.md)).
- Hardened `GET /_percolate/jobs/{id}` with strict bounded async waiting, fail-loud retention
  controls, no-store caching, and native plus familiar status/timing/error fields while preserving
  terminal stream attestation
  ([ADR-132](decisions/adr-132-exhaustive-job-status-api-contract.md)).
- Hardened `DELETE /_percolate/jobs/{id}` with strict input, a native/ES-compatible acknowledged
  response, cooperative running cancellation, and atomic terminal record plus event-id removal
  ([ADR-133](decisions/adr-133-exhaustive-job-delete-api-contract.md)).
- Hardened `GET /_percolate/jobs/{id}/stream` with strict query-free single-consumer semantics,
  cache-safe newline-delimited responses, pre-claim HEAD rejection, and standalone/coordinator
  route parity while keeping the terminally attested protocol explicitly native
  ([ADR-134](decisions/adr-134-exhaustive-job-stream-api-contract.md)).
- Hardened full-result `POST /_mpercolate` with one strict native/ES-shaped request, truthful
  source/timeout/fail-closed controls and batch timing/status fields, generation-consistent
  standalone enrichment, and an explicit coordinator profile boundary
  ([ADR-135](decisions/adr-135-mpercolate-api-contract.md)).
- Hardened `POST /_bulk` with strict NDJSON framing and controls, consistent ordered
  replace-or-create/create-only semantics, source-version and response metadata preservation, and a
  safe fresh-corpus immutable-segment fast path
  ([ADR-136](decisions/adr-136-bulk-api-contract.md)).
- Hardened `GET`/`POST /_flush` with strict body-free controls, exact non-waiting admission,
  ES/OpenSearch shard results, shared standalone/coordinator metrics, and fail-loud local-shard
  durability
  ([ADR-137](decisions/adr-137-flush-api-contract.md)).
- Made native `POST /_compact` actually force all sealed segments, added strict
  Elasticsearch/OpenSearch-familiar `POST /_forcemerge` controls and shard results, moved merge work
  off async runtime workers, and preserved fail-closed rollback
  ([ADR-138](decisions/adr-138-compaction-api-contract.md)).
- Hardened native `POST /_backup` with one strict bounded standalone/coordinator contract,
  synchronous timing and checkpoint-epoch results, single-slot blocking-worker admission that
  survives disconnects with independently supervised outcomes, unique staging, and fail-closed
  atomic no-clobber promotion that refuses dangling or raced destination entries
  ([ADR-139](decisions/adr-139-backup-api-contract.md)).
- Hardened native `POST /_checkpoint` with strict bounded transport, supervised off-runtime
  durability work shared with backup, no-store telemetry, fail-loud persistence errors, and
  explicit `durable`/`shards_checkpointed` results that cannot disguise a stateless coordinator
  maintenance no-op as a recovery point
  ([ADR-161](decisions/adr-161-checkpoint-api-contract.md)).
- Hardened coordinator `GET`/`HEAD /_cluster/state` with strict bounded no-store transport,
  authoritative off-runtime reads, shared introspection admission, sanitized fail-loud errors, an
  exact familiar `version` projection and manager-timeout aliases, and explicit rejection of
  nonexistent index-state semantics
  ([ADR-162](decisions/adr-162-cluster-state-api-contract.md)).
- Hardened native coordinator `POST /_cluster/nodes` with strict endpoint identity and mesh-origin
  validation, exact committed versions, bounded off-runtime consensus writes, outcome-aware
  timeouts, sanitized failures, and explicit separation from voter membership, placement, and data
  movement ([ADR-164](decisions/adr-164-node-registration-api-contract.md)).
- Hardened native coordinator `DELETE /_cluster/nodes/{id}` with strict bodyless identity, exact
  committed versions, reserved bootstrap and in-use voter/assignment protection, bounded
  off-runtime consensus writes, outcome-aware timeouts, sanitized failures, and explicit
  separation from voter membership, placement, data movement, and safe node shutdown
  ([ADR-165](decisions/adr-165-node-deregistration-api-contract.md)).
- Hardened native coordinator `POST /_cluster/rebalance` with strict bounded transport,
  topology-safe defaults that move data before committing resolve-only remote routing and reject
  restart-unsafe CLI-seeded or non-authoritative static routing, positive conflict-free parallelism, one
  supervised off-runtime workflow, manager-start timeouts, final control-state attestation,
  resumable partial reports, shutdown-budget deployment controls, sanitized failures, and an
  explicit non-reroute ES/OpenSearch boundary
  ([ADR-166](decisions/adr-166-cluster-rebalance-api-contract.md)).
- Hardened native `GET /_stats` with a strict no-store transport, truthful physical/live/tombstone
  and resident-memory/WAL projections, familiar timing and shard metadata, single-slot blocking
  collection, fail-loud cluster aggregation, and one shard-count fan-out instead of two
  ([ADR-140](decisions/adr-140-stats-api-contract.md)).
- Reworked native `GET /_cat/stats` into a truthful `metric` / `value` table with strict
  text/JSON, header, column, help, and sort controls; shared its corpus-wide collection admission
  with `/_stats` and moved the scan off async workers
  ([ADR-141](decisions/adr-141-cat-stats-api-contract.md)).
- Hardened native `GET /_cat/segments` with strict bodyless transport, no-store responses, shared
  CAT header/column/help/sort rendering, numeric byte-unit controls, consistent string-valued JSON,
  and honest LSM fields plus exact ES/OpenSearch aliases
  ([ADR-142](decisions/adr-142-cat-segments-api-contract.md)).
- Hardened coordinator `GET /_cat/shards` with strict shared CAT controls, no-store responses,
  bounded blocking-worker admission, string-valued JSON, and fail-loud shard/topology collection
  that no longer disguises control-plane failure as empty node assignments
  ([ADR-143](decisions/adr-143-cat-shards-api-contract.md)).
- Hardened native `GET`/`HEAD /_health` with strict bounded transport, familiar status waiting,
  fail-loud HTTP readiness, complete coordinator serving/control-plane attestation, blocking-worker
  admission, independently bounded pre-body unauthenticated requests and body-read deadlines,
  sanitized failures, deadline-checked observations, and whole-route no-store telemetry
  ([ADR-144](decisions/adr-144-health-api-contract.md)).
- Hardened native `GET`/`HEAD /_metrics` with strict bounded no-store transport, Prometheus text
  0.0.4 semantics, whole-route telemetry, lock-free standalone snapshots, and fail-loud
  coordinator collection that runs one complete shard-count pass off async workers and removes
  stale per-position labels
  ([ADR-145](decisions/adr-145-metrics-api-contract.md)).
- Hardened native `GET`/`HEAD /_vocab` with strict bounded no-store transport, complete
  round-trippable JSON, GET-only request limits, whole-route telemetry, shared blocking-work
  admission, lock-free standalone snapshot capture, and brief off-runtime coordinator locking
  ([ADR-146](decisions/adr-146-get-vocab-api-contract.md)).
- Hardened native `PUT /_vocab` with strict bounded JSON transport, synchronous timing and
  standalone/coordinator response parity, shared off-runtime rebuild admission, complete
  post-recompile verification, and fail-loud durable acknowledgement
  ([ADR-147](decisions/adr-147-put-vocab-api-contract.md)).
- Hardened native `POST /_vocab/learn` with one strict caller-corpus contract in standalone and
  coordinator modes, distinct-query evidence counting, bounded DSL/config/input validation, shared
  blocking-work admission, round-trippable no-store output, and explicit separation from
  ES/OpenSearch synonym management
  ([ADR-148](decisions/adr-148-vocab-learn-api-contract.md)).
- Hardened native `POST /_vocab/learn_and_apply` with strict bodyless controls, timed
  standalone/coordinator response parity, shared off-runtime rebuild admission, complete standalone
  post-recompile verification, and fail-loud durable acknowledgement
  ([ADR-149](decisions/adr-149-vocab-learn-apply-api-contract.md)).
- Hardened native `GET`/`HEAD /_vocab/aliases` with strict bounded no-store transport, familiar
  `from`/`size` review paging, total `count`, whole-registry summaries, shared blocking-work
  admission, lock-free standalone snapshot capture, and brief off-runtime coordinator locking
  ([ADR-150](decisions/adr-150-alias-registry-read-api-contract.md)).
- Split the unchanged core and distributed release/LTO code-gate commands across independent CI
  runners, retained the complete local `check.sh` entry point and one required aggregate result,
  and stopped producing empty test harnesses for binary targets without binary-local tests
  ([ADR-151](decisions/adr-151-parallel-ci-code-gate-lanes.md)).
- Hardened native `POST /_vocab/aliases/import` with strict atomic Solr parsing, familiar
  Elasticsearch rule objects and synchronous refresh plus OpenSearch Solr/expansion controls,
  bounded no-store transport, true no-op retries that finish pending engine, control-plane, and
  durable coordinator state without overwriting incompatible manifests, timed
  standalone/coordinator parity, shared off-runtime mutation admission, complete standalone rebuild
  verification, and fail-loud durable acknowledgement
  ([ADR-152](decisions/adr-152-alias-import-api-contract.md)).
- Hardened native `POST /_vocab/aliases/learn_and_apply` with strict bodyless evidence controls,
  bounded no-store transport, timed standalone/coordinator response parity, shared off-runtime
  corpus/rebuild admission, complete standalone rebuild verification, fail-loud durable
  acknowledgement, and an explicit native boundary from Elasticsearch/OpenSearch synonym management
  ([ADR-153](decisions/adr-153-alias-learn-apply-api-contract.md)).
- Hardened native `POST /_vocab/aliases/discover` with strict optional-JSON transport, validated
  distinct-query evidence and bounded controls, timed no-store standalone/coordinator parity,
  shared off-runtime admission, brief stored-source capture, deterministic response limits, and an
  explicit native boundary from Elasticsearch/OpenSearch synonym management
  ([ADR-154](decisions/adr-154-alias-discover-api-contract.md)).
- Hardened native `POST /_vocab/aliases/discover_and_record` with strict controls-only transport,
  timed no-store output, truthful live-only persistence, shared off-runtime admission, brief
  source capture and registry installation locks, success-only snapshot publication, and a
  validated fail-loud coordinator alternative
  ([ADR-155](decisions/adr-155-alias-discover-record-api-contract.md)).
- Hardened native `GET`/`HEAD /_vocab/aliases/feedback` with strict positive evidence controls,
  familiar bounded `from`/`size` paging, total counts, timed no-store output, shared off-runtime
  admission, page-only evidence snapshots, and an observed fail-loud coordinator alternative
  ([ADR-156](decisions/adr-156-alias-feedback-read-api-contract.md)).
- Hardened native `POST /_vocab/aliases/feedback/reset` with strict bounded bodyless transport,
  timed no-store output, shared off-runtime admission, a linearizable in-place evidence clear that
  preserves tracked candidates, and an observed fail-loud coordinator alternative
  ([ADR-157](decisions/adr-157-alias-feedback-reset-api-contract.md)).
- Hardened native `POST /_vocab/aliases/validate_and_apply` with strict positive evidence
  controls, bounded bodyless transport, timed no-store output, idempotent stamping, shared
  off-runtime admission, success-only publication, fail-loud activation durability, and a
  validated coordinator alternative
  ([ADR-158](decisions/adr-158-alias-feedback-validate-apply-api-contract.md)).
- Hardened native `GET`/`HEAD /_settings` with strict familiar controls, bounded bodyless
  transport, no-store telemetry, shared off-runtime admission and serialization, and coordinator
  lock/default parity while keeping its native compatibility boundary explicit
  ([ADR-159](decisions/adr-159-get-settings-api-contract.md)).
- Hardened native `PUT /_settings` with strict duplicate-safe JSON, bounded familiar controls,
  no-store telemetry, shared off-runtime admission and lock waiting, and coherent mutation/snapshot
  publication while preserving the explicit live-only and coordinator boundaries
  ([ADR-160](decisions/adr-160-put-settings-api-contract.md)).

## 2026-07-25 — Semantic correctness, durability, and performance gates

- Fixed clause-boundary lowering so aliases, phrases, and numeric context cannot leak
  across intervening query clauses ([ADR-118](decisions/adr-118-clause-boundary-compiler-semantics.md)).
- Preserved OR-of-AND semantics for multi-token any-of members
  ([ADR-119](decisions/adr-119-multi-token-anyof-member-semantics.md)).
- Made quoted phrases exact analyzed token-graph adjacency predicates
  ([ADR-120](decisions/adr-120-quoted-phrase-token-graph-semantics.md)).
- Made source sidecar replacement atomic with the manifest-selected segment set
  ([ADR-121](decisions/adr-121-atomic-source-sidecar-commit.md)).
- Rejected stale positional tombstone addresses before WAL append
  ([ADR-122](decisions/adr-122-fail-closed-positional-tombstones.md)).
- Bounded cooperative cancellation latency inside dense segment scans
  ([ADR-123](decisions/adr-123-bounded-in-segment-cancellation.md)).
- Added a variance-aware merge-blocking performance gate and a scheduled 10M-query soak
  ([ADR-124](decisions/adr-124-variance-tolerant-performance-gate.md)).
- Hardened the independent matcher so compiler-semantic regressions cannot cancel out between the
  engine and its oracle ([ADR-087](decisions/adr-087-independent-correctness-oracle.md)).

## 2026-07-23 to 2026-07-24 — Delivery and document API parity

- Added bounded exhaustive background jobs with idempotent streamed chunks and an exact terminal
  checksum ([ADR-114](decisions/adr-114-exhaustive-job-stream-delivery.md)).
- Added source metadata readback and honest `GET` and `HEAD` document behavior
  ([ADR-116](decisions/adr-116-get-document-source-readback.md)).
- Added strict create/index controls, conflict semantics, refresh parsing, and response metadata to
  document writes ([ADR-117](decisions/adr-117-put-document-index-contract.md)).

## 2026-07-17 to 2026-07-18 — Exact ranked delivery

- Split exact Boolean truth from bounded delivery through an explicit ranked result contract
  ([ADR-107](decisions/adr-107-ranked-percolation-result-contract.md)).
- Added typed priority and bounded local top-K collection
  ([ADR-108](decisions/adr-108-typed-priority-local-bounded-ranking.md)).
- Added deterministic distributed emission ownership so one shard emits each logical match
  ([ADR-109](decisions/adr-109-deterministic-distributed-emission-ownership.md)).
- Added distributed top-K merge and winner-only source fetch
  ([ADR-110](decisions/adr-110-distributed-top-k-query-then-fetch.md)).
- Added typed ranked wire errors with a legacy compatibility fallback
  ([ADR-111](decisions/adr-111-typed-ranked-wire-errors.md)).
- Added streamed distributed title batching with one-credit winner fetch
  ([ADR-112](decisions/adr-112-distributed-title-batching.md)).
- Added point-in-time snapshots and signed cursor pagination
  ([ADR-113](decisions/adr-113-pit-cursor-pagination.md)).

## 2026-07-02 to 2026-07-03 — Scale, cost control, and deployability

- Added review-first distributional alias discovery and behavioral match-feedback validation
  ([ADR-102](decisions/adr-102-distributional-alias-discovery.md),
  [ADR-103](decisions/adr-103-match-feedback-alias-validation.md)).
- Proved the durable K=8 cluster path at 20 million stored queries, including mutation and reopen
  ([ADR-104](decisions/adr-104-cluster-scale-soak.md)).
- Added the always-visible columnar hot tier under the two-axis placement rule
  ([ADR-105](decisions/adr-105-hot-tier-two-axis-placement.md)).
- Added in-memory canonical-body posting sharing and cross-segment regrouping
  ([ADR-106](decisions/adr-106-canonical-body-dedup-stage-a.md)).
- Added deployable-mode contracts, local and remote smoke gates, and versioned image publishing
  ([ADR-098](decisions/adr-098-deployable-gate-and-release-pipeline.md)).
- Added cooperative request cancellation and bounded search concurrency
  ([ADR-099](decisions/adr-099-cooperative-cancellation-bounded-concurrency.md)).
- Added per-shard RPC latency histograms and broad-lane cost counters
  ([ADR-100](decisions/adr-100-shard-rpc-latency-histogram.md),
  [ADR-101](decisions/adr-101-shard-broad-lane-cost-counters.md)).
- Added group-aware reassignment, parallel move scheduling, orphan-slot collection, and
  fingerprint-based retained-member reuse
  ([ADR-094](decisions/adr-094-replicated-group-reassignment.md) through
  [ADR-097](decisions/adr-097-content-fingerprint-skip.md)).

## 2026-06-24 to 2026-07-01 — Distributed operations and recovery

- Hardened gRPC transport deadlines, keepalive, retries, and metrics
  ([ADR-085](decisions/adr-085-grpc-transport-hardening.md)).
- Made committed control-plane assignments the routing source of truth with endpoint failover
  ([ADR-086](decisions/adr-086-control-plane-routing-and-failover.md)).
- Added real-process crash injection and a documented security review
  ([ADR-088](decisions/adr-088-crash-injection-harness.md),
  [ADR-089](decisions/adr-089-security-review.md)).
- Added live data-moving reassignment, unattended reconciliation, and multi-shard-per-node hosting
  ([ADR-090](decisions/adr-090-data-moving-reassignment.md) through
  [ADR-093](decisions/adr-093-multi-shard-per-node.md)).

## 2026-06-19 to 2026-06-23 — Packaging, backup, and platform integration

- Added engine-driven consistent backup and restore for durable single-node and in-process-cluster
  data directories
  ([ADR-079](decisions/adr-079-backup-restore.md)).
- Replicated broad and class-D queries across shards to remove the shard-0 hotspot
  ([ADR-080](decisions/adr-080-cluster-replicate-broad-to-all.md)).
- Added release container packaging and the distributed operations runbook
  ([ADR-081](decisions/adr-081-deployment-packaging-runbook.md)).
- Closed advertise-URL and coordinator class-D packaging gaps
  ([ADR-082](decisions/adr-082-packaging-deploy-correctness.md)).
- Connected the coordinator to the durable control quorum
  ([ADR-083](decisions/adr-083-control-plane-coordinator-wiring.md)).
- Added Helm packaging and native gRPC health/readiness endpoints
  ([ADR-084](decisions/adr-084-kubernetes-helm-health.md)).

## 2026-06-10 to 2026-06-11 — Percolator parity and distributed-v1 surfaces

- Established the drop-in translation contract and explicit distributed-v1 graduation criteria
  ([ADR-064](decisions/adr-064-percolator-drop-in-parity-audit.md),
  [ADR-065](decisions/adr-065-distributed-v1-graduation.md)).
- Made base-segment tombstones durable and document replacement atomic
  ([ADR-066](decisions/adr-066-tombstone-durability-at-commit.md),
  [ADR-067](decisions/adr-067-atomic-upsert-put.md)).
- Added the opt-in class-D lane and configurable parity number context
  ([ADR-068](decisions/adr-068-class-d-always-candidate-lane.md),
  [ADR-069](decisions/adr-069-parity-number-context-words.md)).
- Added the cluster REST coordinator, mesh TLS/authentication, and the multi-process single-host
  container-network lifecycle harness ([ADR-070](decisions/adr-070-cluster-rest-surface.md) through
  [ADR-072](decisions/adr-072-multi-machine-harness.md)).
- Closed REST parity gaps, enabled tagged-cluster vocabulary rebuilds, added cluster ranking, and
  made multi-word alias routing lossless
  ([ADR-073](decisions/adr-073-rest-parity-hardening.md) through
  [ADR-076](decisions/adr-076-cluster-multiword-aliases-vocab-shipping.md)).
- Added tag-space recovery attestation and durable in-process cluster resize
  ([ADR-077](decisions/adr-077-tagdict-recovery-fingerprint.md),
  [ADR-078](decisions/adr-078-cluster-resize.md)).

## 2026-06-03 to 2026-06-09 — Vocabulary, parity, and adversarial testing

- Added per-query tags and filtered percolation locally and across shards
  ([ADR-049](decisions/adr-049-percolator-parity-tags.md),
  [ADR-055](decisions/adr-055-cluster-tags-filtered-percolation.md)).
- Added golden front-end tests, fail-closed replacement operations, and review-driven hardening
  ([ADR-050](decisions/adr-050-golden-front-end-tests.md) through
  [ADR-052](decisions/adr-052-external-review-hardening.md)).
- Added corpus phrase learning, lossless equivalence expansion, compaction re-anchoring, and
  versioned frozen dictionaries
  ([ADR-053](decisions/adr-053-corpus-phrase-vocab-source.md) through
  [ADR-057](decisions/adr-057-frozen-dict-format-versioning.md)).
- Added punctuation folding, ranking/pagination, governed learned aliases, and multi-word title
  views ([ADR-058](decisions/adr-058-punctuation-equivalence-folding.md) through
  [ADR-061](decisions/adr-061-token-graph-multiword-aliases.md)).
- Added HTTP bearer authentication and adversarial test generation
  ([ADR-062](decisions/adr-062-server-bearer-auth.md),
  [ADR-063](decisions/adr-063-adversarial-test-hardening.md)).

## 2026-05-31 to 2026-06-03 — Cluster v1

- Added the in-process multi-shard core and the lean-core build boundary
  ([ADR-027](decisions/adr-027-in-process-multi-shard-core.md),
  [ADR-028](decisions/adr-028-lean-core-feature-gate.md)).
- Added the local/remote shard seam, dictionary attestation and shipping, coordinator log, and
  per-shard durable segments
  ([ADR-029](decisions/adr-029-grpc-shardserver-shard-seam.md) through
  [ADR-034](decisions/adr-034-cross-process-dict-shipping.md)).
- Added replication, no-quiesce recovery, retention leases, and a durable Raft control plane
  ([ADR-035](decisions/adr-035-per-shard-replication-peer-recovery.md) through
  [ADR-041](decisions/adr-041-durable-raft-log-recovery.md)).
- Added rendezvous allocation, live handoff, autoscaling policy, and dynamic vocabulary
  ([ADR-042](decisions/adr-042-shard-node-allocator.md) through
  [ADR-046](decisions/adr-046-dynamic-vocabulary.md)).
- Added fail-closed repair for partial distributed writes
  ([ADR-047](decisions/adr-047-remote-partial-apply-resync.md),
  [ADR-048](decisions/adr-048-reliability-hardening.md)).

## 2026-05-27 to 2026-05-30 — Engine foundation

- Established semantic signatures, integer-only verification, broad-query classes, and the
  append-oriented LSM write path ([ADR-001](decisions/adr-001-semantic-signatures.md) through
  [ADR-004](decisions/adr-004-lsm-write-path.md)).
- Added typed errors, structural exclusion of forbidden gates, deterministic generation, and the
  initial specialized dependency set ([ADR-005](decisions/adr-005-typed-errors.md) through
  [ADR-008](decisions/adr-008-deterministic-data-generation.md)).
- Added score-based compaction, fallible normalization, segment filters, mmap segments, WAL
  recovery, and source persistence ([ADR-009](decisions/adr-009-score-based-compaction.md) through
  [ADR-014](decisions/adr-014-query-source-store.md)).
- Added runtime vocabulary, lock-free snapshots, durable bulk ingest, and per-item outcomes
  ([ADR-015](decisions/adr-015-runtime-vocabulary-learning.md) through
  [ADR-018](decisions/adr-018-bulk-ingest-per-item-outcomes.md)).
- Reduced resident memory and added observable durability failures, runtime settings, segment
  introspection, CI, query limits, and columnar broad evaluation
  ([ADR-020](decisions/adr-020-resident-memory-reduction.md) through
  [ADR-026](decisions/adr-026-broad-lane-batch-evaluation.md)).
