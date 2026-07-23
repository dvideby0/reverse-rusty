# ADR-109: Deterministic distributed emission ownership

- **Status:** Accepted
- **Date:** 2026-07-16

## Context

A logical query may be stored at several shard positions: selective any-of placement can fan out,
class-B pair placement is replicated for reachability, and class-C/D queries are replicated across
the broad lane. Before this decision, every routed copy that matched emitted the logical ID. The
coordinator sorted and deduplicated those replies, so the public result set was correct, but reply
bytes, coordinator work, match counts, and future distributed top-K work all scaled with duplicate
physical emissions rather than logical matches.

The owner cannot be inferred from the current physical node. Replication, handoff, node reassignment,
and co-location move or copy shard positions without changing query placement. It also cannot be
re-derived from the current ring after a vocabulary or shard-count rebuild: the same logical query
may have been written under an older placement function. Ownership therefore needs compact durable
identity metadata and a generation fence.

## Decision

### Placement identity and owner function

Every distributed query row carries a `PlacementGeneration`, the shard count, and one placement mode.
A cluster admits at most one live semantic row for each `logical_id`; the several rows discussed below
are the placement/replica copies of that one semantic row:

| Mode | Stored positions | Sole eligible emitter |
|---|---|---|
| standalone | none | all (the existing single-node behavior) |
| selective | sorted, unique `u32` positions | `min(placement ∩ routed_positions)` |
| replicated-always-visible | none | `min(routed_positions)` |
| replicated-broad | none | the request's broad-evaluation position |

Selective placement is used for A/B-any-of/H rows, replicated-always-visible for class-B pair rows,
and replicated-broad for class C and accepted class D. Empty intersections emit nothing. Position
arrays must be strictly sorted, in range, and contain the local position for selective rows.

Generation zero is reserved for standalone data; clusters start at generation one. The generation is
incremented exactly once by either blue/green vocabulary replacement or shard-count resize, because
those operations recompile or re-place the full corpus. Checkpoint, flush, compaction, backup,
replication, recovery, handoff, co-location, node reassignment, and control-plane topology changes do
not increment it. The generation is recorded in both the coordinator state and durable manifest.

### Unique logical-id admission

Distributed bounded reduction needs group-key locality: every semantic row contributing one logical
result must reach the same local collector. Reverse Rusty's placement is derived from query content,
so two different additive rows reusing one `logical_id` can have disjoint placement/routing sets; no
owner function can select one shard that necessarily sees both rows. Coordinator-side deduplication is
too late because duplicates can consume local-K slots and shard totals before the coordinator sees them.

The cluster therefore treats `logical_id` as a unique key. `build` and empty-cluster `ingest` reject a
second accepted row with the same id; `add_query` is insert-only and rejects an id that is already live;
`upsert_query` is the replacement operation; a successful remove permits reuse. Same-id mutations use
striped locks. The coordinator tracks the committed corpus in a sorted `u64` directory (eight bytes per
logical row) plus live add/remove overlays, folds those overlays into the sorted base at flush/checkpoint
maintenance boundaries, and reconstructs it from the durable base before replaying the coordinator-log
tail.

This follows the locality requirement used by mature distributed search systems: the
[SolrCloud sharding guide](https://solr.apache.org/guide/solr/latest/deployment-guide/solrcloud-shards-indexing.html)
routes each unique key to one shard, and the
[Elasticsearch collapse guide](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/collapse-search-results)
recommends routing every collapse key to one shard for reliable global ordering. Standalone `Engine`
ingestion remains additive; the stricter rule is a cluster invariant required by exact bounded
reduction.

### Lifecycle propagation and validation

Placement metadata is decided beside `anchor_plan`, then carried unchanged through build, live add,
atomic upsert, coordinator-log replay, per-shard translog replay, body-dedup membership, flush, both
compaction paths, backup/restore, replication, recovery, and handoff. Vocabulary/resize rebuilds
discard the old placement identity and compute all rows under the single new generation.

Before a shard is published, and before a remote write or recovery artifact is accepted, every row is
validated against its logical shard position, generation, and shard count. A mismatch is the typed
`ShardError::OwnershipMismatch`; it fails closed rather than risking either a duplicate or a missing
logical result. Content fingerprints include placement metadata, so recovery-copy elision cannot
accept semantically equal query bodies with stale ownership identity.

### Match-time policy

Emission is a monomorphized post-verification policy. Existing standalone APIs instantiate
`EmitAll`; cluster reads instantiate `UniqueOwner` with one request-wide ownership context. The check
runs only after exact positive/negative verification and after each member's alive/tag checks, but
before collector emission. Canonical-body members are checked independently, so sharing a verified
body never transfers ownership, aliveness, or tags between logical queries.

Filtered and compatibility-ranked cluster reads use the same context. The coordinator retains its
sort/dedup merge as a defensive backstop and records/asserts that ownership-aware shard replies have
zero cross-shard duplicates. Existing `ClusterEngine::percolate*` result sets and HTTP contracts do
not change.

### Transport attestation

The shard protocol carries placement metadata on writes and placement generation, shard count,
routed positions, broad scope/evaluator, and current logical position on reads. Successful percolate
replies attest `ownership_applied=true`. Dict adoption, co-located slot creation, fences, recovery,
retention leases, content fingerprints, deletes, flushes, and orphan-slot deletion carry the same
placement configuration guard. A pre-ADR-109 peer that proto-defaults these fields, a stale peer, or
a reply without the ownership attestation is refused.

### Persistence and migration

- Segment v7 appends allocation-free SoA columns for generation, shard count, mode, position
  offset/length, and a sorted `u32` position blob. Standalone engine segments v1–v6 remain readable
  and standalone data continues to choose its prior version based on its other features.
- Cluster manifest v6 records the current placement generation and is always written by an ADR-109
  durable cluster.
- Coordinator log and per-shard translog v4 persist placement metadata on Add/Upsert frames.
- Adopted feature-space state v2 records generation and shard count beside the feature/tag spaces.

The selected migration is rebuild-only for clustered data: durable cluster manifests v1–v5,
coordinator/translog files v1–v3, and legacy adopted data-node state are rejected with an actionable
rebuild or wipe/reseed error. Reading them and re-deriving placement would silently mix ownership
generations. This is intentionally stricter than standalone segment compatibility.

## Correctness and verification

Ownership is downstream of exact matching and cannot influence signatures, routing, MUST_NOT
handling, tag filtering, or ranking scores. For every matching distributed row, the owner function is
deterministic and selects at most one routed position; the placement/routing cover ensures the
eligible set is non-empty for visible matches. Together with unique logical-id admission, suppressing
non-owners preserves the full logical union while removing duplicate physical emissions.

Tests exhaustively compare selective ownership with the mathematical minimum intersection and cover
invalid modes, unsorted/out-of-range positions, empty routes, stale generations, and malformed
columns. The cluster oracle asserts owned output equals the single-node/brute union and
`duplicate_emissions == 0` across K = 1/3/8/16, visibility scopes, A/B/C/D/H, filters, ranking,
aliases, replication, resize, and vocabulary rebuild. Durability tests cover flush/compaction,
checkpoint/reopen, backup/restore, the format fences, and repeated rebuilds. The gRPC oracle covers
co-location, RF>1 failover, recovery, fingerprint copy elision, live handoff, missing/stale peer
attestation, and the ownership-applied reply guard.

## Consequences and deferred work

Compatibility cluster result sets are unchanged, but each logical match crosses the shard boundary at
most once. Every distributed row pays fixed SoA metadata plus selective placement's small sorted
`u32` list; replicated rows store no list. Cluster persistence and the internal wire require a
coordinated ADR-109 rebuild/upgrade.

Bounded distributed top-K and query-then-fetch enrichment are delivered separately by
[ADR-110](adr-110-distributed-top-k-query-then-fetch.md). Distributed title batching,
PIT/cursors, and exhaustive jobs/streams were subsequently delivered by ADR-112/113/114;
approximate termination remains separate. This ADR provides the one-logical-emission foundation
without itself introducing a new response contract.
