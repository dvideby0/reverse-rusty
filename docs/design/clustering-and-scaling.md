# Clustering, sharding, and scaling

*Scope: the current cluster architecture—content-routed query placement, shared-nothing durability,
replication, control-plane consensus, movement, and the boundary between built controls and unfinished
automation. Siblings: [`matching.md`](matching.md), [`ingestion-and-updates.md`](ingestion-and-updates.md),
and [`normalization.md`](normalization.md). Shipped chronology belongs in
[`../CHANGELOG.md`](../CHANGELOG.md); unfinished work belongs in
[`../roadmap.md`](../roadmap.md).*

> **Maturity:** Cluster v1—the in-process multi-shard core, durable reopen, and dynamic
> vocabulary—is built and differential-oracle proven (ADR-027/032/046). The optional
> `distributed` feature also implements gRPC shards, replication and peer recovery, a durable Raft
> control plane, data-moving reassignment/reconciliation, multi-shard-per-node slots, mesh security,
> health, and metrics. That stack remains **experimental**: it is proven in-process, over localhost
> gRPC, and across single-host container-network deployments, but independent multi-machine
> production evidence remains open.

**Current contract**

- One frozen `Dict` and `TagDict` define globally consistent integer spaces across all positions.
- The anchor ring places logical query copies; title content selects the positions to probe.
- Broad C/D rows and default-visible rows that cannot be ring-routed safely are replicated to every
  position. Broad C/D work is evaluated on exactly one routed position per title.
- Durable state is local segments + checkpoint metadata + mutation tails. There is no serving-path
  object store.
- Query mutations are primary-authoritative. Raft protects the small topology document, not query
  writes.
- Physical movement is explicit and fenced. Some policy decisions are automatic; remote shard-count
  changes and online splitting are not.

---

## 1. System shape

A cluster has `K` **logical shard positions**. A position is independent of the node currently
hosting it:

```
                         low-rate topology changes
                 ┌─────────────────────────────────┐
                 │ ControlPlane / Raft             │
                 │ membership + position→node map  │
                 └────────────────┬────────────────┘
                                  │ committed assignments
                                  ▼
title ──► coordinator ──route──► logical positions ──► local or remote Shard
              │                         │
              └──── merge/validate ◄────┘

per position: primary Engine + optional in-sync replica Engines
per engine:   mutable memtable + immutable local mmap segments + source metadata
```

`ClusterEngine` can host every `LocalShard` in one process, or point the same `Shard` interface at
gRPC `RemoteShard`s. A `ShardServer` can host several `shard_id` slots over one node-level adopted
feature space (ADR-093), so logical shard count is not tied to process or node count.

The **anchor ring** (`HashRing`) maps positive feature IDs to logical positions. A separate
**allocator** uses rendezvous hashing to map those positions to physical nodes. Keeping the two
layers separate lets a node assignment change without recompiling query placement.

---

## 2. Why content routing works

Generic search shards documents, then scatters a query to every shard because any shard may contain a
matching document. Reverse Rusty shards the much larger set of stored queries and routes each small
incoming title.

The compiler already produces a lossless positive cover. When a query is placed selectively by a
positive feature `a`, every matching title's positive view `P(T)` contains `a`; routing `P(T)` through
the same frozen dictionary and ring therefore reaches the position that stores the query. Negatives
and request tags never participate in routing.

This is a correctness rule, not merely a performance heuristic:

```
positive match(T, Q)
  ⇒ at least one query anchor a is present in P(T)
  ⇒ ring(a) is in route(T)
  ⇒ a copy of Q is probed
```

Fan-out depends on how many distinct non-top-64 features the normalized title contains. The captured
product-title workload usually reaches only a few positions, but that is workload evidence—not a hard
upper bound. A title with many eligible features can route more widely.

---

## 3. Placement and routing

### 3.1 Query placement

`placement_of` consumes the same `anchor_plan` used by the segment compiler. The current mapping is:

| Compiled shape | Stored positions | Visibility/evaluation |
|---|---|---|
| Class A arity-1 | ring owner of its anchor | default-visible main lane |
| Class B any-of proxy family | ring owner of every arity-1 member proxy | default-visible main lane |
| Class H arity-1 / any-of | same selective ring placement as A/B | default-visible hot lane |
| Class B top-64 arity-2 pair | every position | default-visible main lane |
| Class B required-phrase proxy | every position | default-visible positioned main lane |
| Class C | every position | opt-in broad lane |
| Class D with `accept_class_d=true` and a real negative predicate | every position | opt-in universal broad lane |
| Class D otherwise | nowhere | rejected |

Replicating a top-64 pair is necessary because neither individual feature is a lossless selective
ring key. Required-phrase candidate labels are also replicated: positioned graph labels need not
appear in ordinary flat routing. Replication is a placement decision; ADR-109 ownership still permits
only one logical emitter.

Class H is deliberately placement-identical to A. The θ threshold can move work between the
default-visible main and hot indexes but cannot move a query to another logical position or change
visibility (ADR-105).

### 3.2 Title routing

The coordinator normalizes a title with the authoritative normalizer. When aliases are active it
routes from the maximal positive view `P(T)`; otherwise the one flat view is sufficient. Every
feature outside the frozen top-64 mask contributes `ring.lookup(feature)` to a deduplicated position
set. This predicate intentionally does not use θ, matching the placement-invariance rule above.

The replicated broad lane is complete on every position (ADR-080), but only one routed position
evaluates it:

- if selective routing produced positions, a stable title hash chooses one of them;
- otherwise the same hash chooses one fallback position and adds it to the route.

This avoids a fixed “broad shard” hotspot and adds no broad-only fan-out when a selective position is
already being probed. `include_broad=false` suppresses C/D evaluation; it never suppresses H or other
default-visible replicated rows.

### 3.3 Shared feature and tag spaces

The coordinator owns one frozen `Dict` and `TagDict`. In-process shards share them; a remote
coordinator ships and fingerprint-checks them during `AdoptDict`, then can add co-located slots without
shipping them again. Post-freeze misses resolve to deterministic synthetic IDs, so every process maps
the same unseen feature/tag to the same integer.

Raw DSL and raw tag values are persisted in coordinator mutations and re-resolved on replay.
Request tag filters compile once at the coordinator and fan out as integer `TagId` groups. Tags apply
only after candidate retrieval and therefore cannot invalidate the routing proof.

Named CPU ranking profiles are shared by semantic identity rather than model bytes (ADR-163).
The coordinator and every remote shard independently load a strict registry at startup. Each ranked
request carries the selected name plus compiled fingerprint, the shard resolves it locally, and the
terminal reply echoes it. Unknown, divergent, missing, or pre-attestation peers fail the whole ranked
operation; unused extra profiles on a node are harmless. The public format, loading, and rollout
contract is in the [ranking reference](../reference/ranking.md).

---

## 4. Shared-nothing durable architecture

ADR-033 rejected the earlier shared-object-store sketch. Serving durability is entirely local:

```
coordinator directory
  cluster manifest       atomic checkpoint selector
  coordinator log        post-checkpoint raw mutation tail
  per-position registry  committed segment/source generations

data-node shard slot
  immutable .seg files   compiled local rows
  source sidecar         canonical query source/metadata
  shard checkpoint       durable segment + translog watermark
  translog               post-checkpoint per-shard mutation tail
```

Object storage may be used outside the serving path as an operator-managed backup destination, but no
match, write, reopen, recovery, or control-plane operation depends on it.

### 4.1 What is authoritative

A checkpoint and its tail are authoritative together:

- The **cluster manifest** atomically selects each position's segment files, source sidecar generation,
  ring parameters, frozen dictionaries/vocabulary, placement generation, and log watermark.
- The **coordinator log** preserves accepted mutations after that watermark. Live apply and replay use
  the same placement/apply funnel.
- Each durable remote shard uses its own **translog** for restart and peer-recovery catch-up.

Logs are not complete forever; checkpointing can trim them. A cluster cannot be rebuilt from a
truncated log alone. Manifest-selected segments and canonical source metadata are part of durable
truth. The exact current/readable format matrix is owned by
[`../operations/rolling-upgrade.md`](../operations/rolling-upgrade.md).

### 4.2 Checkpoint and reopen

`ClusterEngine::checkpoint` seals/reseals local rows as needed, commits segment and source
generations, advances the coordinator cursor, and only then permits old artifacts/tail records to be
reclaimed. `ClusterEngine::open` attaches and mmaps the committed segments, restores dictionaries and
source state, then replays only the log tail. Base-segment tombstones are baked into checkpoint output
so trimming a remove cannot resurrect a query.

Standalone and in-process cluster backup APIs checkpoint before copying the manifest-selected files.
Operational procedures and restore validation live in
[`../operations/backup-restore.md`](../operations/backup-restore.md).

### 4.3 Control data is separate

The control-plane document does not contain query mutations or segment bytes. Conversely, a
coordinator mutation log is not a Raft topology log. This boundary keeps consensus off the title and
query-write hot paths, but it also means operators must protect coordinator durable state according
to the deployment-mode RPO in
[`../operations/disaster-recovery.md`](../operations/disaster-recovery.md).

---

## 5. Replication, writes, and peer recovery

`ReplicatedShard` presents one position as a primary plus zero or more replicas:

- A write applies to the primary first, then fans to replicas.
- A replica failure marks that copy out of sync; it does **not** fail an already successful primary
  write. There is no quorum-ack query-write mode.
- Reads use the primary and fail over on a transport failure only to a replica marked in sync.
- Aggregation, source fetch, checkpoint identity, and content fingerprints remain
  primary-authoritative.

Peer recovery copies a sealed segment set at translog position `P`, then replays operations after
`P`. A retention lease prevents checkpoint trimming from deleting an in-flight recovery's required
tail. Catch-up loops until the residual tail converges, and promotion to in-sync occurs only after the
final fenced drain. The same protocol runs in-process and through `FetchSegments`, `FetchTranslog`,
`RecoverFrom`, and `RetentionLease` gRPCs.

A durable shard server also reopens its own slots from local checkpoint sidecars. Recovery refuses
truncated streams, malformed manifests, dictionary mismatches, stale placement, or invalid content
fingerprints rather than attaching a partial corpus.

### 5.1 Cross-position partial apply

A coordinator mutation may target several positions. It is logged before fan-out, but a remote RPC
can fail after another position applied. The coordinator reports the partial state, emits an event,
and records the failed targets for `resync`; replay of the durable coordinator log is the backstop.
Reads that promise exact exhaustive completion refuse while repairs are pending. The in-process RF=1
path is infallible at that seam. The strict native REST boundary runs one independently supervised,
admission-bounded pass and reports any still-unreachable mutations explicitly; it is not an alias for
Elasticsearch/OpenSearch shard-allocation reroute
([ADR-169](../decisions/adr-169-cluster-resync-api-contract.md)).

### 5.2 Query-write consistency versus topology consensus

Raft quorum is required for control-state transitions such as membership or assignment. It is not
consulted for ordinary query mutations. Read-your-writes through one healthy coordinator follows from
log-first apply plus snapshot publication, not from a replica quorum.

---

## 6. Control plane and physical assignments

`trait ControlPlane` owns a small `ClusterState` document:

- registered nodes and manager voters;
- position→node assignments;
- `num_shards` and virtual-node ring parameters;
- frozen dictionary fingerprint and model counter;
- application epoch and logical placement generation.

The default in-process backend applies the same deterministic state transitions under a lock. The
`distributed` feature supplies a durable OpenRaft backend, gRPC `ControlService`, and
`RemoteControlPlane` client. The state machine reuses one `control::apply` function for live and replay
paths; persisted vote, committed-log identity, snapshot, and CRC-framed log allow a manager to restart
and rejoin.

Node registration upserts only the descriptor by logical id; it does not change voters,
assignments, or physical data. A manager role is eligibility metadata, while the Raft voter set is
changed separately through joint consensus. The REST boundary reserves bootstrap `NodeId(0)`,
requires an HTTP(S) mesh origin, and reports the exact committed application version; replacement
must name the same recovered logical node unless data was moved safely first. The full operator
contract is in the [cluster-control API reference](../reference/api/cluster.md).

Deregistration is the symmetric descriptor-only transition. It reserves the same bootstrap
identity, returns the proposal's exact application version, and fails closed while the id remains
a voter or appears in any assignment. Drain a data node first by changing its descriptor role to
manager—preserving the endpoint while excluding it from desired data placement—and running a
data-moving reconcile/rebalance. The successful removal deliberately leaves voter membership,
assignments, live routing, and physical data unchanged; manager voters must leave separately
through joint consensus.

The allocator ranks registered nodes for each logical position with rendezvous (HRW) hashing.
`register_node` and `deregister_node` mutate its membership inputs; `rebalance` computes and commits
desired assignments. A committed map is routing authority only after deployment topology is
resolved and, for a populated remote cluster, the corresponding data movement has completed. Boot
and reconcile guards fail closed instead of routing a position to an empty slot.

The REST rebalance boundary enforces that distinction (ADR-166). A bodyless in-process request can
commit the advisory map because all shards remain co-resident. A bodyless resolve-only remote
request drives the data-moving move-then-commit workflow; explicit `move:false` is rejected before
planning. A CLI-seeded assignment-routed coordinator is rejected until its deployment removes the
restart guard's endpoint list: otherwise a successful changed map would make its next start fail.
A static endpoint-order remote coordinator is also rejected because its live source may not match
the committed map. The public route therefore cannot create the known map-without-data state, hand
off from a non-authoritative source, or acknowledge a topology the deployment cannot restart. The
underlying map-only library primitive remains available for in-process assembly and deterministic
allocator tests.

Consensus holds topology only. It never stores query DSL, tags, source, translog records, or compiled
segments.

---

## 7. Cross-shard reads and single emission

Each routed shard performs the normal candidate and exact-verification pipeline. Distributed rows
carry a generation-fenced `QueryPlacement`:

- selective rows name their logical positions;
- replicated-always-visible rows name the replicated mode;
- C/D rows name replicated-broad mode.

After exact verification, `UniqueOwner` chooses one emitter from the intersection of placement and
this request's routed positions. Replicated broad rows emit only from the named broad evaluator. This
makes shard replies disjoint by construction; the coordinator still validates/deduplicates
defensively on compatibility paths. A stale generation, wrong shard count, malformed placement, or
ownership overlap fails closed.

The same ownership context is used by:

- boolean and filtered percolation;
- compatibility ranking;
- exact bounded `/v2/_search` top-K (each position returns at most K owned rows, followed by an exact
  total attestation);
- ranked batch search;
- exhaustive chunk streams.

Top-K performs an exact shard-local bounded collection, global merge, then query-then-fetches source
for final winners only. Standalone and in-process clusters can pin the rank/order/total view with
PIT/cursor pagination; remote gRPC assemblies currently reject PIT. Exhaustive delivery requires
disjoint ownership, zero pending repairs, an authoritative logical-ID directory, and a stable
placement/mutation barrier.

Scalar top-K replies and terminal batch/exhaustive summaries also attest the selected ranking-profile
fingerprint. Streamed frames remain provisional until that terminal identity is validated, so model
drift or mixed-version omission cannot escape as a successful partial ranking.

---

## 8. Elasticity and autoscaling: built versus automatic

The cluster exposes powerful primitives, but “self-tuning” is not the current operational contract:

| Capability | Current behavior |
|---|---|
| Suggested shard count | `recommended_shard_count` computes an operator-invoked recommendation from configured capacity assumptions |
| In-process shard-count change | `resize` / `resize_to_recommended` rebuild live source under a fresh ring and atomically swap; durable mode commits the new layout |
| Remote shard-count change | not built; requires fresh/coordinated deployment or rebuild |
| Node membership rebalance | HRW planner is built; resolve-only remote mode moves data before committing new routing, while CLI-seeded and static remote modes are refused |
| Skew handoff | autoscaler can drive a fenced data-moving handoff when no conflicting rebalance ran |
| Corpus split pressure | `RecommendSplit` is advisory; targeted online splitting is not built |
| Scale-out recommendation | advisory; provisioning nodes is external |
| Reconcile loop | opt-in, idempotent convergence of committed HRW placement using physical moves |
| Parallel movement | opt-in conflict-free waves; shared endpoints serialize through the move ledger |
| Orphan-slot GC | opt-in guarded sweep using committed placement plus live-routing keep sets |

`AutoscaleConfig` is disabled by default. When enabled, `tick` gathers a fail-closed load snapshot,
repairs queued partial applies opportunistically, evaluates the pure policy, and executes the safe
subset. On a resolve-only remote cluster, membership rebalance moves data rather than changing the
map alone; CLI-seeded and static endpoint-order remote routing fail closed.
Split/scale-out recommendations remain decisions for an external operator or controller.

Adding positions does not reduce the replicated C/D corpus per node unless physical placement changes,
so `collect_load` subtracts the replicated broad share when assessing selective split pressure.

---

## 9. Movement and failure recovery

### 9.1 Fenced handoff

Raw `execute_handoff` performs:

1. reserve the endpoint footprint and attest that the claimed source is the position's current
   live primary while the target is outside its live replica set;
2. peer-copy the target while the source continues serving;
3. fence source writes (reads and recovery stay available);
4. drain the finite residual translog to convergence;
5. swap the runtime `HandoffShard` backing.

The higher-level reassignment path then commits the new assignment and retires/unfences retained
members as appropriate.

The public reassignment path is **move then commit**. If a crash lands after the runtime flip but
before the control commit, the fenced old owner still serves reads and retains data, so the committed
map still resolves to a data-holding endpoint. `reconcile` can finish the transition.

RF>1 group moves fence the committed primary once, establish every target member from the frozen
source, swap the composite, and CAS-commit the complete group. A retained member with an identical
content fingerprint can be promoted without an O(corpus) recopy.

### 9.2 Concurrency and cleanup

Every move reserves its full source/target endpoint set in a `MoveLedger`. Conflicting moves
serialize; disjoint moves may execute in configured waves. Tickets are RAII and failed handoffs
auto-unfence, preventing a forgotten fence from becoming a permanent write outage. The REST raw
handoff can apply a manager deadline to this reservation; a deadline loss is guaranteed not to
start recovery later. The raw route changes live routing only and is explicitly uncommitted;
restart-stable operator movement uses the move-then-commit reassignment path.

After assignments converge, `gc_orphan_slots` can list remote slots and drop only those outside both
the committed keep set and live routing. Unassigned positions fail safe (skip), and the drop path
fence-probes immediately before a rename-to-trash deletion.

### 9.3 Failure boundaries

- Primary transport failure may read-failover only to an in-sync replica.
- Lost RF=1 shard storage requires restore/rebuild from authoritative corpus; the control plane cannot
  recreate query bytes.
- Loss of a control-plane majority blocks topology writes but does not itself erase local shard data.
- A fresh remote coordinator attached to populated slots lacks an authoritative logical-ID directory
  for some mutation/exhaustive operations; that authority gap remains explicit and fail-closed.

Runbooks and RPO/RTO expectations belong to
[`../operations/disaster-recovery.md`](../operations/disaster-recovery.md), not this design page.

---

## 10. Implementation and evidence map

This map replaces the old “future build steps” narrative. Every row below is built; maturity is stated
at the top of this page.

1. **Shard seam and transport.** `LocalShard`, `RemoteShard`, `ShardServer`, dictionary adoption,
   multi-slot `shard_id` routing, per-call deadlines/retries, mesh TLS/token auth, health, and metrics.
2. **Coordinator and routing.** `ClusterEngine`, `HashRing`, P(T)-aware content routing, replicated
   broad/default-visible placement, ownership-fenced merge, tags, ranking, and exhaustive streaming.
3. **Durability.** Coordinator log + cluster manifest, per-position segments/source generations,
   shard checkpoints/translogs, durable reopen, backup, and compatibility fences.
4. **Replication.** `ReplicatedShard`, in-sync failover, retention-leased peer recovery, and group
   recovery/movement.
5. **Control plane.** In-memory seam, durable OpenRaft backend, remote client, HRW allocator,
   topology resolution, and generation-fenced assignments.
6. **Elasticity and repair.** Runtime in-process resize, handoff, data-moving reassignment/rebalance,
   reconciliation, move ledger/waves, autoscaler driver, and orphan GC.

Primary differential suites live under `engine/tests/cluster_oracle/`,
`engine/tests/cluster_durability_oracle/`, `engine/tests/cluster_grpc_oracle/`,
`engine/tests/cluster_control_raft_oracle/`, and the other `engine/tests/cluster_*` harnesses. The
Compose harness exercises crash/restart, partial apply, peer recovery, control failover, and
reassignment over container networking. Release validation also runs the Helm chart in kind.

Those are meaningful distributed-system checks, but Compose and kind on one host do not establish
independent-machine latency, storage, network-partition, kernel, or failure-domain behavior. The
supported deployment contract is in
[`../operations/deployment-modes.md`](../operations/deployment-modes.md); the independent-cluster
acceptance exercise is in the roadmap.

---

## 11. Bottom line

Reverse Rusty replaces all-shard scatter/gather with lossless content routing over a shared frozen
feature space. Selective A/B/H rows are ring-placed when safe; unroutable default-visible rows and
opt-in C/D rows are replicated with deterministic single-emitter ownership. Local immutable segments,
source metadata, mutation tails, peer recovery, and a topology-only Raft plane make the distributed
stack shared-nothing.

The core mechanisms are built. The honest remaining boundary is automation and evidence: remote
shard-count changes, online split policy, independent multi-machine acceptance, and production
operational history remain open rather than being implied by the word “autoscaling.”
