# Clustering, Sharding & Auto-Scaling — dead-simple, self-tuning, shared-nothing

*Scope: take the single-node engine and make it scale horizontally to 100M+ stored queries and
arbitrary title throughput, **automatically and with near-zero configuration**, reusing the
**shared-nothing** cluster-formation and storage patterns Elasticsearch and Cassandra proved in
production (local storage + per-node WAL + replication + quorum control plane — no shared object
store; ADR-033) — while exploiting
the one structural advantage our workload has over a generic search engine. Siblings:
[`ingestion-and-updates.md`](ingestion-and-updates.md) (the durable mutation log / write path this
shares), [`matching.md`](matching.md) (the per-shard hot path), [`normalization.md`](normalization.md).
Read the [overview](README.md) for the correctness contract; the self-tuning draws on the feature model
in [`../research/corpus-feature-learning.md`](../research/corpus-feature-learning.md).*

> **Implementation status:** The in-process multi-shard core (build-path §10 steps 1–2) is **built and
> oracle-proven** — `src/cluster/` (ADR-027): entity-anchor sharding, content routing, and a designated
> broad-lane shard over K shards in **one process**, dependency-free. Step 1's **gRPC transport is also
> built** — a `ShardServer` + `RemoteShard` behind the off-by-default `distributed` feature (ADR-029), with
> the coordinator **shipping its frozen dict** to each server at connect (ADR-034), so a data node starts
> empty rather than rebuilding a byte-identical dict from the corpus; proven by `tests/cluster_grpc_oracle.rs`.
> **Step 3a — a durable coordinator mutation log** (`trait ClusterLog` + crash-rebuild via
> `ClusterEngine::open`) — **is built** (ADR-031), and **per-shard durable compiled segments** so reopen
> **attaches-and-mmaps** instead of re-ingesting — **is built** (ADR-032); both proven by
> `tests/cluster_durability_oracle.rs`.
>
> **Architecture note (ADR-033):** this design follows the **shared-nothing** model of
> Elasticsearch/Cassandra/Kafka — **local** per-shard segments + a **per-node/coordinator WAL** for
> durability + **peer recovery** for HA + a **quorum/Raft control plane** for membership — **not** the
> Aurora "disaggregated shared object-storage" model an earlier draft borrowed. There is **no object store
> and no cloud dependency** in the serving path; object storage, if ever added, is only an optional
> pluggable backup target (local-fs default). The prior-art survey + the hashing-variant/correctness
> rationale behind ADR-027 are in [`../research/clustering-prior-art.md`](../research/clustering-prior-art.md).
> Per-shard **replication + peer recovery** is built — **in-process** (ADR-035; the `ReplicatedShard`
> composite — primary + N replicas, read failover, `peer_recover`) and over **gRPC** (ADR-036; remote
> replicas via `connect_replicated` + a streaming `FetchSegments`/`RecoverFrom` peer-recovery path). The
> **quorum/Raft control plane is built** too — its seam (a `trait ControlPlane` + in-memory backend holding
> the cluster-state document, step 5a; ADR-037) AND the **openraft backend** behind it (step 5b; ADR-038 — a
> `RaftControlPlane` over `Raft<C>` + a gRPC `ControlService` + a `controlserver` bin, multi-process elections
> + leader failover, `distributed`-gated). The durable/replicated **per-shard query log (translog)** is built
> too (step 5c; ADR-039) — peer recovery streams a peer's segments then replays the translog tail, so it need
> **not quiesce** writes for the copy window (in-process + over gRPC via `FetchTranslog`), and a durable data
> node self-restarts from its own checkpoint sidecar. **Translog retention leases + finalize** (step 5d; ADR-040)
> close 5c's gaps: `seal_for_checkpoint` trims to `min(P, lease_floor)` so a concurrent seal can't strand an
> in-flight recovery (and the translog GCs when idle), and a lease-held convergence loop + atomic in-sync
> promotion shrink the quiesce window to the residual delta. The openraft control plane is **durable** too
> (step 5e; ADR-041) — a CRC-framed Raft log + persisted vote/committed/snapshot let a `controlserver --data-dir`
> survive a restart and rejoin the quorum. Still design-only: an allocator on the shard→node map,
> normalizer/vocab shipping, TLS/auth, and autoscaling/auto-split.

**TL;DR (for agents)**
- **Owns:** Horizontal scaling design — sharding, replication, autoscaling, durable cluster storage
- **Key idea:** Shard by entity hash (player/brand); titles fan out to ~2–5 shards (not all N) because entity is known from normalization
- **Asymmetry exploited:** Queries are the large corpus (sharded); titles are small and routed — the inverse of a normal search engine
- **Patterns borrowed:** Elasticsearch/Cassandra **shared-nothing** (local segments + WAL + peer recovery + quorum control plane) and consistent hashing — **not** Aurora's shared object storage (ADR-033)
- **Status:** In-process multi-shard core **built** (steps 1–2 below; ADR-027), plus the gRPC transport with coordinator **dict shipping** (ADR-029/034), a durable coordinator log (step 3a; ADR-031), per-shard local durable segments with attach-and-mmap reopen (step 3b; ADR-032), and **per-shard replication + peer recovery** — in-process (step 4a; ADR-035 — the `ReplicatedShard` composite) and over gRPC (step 4b; ADR-036 — remote replicas + `FetchSegments`/`RecoverFrom`), and the **quorum/Raft control plane** — its seam (step 5a; ADR-037 — a `trait ControlPlane` + in-memory backend holding the shard→node map) and the **openraft backend** behind it (step 5b; ADR-038 — a `RaftControlPlane` + gRPC `ControlService`, multi-process elections + leader failover); the remaining multi-node layers (a durable per-shard query log / step 5c, autoscale) are design-only (roadmap Tier 3 — see [`../STATUS.md`](../STATUS.md)); the single-node engine extrapolates to 100M with stated assumptions
- **Gotchas:** Broad-lane queries must be replicated to all shards; scale-to-zero needs entity-frequency stats from the feature dictionary; **no object store / cloud dependency** — durability is a local WAL + replicas (ADR-033)

---

## 1. Sharding sketch (the design baseline)

100M compiled queries do not fit in a small node's RAM (the benchmark sandbox is 3.8 GiB; see
[`../performance/results.md`](../performance/results.md)). The design shards by a stable hash of the
query's primary entity (player/brand) so that (a) an entity's near-duplicate queries stay co-resident
and (b) a title is routed
to the few shards whose entities it contains. Each shard is the segment+delta engine from
[`ingestion-and-updates.md`](ingestion-and-updates.md). Shards are NUMA-pinned; segments are mmap'd per
node; no cross-NUMA mutable sharing. Reverse Rusty runs one shard and reports per-shard numbers plus an
explicit extrapolation to the 100M target with stated assumptions. The rest of this doc develops that
sketch into a full cluster design.

---

## 2. The asymmetry that makes our sharding *easier* than a generic search engine

In a normal search engine (OpenSearch), **documents** are the large sharded corpus and a **query**
scatter-gathers across *all* shards, because any shard might hold a matching document. That all-shard
fan-out is the dominant scaling tax.

We are the dual, and it pays off:

- **Stored queries are the durable, sharded corpus** (millions–100M of them).
- **Titles are transient probes** (the high-rate stream).
- A title does **not** need to visit every shard. It only needs the shards that could possibly hold a
  matching query — and we already know how to find them, because our compiler picks each query's
  **anchor feature** (its rarest required feature; see [`matching.md`](matching.md) §1).

So we shard by **anchor entity** and get **content-routed percolation** instead of scatter-gather.
That is the central idea; everything else is borrowed plumbing.

---

## 3. Sharding model — entity-anchor consistent hashing

> **Decided (ADR-027), as built:** the consistent-hash ring uses **virtual nodes** and is keyed on the
> **globally-stable `FeatureId`** — the one shared frozen dict (ADR-027) makes integer ids identical across
> shards, so the ring keys on the id directly instead of re-hashing the feature *token* `fnv1a64(feature_name)`
> a per-shard-dict design would have needed. The variant comparison (token-vs-id; ring+vnodes vs jump hash /
> rendezvous / Maglev) and the rationale are in [`../research/clustering-prior-art.md`](../research/clustering-prior-art.md) §1.

**Placement.** Each compiled query is stored on the shard that owns its **anchor feature**:

```
shard(query) = ring.lookup( query.anchor_feature )      // consistent-hash ring over feature IDs
```

(For an arity-2 class-B anchor, use the rarer of the two features. Class-C broad queries are handled
separately — §7.)

**Routing a title.** A title is sent only to the shards that own the title's *candidate-anchor*
features:

```
shards(title) = { ring.lookup(f) : f ∈ title.features , f is anchor-eligible }
```

Anchor-eligible = the rare (non-"hot") features — players, sets, card numbers — which is exactly the
set our optimizer ever anchors on. A title has only a handful of those (its player, its set), so
**fan-out is ~2–5 shards, not N.** Hot features (`grade:10`, `year`, `brand`) are never sole anchors,
so they don't trigger a shard probe.

**Cross-shard correctness (no false negatives).** For any query `Q` a title `T` can match, `Q`'s
anchor feature `a` is — by construction — a *required* feature of `Q`, hence present in `T`. `T`
routes to `ring.lookup(a)`, which is where `Q` lives. Therefore `T` probes `Q`'s shard. The
single-node lossless-cover contract then applies within that shard. The shard boundary cannot drop a
match. ∎ (This is the distributed extension of the [overview](README.md) §2 contract, and it's why
anchor-based placement is not just an optimization but a *correctness-preserving* one.)

**Why consistent hashing.** Adding/removing a node moves only ~1/N of the feature ranges (and thus
queries), not the whole corpus — the standard elastic-rebalance property, and it bounds the data that
must be re-materialized when the cluster grows.

---

## 4. Cluster architecture — three layers, shared-nothing (Elasticsearch/Cassandra-patterned)

> **ADR-033:** an earlier draft modeled the durable layer on **Aurora's disaggregated shared object
> storage**. We don't. The cluster is **shared-nothing** — each node owns its shards on **local disk**,
> durability is a **per-node/coordinator WAL**, and HA comes from **replicas + peer recovery** — exactly
> like Elasticsearch and Cassandra. There is **no object store and no cloud dependency** in the serving
> path.

```
        ┌──────────────────────── control plane (Elasticsearch-style) ─────────────────────┐
        │  cluster-manager quorum (Raft): cluster state = ring + shard→node map +          │
        │  feature-model version + log epoch. Election, membership, allocation, rebalance. │
        └──────────────────────────────────────────────────────────────────────────────────┘
                 ▲ gossip/join                         ▲ assigns shards
        ┌────────┴───────────┐               ┌─────────┴─────────────────────────────────────┐
        │ coordinator nodes  │  route title  │ data/matcher nodes                            │
        │ (content routing,  │ ───────────►  │  own shards as LOCAL mmap'd segments +        │
        │  scatter to ~2-5   │ ◄───────────  │  the hot delta from their own WAL tail        │
        │  shards, merge)    │  matched qids │  → run the matching hot path locally          │
        └────────────────────┘               └───────────────────────────────────────────────┘
                                              ▲ replicate (primary→replica)   ▲ append mutations
        ┌──────────────── durable layer — shared-NOTHING (Elasticsearch / Cassandra) ─────────────┐
        │  Per shard: a PRIMARY + N REPLICAS on DIFFERENT nodes, each on LOCAL disk.               │
        │  (a) an ordered MUTATION LOG (WAL) of add/update/tombstone — the source of truth —       │
        │      replicated primary→replica before ack (≈ the ES translog / Cassandra commitlog).    │
        │  (b) immutable compiled SEGMENTS on LOCAL disk (candidate index + exact SoA),            │
        │      materialized views of the log. A new/recovering replica streams them FROM A PEER    │
        │      (peer recovery) + replays the log tail — no shared storage anywhere.                │
        └─────────────────────────────────────────────────────────────────────────────────────────┘
```

### 4.1 Durable layer — local WAL + replication (the Elasticsearch/Cassandra shape)
The source of truth is an **ordered log of query mutations** (`add(qid, dsl)`, `update`, `tombstone`) —
our hot delta from [`ingestion-and-updates.md`](ingestion-and-updates.md), made durable per node and
replicated. This is **built today** as the coordinator's `ClusterLog` (ADR-031), the analogue of
Elasticsearch's per-shard **translog** / Cassandra's commitlog.

- **Immutable segments** (the compiled candidate index + exact SoA for a shard's feature range) live on
  the owning node's **local disk** and are **materialized views of the log**, produced by the "improving
  compaction" job (see [`ingestion-and-updates.md`](ingestion-and-updates.md) §7). Built today as the
  per-shard segments-only durable engine (ADR-032).
- **Durability = a per-node WAL + replication factor N** (write the primary, replicate to in-sync
  replicas before ack). No external storage service is authoritative — the cluster is self-contained.
  *(This is deliberately **not** Aurora's quorum-over-shared-storage; ADR-033.)*

### 4.2 Compute layer — data nodes own local shards; replicas via peer recovery
- A data/matcher node "owns" a shard by holding **that shard's segments on its own local disk** plus the
  hot delta replayed from its WAL tail, and runs the matching hot path locally.
- HA + read (title) scaling come from **multiple replicas per shard on different nodes**; a title probe
  can hit any replica. A write goes to the primary and replicates to the replicas before acking (§6).
- **Spinning up / recovering a replica is peer recovery**: the new owner **streams the shard's segments
  from a peer** that already holds them, then replays the log tail — the Elasticsearch/Cassandra model.
  (No attach-from-shared-storage, because there is no shared storage; the cost is one peer-to-peer segment
  copy at recovery, which a warm standby replica avoids — failover is then just promotion.)
- **Recovery does not pause writes** (built — ADR-039/040). The source keeps serving and accepting writes
  during the copy: the new owner streams segments at position `P` then replays the per-shard **translog** tail
  (> `P`). A **retention lease** pins that tail so a concurrent seal can't trim it (the Elasticsearch
  peer-recovery retention lease), and a brief **finalize** loop drains the residual before the replica is
  promoted into the in-sync set — so the quiesce window is the residual delta, not the whole copy.

### 4.3 Control plane — quorum cluster-manager (Elasticsearch-style)
- A small set of **cluster-manager-eligible nodes** hold the **cluster state**: the consistent-hash
  ring, shard→node assignments, the feature-model version, and the log epoch. They elect a leader by
  **quorum/majority vote** (Elasticsearch's model: any eligible node can call an election, majority
  wins, which prevents split-brain). Use **3 or 5 managers** to tolerate 1 or 2 failures.
- The cluster-manager does only coordination — membership, **shard allocation**, and **rebalancing**
  — and never sits in the title hot path. (The same separation Elasticsearch enforces with dedicated
  master-eligible nodes.)
- **Built (ADR-037 + ADR-038):** the control plane is a `trait ControlPlane` seam (document-mutation +
  linearizable-read — the `ClusterLog` sibling) with two backends — an in-memory one (the default; the
  coordinator stays byte-identical) and an **openraft** one (`RaftControlPlane` over `Raft<C>` + a gRPC
  `ControlService` carrying an opaque-bytes envelope + a `controlserver` manager bin). The state machine
  reuses the single `control::apply` funnel, so the two backends are live ≡ replay. Consensus holds **only**
  the cluster-state document — never the ~750k/sec query mutations (those stay on the `ClusterLog` + the
  per-shard primary→replica path) nor the per-shard segment registry (the local manifest). openraft is
  `distributed`-gated, so the lean core never compiles a consensus engine.
- **Durable + restart-recoverable (ADR-041).** The openraft backend's hard state is persisted by
  `src/cluster/control_store.rs` — a CRC-framed Raft log + atomic vote/committed/last-purged/snapshot files
  (reusing `storage::crc32` + the `clog`/`wal` torn-tail pattern). A `controlserver --data-dir` manager node
  survives a crash, resumes its committed cluster-state document, and rejoins the quorum; the in-memory backend
  (no dir) stays byte-identical to ADR-038. The state machine is rebuilt from the snapshot + replayed log on
  restart, so `apply` stays the in-memory funnel. The allocator that *acts* on the shard→node map (physically
  moving shards on a reassignment) is the next increment.

---

## 5. Pattern-borrowing scorecard

We follow the **shared-nothing** column (Elasticsearch / Cassandra). The Aurora column is kept as the
*rejected* alternative (ADR-033) — it shows why a shared-storage design is tempting and what we give up
(and gain) by not taking it.

| Concern | Elasticsearch / Cassandra (**adopted**) | Aurora (**rejected**, ADR-033) | What Reverse Rusty does |
|---|---|---|---|
| Cluster formation | seed hosts, gossip, **quorum manager election** | — | same: quorum-elected cluster-manager holds the ring + epoch |
| Source of truth | per-node **WAL** (translog/commitlog), replicated | redo log → shared storage | **mutation log** (WAL, replicated), local segments are materialized views |
| Data placement | `hash(routing) % primaries` / consistent hash | shared volume (no sharding) | **consistent hash over anchor feature** (entity affinity) |
| Read/match path | **scatter-gather all shards** | replicas read shared storage | **content route to ~2–5 anchor shards** (our win) |
| Replicas / HA | primary + replica shards on **local disk** | up to 15 readers on shared storage | replicas hold **local segments** (peer recovery), replay log tail |
| Failover | **promote a replica** | promote reader, <60s | promote a warm replica (already holds local segments) + replay tail |
| Grow capacity | add node → **rebalance** (peer recovery), `_split` | storage auto-grows in 10GB chunks | add node → consistent-hash moves ~1/N (streamed from a peer); **auto-split hot shards** |
| Elastic compute | scale data/replica nodes | Serverless: autoscale ACUs, scale-to-zero | autoscale matcher/coordinator on title QPS; scale-to-zero idle compute |

---

## 6. Updates & consistency model
- An update is `append-to-log` (compile new version, tombstone old). Default visibility is
  **near-real-time**: visible on a shard once its replicas apply that log entry (≈ log-replication
  latency). This matches the measured ~750k updates/sec/core local path, now durable.
- Optional **read-your-writes / synchronous** mode: `add_query` returns after a quorum of the owning
  shard's replicas have applied the entry (the Elasticsearch in-sync-replica ack model). A knob, off by
  default.
- No index-wide refresh, no segment rebuild on the write path — the cost we deliberately avoided vs
  the Lucene/percolator refresh model.

---

## 7. Broad queries in a cluster
Class-C broad queries (anchored only on a hot feature like `grade:10`) would all hash to one **hot
shard**. Don't let them. Because they are few in count (~0.2% in our data) but high-traffic, **replicate
the entire broad lane to every matcher node** (it's small) and evaluate it locally in batch. This
turns a hot-shard problem into a cheap local scan and keeps the selective ring balanced — the cluster
analogue of the broad-query quarantine in [`matching.md`](matching.md) §4.

---

## 8. What makes it *dead simple* (zero-config + self-tuning)

The operator experience should be: **one binary, one join command, everything else automatic.**

1. **One binary, auto-roles.** `reverse-rustyd` starts, discovers peers via a seed/gossip list, and the
   cluster negotiates who is manager / coordinator / data — no per-role config. (`reverse-rustyd join
   <seed>` is the entire setup step; a single node just runs standalone.)
2. **Auto shard count.** Default primary-shard count is *derived*, not chosen: from the measured
   ~256 B/query (→ a target like ~5–10M queries/shard within a node's RAM budget) and the live corpus
   size. Operators never pick a shard count.
3. **Auto-split / auto-merge.** Because segments are immutable and the log is the source of truth,
   splitting a shard = split its hash range and **re-materialize the two halves from the local segments +
   log tail online** (no downtime, no reindex) — like an Elasticsearch shard `_split`. The compaction job
   emits a **`recommended_shard_count`** from telemetry — driven by our "compaction that improves" loop.
   The cluster reshards itself when a shard exceeds size/latency thresholds.
4. **Auto-rebalance via peer recovery.** Adding/removing a node changes the ring; new owners **stream the
   moved shard's segments from a current owner (peer recovery)** and replay the log tail. Consistent
   hashing moves only ~1/N of the ranges, so the copy is bounded — the Elasticsearch/Cassandra rebalance
   model (there is no shared storage to "fetch" from; ADR-033).
5. **Auto-scale compute.** Matcher/coordinator replicas scale on title QPS / candidate load (HPA-style),
   decoupled from storage; **scale-to-zero** idle compute (the serverless-autoscaler pattern). Shard
   *ownership* scales with corpus size; *throughput* scales with replica count — decoupled.
6. **Self-heal.** A dead node's shards are promoted on replicas that already hold **their own local copy**
   of those segments; they just replay the log tail — fast failover with no data movement (warm replicas
   make failover a promotion, not a copy).
7. **Self-tune the feature model too.** The same compaction pass re-runs the corpus learner
   ([`../research/corpus-feature-learning.md`](../research/corpus-feature-learning.md)), republishes the
   feature-model version in cluster state, and nodes hot-swap to it at an epoch boundary. Sharding,
   signature arity, broad-threshold, and the tokenizer all track the live workload with no human input.

**Minimal surfaces:**

```
Data plane (client):     add_query(dsl[, id]) · update_query(id, dsl) · remove_query(id)
                         percolate(listing) -> [qid]            // routing/sharding hidden
Cluster ops:             reverse-rustyd join <seed>             // that's it
                         (optional) set desired_capacity | fully serverless
Observability:           /cluster/state · /shards · explain(qid) · explain(listing, qid)
```

Everything in §8 (shard count, placement, rebalance, scaling, failover, model refresh) happens
without operator action. The defaults are the product.

---

## 9. Failure & correctness notes
- **No cross-shard false negatives** — proven in §3; the anchor is always present in a matching title,
  so the title always reaches the right shard.
- **Split-brain** prevented by manager quorum (OpenSearch model); writes need quorum on the log.
- **Stale reads during rebalance** — a title in flight to an old owner is correct as long as the old
  owner still serves that range until handoff completes; consistent-hash handoff + epoch fencing makes
  this safe (serve-then-drop).
- **Hot keys** (a viral player) — that shard gets more *replicas* (throughput), and if its corpus grows
  too large it auto-splits; the broad lane absorbs the truly non-selective anchors.

---

## 10. Incremental build path from today's single-node engine
1. **Wrap the current engine as a shard.** ✅ **Done** (ADR-027, ADR-029, ADR-034): the in-process
   `LocalShard` owns an `Engine` + `ArcSwap<EngineSnapshot>`; the local↔remote `trait Shard` seam abstracts
   the per-shard operation, and behind the `distributed` feature a gRPC `ShardServer` + `RemoteShard` lift it
   onto the network (`ClusterEngine::connect_remote`). The coordinator **ships its frozen dict** to each
   server at connect (ADR-034), so a data node starts **empty** instead of rebuilding a byte-identical dict
   from the corpus out-of-band. Proven by `tests/cluster_grpc_oracle.rs`.
2. **Add a coordinator** with the consistent-hash ring + content routing (§3) over K local shards in
   one process. ✅ **Done** (ADR-027): `cluster::ClusterEngine` + `HashRing` over anchor `FeatureId`,
   entity-anchor placement, a designated broad-lane shard (§7), cross-shard merge — validated by the
   multi-shard correctness oracle (`tests/cluster_oracle.rs`: cluster ≡ single-node ≡ brute, K∈{1,3,8,16}).
3. **Externalize the mutation log** (start with a single-node WAL, then Raft) and keep each shard's
   compiled segments durable on **local disk** — the shared-nothing storage shape (ADR-033; **no object
   store**, the source of truth is the local WAL + replicas).
   - 3a. **Single-node coordinator WAL.** ✅ **Done** (ADR-031): a durable, ordered `trait ClusterLog`
     (`FileClusterLog`/`NullClusterLog`) plus a coordinator manifest + base snapshot; `ClusterEngine::{open,
     checkpoint}` rebuild the whole cluster — byte-identical placement, zero false negatives — from the log
     alone (proven by `tests/cluster_durability_oracle.rs`). Raw DSL is the logged source of truth; one
     `apply` funnel serves both live writes and replay.
   - 3b. **Per-shard local durable segments.** ✅ **Done** (ADR-032): each shard is a segments-only
     durable engine (`shard_<i>/segments/*.seg` on **local disk**, no per-shard WAL/manifest);
     `ClusterEngine::open` **attaches-and-mmaps** each shard's committed compiled segments and replays only
     the log tail — no re-ingest/recompile. The coordinator manifest (v2) is the single atomic commit point
     recording the per-shard segment registry + cursor; `checkpoint` re-seals tombstoned base segments so a
     truncated `Remove` can't resurrect a query. Proven by `tests/cluster_durability_oracle.rs`. *(ADR-033:
     these local segments are the durable base — there is no object-store step; the **Raft-backed
     `ClusterLog`** still drops in behind the same seam, which the `apply` funnel + epoch were shaped for.)*
4. **Per-shard replication + peer recovery** — a primary + N replicas per shard; a write fans out to the
   replicas, a read fails over to an in-sync replica, and a new/recovering replica streams the shard's local
   segments from a peer + replays the log tail (the Elasticsearch/Cassandra HA primitive).
   - 4a. **In-process.** ✅ **Done** (ADR-035): the `ReplicatedShard` composite wraps one position's primary +
     N replicas behind the `trait Shard` seam (zero coordinator change); writes fan out to in-sync replicas,
     reads fail over on a transport error (in-sync replicas only — never a stale one), aggregation/durability
     present the primary's view, and `peer_recover` (seal → copy `.seg` → attach-and-mmap) rebuilds a replica
     from a peer. `ClusterConfig::replication_factor` (default 1) drives it; validated by the multi-shard +
     durability oracles at RF > 1 (`tests/cluster_oracle.rs`, `tests/cluster_durability_oracle.rs`).
   - 4b. **gRPC multi-node.** ✅ **Done** (ADR-036): `ClusterEngine::connect_replicated(groups)` wraps each
     position's primary + replica `RemoteShard`s in a `ReplicatedShard` (coordinator unchanged), durable
     server shards (`pending_durable`/`new_durable`; `AdoptDict` builds a durable shard when a `data_dir` is
     set), and two RPCs — server-streaming `FetchSegments` (manifest-first, chunked; the receiver rejects a
     truncated stream rather than attaching a subset) + target-driven `RecoverFrom` (the recovering node
     pulls a peer's segments), orchestrated by `peer_recover_replica`. Validated by
     `tests/cluster_grpc_oracle.rs` (`grpc_replicated_failover_and_peer_recovery`). **Honest scope (lifted by
     5c):** as shipped here recovery **quiesced writes** for the copy window — concurrent-write "stream + replay
     the tail" needed a durable per-shard log; **step 5c (ADR-039) adds that translog and closes the gap.**
5. **Add the cluster-manager quorum** (Raft) holding ring + shard→node map + feature-model version +
   epoch; multi-process cluster.
   - 5a. **The control-plane seam.** ✅ **Done** (ADR-037): a dependency-free `trait ControlPlane`
     (document-mutation + linearizable-read — the `ClusterLog` sibling) + a `ClusterState` document
     (ring + the shard→node map + membership + feature-model version + epoch) + an in-memory backend, wired
     into the coordinator (default = one logical node ⇒ byte-identical). Its shape is fixed for openraft
     (membership distinct from `propose`, a `ForwardToLeader` error, snapshot-read, an app epoch distinct from
     the Raft term). Proven by `tests/cluster_control_plane_oracle.rs`. *(Consensus holds the cluster-state doc
     ONLY — not the query mutations, which stay on the per-shard path.)*
   - 5b. **The openraft backend.** ✅ **Done** (ADR-038): a `RaftControlPlane` over `Raft<C>` behind the
     *unchanged* seam (the default backend stays in-memory, so the coordinator is byte-identical), a new gRPC
     `ControlService` (opaque-bytes envelope) added to the existing `shard.proto`, a tonic `RaftNetwork` +
     `ControlServer`, and a `controlserver` manager bin — multi-process elections + leader failover, all
     `distributed`-gated so the lean core never compiles openraft. The state machine reuses the ONE
     `control::apply` funnel (live ≡ replay with the in-memory backend). Proven by
     `tests/cluster_control_raft_oracle.rs` (3-node in-process convergence + `ForwardToLeader` +
     `change_membership` routing; and over real gRPC servers, survive-the-leader-being-killed). *(Consensus
     holds the cluster-state doc ONLY — never query mutations.)*
   - 5c. **Close the quiesce gap.** ✅ **Done** (ADR-039): each durable shard owns a per-shard **translog** (the
     ES translog — ADR-031's CRC-framed `FileClusterLog` + the logical-id-and-DSL `ClusterMutation`, re-homed per
     shard), appended log-first on every write and trimmed at `seal_for_checkpoint` to a position `P` (segments
     hold ops ≤ `P`, the translog the un-sealed ops > `P`). Peer recovery streams a peer's segments at `P` **then
     replays the translog tail (> `P`)** — the writes that land during the copy, recovered rather than lost — so
     it need **not quiesce**, both in-process (`peer_recover` + `catch_up_replica`) and over gRPC (a server-
     streaming `FetchTranslog(after_seqno)` RPC + `FetchManifest.up_to_seqno`). A durable data node also
     self-restarts from a per-shard checkpoint sidecar (`shard.ckpt`). Proven by
     `tests/cluster_grpc_oracle.rs::grpc_peer_recovery_without_quiescing` + `replica.rs` in-process tests.
     **Distinct from the control-plane doc** — the control plane (5a/5b) holds cluster *state*, never the query
     mutations this log carries.
   - 5d. **Translog retention + finalize.** ✅ **Done** (ADR-040): closes step 5c's two scope gaps. **Retention
     leases** (the Elasticsearch peer-recovery retention lease): the recovery source holds a lease set and
     `seal_for_checkpoint` trims the translog to `min(P, lease_floor)` instead of `P`, so a **concurrent** seal
     (another recovery's `FetchSegments`, a checkpoint) can no longer trim away the tail an in-flight recovery
     still needs — a latent false negative 5c left open — while an idle shard (no lease) trims to `P` (byte-
     identical to 5c) so the translog GCs. **Finalize:** recovery holds one lease across a convergence loop
     (`catch_up_replica` until the tail stops advancing), then promotes the replica into the in-sync set under a
     brief write quiesce, so the window shrinks to the residual delta, not the whole copy. `ReplicatedShard`
     gained runtime replica growth (`add_recovered_replica`); `ClusterEngine::add_replica` exposes it; over gRPC
     a `RetentionLease` RPC plumbs acquire/renew/release. Correctness never depends on the loop converging (the
     lease keeps the tail safe), only the window size does. Proven by `replica.rs` unit tests +
     `tests/cluster_grpc_oracle.rs::grpc_peer_recovery_converges_under_sustained_writes` (a writer thread streams
     adds CONCURRENTLY with the recovery; recovered ≡ live source ≡ brute over the final set).
   - 5e. **Durable Raft log + restart recovery.** ✅ **Done** (ADR-041): closes ADR-038's deferred durability.
     A new `src/cluster/control_store.rs` persists the openraft backend's hard state — a CRC-framed append-only
     Raft log (reusing the `clog`/`wal` forward-scan / torn-tail pattern + `storage::crc32`), plus atomic
     single-value files for the **vote** (election safety), the **committed** log id (`save_committed`, so a
     restart re-applies `(snapshot.last, committed]`), the last-purged id, and the SM **snapshot**. The state
     machine is rebuilt on restart from the snapshot + the replayed log (so `apply` stays the in-memory
     `control::apply` ⇒ live ≡ replay unchanged). `LogStore`/`StateMachine` gained `in_memory()` (the ADR-038
     path — byte-identical) + `open(dir, fsync)`; `start_grpc_node` + a `controlserver --data-dir` flag make a
     manager node durable, so it survives a crash and rejoins the quorum. Proven by
     `tests/cluster_control_raft_oracle.rs::durable_node_recovers_committed_document_after_restart` +
     `control_store.rs` unit tests. All `distributed`-gated; no new dependency.
6. **Auto-split + recommended_shard_count** from telemetry; **autoscale** matcher replicas. *(design-only)*
7. Each step is independently testable; the differential oracle is realized as `tests/cluster_oracle.rs`,
   a multi-shard harness asserting the cluster returns exactly the single-node result set.

(Steps 1–2 — the in-process core — step 1's gRPC transport + dict shipping, step 3a's coordinator log,
step 3b's per-shard local durable segments, step 4's per-shard replication + peer recovery (4a in-process,
4b over gRPC), the **quorum/Raft control plane** — step 5a's seam AND step 5b's openraft backend + gRPC
`ControlService` — AND step 5c's **per-shard translog + no-quiesce peer recovery** are built; ADR-027 + ADR-029
+ ADR-034 + ADR-031 + ADR-032 + ADR-035 + ADR-036 + ADR-037 + ADR-038 + ADR-039 + ADR-040 + ADR-041. The
remaining shared-nothing multi-node work — an allocator acting on the shard→node map, and autoscale +
auto-split (step 6) — is design-only (ADR-033). See [`../STATUS.md`](../STATUS.md).)

---

## 11. Bottom line
Our workload lets us replace a search engine's expensive scatter-gather with **content-routed
percolation by anchor entity** — fan-out of a few shards, with a clean no-false-negative proof. Wrap
that in a **shared-nothing** storage layer — local per-shard segments + a per-node/coordinator mutation
log + replicas with peer recovery (the Elasticsearch/Cassandra model, **no shared object store**;
ADR-033) — and an **Elasticsearch-style quorum cluster-manager** (election, allocation, rebalance), and
make every knob **self-tuning** (shard count, splits, scaling, and the feature model all driven by
telemetry). The result is a cluster that an operator starts with one command and otherwise leaves alone,
with **no cloud dependency** — proven patterns underneath, specialized routing on top.
