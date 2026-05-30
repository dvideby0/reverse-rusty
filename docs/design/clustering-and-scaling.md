# Clustering, Sharding & Auto-Scaling — dead-simple, self-tuning, OS/Aurora-patterned

*Scope: take the single-node engine and make it scale horizontally to 100M+ stored queries and
arbitrary title throughput, **automatically and with near-zero configuration**, reusing the
cluster-formation and storage patterns OpenSearch and Aurora proved in production — while exploiting
the one structural advantage our workload has over a generic search engine. Siblings:
[`ingestion-and-updates.md`](ingestion-and-updates.md) (the durable mutation log / write path this
shares), [`matching.md`](matching.md) (the per-shard hot path), [`normalization.md`](normalization.md).
Read the [overview](README.md) for the correctness contract; the self-tuning draws on the feature model
in [`../research/corpus-feature-learning.md`](../research/corpus-feature-learning.md).*

> **Implementation status:** Design-only — not yet coded. The PoC is single-node.

**TL;DR (for agents)**
- **Owns:** Horizontal scaling design — sharding, replication, autoscaling, durable cluster storage
- **Key idea:** Shard by entity hash (player/brand); titles fan out to ~2–5 shards (not all N) because entity is known from normalization
- **Asymmetry exploited:** Queries are the large corpus (sharded); titles are small and routed — the inverse of a normal search engine
- **Patterns borrowed:** OpenSearch cluster formation, Aurora log-is-the-database, consistent hashing
- **Status:** Entirely design-only (roadmap Tier 3 — see [`../STATUS.md`](../STATUS.md)); single-node PoC extrapolates to 100M with stated assumptions
- **Gotchas:** Broad-lane queries must be replicated to all shards; scale-to-zero needs entity-frequency stats from the feature dictionary

---

## 1. Sharding sketch (the design baseline)

100M compiled queries do not fit in a small node's RAM (the PoC sandbox is 3.8 GiB; see
[`../performance/results.md`](../performance/results.md)). The design shards by a stable hash of the
query's primary entity (player/brand) so that (a) an entity's near-duplicate queries stay co-resident
and (b) a title is routed
to the few shards whose entities it contains. Each shard is the segment+delta engine from
[`ingestion-and-updates.md`](ingestion-and-updates.md). Shards are NUMA-pinned; segments are mmap'd per
node; no cross-NUMA mutable sharing. The PoC runs one shard and reports per-shard numbers plus an
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

## 4. Cluster architecture — three layers, each borrowed from a proven system

```
        ┌──────────────────────── control plane (OpenSearch-style) ───────────────────────┐
        │  cluster-manager quorum (Raft): cluster state = ring + shard→node map +          │
        │  feature-model version + log epoch. Election, membership, allocation, rebalance. │
        └──────────────────────────────────────────────────────────────────────────────────┘
                 ▲ gossip/join                         ▲ assigns shards
        ┌────────┴───────────┐               ┌─────────┴─────────────────────────────────────┐
        │ coordinator nodes  │  route title  │ data/matcher nodes (stateless-ish compute)    │
        │ (content routing,  │ ───────────►  │  own shards by MMAP'ing immutable segments    │
        │  scatter to ~2-5   │ ◄───────────  │  from shared storage + replay hot log tail    │
        │  shards, merge)    │  matched qids │  → run the matching hot path locally          │
        └────────────────────┘               └───────────────────────────────────────────────┘
                                                         ▲ load segments        ▲ append mutations
        ┌────────────────────────────── durable layer (Aurora-style) ───────────────────────────┐
        │  (a) replicated, ordered MUTATION LOG of add/update/tombstone (the source of truth)     │
        │  (b) immutable compiled SEGMENTS in shared object storage (candidate index + exact SoA) │
        │  quorum-durable; "the log is the database", segments are materialized views of it.      │
        └─────────────────────────────────────────────────────────────────────────────────────────┘
```

### 4.1 Durable layer — Aurora's "log is the database"
Aurora's key move is to **ship the redo log to a shared, distributed, log-structured storage** that
replicates 6 ways across 3 AZs and self-heals, instead of shipping data pages. We adopt the same
shape:

- The **source of truth is an ordered, quorum-replicated log of query mutations** (`add(qid, dsl)`,
  `update`, `tombstone`). This *is* our hot delta from [`ingestion-and-updates.md`](ingestion-and-updates.md),
  now made durable and shared.
- **Immutable segments** (the compiled candidate index + exact SoA for a shard's feature range) live
  in **shared object storage** and are **materialized views of the log**, produced by the
  "improving compaction" job (see [`ingestion-and-updates.md`](ingestion-and-updates.md) §7).
- Durability is **quorum** over the log, exactly like Aurora's 4/6 write quorum — no node-local disk
  is authoritative.

### 4.2 Compute layer — Aurora replicas + OpenSearch data nodes
- A matcher node "owns" a shard by **`mmap`-ing that shard's immutable segments from shared storage**
  and replaying the **tail of the mutation log** into an in-memory hot delta. Because segments are
  shared and immutable, **spinning up a new replica is attach-and-mmap, not a data copy** — Aurora's
  trick that makes replicas and failover fast (Aurora restores service in <60s, often <30s).
- Multiple replicas per shard give HA + read (title) scaling; a title probe can hit any replica.

### 4.3 Control plane — OpenSearch cluster-manager quorum
- A small set of **cluster-manager-eligible nodes** hold the **cluster state**: the consistent-hash
  ring, shard→node assignments, the feature-model version, and the log epoch. They elect a leader by
  **quorum/majority vote** (the OpenSearch model: any eligible node can call an election, majority
  wins, which prevents split-brain). Use **3 or 5 managers** to tolerate 1 or 2 failures.
- The cluster-manager does only coordination — membership, **shard allocation**, and **rebalancing**
  — never sits in the title hot path. (Same separation OpenSearch enforces with dedicated manager
  nodes.)

---

## 5. Pattern-borrowing scorecard

| Concern | OpenSearch | Aurora | What Percolator does |
|---|---|---|---|
| Cluster formation | seed hosts, gossip, **quorum manager election** | — | same: quorum-elected cluster-manager holds the ring + epoch |
| Source of truth | replicated cluster state | **redo log → shared storage** | **mutation log** (quorum), segments are materialized views |
| Data placement | `hash(routing) % primaries` | shared volume (no sharding) | **consistent hash over anchor feature** (entity affinity) |
| Read/match path | **scatter-gather all shards** | replicas read shared storage | **content route to ~2–5 anchor shards** (our win) |
| Replicas / HA | primary + replica shards | up to 15 readers on shared storage | replicas **mmap shared segments**, replay log tail |
| Failover | promote replica shard | **promote reader, <60s** | promote replica (already has segments mmap'd) + replay tail |
| Grow capacity | add node → **rebalance**, `_split` | storage auto-grows in 10GB chunks | add node → consistent-hash moves ~1/N; **auto-split hot shards** |
| Elastic compute | — | **Serverless: autoscale ACUs, scale-to-zero** | autoscale matcher/coordinator on title QPS; scale-to-zero idle |

---

## 6. Updates & consistency model
- An update is `append-to-log` (compile new version, tombstone old). Default visibility is
  **near-real-time**: visible on a shard once its replicas apply that log entry (≈ log-replication
  latency). This matches the measured ~750k updates/sec/core local path, now durable.
- Optional **read-your-writes / synchronous** mode: `add_query` returns after a quorum of the owning
  shard's replicas have applied the entry (Aurora-style quorum commit). A knob, off by default.
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

1. **One binary, auto-roles.** `percolatord` starts, discovers peers via a seed/gossip list, and the
   cluster negotiates who is manager / coordinator / data — no per-role config. (`percolatord join
   <seed>` is the entire setup step; a single node just runs standalone.)
2. **Auto shard count.** Default primary-shard count is *derived*, not chosen: from the measured
   ~256 B/query (→ a target like ~5–10M queries/shard within a node's RAM budget) and the live corpus
   size. Operators never pick a shard count.
3. **Auto-split / auto-merge.** Because segments are immutable and the log is the source of truth,
   splitting a shard = split its hash range and re-materialize the two halves from shared storage
   **online** (no downtime, no reindex). The compaction job emits a **`recommended_shard_count`** from
   telemetry — driven by our "compaction that improves" loop. The
   cluster reshards itself when a shard exceeds size/latency thresholds.
4. **Auto-rebalance with no peer copy.** Adding/removing a node changes the ring; new owners
   **fetch segments from shared storage** rather than streaming from a peer (Aurora-style), so
   rebalance is bandwidth-cheap and fast.
5. **Auto-scale compute (serverless).** Matcher/coordinator replicas scale on title QPS / candidate
   load (HPA-style), independent of storage; **scale-to-zero** when idle, like Aurora Serverless. Shard
   *ownership* scales with corpus size; *throughput* scales with replica count — decoupled.
6. **Self-heal.** A dead node's shards are promoted on replicas that already mmap the same shared
   segments; they just replay the log tail — fast failover with no data movement.
7. **Self-tune the feature model too.** The same compaction pass re-runs the corpus learner
   ([`../research/corpus-feature-learning.md`](../research/corpus-feature-learning.md)), republishes the
   feature-model version in cluster state, and nodes hot-swap to it at an epoch boundary. Sharding,
   signature arity, broad-threshold, and the tokenizer all track the live workload with no human input.

**Minimal surfaces:**

```
Data plane (client):     add_query(dsl[, id]) · update_query(id, dsl) · remove_query(id)
                         percolate(listing) -> [qid]            // routing/sharding hidden
Cluster ops:             percolatord join <seed>                // that's it
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

## 10. Incremental build path from today's PoC
1. **Wrap the current engine as a single shard** behind a `ShardServer` (gRPC): `add/remove/percolate`.
2. **Add a coordinator** with the consistent-hash ring + content routing (§3) over K local shards in
   one process — validates routing/fan-out and the cross-shard correctness oracle.
3. **Externalize the mutation log** (start with a single-node WAL, then Raft) and make segments
   loadable from a shared path (local dir → object store) — gets the Aurora storage shape.
4. **Add the cluster-manager quorum** (Raft) holding ring + epoch; multi-process cluster.
5. **Auto-split + recommended_shard_count** from telemetry; **autoscale** matcher replicas.
6. Each step is independently testable; the differential oracle (`tests/oracle.rs`) extends naturally
   to a multi-shard harness asserting the cluster returns exactly the single-node result set.

(All of this is design-only — the PoC is single-node; see [`../STATUS.md`](../STATUS.md).)

---

## 11. Bottom line
Our workload lets us replace a search engine's expensive scatter-gather with **content-routed
percolation by anchor entity** — fan-out of a few shards, with a clean no-false-negative proof. Wrap
that in **Aurora's disaggregated log-is-the-database storage** (shared immutable segments + quorum
mutation log → fast replicas, fast failover, cheap rebalance) and **OpenSearch's quorum
cluster-manager** (election, allocation, rebalance), and make every knob **self-tuning** (shard count,
splits, scaling, and the feature model all driven by telemetry). The result is a cluster that an
operator starts with one command and otherwise leaves alone — proven patterns underneath, specialized
routing on top.
