# Clustering prior art — consistent hashing & content-routed percolation

Evidence base for the horizontal-scaling design
([`../design/clustering-and-scaling.md`](../design/clustering-and-scaling.md)). It surveys the
consistent-hashing family against *our* constraints, shows that "content-routed percolation by anchor
entity" is a known-good technique in content-based publish/subscribe, contrasts our approach with
Elasticsearch's distributed percolator, and (§5) compares the **shared-nothing** storage model we adopt
against the Aurora-style disaggregated alternative. The hashing decision is recorded in
[`../DECISIONS.md`](../DECISIONS.md) ADR-027; the storage-model decision (shared-nothing over a shared
object store — i.e. no S3/cloud dependency) in **ADR-033**.

> Status: the in-process multi-shard core + gRPC transport + dict shipping + durable coordinator log +
> per-shard local segments are **built** (ADR-027/029/034/031/032); the multi-node layers (per-shard
> replication + peer recovery, the Raft/quorum control plane) are design-only (roadmap Tier 3 — see
> [`../STATUS.md`](../STATUS.md)). This file is the prior-art backing those decisions, per the
> "research first, implement second" ethos.

---

## 1. Consistent-hashing variants — fair comparison against our constraints

The "best" variant is workload-specific; ours differs from a generic web-cache or a 1000-node store.
What we actually need:

- **Arbitrary-node removal.** Failover and self-heal (design §9, §8.6) mean *any* shard can leave, not
  just the last one added.
- **~1/N rebalance.** Adding/removing a node must move only ~1/N of the keys (the elastic property,
  design §3) so growth doesn't re-materialize the whole corpus.
- **Cheap at small K.** The first slice is a handful of shards (not thousands); memory and lookup cost at
  small K matter more than asymptotic scaling to thousands of nodes.
- **Deterministic & reproducible.** The engine hashes with `util::fnv1a64`, which is stable across runs
  (so benchmarks and the differential oracle reproduce); the ring must inherit that.
- **Range-splittable.** Auto-split (design §8.3) "splits a hot shard's hash *range* and re-materializes
  the two halves online" — phrasing that presumes a contiguous-range (token) model.

| Variant | Arbitrary removal | Rebalance | Memory @ small K | Lookup | Range-split fit | Verdict |
|---|---|---|---|---|---|---|
| **Ring + virtual nodes** (Karger '97; Dynamo/Cassandra) | ✅ | ~1/N | trivial (≈ vnodes × K entries) | O(log vK) binary search | ✅ native (token ranges) | **Chosen** |
| Rendezvous / HRW (Thaler & Ravishankar '96) | ✅ | ~1/N (optimal) | O(1); great balance, no vnode tuning | O(K) score-all | ⚠️ no explicit range | **Runner-up** |
| Jump hash (Lamping & Veach '14) | ❌ sequential ints only | tail-only | O(1); near-perfect balance | O(ln n) | n/a | Declined |
| Multi-probe (~21 probes) | ✅ | ~1/N | O(1) | ~21 hashes | ⚠️ | Wrong scale (100s–1000s nodes) |
| Maglev (Google, NSDI '16) | ✅ (via table regen) | table regen; caps backends | lookup table (~100× K) | O(1) | ⚠️ | Wrong scale (packet-rate LB) |

**Reading the table.** *Jump hash* has the best balance and O(1) memory, but its buckets are sequential
integers `0..n-1`: you cannot remove an arbitrary bucket without renumbering the rest, which collides
head-on with arbitrary-node failover. *Maglev* and *multi-probe* are tuned for hundreds-to-thousands of
backends at packet rate; our routing decision is made once per title over a handful of shards, so their
machinery is overkill. *Rendezvous/HRW* is an excellent fit — arbitrary removal, optimal rebalance, and
great balance at small K with **no** virtual-node tuning — and is the runner-up; its only gap is the
absence of an explicit contiguous range for the design's range-split auto-split. *Ring + virtual nodes*
is the one variant that satisfies every axis at once, and its token-range model is exactly what
Dynamo/Cassandra split when a shard grows hot — matching design §8.3 directly.

→ **Decision: ring + virtual nodes, hashed with `util::fnv1a64`.** Full rationale and the rejected
alternatives are in [`../DECISIONS.md`](../DECISIONS.md) ADR-027. *As built (ADR-027): the ring hashes the
globally-stable integer `FeatureId` — the one shared frozen dict makes those ids identical across shards —
rather than re-hashing the feature-name token this survey assumed a per-shard-dict design would need.*

---

## 2. Content-routed percolation is a known-good pattern (content-based pub/sub)

The design's central claim — route a title only to the ~2–5 shards that *could* match it, instead of
scatter-gathering across all N — is the percolation dual of **content-based publish/subscribe routing**,
a well-studied area whose techniques map onto ours almost one-to-one:

- **Attribute-rendezvous placement.** DHT-based pub/sub systems (e.g. *Ferry*) have each subscriber
  "choose an attribute from its subscription whose consistent-hash value maps to a rendezvous node," and
  route each event to the rendezvous nodes of *its* attributes. That is precisely our scheme: a query is
  placed at the rendezvous (ring) point of one chosen attribute — its **anchor feature** — and a title is
  routed to the rendezvous points of the anchor-eligible features it contains.
- **Selectivity-ordered single-attribute matching.** Content matchers "sort single-attribute matchings
  by selectivity so the search space shrinks fastest" — the same instinct as anchoring on the **rarest**
  (most selective) required feature.
- **Broadcast + prune (two-layer).** Several systems "combine a broadcast distribution layer with a
  content-based routing layer that prunes broadcast paths." That is our split exactly: the **broad lane**
  is broadcast/replicated to every shard, while the **selective lane** is content-routed and pruned to a
  few shards.

The theoretical point: scatter-gather is the generic default *because any shard might match*. Content
routing wins only when the event carries the attribute that determined placement. Our workload guarantees
that — the anchor is a *required* feature, so it is present in every matching title — which is why
content routing here is **correctness-preserving**, not merely an optimization (the no-false-negative
argument, design §3 / ADR-027).

---

## 3. Elasticsearch's distributed percolator — the production reference

Elasticsearch percolator is the closest production analog (reverse search: register queries, send a
document, get back the matching queries). Its distribution model:

- Queries are partitioned by a **custom routing value supplied at index time**; the *same* value is
  passed with a percolated document so it "is only executed on the required shard" (the REST endpoint
  takes a `routing` parameter).
- **Without** a routing value, percolating a document runs "in the same manner as a distributed search
  request" — i.e. scatter-gather across all shards.

So ES already validates *routing a percolator by a stored value*. Our contribution is the two things ES
leaves to the user:

1. **An automatic routing key.** ES makes the operator choose a routing value per query (and supply the
   matching value per document). We *derive* it from the compiler's anchor feature — no manual input,
   nothing to get wrong.
2. **A correctness guarantee.** ES gives no assurance that a manually-routed document reaches every query
   it could match; a wrong routing value silently drops matches. We *prove* zero false negatives: the
   anchor is required, hence present in any matching title, hence the title always reaches the query's
   shard.

We are not inventing sharded percolation — ES has it. We are making the routing key **automatic and
provably lossless** by deriving it from the same anchor feature the single-node optimizer already picks.

---

## 4. What the design takes from each

| Source | Borrowed | Not adopted (and why) |
|---|---|---|
| Ring + vnodes (Dynamo/Cassandra) | token-range placement, ~1/N rebalance, native range-split | name/IP-keyed routing — we key on the feature itself (the globally-stable `FeatureId`; ADR-027) |
| Content-based pub/sub (Ferry et al.) | attribute-rendezvous placement; broadcast+prune two-layer | multi-attribute server-overlay routing trees — our title fan-out is tiny (~2–5) |
| ES percolator | route-by-stored-value to escape scatter-gather | manual routing value; no lossless guarantee |
| Elasticsearch/Cassandra **shared-nothing** (design §4, ADR-033) | local segments + per-node WAL + replication + peer recovery + quorum control plane | — (this is the adopted model; see §5) |
| Aurora "log is the database" | the disaggregated shared-storage shape | **rejected (ADR-033)** — we are shared-nothing (local WAL + replicas, no shared object store); see §5 |

**Thesis.** A percolator can replace a search engine's scatter-gather with **content-routed placement by
the compiler-chosen anchor feature** — borrowing ring placement from Dynamo and the broadcast+prune split
from content-based pub/sub, while adding the one thing the prior art leaves out: an automatic,
provably-lossless routing key. Its storage layer is **shared-nothing** (§5), so it needs no cloud object
store.

---

## 5. Storage model — shared-nothing, not disaggregated (ADR-033)

The design doc's durable layer originally borrowed **Aurora's disaggregated "log is the database"** shape
(compute nodes attach to *shared* object storage). We rejected that (ADR-033) for the **shared-nothing**
model. The deciding question was concrete: *"how do you scale this without depending on a cloud object
store like S3?"* — and the production systems answer it the same way.

| System | Durability (source of truth) | HA / replicas | Recovery / rebalance | Control plane | Object store in serving path? |
|---|---|---|---|---|---|
| **Elasticsearch / OpenSearch** | per-shard **translog** (WAL), local disk | primary + N replica shards on other nodes | **peer recovery** — stream segments from a peer | Raft-like master quorum (cluster state = routing table) | **No** — only optional snapshot/backup (`fs`/S3/HDFS/Azure/GCS; `fs` = a plain shared dir) + optional cold "searchable snapshots" |
| **Cassandra / Scylla** | local **commitlog** + memtable→SSTable | replication factor N over the ring | hinted handoff + **repair/streaming** from peers | gossip; newer versions drop the single coordinator | **No** |
| **Kafka** | per-partition **local log** | ISR follower replicas on other brokers | follower fetch from the leader | **KRaft** (Raft) / ZooKeeper | **No** — optional *tiered storage* offloads only cold segments |
| **Aurora / Neon** (disaggregated) | redo log → **shared** distributed storage | readers attach to the shared volume | attach-from-shared-storage (no data copy) | managed | **Yes — central to the model** |

**The first three do not rely on AWS/S3 for the serving path.** Durability is a **local WAL + N
replicas**; recovery/rebalance is **peer-to-peer streaming**; membership is a **quorum/Raft** control
plane. Object storage, where it appears at all, is an *optional, pluggable* backup/cold tier — never the
hot path, and the on-prem default (ES's `fs` repository) is just a shared directory.

That is the model Reverse Rusty adopts (ADR-033). It is **self-contained** (fits the lean, std-only-core,
no-cloud-dependency ethos), and our building blocks already match it: per-shard **local** segments
(ADR-032) + a coordinator **WAL** (ADR-031), with the gRPC transport now **shipping the dict** so a data
node starts empty (ADR-034). The Aurora school's upside — cheap replicas / fast failover because storage
is shared — is bought instead with **warm replicas + peer recovery** (a bounded one-time segment copy, or
none at all for an already-warm standby); its downside — a distributed object-store dependency in the
critical path — is exactly what we decline to take on. The remaining shared-nothing steps (per-shard
replication, the Raft control plane) are in `clustering-and-scaling.md` §4/§10.

---

## Sources

- [Consistent Hashing: Algorithmic Tradeoffs — Damian Gryski](https://dgryski.medium.com/consistent-hashing-algorithmic-tradeoffs-ef6b8e2fcae8)
- [A Survey and Fair Comparison of Consistent Hashing Algorithms (CEUR-WS Vol-3478)](https://ceur-ws.org/Vol-3478/paper03.pdf)
- [A Fast, Minimal Memory, Consistent Hash Algorithm — Lamping & Veach, 2014 (arXiv:1406.2294)](https://arxiv.org/abs/1406.2294)
- [Routing Algorithms for Content-Based Publish/Subscribe Systems](https://www.researchgate.net/publication/224116209_Routing_Algorithms_for_Content-Based_PublishSubscribe_Systems)
- [Distributed percolator engine — elastic/elasticsearch #3173](https://github.com/elastic/elasticsearch/issues/3173)
- [When and How to Scale Percolator — Elastic Blog](https://www.elastic.co/blog/when-and-how-to-percolate-2)
