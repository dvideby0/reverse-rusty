# Dynamic vocabulary — absorbing new terms after the dict is frozen

The output of the **research spike** that picked how Reverse Rusty's clustered core absorbs vocabulary
that first appears in a *live write* (a query added after the shared dictionary is frozen). This is the
headline **Cluster v1** correctness item ([`../STATUS.md`](../STATUS.md) Tier 0). The decision is recorded
in [`../DECISIONS.md`](../DECISIONS.md) **ADR-046**; this file is the prior-art survey + the codebase
feasibility + the reasoning behind it, per the "research first, implement second" ethos
([`../../CLAUDE.md`](../../CLAUDE.md)).

> **Status: spike complete.** Decision: **(1) deterministic feature-hashing for new tokens + (2) runtime
> normalizer learning for new alias/synonym rules.** Both land in the in-process Cluster v1 core; the
> cross-process *shipping* of alias updates is deferred to the experimental distributed layers.

---

## 1. The problem

The cluster's correctness model rests on **one shared, frozen dictionary**: every term maps to a dense
integer `FeatureId`, and *all* shards plus the coordinator agree on that mapping. Globally-consistent
`FeatureId`s are what make the cross-shard signature cover **lossless** — anchors and title features are
compared as integers, so if the integers disagree across shards a real match can be dropped (ADR-027).

Freezing the dict was the right simplification. The cost: a term absent from the frozen dict has no
`FeatureId`, and today it is **silently dropped** in the read-only compile that live writes use
(`cluster/coordinator/ingest.rs:140`, `cluster/server.rs:211` → `compile::extract_readonly` →
`normalize::compile_features_readonly`, which only keeps features `dict.get(name)` resolves). So a
required positive term vanishes → the query **broadens**; an any-of group whose members are all unknown
collapses → at worst an unsatisfiable group, a **false negative**.

**The asymmetry that makes this hard.** In a normal engine, *documents* are sharded and a query
scatter-gathers across *all* shards — so no two nodes ever need to agree on a term's id. Reverse Rusty is
the dual: **stored queries are the sharded corpus and a title routes by its anchor `FeatureId` to a few
shards** (content routing, ADR-027 §3). That routing **requires cross-shard agreement on a new term's
id** — the property the prior art below either provides expensively or not at all.

---

## 2. The correctness bar

- **Zero false negatives is non-negotiable** (the project's hard guarantee), across shards and after live
  writes.
- **Bounded false-positive *candidates* are acceptable** — the exact matcher rejects them. Sharp edge: if
  two distinct terms are made to share a `FeatureId`, the integer exact matcher *accepts* the non-match
  (a true emitted false positive). Any id-sharing scheme must keep that rate small and provably never an
  FN.
- **No coordination on the match hot path** (it is allocation-free integer work).
- **In-process ≡ cross-process** behaviour at the `Shard` seam.
- **Shared-nothing** — no object store / external coordination service (ADR-033).

---

## 3. Prior-art survey — two camps, neither a direct fit

The systems that absorb new vocabulary at write time fall into two camps. Crucially, **neither gives us
cross-shard-consistent ids without either a rebuild or coordination** — which is what points to a third
technique.

### 3a. Growable *local* dictionaries — Vespa, Lucene/ES per-segment

**Vespa** builds its attribute dictionary **dynamically in real time** as documents are written — "values
are immediately searchable," updates at ~40–50K/s per content node, **no cross-node coordination**. But
the dictionary is a per-node b-tree (or hash) of values → posting lists of **local doc ids**; term/value
ids are **local to each content node**, never globally agreed.[^vespa] This works *because Vespa
scatter-gathers* queries across content nodes and merges — it never needs two nodes to share a term id.
**Lucene** is the same shape one level down: per-segment FST term dictionaries with **segment-local** term
ordinals.

*Why it doesn't port directly:* our ring routes a title by its anchor `FeatureId`, so node-local ids would
send a query and a matching title to **different** shards → a false negative. A growable *local* dict
needs the scatter-gather we deliberately avoid.

### 3b. Rebuild-based globalization — Elasticsearch/OpenSearch global ordinals

ES unifies segment-local ordinals into a **per-shard** "global ordinal" mapping, **lazily rebuilt on the
first aggregation since the last refresh** (or eagerly per refresh); a modified shard recomputes, and the
cost grows with field cardinality.[^es-blog][^es-docs] It is exact (no collisions) but (a) **per-shard,
not cross-shard**, and (b) a **full rebuild** when segments change.

*Why it doesn't port directly:* a cross-node analogue would have the coordinator assign + propagate each
new term's exact id — exact, but it adds a coordination step and a **propagation window** (one node knows
the id before another → transient FN), and couples to the Raft control plane.

### 3c. The "version a shared artifact" precedent — RocksDB

RocksDB's trained ZSTD **dictionary compression** shares one dictionary across many immutable SSTs and
versions it by a dictionary id stored per file. It is a *compression* dictionary, not a term dictionary,
so the analogy is loose — but it is the precedent for "**if you must evolve a frozen shared artifact,
version it and let immutable files reference the version**," which informs the deferred cross-process
normalizer-shipping path (§6) rather than the token id-assignment.

### 3d. The technique that fits — deterministic feature hashing

**Feature hashing** ("the hashing trick," Weinberger et al. 2009) maps a term to an id by a *fixed,
deterministic* hash into a bounded range — so **every node computes the same id with zero
coordination**.[^weinberger][^wiki][^fully] Collisions are the known cost; the literature gives tight tail
bounds.[^fully] This is precisely the cross-shard-consistent-id-without-coordination property our content
routing needs, that neither §3a (local ids) nor §3b (rebuild + window) provides.

---

## 4. Codebase feasibility — no architectural blockers

Grounded against the engine (`engine/src/`):

- **`FeatureId = u32`** (`dict.rs:13`), interned **densely from 0** (`dict.rs:75` `intern`) and frozen via
  `finalize_mask` (`dict.rs:126`). Interned ids stay small (≪ 2²⁰ in practice), so a **reserved high
  range** (e.g. top bit set) is collision-free against real ids.
- The dict is an **immutable `Arc<Dict>`** shared read-only into every shard (`coordinator.rs:201`,
  `server.rs:66`) — no interior mutability; post-freeze mutation would fork the feature space. So new ids
  must live **outside** the dict.
- The normalizer→dict boundary is **pure name→id lookup** (`normalize.rs:365` `match_features`,
  `normalize.rs:345` `compile_features_readonly`): an unknown name is dropped at the `dict.get(name) ==
  None` point. A deterministic hash can substitute there with no signature change (the return is already
  `Vec<FeatureId>`).
- **`util::fnv1a64`** (`util.rs:13`) is the stable cross-process hash that already keys the ring and the
  dict fingerprint — so hashed ids are identical on every node *by construction*.
- The exact matcher compares ids by **`binary_search`** (`exact.rs:54`/`:63`), so synthetic ids work
  unchanged; a collision is a **false positive that survives verification, never a false negative**
  (a term always hashes the same → query-requires-`t` and title-contains-`t` always agree).
- Serialization (`storage.rs:1370` `serialize_dict`) + the fingerprint (`dict.rs:176`) cover only the
  interned table, so **synthetic ids are never serialized and never change the fingerprint** — the ADR-034
  handshake is untouched.
- **ADR-015 `Vocab`** (`vocab.rs`) already *learns synonyms from query any-of groups* and rebuilds the
  `Normalizer`, with a snapshot `set_vocab` swap + (partial) vocab-epoch staleness tracking — the
  machinery the alias half reuses.

Hook sites for the change: `normalize.rs` (`match_features` + `compile_features_readonly`),
`compile.rs::extract_readonly`, and guards at the by-id dict lookups (`mask_bit`/`kind`/`name`).

---

## 5. Decision (→ ADR-046): two complementary mechanisms

### 5a. New **tokens** → deterministic feature-hashing
A term absent from the frozen dict gets `FeatureId = RESERVED_BASE | fold_u32(fnv1a64(name))`, in a
reserved high-`u32` range disjoint from the interned range. Properties: **no coordination** (in-process ≡
cross-process for free — hashed tokens need *no shipping*), **bypasses the immutable dict**, **zero-FN**,
and it **fixes both original bugs** (broadening + any-of collapse). Cost: bounded, tunable false positives
from collisions.

**Load-bearing correctness:**
- **Both sides hash.** The title path (`match_features`) *and* the query path (`compile_features_readonly`)
  must hash unknown tokens — dropping a title token would re-introduce an FN. *(This revises the earlier
  charter note that "title-side dropping is safe" — true only under a non-absorbing frozen dict.)*
- **Collisions: bounded, tunable, never FN.** A ~31-bit range gives birthday collisions around tens of
  thousands of *distinct* unknown terms; a raw id-collision becomes a *visible* wrong match only when a
  query is anchored on the collided term *and* a title carries the other colliding term, so the effective
  rate is far lower. Tunable by range size; an optional **v2** refinement promotes hot hashed terms into
  exact interned ids at compaction (collision cleanup).
- **Guard by-id dict lookups.** A synthetic id is out of the interned Vecs' range → treat as non-hot,
  non-mask, unknown-name. Synthetic ids are rare by construction, so they never belong in the 64-hot
  common mask and always land in the exact verifier's non-mask required tail.

### 5b. New **alias / synonym rules** → runtime normalizer learning
Aliases (`Upper Deck` ≡ `UD`) are a **normalizer** operation: only the normalizer sees raw text and can
canonicalize two surface forms to one feature name *before* id assignment, so hashing cannot express them.
Reuse the ADR-015 `Vocab` machinery to learn the alias (it already learns from any-of groups), rebuild the
`Normalizer`, and swap its `Arc`. **In-process (the Cluster v1 core) there is one shared `Arc<Normalizer>`,
so the swap is atomic — no propagation window.** A normalizer change bumps the vocab epoch; queries
compiled under the old epoch are recompiled (the existing vocab-epoch staleness machinery) so the "same
normalizer for queries and titles" invariant holds and zero-FN is preserved.

---

## 6. Scope for v1 + open questions for implementation

**In v1 (in-process core):** both new tokens *and* new alias rules. Hashed tokens are cross-process-free.
The alias half's **cross-process normalizer *shipping*** (versioned normalizer + the propagation-window
consistency design — the §3c "version the artifact" pattern, analogous to dict shipping ADR-034) rides
with the experimental distributed layers and is **beyond v1**.

**Open questions to settle during the build (not blockers):**
1. **Reserved-range layout** — `RESERVED_BASE` value + the fold from `u64`→`u32`; assert interned ids can
   never reach it (cap + debug-assert).
2. **Anchor selection for hashed ids** — a hashed (rare) term should be a *good* selective anchor; confirm
   `compile::anchor_plan` treats an out-of-dict id as rare/non-hot without a `freq` lookup panic.
3. **Vocab-epoch recompile** — exact trigger + cost of recompiling queries when an alias bumps the epoch
   in-process (lazy vs eager); confirm it preserves zero-FN under concurrent writes.
4. **Oracle assertions** — extend `tests/cluster_oracle.rs` with absorb-correctly cases: a live add whose
   query introduces a new token is found (zero FN); an all-unknown any-of group is satisfiable; a declared
   alias makes both surface forms match; a measured-bounded false-positive check.

---

## Sources

[^vespa]: Vespa — *Attributes* (dictionary built dynamically at write; node-local ids; b-tree/hash → local-doc-id posting lists). <https://docs.vespa.ai/en/attributes.html>
[^es-blog]: Elastic — *Improving the performance of high-cardinality terms aggregations* (global ordinals: per-shard, rebuilt on refresh; cost grows with cardinality). <https://www.elastic.co/blog/improving-the-performance-of-high-cardinality-terms-aggregations-in-elasticsearch>
[^es-docs]: Elastic — *eager_global_ordinals* reference. <https://www.elastic.co/guide/en/elasticsearch/reference/current/eager-global-ordinals.html>
[^weinberger]: Weinberger, Dasgupta, Langford, Smola, Attenberg — *Feature Hashing for Large Scale Multitask Learning* (2009). <https://alex.smola.org/papers/2009/Weinbergeretal09.pdf>
[^wiki]: *Feature hashing* (the hashing trick; collisions). <https://en.wikipedia.org/wiki/Feature_hashing>
[^fully]: Freksen, Kamma, Larsen — *Fully Understanding the Hashing Trick* (2018; tight collision/tail bounds). <https://arxiv.org/abs/1805.08539>
