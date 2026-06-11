# ADR-075: Cluster ranking — rank-at-shard, merge at the coordinator

> [Back to the decisions index](../DECISIONS.md)


- **Status:** Accepted (2026-06-11). Closes [ADR-065](adr-065-distributed-v1-graduation.md)
  criterion **5** (the [ADR-059](adr-059-percolate-ranking-pagination.md) deferral): the
  `rank` block now works in cluster mode — `/_search` + `/_mpercolate` stop answering 400.
- **Context:** ADR-059 built percolate ranking single-node only: the post-match additive
  score (`Σ request-boosts + priority-tag value`) reads each matched query's **tag column**,
  and in a cluster those columns live in the shards' SoA stores — the coordinator merge
  point has ids, not tags. ADR-059's scope note: "cross-shard ranking needs each shard's
  per-query priority fetched at the coordinator merge point, a distinct problem behind the
  same `RankSpec` seam."
- **Decision — rank-at-shard, compile-once-fan (the ADR-055 pattern), merge scored rows.**
  ES's query-then-fetch reduce confirms the shape: score where the data is, merge-sort at
  the coordinator.
  - `ClusterEngine::compile_rank_spec` resolves the request's boost `(key,value)`s ONCE
    against the ONE shared frozen `TagDict` (`get_or_synthetic` — exactly
    `compile_tag_predicate` / the single-node `EngineSnapshot::compile_rank_spec`, same
    dict, so the integer boost ids are directly comparable cluster-wide).
  - A new required `Shard::percolate_filtered_ranked(title, include_broad, pred, spec)`
    fans the same `&CompiledRankSpec` to every probed shard; `LocalShard` matches and
    scores against ONE snapshot (`EngineSnapshot::rank` — per-shard newest-live-copy tag
    resolution, the single-node semantics), so the tags scored are exactly the tags of the
    copies that matched.
  - `ClusterEngine::percolate_filtered_ranked` merges the scored rows, dedups by id, and
    returns the set **sorted by id** — the caller owns the `(score desc, _id asc)`
    presentation order + `from`/`size`, the single-node `rank` contract. **Why dedup is
    safe:** copies of one logical id are version-identical across shards (identical op
    streams; an upsert tombstones everywhere atomically), so every shard reports the same
    score for the same id — any copy wins.
  - **Wire:** `PercolateRequest` gains the compiled spec (`RankSpec`: priority key +
    resolved `TagId`/weight boosts — resolved ids on the wire, the ADR-055 precedent);
    `PercolateReply` gains `scores` (parallel to `ids`) + a **`ranked` echo**. The client
    requested ranking and checks the echo: an older server that ignores the field leaves
    it false and `RemoteShard` fails **loud** — version skew can never silently hand the
    caller an unranked ordering it will present as ranked.
  - **Handlers:** the cluster `/_search`/`/_mpercolate` parse the same `RankBody` as
    single-node (shared module), order `(score desc, _id asc)`, slice `from`/`size`, and
    emit `_score` — per-slot AND on the merged multi-document union (whose cross-document
    dedup is also score-safe: scores are per-query, not per-document). `explain` remains
    the one loud 400.
- **The synthetic-tag boundary (pinned, not hidden):** a post-freeze tag resolves to a
  synthetic id with no stored string (ADR-046/055). A **boost** on it fires — boost
  matching is id-equality. A synthetic **priority** tag contributes **0** — priority needs
  the tag's *value string* (`TagDict::key_value`), which only an interned (build-time) tag
  has. This is inherent to the strings-die-at-the-boundary design, identical to what a
  single-node engine does given the same frozen tag space, and affects presentation only —
  never matching. Operators who need live-added priorities intern the priority tags at
  build (or re-build); documented in the API reference.
- **Why this is safe:** ranking remains a post-match presentation layer (ADR-049 §5.4) —
  it touches neither the candidate index nor the verifier, and the oracle pins
  ranked-id-set ≡ unranked-id-set at every K (zero FN trivially). No `rank` block ⇒
  byte-identical responses and a `rank: None` wire field (the pre-rank proto decodes it
  absent). Per-request compile of the spec matches the existing per-call filter compile —
  trivial against a percolate fan-out.
- **Proven:** `tests/cluster_oracle/ranking.rs` — K-swept `{1,3,8}` differential (cluster
  scored rows ≡ single-node `rank`, same `(score desc, _id asc)` order, recall guard),
  the synthetic boost-vs-priority boundary, rank∘filter composition (scored set ≡ filtered
  set); `tests/cluster_grpc_oracle` — ranked percolate over real localhost gRPC ≡
  single-node (compiled spec + parallel scores + echo round-trip);
  `tests/cluster_durability_oracle` — exact scores from a reopened (mmap-backed) cluster;
  handler tests — `_score` order, ranked `from`/`size`, `/_mpercolate` per-slot, unranked
  byte-identical (no `_score` key), `explain` still 400.
- **See also:** ADR-059 (the single-node mechanism + the deferral this closes), ADR-049
  §5.4 (ranking is presentation), ADR-055 (compile-once-fan + the shared frozen tag
  space), ADR-065 (the program), ADR-074 (criterion 4 — tags through the vocab rebuild).
