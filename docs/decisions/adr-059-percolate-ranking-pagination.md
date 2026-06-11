# ADR-059: Percolate ranking + pagination (ADR-049 decision point 4, single-node)

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted

- **Context.** [ADR-049](adr-049-percolator-parity-tags.md) carved percolator parity into four decision
  points and built the high-value three (per-query metadata tags, filtered percolation pushed into
  verify, never-gating). Its **decision point 4 — ranking + the `/_mpercolate` pagination gap —** was
  left design-only, and the same item is the tail explicitly deferred from the
  [ADR-052](adr-052-external-review-hardening.md) external-review pass (#3's `from`/per-slot tail). Two
  concrete gaps remained on the HTTP percolate surface: (a) **no result ranking** — matched query ids
  come back in engine order, with no way to order by a per-query priority or a request boost; and (b)
  **partial pagination** — `/_search` has `from`/`size`, but `/_mpercolate` was `size`-only (no `from`),
  and `/_search`'s per-slot `slots[*].hits` ignored `size` entirely (complete-per-slot). This ADR builds
  decision point 4 on the **single-node** engine — the surface the REST endpoints actually run against
  (the cluster is a separate Rust/gRPC path; see Scope).
- **Decision.**
  1. **Ranking is a post-match, out-of-core layer (the ADR-049 §5.4 shape, now built).** A new
     `EngineSnapshot::rank(ids, spec) -> Vec<(id, score)>` scores the already-final matched id set; the
     handler sorts by `(score desc, _id asc)` — a *total* order, so pagination is byte-stable — then
     applies `from`/`size`. It runs entirely *after* verification on a `Vec<u64>` and touches **neither
     the candidate index nor the verifier**, so it can only reorder + paginate, never add or drop a match.
     Lives in a new lean-core `src/rank.rs` (`RankSpec`/`CompiledRankSpec`/`score`), sibling to
     `exact.rs`'s `TagPredicate`.
  2. **Score model: additive integer.** `score = Σ(weight for each (key,value) boost the query's tags
     match) + (numeric value of the query's `priority_key` tag)`. Priority **reuses the tag mechanism**
     (ADR-049 §5.1): a designated tag key (default `"priority"`) whose value parses to `i64`
     (`parse::<i64>().unwrap_or(0)` — a total fallback, never a panic; honoring "no `unwrap()` in library
     code"). Request boosts compile to `TagId`s via the same `get_or_synthetic` resolution as
     `compile_tag_predicate`, so a boost value never seen at ingest yields a `TagId` no stored query
     carries and simply never fires — no over-boost, mirroring the safe `terms`-filter semantics.
     *Additive* (vs strict `(boost, priority)` lexicographic, which the §5.4 sketch wrote) is the simpler
     ES-`function_score`-"sum"-style realization and the better fit for this workload, where
     operator-supplied boosts are meant to be commensurate with priority; strict dominance is reachable
     by choosing boost magnitudes above the priority range — a request-shaping choice, not a code branch.
  3. **Newest-live-copy tags.** `rank` resolves each matched `logical_id` to its tags by picking the
     **newest live copy** — memtable first (all writes land there), then base segments newest→oldest —
     mirroring the live-copy resolution in `Engine::delete_by_logical_id`. So a query updated in place is
     ranked on its current tags, not a stale base copy's.
  4. **Pagination is uniform.** `size`/`from` now bound *every* hits array: single-doc, the multi-doc
     merged view, **per-slot** `slots[*].hits`, and `/_mpercolate` per-doc. This adds `from` to
     `/_mpercolate` and **per-slot hit truncation** to multi-doc `/_search` — closing ADR-052 #3's
     deferred tail — with `total` always reporting the untruncated count.
  5. **`_score` surfaces only when ranked.** `SearchHitItem` gains `_score: Option<i64>`
     (`skip_serializing_if = "Option::is_none"`); it is `Some` only on the ranking path.
- **Why this is safe (no false negative + byte-identical default).** Ranking changes *order*, never
  *membership*: it runs after the boolean match on the final id set, so the lossless-cover contract
  ([`design/README.md`](../design/README.md) §2) is untouched — same as ADR-049's argument for tags,
  and structurally, not test-dependently, true. Ranking is **opt-in**: with no `rank` block (or an empty
  one — `CompiledRankSpec::is_noop`), the handler keeps the engine's existing order and slice, emits no
  `_score`, and the response is byte-identical to the pre-ranking engine. The one default behavior
  change is per-slot truncation by `size` (default 1000) for >1000-hit slots — `total` preserves the
  count, and the recall-first core jobs consume the count/unranked set anyway. Recall guard: tests assert
  the ranked id set equals the unranked set (nothing added/dropped) — at both the engine and HTTP layers.
- **Scope.** **Single-node only.** The REST `/_search` + `/_mpercolate` handlers run against a single-node
  `EngineSnapshot`, so this fully closes the REST-surface gap. **Cluster ranking stays design-only**
  (consistent with [ADR-055](adr-055-cluster-tags-filtered-percolation.md)): cross-shard ranking needs
  each shard's per-query priority fetched at the coordinator merge point
  (`cluster/coordinator/matching.rs`), a distinct problem behind the same `RankSpec` seam, deferred
  *(resolved by [ADR-075](adr-075-cluster-ranking.md): rank-at-shard behind
  `Shard::percolate_filtered_ranked`, merged at the coordinator)*.
- **Alternatives declined.** *Scoring inside the verifier / threading a rank budget into the matcher* —
  rejected (ADR-049 already): it entangles a presentation concern with the boolean hot path and could
  push `from`/`size` into matching, where a low-priority early match could wrongly survive over a
  high-priority one. *A dedicated numeric priority column in the SoA + `.seg` format* — rejected: it is a
  format bump for a low-priority presentation feature; reusing the tag value (parsed at rank time, off
  the hot path, only on the opt-in path) avoids any on-disk change.
- **Consequence.** "Percolate, then narrow by category, then order by priority/boost and page" is now a
  single in-engine call with an integer-only filter on the hot path and an opt-in post-match sort — no
  external store, zero-false-negative guarantee intact, default path byte-identical. Closes the last open
  ADR-049 decision point on single-node and the ADR-052 #3 pagination tail.
- **See also:** [ADR-049](adr-049-percolator-parity-tags.md) (the four decision points; this builds #4),
  [ADR-055](adr-055-cluster-tags-filtered-percolation.md) (tags through the cluster; cluster ranking
  still deferred), [ADR-052](adr-052-external-review-hardening.md) (#3 — the `from`/per-slot tail this
  closes), [ADR-026](adr-026-broad-lane-batch-evaluation.md) (`/_mpercolate` batch — where pagination
  attaches), [ADR-006](adr-006-forbidden-features-never-gate.md) (the never-gate invariant this mirrors).
  Design: [`design/matching.md`](../design/matching.md) §5.4. Code sites: `src/rank.rs`
  (`RankSpec`/`CompiledRankSpec`/`score`), `src/segment/snapshot.rs`
  (`compile_rank_spec`/`tags_for_logical`/`rank`), `src/bin/server/handlers/search.rs` (the `rank` block,
  `from` on `/_mpercolate`, `_score`, the `order_and_page` chokepoint). Tests: `src/rank.rs` units,
  `tests/ranking.rs` (engine-level + newest-copy + recall guard), the co-located handler tests in
  `search.rs`.
