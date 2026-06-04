# ADR-055: Threading per-query tags + filtered percolation through the cluster

> [Back to the decisions index](../DECISIONS.md)


- **Status:** **Built + oracle-proven (2026-06-04)** — end-to-end through the in-process multi-shard
  core AND the experimental `distributed`/gRPC path. Builds on the single-node feature
  ([ADR-049](adr-049-percolator-parity-tags.md)) and the dynamic-vocabulary id model
  ([ADR-046](adr-046-dynamic-vocabulary.md)).
- **Context:** Per-query metadata tags + filtered percolation are the dominant production read pattern
  ("percolate, then narrow to one category/status"). ADR-049 built them on the single-node engine
  (interned `TagId`s, a SoA tag column, a verify-stage `TagPredicate` that never gates, `.seg` v3 / WAL
  v2 persistence). But the **cluster dropped tags**: `Engine::ingest_extracted`/`insert_extracted` (the
  cluster-shard ingest paths) passed `&[]` for tags, the `ClusterEngine` had no public tag ingress, and
  the gRPC wire carried no tags or filter. A cluster deployment therefore could not use a feature its
  own single-node engine supported.
- **Decision:** Thread tags through the cluster by mirroring exactly how the frozen feature `Dict` is
  shared cross-shard, applied to the `TagDict` (which was built for this — `get_or_synthetic`,
  `mark_finalized`, `fingerprint` are documented "for the cluster apply path"):
  1. **One frozen `Arc<TagDict>`, shared like `Arc<Dict>`.** The coordinator builds the `TagDict` over
     the corpus tags at `build_with_tags`, `mark_finalized()`s it, and shares the same `Arc<TagDict>`
     read-only into every shard's `Engine`. `ClusterEngine` gains a `tag_dict` field, persisted in the
     cluster manifest (`tag_dict_data`, manifest v4 already round-trips it) and restored on `open`.
  2. **Raw `(key,value)` tags live in the log and travel the seam** — `ClusterMutation::Add` gains a
     `tags` field (clog/translog `CLOG_VERSION` 1→2; the tag block is appended only when non-empty, so
     an untagged frame is byte-identical to v1, and a length-framed v1 record reads back as empty). This
     matches the project's "raw DSL in the log, re-derive on replay" invariant — the tags-on-wire/in-log
     analogue of raw DSL.
  3. **Resolve read-only at apply, never `intern`.** Tags resolve to `TagId`s via
     `tag_dict.get_or_synthetic(k,v)` — a hit keeps its dense id, a post-freeze miss gets a deterministic
     *synthetic* id every node agrees on (ADR-046). **Never** `Engine::intern_tags` (the `Arc::make_mut`
     copy-on-write path), which would fork the shared dict per shard and assign **inconsistent dense
     ids** → silent mis-filtering. A new `Engine::resolve_tags_readonly` enforces this (with a
     `debug_assert!(is_finalized())` tripwire); the cluster ingest paths call it instead of the live
     `*_with_tags` family.
  4. **Filter resolved once at the coordinator.** `ClusterEngine::percolate_filtered(title, filter)`
     compiles the `FilterSpec` → `TagPredicate` via the coordinator's frozen `TagDict` (mirroring
     `EngineSnapshot::compile_tag_predicate`), then threads the **same `&TagPredicate`** to every probed
     shard's `Shard::percolate_filtered` (like the already-resolved `include_broad`). An empty predicate
     is byte-identical to the unfiltered `percolate`.
  5. **gRPC: ship the tag dict with the dict; ship resolved filter ids.** `AdoptDict` carries the
     serialized `TagDict` + a `tag_dict_fingerprint`, adopted **atomically** with the dict and
     fingerprint-checked (a divergent tag space fails loud, like `DictMismatch`). `AddItem` carries raw
     `(key,value)` tags (riding into both ingest/insert AND the `FetchTranslog` peer-recovery stream, so
     a recovered replica keeps its tags); `PercolateRequest` carries the **already-resolved** `TagId`
     filter groups (globally consistent, so the server rebuilds `TagPredicate::new(groups)` without
     re-resolving strings — immune to any server-side tag-space skew on reads).
  6. **Tags never gate** — `route()` keys only on features; the predicate applies post-candidate in
     verify. The lossless-cover contract is untouched, structurally as single-node (ADR-006/049).
- **Public surface (additive — the untagged path is byte-identical):** `build_with_tags`,
  `add_query_with_tags`, `ingest_with_tags`, `percolate_filtered` (+ `percolate_filtered_with_broad`),
  `compile_tag_predicate`; the existing `build`/`add_query`/`ingest`/`percolate` delegate with empty
  tags/filter. The internal `Shard` trait drops the unfiltered/untagged convenience methods in favor of
  `percolate_filtered` + `insert_extracted_with_tags` (one method per operation); the bulk-ingest item
  is a named `PlacedQuery` struct (replacing the `(u64,Extracted,String,u32)` tuple) carrying its tags.
- **Consequences:**
  - Filtered percolation works through the cluster (in-process + gRPC) with the same boolean-correct,
    zero-false-negative semantics as single-node, surviving durable reopen and peer recovery.
  - **Synthetic-vs-dense consistency invariant** (the subtle one): a corpus tag interns to a *dense* id
    at build and re-resolves dense via `get_or_synthetic` (a hit); a tag only ever seen post-freeze
    resolves *synthetic* on both the add and the filter-compile side (both use `get_or_synthetic` against
    the same frozen `TagDict`). The danger would be one side interning while the other hashed — which
    cannot happen because the cluster never interns post-build.
  - Byte-identical untagged path: empty `TagPredicate` ≡ `match_title`; untagged clog frames write no tag
    bytes; `tag_dict_data` round-trips empty; the lean (`--no-default-features`) build is unaffected
    (`TagPredicate`/`TagDict` live in always-compiled modules). Every prior cluster oracle stays green.
- **Scope / deferred:** A runtime **vocabulary change on a tagged cluster** (`set_vocab` /
  `learn_and_apply`) is **refused** (fail-loud): the blue/green rebuild reconstructs queries from their
  DSL via `live_sources()`, which carries no tags, and a synthetic post-freeze tag has no recoverable
  string — so a rebuild would silently drop tags. The tag space is orthogonal to vocabulary and is
  otherwise preserved; combined tags + live vocab change is a follow-on. Ranking + `/_mpercolate`
  pagination remain ADR-049 decision-point-4 (design-only). Cross-process normalizer shipping is
  unchanged (still the experimental-path assumption).
- **Proven by:** `tests/cluster_oracle.rs` (`filtered_percolation_matches_single_node_and_oracle` —
  cluster ≡ single-node ≡ brute under a filter sweep, across K∈{1,3,8,16}×RF∈{1,2}, filtered ⊆
  unfiltered; `live_tagged_add_is_filterable_with_post_freeze_tag` — synthetic-tag cross-shard
  consistency), `tests/cluster_durability_oracle.rs` (`tagged_cluster_survives_checkpoint_and_reopen` —
  the manifest `tag_dict_data` + clog/segment tag round-trips), and
  `tests/cluster_grpc_oracle.rs` (`grpc_filtered_percolation_matches_single_node_and_oracle` — tag-dict
  shipping + fingerprint handshake + tagged bulk load + filtered percolate + a live tagged add, all over
  the wire).
- **Design:** [`design/matching.md`](../design/matching.md) §5;
  [`design/clustering-and-scaling.md`](../design/clustering-and-scaling.md) §3.
