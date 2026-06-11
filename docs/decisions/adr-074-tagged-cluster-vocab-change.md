# ADR-074: Tagged-cluster vocabulary change — TagId carry-through

> [Back to the decisions index](../DECISIONS.md)


- **Status:** Accepted (2026-06-10). Closes [ADR-065](adr-065-distributed-v1-graduation.md)
  criterion **4** (the [ADR-055](adr-055-cluster-tags-filtered-percolation.md) deferral):
  `set_vocab` / `learn_and_apply` / the alias-registry apply paths now work on a **tagged**
  cluster instead of refusing fail-loud.
- **Context:** The cluster's blue/green vocabulary rebuild (ADR-046 mechanism 2) reconstructs
  every query from its live DSL via `live_sources()`, which carried no tags — and a query's tags
  cannot be reconstructed from a shard's stored `TagId`s alone once the strings are gone: a
  post-freeze tag resolves to a *synthetic* id (ADR-046/055) whose string is never interned
  anywhere durable (the clog carries raw tags, but a checkpoint truncates it). ADR-055 therefore
  refused the rebuild on any tagged cluster, latched by `tags_present`. ADR-065 hypothesized
  "persist raw tag strings so the rebuild can reconstruct synthetic tags."
- **Decision — carry stored `TagId`s through the rebuild; do NOT persist strings.** The repo
  already holds the proven precedent: the single-node recompile
  (`Engine::recompile_stale_segments`, ADR-049) carries each live query's stored ids through a
  vocabulary change unchanged, because **the tag space is orthogonal to vocabulary and is
  preserved across the rebuild** — an interned id keeps its dense slot in the shared frozen
  `TagDict` (the rebuild re-mints the *feature* dict, never the tag dict), and a synthetic id is
  a deterministic FNV hash, stable forever. The cluster does the same:
  - `Engine::live_sources_tagged()` pairs `live_sources` with each logical's current live
    `TagId`s (the existing `live_tag_ids_for` lookup); a new `Shard::live_sources_tagged` seam
    (default `Err`, `LocalShard` + `ReplicatedShard` impls) gathers it per shard, and
    `ClusterEngine::set_vocab` dedups by logical (fanned-out copies carry identical tags).
  - `PlacedQuery` gains `tag_ids: Vec<TagId>` — pre-resolved ids carried verbatim; the shard's
    `ingest_extracted` unions them with the resolved raw `tags` (re-establishing the
    sorted/deduped column invariant). Re-placement moves a query's tags with it: an alias can
    change a query's anchor — hence its shard — and the filtered-read contract requires its tags
    on whichever shard now holds it.
  - **Why not the ADR-065 raw-string hypothesis:** id carry-through is *byte-identical*
    reconstruction (no re-resolution, no synthetic-collision ambiguity) with **zero new durable
    state** — strings would need a manifest format bump plus a permanent coordinator-side
    synthetic-string table, to reconstruct strictly weaker information that `get_or_synthetic`
    would just hash back to the same ids. It also keeps ADR-049's "tag strings die at the
    boundary" invariant intact. A future cross-process rebuild composes: ids are exactly what the
    tag-dict fingerprint handshake (ADR-055; hardened by ADR-065 criterion 9) certifies both
    sides agree on.
  - **The wire stays dict-agnostic:** `tag_ids` never cross gRPC — the proto ships raw
    `(key,value)` tags only, and a synthetic id has no recoverable string to send.
    `RemoteShard::ingest_extracted` refuses a non-empty `tag_ids` loudly (defense in depth;
    `set_vocab` refuses a non-local cluster before ever building such a bucket).
  - The `tags_present` latch is demoted to `/_stats` introspection (`has_tagged_queries`) — it is
    no longer load-bearing for correctness (it was also lossy: a checkpointed synthetic-only
    cluster restores it `false`, which under the old guard would have *passed* the refusal and
    silently dropped tags — a blind spot this mechanism closes structurally, since the rebuild
    reads ids from the shards themselves). The non-local and multi-word-alias refusals (ADR-061)
    are unchanged.
- **Two pre-existing durability bugs found by the new oracle (both fixed here).** The first
  durable tagged-rebuild test immediately failed — for reasons predating tags entirely. The
  segments-only shard mode (ADR-032) never persisted the **source store** on two paths, so a
  checkpoint + reopen left `sources.dat` absent or stale; matching never noticed (segments are
  the match path), but `live_sources` — the rebuild's gather, and `GET /_doc`'s `_source` — was
  silently wrong:
  1. **Bulk ingest never wrote sources.** `Engine::ingest_extracted` sealed durable segments but
     skipped the `sources.dat` write the single-node bulk path (`commit_base_segment`) has always
     done ("bulk has no WAL backstop"). A cluster built durable, checkpointed, and reopened
     gathered an **empty** corpus — `set_vocab` then rebuilt the cluster to zero queries.
     Fixed: `ingest_extracted` persists the source store after a successful seal.
  2. **A clean-shard checkpoint skipped the sources rewrite.** `flush()` early-returns on an
     empty memtable (its sources save included), so a delete (a tombstone, not a memtable entry)
     followed by a checkpoint left the deleted query's source on disk — and a post-reopen
     `set_vocab` would **resurrect a deleted query**. Fixed: the checkpoint seal
     (`seal_for_checkpoint_at`) now runs `flush_and_persist_sources_for_checkpoint`, which
     persists the store even when the memtable is empty; a write failure degrades
     `persistence_healthy` and the seal aborts fail-closed *before* trimming the translog
     (ADR-051 shape).
- **Why this is safe:** the carry-through touches neither signature gating nor the verifier —
  tags only ever *remove* matches at the verify stage (ADR-049), so carrying them exactly
  preserves filtered recall, and the unfiltered path is untouched. An untagged cluster gathers
  empty tag vectors ⇒ byte-identical to the pre-ADR-074 rebuild. The sources fixes only *add*
  writes at existing commit points (bulk seal, checkpoint seal); in-memory engines no-op.
- **Proven:** `tests/cluster_oracle/filtered.rs` — the flipped refusal test (synthetic-only live
  tags survive `set_vocab` + a `learn_and_apply` second rebuild) and the K-swept differential
  (tagged corpus + post-freeze synthetic live add + alias rebuild: filtered ≡ brute-with-tags,
  filtered ⊆ unfiltered, a re-placed alias-form query keeps its tags);
  `tests/cluster_durability_oracle/vocab.rs` — checkpoint → reopen → `set_vocab` → second reopen
  with the same differential (the synthetic string exists nowhere when the rebuild runs), the
  synthetic-only latch-blind-spot regression, and the two sources-durability regressions
  (bulk-built corpus survives reopen + rebuild; a deleted query is NOT resurrected);
  `cluster::remote` unit — the wire guard. Handler test flipped: cluster `PUT /_vocab` on a
  tagged cluster returns 200 and the filter still narrows. Full default + `distributed` suites
  green.
- **See also:** ADR-046 (dynamic vocabulary — the rebuild this extends), ADR-049 (single-node
  tags + the carry-through precedent), ADR-055 (cluster tags + the deferral this closes),
  ADR-061 (the multi-word refusal that stands), ADR-065 (the Distributed-v1 program), ADR-032
  (segments-only shard durability — the mode the sources fixes harden), ADR-051 (fail-closed
  persistence).
