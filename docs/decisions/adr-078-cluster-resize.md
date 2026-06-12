# ADR-078: Cluster resize — the auto-split mechanism

**Status:** Accepted (2026-06-11)

**Context.** ADR-065 criterion 7 — *"the ring's `num_shards` stops being fixed at
construction."* The autoscaler already **detects** an over-large shard
(`autoscale::corpus_split` → an advisory `RecommendSplit`, ADR-045) but had no mechanism to
act on it: the shard count `K` was frozen at `ClusterEngine::build`. The roadmap framed this
as the largest open item — an ES `_split`-style online ring re-keying with the data moved by
the live handoff (ADR-044). Research into the existing code found a far cheaper, lower-risk
path: `ClusterEngine::set_vocab` (ADR-046) is *already* a full blue/green rebuild — gather the
deduped live `(logical, dsl, tag_ids)` corpus across shards, re-mint the dict, **re-place every
query** via `placement_of(&dict, &self.ring, &ex)`, build fresh shards, atomic swap under
`&mut self` + checkpoint. The *only* thing tying that rebuild to the shard count is `self.ring`.

**Decision.** A resize is the `set_vocab` blue/green rebuild with the **ring swapped (K→K′)
instead of the normalizer**. The shared core `rebuild_from_live(new_norm, new_ring, new_vocab)`
(extracted from `set_vocab`) re-places every live query under a fresh `HashRing::new(K′,
vnodes)`. Re-placing the *whole* corpus under a fresh ring makes correctness trivial — placement
and routing both read the new ring, the same invariant a fresh `build` relies on — so the hard
"online ring re-keying" problem is **sidestepped entirely** (we never mutate a ring in place).

- **Public surface:** `ClusterEngine::resize(K′)` (the mechanism), `recommended_shard_count(snapshot,
  config)` = `K + #(shards over `split_corpus_threshold`)` (the pure, monotone-within-a-snapshot
  signal), `resize_to_recommended(config)` (operator/test one-call apply), and `POST
  /_cluster/resize` (the coordinator-mode endpoint, write-locked like `PUT /_vocab`). The
  autoscaler's `tick` keeps `RecommendSplit` **advisory** — always-on auto-execution is deferred
  (it needs hysteresis to avoid thrash, since a resize is non-idempotent and `O(corpus)`).
- **In-process only (v1),** refusing a non-local / handoff-wrapped cluster — exactly the boundary
  `set_vocab` enforces. A remote shard would keep its old placement while the coordinator routes
  under the new ring (a silent cross-process false negative). A cross-process resize (shipping the
  re-keyed data over the handoff machinery) is the documented follow-on.
- **Durable for free:** the only durable change is `num_shards` growing/shrinking plus a
  correspondingly longer/shorter per-shard registry — both already expressible in `ClusterManifest`
  (no format bump). `checkpoint` writes `num_shards = self.ring.num_shards()`; `open` re-derives
  `HashRing::new(num_shards, vnodes)` and re-seeds the control plane via `single_node(num_shards)`.

**The one place resize differs from `set_vocab`: the shard-directory SET changes.** Handled
explicitly:

- **Grow** — new positions (`s ≥ old K`) have no shard to coexist with, so they FORCE-CLEAN their
  dir (`clean_shard_dir`) and build fresh via `new_durable`, never reusing an arbitrary segment
  counter. The clean is load-bearing: `new_durable` self-restarts from a leftover checkpoint
  sidecar, so a stale dir from a *prior shrink* would otherwise resurrect an old corpus (or, when
  the corpus diverged, fail loud with a `DictMismatch` — the fallback safety net).
- **Shrink** — surviving positions rebuild in place (the `set_vocab` coexist path, crash-safe: a
  crash before the manifest commit leaves the old segments authoritative); orphaned dirs
  `shard_{K′}..shard_{K-1}` are removed AFTER the commit (`remove_orphan_shard_dirs`), so the
  on-disk set is exactly `shard_000..shard_{K′-1}`.
- **Control plane** is re-seeded to `K′` (a new `ClusterStateChange::SetShardCount` through the
  shared `apply` funnel) so `collect_load` / `assignment_for` stay consistent.
- **`pending_repair`** (ADR-047) is cleared (its shard indices index the old space; the durable
  backstop is the log).
- **Vocab + tags preserved:** the normalizer is reused as-is and the existing vocab's equivalence
  groups are re-resolved onto the re-minted dict (a declared alias does not go silent); per-query
  tags carry through as stored `TagId`s (ADR-074). The re-minted dict is identical (same corpus,
  same normalizer) ⇒ the dict fingerprint is **invariant** across a resize.

**Why this is safe.** Every step reuses an already-oracle-proven mechanism (the `set_vocab`
rebuild, the `checkpoint` fail-closed commit ordering, the `open` reattach). Re-placing the full
corpus under a fresh ring is internally consistent by construction, so zero false negatives is
preserved trivially; an untouched cluster (K′ = K) is a no-op. The percolate hot path is
unchanged.

**Proven.** `tests/cluster_oracle/resize.rs` — resize K→K′ ≡ single-node ≡ brute across grow,
shrink, **shrink-to-1**, a five-step round-trip, broad on/off, tagged (filtered ≡ oracle), the
no-op/`0`-error guards, and `resize_to_recommended`. `tests/cluster_durability_oracle/resize.rs`
— survives checkpoint + reopen ≡ pre-crash ≡ brute (grow + shrink); the manifest records `K′`
with an invariant dict fingerprint; a shrink leaves exactly `K′` shard dirs; a post-resize live
add replays over the resized manifest; tags + a declared alias carry through the resize across
the restart; and **`shrink_then_regrow_does_not_resurrect_deleted_queries`** — the changed-dir-set
hazard, mutation-validated (disabling the dir cleanup fails it).

**See also:** ADR-046 (the `set_vocab` blue/green rebuild this reuses), ADR-027 (placement +
the cover proof), ADR-031/032 (the durable manifest + reattach), ADR-045 (the autoscaler split
advisory this makes real), ADR-074 (tag carry-through), ADR-065 (the program). Deferred: always-on
auto-resize behind hysteresis; cross-process/online resize over the handoff machinery.
