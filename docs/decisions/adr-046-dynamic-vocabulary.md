# ADR-046: Dynamic vocabulary (Cluster v1) — feature-hashing for new tokens + runtime normalizer learning for aliases

> [Back to the decisions index](../DECISIONS.md)


- **Status:** Accepted + **implemented** (approach chosen by the dynamic-vocabulary research spike).
  **Both mechanisms are built and oracle-proven.** (1) feature-hashing for new tokens
  (`dict::synthetic_id`/`get_or_synthetic` + both readonly paths hash; additive — prior oracles
  byte-identical). (2) runtime normalizer learning for aliases: a synchronous **recompile pass**
  (`Engine::recompile_stale_segments` — recompile every live query under the new normalizer, clearing
  the vocab-epoch staleness) for the single engine + a cluster **blue/green rebuild**
  (`ClusterEngine::set_vocab` — re-mint the dict, re-place every query, atomic swap; durable via a
  manifest `vocab_data` blob, manifest **v3**) + **auto-learning** (`learn_and_apply` wires the ADR-015
  any-of learner as a runtime vocab source). Proven by `tests/cluster_oracle.rs`
  (absorb-without-broadening, satisfiable all-unknown any-of, **declared alias makes both surface forms
  match**, auto-learn) + `tests/cluster_durability_oracle.rs` (alias survives reopen + rebind) +
  `tests/hardening_fixes.rs` (single-engine recompile + learn). **In-process only:** `set_vocab` refuses a
  non-local cluster (an alias is normalizer-only and is not shipped to a `RemoteShard` — the cross-process
  shipping below is beyond v1). Prior-art survey: [`research/dynamic-vocabulary.md`](../research/dynamic-vocabulary.md).
- **Context:** The cluster freezes one shared dictionary so every shard agrees on each term's integer
  `FeatureId` (ADR-027 — globally-consistent ids are what make the cross-shard signature cover lossless).
  The cost: a live write whose query introduces a term absent from the frozen dict was **silently dropped**
  in the read-only compile (`cluster/coordinator/ingest.rs:140`, `cluster/server.rs:211` →
  `compile::extract_readonly`) — the query broadened, and an all-unknown any-of group risked a **false
  negative**. A production percolator over eBay-style listings must instead **absorb** new vocabulary. The
  hard part: our **content-routed** sharding (a title routes by its anchor `FeatureId`) needs *cross-shard
  agreement* on a new term's id — unlike a scatter-gather engine, which never needs two nodes to agree.
- **Prior art (two camps, neither a direct fit — survey in the research doc):** *growable local
  dictionaries* (Vespa attributes — dynamic + real-time, but **node-local** ids, works only because Vespa
  scatter-gathers; Lucene/ES per-segment term dicts) and *rebuild-based globalization* (ES/OS **global
  ordinals** — exact, but **per-shard** and a **rebuild** on refresh). Cross-shard-consistent ids *without*
  a rebuild or coordination point to **feature hashing** (Weinberger et al. 2009 — the hashing trick).
- **Decision — two complementary mechanisms:**
  1. **New tokens → deterministic feature-hashing.** A term absent from the frozen dict gets
     `FeatureId = RESERVED_BASE | fold_u32(fnv1a64(name))`, in a reserved high-`u32` range disjoint from the
     dense interned ids (`dict.rs:13` — ids are `u32`, interned densely, so a high range is free). Every
     shard + the coordinator compute the **same** id independently (no coordination; in-process ≡
     cross-process — `fnv1a64` is already our stable cross-process hash, `util.rs:13`). Synthetic ids
     **bypass the immutable `Arc<Dict>`** (never interned, never serialized, don't change the fingerprint —
     `storage.rs:1370`/`dict.rs:176`), so the ADR-034 handshake is untouched. The exact matcher compares
     ids by `binary_search` (`exact.rs:54`), so synthetic ids work unchanged — and a collision is a
     **bounded false *positive* that survives verification, never a false negative** (a term always hashes
     the same, so query-requires-`t` and title-contains-`t` always agree). This *fixes* both original bugs
     (broadening + any-of collapse).
  2. **New alias / synonym rules → runtime normalizer learning.** Aliases (`Upper Deck` ≡ `UD`) are a
     *normalizer* operation — only the normalizer sees raw text and can canonicalize two surface forms to
     one feature name *before* id assignment, so hashing cannot express them. Reuse the ADR-015 `Vocab`
     machinery (it already learns synonyms from query any-of groups) to rebuild the `Normalizer` and swap
     its `Arc`. **In-process (the Cluster v1 core) there is one shared `Arc<Normalizer>`, so the swap is
     atomic — no propagation window.** A change bumps the vocab epoch; queries compiled under the old epoch
     are recompiled (the existing vocab-epoch staleness machinery) so the "same normalizer for queries and
     titles" invariant holds and zero-FN is preserved. **As built:** the recompile must be **synchronous**
     (a stale segment carries old-normalizer ids, so a lazy window would drop matches), and at the cluster
     level it is a full **blue/green rebuild from the live corpus** — re-mint the dict + **re-place** every
     query, because an alias can change a query's anchor feature (hence its shard), so an in-shard recompile
     would strand it on the wrong shard. Durability lives in the manifest (a serialized `Vocab`), not the
     log — `set_vocab` rebuilds then checkpoints, so a runtime alias survives reopen; no `SetVocab` log op
     (which would mis-replay alias rebind/removal, since `Vocab::add_synonym` is first-write-wins).
- **Correctness (load-bearing):** (a) **both sides hash** — the title path (`normalize::match_features`)
  *and* the query path (`compile_features_readonly`) must hash unknown tokens; dropping a title token would
  re-introduce an FN. (b) **collisions are bounded, tunable, never FN** — a ~31-bit range gives birthday
  collisions around tens of thousands of *distinct* unknown terms, and a raw id-collision becomes a
  *visible* wrong match only when a query is anchored on the collided term *and* a title carries the other
  colliding term, so the effective rate is far lower; tunable by range size, with an optional **v2**
  hot-term promotion into exact interned ids at compaction. (c) **guard by-id dict lookups**
  (`mask_bit`/`kind`/`name`) — a synthetic id is out of the interned Vecs' range → treat as
  non-hot/non-mask/unknown-name (synthetic ids are rare by construction → never in the 64-hot common mask,
  always the exact verifier's non-mask required tail).
- **Scope for v1 (tokens + aliases, in-process):** hashed tokens need **no shipping** cross-process (every
  node computes them), so the token half works in-process *and* cross-process for free. The alias half's
  **cross-process normalizer *shipping*** (a versioned normalizer + the propagation-window consistency
  design, analogous to dict shipping ADR-034) rides with the experimental distributed layers and is
  **beyond v1** *(decided at v1 by [ADR-076](adr-076-cluster-multiword-aliases-vocab-shipping.md): live
  shipping stays refused — remote-cluster vocabulary is deploy-time configuration, the ES
  analyzer-reindex precedent)*.
- **Alternatives declined:** *coordinator-assigned exact ids* (ES global-ordinals style via the control
  plane) — exact, but adds a coordination step + a propagation window (transient FN) + Raft coupling, a
  hazard hashing avoids; *post-freeze dict mutation* — the dict is an immutable shared `Arc`, and breaking
  it would fork the feature space across shards.
- **Consequence:** new vocabulary is absorbed with matching correct (**zero false negatives**), no
  coordination for tokens, and reuse of existing `Vocab`/epoch machinery for aliases. The only cost is a
  bounded, tunable false-positive rate from token-hash collisions (plus, for a durable cluster, a benign
  per-shard `sources.dat` accumulation across repeated `set_vocab` calls — matching uses segments and
  `live_sources` de-dups, so correctness is unaffected; a future sources rewrite reclaims it). Both
  mechanisms + the absorb-correctly oracle assertions are **built** — the Cluster-v1 Tier-0 deliverable
  (STATUS.md).
- **See also:** ADR-027 (the shared frozen dict + content routing this preserves), ADR-015 (the `Vocab`
  synonym-learning the alias half reuses), ADR-034 (dict shipping — the template for the deferred
  cross-process normalizer shipping), [`research/dynamic-vocabulary.md`](../research/dynamic-vocabulary.md).
  Code sites: `src/dict.rs`, `src/normalize.rs` (`match_features`, `compile_features_readonly`),
  `src/compile.rs` (`extract_readonly`), `src/vocab.rs`, `src/cluster/coordinator/ingest.rs`,
  `src/cluster/server.rs`.

