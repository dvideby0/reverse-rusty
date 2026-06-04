# ADR-053: NPMI corpus phrase induction as a runtime vocab source

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted

- **Context.** Two corpus learners existed but were never connected. The `learn` binary
  (`src/bin/learn.rs`) mines multi-token **entities** (e.g. `upper deck` → `upper_deck`) from query
  text via **NPMI collocation mining** (the word2vec/Mikolov phrase trick) — proven in the research
  spike ([corpus-feature-learning.md](../research/corpus-feature-learning.md)) to rediscover every
  hand-built anchor with zero hand-coded vocabulary. But its core was inline in the binary, printed to
  stdout, and produced no runtime artifact. Separately, the `Vocab` system already feeds the proven
  runtime-apply path — `Engine::set_vocab` → `recompile_stale_segments` (single engine) and the
  blue/green re-place rebuild (cluster) — but only the **ADR-015 any-of learner** (synonyms *declared*
  in query DSL) drove it, via `learn_and_apply`. The corpus learner, which induces the feature model
  from the corpus *itself*, was disconnected. This is the Tier-2 "wire the NPMI learner as the runtime
  vocab source" roadmap item.

- **Decision.** Lift the NPMI core into the library and compose it under the any-of learner as an
  **opt-in** runtime vocab source:
  - **`src/corpus.rs`** (new, lean-core / std-only): the NPMI core moved out of `bin/learn.rs`
    (`tokenize` / `learn_phrases` / `apply_phrases` / `Phrase`, signatures unchanged) plus a high-level
    `learn_phrases_from_text(corpus, min_count, tau, iterations) -> Vocab`. It tokenizes each query's
    text, runs NPMI bigram mining iterated bigram→trigram, and emits each discovered entity `a_b[_c…]`
    as a `PhraseEntry` mapping its parts to the canonical feature `term:a_b[_c…]` (the same `term:`
    convention as `vocab::learn_from_queries`), kind `Generic`. Output is token-sorted for determinism.
    The `learn` binary now calls the library functions — its output is unchanged.
  - **`vocab::CorpusLearnConfig`** + **`vocab::learn_vocab_from_corpus`**: the composition seam. The
    any-of learner is the base; when `corpus_phrases` is set, NPMI phrases are merged **second**, so on
    a token collision the user-declared any-of phrase wins (`Vocab::merge` is first-wins). The default
    **disables** NPMI, so the result is byte-identical to `learn_from_queries` alone.
  - **`Engine::learn_and_apply_with(cfg)`** and **`ClusterEngine::learn_and_apply_with(cfg)`**: thread
    the config through the existing apply paths unchanged. `learn_and_apply(min_count)` becomes a thin
    wrapper (NPMI off), so every existing caller and oracle is unaffected.
  - **REST**: `POST /_vocab/learn` (review) and `POST /_vocab/learn_and_apply` (apply) gain opt-in
    `corpus_phrases` / `npmi_tau` / `npmi_min_count` / `npmi_iterations` params; absent ⇒ today's
    behavior exactly.

- **Why it is recall-safe (the priority).** Reverse Rusty is a **recall-first stage-one candidate
  generator** — a downstream precise matcher filters false positives, so a *dropped candidate* is the
  cardinal sin and false positives are cheap. NPMI emits **phrases only** (entity gluing), never
  aliases. Critically, corpus phrases are applied **additively**: a phrase match emits the phrase
  feature AND keeps the component features, so a query referencing a *component* of an induced phrase
  never loses a candidate (the original collapse behavior — consume the components — *would* drop it,
  which is why it was rejected). The same normalizer is applied to queries (recompile / cluster
  rebuild) and titles (match), so the differential oracle — an independent brute force using that
  normalizer — stays equivalent (engine ≡ brute), faithful to the model.
- **Honest scope (the residual).** Phrase induction is still a *re-tokenization*: a query whose text
  is *phrased* as the entity (e.g. `upper deck`) tightens to require the adjacent phrase, so it no
  longer matches a title where the two tokens are non-adjacent. For genuine entities (which appear
  adjacent in real titles) this is negligible, and the feature is **opt-in + reviewable + reversible**;
  it is *not* a blanket "no prior match ever changes" guarantee. The contract that always holds is the
  **lossless cover for the active model** ([design/README.md](../design/README.md) §2) — the
  implementation retrieves everything that matches under the current normalizer. This is the
  load-bearing distinction from **alias / equivalence learning** ([ADR-054](adr-054-equivalence-expansion.md)),
  which is applied by **expansion** and is therefore *fully monotonic* (it only ever adds matches).
  Pinned by `tests/oracle.rs::corpus_phrase_induction_preserves_component_query_recall` (additive
  recall preservation) and `::corpus_phrase_induction_tightens_phrase_query_to_adjacency` (the residual).

- **Alternatives considered.** (1) *Fold NPMI into `learn_and_apply` on by default* — rejected: it
  would change the existing endpoint's behavior and perturb every oracle. Opt-in keeps the default path
  byte-identical and matches the project's precision-first ethos (operators review via `/_vocab/learn`,
  then apply). (2) *A separate `learn_phrases` module folded into `vocab.rs`* — rejected: a new
  `corpus.rs` keeps the collocation math separate from the `Vocab` data model (one concern per module).
  (3) *Replace the any-of learner with NPMI* — rejected: the two are complementary (declared aliases
  are high-confidence; corpus phrases are induced), so they compose rather than compete.

- **Scope / non-goals.** Phrases only — alias/equivalence learning stays deferred (its safety rail is a
  separate item). `npmi_min_count` defaults small (3, configurable) because a live corpus is far smaller
  than the `learn` binary's 500k-synthetic-query default (50). The cross-process shipping of the learned
  normalizer to a remote shard remains deferred (the in-process / RF=1 path is exercised here, same as
  ADR-046). **Compaction-that-improves** (re-anchoring on frequency drift) — the sibling Tier-2 item —
  is independent and not addressed here.

- **Testing.** `corpus.rs` unit tests (induces a planted collocation → exact `PhraseEntry`; respects
  `min_count`/`tau`; dedup + determinism; empty corpus; bigram→trigram growth). A single-engine
  differential (`tests/oracle.rs::zero_false_negatives_after_corpus_phrase_learn_and_apply`): build with
  the empty `default_vocab`, `learn_and_apply_with(corpus_phrases=true)`, then assert engine ≡ a brute
  carrying the engine's **own learned normalizer** — zero FN/FP. A cluster differential
  (`tests/cluster_oracle.rs::learn_and_apply_with_corpus_phrases_preserves_zero_false_negatives`):
  K∈{1,3,8}, induce a planted phrase, assert `percolate` ≡ the phrase-aware brute over the live set
  (re-placement under an induced feature preserved, zero FN). Composition guards in `vocab.rs` (default-off
  equals any-of alone; on adds the phrase). The recall-first additive behavior + the residual are pinned
  by `tests/oracle.rs::corpus_phrase_induction_preserves_component_query_recall` and
  `::corpus_phrase_induction_tightens_phrase_query_to_adjacency`. The default-off existing oracles are
  byte-identical by construction.

- **Consequences.** The engine can self-derive multi-token entity features from its own live corpus and
  apply them through the proven machinery — **additively, so a component query never loses a candidate**
  (recall-first) — closing the headline Tier-2 self-tuning item. The feature model can improve without
  hand-coded vocabulary or declared any-of groups; the default behavior is unchanged. The honest caveat
  (a phrase-form query tightens to adjacency) is documented above and pinned by tests.
