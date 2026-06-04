# ADR-054: Equivalence (alias) learning via expansion, not collapse

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted

- **Context.** Surface-form variation is the dominant threat to recall in this engine's domain —
  eBay titles spell the same entity many ways (`UD`/`Upper Deck`/`upperdeck`, `rc`/`rookie`,
  `psa10`/`PSA 10`). A saved search that misses a listing because of a variant is a **false
  negative — the cardinal sin**; over-matching is tolerable (the integer exact-verifier and the
  domain both absorb it). So equivalence handling serves the zero-FN mission, but its failure mode
  must be FP, never FN. The existing alias path (ADR-015 any-of learning → `Vocab` synonyms) applies
  equivalences by **collapse**: the normalizer rewrites an alias to the canonical's feature, and the
  canonical's bare token emits the same feature, so both sides agree. That is FN-safe *only* because
  the shared normalizer + recompile keep query and title symmetric — its failure mode if anything
  desyncs is a **false negative**. For *learned* (less-certain) equivalences that is the wrong risk.

- **Decision.** Add a first-class equivalence primitive applied by **expansion, not collapse**
  (the roadmap's precision-first safety rail):
  - **`Extracted::expand_equivalences`** (`compile.rs`): a query-side compile-time rewrite — a
    required feature in a learned equivalence group `G` moves out of `required` and becomes an
    any-of group `G` (a title bearing ANY member still retrieves the query); existing any-of groups
    are widened by their members' groups. `forbidden` is never touched. Because it only ever
    **widens** the accepted positive set, the query's match set can only grow.
  - **`dict::EquivMap`** (member `FeatureId` → its group), carried on the `Dict` as a **transient**
    field (re-derived from the vocab; not serialized, not part of `Dict::fingerprint`, so the
    dict's cross-process identity is unchanged). `extract`/`extract_readonly` consult it, so the
    single-engine compile paths (build/recompile/insert) expand uniformly; the cluster `set_vocab`
    additionally applies the pure pass to its already-extracted vec so re-placement + ingest use
    the widened form (a query whose anchor is now an any-of fans to every member's shard).
  - **`Vocab.equivalences: Vec<Vec<String>>`** — the first-class, declarable + learnable + persisted
    representation. Resolved to an `EquivMap` against the normalizer + dict at apply time
    (`Vocab::resolve_equivalences`; a form is skipped unless it resolves to exactly one feature, and
    a group needs ≥2 distinct features).
  - **Sources (high-precision first):** *declared* — operators `PUT /_vocab` an `equivalences` block
    (curated alias lists); *learned* — `learn_equivalences_from_queries` mines any-of co-occurrence
    and emits equivalence groups, opt-in via `CorpusLearnConfig::learn_equivalences` (expansion mode)
    instead of collapse synonyms. Threaded through `Engine`/`ClusterEngine::learn_and_apply_with`
    and the `/_vocab/learn[/_and_apply]?learn_equivalences=true` params.
  - **Opt-in + reversible + default-off byte-identical.** No declared/learned equivalences ⇒ empty
    `EquivMap` ⇒ expansion is a no-op ⇒ every existing oracle is unaffected. Reversal is dropping the
    group + recompiling.

- **Why it is FN-safe (the load-bearing property).** Expansion only widens a query's accepted
  positive features, so its match set is a **superset** of the literal query's — it **cannot
  introduce a false negative**, *structurally*, with no dependence on the symmetric-recompile
  discipline that collapse needs. A wrong/low-confidence equivalence degrades to a **bounded false
  positive** (the engine's tolerable failure mode). It also correctly relaxes the common-mask gate
  (a hot feature moved to any-of leaves the required mask, so the gate can't wrongly reject a title
  bearing only an alias). Proven by `tests/oracle.rs::wrong_equivalence_never_causes_false_negatives`
  (a nonsense equivalence drops zero true matches vs the original-semantics oracle) +
  `::equivalence_expansion_grows_matches_and_is_fn_safe` (monotone).

- **Best-long-term framing / what's deferred.** This ships the **durable, correct foundation** — the
  FN-safe application primitive + a source-agnostic representation + the two high-precision sources
  (declared, any-of-learned). The lower-precision automated discovery is deliberately deferred behind
  this seam, in precision order: **distributional discovery** (context-set similarity — noisy: it
  conflates substitutes like `rc`/`rookie` with co-hyponyms like `psa`/`bgs`, so it needs review-first
  gating) and **match-feedback validation** (the highest-precision automated signal, but it needs an
  operational title→query loop). Building those before the mechanism is proven — and before feedback
  exists to validate them — would put a low-trust heuristic in front of an unproven primitive. Both
  feed the same `Vocab.equivalences` + expansion mechanism when built.

- **Alternatives.** (1) *Reuse collapse for learned aliases* — rejected: its failure mode is FN, the
  cardinal sin, and it merges the feature space globally (changes anchoring/stats for everything).
  (2) *Apply expansion at the DSL-rewrite level* — rejected: equivalence is feature-level, and
  feature→surface-form inversion is not 1:1. (3) *Thread the map through `extract` as a parameter* —
  rejected for ~15 call sites; the transient dict field covers them uniformly without changing
  `extract`'s signature, and a missed site degrades safely (no expansion ≠ FN).

- **Testing.** `compile.rs` units (the pure rewrite: required→any-of, widening, no-op, idempotent);
  `vocab.rs` units (learns equivalences from any-of; expansion mode emits groups not synonyms);
  `tests/oracle.rs` (declared equivalence makes both forms match + monotone; the wrong-equivalence
  FN-safety proof; the learned-via-expansion end-to-end path); `tests/cluster_oracle.rs` (declared
  equivalence ≡ an equivalence-aware brute across K∈{1,3,8}, re-placement under expansion, zero FN);
  `tests/cluster_durability_oracle.rs` (equivalence survives crash + reopen ≡ pre-crash ≡ oracle).

- **Consequences.** The engine can now express and apply entity equivalences the FN-safe way —
  declared by operators or learned from the corpus's any-of groups — closing the alias half of
  Tier-2 self-tuning (the phrase half is [ADR-053](adr-053-corpus-phrase-vocab-source.md)). The
  zero-false-negative contract is preserved structurally, and the default path is byte-identical.
