# ADR-102: Distributional alias discovery (review-first candidates)

> [Back to the decisions index](../DECISIONS.md)

- **Status:** **Accepted (2026-07-02).** `POST /_vocab/aliases/discover[_and_record]` — PPMI-cosine
  context-similarity candidates over the stored queries, filed as review-only `Candidate`s under a
  new never-auto-active provenance.

- **Context:** The Tier 2 roadmap item; technique 1 of
  [`research/corpus-feature-learning.md`](../research/corpus-feature-learning.md) §5. The shipped
  alias machinery is complete downstream of a *proposal*: ADR-054 applies equivalences FN-safely
  (expansion, never collapse), ADR-060 governs them (provenance/kind/status/confidence,
  `Rejected` stickiness), ADR-061 makes multi-word forms expressible. But the only automated
  proposal *sources* are query any-of co-occurrence (needs the operator to have written the
  disjunction) and Solr import (needs the operator to already know the synonym). The genuinely
  hard aliases — non-adjacent abbreviations like `ud` ≡ `upper deck` — arrive from nowhere today.
  Distributional similarity is the cheap corpus-derived source: two tokens used in the same query
  *contexts* are equivalence candidates. It is also structurally noisy — co-hyponyms (`psa`/`bgs`)
  share contexts exactly like substitutes do — which the roadmap acknowledges: "review-first,
  never auto-active."

- **Decision:**
  1. **Signal** (`src/vocab/distributional.rs`, lean/std-only): corpus = the engine's own stored
     queries (`live_sources()`; the compute-only endpoint also accepts an explicit `queries`
     body — the `POST /_vocab/learn` precedent, and the cluster dry-run path). Per query:
     positive clauses only (a forbidden term is not semantic context), atom surfaces tokenized by
     `corpus::tokenize` (the NPMI learner's granularity). Optional **NPMI phrase glue** first
     (default on) so `upper deck` participates as the unit `upper_deck` — what makes the
     token-vs-multi-word case discoverable. Context vector = same-query co-occurrence over the
     top-`max_vocab` eligible tokens; similarity = **cosine over PPMI-weighted vectors**,
     accumulated sparsely via an inverted index over context tokens (PPMI zeroes hub contexts,
     which keeps the accumulation sparse; the `max_vocab` cap bounds the key space at ≤ N²/2).
     Deterministic output: (similarity desc, forms asc), capped at `max_pairs`. Two build-time
     hardenings: the glue support floor is **10** (deliberately above the ADR-053 corpus-phrase
     default of 3 — there a junk phrase is harmless additive indexing; here a junk glued unit
     spawns whole families of high-similarity noise pairs that crowd the `max_pairs` budget),
     and a **shared-token filter** drops any pair whose forms share a literal token (`zzud
     ctxp0` vs `zzud ctxp5` is one phrase family — glue noise or a variant — never the
     abbreviation-style equivalence this discoverer exists for). Determinism is engineered, not
     assumed: the sparse PPMI vectors are sorted before any accumulation, so float summation
     order is fixed — an unsorted HashMap iteration order would perturb near-ties between two
     identical runs (caught by the determinism unit test).
  2. **The noise model — substitutes vs co-hyponyms.** Both share neighbor distributions; the one
     cheap discriminator the counting data offers is *syntagmatic* co-occurrence: true
     substitutes are paradigmatic (they fill the same slot, so a query rarely contains both);
     co-hyponyms co-occur (`(psa,bgs)` any-ofs, `jordan pippen` duals). A pair whose
     `cooc / min(freq_a, freq_b)` exceeds `max_cooccurrence_rate` is dropped. Numeric-only tokens
     (years, grades) are excluded by default — textbook co-hyponyms with near-identical contexts.
     Both are heuristics, hence the governance below, and the cost asymmetry that makes this
     safe: a wrong *candidate* costs a reviewer's minute; a wrong *activation* costs bounded
     false-positive candidates via ADR-054 expansion — never a false negative, in any case.
  3. **Governance: a new `AliasProvenance::LearnedDistributional` that NEVER auto-activates.**
     One arm in `default_status_for` maps it to `Candidate` for **all** kinds — including
     variant-looking pairs that would auto-activate from any other source (the roadmap's "never
     auto-active", taken literally). `provenance_rank = 0` (least trusted, tied with
     `LearnedFromQueries`): reconciliation is then structurally safe with no other change — the
     same-rank promotion branch in `add_classified` requires the *computed* status to be
     `Active`, which never happens for this provenance, so re-discovery can only max confidence;
     `Rejected` short-circuits first (stickiness free). A later any-of re-learn MAY promote a
     distributionally-seeded variant under *its own* shipped policy — that is ADR-060's trust
     level acting on its own signal, not distributional auto-activation. Confidence = the cosine
     similarity (finite-guarded), review-sort metadata only.
  4. **A metadata-only vocab install seam** (`Engine::install_vocab_metadata_only`,
     `pub(crate)` deliberately — external callers go through `set_vocab`). The shipped
     apply path (`set_vocab`) unconditionally bumps `vocab_epoch` and recompiles the corpus —
     O(corpus) for a change that, here, activates *nothing*. The seam's fast path structurally
     verifies (never trusts) BOTH: everything **outside the alias registry is byte-identical**,
     compared over the serialized vocab documents with the registries blanked — so
     synonyms/phrases/graders/punctuation/number-context/declared equivalences AND any future
     `Vocab` field automatically participate (a field-list compare would silently rot — codex
     review); and the registry's matching-relevant projections
     (`effective_equivalence_groups` + `active_alias_forms`) are equal. Only then does it swap
     the Arc with no epoch bump, no normalizer rebuild, no recompile — the advertised vocab can
     never desync from the live normalizer. Durability follows the EXISTING
     single-node vocab semantics: the engine manifest carries **no** vocab blob (only the
     `ClusterManifest` does, ADR-046), so — like `PUT /_vocab`, `learn_and_apply`, and the alias
     import before it — a recorded registry rides the vocab *document*: `GET /_vocab` → the
     operator's vocab file → `open_with_vocab` on restart (oracle-proven). If the projections
     differ (unreachable from the candidate-only paths; belt-and-braces), the seam falls back to
     the full `set_vocab` + recompile — the fast path can never cause an FN even if a future
     registry change breaks the invariant.
  5. **Surface.** Single-node REST: `POST /_vocab/aliases/discover` (compute-only; returns
     proposals + evidence; engine-sourced unless a `queries` body is supplied) and
     `POST /_vocab/aliases/discover_and_record` (files candidates in the registry via the
     metadata-only seam; response reports `recompiled: 0` deliberately). Cluster mode: `discover`
     with an explicit `queries` body only (engine-sourced gather is a cross-shard op with no
     seam; 400 with a hint), `discover_and_record` = 501-with-alternative (a cluster vocab
     install is a full blue/green rebuild — ADR-074/076 — grossly disproportionate for
     candidates; review on a single-node replica, then install reviewed entries via the existing
     vocab path). `bin/learn.rs` prints the top pairs after its NPMI tables.

- **Safety.** Discovery is compute-only; recording changes no matching-relevant state (the
  differential no-op oracle proves match results byte-identical before/after
  `discover_and_record`); activation stays an explicit operator act through the proven ADR-060
  path. Serde: the new enum variant is one-directional (old binaries cannot read a vocab JSON
  containing it — the repo's stated format-forward stance; remote vocab shipping is a named v1
  non-goal, so no cross-version path exists). Default byte-identical: nothing changes until an
  operator both invokes discovery *and* activates a candidate.

- **Alternatives considered.**
  - **Embedding-based similarity** (word2vec/fastText or an external model) — rejected: a new
    dependency or an external tool + non-determinism, against the lean std-only stance, for a
    signal that still lands in the same review queue.
  - **Reusing `LearnedFromQueries` + a force-candidate flag** on `add_classified` — rejected:
    forks the reconciliation logic (which branch wins on re-import?); activation policy belongs
    in `default_status_for`, keyed by provenance.
  - **Auto-activating high-similarity `SingleTokenVariant` pairs** — rejected: the roadmap says
    never; the whole value of the provenance is that a reviewer sees distributional guesses.
  - **Title-corpus contexts** — declined for v1: listing style conflates with query intent;
    stored queries are the domain-native context (and the explicit-`queries` body leaves the
    door open).

- **Proven.** Unit: planted substitute discovered / co-hyponym suppressed by the co-occurrence
  penalty / numeric excluded (and opted back in) / negated clauses contribute nothing /
  phrase-glue discovers the token-vs-multi-word pair space-joined / two runs byte-identical +
  capped best-first / a uniform corpus (PMI ≡ 0) proposes nothing; governance: never
  auto-activates (incl. a variant-looking pair), `Rejected` stickiness on re-discovery, a
  re-discovery only maxes confidence, a later declared import still upgrades, JSON round-trip
  with the new provenance. Oracle (`tests/oracle/alias_discovery.rs`, over a generated corpus
  salted with a planted substitute + a planted co-listed alternative): `discover_and_record`
  changes NO match result on any title AND the vocab epoch does not move (the recompile-skip
  soundness proof); operator activation of the discovered pair makes the cross-form match appear
  with every prior match preserved (widening-only); the quality split (substitute proposed,
  co-listed suppressed); candidates ride the vocab document across a durable reopen
  (`open_with_vocab` — the operator's actual persistence path). Handler tests cover both
  endpoints + the explicit-corpus refusal on the record path. The seam guard has its own unit:
  a candidate-only change takes the fast path (no epoch bump) while a synonym added with
  identical alias projections falls back to the full `set_vocab` path (epoch bumped).

- **Deferred follow-ons.** Match-feedback validation of these candidates (ADR-103 — the sibling
  item); a cluster-side gather (needs a cross-shard sources RPC); title-corpus contexts as an
  additional signal.
