# ADR-060: Learned-alias evolution — Phase 1 (safe single-token activation)

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted

- **Context.** Real deployments register hundreds of equivalences (abbreviation → canonical, variant
  spellings, expansions like `auto ≡ {autograph, autographed, signature, signed}`) and want them to
  evolve live. The engine already ships the *mechanism* — equivalence **expansion** (required → any-of,
  structurally FN-safe; [ADR-054](adr-054-equivalence-expansion.md)), the any-of learner (ADR-015),
  corpus phrase induction (ADR-053), and the live `set_vocab` + `recompile_stale_segments` apply path.
  What was missing is the layer that **governs, persists, and safely activates** aliases. A first
  attempt (PR #37) coupled this to multi-word aliases and was abandoned after five review rounds on a
  *fundamental* conflict, now recorded in
  [`research/multiword-synonyms.md`](../research/multiword-synonyms.md): a title emits **one** flat
  feature set used for **both** required and forbidden checks, but the overlapping superset of phrase
  entities needed for multi-word *retrieval* is unsafe for *negation* (`foo -"new york"` would wrongly
  reject `foo new york city`). The takeaway: **single-token aliases are a vocabulary feature; multi-word
  aliases are a matching-model feature.** This ADR ships the vocabulary half (Phase 1); the matcher half
  is Phase 2 (a separate ADR when it lands).

- **The embedded real bug (fixed here).** Independent of multi-word, equivalences were resolved
  **once** at apply time, and a form not yet interned resolved to a deterministic **synthetic** id
  (read-only `get_or_synthetic`, ADR-046). A *later* live `PUT /_doc` interns that same form as a
  **dense** id via the mutating compile path, but the `EquivMap` (keyed by `FeatureId`) was never
  re-resolved — so an alias installed on a fresh single-node index **silently went inactive** for
  queries added after the table loaded: a false negative, the cardinal sin. (The cluster is immune: it
  shares ONE frozen dict and resolves both the table and every incremental add read-only against it, so
  they cannot disagree.)

- **Decision.** Add a first-class **`AliasRegistry`** (`src/vocab/alias.rs`) as the **governance layer
  over ADR-054 expansion** — no matcher change.
  - **`AliasEntry { forms, provenance, kind, status, confidence }`**, serialized inside `Vocab` (so it
    survives reopen and rides `PUT /_vocab` for free). `provenance` ∈ {declared-file, learned, manual};
    `status` ∈ {candidate, active, rejected}; `confidence` is review-prioritization metadata only.
  - **Structural classifier** (`alias/classify.rs`): `SingleTokenVariant` (all forms single-token and
    every pair shares a ≥3-char common prefix — plurals / truncations / hyphenation folds),
    `SingleTokenDistinct` (single-token but not all-variant — graders `(psa, bgs, sgc)`), `MultiWord`
    (any multi-token form — Phase 2), `MixedKind` (forms span >1 known `FeatureKind`). It is purely
    structural — it never judges semantics; that judgement is exactly what Phase 1 defers to a reviewer.
  - **Conservative auto-activation policy.** `SingleTokenVariant` → active from any source. A declared /
    manual `SingleTokenDistinct` → active (operator intent). A **learned** `SingleTokenDistinct` (the
    `(psa, bgs, sgc)` case — an any-of is a *disjunction*, not an equivalence assertion), `MultiWord`,
    and `MixedKind` → **candidate, never silently active**. `Rejected` is sticky so a re-learn cannot
    resurrect it; `activate()` refuses a multi-word / mixed-kind group so review can't enable something
    the matcher would ignore.
  - **Sources.** *Learn* from query any-of groups at the **group level** (`learn_anyof_groups` keeps
    `(psa,bgs,sgc)` as ONE 3-form group, so it classifies as a category alternative, not three variant
    pairs). *Import* Solr/Lucene synonym files (`alias/solr.rs`: comma lists + `a,b => c,d` mappings
    unioned into one **bidirectional** group — RR equivalences are bidirectional, a recall-safe
    over-approximation; `#` comments, `\,` escapes).
  - **Active groups feed matching** via `Vocab::effective_equivalence_groups()` = directly-declared
    `equivalences` (ADR-054) **∪** the registry's active single-token groups. `resolve_equivalences`
    reads this union; candidates contribute nothing.
  - **The ID-stability fix.** `Vocab::intern_equivalence_forms` interns every effective form into the
    **mutable** single-node dict *before* resolving, forcing the same interning a future insert would
    do — so resolve-time and insert-time agree on a dense id. Called from `Engine::{with_vocab,
    set_vocab, adopt_vocab}`. A no-op without equivalences (byte-identical); never touches the cluster's
    frozen dict (it is provably immune).
  - **Live apply + ops.** `Engine::{import_alias_synonyms, learn_aliases_and_apply}` reuse the existing
    `set_vocab` + `recompile_stale_segments` path (no restart, no full rebuild) and return an
    `AliasApplyReport { activated, recompiled, summary }`. REST: `GET /_vocab/aliases` (review),
    `POST /_vocab/aliases/import`, `POST /_vocab/aliases/learn_and_apply`.

- **Why it is FN-safe (the load-bearing property).** Active aliases apply through the unchanged ADR-054
  expansion, which only **widens** a query's accepted positive set — its match set can only grow, so it
  cannot introduce a false negative; a wrong activation degrades to a bounded false positive (the
  tolerable failure mode for a recall-first candidate generator). Candidates affect nothing. The
  ID-stability fix closes the one residual FN path (an alias dying across the synthetic/dense boundary).
  Proven by `tests/oracle/alias.rs::alias_ids_are_stable_after_future_insert` (fails on the pre-fix
  code) and `::alias_registry_application_is_fn_safe_at_scale` (zero FN vs the original-semantics oracle
  over a generated corpus).

- **Scope / what's deferred.** **Single-node first** (like ADR-054; `set_vocab` already refuses a
  non-local or tagged cluster). Deferred: **Phase 2** multi-word aliases (the token-graph matcher
  feature the registry already records as candidates); **cluster** registry governance; the
  lower-precision **alias-discovery sources** (distributional, match-feedback — ADR-054's deferred
  seam); and **richer variant signals** (subsequence abbreviations, bounded edit distance) that can only
  *widen* the auto-active set. The deliberately narrow ≥3-char-prefix variant rule errs toward
  *candidate*, so a recall-first deployment never silently merges two distinct tokens.

- **Alternatives.** (1) *Re-land PR #37's approach* — rejected: it coupled the safe single-token work to
  the unsafe flat-feature-set multi-word model. (2) *Auto-activate every learned single-token group* —
  rejected: a learned `(psa, bgs)` any-of is a disjunction, not an equivalence; activating it silently
  bridges distinct entities. (3) *Reclassify on every load* — rejected: classification needs a
  normalizer + dict; storing `kind` keeps `GET /_vocab` cheap and review stable. (4) *Mutate the cluster
  dict for symmetry* — rejected as unnecessary (the cluster is immune) and risky (it would perturb the
  frozen-dict fingerprint handshake).

- **Testing.** `vocab/alias/tests.rs` units (classifier, Solr parse, registry reconciliation /
  reject-stickiness / activate-refuses-multiword / JSON round-trip); the five named oracle tests in
  `tests/oracle/alias.rs` (`learns_single_token_alias_from_anyof_group`,
  `does_not_auto_activate_category_alternatives`, `alias_ids_are_stable_after_future_insert`,
  `vocab_apply_recompiles_existing_queries_without_restart`,
  `multiword_alias_candidate_is_recorded_but_not_activated`) + the at-scale FN-safety proof.

- **Consequences.** Operators can now register, learn, review, and live-apply single-token aliases with
  governance (provenance / confidence / status) and zero false negatives, building on the ADR-054
  primitive. Multi-word aliases are captured as reviewable candidates, ready for the Phase-2 matcher.
  The default path (empty registry) is byte-identical to before this ADR.
