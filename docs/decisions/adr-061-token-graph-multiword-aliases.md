# ADR-061: Token-graph multi-word aliases — Phase 2 (positive / negative title feature views)

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted

- **Context.** [ADR-060](adr-060-learned-alias-evolution.md) (Phase 1) shipped the alias *governance*
  layer — the `AliasRegistry` records, classifies, and persists alias groups — but only **single-token**
  groups auto-activate and reach the matcher (through the unchanged ADR-054 equivalence expansion). A
  **multi-word** alias (`ny ≡ new york`, `nyc ≡ new york city`) classifies as `AliasKind::MultiWord` and
  is parked as a *candidate*: `AliasEntry::is_active_for_matching` and `AliasRegistry::activate` both
  structurally refuse it, "so review can never activate something the matcher would silently ignore."
  This ADR builds the matcher half that activates those candidates. It is a **matching-model** change,
  designed up front (with its oracle) per the process lesson from the abandoned first attempt
  ([`research/multiword-synonyms.md`](../research/multiword-synonyms.md)).

- **The wall (why the flat-set first attempt failed).** A title `T` emits **one** flat feature set,
  consumed by `exact::verify` for **both** the positive checks (required-mask / required-tail / any-of)
  and the negative check (forbidden-mask / forbidden-tail) — the forbidden tail binary-searches the
  *same* `tfeats` slice the required tail does (`exact/store.rs`, clauses 2 and 3). But the two polarities
  want **different** sets:
  - **Positive matching wants the overlapping superset.** With nested aliases (`new york` ⊂
    `new york city`), a title `new york city` must still satisfy a `new york` query — so the title's
    *positive* view must contain **every** overlapping alias entity, not just the leftmost-longest one.
  - **Negative matching wants the canonical, non-overlapping set.** A query `foo -"new york"` must still
    match `foo new york city` (recall-first: a forbidden clause must not over-reject). So the title's
    *forbidden* view must be the **leftmost-longest** parse, which reads `new york city` as one entity and
    does **not** contain the hidden `new york`.

  A single feature set cannot be both. The first attempt rescued positive retrieval with a title-side
  overlap superset, then that superset tripped the forbidden check (`foo -"new york"` wrongly rejected
  `foo new york city`) — a false negative in the most sacred area. That is not patchable inside one set.

- **Decision — two title-side feature views.** Split the title's feature set into two, computed once per
  title and threaded through verification:
  - **`P(T)` — positive view (the maximal parse-union).** Every token feature *plus* every
    overlapping phrase entity — computed as a **force-additive** emit (all phrases additive, nothing
    consumed ⇒ every token reaches phase 2b and every leftmost-longest entity is emitted) **∪** an
    **overlapping** (`MatchKind::Standard`) entity pass over **all** phrases. This is a strict
    superset of *every* parse, so it never drops a feature a different parse would emit — including
    the **components of a phrase displaced** from the leftmost-longest parse by an overlapping one
    (e.g. a collapsing `new york` consuming the `york` of an alias `york city`). Used for: signature
    **retrieval**, the required-mask gate, the required tail, and any-of groups. (`N(T) ⊆ P(T)`: the
    positive view only ever **adds**, so it can introduce a bounded false positive, never a negative.)
  - **`N(T)` — negative view (canonical leftmost-longest).** The ordinary leftmost-longest additive
    feature set the engine already produces. Used for: the forbidden-mask gate and the forbidden tail —
    **and nothing else**.

  `verify(id, tmask_pos, tfeats_pos, tmask_neg, tfeats_neg, pred)`: clauses 1-pos/2/4 read the positive
  pair; clauses 1-neg/3 read the negative pair. The columnar broad-lane transpose (`exact::eval_batch` /
  `eval_batch_slices`) gets the same split — a positive per-feature title bitmap for required/any-of and a
  negative one for the forbidden AND-NOT. **When no multi-word alias is active, `P(T) == N(T)`** and both
  pairs are the same slices ⇒ the verifier is byte-for-byte the pre-ADR path.

- **The query/title asymmetry (the ES `synonym_graph` model, realized in RR).** Multi-word aliases need
  the index (title) to keep component tokens while the query collapses to the entity. RR already has the
  two emit families; the asymmetry rides them:
  - **Query / compile side** (`compile_features`, `compile_features_readonly`): an active alias phrase
    **collapses** to its single entity feature (components consumed). So a stored query `new york`
    compiles to required `{term:new_york}`, which ADR-054 expansion widens to any-of
    `{term:new_york, ny}`. Collapse (not additive) on the query side is **required** — keeping `new`/`york`
    required would make a `new york` query miss a `ny` title (the one-way-alias failure).
  - **Title / match side** (`match_features`): the same alias phrase is **additive** (entity *and*
    components), so a pre-existing component query (`york`) still matches; the overlap pass then adds the
    nested entities for `P(T)`.

  This maps onto the existing function boundary exactly — `compile_features*` are the query side,
  `match_features` is the title side — so the asymmetry needs a `Side` discriminant on `emit`, not a new
  flag at every call.

- **The wiring is small because the equivalence machinery already supports it.**
  `Vocab::resolve_equivalences` resolves each alias form to features through the read-only compile path
  and **keeps a form only if it resolves to exactly one feature** (`fs.len() == 1`); a multi-word form
  resolves to many features today and is silently dropped. Register the active multi-word form as a
  **collapse phrase** in the normalizer and `compile_features_readonly("new york")` returns
  `[term:new_york]` (len 1) — so the group `{ny, new york}` resolves to `{id(ny), id(term:new_york)}` and
  the **unchanged** `resolve_equivalences` / expansion path produces the bidirectional any-of. Concretely:
  - `AliasRegistry`: a new `active_multiword_groups()` (parallel to Phase 1's `active_groups`), and
    `activate` / `is_active_for_matching` accept `MultiWord` (the Phase-1 refusal is lifted now that the
    matcher can express it; `MixedKind` stays refused).
  - `Vocab::to_normalizer`: register each active multi-word alias form as an **alias-mode phrase**
    (`PhraseMode::Alias`) emitting the deterministic entity `term:<tokens.join("_")>` (the corpus `term:`
    convention, so an alias and a corpus phrase over the same tokens share one entity; alias mode wins the
    dedup so collapse-on-query is preserved).
  - `Vocab::effective_equivalence_groups`: already the union point — it now also contributes the
    multi-word groups, so `resolve_equivalences` + `intern_equivalence_forms` pick them up with no change.
  - **ID stability** reuses the Phase-1 fix verbatim: `intern_equivalence_forms` calls `compile_features`
    per form, which now interns the entity dense (the alias phrase is registered), so the
    synthetic→dense boundary cannot kill a multi-word alias either.

- **The forbidden policy (decided up front, recall-justified).** A title `T` *forbidden-contains* a
  phrase iff that phrase is an entity in `T`'s **leftmost-longest canonical parse** `N(T)`. Consequences,
  all on the recall-safe side (a wrong call here is a bounded false positive the exact/stage-two filters
  catch, never a false negative):
  - `foo -"new york"` **matches** `foo new york city` (the canonical parse reads `new york city`, so
    `term:new_york ∉ N(T)`).
  - Forbidden clauses **do not expand** through equivalences (unchanged from ADR-054): `foo -ny` does
    **not** reject `foo new york` (`ny ∉ N(T)`; only the literal token is forbidden). A title that
    literally contains a forbidden token (`foo -york` over `foo new york`) is still rejected — `york ∈
    N(T)` via the additive components.

- **Why it is FN-safe (the load-bearing proof).** Let the spec be: `Q` matches `T` iff every
  expansion-widened required feature and any-of group of `Q` is satisfied by `P(T)`, and no forbidden
  feature of `Q` is in `N(T)`. The engine computes the **same** `P(T)` and `N(T)` (not an
  approximation). A false negative needs the engine to drop a spec-match at one of two points:
  1. **Retrieval.** Signatures are built only from `Q`'s required + any-of (the lossless cover, unchanged)
     and probed from `P(T)`. `P(T) ⊇ N(T)` only **adds** features, so it generates a **superset** of
     signatures — retrieval can only widen, never miss. The cover contract holds with `P(T)` as the title
     feature set (the same set the verifier's positive clauses use, so they are self-consistent).
  2. **Verification.** Positive clauses test `Q`'s required/any-of against `P(T)`; a spec-match has them
     all in `P(T)`, so no positive clause false-rejects. The forbidden clause tests against `N(T)`; a
     spec-match has no forbidden feature in `N(T)`, so it does not false-reject. ∎

  The split is what makes the spec *realizable*: the old single set forced a choice between a positive-FN
  (if it used `N`) and a forbidden-FN (if it used `P`); two sets pay neither. False positives (the
  superset retrieving/passing more than a stricter reading would) remain allowed and cheap.

- **Hot-path budget / default byte-identical.** The second (overlapping) automaton and the dual view are
  built **only when ≥1 multi-word alias is active**; otherwise `match_features` stays single-view, the
  overlap automaton is `None`, `P(T) == N(T)`, and every lane is byte-identical to pre-ADR-061. When
  active, the extra cost is one Aho-Corasick pass over the (typically small) alias-phrase set plus a
  second mask/slice — paid only on titles that actually contain an overlapping alias phrase.

- **Scope / what's deferred.** **Single-node first** (like ADR-054 / ADR-059 / ADR-060). The cluster
  deferral is now **enforced, not silent**: cluster content routing derives a title's target shards from
  the canonical leftmost-longest view `N(T)` (the `route` primitive reuses `match_features`), so a nested
  alias entity that lives only in the positive superset `P(T)` would never probe the shard holding a
  query anchored on it — a false negative the shard-local two-view verifier cannot recover. Every cluster
  path therefore **refuses a multi-word-alias normalizer** via `Normalizer::has_multiword_aliases()`:
  `ClusterEngine::from_parts` — the ONE assembly seam every constructor routes through (`build` /
  `build_with_tags`, `open`, and the distributed `connect_remote` / `connect_replicated`) — is the central
  backstop; `build_with_tags` *also* checks **early**, before any durable shard ingest or manifest commit,
  so a `data_dir` build cannot leave a reopenable durable cluster compiled under the unsupported
  normalizer; and the in-place `set_vocab` swap (which does not reconstruct through `from_parts`) guards
  its own path. (Regression-guarded by `cluster_oracle::vocab_learning::{set_vocab_refuses_active_multiword_alias_on_cluster,
  build_refuses_a_multiword_alias_normalizer, durable_build_with_multiword_alias_leaves_no_recoverable_state}`.)
  Single-token cluster aliases (`N(T) == P(T)`) are unaffected and keep working. Cluster multi-word
  support — **P(T)-aware routing** + cross-process normalizer shipping — is the follow-on. Deferred with it: cluster registry governance, and the
  lower-precision multi-word discovery sources (distributional / match-feedback). Quoted-phrase *required*
  clauses and overlapping aliases inside a single query clause keep query-side leftmost-longest (the
  author wrote one reading); only the **title** side needs the overlap superset.

- **Alternatives.** (1) *One flat superset set for both polarities* — the first attempt; rejected (the
  wall: forbidden-FN). (2) *Collapse-only multi-word phrases* (consume components on the title side too) —
  rejected: a component-token query (`york`) loses the match (positive-FN). (3) *Additive-on-query* (keep
  components required on the query side) — rejected: one-way alias (`new york` query → `ny` title fails).
  (4) *A reserved feature-id range that the forbidden check skips* — rejected: forbidden clauses can
  legitimately name an alias entity (`-"new york"`), so the distinction is "did leftmost-longest pick it,"
  a per-title parse fact, not a fixed id range. (5) *Mutate the cluster's frozen dict to add entities* —
  rejected (perturbs the frozen-dict fingerprint handshake); single-node first instead.

- **Testing (oracle designed with the model).** A differential oracle whose brute force encodes the spec
  above — `P(T)` (overlap superset), `N(T)` (leftmost-longest), query-side collapse + ADR-054 expansion,
  forbidden-against-`N(T)` — and, **from day one**, includes the cases the flat set silently broke:
  **forbidden-feature queries over multi-word-alias titles**, **overlapping / nested aliases**
  (`new york` ⊂ `new york city`) as first-class, the **reverse direction** (multi-word query → single-token
  title), **component-token** queries over alias titles, and **dynamic-vocab** stability (activate an
  alias, then `PUT` a query, still matches). At scale, an FN-safety sweep vs the original-semantics oracle.
  Plus normalizer units (collapse-query / additive-title / overlap emit) and a registry unit (multi-word
  now activates).

- **Consequences.** Operators can activate multi-word aliases and have them match bidirectionally, with
  nested/overlapping phrases handled and forbidden clauses staying recall-correct, at zero false
  negatives. The default (no active multi-word alias) path is byte-identical. The matcher gains a second
  per-title feature view — a reusable seam for any future "positive superset vs canonical set" need.
