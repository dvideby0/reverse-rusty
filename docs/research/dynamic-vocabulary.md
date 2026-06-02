# Dynamic vocabulary — absorbing new terms after the dict is frozen (research charter)

Charter for the **research spike** that picks how Reverse Rusty's clustered core absorbs vocabulary
that first appears in a *live write* — i.e. a query added after the shared feature dictionary has been
frozen. This is the headline **Cluster v1** correctness item ([`../STATUS.md`](../STATUS.md) Tier 0).
Per the project's "research first, implement second" ethos
([`../../CLAUDE.md`](../../CLAUDE.md)), this file is the **charter**: it states the problem, the
correctness bar, the prior art to study, and the decision criteria. The spike fills it in with the
survey + a recommendation, which is then recorded in **ADR-046** and built in the in-process core.

> **Status: open spike (charter only).** Nothing here is a decision yet. The candidate approaches in
> §4 are hypotheses to evaluate, not conclusions. Output → this doc completed + ADR-046 + the Tier-0
> implementation.

---

## 1. The problem

The cluster's correctness model rests on **one shared, frozen dictionary**: every term maps to a dense
integer `FeatureId`, and *all* shards plus the coordinator agree on that mapping. Globally-consistent
`FeatureId`s are what make the cross-shard signature cover **lossless** — a query's anchor and a title's
features are compared as integers, so if the integers disagree across shards, a real match can be
dropped (ADR-027; [`../design/clustering-and-scaling.md`](../design/clustering-and-scaling.md) §3).

Freezing the dict was the right simplification to get the cluster correct. The cost: a term that is
**not** in the frozen dict has no `FeatureId`. Today such a term is **silently dropped** during the
read-only compile that live writes use (`cluster/coordinator/ingest.rs` and `cluster/server.rs`), so:

- a required positive term vanishes → the query **broadens** (matches titles it should not); and
- an any-of group whose members are *all* unknown collapses → at worst an unsatisfiable group, a
  **false negative** (the dangerous case).

This is corruption presented as success — the write returns OK. Cluster v1 must not ship it.

**Title-side vs query-side asymmetry (already safe; keep it).** A *title* token absent from the dict is
dropped at match time — and that is correct: no query can require a term outside the shared dict, so a
dropped title token changes no match outcome. The defect is entirely on the **query / live-write** side.

**Two sub-problems, different difficulty (pin the boundary in the spike):**
1. **New single token** (e.g. a new brand token `vapormax`). The normalizer already emits it as a token;
   only a *`FeatureId` assignment* is missing. This is the tractable case.
2. **New multi-token alias / synonym rule** (e.g. `Upper Deck` ≡ `UD`, abbreviation expansions). This
   needs a change to the **normalizer** automaton itself (`default_vocab()` / `Vocab`), not just an id —
   i.e. normalizer/vocab *shipping*. Harder; may remain a documented v1 limitation.

---

## 2. The correctness bar

- **Zero false negatives is non-negotiable** — the project's hard guarantee. Whatever we pick must
  preserve it across shards and after live writes.
- **Bounded false-positive *candidates* are acceptable** — the exact matcher rejects them. **But note
  the sharp edge:** if two distinct terms are made to share a `FeatureId` (a hash collision), the exact
  matcher compares ids and *accepts* the non-match — a true emitted false positive, not just a
  candidate. So any id-sharing scheme must keep that rate provably small and argue it never becomes a
  false *negative*.
- **No coordination on the hot path.** Title matching is allocation-free integer work; a new-vocab
  mechanism may touch the (compile-time) write path but must not add coordination to match time.
- **In-process ≡ cross-process.** The `Shard` seam is meant to behave identically in-process and over
  gRPC. A mechanism that "just works" in-process but diverges across nodes re-introduces drift; prefer
  one mechanism that holds for both (even if cross-process lands in a later phase).
- **Shared-nothing.** No object store / external coordination service (ADR-033) — consistent with the
  rest of the cluster.

---

## 3. Prior art to study (the systems, with the specific mechanism + the question)

| System | Mechanism to study | The question for us |
|---|---|---|
| **Elasticsearch / OpenSearch** | **Global ordinals** — per-segment term→ordinal maps lazily "globalized" into a consistent cross-segment ID space on refresh; and **dynamic mapping** — a newly-seen field/term propagated through the master/cluster-state. | This is the closest analogue: a coordinator-published, consistent term→id space that grows. What does globalization cost, when does it happen, and does the cluster-state-propagation (consensus) model fit our control plane (ADR-037/038)? |
| **Vespa** | Real-time indexing (no segment-merge visibility lag) and the **distributor / ideal-state** model for placement; attribute/index dictionaries under live writes. *(Verify specifics in the spike.)* | How does a real-time-first engine keep term/attribute identity consistent under continuous writes **without** a stop-the-world rebuild? Is there an idea that beats both global-ordinals and hashing for our asymmetry? |
| **RocksDB** | Trained **ZSTD dictionary** compression — a dictionary shared across immutable SSTs, with a dictionary id / versioning. | The "shared dict across immutable files, version it" angle: how is a shared dictionary evolved without rewriting every file? Maps to our shared-dict-across-segments situation. |
| **Lucene** | Per-segment **FST term dictionaries** — term ids are segment-local; global ordinals bridge them. | Confirms the "segment-local id + a global bridge" pattern we already mirror (segment-local `u32` → global `u64`). Is the bridge cheap enough to do per-write? |
| **Feature hashing** (Vowpal Wabbit; scikit-learn `HashingVectorizer`) | Deterministic term→id via a fixed hash into a reserved id range — every node computes the **same** id with **no coordination**. | The no-consensus shortcut. We already hash with `util::fnv1a64` (stable across runs). Trade-off: collisions → bounded over-match (never a miss). Is the collision rate acceptable for a reserved-range size we can afford? |

---

## 4. Candidate approaches for *our* constraints (hypotheses, to be evaluated)

1. **Deterministic feature-hashing into a reserved `FeatureId` range.** Unknown terms map to
   `BASE + fnv1a64(term) mod R`, with `[BASE, BASE+R)` disjoint from the dense interned range. Every
   shard + the coordinator compute the *same* id independently → **no consensus, in-process ≡
   cross-process for free.** Cost: collisions among unknown terms (bounded over-match, never a false
   negative). Likely the strongest candidate for the *single-token* case; the spike must size `R`
   against an acceptable collision/false-positive rate and prove the zero-FN property.
2. **Coordinator-published id assignment** (the ES global-ordinals / dynamic-mapping analogue). A new
   term is assigned the next dense id by the control plane (ADR-037/038) and the assignment is committed
   + propagated before the query goes live. Exact (no collisions) but adds a coordination step on the
   new-vocab write path; must not block the hot path and must define the in-process degenerate case.
3. **Hybrid.** Hash-by-default for instant absorption (no coordination), with a background
   densify/promotion of frequently-seen hashed terms into the exact interned space (collision cleanup) —
   to be weighed only if §1 or §2 alone doesn't clear the bar.

The **normalizer/alias** sub-problem (§1.2) is orthogonal to id assignment and is a separate
(normalizer-shipping) decision; the spike states whether it's in v1 or deferred.

---

## 5. Decision criteria (how the spike picks)

1. **Preserves zero false negatives** across shards and after live writes (the gate).
2. **Hot-path budget** — no coordination/allocation added to match time.
3. **Coordination cost** — no-consensus strongly preferred (in-process ≡ cross-process parity; ADR-033
   shared-nothing).
4. **False-positive rate** — if id-sharing is used, a quantified, tunable, acceptable bound.
5. **Token vs alias boundary** — which of §1.1 / §1.2 lands in v1, stated honestly.
6. **Implementation surface** — fits the existing dict (`dict.rs`), the read-only compile
   (`compile.rs::extract_readonly`), and the `Shard` seam without re-opening the cover proof.

---

## 6. Output of the spike

- This doc, completed: the survey written up against §3, the recommendation chosen against §5.
- **ADR-046** recording the decision (the chosen approach + the zero-FN argument + the token/alias
  boundary).
- Implementation in the in-process core + the absorb-correctly tests (zero FN, bounded FP, incl. the
  all-unknown any-of group), wired into the named Cluster-v1 acceptance gate ([`../STATUS.md`](../STATUS.md)
  Tier 0). Cross-process phasing, if any, noted there and in [`clustering-prior-art.md`](clustering-prior-art.md).
