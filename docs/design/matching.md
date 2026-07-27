# Matching — signature optimizer, candidate index, exact matcher, broad lane, metadata & ranking, explain

*Scope: the heart of the engine — how compiled queries are gated and verified. Covers the
signature-cover optimizer, the candidate index, the integer-only exact matcher, broad-query cost
classes, and explain tooling. Siblings:
[`normalization.md`](normalization.md) (where features come from),
[`ingestion-and-updates.md`](ingestion-and-updates.md) (how this data is stored/updated),
[`clustering-and-scaling.md`](clustering-and-scaling.md). See the [overview](README.md) for the
correctness contract this section must uphold.*

> **Implementation status:** Signature optimizer, candidate index, exact matcher, broad-lane cost
> classes (A/B/C/D/H), and explain tooling are implemented and tested. The broad/hot lanes'
> **batch / columnar evaluation** (§4) is now implemented too — once-per-batch scans + bitmap-algebra
> verification + a pure-anchor skip-verify fast path, exposed as `match_titles_batch` / `POST
> /_mpercolate` (ADR-026). Near-duplicate queries are
> clustered *implicitly* — they share signature anchors in the candidate index, so a single failed
> anchor probe drops the whole cluster's candidates. An explicit query-family / shared-prefix-DAG
> structure (subtree pruning) was evaluated and deliberately **not** pursued; see
> [DECISIONS](../DECISIONS.md) ADR-019 for the reasoning. **Per-query metadata, filtered percolation,
> ranking, and pagination (§5) are built end-to-end in standalone and cluster modes**
> ([DECISIONS](../DECISIONS.md) ADR-049/055/059/075/107/108/110).

**TL;DR (for agents)**
- **Owns:** signature optimizer (`compile.rs`), candidate index (`index.rs`), exact matcher (`exact.rs`), explain (`explain.rs`)
- **Key invariant:** Signatures built ONLY from required features / any-of groups, never from forbidden features (lossless cover contract)
- **Hot path:** title signatures → probe index → union candidate IDs → common-mask gate (2× `u64` ops) → sorted-slice verification → emit matches
- **Placement classes:** A/B (default-visible main) / C (opt-in broad) / D (rejected by default; opt-in universal) / H (default-visible hot tier)
- **Measurements:** the current pinned captures and regression invariants live only in [performance/results.md](../performance/results.md)
- **Gotchas:** Adaptive in-memory postings (inline ≤8 → `Vec` ≤256 → Roaring >256); C/D visibility is request-controlled, H visibility is not

---

## 1. Signature-cover optimizer (the heart of the compiler)

A **signature** is an arity-1 or arity-2 group of positive `FeatureId`s hashed to a `u64` key.
`anchor_plan` is the single source of truth for choosing the lossless cover and its class;
`build_signatures` only hashes that plan. The implementation uses deterministic frequency rules, not
a learned weighted score:

1. If the query has ordinary required features, sort them by query frequency. A non-top-64 rarest
   feature becomes one arity-1 anchor (class A), unless an enabled θ threshold moves that
   default-visible work to H. If the rarest feature is in the frozen top-64 mask, pair it with the
   next-rarest required feature (class B). A lone top-64 required feature becomes class C.
2. If there is no ordinary required feature, select the any-of proxy group whose **most frequent
   member** is least frequent. Emit one arity-1 anchor for every member, so every satisfying branch
   reaches the query. The worst member determines B, C, or H. Complete multi-feature members remain
   in the exact predicate program; their proxy is necessary, never sufficient (ADR-119).
3. A required phrase may supply a default-visible family of arity-1 candidate-only label proxies
   (class B). Exact positioned graph matching still decides whether the phrase is present (ADR-120).
4. A query with no positive requirement receives the empty universal broad signature (class D).
   Ingest rejects D by default; `accept_class_d` permits it deliberately.

The frequency inputs are query-document frequencies in the shared dictionary. The top-64 mask is
frozen after the initial compile pass; θ is an independent runtime configuration boundary that may
move only default-visible A/B work to H. Runtime title-hit or candidate-survival feedback does **not**
currently choose covers. Re-anchoring during compaction reruns these same rules with strict
visibility guards.

---

## 2. Candidate index (segment-local)

```
signature_key (u64)  →  posting list of SegmentLocalQueryId (u32)
```

The mutable in-memory index is a fast `u64 → Posting` hash map. Postings are append-only in increasing
local-ID order and adapt at exact cardinality boundaries:

| Cardinality | Representation | Rationale |
|---|---|---|
| 0–8 | inline tiny array in the bucket header | no heap, no pointer chase |
| 9–256 | `Vec<u32>` | compact sequential iteration |
| >256 | `RoaringBitmap` | compressed sorted set and allocation-free iteration |

Sealed mmap segments serialize postings into frozen index tables and posting blobs; that on-disk
reader is a distinct representation, not the mutable map above. Main, broad, and hot lanes each use
the same logical `signature → local IDs` contract.

**Segment-local IDs.** Only `u32` `SegmentLocalQueryId` rides the hot path. The `u64`
logical ID and per-row version metadata are read only after a candidate passes verification. This
keeps candidate postings and most verifier columns compact.

**Probing.** For a title the matcher emits the arity-1 keys needed by the active lanes and the
top-64-keyed arity-2 pairs used by class B, probes each relevant per-segment index, and deduplicates
local IDs in reusable scratch. The operation is a union, not an intersection, because any member of a
lossless cover is sufficient to retrieve the query.

---

## 3. Exact match plan (integer-only verification)

Per segment, the exact-match data is **struct-of-arrays**, indexed by `SegmentLocalQueryId`:

```
// parallel arrays, one entry per query in the segment
required_common_mask:   [u64]     // bitmask over the ~64 hottest global features
forbidden_common_mask:  [u64]     // ditto, for negatives
required_off:  [u32]   required_len:  [u16]   // slice into required_blob
forbidden_off: [u32]   forbidden_len: [u16]   // slice into forbidden_blob
required_blob:  [u32]   // remaining required feature IDs, sorted, beyond the common mask
forbidden_blob: [u32]   // remaining forbidden feature IDs, sorted
anyof_meta_off: [u32]   anyof_groups: [...]   // packed (offset,len) any-of groups
predicate_off: [u32]   predicate_len: [u32]   // optional compound program per query
predicate_blob: [u32]                         // OR-of-AND members + quoted token graphs
version: [u32]   logical_id: [u64]            // resolved only on match
```

Verification of one candidate against a title's feature set `F` (also reduced to a `common_mask` + a
sorted tail):

1. **Common-mask gate (1–2 instructions):** `(req_mask & F.mask) == req_mask` and
   `(forb_mask & F.mask) == 0`. The ~64 hottest features (grades, top graders, common card terms)
   live here, so the overwhelming majority of rejects happen in a couple of AND/compare ops with no
   memory traffic beyond the candidate's two `u64`s.
2. **Required tail:** every ID in `required_blob[off..off+len]` must be present in `F.tail`
   (merge/galloping over two sorted slices).
3. **Forbidden tail:** no ID in `forbidden_blob[..]` present in `F` → reject if any is.
4. **Any-of proxy groups:** each group needs ≥1 member proxy present. This is the complete exact
   predicate for ordinary single-feature groups and a necessary precondition for compound groups.
5. **Compound members:** for each positive group, at least one complete member must satisfy all of
   its requirements against `P(T)`; reject if any complete member of a negated group is present in
   `N(T)`.
6. **Quoted clauses:** every required phrase graph must occur as a connected analyzed path in
   `P(T)`; reject when a forbidden phrase graph occurs in canonical `N(T)` (ADR-120).
7. Survivors → resolve `logical_id`/`version`, emit.

No strings, no regex, no virtual dispatch, no allocation. Ordinary predicates stay entirely in the
SoA mask+slice form. A query with a multi-feature any-of member or quoted clause carries a compact,
structurally validated `u32` subprogram. Program v1 contains ADR-119's nested Boolean shape and is
evaluated in both scalar and bitmap form. Program v2 appends ADR-120 required/forbidden token graphs;
the scalar verifier intersects integer-labeled graph paths with reusable scratch. While any phrase
row is live in a snapshot, the batch driver uses that positioned scalar path for broad/hot work rather
than silently flattening the graph into its bitmap kernel. The public low-level positionless
`ExactStore::eval_batch` / `eval_batch_slices` surfaces reject a v2 row with
`BatchEvalError::PositionedPredicate` and clear the output bitmap, so bypassing the driver cannot
silently skip adjacency. Phrase-free snapshots retain the old path.

**Two title feature views — multi-word aliases (ADR-061).** The steps above describe one title feature
set `F`. With a multi-word alias active, the verifier instead receives a **`TitleView`** carrying *two*
views (built once per title by `Normalizer::match_features_dual`): a **positive** overlapping superset
`P(T)` and a **negative** canonical leftmost-longest set `N(T) ⊆ P(T)`. The required-mask gate, required
tail, any-of proxies, and positive compound members (steps 1-required, 2, 4, 5-positive) read `P(T)` —
so a `new york` query finds a `new york city` title via the nested entity the overlap pass adds. The
forbidden-mask gate, forbidden tail, and negative compound members (steps 1-forbidden, 3, 5-negative)
read **only** `N(T)` — so `foo -"new york"` still matches `foo new york city` (the
canonical parse reads `new york city`, which does not forbidden-contain `new york`). This split is what
lets one verifier serve both polarities without a false negative; the single flat set could not (the
superset needed for retrieval over-rejects negation — the wall the first attempt hit). With no active
multi-word alias `P(T) == N(T)` and the verifier is byte-identical to the single-view path. Full design,
including the FN-safety proof and the query-side collapse / title-side overlap asymmetry: [DECISIONS](../DECISIONS.md)
ADR-061.

**Quoted token graphs (ADR-120).** A flat `P(T)`/`N(T)` set cannot distinguish `red shoe` from
`red leather shoe`, so phrase-bearing snapshots also build positioned positive/canonical edge lists
with `Normalizer::match_phrase_views`. Ordinary tokens span `i → i+1`; analyzer entities may span
several positions, and every overlapping declared entity contributes an alternate positive path
even when no alias is active. Required query-graph
labels are equivalence-widened and checked against `P(T)`; forbidden labels remain canonical and are
checked against `N(T)`. Every required graph label enters a **candidate-only** proxy family: every
satisfying path has a labeled edge, but the proxy never enters the flat exact any-of columns and
exact connected-path intersection decides truth. These graph-only proxies are probed solely as
arity-1 main-lane signatures; pair, hot, and broad probes remain keyed to flat `P(T)` so positioned
analysis cannot manufacture unrelated lane work. Proxy labels contribute query-document frequency
once per distinct label per query—even when several quoted/bare clauses share it—so clause repetition
cannot perturb the frozen top-64 visibility boundary. Phrase covers stay on the default-visible main
lane; every cluster cover that uses a phrase proxy is replicated rather than selectively placed by
graph-only labels. Graph work is capped at 65,536 visited position pairs and 65,536 charged arc
inspections, and positioned analysis bounds same-grader starts. Exhaustion or an incomplete bounded
graph fails open by polarity (required does not reject; forbidden does not trip), so the safety valve
can add an over-match but never a false negative. Explain mirrors the same bounded outcome. Each
segment maintains a live phrase-row count; the engine refreshes an aggregate after mutations and
captures it in each snapshot, keeping the phrase-capability decision O(1) per title.

---

## 4. Broad-query handling (cost classes × the two-axis placement model)

Every compiled query is classified by the selectivity of its **best achievable signature cover**.
Since ADR-105 the classification answers TWO independent questions — **who can see the query**
(visibility) and **how its work is scheduled** (evaluation):

| Class | Meaning | Visibility | Evaluation |
|---|---|---|---|
| **A** | highly selective (rare arity-1 anchor) | default-visible (every request) | main index, realtime per-title |
| **B** | acceptable selectivity (arity-2 pair / selective any-of) | default-visible | main index, realtime per-title |
| **C** | broad — only a **top-64** anchor available (`PSA 10`, `rookie`) | **opt-in** (`include_broad`) | broad lane, columnar batch |
| **D** | negation-only (only forbidden clauses) | opt-in, and rejected at ingest by default (`accept_class_d` stores it as an **always-candidate**, ADR-068) | broad lane, universal signature |
| **H** | **θ-hot anchor** (frequency ≥ `hot_anchor_threshold`, *no* top-64 mask bit — the ADR-104 rank-cliff population) | **default-visible** — probed on every request | **hot index**, columnar batch (per-title inline on the scalar path) |

### 4.1 The two-axis placement rule (ADR-105 — an architecture invariant)

> **Cost movement must never imply visibility movement.**

Visibility ∈ {default-visible, opt-in broad, rejected/explicit-universal} and evaluation
strategy ∈ {realtime anchor, columnar hot, columnar broad, universal} are separate axes; any
lever that moves a query for *cost* reasons must keep its visibility cell fixed. The hot tier
is the first non-trivial cell (default-visible × columnar): a θ-hot-anchored query leaves the
realtime lane's per-title scans but stays visible to every request — unlike class C, whose
opt-in visibility is part of the documented request semantics and whose boundary therefore
stays keyed to the **frozen top-64 mask**, never θ. This is enforced structurally in
`anchor_plan` (the C branch and the title-side pair loop never read θ) and pinned by the
visibility-invariance oracle (`tests/oracle/hot.rs`: θ-on ≡ θ-off byte-identically on both
`include_broad` modes). The same rule is why the ADR-056 compaction demote-guard refuses
{A,B,H}→C, and why a θ flip — config drift, WAL replay, a coordinator/shard mismatch — is
correctness-benign: it can only move queries between the two always-visible lanes (A↔H),
which also place identically in the cluster (`Target::Selective` on the same ring anchor).

A class-C query's best signature is still too common (posting would be "huge"). Putting it in the main
index would poison candidate selectivity for *every* title that has that feature. Instead the **broad
lane** (implemented in `segment/broad_batch.rs`, ADR-026):

- holds class-C queries indexed by their (few, coarse) features (the per-segment `broad` index);
- is evaluated with **batch / columnar** scans over a title batch (`match_titles_batch`), amortizing
  each huge posting's scan over the whole batch rather than re-scanning it per title in the hot path.
  Mechanics: a per-batch feature→title-bitmap inverted index, one probe per distinct broad anchor per
  batch, then per-query **bitmap-algebra verification** (`exact::eval_batch`, the bitwise transpose of
  `verify`) — broad postings scanned amortize ~1/`broad_batch_size` (29× at 256), ~2.4× end-to-end
  throughput over the inline path;
- runs a **pure-anchor fast path** — broad queries whose entire semantics is their hot anchor emit
  straight from the anchor's title bitmap with no verification (the streaming-safe analog of the
  design's "materialized/precomputed subscriptions"; literal periodic-refresh materialization doesn't
  map to streaming percolation, see ADR-026);
- is metered through dedicated broad `MatchStats` counters (and Prometheus on `/_mpercolate`) — the
  "higher cost class" intent. Class-C ingest rewrite suggestions ("add a year or set to make this
  realtime") remain a separate, not-yet-built feature.

The columnar path is **byte-identical** to the per-title broad path (`tests/broad_batch.rs` + the batch
oracle); a `broad_columnar=false` setting reverts to the inline per-title probe (the kill-switch). This
is the direct, structural fix for the percolator "unsupported query becomes an always-candidate"
failure mode: we *detect* low selectivity at compile time, quarantine it, and then evaluate it cheaply
in batch — instead of paying for it silently on every title. (Roaring-bitmap / SIMD posting
intersection for the very broadest postings is a further micro-optimization, not yet done.)

**The hot tier (class H, ADR-105)** rides the SAME columnar machinery, lane-parameterized
(`Lane::{Broad, Hot}` through the kernel), with three deliberate differences from class C: it is
probed on **every** request (the batch driver lifts it into the columnar pass even when
`include_broad=false`; the scalar path probes it inline per title, skip-when-empty — structurally
free on hot-free corpora); its vacuous accept is `pure_tail_anchor` (a θ-hot anchor has no mask
bit, so the single required feature lives in the required *tail* and `is_pure_anchor` is
structurally false for it); and the universal-signature probe stays broad-only (class D lives in
the broad index). The batch's **count-gate pre-reject** (`broad_prefilter`, lever 5a) serves both
lanes: a reached candidate whose required features / any-of proxy groups cannot all be satisfied by
ANY title in the batch skips full bitmap verification — a necessary-condition filter (compound
members may under-reject here and are checked by the full kernel; forbidden features are never
consulted).

**Class-D always-candidates (the opt-in lane, ADR-068).** With `accept_class_d` on, a negation-only
query is the *deliberate* version of that always-candidate: its lossless cover of an empty positive
set is the **universal signature** (`anchor_plan` returns one empty broad-anchor group, hashed to
`sig_key(&[])` = `util::universal_sig()`), stored in the same per-segment broad index. The title side
probes that one constant key per segment (scalar) or **once per batch** (columnar — the amortization
the lane rides this machinery for); reached entries always take full verification (`is_pure_anchor`
is structurally false for an empty required mask), where their forbidden features are enforced against
`N(T)` — the vacuous semantics "matches every title bearing none of my forbidden features", exactly
ES/OS's `fixNegativeQueryIfNeeded` match-all-except evaluated blindly per document. Because the cover
is optimizer-derived (not a side table), compaction re-anchoring, the vocab recompile, and explain all
reproduce it by construction. The probe is unconditional within the broad lane — the knob gates
*acceptance*, never *visibility* — so a stored entry stays matchable however the knob is later
toggled; with none stored it costs one bloom miss per sealed segment. Like class C, an
always-candidate is visible only when the request includes the broad lane.

### 4.2 Canonical-body dedup, Stage A (ADR-106)

Orthogonal to lane placement: queries whose **semantic bodies** are identical (masks +
required/forbidden tails + canonical any-of groups + canonical compound predicate program — never
identity: logical id, version, tags) share ONE posting entry per in-memory segment. At `add_compiled`
a body-hash hit
confirmed by exact equality joins the group — the duplicate inserts no postings and **adopts
the leader's class** (identical bodies can plan A vs H across a θ-crossing frequency bump;
adoption is lossless because A/B/H are all always-visible, and C/D are structural under the
frozen mask). Every match path — the scalar probe, the columnar kernel's vacuous-accept and
full-verification arms, the class-D universal probe — verifies the shared body **once** and
fans emission out per member, each gated on its OWN aliveness and tags (a dead leader never
drops alive members; grouped `eval_into` runs with the empty predicate so the leader's tags
cannot veto a member). Flush **expands** groups into plain postings (the `.seg` format is
untouched; mmap segments are always group-free and take the exact pre-dedup paths); both
compaction merges regroup on the destination side by body — which makes compaction the
**cross-segment** dedup mechanism. `dedup_bodies` (default on, dynamic) gates new grouping
only; the observe sketch (`bodies_total`/`dup_joined`/`distinct_bodies_est`) sizes Stage B —
the persisted indirection this stage deliberately defers.

### 4.3 One distributed emitter per logical match (ADR-109)

Cluster placement can put one logical query on several shard positions. Candidate retrieval and exact
verification still run wherever routing requires, but a post-verification `UniqueOwner` policy permits
only one routed position to emit the logical ID:

- selective A/B-any-of/H placement emits from the minimum position in
  `placement_positions ∩ routed_positions`;
- replicated-always-visible class-B pairs emit from the minimum routed position;
- replicated-broad class C/D emits from the request's broad-evaluation position.

The policy runs after exact positive/negative verification and after each member's own alive/tag
checks, immediately before the collector. Canonical-body sharing therefore still verifies once, but
each logical member independently decides whether it is alive, tag-eligible, and owned. Placement
metadata never participates in signature retrieval, exact semantics, visibility, or score.

Standalone matching monomorphizes the same code with `EmitAll`, preserving the prior hot path. Cluster
reads pass one generation-fenced ownership context to every routed shard; filtered and compatibility-
ranked paths use the identical context. The coordinator retains sort/dedup defensively, while
`duplicate_emissions` asserts the shard replies are already disjoint. See the placement/persistence
contract in [`clustering-and-scaling.md`](clustering-and-scaling.md) §7 and ADR-109.

Exhaustive delivery additionally requires `pending_repairs=0`. During an ADR-047 partial upsert, an
old body can remain live under its old placement while the replacement is already live under a new
placement; both positions can then be valid emitters for the same logical id. The bounded
coordinator cannot deduplicate that state without result-sized memory, so it refuses exact
completion until `resync` or a durable log replay restores one converged version. It also requires
an authoritative coordinator logical-id directory: a fresh coordinator reattached to populated
remote shards cannot prove that a prior process left no unrecorded partial apply, even though its
new `pending_repairs` map is empty. The same authority is revoked when an initial multi-shard bulk
ingest fails after an ambiguous subset of shard writes; that path predates the per-logical repair
journal, so its empty repair map is not a convergence attestation either. Both shapes refuse before
emitting and require fresh shard slots rebuilt from the authoritative corpus. The coordinator
rechecks convergence at every shard boundary so a newly queued repair fails an in-flight stream
closed. The full exhaustive fan-out also holds the exclusive mutation/PIT-open barrier (live writes
and `resync` hold the shared side), preventing a healthy successful re-placement from interleaving
between shard reads.

---

## 5. Per-query metadata, filtered percolation, and ranking

> **Status:** metadata, filtered percolation, compatibility ranking, bounded top-K ranking, pagination,
> and exhaustive bounded delivery are implemented in standalone and cluster paths. One frozen
> `TagDict` is shared into every shard; the coordinator resolves each filter/rank program once and
> fans integer IDs to the shards. The design is motivated by the reference workload in
> [`../research/percolator-workload.md`](../research/percolator-workload.md). Code:
> `src/tagdict.rs` (tag interning),
> `src/exact.rs` (`TagPredicate` + SoA tag column + verify-stage filter), `src/rank.rs` (the post-match
> scorer — ADR-059/108), `src/segment/` (ingest/match threading + `EngineSnapshot::{rank,
> try_match_title_top_k}`), `src/storage/segment.rs` + `src/wal.rs` (durable tag, priority, predicate,
> and ownership columns; current version matrix:
> [`rolling-upgrade.md`](../operations/rolling-upgrade.md)), `src/bin/server/`
> (the REST filter + rank/pagination surface), `src/cluster/` (`coordinator/{lifecycle,ingest,matching}` +
> `clog` + `shard` + the gated `remote`/`server` — ADR-055/109).

Production percolators store **structured tags** alongside each query (a category, a status, secondary
keys) and at match time **filter and optionally rank matches by those tags**. Reverse Rusty implements
that model without touching the lossless-cover contract: tags never participate in candidate gating.

### 5.1 Metadata model — interned integer tags in the SoA

A stored query may carry a small set of `key → value` tags. Each distinct `(key, value)` resolves to
an integer `TagId` at compile time (dense while the dictionary is mutable, deterministic synthetic
after it is frozen) — the same move used for `FeatureId`s, so **no strings reach the match path**. The per-query tags become one more
**column in the exact-match SoA** (`exact.rs`, §3): `tag_off: [u32]` / `tag_len: [u16]` into a sorted
`tag_blob: [u32]`, exactly parallel to the `required_blob` layout. Tags are written on insert / update /
bulk, persist in the `.seg` format, and survive reopen (see [`ingestion-and-updates.md`](ingestion-and-updates.md) §11).

### 5.2 Filtered percolation — push the filter into verification

A percolate request may carry a **tag predicate** — a conjunction of "key ∈ {values}" terms (e.g.
`category ∈ {A,B} AND status ∈ {X}`). Compile it once per request to required `TagId`s, then, **during
exact verification** of each retrieved candidate (§3), test the candidate's `tag_blob` against the
predicate — a sorted-slice / membership check that reuses the cursor already walking the
required/forbidden tails. Candidates failing the predicate are dropped before they reach the output: no
extra pass, no per-hit metadata lookup, allocation-free.

### 5.3 The load-bearing invariant — tags never gate (mirror MUST_NOT)

**Tags are checked only in the post-candidate verify stage — never in the signature optimizer.** This is
structurally the same rule as "forbidden features never gate" (ADR-006, §1 invariant): signatures stay
built **only** from required features + any-of groups, so the title→query **lossless-cover contract
([overview](README.md) §2) is untouched**. A tag filter only ever *removes* queries the caller did not
ask for; it cannot drop a query the caller *did* want, so it introduces **no false negative** within the
requested tag scope. An implementer must not "optimize" by letting a tag influence candidate retrieval —
that would couple a caller-supplied filter to the cover proof.

### 5.4 Ranking — an optional layer *over* the boolean-correct set

Matching stays boolean and complete; ranking is an **optional sort applied to the already-final result
set**, never a change to which queries match. A query may carry a numeric **priority** (the value of a
designated tag key, default `"priority"`, reusing §5.1) and/or the request may supply additive **boosts**
keyed on a `(tag key, value)`; `EngineSnapshot::rank` (`src/rank.rs`) scores each matched id as
`Σ boosts + priority` (**additive**, not strict `(boost, priority)` lexicographic — the simpler
ES-`function_score`-"sum" model; strict dominance is reachable by choosing boost magnitudes above the
priority range), and the handler orders by `(score desc, _id asc)` — a total order — then applies
`from`/`size` and emits `_score`. This also adds `from` to `/_mpercolate` and per-slot hit truncation to
multi-doc `/_search` (closing the ADR-052 #3 pagination tail). Because it runs after verification on a
`Vec<u64>`, it touches neither the candidate index nor the verifier — and it is **opt-in**, so with no
`rank` block the response is byte-identical to the pre-ranking engine. Tags are resolved to the **newest
live copy** of each id (memtable first, then base segments newest→oldest). **Cluster ranking is built too**
([ADR-075](../DECISIONS.md)): the coordinator compiles the `RankSpec` once against the shared frozen tag
space (the [ADR-055](../DECISIONS.md) compile-once-fan pattern), each probed shard scores its own matched
ids via the same `EngineSnapshot::rank`, and the merge dedups by id — copies of a logical are
version-identical across shards, so every shard reports the same score. One **compatibility-RankSpec**
boundary, pinned: a post-freeze (synthetic) `priority` tag scores 0 (that path reads the tag's value
string, which only an interned tag has); boosts fire for both (id-equality). ADR-108/110's strict typed
`rank_fields.priority` instead stores/reconstructs a signed integer row value, including after tag-dict
freeze. Ranking remains a presentation-surface concern, not a matching-core one.

Compatibility `GET|POST /_search` also keeps matching, ranking, and hit enrichment on one published
snapshot. The source store is structurally shared across cheap snapshots, so a concurrent same-id
replacement can advance its source generation after an older reader matched. Source fetch compares
that generation atomically under the store read guard with the exact row generation captured by the
snapshot. A mismatch fails the requested enrichment instead of pairing a newer query source or
explanation with an older Boolean result. Compatibility cluster requests that explicitly ask for
sources take the exclusive `ClusterReadView` side of the core mutation barrier through matching and
cloning the paged sources. That fence covers direct library mutations as well as REST writes; the
default source-free cluster path remains fully concurrent (ADR-126).

Full-result `POST /_mpercolate` uses the same strict native/ES-shaped percolate resolver and keeps
standalone matching, compatibility ranking, and source projection on one captured snapshot. A
shared source row that has advanced to a replacement generation therefore fails enrichment rather
than splicing newer query text onto an older match. Its `responses[]` envelope is
multi-search-familiar but deliberately native: one shared JSON option set, ordered fail-closed
slots, and no NDJSON or partial slot success. Only the standalone implementation drives the
ADR-026 columnar batch kernel and can emit its aggregate broad profile; the coordinator's
compatibility path fans out per-title matches and rejects `profile: true` explicitly (ADR-135).

**Bounded local + distributed ranking (ADR-107/108/110).** `ExactStore` also carries one fixed signed
`i64` priority column. `RankProgramSpec` compiles the priority field and tag boosts to an integer-only
`CompiledRankProgram`; addition saturates. `EngineSnapshot::try_match_title_top_k` connects the
post-verification `TopKCollector` directly to the scalar matcher and retains only
`O(K + total-threshold)` state. The scorer deliberately receives only `logical_id`: it then resolves
priority + tags from the newest live copy, so segment probe order or an older duplicate cannot change
rank. `MatchSink::on_match(logical_id)` stays unchanged, keeping all metadata work out of unranked and
compatibility collectors. Local `POST /v2/_search` exposes this path for one document with deterministic
`(score desc, logical_id asc)` winners and honest `eq`/`gte` totals. Source/explain enrichment is
winner-only and fail-closed.

ADR-110 generalizes the same snapshot path over the post-verify `EmissionPolicy`: standalone uses
`EmitAll`, while a cluster shard applies ADR-109 `UniqueOwner` **before** its `TopKCollector`. Each
routed position therefore returns at most K owned rows. The coordinator rejects overlap, malformed
ordering, or stale attestations, merges by the same total order, and truncates to K. The merge is exact:
if a global winner were below its owner's local K, that owner alone would contain K globally better
rows. Exact shard totals are summed; global `eq` is returned only when every shard is exact and the sum
does not cross the threshold, otherwise the result is the request threshold with relation `gte`.

Cluster `/v2/_search` then performs query-then-fetch: final winner IDs are grouped by owning logical
position and only their source is fetched. Missing source, placement-generation drift, a
malformed/failing stream, deadline expiry, or enrichment-cap overflow invalidates the whole response.
Explanations are compiled at the coordinator from fetched source under its authoritative normalizer and
dictionary; explanation objects never cross the shard wire. Any cluster request needing source for
`_source` or explanation holds a short `ClusterReadView` across bounded matching, winner fetch, and
assembly. It acquires that mutation fence before entering the coordinator Rayon pool, so direct or
REST same-ID writes cannot interleave the two phases and blocked fence acquisition cannot consume a
shared worker. Source-free ranked requests remain fully concurrent. Without a PIT this is a
current-view operation. Standalone and in-process cluster requests may pin matching, score, order,
and totals with the `/v2/_pit` cursor flow; source/explain enrichment is deliberately current-view
at the request fence and fails typed if the winner is no longer live. Remote/gRPC coordinator
assemblies reject PIT operations with `501 pit_unsupported`. ADR-075 compatibility cluster ranking
remains current-view and unchanged.

PIT creation validates its strict native/Elasticsearch/OpenSearch control aliases before pinning.
A successful local open reports one pinned logical shard; an in-process coordinator response reports
every position from the same all-or-nothing mutation-barrier fan and exposes the identical signed
token under both `id` and `pit_id` (ADR-129). These HTTP aliases do not change registry, snapshot,
placement, or cursor semantics.

PIT close first bounds the request at the HTTP body boundary, then authenticates the complete
bounded `id`/`pit_id` scalar-or-array request before touching the registry. It releases each live
pin and returns one ES/OpenSearch/native response superset. The local freed-context count is one
per live PIT; the coordinator count is the number of pinned logical primary positions, never the
physical replica count. Already-absent entries remain a successful goal state but are reported as
not closed, and the API exposes no cross-client delete-all operation (ADR-130).

`POST /v2/_mpercolate` applies the same bounded collector to every title through the columnar batch
kernel and returns request-ordered exact top-K slots under one aggregate heap admission, deadline,
and winner-source credit (ADR-112). Source-enriched cluster batches use the same short
`ClusterReadView` principle as scalar v2 search: the view spans the one-call-per-shard batch match
and deduplicated union winner fetch, so every delivered source belongs to the matched same-ID
version. The fence is acquired before entering Rayon; source-free batches remain concurrent
(ADR-128).

### 5.5 Exhaustive bounded delivery (ADR-114/131/132/133/134)

`result_mode=all` uses the same post-verification collector seam without materializing the full
answer. `ChunkCollector` retains one fixed-capacity `Vec<ExhaustiveMatch>`; a synchronous
`ChunkSink` accepts each provisional chunk, so bounded-channel or gRPC flow-control backpressure
stops the matching worker. Its default-infallible `check_cancelled` hook lets job/gRPC sinks
surface cancellation, deadline, disconnect, or a prior send failure even before a chunk exists;
the exhaustive collector polls it before title-normalization/deduper setup and at
probe/candidate boundaries, and threads the same hook through both legacy duplicate selection and
newest-live ranked-metadata scans. A successful pass returns an exact unique total, chunk count,
and order-independent checksum. The serving layer alone emits completion; any deadline, sink,
shard, ownership, or protocol failure leaves already-sent chunks provisional.
For the HTTP reference sink, enqueueing terminal bytes is not yet completion: the worker waits for
the single-consumer body to dequeue that record. Only then does job status expose the exact
summary; dropping the response while the completion is still queued invalidates it and fails the
job. Shard nodes apply an independent server-owned maximum to the caller's remaining stream
budget before acquiring admission, so a direct client cannot retain every blocking worker with an
arbitrarily distant deadline.

The creation HTTP boundary is strict and defaults the route-implied mode/sink instead of requiring
redundant native fields. It accepts native millisecond/partial controls and ES/OpenSearch time-value
and partial-result aliases in either body or query string, rejecting aliases, locations, unknowns,
nulls, and async retention/wait controls that cannot map truthfully. Client event identity is
optional; generated identity improves first-use ergonomics while a caller-supplied key retains the
idempotent retry contract. The 202 response preserves native job fields and adds familiar
`id`/running/partial/start-time projections. A route-local pre-deserialization cap bounds control
request memory independently of the server's bulk-ingest allowance (ADR-131). None of these HTTP
projections changes exact completion or stream ownership.

Retained status preserves the native lifecycle and exact-summary fields while adding familiar
identity, running/partial, timing, and structured-error projections. A strict
`wait_for_completion_timeout` query can wait on terminal publication up to the configured job
maximum; it does not claim the stream, and a record cannot become `completed` until the concurrent
single consumer dequeues completion. Status holds the accepted record across count-based pruning,
uses `no-store`, and rejects `keep_alive` because the registry has no client-selected time expiry
(ADR-132).

The stream route remains native because ES/OpenSearch retained async-search JSON has no equivalent
for provisional chunks committed by a terminal checksum. It accepts only a query-free GET, rejects
every other method with `Allow: GET` before claiming, and returns no-store NDJSON with one
newline-terminated object per frame. Unknown and duplicate claims use structured 404 and 409
errors. Dropping a claimed response retains ADR-114's failure-without-summary guarantee (ADR-134).

DELETE uses the same terminal distinction as familiar async-search cleanup. A running record
receives cooperative cancellation and stays pollable until terminal publication; a subsequent
DELETE atomically removes that terminal job from both retained indexes and releases its event id.
The response preserves the native status snapshot while adding `acknowledged`, `deleted`, and the
familiar `id` alias. Cancellation remains linearized by the exact-delivery terminal gate, and no
active worker or stream record is removed early (ADR-133).

The compatibility collector historically sorts/deduplicates its complete `Vec<u64>`, because
library callers can leave multiple live physical rows for one logical id. Exhaustive delivery
cannot retain that result-sized set. Its collector therefore receives `(source ordinal, local id)`
after exact verification and consults each segment's existing logical-id reverse index. It emits
only the deterministic first physical row that itself passes aliveness, visibility, tag,
ownership, and exact checks against the already-normalized title. The rule preserves the case
where an older body matches but a newer duplicate body does not, while keeping result memory
`O(chunk_size)`. This additional lookup/reverification is exhaustive-only; compatibility and
top-K collectors keep their prior callback shape.

Across shards, ADR-109 ownership makes output sets disjoint. `PercolateAll` streams bounded,
contiguous shard-local chunks followed by one summary attesting placement identity, counts,
checksum, and stats. The coordinator validates each part and rewrites sequences into one
contiguous job stream using at most one additional chunk. It never repairs overlap by
deduplicating: overlap means the ownership contract failed and the exact job fails closed. A
shard asked to include the broad lane must itself be the context's named broad evaluator; missing
or mismatched broad ownership is rejected before execution. A
nonzero partial-repair queue is refused, and the full sequential shard read holds the exclusive
coordinator mutation barrier so a successful concurrent re-placement cannot move between owners
mid-stream. `resync` and live shard mutations hold the shared side.

### 5.6 Alternatives

- **Post-match external filter** (return everything, look up each id's metadata afterward) — effectively
  what callers did before ADR-049. Rejected as the long-term design: it still verifies every match
  and needs an external metadata store; 5.2 is strictly better now that tags live in the SoA.
- **Tag-partitioned segment skip** — for the *dominant* single-key filter (the `category` tag), index or
  route queries by that tag so a filtered probe skips whole segments (composing with the entity-anchor
  sharding in [`clustering-and-scaling.md`](clustering-and-scaling.md)). A real optimization, but it must
  be **filter-driven and fail-open** (skip only when the request's filter proves a segment irrelevant;
  when unsure, probe) so it can never drop a wanted query. The full proposal and completion test live
  in [`Tag-aware segment skipping`](../roadmap.md#tag-aware-segment-skipping).

---

## 6. Explain / debug tooling (always available)

For any query: show parsed AST, compiled required/forbidden/any-of proxy groups, complete compound
member predicates, chosen signatures, and cost class. For any (title, query)
pair: show the title's extracted features, which signature(s) made the query a candidate (or why it
was never a candidate), and the exact-match pass/fail with the specific failing predicate (missing
required / present forbidden / unsatisfied any-of member). This is built in, not bolted on — it's the
same SoA data read in a verbose mode.
