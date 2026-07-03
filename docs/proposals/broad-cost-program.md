# PROPOSAL — The Broad-Query Cost Program

> **Status: DRAFT FOR MAINTAINER REVIEW — not on the roadmap, nothing here is decided.** On
> approval, the accepted levers become roadmap Tier items (each shipping under its own ADR) and
> this document graduates into the design docs. Evidence base:
> [ADR-104](../decisions/adr-104-cluster-scale-soak.md) (the 20M scale soak that measured the
> problem) and [`research/broad-scaling-prior-art.md`](../research/broad-scaling-prior-art.md)
> (the four-thread prior-art survey behind every claim of the form "the field does X").

---

## 1. Executive summary

The 20M scale soak measured two distinct phenomena that look like one "broad scaling issue":

1. **An inherent property (not fixable, by design):** with the broad lane enabled, matches/title
   grows with corpus size because broad queries *genuinely match* that much — ~99% of broad-lane
   candidates at 20M are true matches, and the zero-false-negative contract requires emitting
   them. No system in the surveyed literature bounds emitted volume under a full-recall contract.
2. **A real defect (fixable):** query cost-classification is keyed to a **top-64-by-frequency
   rank list**, not a frequency threshold. At 20M queries, a feature ranked #65 can carry a
   43,533-entry posting yet still classify as "selective," landing structurally-broad queries in
   the realtime lane — measured as a **32× realtime-lane slowdown** on the broad-bearing corpus
   (437,730 → 13,603 titles/s/core).

Plus one **missing optimization** the strongest peer systems treat as first-order: duplicate
stored queries are stored and evaluated N times instead of once-with-an-ID-list.

This proposal specifies five levers (§5), their FN-safety arguments, and a sequencing gated on a
**real-corpus measurement phase** (§5.0) — because the payoff of the two biggest levers is a
function of real user behavior (duplication rate, class mix) that the synthetic generator cannot
predict. Everything is scoped to change *internal work per title only*: request/response
semantics, the zero-FN contract, and default visibility are all invariant.

---

## 2. How the system works today (the mechanics this program touches)

### 2.1 Compile path: DSL → classified, anchored query

Every stored query compiles once, off the hot path (`src/dsl.rs` → `src/normalize.rs` →
`src/compile.rs`):

1. **Parse + normalize** into `Extracted { required: Vec<FeatureId>, forbidden: Vec<FeatureId>,
   anyof: Vec<Vec<FeatureId>> }` — dense integer feature IDs from the ONE shared dictionary
   (`src/dict.rs`), the same normalizer both queries and titles use (a load-bearing invariant:
   the feature spaces must line up).
2. **Signature-cover optimization** (`compile/plan.rs::anchor_plan`): choose the *gate* — which
   feature (or feature pair) the query is filed under in the candidate index. The contract is the
   **lossless cover**: if a title could satisfy the query's positive semantics, the title must
   generate at least one signature that retrieves it. Forbidden features are structurally
   invisible to this step (gating on an absence would drop real matches).
3. **Cost classification** — the decision this program changes. Verbatim logic today:
   - rarest required feature **not hot** → **class A** (single-feature anchor, realtime lane);
   - rarest required feature **hot** → pair it with the next-rarest → **class B** (arity-2
     anchor, realtime lane);
   - a single hot required feature with nothing to pair → **class C** (broad lane);
   - any-of queries: per-group best representative; all representatives selective → B, else C;
   - no positive feature at all → **class D** (rejected loudly by default; opt-in
     always-candidate lane under the universal signature, ADR-068).
4. **"Hot" is defined as top-64 by rank** (`compile/extract.rs:22`):
   `is_hot(f) = dict.mask_bit(f) != NO_MASK_BIT`, and `Dict::finalize_mask` (`dict.rs:232`)
   assigns mask bits to exactly the **64 highest-query-frequency features — no absolute
   threshold**. This one predicate serves **two masters**: (a) the 64-bit common-mask fast-reject
   in exact verification (`req_mask`, baked into every segment's SoA — must stay frozen), and
   (b) the classification/pairing decision above. The defect lives entirely in role (b).

### 2.2 Match path: title → matches

Per title (`segment/seg.rs::match_into`, allocation-free, integer-only):

1. **Normalize the title** into sorted feature IDs (two views under multi-word aliases: positive
   superset `P(T)` for retrieval, canonical `N(T)` for forbidden checks — ADR-061).
2. **Generate signatures and probe the candidate index** (`src/index.rs`; postings are
   three-tier: inline ≤8 → `Vec<u32>` ≤256 → roaring >256; a per-segment blocked-bloom filter
   skips absent keys):
   - **arity-1**: one probe per title feature against the main index;
   - **arity-2**: pairs `{hot} × {every other title feature}` — note the title side generates
     pair signatures **only for top-64-hot features**, mirroring the compile side; this
     compile/match agreement is what keeps pair-anchored class-B queries reachable (and is the
     invariant lever 3 must extend on both sides at once);
   - **broad lane** (only if `include_broad`): arity-1 probes against the separate broad index.
3. **Exact verification** (`src/exact.rs`): SoA columns, 64-bit common-mask gate, then integer
   subset/any-of/forbidden checks. Forbidden features are checked **only** here.
4. **The broad lane's evaluator** (ADR-026, `segment/broad_batch/`): for batches
   (`/_mpercolate`), broad queries are evaluated **columnar** — per-feature bitmaps over the
   title batch, each broad posting scanned once per batch instead of once per title (measured
   amortization ~1/batch_size; 115× at bs=1024 @1M; at 20M broad-on, columnar 69,925 titles/s vs
   3,487 inline = 20×). Pure-anchor broad queries take a **vacuous accept** (retrieval is proof
   of match; no verification) — the same trick ES ships as "verified candidates."
5. **Visibility semantics — the contract lever 2 must respect:** the broad lane is
   **request-gated**. Engine per-title default is `include_broad = false`; `/_search` and
   `/_mpercolate` accept per-request `include_broad` (ADR-073); the cluster coordinator's config
   default is `include_broad: true` (`ClusterConfig`). So which lane a query lives in determines
   **which requests can see it** — cost placement and visibility are currently the same axis.
6. **In the cluster** (ADR-027/080): selective queries place on their anchor's ring shard(s)
   (~2–5 shard fan-out, measured avg 3.18 / p99 5 at 20M); broad-lane queries **replicate to
   every shard** and are evaluated on one per-title broad-eval shard.

### 2.3 Update path: LSM + the compaction seam

Queries live in immutable mmap'd segments + a memtable; deletes are tombstones; compaction merges
segments (`segment/compaction.rs`). Two facts matter here:

- **Compaction is already the re-optimization seam** (ADR-056, opt-in `compaction_reanchor`):
  a merge re-derives drifted covers against current dictionary frequencies.
- **The demote guard** (`segment/seg.rs` ~line 425, review-caught in PR #31): re-anchoring
  **refuses to move an A/B query into the broad lane** even when its anchor has gone hot, with
  this comment: *"the main index is probed on every percolate; the broad lane is opt-in … moving
  a query main→broad would hide it there — a false negative. A hotness reclassification is a
  major-version blue/green concern, NOT a silent compaction change."* This is the in-repo
  precedent for the **cost lane ≠ visibility lane** obligation that shapes lever 2.

### 2.4 Duplicates today

There is no deduplication at any layer. N users storing the identical query produce N logical
IDs, N SoA verification rows, N posting entries under the same signature, and N verifications
per retrieving title. (Near-duplicates already share *gate* work implicitly — same anchor ⇒ same
posting — which is why the explicit family DAG was declined in ADR-019; that decision was about
the *selective* path and stands.)

---

## 3. The problem, precisely

### 3.1 What was measured (ADR-104 + the 20M bench cross-check)

| Measurement | Value | Meaning |
|---|---|---|
| Candidates/title, broad ON | 85.64 @100k → 682 @1M → **10,036 @20M** | grows with corpus **by design** |
| …of which true matches @20M (fixed pools) | 6,563 of 6,617 candidates (**99.2%**) | the volume is answer, not waste |
| Candidates/title, broad OFF @20M | **54.56** (p95 96, p99 112; max posting 104) | the flatness contract holds where defined |
| Class split @20M broad=0.05 (fixed pools) | A=19,912,766 / B=43,589 / **C=43,645** | ~956k broad-*intent* queries classified **A** |
| Max main-lane posting @20M broad-on | **43,533** (vs 104 broad-off) | the top-64 cliff's fingerprint |
| Realtime lane @20M: broad-off vs broad-on corpus | 437,730 → **13,603 titles/s/core (32×)** | the measured cost of the defect |
| Columnar vs inline broad @20M | 69,925 vs 3,487 titles/s (**20×**) | the quarantine lane works — when queries actually land in it |

### 3.2 Root causes, as decisions

| # | Decision | Verdict |
|---|---|---|
| 1 | **Zero-false-negative contract** — every true match is emitted | Deliberate, keep. Makes broad output irreducible; the field is unanimous that full recall ⇒ unbounded emissions (only top-N caps volume, sacrificing recall). |
| 2 | **Broad quarantine + columnar batch lane** (ADR-003/026) | Deliberate, keep. Independently invented ≥3 times (Whang's Z-list, Vespa's zero-constraint lane, ES/Monitor fallback buckets); ours is the only one that's *batched*. |
| 3 | **"Hot" = top-64 rank, no threshold** (`finalize_mask`) | **The defect.** Correctly sized for the verify-mask role (64 bits are baked into segments); wrong as the classification predicate at ≥1M scale. Peer evidence: Lucene Monitor's `termFreqWeightor` is threshold-shaped, not rank-shaped. |
| 4 | **No duplicate interning** | Missing optimization. Whang measured dedup "a significant factor" of k-index performance; Vespa ships a dedicated interned-conjunction index; A-Tree made subexpression sharing its design axis. (ES lacks it too — we're even with the incumbent, behind the leaders.) |

### 3.3 Alignment verdict (from the prior-art survey)

Aligned-or-ahead: quarantine lane (ahead: batched + columnar), never-gate-on-MUST_NOT (confirmed
independently 3×), vacuous accept (ES parity; ours survives must-clauses), title batching, tag
pre-filtering (ES users hand-roll what we ship). Behind: threshold-based hotness (lever 2),
duplicate interning (lever 1). Nobody anywhere: bounded emissions under full recall — the honest
target is **bounded wasted work per title**, which is exactly what levers 2–5 buy.

### 3.4 Risks of doing nothing

Cost and latency grow with corpus shape — not correctness (zero FN held at 20M). The deeper risk
is **unpredictability**: performance is a function of workload shape (broad fraction, duplication
rate, frequency skew) with no enforced bound — "corpus luck." One risk applies to *acting*
carelessly: a naive fix for decision 3 (silently demoting hot queries to the broad lane) would
convert a cost problem into a **correctness regression** for `include_broad=false` requests — the
exact FN the ADR-056 guard exists to prevent. Lever 2 is designed around this.

---

## 4. Design obligations (cross-cutting, non-negotiable)

1. **Cost lane ≠ visibility lane.** No lever may change *which requests see a query*. Anything
   that moves a query for cost reasons must keep it default-visible (the new tier in lever 2) or
   ride an explicit versioned migration — never a silent lane move.
2. **The agreement fence.** Any classification the *match side* must mirror (today: the top-64
   pairing predicate; after lever 3: the extended predicate + θ) is a pure function of a
   **durably recorded stats snapshot** persisted with the dictionary (RDCT-additive field,
   ADR-057 pattern) and consumed at match time as the recorded decision — never a live statistic.
   Rationale: in a DBMS a stale statistic yields a slow plan; here a compile/match disagreement
   yields a false negative.
3. **Compaction is the only reorganization point**, under the settled merge-safety contract:
   output = pure function of (inputs + pinned stats snapshot); segment-local evidence only for
   segment-local actions; every intermediate state reader-correct (both lanes stay lossless, so
   half-migrated corpora are safe); per-merge work caps; observe-first rollout (log intended
   moves before enforcing them — the OpenSearch `monitor_only` pattern).
4. **Oracle-gated migration.** Every lever lands behind the existing proof stack — the
   differential oracle, the front-end-independent oracle (ADR-087), and an ADR-104 soak re-run
   as the at-scale acceptance — so "zero FN" is demonstrated, not argued, at each step.

---

## 5. The five levers

### 5.0 Phase 0 — measure before building (gates everything)

- **Real-corpus sample** (same intake as ADR-065 criterion 12's open half: the ADR-087
  `RR_ORACLE_CORPUS` JSONL hook): measure **duplication rate** (exact-duplicate cluster-size
  distribution of compiled bodies), **class mix** under real queries (the generator's broad
  queries are single-token by construction — unrepresentative), **posting-length distribution**,
  and pair-frequency shape. These numbers size levers 1–3; ICDCS'05's covering-rate collapse
  (75% → 45% as predicate diversity grew) is the cautionary tale for building sharing structure
  before measuring sharing potential.
- **Telemetry, cheap and immediate:** posting-length percentiles per lane in `/_stats`
  (`index.rs` already computes `max_posting_len`/`count_over`), a duplication-rate estimate at
  ingest (canonical-body hash into a mergeable sketch), and a would-be-reclassified counter —
  the observe-first mode of lever 2 before any enforcement exists.

### 5.1 Lever 1 — identity dedup with ID fan-out *(biggest expected win; sized by Phase 0)*

**Mechanism.** Dedup key = the canonical **compiled body**: (`required` sorted, `anyof` groups
canonicalized, `forbidden` sorted) — *post* equivalence-expansion, so surface variants that
compile identically dedup too. Storage becomes two layers: one **body row** (masks, required,
any-of, forbidden — everything the verifier reads) + a **member list** of logical IDs, each
member keeping its own per-ID version and tags. Posting entries reference the body; the verifier
runs **once per body**; on accept, emission expands the member list (and per-member work — tag
`TagPredicate` filtering, ADR-059 rank scores — applies per member *after* the body-level
accept, which is exactly today's verify-stage semantics, just factored).

**Scope of change.** `exact.rs` SoA gains the body→members indirection; segment format bump
(v5, ADR-057-style additive); WAL/upsert/delete become member operations (delete = remove the
member, body dies with its last member; upsert moves a member between bodies); per-segment dedup
at flush/compaction first (cross-segment dedup falls out of compaction merging lists — no global
online structure, the lesson from the pub/sub literature's poset-maintenance pain).

**FN-safety:** emission is identical by construction — one evaluation's verdict applied to every
member is semantically the N evaluations of identical bodies. **Evidence:** unanimous +
measured (Whang, Vespa `ConjunctionIndex`, A-Tree, YFilter's shared accepting states, Gryphon's
coalescing assumption). **Effect:** verification and posting volume scale with *distinct* bodies;
emission cost unchanged (it's the answer). Broad queries are short → the most likely to be
exactly duplicated → this directly attacks the broad lane's evaluated-query count.

### 5.2 Lever 2 — frequency-threshold cost reclassification + the always-visible hot tier *(fixes the measured 32×)*

**Mechanism.** Split `is_hot`'s two roles. The 64-bit verify mask stays exactly as-is (baked
into segments, frozen). Classification gains `is_hot_anchor(f) = top64(f) ∨ freq(f) ≥ θ` (θ
tied to the posting-length bound the index already recognizes, e.g. the roaring-tier boundary
~1024, or a corpus-relative fraction — Phase 0 decides). Queries whose anchor is θ-hot but not
top-64 stop polluting the realtime lane.

**Where they go — the new piece:** a **hot tier** that separates the cost axis from the
visibility axis: evaluated like the broad lane (columnar, batched, vacuous-accept where
pure-anchor) but **probed on every request** like the main lane — because these queries were
default-visible before and must stay so (obligation 1; the ADR-056 guard's reasoning, now
honored instead of worked around). Genuinely-broad *intent* (class C) keeps its opt-in
semantics; the hot tier is purely a cost quarantine.

**Migration:** at compaction (ADR-056 seam) under work caps, margin-gated both directions
(demote at freq ≥ θ, promote back at ≤ θ/2 — no oscillation), driven by the pinned stats
snapshot; observe-first counter ships first (Phase 0). **FN-safety:** the hot tier is probed
arity-1 for every title feature on every request — retrievability and visibility both invariant;
no title-side change needed. **Effect:** restores the realtime lane toward its broad-off
throughput (the 32×) and gives the reclassified queries the columnar lane's 20× over inline.

### 5.3 Lever 3 — pair-anchor escalation with measured joint frequencies *(the fence lever)*

**Mechanism.** Today a hot-rarest query pairs with its next-rarest feature using **single**-
feature frequencies only. For θ-hot multi-feature queries, choose the anchor pair by estimated
**joint** frequency from a count-min sketch. The which-pairs problem that defeats DBMS
optimizers doesn't exist here: **the stored queries nominate the candidate pairs** (only pairs
co-occurring inside some query's positive feature set can ever anchor), and CM's one-sided error
(never underestimates) means a mistake can only *miss an optimization*, never fatten a chosen
pair.

**The fence, explicitly:** the title side generates pair signatures only for `{hot} × {other}`
(`seg.rs` pair loop). Extending compile-side pairing to θ-hot features **requires the identical
predicate on the title side**, or pair-anchored queries become unreachable (a structural FN). So
the predicate (+ θ, + the sketch snapshot ID it was derived from) is persisted with the dict and
frozen like the mask; both sides read the recorded decision (obligation 2). **Evidence:**
PSTHash — k=2 access-predicate signatures won the dense real-ads benchmark (0.24 ms @3M subs);
Fabret's multi-attribute clustering (46,600 → 26,500 checks in their worked example).
**Sequencing note:** lands after lever 2 (which removes the single-feature fat-anchor case
cheaply and without any fence).

### 5.4 Lever 4 — residual factoring inside posting lists *(verify-layer only)*

Within one posting, group members by residual plan shape; **subtract the probe-implied features
from each member's verify plan** (the signature already proved them); evaluate shared residual
cores once with size-specialized kernels. Our pure-anchor vacuous accept is the empty-residual
case; lever 1 is the identical-residual case; this is the general form. Evidence: Fabret's
measured **35×** phase-2 gap from exactly this (columnwise size-grouped clusters + prefetch —
their layout is literally SoA). Touches only `exact.rs` evaluation order — gating untouched, so
FN-safety is structural.

### 5.5 Lever 5 — broad-lane count-gate + dense-posting promotion *(small, contained)*

Two Vespa-proven internals for the broad/hot lanes' batch pass: (a) a **`min_feature`
pre-reject** — per query, a conservative lower bound on how many distinct positive title
features any match must contribute (safe bound: `|required|`, +1 per any-of group provably
disjoint from features already counted); the columnar pass compares a per-candidate hit count
against it before full verification — a necessary-condition filter, so under-rejecting is the
only possible error direction; (b) **dense-representation promotion** for hot postings in the
counting pass past a relative-size threshold (Vespa ships 0.40 with documented failure modes on
both sides; our three-tier postings already do this for *storage* — this extends it to the
batch evaluator's internal representation).

### 5.6 Sequencing

| Phase | Contents | Gate |
|---|---|---|
| 0 | Real-corpus measurement + telemetry + observe-only reclassification counter | corpus sample availability |
| 1 | Lever 2 (threshold + hot tier) · Lever 5 | Phase 0 numbers; oracle + soak re-run |
| 2 | Lever 1 (dedup) | Phase 0 duplication rate justifies the format bump |
| 3 | Lever 3 (pairs + fence) · Lever 4 | Phase 1/2 residual profile |

Each phase is one-or-two ADR-sized PRs, codex-gated, with the ADR-104 soak as the standing
at-scale acceptance run.

---

## 6. Explicitly out of scope (decided against, with reasons)

- **Top-N / WAND-style early termination** — safe only for the top-k set; a controlled false
  negative against the all-matches contract (filed as contingency in the research doc §7 if the
  product contract ever changes).
- **SIFT-style global per-query counters on the hot path** — an N-wide mutable array per title
  violates the allocation-free budget; counting pays only inside an already-gated candidate set
  (which is what the verifier is).
- **Positive covering to drop subsumed queries** — sound for pub/sub *forwarding*, wrong for
  emit-all-matches (every ID must be reported). The negative direction (general-gate-fails ⇒
  skip covered) may fall out of lever 4's grouping for free; not a standalone lever.
- **Adaptive in-place index structures** (BE-Tree-style) — conflict with immutable mmap'd
  segments; their *feedback policy* is adopted (lever 2's margin gates), their mutable trees are
  not.
- **Reducing emitted matches by any means** — the volume is the product's correct answer;
  rank/size/filter caps already exist at the API layer for consumers that want less.

## 7. Open questions for review

1. **The hot tier vs. documented semantics:** lever 2 proposes a third always-visible tier.
   The cheaper alternative — reclassify only *newly ingested* queries into class C and document
   the semantic — changes visibility for those queries under `include_broad=false`. Recommend
   the tier; confirm.
2. **θ policy:** absolute posting-length bound (~1024, the roaring boundary) vs corpus-relative
   frequency. Phase 0 data decides; default recommendation is the absolute bound (it's the
   quantity the index already treats as a regime change).
3. **Dedup member semantics:** per-member versions/tags are kept (required for ADR-049/059
   parity) — confirm that upsert-moves-member-between-bodies is acceptable WAL semantics
   (single-frame, ADR-067 pattern).
4. **Corpus sample logistics:** size, sanitization, and delivery of the real saved-search
   sample (same blocker as criterion 12's real-corpus audit — one sample serves both).
