# Matching — signature optimizer, candidate index, exact matcher, broad lane, metadata & ranking, explain

*Scope: the heart of the engine — how compiled queries are gated and verified. Covers the
signature-cover optimizer, the candidate index, the integer-only exact matcher, broad-query cost
classes, and explain tooling. Siblings:
[`normalization.md`](normalization.md) (where features come from),
[`ingestion-and-updates.md`](ingestion-and-updates.md) (how this data is stored/updated),
[`clustering-and-scaling.md`](clustering-and-scaling.md). See the [overview](README.md) for the
correctness contract this section must uphold.*

> **Implementation status:** Signature optimizer, candidate index, exact matcher, broad-lane cost
> classes (A/B/C/D), and explain tooling are fully implemented and tested. The broad lane's
> **batch / columnar evaluation** (§4) is now implemented too — once-per-batch scans + bitmap-algebra
> verification + a pure-anchor skip-verify fast path, exposed as `match_titles_batch` / `POST
> /_mpercolate` (ADR-026). Near-duplicate queries are
> clustered *implicitly* — they share signature anchors in the candidate index, so a single failed
> anchor probe drops the whole cluster's candidates. An explicit query-family / shared-prefix-DAG
> structure (subtree pruning) was evaluated and deliberately **not** pursued; see
> [DECISIONS](../DECISIONS.md) ADR-019 for the reasoning. **Per-query metadata, filtered percolation,
> and ranking (§5) are design-only** — the percolator-parity work in [STATUS](../STATUS.md) Tier 4 /
> [DECISIONS](../DECISIONS.md) ADR-049. See [STATUS](../STATUS.md).

**TL;DR (for agents)**
- **Owns:** signature optimizer (`compile.rs`), candidate index (`index.rs`), exact matcher (`exact.rs`), explain (`explain.rs`)
- **Key invariant:** Signatures built ONLY from required features / any-of groups, never from forbidden features (lossless cover contract)
- **Hot path:** title signatures → probe index → union candidate IDs → common-mask gate (2× `u64` ops) → sorted-slice verification → emit matches
- **Cost classes:** A (selective, realtime) / B (moderate) / C (broad → quarantine lane) / D (reject with rewrite suggestions)
- **Measured:** ~54 candidates/title, flat from 1M–5M queries; ≈710k titles/sec/core (full numbers: [performance/results.md](../performance/results.md))
- **Gotchas:** Adaptive postings (inline ≤8 → Vec ≤256 → Roaring >256); broad lane is ~9× slower than selective path

---

## 1. Signature-cover optimizer (the heart of the compiler)

A **signature** is a small combination (1–3) of *required* features hashed to a `u64` signature key.
For each query we must choose a set of signatures that (a) satisfies the lossless-cover contract (see
[overview](README.md) §2) and (b) minimizes expected match-time cost.

**Candidate signature generation.** From a query's required features `R` and required any-of groups
`G1..Gk` (each group is "≥1 of these must be present"):

- A valid signature must be **hittable by every matching title**. So a signature is any combination
  that is a subset of `R` (always present) — and, to incorporate any-of groups losslessly, we must
  emit a *cross-product family*: pick at most one representative from each group we include. The
  cheapest correct scheme used by the engine:
  - Build the anchor from the **rarest features of `R`** (1–3 of them).
  - If `R` alone is empty or too common, we must cover via groups: emit one signature per element of
    the rarest group (so whichever branch the title satisfies, a signature fires) — exactly the
    "extract a term from every OR branch" rule, applied to the single cheapest group.

**Cost model.** For a candidate signature `s` we estimate expected candidates it contributes and its
overheads using compile-time statistics:

```
score(s) =  w1 * E[candidates_retrieved(s)]      // ≈ posting length × title-hit-rate
          + w2 * postings_memory(s)
          + w3 * update_fanout(s)                 // how many signatures churn on update
          + w4 * hot_key_risk(s)                  // p99 spike potential
```

We pick the **minimal lossless cover that minimizes total score** (greedy: take the lowest-score valid
anchor; if any-of groups remain uncovered, extend with the cheapest covering set). Statistics tracked:
per-feature query-frequency, per-signature query-frequency, observed per-signature title hit-rate, and
candidate survival rate (how often a candidate from `s` survives exact match) — the last is fed back
from runtime telemetry on compaction.

**Why this beats single-term anchoring.** A single rare *term* can still be a hot key (popular player).
A 2–3 feature *semantic* signature (`player:michael_jordan + year:1994 + grader_grade:psa10`) is far
more selective, and the optimizer prices each candidate so it avoids the popular-player trap by adding a
second feature when the first is hot.

---

## 2. Candidate index (segment-local)

```
signature_key (u64)  →  posting list of SegmentLocalQueryId (u32)
```

Stored as an open-addressed hash table (signature_key → posting offset) plus a posting arena. Postings
are **adaptive by cardinality** (the roaring lesson, specialized):

| Cardinality | Representation | Rationale |
|---|---|---|
| 0–8 | inline tiny array in the bucket header | no heap, no pointer chase |
| 9–4096 | sorted `u32` array (arena slice) | branch-predictable galloping/merge intersection |
| medium | blocked sorted array (SIMD-friendly) | vectorized intersection |
| large | roaring bitmap (`roaring` crate) | compressed, fast union |
| huge | **not stored** — routed to broad lane | a signature this common is not selective |

**Segment-local IDs.** Only `u32` `SegmentLocalQueryId` rides the hot path. The `u64`
`GlobalLogicalQueryId` and `PhysicalVersionId` are looked up **once per confirmed match**, at the very
end, via a per-segment `local → (logical, version)` table. This keeps hot-path working sets small and
cache-resident.

**Probing.** For a title we enumerate `sigs(T)` (bounded: combinations over the title's features up to
the max signature arity, typically a few dozen), look each up, and **union** the postings into a
candidate buffer (reused, allocation-free). Union, not intersection, because any single matching
signature is sufficient — intersection would violate the cover contract.

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
4. **Any-of groups:** each group needs ≥1 member present.
5. Survivors → resolve `logical_id`/`version`, emit.

No strings, no regex, no virtual dispatch, no allocation. A "bytecode VM" variant is described as an
alternative (a tiny opcode stream per query) but the SoA mask+slice form is faster for this shape and
is what the engine implements.

---

## 4. Broad-query handling (cost classes)

Every compiled query is classified by the selectivity of its **best achievable signature cover**:

| Class | Meaning | Handling |
|---|---|---|
| **A** | highly selective (rare multi-feature anchor) | main index, realtime |
| **B** | acceptable selectivity | main index, realtime |
| **C** | broad (`PSA 10`, `Michael Jordan`, `rookie`) | **separate broad lane** |
| **D** | pathological (e.g. only a forbidden clause, or no required feature at all) | **reject at compile**, return rewrite suggestions |

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

---

## 5. Per-query metadata, filtered percolation, and ranking

> **Status:** metadata + filtered percolation (§5.1–§5.3) are **built (single-node) + oracle-proven**
> (2026-06-03, [DECISIONS](../DECISIONS.md) ADR-049, [STATUS](../STATUS.md) Tier 4), and now thread
> **end-to-end through the cluster** (in-process + the experimental gRPC path) — one frozen `TagDict`
> shared into every shard like the `Dict`, raw tags in the log + read-only `get_or_synthetic`
> resolution, the filter resolved once at the coordinator + shipped as `TagId` groups
> ([ADR-055](../DECISIONS.md), 2026-06-04); **ranking + pagination (§5.4) are now built single-node**
> ([ADR-059](../DECISIONS.md), 2026-06-04 — cluster ranking still deferred). Motivated by the reference
> workload in [`../research/percolator-workload.md`](../research/percolator-workload.md), whose dominant
> read pattern is "percolate, then narrow to one category." Code: `src/tagdict.rs` (tag interning),
> `src/exact.rs` (`TagPredicate` + SoA tag column + verify-stage filter), `src/rank.rs` (the post-match
> scorer — ADR-059), `src/segment/` (ingest/match threading + `EngineSnapshot::rank`),
> `src/storage/segment.rs` + `src/wal.rs` (`.seg` v3 / WAL v2 persistence), `src/bin/server/`
> (the REST filter + rank/pagination surface), `src/cluster/` (`coordinator/{lifecycle,ingest,matching}` +
> `clog` + `shard` + the gated `remote`/`server` — ADR-055).

Production percolators store **structured tags** alongside each query (a category, a status, secondary
keys) and at match time **filter the percolated candidates by those tags** — and sometimes rank them.
Reverse Rusty today returns a bare `Vec<u64>` of matched `logical_id`s with no tag awareness. This is
the design for closing that gap **without touching the lossless-cover contract**.

### 5.1 Metadata model — interned integer tags in the SoA

A stored query may carry a small set of `key → value` tags. Each distinct `(key, value)` is **interned
to a dense integer `TagId`** at compile time — the same move that turns feature strings into
`FeatureId`s (`dict.rs`), so **no strings reach the match path**. The per-query tags become one more
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

### 5.4 Ranking — an optional layer *over* the boolean-correct set (built single-node, ADR-059)

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
live copy** of each id (memtable first, then base segments newest→oldest). **Single-node** (the REST
surface); cluster ranking is deferred behind the same `RankSpec` seam (cross-shard priority fetch at the
coordinator merge — [ADR-055](../DECISIONS.md)/[ADR-059](../DECISIONS.md)). Consistent with the reference
workload, where ranking is a presentation-surface concern, not a matching-core one.

### 5.5 Alternatives (documented, deferred)

- **Post-match external filter** (return everything, look up each id's metadata afterward) — effectively
  what callers do *today*, outside the engine. Rejected as the long-term design: it still verifies every
  match and needs an external metadata store; 5.2 is strictly better once tags live in the SoA.
- **Tag-partitioned segment skip** — for the *dominant* single-key filter (the `category` tag), index or
  route queries by that tag so a filtered probe skips whole segments (composing with the entity-anchor
  sharding in [`clustering-and-scaling.md`](clustering-and-scaling.md)). A real optimization, but it must
  be **filter-driven and fail-open** (skip only when the request's filter proves a segment irrelevant;
  when unsure, probe) so it can never drop a wanted query. **Deferred** past the 5.2 baseline.

---

## 6. Explain / debug tooling (always available)

For any query: show parsed AST, compiled required/forbidden/any-of, chosen signatures with their cost
scores, and cost class. For any (title, query) pair: show the title's extracted features, which
signature(s) made the query a candidate (or why it was never a candidate), and the exact-match
pass/fail with the specific failing feature (missing required / present forbidden / unsatisfied
any-of). This is built in, not bolted on — it's the same SoA data read in a verbose mode.
