# Matching — signature optimizer, candidate index, exact matcher, broad lane, explain

*Scope: the heart of the engine — how compiled queries are gated and verified. Covers the
signature-cover optimizer, the candidate index, the integer-only exact matcher, broad-query cost
classes, and explain tooling. Siblings:
[`normalization.md`](normalization.md) (where features come from),
[`ingestion-and-updates.md`](ingestion-and-updates.md) (how this data is stored/updated),
[`clustering-and-scaling.md`](clustering-and-scaling.md). See the [overview](README.md) for the
correctness contract this section must uphold.*

> **Implementation status:** Signature optimizer, candidate index, exact matcher, broad-lane cost
> classes (A/B/C/D), and explain tooling are fully implemented and tested. Near-duplicate queries are
> clustered *implicitly* — they share signature anchors in the candidate index, so a single failed
> anchor probe drops the whole cluster's candidates. An explicit query-family / shared-prefix-DAG
> structure (subtree pruning) was evaluated and deliberately **not** pursued; see
> [DECISIONS](../DECISIONS.md) ADR-019 for the reasoning. See [STATUS](../STATUS.md).

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
  cheapest correct scheme used by the PoC:
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
is what the PoC implements.

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
lane**:

- holds class-C queries indexed by their (few, coarse) features;
- is evaluated with **batch / columnar** scans over a title batch, amortizing cost, rather than per
  title in the hot path;
- can maintain **precomputed subscriptions** (materialized result sets refreshed periodically) for the
  very broadest;
- is metered to a higher cost class (the spec's "higher price/cost class") and can return
  **rewrite suggestions** ("add a year or set to make this realtime").

This is the direct, structural fix for the percolator "unsupported query becomes an always-candidate"
failure mode: we *detect* low selectivity at compile time and quarantine it, instead of paying for it
silently on every title. (The class-B/C escalation could additionally use roaring-bitmap intersection.)

---

## 5. Explain / debug tooling (always available)

For any query: show parsed AST, compiled required/forbidden/any-of, chosen signatures with their cost
scores, and cost class. For any (title, query) pair: show the title's extracted features, which
signature(s) made the query a candidate (or why it was never a candidate), and the exact-match
pass/fail with the specific failing feature (missing required / present forbidden / unsatisfied
any-of). This is built in, not bolted on — it's the same SoA data read in a verbose mode.
