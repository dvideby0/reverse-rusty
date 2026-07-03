# ADR-106: Canonical-body deduplication, Stage A — per-segment posting sharing

**Status:** accepted (2026-07-03) · **Program:** Broad-Query Cost Program increment 2
([`../proposals/broad-cost-program.md`](../proposals/broad-cost-program.md) §5.1), paired with
increment 1's hot tier (ADR-105) per the review outcome: the two ship together because the
*measured* 20M defect is an **identical-query concentration**, which the hot tier alone cannot
recover.

## Context

PR-A's observe telemetry falsified the program spec's corpus sizing: the ADR-104 32× is not a
population of θ-classifiable fat class-A anchors (`would_be_hot` = **782** at θ=1024) — it is
**~43.5k byte-identical "psa 10"-shaped queries** whose shared class-B pair posting is the
single 43,533-entry list every hitting title scans and verifies end-to-end. Every peer system
surveyed deduplicates stored queries before optimizing their evaluation
([`../research/broad-scaling-prior-art.md`](../research/broad-scaling-prior-art.md) ranked it
#1-unanimous); we stored every copy as an independent posting entry.

Stage A is the spec's "measure by building the cheap half": in-memory sharing only, **no
on-disk format change**, reversible, and itself the instrument (duplication-rate sketch) that
sizes Stage B (the persisted body→member indirection, increment 3).

## Decision

**One posting entry per distinct canonical body per in-memory segment.** At `add_compiled`,
each accepted entry hashes its **semantic body** — required/forbidden masks + sorted tails +
canonical any-of groups, with domain separation; never identity (logical id, version, tags) —
and joins an existing group when a hash hit is confirmed by **exact body equality**
(`ExactStore::bodies_equal`; a collision can never false-share). A duplicate:

- skips posting insertion entirely (`dup_of[local] = leader`, `dup_members[leader] += local`);
- **adopts the leader's cost class** — identical bodies *can* plan different classes when a
  θ-crossing frequency bump lands between two adds (A→H), and the member rides the leader's
  postings, so its class byte must describe the lane it actually lives in. Lossless because
  A/B/H are all always-visible and the structural classes (C keyed to the frozen top-64 mask,
  D negation-only) cannot diverge between identical bodies;
- is emitted through the leader: every match path (scalar `probe`, the columnar kernel's
  vacuous-accept and full-verification arms, the class-D universal probe) verifies the shared
  body **once** and fans emission out per member, each gated on **its own** aliveness and tags.
  A dead leader never drops its alive members; on grouped segments `eval_into` runs with the
  empty predicate so the leader's tags cannot veto a member (ADR-049 identity stays
  per-member).

**Flush expands.** `freeze_index` writes each group flat (leader + members, re-sorted), so the
`.seg` format, every mmap reader, and every pre-dedup binary are untouched — an mmap-attached
segment carries no groups (`has_dup_groups() == false`) and takes the exact pre-dedup match
paths. Stage B is precisely the decision to persist the indirection instead.

**Compaction regroups — and is the cross-segment dedup mechanism.** Both merges re-derive
groups on the destination side by canonical body, so sharing survives merging and identical
bodies from *different* source segments collapse into one group. A source member's carried
cover is its **source leader's** key set (a valid lossless cover for the shared body — a dead
leader's keys still anchor its alive members); in the re-anchoring merge (ADR-056/105) only
dest leaders re-derive covers, members adopt the leader's (possibly migrated, possibly
kept-old) class — adoptions count in `hot_promoted`/`hot_demoted` (the class split moved) but
are exempt from `hot_migration_max_moves` (the cap bounds posting-rebuild work; an adoption
does none).

**Knob:** `dedup_bodies` (default **on**, dynamic via `/_settings`) gates the grouping of new
writes only; existing groups stay grouped (and correct) when toggled off. Group-free segments
pay one segment-level branch — the kill-switch idiom.

**Observe telemetry** (fed on every accepted compile regardless of the knob): `bodies_total`,
`dup_joined`, and a lazily-allocated 2²²-bit linear-counting sketch estimating **global**
distinct bodies (`distinct_bodies_est`) — the cross-segment/cross-flush duplication Stage A's
per-segment groups cannot capture, i.e. the Stage B sizing evidence. Surfaced in `/_stats`
(`dedup` block), `/_cat/stats`, Prometheus gauges, and the bench build report.

## Why this is safe (zero false negatives)

A group member shares its leader's *entire positive and negative semantics*, so the leader's
signature cover is a lossless cover for the member — retrieval through the leader retrieves
the member by construction. Body verification is class-invariant (`verify` reads columns, not
classes). Aliveness and tags gate **emission only**, exactly as they did per-entry. The
adopted class only ever swaps lanes among always-visible ones (A/B/H). Disk stays expanded, so
durability, recovery, replication, and the cluster attach path are byte-identical.

## Proven

`tests/oracle/dedup.rs` (9 legs): heavy-duplication differential ≡ brute (per-title + batch,
multi-segment), dedup-on ≡ dedup-off on both `include_broad` modes, tombstoned-leader /
tag-divergent-member emission, flush→mmap reopen + WAL-tail replay ≡ brute, cross-segment
compaction regroup (posting scans shrink, no member lost), upsert group moves, the
**1000-identical-queries ⇒ ~one-posting-entry-scanned** structural pin (the ADR-104 finding
inverted), and linear-counting sketch accuracy. The pre-existing oracle stack (incl. the
ADR-105 hot suite and the ADR-104 soak) runs green with dedup **default-on**. The
memory-vs-mmap stats-parity regression (`coverage_gaps/broad_lane.rs`) pins the ADR-101
under-count class with `dedup_bodies=false`, since leader-scanning vs expanded postings is a
stats divergence *by design*.

## Measured (the combined 20M recovery — capture log 2026-07-03, `docs/performance/benchmark-results.txt`)

On the ADR-104 broad=0.05 20M corpus: main max posting **43,533 → 103**, candidates/title
**6,616.65 → 53.75** (the broad-free flat-~54 pin restored on the broad-bearing corpus),
per-title selective **13,383 → 84,708 t/s/core (6.3×)**, batch selective **93,452 →
391,388 (4.2×)**, broad postings/pass @bs=1 **99.5M → 2,281**; matches/title unchanged at
6,562.671 (emission is the answer — the residual per-title gap vs the broad-free 423k lane
is emission-bound, levers 1/4 territory). θ=1024 moves exactly `would_be_hot` = 782 → class
H with results byte-identical; hot-empty overhead pinned free (423,045 vs 416,172 t/s/core,
run noise). The 20M K=8 durable soak is green at θ ∈ {0, 1024} with zero mismatches; its
candidate volume matches the ADR-104 canonical exactly — the durable path flushes to
expanded mmap postings, so Stage A's candidate-volume win is in-memory-only by design
(Stage B is the durable counterpart).

## Alternatives considered

- **Persist the groups now (skip to Stage B).** Rejected: a segment-format indirection is the
  most expensive-to-un-ship change in the program; the spec gates it on Stage A's measured
  rates deliberately.
- **Dedup by source text instead of compiled body.** Rejected: misses semantically-identical
  variants (token order, tag-only differences) and couples the group key to the normalizer
  epoch; the compiled body is what evaluation actually costs.
- **Refuse groups whose members classify differently.** Rejected in favor of class adoption:
  refusal splits groups on a θ-frequency race (add-order dependent), while adoption is
  deterministic and lossless among always-visible lanes.
- **Global (cross-segment) dedup at ingest.** Rejected for Stage A: a global body index is a
  write-path coupling across sealed segments; compaction-time regrouping captures the same
  convergence lazily.

## Deferred

- **Stage B** (increment 3): persisted body→member indirection, gated on the sketch's measured
  global duplication rate on the real corpus.
- Member-aware ranking short-circuits (rank reads per-member tags today, unchanged).
