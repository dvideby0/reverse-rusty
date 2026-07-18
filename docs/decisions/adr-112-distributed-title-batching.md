# ADR-112: Distributed ranked title batching

- **Status:** Accepted
- **Date:** 2026-07-18

## Context

ADR-110 made distributed ranked reads exact and bounded, but single-title: every incoming title
costs one `PercolateTopK` RPC per routed shard, and the cluster batch surface (`/_mpercolate` in
coordinator mode) fans N independent scalar percolates — the columnar broad/hot batch kernel
(ADR-026/080/105), the whole point of the broad-cost program's amortization, was unreachable from
the distributed path. Worse, the batch kernel itself hardcoded `EmitAll` collect-all: ADR-109
emission ownership did not exist on the batch path at all, so no bounded batch wire could be
correct. The ranked-percolation program's Increment 5 (deferred by ADR-110 §Consequences) is this
increment.

## Decision

### Per-title ownership and bounded collection through the columnar kernel

The batch driver generalizes over two monomorphized seams, both compiling to the previous shape
under the defaults: a `BatchMatchCollector` (the indexed analogue of `MatchCollector`; the
compatibility `AllBatchCollector` and the new per-title `BatchTopKCollector` — one scorer-free
`TopKState` slot per title sharing ONE newest-live scorer) and a `BatchEmissionPolicy` (per-title
`EmissionPolicy`: `EmitAll`, or `PerTitleUniqueOwner` over index-aligned `OwnershipContext`s). The
columnar kernel reads each candidate's ADR-109 placement once (per body-group member, per row) and
consults `should_emit(title_index, placement)` per set title bit — after verification, identity
metadata only, exactly the scalar rule. Chunk-local context slices are cut from the same base as
the title chunk, so an index mix-up (which would silently move an emission to the wrong title's
owner) is structurally prevented and `debug_assert`ed.

New snapshot entries `try_match_titles_batch_top_k` / `_owned` admit, then drive per-rayon-chunk
`BatchTopKCollector`s: per-title exact winners + honest totals; batch-aggregate `MatchStats` with
`stats.matches` = the saturating sum of per-title totals (per-title match statistics are
structurally impossible on the columnar path).

### Admission: titles, bytes, and the aggregate heap budget

Two lean-core consts (deliberately NOT config knobs — zero new surface, defaults byte-identical):
`MAX_RANKED_BATCH_TITLES = 10_000` (aligned with `max_percolate_batch`'s default) and
`MAX_RANKED_BATCH_HEAP_ROWS = 2^20` bounding `size × titles` (each per-title collector eagerly
reserves K heap + K id-set slots ≈ 40 B/row ⇒ ~40 MiB ceiling; the total tracker stays lazy and
threshold-capped). Typed `TopKAdmissionError` variants reject before any matching, on the
coordinator AND trust-but-verify on the server. The HTTP layer additionally composes the dynamic
`max_percolate_batch` knob. The wire request must fit the same cap ceiling replies obey
(client-side pre-check).

### The streamed wire

Additive `PercolateTopKBatch`: one bounded unary request — `repeated BatchTitle{title,
OwnershipContext}` (routing is per title, so contexts are per entry), one shared
program/K/threshold/scope, ONE `remaining_micros` — answered by a server stream of per-title
bounded frames IN ORDER (each the single-title reply shape + `title_index`, each under the
existing 1..=4 MiB `check_result_bytes` cap) plus exactly one trailing summary frame
(`titles_served` + aggregate stats) — the completeness sentinel. The `RemoteShard` client enforces
strict in-order completeness: a gap, duplicate, reorder, missing summary, count disagreement, or
extra frame fails the whole batch loudly; attestation failures carry the ADR-111
`protocol`/`ownership_mismatch` wire codes so the legacy substring fallback cannot mistype them.
An old server answers `UNIMPLEMENTED` — fail-loud per the ADR-110 version-skew-honesty rule; v1
`/_mpercolate` (unchanged, per-title fan) remains the rolling-upgrade batch surface.

### Coordinator fan, per-title exact merge, one-credit union fetch

`try_percolate_filtered_top_k_batch` routes every title independently, groups titles by shard, and
fans ONE call per involved shard. A shard broad-evaluates only when some sub-batch title selected
it as the ONE broad evaluator (the ADR-080 bounded broad fan-out); its columnar broad pass then
evaluates broad candidates against every sub-batch title, but per-title ownership suppression
keeps the emitted set identical to N single calls — only `MatchStats` may differ, which oracles
never compare. Each title merges through the same core as the single-title path (`validate_part` +
the extracted `merge_title_rows`: ADR-109 disjointness, the exact/lower-bound total rule, the one
shared comparator, truncate-at-coordinator).

`fetch_ranked_sources_batch_bounded` fetches each DISTINCT cross-title winner once (per-title
disjointness is across shards *within* a title, so one id may win several titles, possibly with
different owners — sources are version-identical, the first-observed owner serves), drains owner
groups sequentially under ONE credit, and charges the credit per DELIVERED occurrence, so the
returned enrichment never exceeds the bound even under cross-title duplication.

### `POST /v2/_mpercolate`

Both serving modes, over the ADR-111-era shared delivery seams (`run_bounded` + the shared failure
classifier — the batch endpoint adds no third copy of the timeout/classification arms). The v1
batch shape (one shared parameter set + `documents[]`) with v2 slot semantics: per-slot exact
top-K + honest totals (pinned ≡ `/v2/_search` per slot), optional winner `_source` under the ONE
16 MiB credit with distinct-winner dedup in both modes, one permit, one absolute deadline (default
30 s), whole-batch 408, empty `documents` ⇒ 200 empty `responses`. `explain` stays on
`/v2/_search` and is a named 400 here, as are `from`/`cursor`/`allow_partial_results`/`document`/
`query`.

## Alternatives rejected

- **Repeated-in-one-unary reply** — unbounded encode; the per-frame stream keeps every message
  under the existing cap with no new knob.
- **Paged unary replies** — cursor state for no benefit over HTTP/2 stream flow control.
- **Per-title K/threshold/filter** — explodes the admission product bound and the one
  `requested_size` attestation; no surface precedent (v1 `/_mpercolate` is shared-options);
  heterogeneous-K callers split batches.
- **Silent per-title fallback on old servers** — version-skew honesty; loud `UNIMPLEMENTED`.

## Invariant compliance

Collectors run post-verification (they structurally cannot affect candidate retrieval or the
lossless cover); ownership is identity metadata and never gates matching; the batch-wide broad
evaluation moves COST only — per-title visibility is unchanged (the two-axis rule); no per-candidate
allocation is added (placement reads are borrowed slice views; the `EmitAll` monomorph folds to
the pre-ADR-112 shape, proven by the unchanged `tests/broad_batch` matrix + oracle batch passes +
the canonical rankbench checksums).

## Consequences and deferred work

- **No durable-format change**: placement identity is durable since ADR-109; manifests, segments,
  WAL, and protobuf message numbering are untouched (the service change is additive).
- Deferred: a broad-title column mask (skip broad bitmap columns for titles whose evaluator is
  elsewhere — a stats/perf refinement, not correctness), cross-title scorer memoization (tracked
  with the ranked perf polish), per-title `MatchStats`, an NDJSON `_msearch`-style surface, and
  batch admission knob-ification (consts suffice until an operator needs otherwise).
