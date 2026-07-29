# ADR-156: Alias-feedback read REST API contract

> [Engine, errors, dependencies & ops decisions](areas/engine-quality-and-operations.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

`GET /_vocab/aliases/feedback` renders ADR-103's rolling behavioral evidence for candidate alias
pairs. Its evidence core was bounded and oracle-proven, but the HTTP boundary remained prototype
quality. Unknown and duplicate query parameters were ignored. Invalid overlap thresholds were
silently replaced or clamped, zero evidence thresholds could label an empty observation as
validated, and arbitrary request bodies were ignored under the server-wide 100 MiB limit.

The handler held the live feedback mutex while it resolved sampled query sources, tokenized them,
filtered degenerate evidence, calculated overlap, and serialized every tracked pair on a Tokio
worker. It had no administrative admission, page or response bound, body deadline, cache policy,
HEAD behavior, or fixed request telemetry. Coordinator mode returned an unobserved generic 501
without first validating the shared route contract.

[Elasticsearch synonym-set listings](https://www.elastic.co/guide/en/elasticsearch/reference/current/list-synonyms-sets.html)
use `from` and `size` with a total `count`, but Elasticsearch synonym APIs manage explicit rules in
named sets. OpenSearch likewise configures explicit rules through its
[synonym token filter](https://docs.opensearch.org/latest/analyzers/token-filters/synonym/).
Neither system passively compares reverse-query match populations or reports rolling validation
evidence, so an Elasticsearch/OpenSearch synonym path would misstate this operation.

## Decision

- Keep native `GET` and `HEAD /_vocab/aliases/feedback`. Do not add a `_synonyms` alias.
- Accept only `min_overlap`, `min_titles`, `min_queries`, `from`, and `size`. Reject unknown,
  duplicate, and malformed parameters. Require finite `min_overlap` within `[0,1]` and positive
  title/query evidence thresholds.
- Adopt the familiar `from`/`size` offset controls and a top-level total `count`. Default `size`
  to 256, the shipped feedback tracking cap, and reject sizes above 256. Preserve
  `tracked_pairs` as a compatibility spelling equal to `count`; `pairs` contains only the selected
  page. Out-of-range `from` and `size=0` return an empty page.
- Require an empty request body. Bound extraction at 64 KiB and 250 ms. Bound serialized output at
  1 MiB and direct callers to reduce `size` if a page exceeds it.
- Wait asynchronously for the shared one-slot administrative permit. On a blocking worker, hold the
  feedback mutex only long enough to clone the requested page and capture the corresponding engine
  snapshot. Release the mutex before query-source lookup, exclusion tokenization, overlap
  calculation, and serialization. The owned permit remains with the worker through completion.
- Return whole-route `took` and `took_ms`, the capture flag, total counts, echoed thresholds, and the
  evidence page. Use the standard JSON error envelope, `Allow: GET, HEAD`, and
  `Cache-Control: no-store` for every route-reached response. Count and time outcomes under the
  fixed `vocab_aliases_feedback_get` label.
- Keep coordinator mode fail-loud with 501 and the single-node-replica plus `PUT /_vocab`
  alternative. Apply the same method, query, body, size, and deadline contract before returning the
  capability boundary, and observe every outcome with the same no-store telemetry.

## Consequences

The historical default response remains complete under the shipped 256-pair tracking default.
Deployments that deliberately track more pairs page the report instead of creating unbounded work.
Passive capture pauses only for a bounded page clone rather than the full evidence calculation, and
feedback lock contention cannot block a Tokio worker.

Paging is offset-based over ADR-103's deterministic confidence/forms order, not a stable cursor.
Evidence can change between pages because it is intentionally rolling. Callers needing one coherent
window should stop capture or reset and quiesce the title stream before reading all pages.

The route stays native: familiar paging is useful, but mapping behavioral evidence onto explicit
synonym-rule APIs would be lossy.

## Safety and proof

This read clones evidence and an immutable engine snapshot; it does not change alias state,
normalization, candidate retrieval, exact verification, or matching. The ADR-103 exclusion and
Jaccard calculations are unchanged.

Core tests pin bounded page snapshots and retained evidence. Standalone route tests cover timed
no-store output, total and compatibility counts, paging order, validation, GET/HEAD behavior,
method/query/body strictness, body size and deadline, fixed telemetry, asynchronous admission,
closed admission, and feedback-lock waiting off the async runtime. Coordinator tests cover shared
validation and the observed no-store 501 alternative.
