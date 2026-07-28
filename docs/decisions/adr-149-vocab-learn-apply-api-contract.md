# ADR-149: Vocabulary learn-and-apply REST API contract

> [Engine, errors, dependencies & ops decisions](areas/engine-quality-and-operations.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

`POST /_vocab/learn_and_apply` already composed any-of synonym/equivalence learning and optional
NPMI phrase induction over the server's live query sources, then rebuilt under the merged
vocabulary. Its HTTP and execution boundaries remained prototype quality. Unknown query parameters
were ignored, zero and unbounded controls were accepted, NPMI controls could be supplied while
phrase induction was disabled, and arbitrary request bodies were ignored under the server-wide
100 MiB ceiling.

The O(corpus) learning and rebuild ran directly in an async handler while holding a `parking_lot`
engine or cluster lock. It had no administrative admission, route timing/counter labels, cache
policy, body deadline, worker-failure boundary, or disconnect-safe ownership. Coordinator success
returned `{acknowledged, rebuilt}` while standalone and the reference documented
`{acknowledged, recompiled}`. Standalone also mapped every failure to 400 and acknowledged a rebuild
without checking complete source coverage or whether its durable commit had degraded.

Elasticsearch manages explicit named synonym sets and can
[reload eligible search analyzers](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-reload-search-analyzers-1).
OpenSearch exposes explicit rules through its
[synonym token filter](https://docs.opensearch.org/latest/analyzers/token-filters/synonym/) and can
refresh updateable search analyzers. Neither API learns a feature model from stored reverse-query
DSL and recompiles that query corpus. An ES/OpenSearch path alias or analyzer-shaped response would
therefore claim semantics this operation does not have.

## Decision

- Keep `POST /_vocab/learn_and_apply` native. Preserve its query-parameter controls for compatibility
  but do not add an index/analyzer or synonym-set alias.
- Accept POST only and require an empty body. Parse query parameters with unknown-field rejection.
  Bound body extraction at 64 KiB and 250 ms, even though any non-empty body is invalid. Return the
  standard JSON error envelope, `Allow: POST` for unsupported methods, and `Cache-Control: no-store`
  on every route-reached outcome.
- Share the scalar-control validator with review-first learning: `min_count >= 1`; NPMI knobs require
  `corpus_phrases=true`; `npmi_tau` is finite within `[-1, 1]`; `npmi_min_count >= 1`; and
  `npmi_iterations` is within `1..=8`. `learn_equivalences` remains independently composable.
- Wait asynchronously for the server's one administrative-work permit, then move the permit and
  complete mutation onto one blocking worker. Corpus gathering, learning, engine/cluster lock waits,
  compilation, persistence, and publication remain synchronous as one operation, but none blocks a
  Tokio worker. The owned permit is released only when work actually ends, including after client
  disconnect.
- Return one response shape in standalone and coordinator modes:
  `{took, took_ms, acknowledged, recompiled}`. `recompiled` counts canonical unique live sources,
  not physical duplicate rows or placement copies.
- Before standalone mutation, refuse unhealthy durable state. After learning, require the rebuild
  count to equal the canonical live-source count and require no stale segment. Publish a coherent
  live rebuild even if its storage commit fails, but return `503 persistence_unavailable` rather
  than acknowledgement. Preserve the coordinator's atomic blue/green and checkpoint behavior and
  map its typed `ShardError` without weakening non-local or durability refusals.
- Count and time every outcome under fixed `vocab_learn_apply` labels, starting before transport
  validation. Sanitize blocking-worker failures as 500 and closed admission as 503.
- Preserve learning semantics: installed vocabulary wins collisions, learned rules merge beneath
  it, and prior installed learned rules remain. Operators who need review or replacement use
  `POST /_vocab/learn` followed by an edited `PUT /_vocab`.

## Consequences

The operation is now an explicit, synchronous feature-model mutation rather than an unbounded async
handler. Clients get strict controls, stable mode-independent results, whole-route telemetry, and a
durability acknowledgement they can trust. Large live corpora are not given an arbitrary REST
cardinality ceiling: they are server-owned state and the engine is designed for them. Instead,
one-slot admission bounds concurrent memory/CPU amplification, and the operation remains
intentionally O(live corpus).

There is no execution timeout after admission. Cancelling the HTTP request cannot safely roll back a
partially persisted model change, so the worker finishes to a coherent terminal state and owns
publication. Operators should preview noisy learning configurations through the dry-run endpoint.

## Safety and proof

The existing learning/application primitives are unchanged. Synonym collapse still uses the same
normalizer on query and title sides; equivalence expansion remains widening-only; NPMI phrases remain
additive for component features while retaining the documented adjacency caveat for phrase-form
queries. The transport cannot weaken candidate retrieval or exact verification.

Standalone route tests pin method/query/body limits and deadlines, scalar validation, synchronous
timed results, `recompiled` identity, no-store telemetry, asynchronous admission, off-runtime engine
lock waits, closed admission, learned matching behavior, complete stale-state clearance, and
fail-loud durable degradation with coherent snapshot publication. Coordinator tests pin the same
response identity, admission behavior, validation, metrics, installed vocabulary, and matching
outcome over a real in-process multi-shard cluster. Existing standalone and cluster differential
oracles continue to prove synonym, phrase, and equivalence behavior after learn-and-apply.
