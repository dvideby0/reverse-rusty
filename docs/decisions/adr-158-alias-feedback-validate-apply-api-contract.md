# ADR-158: Alias-feedback validate-and-apply REST API contract

> [Engine, errors, dependencies & ops decisions](areas/engine-quality-and-operations.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

`POST /_vocab/aliases/validate_and_apply` turns the passive match evidence from ADR-103 into
governed registry metadata and, only when the caller explicitly supplies `activate=true`, promotes
eligible candidates through the full query-recompile path. Rejected and mixed-kind entries remain
ineligible for automated activation.

The original REST handler silently clamped invalid overlap controls, accepted zero evidence
thresholds, ignored unknown query parameters and request bodies under the server-wide 100 MiB
limit, and had no body deadline or method fallback. It performed evidence reporting, source
lookups, engine-lock waits, and an optional O(corpus) recompile on a Tokio request worker without
bounded administrative admission. It always published a snapshot, including after failure and
identical retries, and returned no timing, persistence boundary, no-store header, or fixed endpoint
telemetry. Its coordinator 501 skipped the request contract entirely.

[Elasticsearch synonym-set writes](https://www.elastic.co/guide/en/elasticsearch/reference/current/put-synonyms-set.html)
replace explicit rules in a named set and can reload analyzers. OpenSearch configures explicit rules
through a [synonym token filter](https://docs.opensearch.org/latest/analyzers/token-filters/synonym/)
and exposes a [search-analyzer refresh](https://docs.opensearch.org/latest/im-plugin/refresh-analyzer/index/).
Neither operation evaluates passive reverse-query match populations, stamps behavioral evidence, or
conditionally promotes a governed review candidate.

## Decision

- Keep the native `POST /_vocab/aliases/validate_and_apply` path. Do not expose `/_synonyms` or an
  analyzer-refresh alias whose resource, input, and governance semantics would be false.
- Accept only `min_overlap`, `min_titles`, `min_queries`, and `activate` query parameters.
  `min_overlap` defaults to `0.5` and must be finite within `[0,1]`; title and query thresholds
  default to 50 and 20 and must be positive; activation defaults to false. Reject unknown,
  duplicate, malformed, and out-of-range parameters instead of coercing them.
- Keep the operation bodyless. Bound extraction at 64 KiB and 250 milliseconds. Unsupported
  methods return the standard error envelope with `Allow: POST`.
- Return whole-route `took` and `took_ms`, `acknowledged`, `result` (`updated` or `noop`),
  `persisted: false`, the exact controls, `validated`, `stamped`, `activated`, `recompiled`, and the
  resulting registry summary. `persisted: false` states that standalone runtime vocabulary changes
  are not written to the operator's startup vocabulary file.
- Make feedback stamping idempotent in the embedded registry: an identical evidence/confidence
  retry is not counted as a changed stamp. If neither metadata nor status changes, skip the
  vocabulary-Arc swap and snapshot publication.
- Wait asynchronously for the shared one-slot administrative permit. A blocking worker owns the
  permit while it snapshots the operator-bounded evidence set, resolves sampled query sources,
  computes validation, waits for the engine lock, mutates the registry, recompiles when activation
  changes matching, and publishes successful live state. Snapshot only the evidence under its
  mutex; perform source lookup and overlap calculation after releasing the capture lock.
- For an activation, refuse an already-unhealthy durable engine, require the recompiled count to
  equal the live source count, require no stale segment to remain, and check persistence health
  after the rebuild. A coherent live rebuild whose storage commit fails is published but returns
  503 and is not acknowledged.
- Keep coordinator mode fail-loud with 501 and the single-node validation plus `PUT /_vocab`
  alternative. Apply the same method, query, and body validation first.
- Make every route-reached result `Cache-Control: no-store` and count/time it under fixed
  `vocab_aliases_validate_and_apply` labels.

## Consequences

The API is strict, bounded, observable, and truthful about the difference between live
acknowledgement and operator-file persistence. Invalid evidence thresholds cannot be silently
weakened, expensive validation and activation do not block async request workers, identical retries
are real no-ops, and feature-model activation cannot claim success with stale or uncommitted query
state.

Operators still control matching changes explicitly. The default call records evidence and raises
confidence only; `activate=true` is required to widen matching. To survive a standalone restart,
save the resulting `GET /_vocab` document to the vocabulary file used on reopen.

## Safety and proof

Evidence calculation and the ADR-103 thresholds are unchanged. Metadata-only stamping preserves the
vocabulary epoch and every match result. Automated activation still acts only on a current
`Candidate`, refuses `Rejected` and `MixedKind`, and widens positive query requirements through the
shared equivalence expansion. The full recompile and stale-state checks preserve the lossless
signature-cover contract.

Standalone route tests cover timed no-store stamping, exact no-op retry, explicit activation,
cross-form matching, complete recompilation, strict controls/body/method/size/deadline handling,
fixed telemetry, asynchronous and closed admission, and off-runtime feedback/engine lock waits.
Coordinator tests cover the observed no-store 501, shared validation, telemetry, and method
fallback. Core tests cover identical evidence retry semantics; the existing differential oracle
continues to prove stamping is match-neutral and activation is widening-only.
