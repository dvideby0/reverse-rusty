# ADR-157: Alias-feedback reset REST API contract

> [Engine, errors, dependencies & ops decisions](areas/engine-quality-and-operations.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

`POST /_vocab/aliases/feedback/reset` starts a new ADR-103 behavioral-evidence
measurement window. The original handler cleared the entire tracked-pair
aggregator and then published a new engine snapshot solely to rediscover the
same candidate pairs. Query strings and request bodies were silently ignored
under the server-wide 100 MiB limit. The route had no body deadline,
administrative admission, fixed telemetry, cache policy, method fallback, or
standalone tests. Coordinator mode returned a generic, unobserved 501 without
validating the shared request contract.

Clearing the pair universe also created a gap between the clear and snapshot
publication in which concurrent observations had no tracked pairs. A
concurrent publish could repopulate the universe during that gap, allowing
new-window evidence to be retained by the reset's later re-sync. The endpoint
therefore lacked one unambiguous evidence-window boundary.

[Elasticsearch's delete-synonym-set API](https://www.elastic.co/guide/en/elasticsearch/reference/current/delete-synonyms-set.html)
deletes one configured rule set and refuses a set used by an analyzer.
[OpenSearch synonym filters](https://docs.opensearch.org/latest/analyzers/token-filters/synonym/)
likewise configure explicit analyzer rules. Neither operation clears passive
behavioral evidence while retaining its governed alias candidates, so a
`DELETE /_synonyms/...` alias would misstate this endpoint.

## Decision

- Keep native, query-free `POST /_vocab/aliases/feedback/reset`; do not add an
  Elasticsearch/OpenSearch path or `DELETE` alias.
- Require an empty request body. Bound extraction at 64 KiB and 250 ms. Reject
  other methods with 405 and `Allow: POST`.
- Wait asynchronously for the shared one-slot administrative permit. Move
  feedback-lock acquisition and the bounded evidence clear to a blocking
  worker; the owned permit remains with that worker through completion.
- Clear counters and sketches in place while preserving each tracked pair,
  canonical ordering, and pre-tokenized forms. The feedback mutex defines the
  exact boundary: observations before the guard are cleared and observations
  after it enter the new window. Do not acquire the engine lock or publish an
  unchanged engine snapshot.
- Preserve `acknowledged: true` and add whole-route `took`, `took_ms`, the
  current `capture_enabled` setting, and `tracked_pairs`, the number of pair
  evidence records reset.
- Use the standard JSON error envelope and `Cache-Control: no-store` for every
  route-reached response. Count and time outcomes under the fixed
  `vocab_aliases_feedback_reset_post` label.
- Keep coordinator mode fail-loud with 501 and the single-node-replica
  alternative. Apply the same method, query, body-size, and body-deadline
  contract before returning the capability boundary, and observe every
  outcome with the same no-store telemetry.

## Consequences

Reset is now a single, linearizable measurement-window transition. Capture can
continue immediately against the same tracked universe without an empty-pair
gap, redundant registry scan, engine writer lock, or snapshot publication.
Because capture is concurrent, the response can already be followed by
new-window evidence; operators requiring a quiescent report must still pause
the title stream or disable capture around reset and read.

The endpoint remains native. Its familiar acknowledgement and timing fields
improve operational ergonomics without pretending that rolling match evidence
is an Elasticsearch or OpenSearch synonym resource.

## Safety and proof

Clearing evidence changes neither the alias registry nor active
normalization, compiled queries, candidate retrieval, exact verification,
durability, or the immutable engine snapshot. The lossless signature-cover
contract is therefore untouched.

Tests prove that every counter and sketch is cleared, pair order and
cardinality remain intact, and the next observation accumulates without a
snapshot publish. Standalone route tests also cover the timed no-store response,
strict method/query/body contract, body size and deadline, fixed telemetry,
asynchronous admission, closed admission, and feedback-lock waiting off the
async runtime. Coordinator tests cover shared validation and the observed
no-store 501 alternative.
