# ADR-133: Exhaustive-job delete API contract — cancel running, remove terminal

> [Percolator parity decisions](areas/percolator-parity.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

- **Context.** ADR-114 introduced `DELETE /_percolate/jobs/{id}` as cooperative cancellation, but
  the route returned only a pre-cancellation native status view, silently accepted every query
  parameter, and did nothing at all to a terminal record. That made a method named DELETE unable
  to release retained results or the event id that guards idempotent reuse. Elasticsearch and
  OpenSearch asynchronous search deletion return `acknowledged` and distinguish cancellation of
  running work from deletion of saved results
  ([Elasticsearch](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-async-search-delete),
  [OpenSearch](https://docs.opensearch.org/latest/search-plugins/async/index/)).

- **Decision — truthful two-phase deletion.** A retained running job receives the existing
  cooperative cancellation request and remains addressable until the worker publishes its
  terminal phase. The response is `200`, `acknowledged: true`, and `deleted: false`; clients poll
  status until `cancelled` or another terminal phase, then DELETE again to release the record. A
  retained terminal job is removed atomically from both job and event-id indexes and decremented
  from the retained-phase gauge. Its final native status snapshot is returned with
  `acknowledged: true` and `deleted: true`; later status, stream, or delete requests receive the
  standard 404. Releasing the event index intentionally permits that event id to name a new
  request.

- **Decision — additive familiar response and strict input.** The response preserves every prior
  native `JobView` field, adds `id == job_id`, and adds Boolean `acknowledged` and `deleted`.
  Therefore existing consumers do not lose their immediate state view, while ES/OpenSearch clients
  receive the familiar acknowledgment and all clients can distinguish cancellation from actual
  removal. Success is `Cache-Control: no-store`. The route accepts no query controls; typed parsing
  rejects any query field or malformed encoding with the standard structured 400 envelope.

- **Race and correctness boundary.** Lookup, terminal-state inspection, record removal, event-id
  release, and retained-gauge adjustment serialize on the registry lock. Cancellation still
  linearizes through ADR-114's terminal gate against completion dequeue, deadline, disconnect, and
  prior failure. If completion wins just before a DELETE observes the still-running published
  state, that response truthfully says `deleted: false`; the terminal status becomes pollable and a
  later DELETE removes it. No running record is removed while its worker, bounded stream, permit,
  or cluster mutation barrier remains active. Matching, exact delivery, idempotency keys, durable
  and wire formats, and standalone/coordinator semantics are unchanged.

- **Proof.** Standalone production-router tests cover a running cancellation acknowledgment,
  terminal polling, terminal removal, repeated-delete 404, event-id reuse, no-store caching,
  strict query rejection, and the standard unknown-job envelope. Coordinator routing proves the
  same acknowledgment and native response shape. Existing lifecycle tests continue to prove
  cancellation/deadline/completion arbitration, bounded cancellation latency, stream attestation,
  admission, and registry pruning.
