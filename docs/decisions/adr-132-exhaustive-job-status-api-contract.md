# ADR-132: Exhaustive-job status API contract — strict async polling

> [Percolator parity decisions](areas/percolator-parity.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

- **Context.** ADR-114 established a correct retained lifecycle and ADR-131 made job creation
  strict and familiar. The status boundary still returned only native `job_id`,
  `created_unix_ms`, and a failure string, silently ignored every query parameter, and supplied no
  cache policy. Elasticsearch async status exposes identity, running/partial state, start and
  completion times, and structured errors; OpenSearch asynchronous result polling accepts
  `wait_for_completion_timeout`
  ([Elasticsearch](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-async-search-status),
  [OpenSearch](https://docs.opensearch.org/latest/search-plugins/async/index/)).

- **Decision — one native/familiar response superset.** `GET /_percolate/jobs/{id}` preserves
  `job_id`, `event_id`, lowercase `state`, scope, generation, native timestamps, exact summary,
  checksum, and `failure`. It adds `id == job_id`, `is_running`, `is_partial`,
  `start_time_in_millis`, and terminal `completion_time_in_millis`. Running is partial; only a
  terminally attested `completed` record is non-partial. Failed and cancelled records remain
  partial and add a structured top-level `error` while preserving the native diagnostic. Responses
  are `Cache-Control: no-store`. No expiration time or completion HTTP status is invented because
  count-based pruning is not a time expiry and the streamed result has no retained HTTP response
  status.

- **Decision — strict bounded polling.** The query string is typed and rejects unknown fields,
  duplicate scalars, malformed types, and invalid time values with the standard 400 envelope.
  Omission or `wait_for_completion_timeout=0s` returns the current view immediately. A positive
  integer time value (`nanos|micros|ms|s|m|h|d`) waits until terminal publication or that duration,
  bounded by the configured exhaustive-job maximum. Waiting subscribes to the retained record
  before sleeping and holds that record across count-based pruning, so a successful lookup cannot
  become a spurious 404 mid-poll. `keep_alive` is recognized only to return an explanatory 400:
  this in-memory registry has bounded count retention, not client-selected expiry.

- **Exact-delivery boundary.** Status waiting never claims or reads the single-consumer NDJSON
  stream. `completed` is still published only after that stream dequeues its completion frame.
  Therefore a poll with no concurrent stream consumer legitimately times out as `running`; it
  cannot weaken ADR-114 by turning queued terminal bytes into an exact result. Cancellation,
  deadline, disconnect, first-failure arbitration, checksums, idempotency, admission, cluster
  fencing, auth, wire formats, and durable formats are unchanged.

- **Proof.** Standalone production-router tests cover immediate running aliases, no-store caching,
  early terminal wakeup after completion dequeue, exact summary/timing projection, structured
  cancellation, zero waits, malformed/over-limit waits, unsupported retention, unknown fields,
  and the standard 404 envelope. Coordinator routing proves the same status shape and wait control.
  Existing exhaustive lifecycle tests continue to prove terminal linearization, disconnect
  failure, deadline/cancellation arbitration, bounded delivery, and exact completion.

- **Later deletion contract.** ADR-133 keeps a DELETE-requested running job retained and pollable
  through this status surface, then lets a later DELETE atomically remove the terminal record and
  release its event id.
