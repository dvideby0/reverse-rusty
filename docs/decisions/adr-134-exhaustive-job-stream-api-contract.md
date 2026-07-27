# ADR-134: Exhaustive-job stream API contract — strict single-consumer NDJSON

> [Percolator parity decisions](areas/percolator-parity.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

- **Context.** ADR-114 established the bounded, terminally attested stream and its lifecycle
  correctness, but the HTTP route silently ignored arbitrary query strings and did not advertise
  its one allowed method when rejecting `HEAD`. Its standalone tests covered the delivery
  machinery without a dedicated production-route contract, and the coordinator route lacked the
  same boundary proof. Elasticsearch and OpenSearch asynchronous search retrieval returns one
  retained JSON result; neither has an equivalent for a one-consumer stream of provisional chunks
  committed by a terminal checksum
  ([Elasticsearch](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-async-search-get),
  [OpenSearch](https://docs.opensearch.org/latest/search-plugins/async/index/)). Mapping their
  retention controls or response shape onto this route would misstate its semantics.

- **Decision — native, strict HTTP boundary.** `GET /_percolate/jobs/{id}/stream` remains a native
  endpoint and accepts no query parameters. Any non-empty raw query is rejected with the standard
  structured 400 validation envelope before registry lookup or stream claim, including malformed
  encodings that must not be silently discarded. Every non-GET method is rejected before query
  validation or claim with 405 and `Allow: GET`; notably, a `HEAD` probe cannot consume the one
  receiver. Missing jobs return the standard `404 job_not_found` envelope, and a second GET
  returns `409 stream_already_claimed`.

- **Decision — response and frame contract.** A successful GET claims the receiver and returns
  `200`, `Content-Type: application/x-ndjson`, and `Cache-Control: no-store`. Every frame is one
  UTF-8 JSON object followed by `\n`. Match-chunk sequences are contiguous from zero, every member
  carries its ADR-114 idempotency key, and only the final completion frame commits the exact total,
  chunk count, snapshot generation, and checksum. The status endpoint—not this stream—carries the
  additive ES/OpenSearch-familiar async projections introduced by ADR-132.

- **Correctness boundary.** Method and query rejection happen before `take_stream`, while a 200
  response means the single claim has occurred. Dropping that response drops the receiver; if the
  terminal frame has not been dequeued, the worker fails and publishes no exact summary.
  Completion, cancellation, deadline, disconnect, and failure continue to linearize through
  ADR-114's terminal gate. The change does not alter matching, chunk construction, idempotency,
  backpressure, distributed ownership, durable formats, or wire formats.

- **Proof.** Standalone production-router tests pin strict-query and non-GET rejection before
  claim, the `Allow` header, standard 404/409 envelopes, exact newline framing and response
  headers, contiguous chunks and idempotency keys, terminal completion, and response-drop failure
  without a summary. A coordinator production-router test consumes the same NDJSON contract
  through the cluster handler. Existing exhaustive suites continue to prove bounded memory,
  backpressure, exact checksums, terminal arbitration, fail-closed shard behavior, and real-wire
  delivery.
