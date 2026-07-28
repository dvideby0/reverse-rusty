# ADR-144: Health API contract

> [Engine, errors, dependencies & ops decisions](areas/engine-quality-and-operations.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

The standalone and coordinator `GET /_health` routes exposed different prototype contracts.
Standalone always returned HTTP 200, even for broken WAL or persistence state. Coordinator returned
503 for red, but synchronously probed shards on a Tokio request worker, did not verify the control
plane or assignment completeness, and exposed raw backend errors through an unauthenticated
response. Both routes ignored query strings and request bodies, inherited the global 100 MiB body
ceiling, relied on router fallbacks for method handling, omitted cache and route metrics, and had no
bounded health-wait primitive.

[Elasticsearch cluster health](https://www.elastic.co/guide/en/elasticsearch/reference/8.19/cluster-health.html)
and [OpenSearch cluster health](https://docs.opensearch.org/latest/opensearch/rest-api/cluster-health/)
use `/_cluster/health`, health-color ordering, `wait_for_status`, `timeout`, and `level`. Their
colors and detailed fields describe allocation of primary and replica Lucene index shards. Reverse
Rusty's health describes its own serving, durability, logical-position, and repair dependencies.
Adding that standard path or fabricating index-allocation fields would imply compatibility the
engine does not have.

## Decision

- Keep the native `/_health` path and payload. Do not add `/_cluster/health`. Adopt only the
  familiar controls that map exactly: ordered `wait_for_status=red|yellow|green`,
  time-valued `timeout` (30 seconds by default), and `level=cluster`. Reject unknown controls,
  unsupported values, non-empty bodies, and methods other than GET/HEAD.
- Give the route a 64 KiB extraction ceiling, structured 400/405/413 errors, `Allow: GET, HEAD`,
  bodyless HEAD responses, `Cache-Control: no-store`, and the low-cardinality `health` request and
  duration metric labels. GET and HEAD remain the intentionally unauthenticated readiness surface.
- Return `mode` and `timed_out` in both payloads. Green and yellow return 200, native red returns
  503, and an expired coordinator observation or unmet `wait_for_status` returns 408 with
  `timed_out=true`. Health colors are ordered, so waiting for yellow accepts yellow or green.
- Standalone red means WAL or persistence failure. Yellow means one or more skipped or stale
  segments while durability remains healthy. Green means those serving and durability indicators
  are healthy. The lock-free engine snapshot remains the observation source.
- Coordinator health must successfully read the committed control state and one physical count
  from every logical shard position. The committed shard count must equal the serving-ring count,
  with exactly one in-range assignment per position. Any probe or topology-validation failure is
  red; queued partial-apply repairs are yellow; otherwise health is green.
- Coordinator collection shares the single stats admission permit and executes its potentially
  remote probes on a blocking worker. The request deadline bounds admission and waiting for the
  blocking result; already-running blocking/network work is not forcibly interrupted when the HTTP
  wait ends.
- Log detailed coordinator failures, but return only a stable generic red reason. The endpoint is
  intentionally unauthenticated, so hostnames, transport messages, control-plane details, and
  credentials embedded in an upstream error must not cross its response boundary.

## Consequences

Operators get one strict readiness contract in standalone and coordinator modes, including a
familiar way to wait during rollout without polling client-side. Red is now reliably fail-loud at
the HTTP layer, and coordinator green attests to both serving positions and a complete committed
topology rather than only successful count probes.

The endpoint remains deliberately native. ES/OpenSearch clients that require
`/_cluster/health` allocation fields cannot treat it as a drop-in replacement. A timed-out
coordinator request can leave a detached blocking probe running until its transport-level bounds
complete; its stats permit remains held during that work, preventing unbounded fan-out.
