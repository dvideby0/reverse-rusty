# ADR-145: Metrics REST API contract

> [Engine, errors, dependencies & ops decisions](areas/engine-quality-and-operations.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

The standalone and coordinator `GET /_metrics` routes exposed useful Prometheus families but still
had prototype transport and collection semantics. Both silently accepted query parameters and
request bodies, inherited the global 100 MiB ingest ceiling, relied on router-generated method
errors, omitted cache controls, and did not count or time the metrics route itself. A client could
therefore stream a body to an otherwise bodyless operational endpoint for an unbounded duration.

Coordinator collection synchronously held the cluster read guard and made potentially remote shard
calls on a Tokio request worker. It fetched shard counts twice, silently ignored a failed count
pass, and then encoded whatever gauge values remained from an earlier successful scrape. The
per-position gauge only inserted label values, so a topology shrink could leave removed shard
positions visible forever. Finally, unsigned engine and transport values used unchecked casts into
Prometheus signed gauges and could wrap negative at extreme values.

[Elasticsearch node stats](https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-nodes-stats.html)
and [OpenSearch node stats](https://docs.opensearch.org/latest/api-reference/nodes-apis/nodes-stats/)
return product-specific JSON under `/_nodes/stats`. Reverse Rusty's route is a Prometheus registry
whose engine, LSM, matching, delivery, and transport families do not map honestly to those Lucene
node-stat groups. Prometheus text exposition 0.0.4 has a defined content type, while exporter
guidance permits a 5xx when a scrape cannot be completed and warns that direct label-vector updates
can retain label values that disappear from the source
([exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/),
[exporter guidance](https://prometheus.io/docs/instrumenting/writing_exporters/)).

## Decision

- Keep the native `/_metrics` path. Do not add `/_nodes/stats` or fabricate Elasticsearch/OpenSearch
  node-stat fields. Serve Prometheus text 0.0.4 with
  `Content-Type: text/plain; version=0.0.4; charset=utf-8`.
- Accept only query-free, bodyless GET and HEAD. Give the route a 64 KiB extraction ceiling and a
  separate 250 ms body-read deadline. Return structured 400/405/408/413 errors, include
  `Allow: GET, HEAD` on method rejection, remove the body from every HEAD response, and attach
  `Cache-Control: no-store` to every terminal outcome.
- Account for every outcome under the fixed `metrics` endpoint label. Start duration measurement
  before method, query, and body validation so admission and all rejection paths are included. The
  outer authentication boundary applies the same accounting, no-store, and bodyless-HEAD
  finalization when protected reads reject before the route extractor. The current request is
  finalized after registry gathering and therefore appears only in a later scrape.
- In standalone mode, refresh engine gauges from one immutable lock-free snapshot before gathering
  the process-local registry.
- In coordinator mode, share the single stats-admission permit with `/_stats` and CAT stats. Move
  the cluster read guard and potentially remote probes to one blocking worker, fetch the complete
  per-position count vector once, derive its saturating aggregate locally, and snapshot transport
  metrics in that same collection.
- Publish coordinator gauges only after the complete collection succeeds. Return a sanitized
  `503 metrics_unavailable` if a required shard count cannot be collected; log the detailed source
  failure but do not claim a successful partial or fresh scrape. Keep the stats permit through
  gauge refresh and registry encoding so another corpus-wide stats operation cannot interleave
  publication.
- Replace the entire per-position label vector on every successful coordinator refresh, removing
  positions absent from the new snapshot. Clamp unsigned integer gauge values to `i64::MAX`
  instead of allowing a narrowing cast to wrap negative.
- Keep the low-level `shardserver` and `controlserver` metrics listeners as distinct lean node
  endpoints. Their separate-address contract is not changed by this coordinator REST audit.

## Consequences

Prometheus receives one deterministic, cache-safe content type and operators can use HEAD for a
bodyless contract check. Malformed or stalled input is bounded and observable. Coordinator success
now means every required serving position contributed to the same count pass, while a failure is
explicit and cannot masquerade as a successful scrape of stale shard gauges. Blocking network work
no longer occupies async request workers, and a topology shrink removes obsolete labels on the
next successful scrape.

The route remains deliberately native rather than an Elasticsearch/OpenSearch node-stats
compatibility endpoint. A caller waiting for shared stats admission has no separate metrics-route
timeout, and an already-running blocking collection relies on the transport's own network bounds.

## Safety and proof

Metrics collection is read-only and does not participate in candidate retrieval or exact
verification, so the lossless signature-cover contract is unchanged. Standalone and distributed
route tests pin the text content type, no-store behavior, GET/HEAD parity, strict query/body/method
handling, body size and read deadlines, whole-route request telemetry, asynchronous shared
admission, sanitized fail-loud collection, internally consistent single-pass aggregation, stale
label removal, and saturating gauge conversion.
