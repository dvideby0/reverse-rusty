# `GET` / `HEAD /_metrics` — Prometheus metrics

> [Observability APIs](../observability.md) · [REST API hub](../../api.md)

```bash
curl localhost:9200/_metrics
curl --head localhost:9200/_metrics
```

This is a native Prometheus scrape route, not an Elasticsearch/OpenSearch node-stats alias. Both
those products expose JSON node statistics under `/_nodes/stats`; Reverse Rusty keeps `/_metrics`
because its registry is already Prometheus-shaped and its engine, LSM, percolation, and transport
families do not honestly map to Lucene node-stat groups (ADR-145). GET returns Prometheus text
exposition 0.0.4 with
`Content-Type: text/plain; version=0.0.4; charset=utf-8`; HEAD performs the same collection and
returns the same status and headers without a body. Successes and structured errors carry
`Cache-Control: no-store`.

The transport is deliberately strict. Query parameters and non-empty bodies return 400; methods
other than GET/HEAD return 405 with `Allow: GET, HEAD`; a body over the route's 64 KiB ceiling
returns 413; and a body that does not complete within 250 ms returns 408. The `metrics` HTTP request
counter and duration histogram include both success and every rejection, including a 401 produced
by `--auth-protect-reads` before collection begins. Since the registry is gathered before the
current response is finalized, a scrape reports completed earlier scrapes, not itself.

The exposition includes engine gauges, event counters, per-endpoint HTTP latency, an
in-flight-request gauge, WAL size/pending gauges, cumulative flush/compaction-time counters, a
`durability_failures_total{op}` counter (ADR-021), and — when bearer-token auth is enabled — an
`auth_failures_total{reason="missing"|"invalid"}` counter for rejected requests (ADR-062).

Standalone collection refreshes engine gauges from one lock-free snapshot. In
cluster-coordinator mode the route shares the single stats-admission slot with `/_stats` and CAT
stats, runs all potentially remote shard probes on a blocking worker, and derives the aggregate
from one complete per-position count pass. Any required-position failure returns a sanitized
`503 metrics_unavailable`; no partial or previously collected shard values are presented as a
successful fresh scrape. A successful refresh replaces the whole
`cluster_shard_queries{shard="N"}` label set, so positions removed by a shrink disappear from the
next scrape. Unsigned values that exceed Prometheus's signed integer gauge range saturate at
`i64::MAX` instead of wrapping negative.

A standalone `shardserver` or `controlserver` exposes its lean node metrics on the separate address
configured with `--metrics-addr`; that endpoint is distinct from the coordinator REST contract
audited here.

ADR-108 adds low-cardinality local bounded-ranking telemetry:
`ranked_requests_total{outcome,scope}`, `rank_total_relation_total{relation}`,
`rank_admission_rejections_total{reason}`, `rank_evaluations_total`,
`rank_heap_replacements_total`, `rank_source_bytes_total`,
`rank_true_match_lower_bound_total`, and the current `ranked_search_permits_in_use` gauge. Slow v2
logs include K, scope, total relation, the true-match lower bound, candidates, routed shards,
shard rows/result bytes, rank wall time, and cancellation outcome.

ADR-110 uses the same families for cluster `/v2/_search` and adds
`rank_shard_rows_received_total`, `rank_shard_result_bytes_total`, and
`rank_enrichment_rejections_total`. Coordinator transport families expose fixed method labels
`percolate_top_k` and `fetch_matches` for calls/errors/timeouts/retries/latency.

ADR-114 adds exhaustive-job/stream telemetry:

- `percolate_stream_chunks_total`
- `percolate_stream_bytes_total`
- `percolate_stream_backpressure_seconds_total`
- `percolate_jobs{state="running"|"completed"|"failed"|"cancelled"}`
- `percolate_jobs_total{outcome="completed"|"failed"|"cancelled"}`
- `exhaustive_permits_in_use`

The job gauge counts retained records by current state, so terminal series fall when retention
prunes them; the terminal counter never falls. Backpressure time includes a blocked send that
ultimately ends in cancellation, deadline, or disconnect. Coordinator transport metrics and shard
RPC latency use the additional fixed method label `percolate_all`.

On each shard node, `reverse_rusty_shard_rpc_duration_seconds{shard,method,le}` includes
`percolate_top_k`, `fetch_matches`, and `percolate_all`. The following fixed-cardinality counters
make bounded delivery and its fail-closed limits visible:

- `reverse_rusty_shard_top_k_hits_total{shard}`
- `reverse_rusty_shard_top_k_result_bytes_total{shard}`
- `reverse_rusty_shard_source_fetch_bytes_total{shard}`
- `reverse_rusty_shard_rank_total_relation_total{shard,relation="eq"|"gte"}`
- `reverse_rusty_shard_rank_cancellations_total{shard}`
- `reverse_rusty_shard_result_cap_rejections_total{shard}`
