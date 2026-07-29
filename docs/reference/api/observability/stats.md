# `GET /_stats` — Engine metrics (JSON)

> [Observability APIs](../observability.md) · [REST API hub](../../api.md)

```bash
curl localhost:9200/_stats
```

The route is a native Reverse Rusty operational snapshot despite sharing its path with the
Elasticsearch/OpenSearch index-stats API (ADR-140). It accepts exactly a bodyless, query-free
`GET`; index selectors, metric paths, and controls such as `level` are not implemented and fail
loudly rather than being ignored. Responses are marked `Cache-Control: no-store`.

```json
{
  "took": 0,
  "took_ms": 0.187,
  "_shards": {"total": 1, "successful": 1, "failed": 0},
  "mode": "standalone",
  "total_queries": 4,
  "live_queries": 3,
  "tombstoned_queries": 1,
  "base_segments": 1,
  "memtable_entries": 0,
  "dict_features": 24,
  "rejected_parse": 0,
  "rejected_class_d": 0,
  "would_be_hot": 0,
  "dedup": {
    "bodies_total": 3,
    "dup_joined": 0,
    "distinct_bodies_est": 3
  },
  "class_counts": {"a": 2, "b": 1, "c": 0, "d": 0, "h": 0},
  "postings": {
    "main":  {"count": 3, "p50": 1, "p95": 1, "p99": 1, "max": 1},
    "broad": {"count": 0, "p50": 0, "p95": 0, "p99": 0, "max": 0},
    "hot":   {"count": 0, "p50": 0, "p95": 0, "p99": 0, "max": 0}
  },
  "segment_sizes": [3],
  "segment_holes": [0.0],
  "memory": {
    "exact_bytes": 1024,
    "index_bytes": 2048,
    "filter_bytes": 512,
    "dict_bytes": 768,
    "query_store_bytes": 256,
    "logical_index_bytes": 128,
    "alive_bytes": 8,
    "total_resident_bytes": 4744
  },
  "translog": {"operations": 0, "size_in_bytes": 0}
}
```

- **took / took_ms / _shards** — whole-request time (including asynchronous admission) and the
  familiar successful-shard projection. A standalone engine is one shard
- **total_queries / live_queries / tombstoned_queries** — physical rows retained by the LSM,
  tombstone-aware live rows, and retained dead rows respectively; therefore
  `total_queries = live_queries + tombstoned_queries`. `total_queries` is intentionally preserved
  for compatibility and is not a live document count
- **class_counts** — how many physical stored rows, including tombstones, fell into each cost
  class. `d` counts the always-candidates stored under the `accept_class_d` lane (ADR-068) — zero
  unless the lane has accepted rows; rejected class-D queries are counted only in
  `rejected_class_d`. `h` counts the hot tier (class H, ADR-105 — θ-hot-anchored,
  always-visible, columnar-evaluated) — zero while `hot_anchor_threshold` is off
- **would_be_hot** — observe-first hot-tier telemetry (the Broad-Query Cost Program): accepted
  compiles since process start whose plan keeps a main-lane query whose deciding anchor's
  frequency is already ≥ the default hot-anchor threshold (1024) without a top-64 mask bit —
  the queries a frequency-threshold reclassification would move. Counts compile events (incl.
  WAL replay and vocab recompiles), resets on restart; also a Prometheus gauge on `/_metrics`
- **dedup** — canonical-body dedup telemetry (Stage A, ADR-106): `bodies_total` (accepted
  compiles since process start), `dup_joined` (compiles that joined an existing per-segment
  body group — what sharing actually captured), and `distinct_bodies_est` (a linear-counting
  estimate of GLOBAL distinct bodies — the cross-segment duplication Stage A cannot reach; the
  Stage B sizing instrument). All three are also Prometheus gauges on `/_metrics`
  (`dedup_bodies_total`, `dedup_joined`, `dedup_distinct_bodies_est`)
- **postings** — posting-length percentiles per candidate-index lane (nearest-rank, computed
  on demand across all segments + the memtable). A fat `main.max` against a modest `main.p99`
  is the top-64 rank-cliff fingerprint the hot tier targets (ADR-104). This and the persisted
  class-column tally are corpus-wide work: one stats job is admitted per server, excess calls
  wait asynchronously, and the scan runs off Tokio request workers
- **segment_holes** — fraction of tombstoned entries per segment (drives compaction decisions)
- **memory** — resident-byte breakdown across exact predicates, candidate indexes, filters,
  dictionary, retained source store, logical-id indexes, and liveness overlays.
  `total_resident_bytes` is the saturating sum of those seven fields; file-backed mmap pages are
  not counted as resident heap
- **translog** — ES/OpenSearch-familiar names for the native WAL backlog:
  `operations` is the number of mutations since the last checkpoint and `size_in_bytes` is the
  current WAL file size. Both are zero for an in-memory engine

This is the single-node shape. Cluster mode returns its coordinator-level shape instead:

```json
{
  "took": 2,
  "took_ms": 2.731,
  "_shards": {"total": 8, "successful": 8, "failed": 0},
  "mode": "cluster",
  "shards": 8,
  "replication_factor": 1,
  "include_broad": false,
  "durable": true,
  "total_queries": 10342,
  "shard_queries": [1301, 1279, 1290, 1287, 1304, 1288, 1296, 1297],
  "class_counts": {"a": 9120, "b": 917, "c": 280, "d": 5, "h": 20},
  "epoch": 4,
  "pending_repairs": 0,
  "has_tagged_queries": true
}
```

`shard_queries` counts stored physical rows per logical position, so a query replicated or placed
on multiple positions contributes to each holder; the coordinator reports the primary view, not
extra replica copies. `total_queries` is the sum of this array and `class_counts` is the
corresponding physical-row tally, both including tombstones. These are placement/capacity
signals—not a distinct live logical-query count. `pending_repairs > 0` means a partial cluster
mutation is queued for `POST /_cluster/resync`.

Cluster collection performs one count request and one class-count request per logical position in
the single admitted blocking job. If any required position fails, the entire request fails loudly;
it never returns a successful partial `_shards` result.
