# Observability — REST API

> Part of the [REST API reference](../api.md). Query language: [`dsl.md`](../dsl.md).

## `GET /_stats` — Engine metrics (JSON)

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

## `GET /_cat/stats` — Engine metrics (human-readable)

```bash
curl localhost:9200/_cat/stats
```

This is a native Reverse Rusty endpoint; Elasticsearch and OpenSearch do not define a
`/_cat/stats` operation (ADR-141). The response nevertheless follows their common CAT table
mechanics where the meaning is exact. It accepts only a bodyless `GET`, has a 64 KiB body ceiling,
rejects unknown/invalid controls as structured errors, and marks all responses
`Cache-Control: no-store`.

```
took_ms                    0.187
mode                       standalone
queries.physical           4
queries.live               3
queries.tombstoned         1
segments.base              1
memtable.entries           0
features                   24
class.a                    2
class.b                    1
class.c                    0
class.d                    0
class.h                    0
memory.total_resident_bytes 4744
translog.operations        0
translog.size_in_bytes     0
segment.0.entries          4
segment.0.holes_percent    25.00
```

Every row has two columns, `metric` and `value`. Values are presentation strings; metrics ending in
`_bytes` are raw bytes. The complete table keeps the same native families as `/_stats`:
physical/live/tombstoned query rows, segment/memtable/feature counts, A/B/C/D/H classes, parse and
class-D rejections, would-be-hot and dedup telemetry, per-lane posting count/p50/p95/p99/max,
all resident-memory components plus their total, WAL/translog backlog, broad-lane settings, and
base-segment entry/hole rows. Collection uses the same one-slot admission and blocking worker as
`/_stats`; calls across the two endpoints cannot multiply the corpus-wide scan.

Common CAT controls:

| Control | Behavior |
|---|---|
| `v` or `v=true` | Add the `metric value` header to text; `v=false` leaves it off |
| `h=metric,value` | Select/reorder columns; aliases are `m` and `v`, and simple `*` wildcards work |
| `help` or `help=true` | Describe the columns without waiting for stats admission |
| `s=metric:desc,value:asc` | Lexically sort rows by one or more named/aliased columns |
| `format=json` | Return an array of selected string-valued fields |

```bash
curl 'localhost:9200/_cat/stats?format=json&h=metric,value&s=metric'
```

```json
[
  {"metric": "batch.max", "value": "10000"},
  {"metric": "broad.batch_size", "value": "256"},
  {"metric": "broad.materialize", "value": "true"}
]
```

`/_cat/stats` is single-node only. Cluster mode returns 501 with directions to `/_stats` or
`/_cat/shards`.

## `GET /_cat/segments` — Per-segment LSM detail

Per-segment introspection (ADR-023/142), read lock-free from one snapshot. This is a native LSM
projection, not a claim that Reverse Rusty has Lucene indices or shards. It nevertheless follows the
common [Elasticsearch CAT segments](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-segments)
and [OpenSearch CAT segments](https://docs.opensearch.org/latest/api-reference/cat/cat-segments/)
table mechanics wherever the meaning is exact.

The route accepts only a bodyless `GET`, has a 64 KiB body ceiling, rejects unknown or invalid
controls as structured errors, and marks every response `Cache-Control: no-store`. Text is the
default and, like ES/OpenSearch CAT output, has no header unless `v` is enabled. The final row
(`kind=memtable`) is the active in-memory segment and is present even when empty.

```bash
curl 'localhost:9200/_cat/segments?v'
```

```
segment kind     entries docs.count docs.deleted holes.percent vocab.epoch stale size.memory memory.payload memory.overhead
      0 mmap        1000        996            4         0.40%           0 false         48kb              0             48kb
      1 memtable     128        128            0         0.00%           0 false         60kb           52kb              8kb
```

| Column | Aliases | Meaning |
|---|---|---|
| `segment` | `ordinal`, `seg` | Dense native LSM ordinal; base segments are oldest-first and the memtable is last |
| `kind` | `k` | `memory`, `mmap`, or `memtable` |
| `entries` | `e` | Physical rows: live plus tombstoned |
| `docs.count` | `alive`, `dc` | Live stored-query rows |
| `docs.deleted` | `deleted`, `dd` | Tombstoned rows awaiting compaction |
| `holes.percent` | `holes`, `holes_ratio`, `hp` | Tombstoned percentage of physical rows |
| `vocab.epoch` | `epoch`, `vocab_epoch`, `ve` | Vocabulary epoch used to compile the segment |
| `stale` | `st` | Whether the segment predates the live vocabulary epoch |
| `size.memory` | `memory`, `sm` | Saturating sum of attributed resident payload and overhead bytes |
| `memory.payload` | `resident`, `resident_bytes`, `mp` | Exact/index/filter payload heap; zero for mmap-backed payloads |
| `memory.overhead` | `overhead`, `overhead_bytes`, `mo` | Always-resident logical-index and liveness-overlay heap |

`docs.count`, `docs.deleted`, and `size.memory` use familiar CAT names because their meanings map
cleanly. The remaining columns stay native: Reverse Rusty has no honest values for the ES/OS
`index`, `shard`, `prirep`, node, Lucene generation/version, committed, searchable, compound, or
on-disk `size` fields. Index path selectors and cluster-state controls are therefore not
implemented rather than fabricated.

Common CAT controls:

| Control | Behavior |
|---|---|
| `v` or `v=true` | Add column headers to text; `v=false` leaves them off |
| `h=segment,docs.count` | Select/reorder columns; the aliases above and simple `*` wildcards work |
| `help` or `help=true` | Describe all columns without collecting segment rows |
| `s=docs.deleted:desc,segment` | Sort stably by one or more columns/aliases; numeric fields sort numerically |
| `format=json` | Return an array with selected canonical column names and string values |
| `bytes=b\|kb\|k\|mb\|m\|gb\|g\|tb\|t\|pb\|p` | Render memory columns as an integer count in that binary unit; the default chooses a human-readable binary unit |

```bash
curl 'localhost:9200/_cat/segments?format=json&bytes=b&h=segment,kind,docs.count,docs.deleted,size.memory&s=docs.deleted:desc'
```

```json
[
  {
    "segment": "0",
    "kind": "mmap",
    "docs.count": "996",
    "docs.deleted": "4",
    "size.memory": "49152"
  }
]
```

CAT JSON values are presentation strings, matching the ES/OpenSearch CAT convention. For stable
typed automation use `GET /_stats`; use `bytes=b` when raw byte strings are needed.

`/_cat/segments` is single-node only. A syntactically valid coordinator request returns 501 because
the coordinator does not own one coherent per-shard LSM snapshot. Use `GET /_cat/shards` for
position-level counts and assignments.

## `GET /_cat/shards` — Logical shard counts and assignments

This cluster-only native endpoint (ADR-143) reports one row per logical Reverse Rusty shard
position. It follows the common
[Elasticsearch CAT shards](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-shards)
and [OpenSearch CAT shards](https://docs.opensearch.org/latest/api-reference/cat/cat-shards/)
table mechanics where the meaning is exact, but it does not claim to expose their per-index,
per-primary/replica-copy Lucene rows.

The route accepts only a bodyless `GET`, has a 64 KiB body ceiling, rejects unknown or invalid
controls as structured errors, and marks every response `Cache-Control: no-store`. Text is
headerless unless `v` is enabled:

```bash
curl 'localhost:9200/_cat/shards?v'
```

```
shard queries nodes
    0    1301 1+2
    1    1279 2+3
```

| Column | Aliases | Meaning |
|---|---|---|
| `shard` | `s`, `sh`, `position` | Native logical shard position |
| `queries` | `q`, `count` | Physical stored-query rows, including tombstones and content-driven multi-position copies |
| `nodes` | `n`, `assignment` | Committed primary node id followed by `+`-separated replica node ids |

`queries` is deliberately not called or aliased `docs`: it is not an ES/OS live-document count.
`nodes` is the committed desired assignment, not a live per-replica readiness attestation.

Common CAT controls:

| Control | Behavior |
|---|---|
| `v` or `v=true` | Add column headers to text; `v=false` leaves them off |
| `h=shard,queries` | Select/reorder columns; the aliases above and simple `*` wildcards work |
| `help` or `help=true` | Describe all columns without probing shards or waiting for stats admission |
| `s=queries:desc,shard` | Sort stably by one or more columns/aliases; counts sort numerically |
| `format=json` | Return an array with selected canonical column names and string values |

```bash
curl 'localhost:9200/_cat/shards?format=json&h=shard,queries,nodes&s=queries:desc'
```

```json
[
  {"shard": "0", "queries": "1301", "nodes": "1+2"},
  {"shard": "1", "queries": "1279", "nodes": "2+3"}
]
```

Collection shares the single blocking stats admission slot with `/_stats` and `/_cat/stats`.
Every logical position must answer and the committed topology must contain exactly one in-range
assignment for every position. A shard or control-plane failure, a ring/topology size mismatch, or
a missing/duplicate assignment fails the entire response; no successful partial table or
fabricated `-` assignment is returned. Counts and the assignment document are separately observed
inside that job rather than presented as a transactional distributed snapshot.

Index selectors plus the ES/OS `bytes`, `time`, `local`, and cluster-manager timeout controls are
unsupported because Reverse Rusty has no exact corresponding index namespace, storage/time
columns, or state-read mode. Stable typed automation should combine `GET /_stats` with
`GET /_cluster/state`.

## `GET /_health` — Health check

```bash
curl localhost:9200/_health
```

```json
{
  "status": "green",
  "total_queries": 3,
  "wal_healthy": true,
  "persistence_healthy": true,
  "skipped_segments": 0,
  "stale_segments": 0
}
```

| Status | Meaning |
|---|---|
| `green` | Single-node durability is healthy, or every cluster position answers with no queued repair |
| `yellow` | Single-node load skipped/stale segments, or cluster partial applies are queued for resync |
| `red` | Single-node WAL/persistence failure, or a cluster position cannot answer; cluster mode returns HTTP 503 |

Cluster health uses a different, deliberately smaller payload:
`{"status":"green","mode":"cluster","shards":8,"pending_repairs":0}`. A yellow or red response also
includes `reason`.

## `GET /_metrics` — Prometheus metrics

```bash
curl localhost:9200/_metrics
```

Returns metrics in Prometheus text exposition format for scraping by Prometheus, Grafana Agent, or
compatible collectors — engine gauges, event counters, per-endpoint HTTP latency, an in-flight-request
gauge, WAL size/pending gauges, cumulative flush/compaction-time counters, a
`durability_failures_total{op}` counter (ADR-021), and — when bearer-token auth is enabled — an
`auth_failures_total{reason="missing"|"invalid"}` counter for rejected requests (ADR-062).

In cluster-coordinator mode this same route refreshes cluster-wide query, per-position, and gRPC
transport series. A standalone `shardserver` or `controlserver` exposes its lean node metrics on the
separate address configured with `--metrics-addr`; that is not the coordinator's REST
`/_metrics` route.

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
