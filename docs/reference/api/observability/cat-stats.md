# `GET /_cat/stats` — Engine metrics (human-readable)

> [Observability APIs](../observability.md) · [REST API hub](../../api.md)

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
