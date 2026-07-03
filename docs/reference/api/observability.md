# Observability — REST API

> Part of the [REST API reference](../api.md). Query language: [`dsl.md`](../dsl.md).

## `GET /_stats` — Engine metrics (JSON)

```bash
curl localhost:9200/_stats
```

```json
{
  "total_queries": 3,
  "base_segments": 1,
  "memtable_entries": 0,
  "dict_features": 24,
  "rejected_parse": 0,
  "rejected_class_d": 0,
  "would_be_hot": 0,
  "class_counts": {"a": 2, "b": 1, "c": 0, "d": 0},
  "postings": {
    "main":  {"count": 3, "p50": 1, "p95": 1, "p99": 1, "max": 1},
    "broad": {"count": 0, "p50": 0, "p95": 0, "p99": 0, "max": 0}
  },
  "segment_sizes": [3],
  "segment_holes": [0.0],
  "memory": {
    "exact_bytes": 1024,
    "index_bytes": 2048,
    "filter_bytes": 512
  }
}
```

- **class_counts** — how many **stored** queries fell into each cost class. `d` counts the
  always-candidates stored under the `accept_class_d` lane (ADR-068) — zero unless the lane has
  accepted queries; rejected class-D queries are counted only in `rejected_class_d`
- **would_be_hot** — observe-first hot-tier telemetry (the Broad-Query Cost Program): accepted
  compiles since process start whose plan keeps a main-lane query whose deciding anchor's
  frequency is already ≥ the default hot-anchor threshold (1024) without a top-64 mask bit —
  the queries a frequency-threshold reclassification would move. Counts compile events (incl.
  WAL replay and vocab recompiles), resets on restart; also a Prometheus gauge on `/_metrics`
- **postings** — posting-length percentiles per candidate-index lane (nearest-rank, computed
  on demand across all segments + the memtable). A fat `main.max` against a modest `main.p99`
  is the top-64 rank-cliff fingerprint the hot tier targets (ADR-104)
- **segment_holes** — fraction of tombstoned entries per segment (drives compaction decisions)
- **memory** — breakdown of heap usage across the exact store, candidate index, and bloom filters

## `GET /_cat/stats` — Engine metrics (human-readable)

```bash
curl localhost:9200/_cat/stats
```

```
queries          3
segments         1 (+ memtable: 0)
features         24
class A/B/C/D    2 / 1 / 0 / 0
rejected parse   0
rejected classD  0
would-be hot     0
postings main    3 sigs (p50 1 p95 1 p99 1 max 1)
postings broad   0 sigs (p50 0 p95 0 p99 0 max 0)
memory           3584 bytes (~0.0 MB)
broad lane       columnar (batch_size 256, materialize true, prefilter true, max_batch 10000)

segment  entries  holes
0        3        0.00%
```

## `GET /_cat/segments` — Per-segment LSM detail

Per-segment introspection (ADR-023), read lock-free from the snapshot. Default is a text table; pass
`?format=json` for machine-readable rows. The final row (kind `memtable`) is the active in-memory
segment.

```bash
curl localhost:9200/_cat/segments
```

```
segment  kind       entries     alive   deleted   holes  epoch stale     resident     overhead
0        mmap           1000       996         4   0.40%      0    no    412.00 KB     48.00 KB
1        memtable        128       128         0   0.00%      0    no     52.00 KB      8.00 KB
```

Columns: `kind` (memory / mmap / memtable), `entries` (live + tombstoned), `alive`, `deleted`,
`holes` (tombstone fraction), `epoch` (vocab epoch), `stale` (built against an older vocab), and a
`resident` vs `overhead` byte split.

```bash
curl 'localhost:9200/_cat/segments?format=json'
```

```json
[
  {
    "ordinal": 0,
    "kind": "mmap",
    "entries": 1000,
    "alive": 996,
    "deleted": 4,
    "holes_ratio": 0.004,
    "vocab_epoch": 0,
    "stale": false,
    "resident_bytes": 421888,
    "overhead_bytes": 49152
  }
]
```

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
| `green` | All systems healthy |
| `yellow` | Some segments were skipped on load, or some are vocab-stale (data may be incomplete) |
| `red` | WAL or persistence subsystem is unhealthy |

## `GET /_metrics` — Prometheus metrics

```bash
curl localhost:9200/_metrics
```

Returns metrics in Prometheus text exposition format for scraping by Prometheus, Grafana Agent, or
compatible collectors — engine gauges, event counters, per-endpoint HTTP latency, an in-flight-request
gauge, WAL size/pending gauges, cumulative flush/compaction-time counters, a
`durability_failures_total{op}` counter (ADR-021), and — when bearer-token auth is enabled — an
`auth_failures_total{reason="missing"|"invalid"}` counter for rejected requests (ADR-062).

