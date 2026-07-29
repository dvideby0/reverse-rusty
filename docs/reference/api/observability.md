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

## `GET` / `HEAD /_cluster/state` — Authoritative control state

Coordinator mode exposes the small committed control-plane document used for membership, logical
position placement, and feature/ring identity:

```bash
curl 'localhost:9200/_cluster/state'
```

```json
{
  "version": 0,
  "epoch": 0,
  "nodes": [{"id": 0, "addr": null, "role": "Data"}],
  "voters": [0],
  "assignments": [{"position": 0, "primary": 0, "replicas": []}],
  "num_shards": 1,
  "vnodes": 128,
  "dict_fingerprint": 123,
  "model_version": 0,
  "placement_generation": 1
}
```

`version` is an exact familiar alias for `epoch`, the monotonically committed application-state
version. It is not the local checkpoint epoch, the Raft term/log index, `model_version`, or
`placement_generation`. The other fields retain their native meanings:

| Field | Meaning |
|---|---|
| `nodes` | Registered logical nodes, optional transport addresses, and data/manager eligibility |
| `voters` | Current Raft manager voter ids |
| `assignments` | One logical position's committed primary and replica node ids |
| `num_shards`, `vnodes` | Ring parameters |
| `dict_fingerprint`, `model_version` | Frozen feature-model identity and model transition counter |
| `placement_generation` | Logical row-placement identity, changed only by model/ring rebuilds |

The exact Elasticsearch/OpenSearch version selector is available without transferring the rest of
the document:

```bash
curl 'localhost:9200/_cluster/state/version'
```

```json
{"version": 7}
```

`/_cluster/state/_all` is equivalent to the base path. `local=false` is accepted;
`local=true` is rejected because every successful response comes from the authoritative,
linearizable control plane. `cluster_manager_timeout` and `master_timeout` are mutually exclusive
aliases. Positive values bound admission plus the read (default and maximum 30 seconds). `0` is a
non-queuing probe: it returns 408 if shared introspection admission is occupied; when admitted, it
executes one authoritative read off the request worker. It is not a cancellation deadline for that
already-started synchronous read. `flat_settings` is accepted but representation-neutral because
this document contains no settings section.

Other ES/OpenSearch metric or index-target paths, metadata-version waiting, and index-expansion
controls fail with a validation error. Reverse Rusty has no index metadata, mapping, index-shard
routing table, state UUID, or local coordinator state to return, so those shapes are not
fabricated.

The route accepts only GET/HEAD and an empty body, with a 64 KiB request ceiling and 250 ms body-read
deadline. It shares the single stats/introspection work slot; lock waiting, a remote linearizable
RPC, and JSON serialization run off Tokio request workers. Responses are capped at 8 MiB and always
carry `Cache-Control: no-store`. Backend details stay in server logs when a control read fails.
HEAD performs the same availability check and returns no body.

## `GET` / `HEAD /_health` — Native readiness

```bash
curl 'localhost:9200/_health?wait_for_status=green&timeout=30s'
```

Standalone response:

```json
{
  "status": "green",
  "mode": "standalone",
  "timed_out": false,
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
| `red` | Single-node WAL/persistence failure, or a required cluster shard/control/topology check failed |

Cluster health uses a deliberately smaller native payload:

```json
{
  "status": "green",
  "mode": "cluster",
  "timed_out": false,
  "shards": 8,
  "pending_repairs": 0
}
```

A yellow or red response also includes `reason`. Detailed shard/control-plane errors are logged but
the unauthenticated response uses a stable generic red reason.

The route is a strict, bodyless GET/HEAD with a 64 KiB extraction ceiling. It rejects unknown query
parameters and unsupported values, returns structured 400/405/413 errors, sends
`Allow: GET, HEAD` on 405, strips the body for HEAD, and includes `Cache-Control: no-store` on every
response. `GET` and `HEAD` remain open even with `--auth-protect-reads` so orchestrator probes do
not need bearer credentials. Admission occurs before body buffering and allows at most eight
concurrent health requests; additional work fails immediately with 429, `Retry-After: 1`, and a
structured `rejected_execution_exception`. The cap therefore also covers slow request bodies, and
body buffering has an independent 250 ms deadline. A body that does not complete by then returns
408 with a structured `request_timeout`, releasing its health permit. Health duration telemetry
starts before method validation, admission, and body extraction, so it includes all transport work
and rejections as well as successful probes and status waits.

Supported query controls:

| Control | Contract |
|---|---|
| `wait_for_status=red\|yellow\|green` | Wait until the observed status reaches at least this ordered color; yellow accepts yellow or green |
| `timeout=<time>` | Bound the wait, stats admission, and coordinator probe result wait; default `30s`; units are `nanos`, `micros`, `ms`, `s`, `m`, `h`, or `d` |
| `level=cluster` | Accepted familiar spelling for this cluster-level native response; index/shard levels are rejected |

Green and yellow return HTTP 200. Red returns 503. If coordinator collection cannot complete by the
deadline, or `wait_for_status` is not reached, the latest response returns 408 with
`"timed_out":true`. The response preserves the last completed observation rather than replacing it
with a synthetic failure at the deadline, and a status first seen after the deadline remains timed
out. The coordinator rechecks the wall clock after each blocking result rather than relying only on
the async timeout race. Explicit status waits and dependency-probe deadlines have distinct stable
reasons. A coordinator request that times out cannot forcibly stop already-running blocking/network
work; that work retains its single shared stats permit until its own transport bounds complete.

Coordinator green requires a successful committed control-state read, a count from every logical
serving position, matching committed/ring shard counts, and exactly one in-range committed
assignment per position. The probe shares bounded stats admission with `/_stats` and CAT stats and
runs off the async request workers.

This is deliberately not Elasticsearch/OpenSearch `/_cluster/health`. Those APIs describe Lucene
index-shard allocation; Reverse Rusty has no honest equivalent for their index, active-primary,
relocating, or unassigned-shard fields, so no `/_cluster/health` alias is exposed (ADR-144).

## `GET` / `HEAD /_metrics` — Prometheus metrics

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
