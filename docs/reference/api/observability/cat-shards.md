# `GET /_cat/shards` — Logical shard counts and assignments

> [Observability APIs](../observability.md) · [REST API hub](../../api.md)

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
