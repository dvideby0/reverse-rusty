# `GET /_cat/segments` — Per-segment LSM detail

> [Observability APIs](../observability.md) · [REST API hub](../../api.md)

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
