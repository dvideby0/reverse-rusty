# `POST /_compact` / `POST /_forcemerge` — Force compaction

> [Ingest & lifecycle APIs](../ingest.md) · [REST API hub](../../api.md)

`POST /_compact` is the strict native force-all operation. It merges every sealed base segment into
one, regardless of the background `max_segments` and `holes_ratio_threshold` policy:

```bash
curl -X POST localhost:9200/_compact
```

The mutable memtable remains a separate hot delta. The request takes no query parameters or body;
unknown input is a 400, a non-`POST` method is a structured 405 with `Allow: POST`, and the configured
body limit remains a 413.

When a merge runs:

```json
{
  "took": 3,
  "took_ms": 3.42,
  "acknowledged": true,
  "_shards": {
    "total": 1,
    "successful": 1,
    "failed": 0
  },
  "segments_merged": 2,
  "entries_before": 150,
  "entries_after": 142,
  "tombstones_reclaimed": 8,
  "reanchored": 0,
  "hot_promoted": 0,
  "hot_demoted": 0
}
```

`reanchored`, `hot_promoted`, and `hot_demoted` report the optional compaction-improvement work
described in ADR-056/105. When fewer than two sealed base segments exist, the force-all target is
already satisfied:

```json
{
  "took": 0,
  "took_ms": 0.08,
  "acknowledged": true,
  "_shards": {
    "total": 1,
    "successful": 1,
    "failed": 0
  },
  "message": "nothing to compact"
}
```

### Elasticsearch/OpenSearch force-merge spelling

The indexless `POST /_forcemerge` alias projects the controls Reverse Rusty can implement truthfully
onto its one implicit `queries` index:

| Query parameter | Default | Behavior |
|---|---:|---|
| `max_num_segments` | policy | Omit it to run one configured policy selection; `1` seals (when `flush=true`) and force-merges every base segment into one |
| `flush` | `true` | Seal the memtable under the same writer lock before merge selection; `false` leaves it as the mutable delta |
| `only_expunge_deletes` | `false` | `false` is accepted; `true` is rejected because Reverse Rusty does not expose the distinct Lucene expunge-only policy |
| `wait_for_completion` | `true` | `true` is accepted; `false` is rejected because there is no task API for a truthful asynchronous result |

For example:

```bash
curl -X POST 'localhost:9200/_forcemerge?max_num_segments=1&flush=true'
```

Values of `max_num_segments` other than `1`, named-index paths, index/alias/wildcard controls, and
unknown or duplicate parameters are structured 400s before any flush or merge. A bare
`/_forcemerge` follows the configured background policy and can answer
`"message": "no segment merge needed"`; use `max_num_segments=1` for the force-all result. The alias
returns the same response superset shown above, including the familiar `_shards` object.

Compaction runs on blocking maintenance work rather than an async runtime worker. The call waits by
default and writes serialize behind the engine writer lock, while already-published read snapshots
remain available. As with Elasticsearch/OpenSearch's synchronous force merge, losing the client
connection does not cancel work already admitted. Run a force-all merge during a quiet/off-peak
window because it rewrites the selected corpus.

If the engine's persistence is already degraded, no new maintenance mutation is attempted. If a
flush or compaction cannot durably commit, `/_compact` and `/_forcemerge` return
**`503 Service Unavailable`** with `"acknowledged": false`, `_shards.failed: 1`, and
`"message": "persistence degraded; compaction not durably acknowledged"`. A failed merge always
rolls back to its source segments, so it never loses data (ADR-051/138).

Both spellings are standalone only. Cluster mode returns 501; each shard engine runs its own
configured compaction policy, and `POST /_checkpoint` remains the cluster durability commit.
