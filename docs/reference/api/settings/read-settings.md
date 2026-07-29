# `GET`/`HEAD /_settings` — Read live settings

> [Settings APIs](../settings.md) · [REST API hub](../../api.md)

Strict native runtime configuration (ADR-022/159), read from an immutable snapshot. The `settings`
object is the complete serialized `EngineConfig`; some fields also have server CLI flags, while
library-only construction fields are read-only here. `HEAD` performs the same read and returns its
representation headers without a body.

The startup-loaded ranking-profile registry is server serving configuration, not `EngineConfig`.
It is intentionally absent from this response and cannot be changed through `PUT /_settings`; see
the canonical [ranking settings contract](../../ranking.md#3-profile-file).

```bash
curl localhost:9200/_settings
```

Representative fields from the full response:

```json
{
  "settings": {
    "max_segments": 8,
    "holes_ratio_threshold": 0.3,
    "memtable_flush_threshold": 100000,
    "auto_compact_on_flush": true,
    "auto_compact_on_ingest": true,
    "compaction_reanchor": false,
    "data_dir": null,
    "wal_sync_on_write": false,
    "retain_source": true,
    "max_query_length": 10240,
    "max_query_clauses": 256,
    "max_anyof_group_size": 64,
    "max_tags": 65535,
    "broad_batch_size": 256,
    "broad_columnar": true,
    "hot_anchor_threshold": 0,
    "dedup_bodies": true,
    "cooperative_cancel": true,
    "max_percolate_batch": 10000,
    "accept_class_d": false,
    "compaction_fixed_cost": 1000.0
  }
}
```

Supported query controls:

- `include_defaults` (Boolean, default `false`) adds a `defaults` object with the same shape and
  built-in values.
- `flat_settings` (Boolean, default `false`) is accepted for ES/OpenSearch familiarity. Reverse
  Rusty's setting keys are already flat, so either value produces the same representation.

Unknown, duplicate, and malformed controls are rejected. The operation accepts no request body;
GET/HEAD transport is capped at 64 KiB with a 250 ms body deadline. Responses, including errors, are
`Cache-Control: no-store`, and serialization is bounded on the shared administrative worker rather
than the async request thread.

Coordinator mode returns its existing topology and assembled per-shard configuration shape:

```json
{
  "mode": "cluster",
  "shards": 8,
  "replication_factor": 1,
  "include_broad": false,
  "durable": true,
  "per_shard": { "max_segments": 8, "...": "complete EngineConfig" },
  "defaults": { "max_segments": 8, "...": "built-in EngineConfig defaults" }
}
```

`defaults` is present only with `include_defaults=true`. Coordinator lock waiting, cloning, and
serialization run off the async runtime under the same bounded administrative admission as other
configuration reads.

This API borrows the useful `include_defaults` and `flat_settings` controls, but it has no honest
Elasticsearch/OpenSearch path alias. Their `/_cluster/settings` response represents explicit
persistent/transient overrides, while Elasticsearch's
[bare `/_settings`](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-settings)
represents index settings. Reverse Rusty instead returns one effective typed engine configuration
and does not fabricate those resource or persistence tiers.
