# Settings — REST API

> Part of the [REST API reference](../api.md). Query language: [`dsl.md`](../dsl.md).

## `GET`/`HEAD /_settings` — Read live settings

Strict native runtime configuration (ADR-022/159), read from an immutable snapshot. The `settings`
object is the complete serialized `EngineConfig`; some fields also have server CLI flags, while
library-only construction fields are read-only here. `HEAD` performs the same read and returns its
representation headers without a body.

The startup-loaded ranking-profile registry is server serving configuration, not `EngineConfig`.
It is intentionally absent from this response and cannot be changed through `PUT /_settings`; see
the canonical [ranking settings contract](../ranking.md#3-profile-file).

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

## `PUT /_settings` — Update settings

Strict native runtime update of the **dynamic** subset (ADR-022/160). The body is one non-empty flat
JSON object of native setting keys. All-or-nothing: if any key is duplicate, unknown, non-dynamic,
the wrong type, or would produce an invalid config, nothing changes. Changes are live in memory and
startup configuration becomes authoritative again after restart.

```bash
curl -X PUT 'localhost:9200/_settings?timeout=5s&flat_settings=true' \
  -H 'Content-Type: application/json' \
  -d '{"max_segments": 16, "holes_ratio_threshold": 0.4}'
```

```json
{
  "acknowledged": true,
  "persistent": false,
  "settings": { "max_segments": 16, "holes_ratio_threshold": 0.4, "...": "full updated config" }
}
```

Supported query controls:

- `flat_settings` (Boolean, default `false`) is representation-preserving because native setting
  keys and the response are already flat.
- `timeout` bounds waiting for shared administrative admission and the engine lock before commit.
  It accepts `nanos`, `micros`, `ms`, `s`, `m`, `h`, and `d`, plus the exact value `0`; defaults to
  30 seconds and may not exceed 30 seconds.

Unknown, duplicate, and malformed controls are rejected. The route requires `application/json` or
an `application/*+json` media type, caps the body at 64 KiB, and gives it a five-second read
deadline. Duplicate JSON keys are rejected instead of silently taking the last value. Every
route-reached response uses the standard JSON envelope, is `Cache-Control: no-store`, and is
observed under the fixed `settings_put` metric label.

After body validation, the server waits asynchronously for the shared administrative permit.
Engine-lock waiting, response serialization, mutation, and snapshot publication run on a blocking
worker. The response is serialized under a 64 KiB ceiling before state changes; the config and its
immutable GET snapshot are then committed under the same engine guard. A timeout before commit
changes nothing, and a successful response therefore names exactly the configuration visible to
subsequent lock-free reads.

- **Dynamic (runtime-tunable):** `max_segments`, `memtable_flush_threshold`, `max_query_length`,
  `max_query_clauses`, `max_anyof_group_size`, `max_tags`, `holes_ratio_threshold`,
  `compaction_fixed_cost`,
  `auto_compact_on_flush`, `auto_compact_on_ingest`, `compaction_reanchor` (re-anchor drifted queries
  on the next merge, ADR-056), the broad-lane batch knobs `broad_batch_size`, `max_percolate_batch`,
  `broad_columnar`, `broad_materialize` (ADR-026), `broad_prefilter` (the batch count-gate
  pre-reject — a necessary-condition filter that skips provably-unmatchable broad candidates
  before bitmap verification; result-identical either way, `false` is the kill-switch),
  the hot-tier knobs `hot_anchor_threshold` (θ, ADR-105 — affects the classification of NEW
  writes immediately and sealed entries at the next re-anchoring compaction; a θ change is
  correctness-benign, it only moves queries between the two always-visible lanes) and
  `hot_migration_max_moves` (the per-merge migration work cap),
  `dedup_bodies` (canonical-body dedup Stage A, ADR-106 — default on; queries with identical
  semantic bodies share one posting entry per in-memory segment, verified once and emitted per
  member; result-identical either way, gates the grouping of NEW writes only),
  `cooperative_cancel` (stop armed match work at
  its deadline, ADR-099), `alias_feedback_capture` and `alias_feedback_max_pairs` (ADR-103
  measurement controls), and `accept_class_d` (store negation-only queries
  as broad-lane always-candidates instead of rejecting them, ADR-068 — gates **acceptance only**:
  already-stored entries stay matchable when toggled off, and WAL replay / the vocab recompile
  deliberately ignore it, so an acknowledged write is never dropped by a flipped knob).
- **Static (startup only):** `data_dir`, `wal_sync_on_write`, `retain_source`.
  `retention_lease_ttl_secs` is also explicitly classified as non-dynamic through this REST surface;
  configure it when constructing a library `EngineConfig`.

Ranking profiles are also startup-only, but they are not a setting key: load their separate
immutable registry with `--ranking-profiles-file` or `RR_RANKING_PROFILES_FILE` as documented in
the [ranking reference](../ranking.md). Attempting to invent a ranking-profile key here is an
unknown-setting error.

The query-complexity limits (`max_query_length`, `max_query_clauses`, `max_anyof_group_size`) and
`max_tags` are enforced on every live/build ingest path; a change applies to **subsequent** ingests,
not retroactively. WAL replay and source-driven rebuild use only the durable format's structural
ceilings, rather than either the live limit or today's defaults, so a tightened setting—or an
originally looser supported setting—never drops an already-acknowledged write (ADR-025/118).

Attempting to set a static or unknown key returns `400`:

```json
{"error": {"type": "settings_error", "reason": "setting [retain_source] is not dynamically updateable; set it at startup"}}
```

Coordinator mode validates this same query, media, JSON, size, and patch contract, then returns
`501 not_supported_in_cluster_mode` for an otherwise valid request. Per-shard configuration is fixed
when the cluster is assembled; restart the coordinator and every consistently configured shard node
with the new flags.

This native API does not accept `settings`, `persistent`, or `transient` wrapper objects, and does
not accept `null` reset. Elasticsearch's
[cluster settings update](https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-update-settings.html)
and the
[OpenSearch Cluster Settings API](https://docs.opensearch.org/latest/api-reference/cluster-api/cluster-settings/)
operate on explicit persistent/transient override tiers. Elasticsearch's
[index settings update](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-settings)
targets indices or data streams. Reverse Rusty has neither resource model nor an override registry
that could preserve precedence or reset to a recorded startup baseline, so it rejects those shapes
instead of fabricating their semantics.

---
