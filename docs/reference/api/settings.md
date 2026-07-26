# Settings — REST API

> Part of the [REST API reference](../api.md). Query language: [`dsl.md`](../dsl.md).

## `GET /_settings` — Read live settings

ES-style runtime configuration (ADR-022), read lock-free from the snapshot. The `settings` object is
the complete serialized `EngineConfig`; some fields also have server CLI flags, while library-only
construction fields are read-only here.

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

Add `?include_defaults=true` to also return a `defaults` object (the same shape, with the built-in
defaults) — like Elasticsearch's `GET /_cluster/settings?include_defaults`.

## `PUT /_settings` — Update settings

Update the **dynamic** subset at runtime. The body is a flat JSON object of setting keys to new
values. All-or-nothing: if any key is unknown, non-dynamic, the wrong type, or would produce an
invalid config, nothing changes and the request is rejected with an ES-style reason (every problem is
reported at once). Changes are in-memory and not persisted across restart.

```bash
curl -X PUT localhost:9200/_settings \
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
  `retention_lease_ttl_secs` is also read-only through this REST surface; configure it when
  constructing a library `EngineConfig`.

The query-complexity limits (`max_query_length`, `max_query_clauses`, `max_anyof_group_size`) and
`max_tags` are enforced on every live/build ingest path; a change applies to **subsequent** ingests,
not retroactively. WAL replay and source-driven rebuild use only the durable format's structural
ceilings, rather than either the live limit or today's defaults, so a tightened setting—or an
originally looser supported setting—never drops an already-acknowledged write (ADR-025/118).

Attempting to set a static or unknown key returns `400`:

```json
{"error": {"type": "settings_error", "reason": "setting [retain_source] is not dynamically updateable; set it at startup"}}
```

---
