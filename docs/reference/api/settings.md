# Settings — REST API

> Part of the [REST API reference](../api.md). Query language: [`dsl.md`](../dsl.md).

## `GET /_settings` — Read live settings

ES-style runtime configuration (ADR-022), read lock-free from the snapshot. Fields mirror
`EngineConfig` / the server CLI flags.

```bash
curl localhost:9200/_settings
```

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
  `max_query_clauses`, `max_anyof_group_size`, `holes_ratio_threshold`, `compaction_fixed_cost`,
  `auto_compact_on_flush`, `auto_compact_on_ingest`, `compaction_reanchor` (re-anchor drifted queries
  on the next merge, ADR-056), and the broad-lane batch knobs `broad_batch_size`, `max_percolate_batch`,
  `broad_columnar`, `broad_materialize` (ADR-026).
- **Static (startup only):** `data_dir`, `wal_sync_on_write`, `retain_source`.

The query-complexity limits (`max_query_length`, `max_query_clauses`, `max_anyof_group_size`) are
enforced by the parser on every ingest path; a change applies to **subsequent** ingests, not
retroactively, and WAL replay on recovery uses the compiled-in ceiling rather than the live limit so a
tightened limit never drops an already-acknowledged write (ADR-025).

Attempting to set a static or unknown key returns `400`:

```json
{"error": {"type": "settings_error", "reason": "setting [retain_source] is not dynamically updateable; set it at startup"}}
```

---

