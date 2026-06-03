# REST API reference

The Reverse Rusty server (`src/bin/server.rs`) exposes an Elasticsearch-style REST API over HTTP. This
page is the complete endpoint reference. For the query language used in `_doc` bodies see
[`dsl.md`](dsl.md); for the engine internals behind these endpoints see
[`../design/matching.md`](../design/matching.md) and [`../design/ingestion-and-updates.md`](../design/ingestion-and-updates.md).

> Server concurrency, settings, and segment-introspection behavior are governed by ADR-016, ADR-022,
> and ADR-023 — see [`../DECISIONS.md`](../DECISIONS.md).

## Running the server

```bash
cd engine
cargo run --release --bin server
```

Options:

| Flag | Default | Description |
|---|---|---|
| `--port` | 9200 | Port to listen on |
| `--data-dir` | *(in-memory)* | Persistence directory for segments and WAL |
| `--load-file` | — | Pre-load queries from a CSV or JSONL file at startup |
| `--vocab-file` | — | Load vocabulary from a JSON file at startup |
| `--threads` | *(physical cores)* | Number of rayon worker threads |
| `--include-broad` | false | Include broad-lane (class C) queries in results |
| `--drain-timeout` | 30 | Graceful shutdown timeout in seconds |
| `--log-format` | pretty | `pretty` for human-readable, `json` for structured |
| `--slow-query-threshold-ms` | 1000 | Log searches exceeding this at `warn` level (0 disables) |
| `--max-segments` | 8 | Max base segments before compaction triggers |
| `--memtable-flush-threshold` | 100000 | Memtable entries before auto-flush |
| `--max-query-length` | 10240 | Maximum query string length in bytes (10 KiB) |
| `--max-query-clauses` | 256 | Maximum clauses per query |
| `--max-anyof-group-size` | 64 | Maximum members in an any-of group |
| `--retain-source` | true | Keep query source text resident; set `false` to store it on disk and fetch `_source`/explain lazily (large memory saving at scale — ADR-020) |

Example with persistence, vocabulary, and pre-loaded queries:

```bash
cargo run --release --bin server -- \
  --port 9200 \
  --data-dir ./data \
  --vocab-file vocab.json \
  --load-file queries.csv \
  --threads 8 \
  --log-format json
```

The server handles SIGINT/SIGTERM gracefully — it drains in-flight requests, flushes the memtable,
and syncs the WAL before exiting.

Many of these knobs are also tunable at runtime via [`PUT /_settings`](#put-_settings--update-settings)
(the dynamic subset); the CLI flags remain the durable startup source.

---

## `GET /` — API root

```bash
curl localhost:9200/
```

```json
{
  "name": "reverse-rusty",
  "version": "0.1.0",
  "tagline": "you know, for matching"
}
```

## `PUT /_doc/{id}` — Register a query

```bash
curl -X PUT localhost:9200/_doc/1 \
  -H 'Content-Type: application/json' \
  -d '{"query": "(laptop,notebook) 16gb -refurbished"}'
```

```json
{"_id": 1, "result": "created", "error": null}
```

If the query fails to parse or has no anchorable features (cost class D), the response includes the
error:

```json
{"_id": 1, "result": "rejected", "error": "query has no anchorable feature (cost class D)"}
```

### Per-query metadata tags (ADR-049)

A stored query may carry **structured tags** — `(key, value)` metadata used to *narrow* percolated
results later (see [filtered percolation](#filtered-percolation-adr-049) below). Provide them either as
a canonical `tags` object or, Elasticsearch-style, as sibling fields of `query` (anything that isn't
`query`/`version`/`tags`); a value may be a string or an array of strings. The two forms are merged.

```bash
# ES-style siblings:
curl -X PUT localhost:9200/_doc/1 -H 'Content-Type: application/json' \
  -d '{"query": "dell laptop", "category": "electronics", "status": "active"}'

# or the canonical `tags` object (equivalent):
curl -X PUT localhost:9200/_doc/1 -H 'Content-Type: application/json' \
  -d '{"query": "dell laptop", "tags": {"category": "electronics", "status": "active"}}'
```

Tags are interned to integers, stored as a hot-path SoA column, and persisted (they survive reopen and
crash recovery). They **never** affect *which* queries a title matches — only the optional filter below
can narrow an already-correct result set, so they cannot introduce a false negative.

## `GET /_doc/{id}` — Retrieve a query

```bash
curl localhost:9200/_doc/1
```

```json
{"_id": 1, "found": true, "_source": {"query": "dell laptop"}}
```

If the query ID doesn't exist:

```json
{"_id": 1, "found": false}
```

## `DELETE /_doc/{id}` — Remove a query

```bash
curl -X DELETE localhost:9200/_doc/1
```

```json
{"_id": 1, "result": "deleted", "deleted_count": 1}
```

If the query ID doesn't exist (or was already deleted):

```json
{"_id": 1, "result": "not_found"}
```

## `POST /_search` — Percolate titles

Match a single title against all stored queries:

```bash
curl -X POST localhost:9200/_search \
  -H 'Content-Type: application/json' \
  -d '{"document": {"title": "Dell XPS 15 Laptop 16GB RAM 512GB SSD New"}}'
```

```json
{
  "took_ms": 0.42,
  "hits": {
    "total": 1,
    "hits": [{"_id": 1, "_source": {"query": "dell laptop"}}]
  }
}
```

Optional request fields:

| Field | Default | Description |
|---|---|---|
| `timeout_ms` | 30000 | Per-request timeout in milliseconds (returns 408 on expiry) |
| `size` | 1000 | Maximum number of hits to return |
| `from` | 0 | Offset into the result set for pagination |
| `include_source` | true | Include original query text in each hit |

`total` always reflects the full match count; `hits` is the paginated window. Set
`include_source: false` to skip query text lookup for faster responses.

Match multiple titles in a single request:

```bash
curl -X POST localhost:9200/_search \
  -H 'Content-Type: application/json' \
  -d '{
    "documents": [
      {"title": "Dell XPS 15 Laptop 16GB RAM 512GB SSD New"},
      {"title": "Vintage Brown Leather Bomber Jacket Size L"}
    ],
    "timeout_ms": 5000
  }'
```

```json
{
  "took_ms": 0.87,
  "hits": {
    "total": 2,
    "hits": [
      {"_id": 1, "_source": {"query": "dell laptop"}},
      {"_id": 2, "_source": {"query": "leather jacket"}}
    ]
  },
  "slots": [
    {
      "slot": 0,
      "total": 1,
      "hits": [{"_id": 1, "_source": {"query": "dell laptop"}}],
      "stats": {
        "unique_candidates": 15,
        "postings_scanned": 47,
        "matches": 1,
        "probes_attempted": 28,
        "probes_skipped": 12
      }
    },
    {
      "slot": 1,
      "total": 1,
      "hits": [{"_id": 2, "_source": {"query": "leather jacket"}}],
      "stats": {
        "unique_candidates": 9,
        "postings_scanned": 22,
        "matches": 1,
        "probes_attempted": 18,
        "probes_skipped": 8
      }
    }
  ]
}
```

The `stats` object per slot shows how much work the engine did: how many candidates were retrieved
from the index, how many posting lists were scanned, how many bloom-filter probes were skipped, and
how many candidates survived to become confirmed matches. The search body also accepts `explain` and
`profile` options for per-query match tracing (see [`../design/matching.md`](../design/matching.md) §6).

### Filtered percolation (ADR-049)

The dominant production read pattern is *"percolate, then narrow to one category."* Attach a tag filter
to a percolate request to keep only the matches whose stored query carries the requested
[metadata tags](#per-query-metadata-tags-adr-049). The filter is a **conjunction across keys** (AND) of
**value sets** (OR within a key); it is applied in the hot-path verify stage and can only *remove*
matches, never add or drop a wanted one. A filter value never seen at ingest matches nothing (the safe
`terms` semantics). Two equivalent shapes are accepted:

**Native** — a `filter` block alongside `document`/`documents`:

```bash
curl -X POST localhost:9200/_search -H 'Content-Type: application/json' -d '{
  "document": {"title": "Dell XPS 15 Laptop 16GB RAM New"},
  "filter": {"category": ["electronics", "computers"], "status": "active"}
}'
```

**Elasticsearch `bool`/`terms` percolate envelope** — for drop-in compatibility with existing percolate
clients. The document(s) come from `query.bool.must.percolate` and the filter from `query.bool.filter`
(an array of `terms`/`term` clauses). A bare `query.percolate` (no `bool`) works for the unfiltered case.

```bash
curl -X POST localhost:9200/_search -H 'Content-Type: application/json' -d '{
  "query": {
    "bool": {
      "must": {"percolate": {"field": "query", "document": {"title": "Dell XPS 15 Laptop New"}}},
      "filter": [
        {"terms": {"category": ["electronics", "computers"]}},
        {"term":  {"status": "active"}}
      ]
    }
  }
}'
```

Only the `percolate` + `bool.filter(terms/term)` subset is supported; any other query clause (e.g.
`match`, `range`) returns **400** rather than silently widening the result set. `/_mpercolate` accepts the
same `filter` block and ES envelope (applied to every document in the batch).

## `POST /_mpercolate` — Batch percolate (high throughput)

The throughput counterpart to `/_search`. Percolates a **batch** of documents in one request and
evaluates the broad lane **once per batch, columnar** (ADR-026) instead of once per document — so a
hot broad anchor's huge posting is scanned once for the whole batch, not re-scanned per document.
Returns an Elasticsearch `_msearch`-style `responses[]` envelope: one entry per input document, in
submission order (`responses[i]` corresponds to `documents[i]`).

```bash
curl -X POST localhost:9200/_mpercolate \
  -H 'Content-Type: application/json' \
  -d '{
    "documents": [
      {"title": "Dell XPS 15 Laptop 16GB RAM 512GB SSD New"},
      {"title": "Vintage Brown Leather Bomber Jacket Size L"},
      {"title": "Generic unmatched listing"}
    ],
    "include_broad": true,
    "profile": true
  }'
```

```json
{
  "took_ms": 0.91,
  "responses": [
    {"hits": {"total": 1, "hits": [{"_id": 1, "_source": {"query": "dell laptop"}}]}},
    {"hits": {"total": 1, "hits": [{"_id": 2, "_source": {"query": "leather jacket"}}]}},
    {"hits": {"total": 0, "hits": []}}
  ],
  "broad": {
    "strategy": "columnar",
    "batch_size": 256,
    "broad_batches": 1,
    "broad_postings_scanned": 0,
    "broad_queries_evaluated": 0,
    "broad_candidates": 0,
    "total_matches": 2
  }
}
```

Optional request fields:

| Field | Default | Description |
|---|---|---|
| `include_broad` | server default (`--include-broad`) | Per-request override: evaluate class-C (broad) queries for this batch |
| `include_source` | true | Include original query text in each hit |
| `size` | 1000 | Maximum hits per document |
| `timeout_ms` | 30000 | Per-request timeout in milliseconds (returns 408 on expiry) |
| `profile` | false | Include the top-level `broad` summary |

Each per-document result is **byte-identical** to calling `/_search` with that single title — batching
is a performance change only, never a semantic one (proven by `tests/broad_batch.rs`). The optional
top-level `broad` summary surfaces the columnar evaluator's amortization: as the batch grows,
`broad_postings_scanned` rises far slower than `broad_candidates` (each huge posting is consulted once
per batch). An empty `documents` array is a valid no-op (`200` with `responses: []`); a missing
`documents` field is a `400`.

**When to use which.** Reach for `/_mpercolate` for high-throughput batch/streaming percolation,
especially with broad queries enabled. Reach for `/_search` when you want the rich, per-document
observability it alone provides — per-slot `stats`, `explain`, `profile`, and pagination (`from`).
Because the broad lane is amortized per batch, `/_mpercolate` deliberately does not produce per-document
candidate/posting stats — only the batch-level `broad` summary.

## `POST /_bulk` — Bulk ingest

NDJSON format, compatible with Elasticsearch's `_bulk` API:

```bash
curl -X POST localhost:9200/_bulk \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary @- <<'EOF'
{"index": {"_id": 1}}
{"query": "(laptop,notebook) 16gb -refurbished"}
{"index": {"_id": 2}}
{"query": "vintage leather jacket -(replica,faux)"}
{"index": {"_id": 3}}
{"query": "\"running shoes\" (nike,adidas) -used"}
EOF
```

```json
{
  "took_ms": 1.23,
  "errors": false,
  "items": [
    {"index": {"_id": 1, "status": 201, "error": null}},
    {"index": {"_id": 2, "status": 201, "error": null}},
    {"index": {"_id": 3, "status": 201, "error": null}}
  ]
}
```

If any query fails, `errors` is `true` and that item gets a `400` status with the parse error message;
successfully ingested queries in the same batch are unaffected (per-item status — ADR-018).

Each source line may also carry [metadata tags](#per-query-metadata-tags-adr-049) — a `tags` object or
ES-style sibling fields — exactly as `PUT /_doc` does, e.g. `{"query": "...", "category": "electronics"}`.

## `POST /_flush` — Flush memtable

Flush the in-memory memtable to an immutable on-disk segment:

```bash
curl -X POST localhost:9200/_flush
```

```json
{
  "acknowledged": true,
  "total_queries": 3,
  "base_segments": 1
}
```

## `POST /_compact` — Force compaction

Trigger segment compaction to merge segments and reclaim tombstones:

```bash
curl -X POST localhost:9200/_compact
```

When compaction runs:

```json
{
  "acknowledged": true,
  "segments_merged": 2,
  "entries_before": 150,
  "entries_after": 142,
  "tombstones_reclaimed": 8
}
```

When no compaction is needed:

```json
{
  "acknowledged": true,
  "message": "no compaction needed"
}
```

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
  "class_counts": {"a": 2, "b": 1, "c": 0, "d": 0},
  "segment_sizes": [3],
  "segment_holes": [0.0],
  "memory": {
    "exact_bytes": 1024,
    "index_bytes": 2048,
    "filter_bytes": 512
  }
}
```

- **class_counts** — how many queries fell into each cost class (A is best, D is rejected)
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
memory           3584 bytes (~0.0 MB)

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
gauge, WAL size/pending gauges, cumulative flush/compaction-time counters, and a
`durability_failures_total{op}` counter (ADR-021).

## `GET /_vocab` — Current vocabulary

```bash
curl localhost:9200/_vocab
```

```json
{
  "synonyms": [
    {"token": "rc", "canonical": "term:rookie", "kind": "generic"}
  ],
  "phrases": [
    {"tokens": ["upper", "deck"], "canonical": "term:upper_deck", "kind": "generic"}
  ],
  "graders": ["psa"],
  "grade_words": ["gem"]
}
```

## `PUT /_vocab` — Replace vocabulary

Replace the engine's vocabulary. If queries have already been ingested, the response includes a
warning — you should reingest for consistent matching.

```bash
curl -X PUT localhost:9200/_vocab \
  -H 'Content-Type: application/json' \
  -d '{"synonyms": [{"token": "rc", "canonical": "term:rookie", "kind": "category"}], "phrases": [], "graders": [], "grade_words": []}'
```

```json
{
  "acknowledged": true,
  "warning": "normalizer changed with existing queries; reingest for consistent matching"
}
```

## `POST /_vocab/learn` — Learn vocabulary from queries

Send raw query text to discover synonym relationships from any-of groups. Returns the learned
vocabulary without applying it — review and then `PUT /_vocab` to use it.

```bash
curl -X POST localhost:9200/_vocab/learn \
  -H 'Content-Type: application/json' \
  -d '{
    "queries": [[1, "(rookie,rc) 2024"], [2, "(rookie,rc) 2023"]],
    "min_count": 2
  }'
```

```json
{
  "synonyms": [
    {"token": "rc", "canonical": "term:rookie", "kind": "generic"}
  ],
  "phrases": [],
  "graders": [],
  "grade_words": []
}
```

The `min_count` parameter (default: 2) controls how many times a synonym pair must appear across
different queries before it's included. Higher values reduce noise. See [`dsl.md`](dsl.md#vocabulary)
for how vocabulary affects matching.

## `POST /_vocab/learn_and_apply` — Learn from stored queries and apply

Learn synonyms from the engine's **own** already-ingested queries and apply them in one step (unlike
`POST /_vocab/learn`, which only returns synonyms learned from caller-supplied queries for review). The
engine re-mints its vocabulary, recompiles every stored query under the new normalizer, and atomically
swaps — so both surface forms of each learned alias match immediately, with zero false negatives
(ADR-046). The change is durable (it survives reopen).

```bash
curl -X POST 'localhost:9200/_vocab/learn_and_apply?min_count=2'
```

```json
{
  "acknowledged": true,
  "recompiled": 1280
}
```

`min_count` (query parameter, default: 2) is the minimum any-of occurrences before a synonym pair is
learned; `recompiled` is the number of stored queries rebuilt under the new vocabulary.

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
  `auto_compact_on_flush`, `auto_compact_on_ingest`.
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

## All endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/` | GET | Version info |
| `/_doc/{id}` | GET | Retrieve a stored query |
| `/_doc/{id}` | PUT | Register a single query |
| `/_doc/{id}` | DELETE | Remove a stored query |
| `/_search` | POST | Percolate one or more titles (rich: per-slot `stats`, `explain`, `profile`, paging) |
| `/_mpercolate` | POST | Batch percolate (high throughput; columnar broad lane; `responses[]` envelope) |
| `/_bulk` | POST | NDJSON bulk ingest (per-item status) |
| `/_flush` | POST | Flush memtable to immutable segment |
| `/_compact` | POST | Force segment compaction |
| `/_stats` | GET | JSON metrics snapshot |
| `/_cat/stats` | GET | Human-readable metrics |
| `/_cat/segments` | GET | Per-segment LSM detail (text table or `?format=json`) |
| `/_health` | GET | Health check (green/yellow/red) |
| `/_metrics` | GET | Prometheus text exposition format |
| `/_vocab` | GET | Current vocabulary as JSON |
| `/_vocab` | PUT | Replace vocabulary |
| `/_vocab/learn` | POST | Learn synonyms from raw query text |
| `/_vocab/learn_and_apply` | POST | Learn synonyms from stored queries + apply (`?min_count=N`) |
| `/_settings` | GET | Read live engine settings (`?include_defaults`) |
| `/_settings` | PUT | Update the dynamic settings subset |
