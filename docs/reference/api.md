# REST API reference

The Reverse Rusty server (`src/bin/server/`) exposes an Elasticsearch-style REST API over HTTP. This
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
| `--host` | 127.0.0.1 | IP address to bind. Loopback by default; set `0.0.0.0` to listen on all interfaces (see Security below) |
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

Many of these knobs are also tunable at runtime via [`PUT /_settings`](api/settings.md#put-_settings--update-settings)
(the dynamic subset); the CLI flags remain the durable startup source.

### Security

The REST API has **no built-in authentication** and exposes mutating/admin endpoints
(`_doc`, `_bulk`, `_flush`, `_compact`, `_vocab`, `_settings`). The server therefore binds
**`127.0.0.1` (loopback) by default** (ADR-052) — not reachable beyond the local host. To serve
other hosts, set `--host 0.0.0.0` (or a specific interface) **only** on a trusted network or behind
a reverse proxy that terminates authentication/TLS; do not expose the port directly to an untrusted
network. (TLS/auth on the engine itself is a tracked, not-yet-built item — see
[STATUS.md](../STATUS.md).)

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

## Endpoint reference

Endpoints are grouped by concern — open the one you need:

- **[Documents](api/documents.md)** — register / retrieve / delete a stored query (`PUT`/`GET`/`DELETE /_doc/{id}`), incl. per-query metadata tags.
- **[Percolate](api/percolate.md)** — match titles against stored queries (`POST /_search`, `POST /_mpercolate`), incl. filtered percolation.
- **[Ingest & lifecycle](api/ingest.md)** — bulk ingest + segment lifecycle (`POST /_bulk`, `/_flush`, `/_compact`).
- **[Observability](api/observability.md)** — metrics, cat tables, health (`/_stats`, `/_cat/stats`, `/_cat/segments`, `/_health`, `/_metrics`).
- **[Vocabulary](api/vocab.md)** — read / replace / learn vocabulary (`GET`/`PUT /_vocab`, `/_vocab/learn`, `/_vocab/learn_and_apply`).
- **[Settings](api/settings.md)** — read + runtime-update engine settings (`GET`/`PUT /_settings`).

The full method/path matrix is below.

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
