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
| `--auth-token` | *(none — auth off)* | Bearer token required on mutating/admin endpoints (ADR-062). Prefer the `RR_AUTH_TOKEN` env var in production — flag values appear in process listings (see Security below) |
| `--auth-protect-reads` | false | Extend bearer-token auth to read endpoints too (everything except `GET /_health`). Requires an auth token |
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
| `--accept-class-d` | false | Store negation-only queries as broad-lane always-candidates instead of rejecting them (ADR-068) — needed at startup for a `--load-file` corpus containing such queries; also dynamic via `/_settings` |

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

The server binds **`127.0.0.1` (loopback) by default** (ADR-052) — not reachable beyond the local
host. To serve other hosts, set `--host 0.0.0.0` (or a specific interface) and gate the
mutating/admin endpoints with **bearer-token auth** (ADR-062):

```bash
export RR_AUTH_TOKEN=$(openssl rand -hex 32)
cargo run --release --bin server -- --host 0.0.0.0
# clients:
curl -X PUT localhost:9200/_doc/1 -H "Authorization: Bearer $RR_AUTH_TOKEN" \
  -H 'content-type: application/json' -d '{"query": "michael jordan"}'
```

With a token configured (`RR_AUTH_TOKEN` env var or `--auth-token`; the env var is preferred — flag
values appear in process listings), **every non-GET/HEAD request requires
`Authorization: Bearer <token>`** except the read-via-POST percolate endpoints (`POST /_search`,
`POST /_mpercolate`). That default-deny rule covers `_doc` writes, `_bulk`, `_flush`, `_compact`,
`_vocab` writes (including `/_vocab/learn*` and `/_vocab/aliases/*`), `_settings` writes — and any
future mutating endpoint, which fails closed rather than open. Reads stay open unless
`--auth-protect-reads` extends the gate to them too (stored queries are data worth protecting on an
exposed port); only `GET /_health` is always open so liveness probes keep working.

Failures return **401** with the standard error envelope (`"type": "security_exception"`) and an
RFC 6750 `WWW-Authenticate: Bearer` challenge (`error="invalid_token"` when a wrong token was
presented), increment `auth_failures_total{reason="missing"|"invalid"}` in `/_metrics`, and log a
structured warning. The token comparison is constant-time. An empty/non-printable token, a
set-but-not-UTF-8 `RR_AUTH_TOKEN`, or `--auth-protect-reads` without a token refuses startup
(fail-loud — a malformed token never silently disables auth); binding a non-loopback interface
*without* auth logs a startup warning.

With **no token configured the server behaves exactly as before** (no auth — strictly opt-in). The
transport is plain HTTP either way: a bearer token is only as private as the link it crosses, so on
an untrusted network still front the server with a reverse proxy that terminates TLS. (TLS, and auth
on the *gRPC* shard/control transports, are the tracked Tier-3 items — see
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
- **[Vocabulary](api/vocab.md)** — read / replace / learn vocabulary (`GET`/`PUT /_vocab`, `/_vocab/learn`, `/_vocab/learn_and_apply`) + the learned-alias registry (`/_vocab/aliases*`, ADR-060).
- **[Settings](api/settings.md)** — read + runtime-update engine settings (`GET`/`PUT /_settings`).

The full method/path matrix is below.

## All endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/` | GET | Version info |
| `/_doc/{id}` | GET | Retrieve a stored query |
| `/_doc/{id}` | PUT | Register **or atomically replace** a query (201 created / 200 updated, ADR-067) |
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
| `/_vocab/learn` | POST | Learn synonyms (+ opt-in NPMI phrases, `corpus_phrases=true`) from raw query text |
| `/_vocab/learn_and_apply` | POST | Learn from stored queries + apply (`?min_count=N`; opt-in NPMI phrases `?corpus_phrases=true`) |
| `/_vocab/aliases` | GET | The governed alias registry + status summary (ADR-060) |
| `/_vocab/aliases/import` | POST | Import a Solr/Lucene synonym file + apply (body `{"synonyms":"..."}`) |
| `/_vocab/aliases/learn_and_apply` | POST | Learn alias candidates from stored queries + apply (`?min_count=N`) |
| `/_settings` | GET | Read live engine settings (`?include_defaults`) |
| `/_settings` | PUT | Update the dynamic settings subset |

---

## Cluster (coordinator) mode — `--cluster` (ADR-070)

The same binary also runs as a **cluster coordinator**: the REST dialect above served over a
multi-shard [`ClusterEngine`](../design/clustering-and-scaling.md) instead of a single-node engine
(Distributed-v1 criterion 1, [ADR-065](../DECISIONS.md)). Auth (ADR-062), request-id middleware, and
Prometheus wiring are identical.

```bash
# In-process cluster: K shards in this process, durable under --data-dir.
cargo run --release --bin server -- --cluster --shards 8 --data-dir ./cluster-data \
  --load-file queries.csv

# Remote cluster (requires --features distributed): one --shard-endpoint per shard
# position, each "primary[,replica,...]" — the coordinator ships its frozen dict +
# tag space to every endpoint at connect (ADR-034/055).
cargo run --release --bin server --features distributed -- --cluster \
  --shard-endpoint http://10.0.0.1:50051,http://10.0.0.2:50051 \
  --shard-endpoint http://10.0.0.3:50051,http://10.0.0.4:50051 \
  --load-file queries.csv
```

Cluster-mode flags: `--cluster`, `--shards` (in-process K, default 8), `--replication-factor`
(in-process copies per position), `--shard-endpoint` (repeatable; remote mode). `--data-dir` makes
an **in-process** cluster durable (build once, reopen on restart — `--load-file` is skipped with a
warning when the reopened cluster is already populated). A **remote** coordinator is stateless and
refuses `--data-dir`: durability lives on the shard nodes (`shardserver --data-dir`, the per-shard
translog — ADR-039); restarting the coordinator reconnects and re-mints the identical frozen dict
from the same `--load-file`, so the fingerprint handshake holds.

Behavior deltas from single-node mode (all deliberate, none silent):

- **`PUT /_doc/{id}` is a cluster-atomic upsert** — one coordinator log frame replaces every prior
  live copy (ES `index` semantics, the ADR-067 contract at the cluster). A partial multi-shard apply
  (remote clusters only) answers 200 with `"result": "partial"`: the write **is** durably logged and
  queued for repair — do **not** re-PUT (it would double-log); `POST /_cluster/resync` converges it.
- **Per-request `include_broad`** is honored on both `/_search` and `/_mpercolate`.
- **`rank` and `explain` are rejected with 400** (cluster ranking is ADR-065 criterion 5) — never
  silently ignored. `profile` works (merged cross-shard `MatchStats`).
- **`include_source` defaults to `false`** (`_source` costs a per-hit source probe); explicitly
  requesting it on a remote cluster answers 501 (remote shards expose no source readback in v1).
- **Single-node-only surfaces answer 501 naming the alternative:** `/_compact` (per-shard policy;
  use `POST /_checkpoint` for the durability commit), `PUT /_settings` (cluster settings are fixed
  at assembly), `/_cat/stats`, `/_cat/segments`.
- **Vocabulary admin** (`PUT /_vocab`, `/_vocab/learn_and_apply`, `/_vocab/aliases/*`) maps onto the
  cluster blue/green rebuild (ADR-046); its refusals — non-local (gRPC) shards, tagged clusters,
  multi-word alias activation (ADR-055/061) — surface as 400s with the engine's message.

Cluster-only endpoints:

| Endpoint | Method | Description |
|---|---|---|
| `/_checkpoint` | POST | The cluster durability commit point (seal shards + commit the coordinator manifest + truncate the log, ADR-031/032); returns the new `epoch` |
| `/_cat/shards` | GET | Per-shard query counts + node assignments (text table or `?format=json`) |
| `/_cluster/state` | GET | The committed control-plane document (membership + shard→node map + ring params, ADR-037) |
| `/_cluster/nodes` | POST | Register a cluster member (`{"id": N, "addr": "...", "role": "data"\|"manager"}`) |
| `/_cluster/nodes/{id}` | DELETE | Deregister a member (idempotent) |
| `/_cluster/rebalance` | POST | Recompute + commit the shard→node map from membership (HRW, ADR-042) |
| `/_cluster/resync` | POST | Re-drive queued partial-apply repairs (ADR-047); returns `{repaired, still_pending}` |

`GET /_stats` in cluster mode reports `{shards, replication_factor, total_queries, shard_queries[],
class_counts, epoch, pending_repairs, has_tagged_queries, durable}`; `GET /_health` is green/yellow
(repairs queued)/red (a shard probe failed).
