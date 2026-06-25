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
| `--wal-sync-on-write` | false | Fsync the WAL on every mutation before acknowledging it (SQLite FULL). When false, appends reach the OS page cache and fsync at the next flush checkpoint — survives a process crash but not power loss until checkpoint (RocksDB sync=false / SQLite NORMAL) |
| `--broad-batch-size` | 256 | Title sub-batch size for the columnar broad lane on `POST /_mpercolate` (ADR-026) — larger amortizes broad-posting scans over more titles. Dynamic via `/_settings` |
| `--broad-columnar` | true | Use the columnar broad evaluator (once per batch); set `false` to fall back to the inline per-title broad probe — the kill-switch (identical results, no amortization). Dynamic via `/_settings` |
| `--broad-materialize` | true | Use the pure-anchor materialization fast path (emit pure-anchor broad queries straight from the anchor bitmap, skipping verification). Dynamic via `/_settings` |
| `--max-percolate-batch` | 10000 | Maximum documents accepted in one `POST /_mpercolate` batch; larger requests are rejected with 400. Dynamic via `/_settings` |

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
`_backup`, `_vocab` writes (including `/_vocab/learn*` and `/_vocab/aliases/*`), `_settings` writes — and any
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

**`POST /_backup` is privileged operator surface.** It writes a snapshot to an arbitrary
server-side `dest` path with the server process's filesystem permissions (UID), so it grants
filesystem-write on the host to anyone who can call it. It is in the default-deny set above and
**must stay behind auth** on any non-loopback bind — never expose it unauthenticated.

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

> `version` is the crate's `CARGO_PKG_VERSION` (from `engine/Cargo.toml`), not a pinned literal —
> the `"0.1.0"` above is illustrative and will track the package version as it bumps.

## Endpoint reference

Endpoints are grouped by concern — open the one you need:

- **[Documents](api/documents.md)** — register / retrieve / delete a stored query (`PUT`/`GET`/`DELETE /_doc/{id}`), incl. per-query metadata tags.
- **[Percolate](api/percolate.md)** — match titles against stored queries (`POST /_search`, `POST /_mpercolate`), incl. filtered percolation.
- **[Ingest & lifecycle](api/ingest.md)** — bulk ingest + segment lifecycle (`POST /_bulk`, `/_flush`, `/_compact`).
- **[Observability](api/observability.md)** — metrics, cat tables, health (`/_stats`, `/_cat/stats`, `/_cat/segments`, `/_health`, `/_metrics`).
- **[Vocabulary](api/vocab.md)** — read / replace / learn vocabulary (`GET`/`PUT /_vocab`, `/_vocab/learn`, `/_vocab/learn_and_apply`) + the learned-alias registry (`/_vocab/aliases*`, ADR-060).
- **[Settings](api/settings.md)** — read + runtime-update engine settings (`GET`/`PUT /_settings`).
- **[Backup & restore](../operations/backup-restore.md)** — snapshot durable state (`POST /_backup`); restore via `--data-dir` (ADR-079).

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
| `/_backup` | POST | Snapshot durable state to a server-side dir (body `{"dest":"..."}`); restore via `--data-dir` ([backup/restore](../operations/backup-restore.md), ADR-079) |
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
(in-process copies per position), `--shard-endpoint` (repeatable; remote mode). Remote links take
the **mesh security** flags (ADR-071): `--grpc-tls-ca` (PEM CA to verify shard servers — endpoints
then use `https://`), `--grpc-tls-domain` (SNI/verification override for raw-IP endpoints), and
`--cluster-token`/`RR_CLUSTER_TOKEN` (the shared mesh secret attached to every gRPC RPC — distinct
from the HTTP `--auth-token`). The server side of the mesh is configured on
`shardserver`/`controlserver` (`--tls-cert`/`--tls-key`/`--cluster-token`; both also take the
client half `--tls-ca`/`--tls-domain` — the controlserver for its peer Raft links, the
shardserver for the `RecoverFrom` outbound pull from a peer source). `--data-dir` makes
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
- **`rank` works (ADR-075)** — the same block as single-node, scored at the shards against the shared
  tag space and merged `(score desc, _id asc)` with `from`/`size` + `_score`. One cluster-specific
  boundary: a **post-freeze (live-added) `priority` tag scores 0** — priority reads the tag's value
  string, which only a build-time interned tag has; boosts fire for both (id-equality). `explain` is
  rejected with 400 — never silently ignored. `profile` works (merged cross-shard `MatchStats`).
- **`include_source` defaults to `false`** (`_source` costs a per-hit source probe); explicitly
  requesting it on a remote cluster answers 501 (remote shards expose no source readback in v1).
- **`GET /_settings` works in cluster mode** — it returns the live cluster + per-shard configuration
  (`mode`, `shards`, `replication_factor`, `include_broad`, `durable`, and the assembled `per_shard`
  `EngineConfig`). Only **`PUT /_settings`** is 501 in cluster mode (see below).
- **Single-node-only surfaces answer 501 naming the alternative:** `/_compact` (per-shard policy;
  use `POST /_checkpoint` for the durability commit), `PUT /_settings` (cluster settings are fixed
  at assembly — restart the coordinator with the new flags), `/_cat/stats`, `/_cat/segments`.
- **Vocabulary admin** (`PUT /_vocab`, `/_vocab/learn_and_apply`, `/_vocab/aliases/*`) maps onto the
  cluster blue/green rebuild (ADR-046); its one refusal — non-local (gRPC) shards — surfaces as a 400
  with the engine's message (remote-cluster vocabulary is deploy-time configuration, ADR-076). A
  **tagged** cluster is not refused (tags carry through by stored `TagId`, ADR-074), and a
  **multi-word alias activates** (P(T)-aware routing, ADR-076). At startup, `--vocab` on a fresh
  in-process cluster fully activates (`build_with_vocab`); on an **empty** durable reopen whose
  manifest carries no vocabulary it activates through the rebuild funnel (a **populated** reopen
  keeps the committed state authoritative and warns — apply explicitly via `PUT /_vocab`); a
  REMOTE assembly refuses ANY vocab file at startup (shard servers run the stock normalizer, so
  even normalizer-level rules would silently diverge the feature space).

Cluster-only endpoints:

| Endpoint | Method | Description |
|---|---|---|
| `/_checkpoint` | POST | The cluster durability commit point (seal shards + commit the coordinator manifest + truncate the log, ADR-031/032); returns the new `epoch` |
| `/_backup` | POST | Snapshot the cluster's durable state to a server-side dir (body `{"dest":"..."}`): checkpoint, then copy the coordinator manifest + per-shard segments + sources + the log. Restore via `--data-dir` ([backup/restore](../operations/backup-restore.md), ADR-079) |
| `/_cat/shards` | GET | Per-shard query counts + node assignments (text table or `?format=json`) |
| `/_cluster/state` | GET | The committed control-plane document (membership + shard→node map + ring params, ADR-037) |
| `/_cluster/nodes` | POST | Register a cluster member (`{"id": N, "addr": "...", "role": "data"\|"manager"}`) |
| `/_cluster/nodes/{id}` | DELETE | Deregister a member (idempotent) |
| `/_cluster/rebalance` | POST | Recompute + commit the shard→node map from membership (HRW, ADR-042). Default (or empty body) is **map-only** — it must NOT be used alone to re-point a populated remote cluster. `{"move": true}` (ADR-090, `--features distributed` only, else 501) additionally MOVES each reassigned position's data via live handoff so routing follows; returns `{acknowledged, moved_data, moved[], failed, not_attempted}` |
| `/_cluster/resize` | POST | Resize the cluster (ADR-078): `{"num_shards": N}` — a blue/green rebuild re-places every live query under a fresh ring; returns `{acknowledged, num_shards, rebuilt}`. In-process only (a non-local cluster → 400); vocab + tags preserved; `O(corpus)` (holds the write lock like `PUT /_vocab`) |
| `/_cluster/resync` | POST | Re-drive queued partial-apply repairs (ADR-047); returns `{repaired, still_pending}` |
| `/_cluster/handoff` | POST | Live data-moving handoff (ADR-044/048/072): `{"position": N, "source": "https://…", "target": "https://…"}` — peer-recover the target, fence + drain the source, flip routing; returns the new `generation`. Fail-closed: an aborted move auto-unfences the source. Requires a `--features distributed` build (else 501). The raw-endpoint primitive; for the map-aware version see `/_cluster/reassign` |
| `/_cluster/reassign` | POST | Data-moving reassignment (ADR-090): `{"position": N, "node": M}` — resolve node M's endpoint from membership, live-handoff the data there, then commit the shard→node assignment (**move-then-commit**), so routing follows live + across a resolve-only restart. Returns `{acknowledged, moved, committed, position, node, generation}`; `committed:false` (200 + `warning`) means the data moved but the durable map commit failed — re-run to reconcile (still zero-FN). Fail-closed (a failed move commits nothing + auto-unfences). Requires a `--features distributed` build (else 501) |

`GET /_stats` in cluster mode reports `{shards, replication_factor, total_queries, shard_queries[],
class_counts, epoch, pending_repairs, has_tagged_queries, durable}`; `GET /_health` is green/yellow
(repairs queued)/red (a shard probe failed).
