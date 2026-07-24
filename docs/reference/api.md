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
| `--max-concurrent-searches` | 0 *(unbounded)* | Max `/_search`+`/_mpercolate` requests occupying the match pool at once; excess queue within their own `timeout_ms` (ADR-099) |
| `--max-ranked-enrichment-bytes` | 16777216 (16 MiB) | Maximum winner source bytes fetched by one local or cluster `/v2/_search`; overflow fails the whole response with `413 rank_enrichment_limit` (ADR-110) |
| `--pit-default-keep-alive-secs` | 60 | Keep-alive for a `POST /v2/_pit` point-in-time when the request names none; renewed on every use (ADR-113) |
| `--pit-max-keep-alive-secs` | 600 | Ceiling on a requested PIT keep-alive; over-ask is a 400 (ADR-113) |
| `--max-open-pits` | 64 | Concurrently open PITs; a breach is `429 pit_limit_exceeded`, never an eviction (ADR-113) |
| `--exhaustive-threads` | 2 | Dedicated Rayon workers for exhaustive jobs; isolated from interactive search (ADR-114) |
| `--max-concurrent-exhaustive-jobs` | 2 | Non-queuing exhaustive admission permits; must not exceed `--exhaustive-threads`; excess starts return 503 |
| `--exhaustive-chunk-size` | 512 | Maximum members per provisional stream chunk (hard ceiling 16,384) |
| `--exhaustive-channel-depth` | 8 | Bounded frames buffered between an exhaustive worker and its stream consumer |
| `--exhaustive-job-timeout-secs` | 300 | Maximum exhaustive admission-to-terminal lifetime (including worker scheduling); a request may ask for less |
| `--max-retained-exhaustive-jobs` | 1024 | In-memory job records; oldest terminal records are pruned, while an all-active full registry rejects with 429 |
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
| `--hot-anchor-threshold` | 0 (off) | The hot-anchor threshold θ (class H, ADR-105; recommended 1024): a query whose deciding anchor has no top-64 mask bit but frequency ≥ θ is stored in the always-probed, columnar-evaluated hot tier instead of fattening the realtime lane. Dynamic via `/_settings`; in remote cluster mode run every `shardserver` with the same value (divergence is cost-only, never correctness) |
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
`POST /v2/_search`, `POST /_mpercolate`). That default-deny rule covers `_doc` writes, `_bulk`, `_flush`, `_compact`,
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

## `GET /` / `HEAD /` — API root

```bash
curl localhost:9200/
```

```json
{
  "name": "reverse-rusty",
  "cluster_name": "reverse-rusty",
  "cluster_uuid": "_na_",
  "version": {
    "distribution": "reverse-rusty",
    "number": "0.1.0"
  },
  "tagline": "you know, for matching"
}
```

The shape follows the familiar Elasticsearch/OpenSearch cluster-information response while staying
honest about Reverse Rusty's own capabilities:

- `version.number` is the crate's `CARGO_PKG_VERSION` (from `engine/Cargo.toml`), not a pinned
  literal — the `"0.1.0"` above is illustrative and tracks the package version as it bumps.
- `cluster_uuid` is `_na_` because Reverse Rusty does not currently persist an externally visible
  cluster identity. The response omits Lucene, wire-compatibility, and index-compatibility fields
  because they do not apply.
- Coordinator mode adds `mode: "cluster"`, `shards`, `replication_factor`, and `durable`.
- `HEAD /` is the lightweight connectivity form: it returns the same `200` and response headers as
  `GET /`, with no body.

## Endpoint reference

Endpoints are grouped by concern — open the one you need:

- **[Documents](api/documents.md)** — register / retrieve / existence-check / delete a stored query
  (`PUT`/`GET`/`HEAD`/`DELETE /_doc/{id}`), including durable metadata-tag read-back and ES/OS
  `_source` filtering.
- **[Percolate](api/percolate.md)** — match titles against stored queries (`POST /_search`,
  local/cluster bounded `POST /v2/_search`, `POST /_mpercolate`), including filtered
  percolation and exhaustive `result_mode=all` jobs with a terminally verified NDJSON stream.
- **[Ingest & lifecycle](api/ingest.md)** — bulk ingest + segment lifecycle (`POST /_bulk`, `/_flush`, `/_compact`).
- **[Observability](api/observability.md)** — metrics, cat tables, health (`/_stats`, `/_cat/stats`, `/_cat/segments`, `/_health`, `/_metrics`).
- **[Vocabulary](api/vocab.md)** — read / replace / learn vocabulary (`GET`/`PUT /_vocab`, `/_vocab/learn`, `/_vocab/learn_and_apply`) + the learned-alias registry (`/_vocab/aliases*`, ADR-060).
- **[Settings](api/settings.md)** — read + runtime-update engine settings (`GET`/`PUT /_settings`).
- **[Backup & restore](../operations/backup-restore.md)** — snapshot durable state (`POST /_backup`); restore via `--data-dir` (ADR-079).

The full method/path matrix is below.

## All endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/` | GET/HEAD | Product, version, and cluster info |
| `/_doc/{id}` | GET/HEAD | Retrieve a stored query / bodyless existence check |
| `/_doc/{id}` | PUT | Register or atomically replace/create-only a query (`op_type=index|create`; strict `refresh`; ES/OS response metadata, ADR-117) |
| `/_doc/{id}` | DELETE | Remove a stored query |
| `/_search` | POST | Percolate one or more titles (rich: per-slot `stats`, `explain`, `profile`, paging) |
| `/v2/_search` | POST | Single-node or cluster, single-document exact bounded top-K + winner-only enrichment (ADR-107/108/110); accepts `pit`/`cursor` pages (ADR-113) |
| `/v2/_pit` | POST/DELETE | Open / close a point-in-time snapshot for cursor pagination (in-process modes; remote assemblies 501 — ADR-113) |
| `/_percolate/jobs` | POST | Start one exact exhaustive background match; returns 202 with status and stream URLs (ADR-114) |
| `/_percolate/jobs/{id}` | GET/DELETE | Inspect a retained exhaustive job / request cooperative cancellation |
| `/_percolate/jobs/{id}/stream` | GET | Claim the job's one bounded `application/x-ndjson` stream consumer |
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
| `/_vocab/aliases/discover` | POST | Distributional alias discovery, compute-only (ADR-102): proposals + similarity/co-occurrence evidence over the stored queries, or an explicit `{"queries": [[id, "dsl"], ...]}` body; knob overrides in the body (`min_similarity`, `max_pairs`, …) |
| `/_vocab/aliases/discover_and_record` | POST | Discover over the engine's OWN stored queries and file every proposal as a review `Candidate` (never activates — `recompiled` is always 0; activation stays `PUT /_vocab` with an edited status) |
| `/_vocab/aliases/feedback` | GET | Match-feedback evidence per tracked candidate pair (ADR-103): title counts, surviving sampled queries, Jaccard `overlap`, `validated` — thresholds `?min_overlap=0.5&min_titles=50&min_queries=20` echoed (capture is opt-in: `alias_feedback_capture`) |
| `/_vocab/aliases/feedback/reset` | POST | Wipe accumulated feedback evidence (a measurement-window boundary) |
| `/_vocab/aliases/validate_and_apply` | POST | Stamp validated pairs into the registry (evidence + confidence — metadata-only, no recompile); `?activate=true` additionally promotes them (refuses operator-rejected groups) via the full recompile path |
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
shardserver for the `RecoverFrom` outbound pull from a peer source). `shardserver` also takes
`--max-grpc-result-bytes` (default/hard ceiling 4 MiB; any positive lower byte bound is valid),
enforced against exact protobuf size for compatibility replies, top-K replies, and every fetched
source stream item (ADR-110). ADR-114 adds node-local exhaustive-stream limits:
`--max-concurrent-exhaustive-streams` (default 2, non-queuing) and
`--max-exhaustive-stream-secs` (default 300, a hard ceiling on the coordinator/direct caller's
remaining budget). In remote mode, configure that duration at least as high as the coordinator's
`--exhaustive-job-timeout-secs`; an over-ask fails loud before shard admission.
`--data-dir` makes
an **in-process** cluster durable (build once, reopen on restart — `--load-file` is skipped with a
warning when the reopened cluster is already populated). A **remote** coordinator is stateless and
refuses `--data-dir`: durability lives on the shard nodes (`shardserver --data-dir`, the per-shard
translog — ADR-039); restarting the coordinator reconnects and re-mints the identical frozen dict
from the same `--load-file`, so the fingerprint handshake holds. Its new boot ID may need to retry
until the 30-second renewable owner lease expires, then wait for any response bodies/streams
admitted under the prior owner to drain before taking over a node.

Behavior deltas from single-node mode (all deliberate, none silent):

- **`PUT /_doc/{id}` is a cluster-atomic upsert** — one coordinator log frame replaces every prior
  live copy (ES `index` semantics, the ADR-067 contract at the cluster). A partial multi-shard apply
  (remote clusters only) answers 200 with `"result": "partial"`: the write **is** durably logged and
  queued for repair — do **not** re-PUT (it would double-log); `POST /_cluster/resync` converges it.
  `op_type=create` uses the coordinator's atomic logical-id reservation and returns 409 without a
  log frame when the id exists; a remote assembly that cannot authoritatively enumerate its
  pre-existing ids refuses create-only writes rather than guessing absence. `refresh=false|true|wait_for`
  are accepted under the stronger publish-before-response model; unsupported write parameters fail
  with 400 instead of being ignored.
- **Per-request `include_broad`** is honored on both `/_search` and `/_mpercolate`.
- **`rank` works (ADR-075)** — the same block as single-node, scored at the shards against the shared
  tag space and merged `(score desc, _id asc)` with `from`/`size` + `_score`. One cluster-specific
  boundary: a **post-freeze (live-added) `priority` tag scores 0** — priority reads the tag's value
  string, which only a build-time interned tag has; boosts fire for both (id-equality). `explain` is
  rejected with 400 — never silently ignored. `profile` works (merged cross-shard `MatchStats`).
  This paragraph describes compatibility `/_search`/`/_mpercolate`.
- **`/v2/_search` uses ADR-110 bounded delivery** — at most K owned hits per routed position,
  exact coordinator merge, honest thresholded totals, current-source fetch for final winners, and
  coordinator-compiled explanations. It defaults `include_source=true`, supports remote shards, and
  fails the whole response on timeout, stale placement, missing source, fetch/protocol failure, or
  enrichment overflow; partial results are unsupported. Strict typed `rank_fields.priority` remains
  signed and available after tag-dict freeze.
- **Compatibility `include_source` defaults to `false`** (`_source` costs a per-hit source probe);
  explicitly requesting it on a remote cluster answers 501. ADR-110 source streaming applies only to
  `/v2/_search`.
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
| `/_cluster/rebalance` | POST | Recompute + commit the shard→node map from membership (HRW, ADR-042). Default (or empty body) is **map-only** — it must NOT be used alone to re-point a populated remote cluster. `{"move": true}` (ADR-090, `--features distributed` only, else 501) additionally MOVES each reassigned position's data via live handoff so routing follows; an optional `"max_parallel": N` runs up to N conflict-free moves concurrently (ADR-095; default 1 = sequential); returns `{acknowledged, moved_data, moved[], failed, not_attempted}` |
| `/_cluster/resize` | POST | Resize the cluster (ADR-078): `{"num_shards": N}` — a blue/green rebuild re-places every live query under a fresh ring; returns `{acknowledged, num_shards, rebuilt}`. In-process only (a non-local cluster → 400); vocab + tags preserved; `O(corpus)` (holds the write lock like `PUT /_vocab`) |
| `/_cluster/resync` | POST | Re-drive queued partial-apply repairs (ADR-047); returns `{repaired, still_pending}` |
| `/_cluster/handoff` | POST | Live data-moving handoff (ADR-044/048/072): `{"position": N, "source": "https://…", "target": "https://…"}` — peer-recover the target, fence + drain the source, flip routing; returns the new `generation`. Fail-closed: an aborted move auto-unfences the source. Requires a `--features distributed` build (else 501). The raw-endpoint primitive; for the map-aware version see `/_cluster/reassign` |
| `/_cluster/reassign` | POST | Data-moving reassignment (ADR-090): `{"position": N, "node": M}` — resolve node M's endpoint from membership, live-handoff the data there, then commit the shard→node assignment (**move-then-commit**), so routing follows live + across a resolve-only restart. Returns `{acknowledged, moved, committed, position, node, generation}`; `committed:false` (200 + `warning`) means the data moved but the durable map commit failed — re-run to reconcile (still zero-FN). Fail-closed (a failed move commits nothing + auto-unfences). Requires a `--features distributed` build (else 501) |
| `/_cluster/reconcile` | POST | One unattended-style reconcile pass (ADR-092): converge the committed shard→node map to the HRW-desired placement by MOVING data, **continuing past per-position failures** (the controller semantics). Idempotent — a converged map moves nothing. An optional `{"max_parallel": N}` body runs up to N conflict-free moves concurrently (ADR-095; empty body = sequential). Returns `{acknowledged, converged, reconciled[], skipped[], uncommitted[], failed[]}` (`acknowledged` is true only when fully converged). The one-shot manual trigger of what the opt-in `--reconcile-interval-secs` loop runs periodically. Requires a `--features distributed` build (else 501) |
| `/_cluster/gc` | POST | One orphan-slot GC sweep (ADR-096): reclaim the fenced, unrouted slots data-moving reassignment strands on their old nodes (slot map + `shard_<id>/` disk). The keep-set is the committed map PLUS live routing (a flip-without-commit source/target is never dropped); unassigned positions are fail-safe skipped; a restarted (unfenced) orphan is fence-armed first. Idempotent; per-slot failures recorded + the sweep continues. Returns `{acknowledged, dropped[], kept_live_routed[], skipped_unassigned[], failed[], skipped_nodes[]}`. The one-shot trigger of the opt-in `--reconcile-gc-orphans` loop epilogue. Requires a `--features distributed` build (else 501) |

`GET /_stats` in cluster mode reports `{shards, replication_factor, total_queries, shard_queries[],
class_counts, epoch, pending_repairs, has_tagged_queries, durable}`; `GET /_health` is green/yellow
(repairs queued)/red (a shard probe failed).
