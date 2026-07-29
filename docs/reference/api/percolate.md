# Percolate — REST API

> Part of the [REST API reference](../api.md). Query language: [`dsl.md`](../dsl.md).

## `POST /v2/_search` — Exact bounded ranked percolation (ADR-107/108/110/127)

Single-node and cluster-coordinator modes serve exact bounded top-K ranking without first
materializing every matching ID. The route accepts exactly one `document`; batching and
approximate `terminated` delivery reject loudly, as does `from` (deep pagination is the
PIT/cursor flow below, ADR-113). Exact exhaustive `all` is deliberately a separate background
job/stream surface below (ADR-114), not a giant `/v2/_search` response. Existing `/_search` and
`/_mpercolate` remain separate compatibility/full-result contracts rather than aliases for this
bounded API. The v2 document is strict and contains only `title`; unknown top-level, document, PIT,
rank, or boost fields are 400 errors rather than ignored input.

```json
{
  "document": {"title": "2024 North Star Wireless Mouse Pro New"},
  "query_scope": "standard",
  "result_mode": "top_k",
  "size": 100,
  "track_total_hits": 10000,
  "rank": {
    "priority_field": "priority",
    "boosts": [{"key": "tenant", "value": "acme", "boost": 1000}]
  },
  "_source": true,
  "explain": false,
  "allow_partial_results": false,
  "timeout": "5s"
}
```

```json
{
  "took": 0,
  "timed_out": false,
  "took_ms": 0.31,
  "complete": true,
  "query_scope": "standard",
  "_shards": {"total": 1, "successful": 1, "failed": 0},
  "hits": {
    "total": {"value": 17, "relation": "eq"},
    "hits": [
      {
        "_id": 42,
        "_score": 1050,
        "_source": {"query": "wireless mouse pro"}
      }
    ]
  }
}
```

The familiar controls `size`, numeric `track_total_hits`, `query_scope`, `explain`, boolean
`_source`, and `timeout` may be supplied in the JSON body or query string. The native aliases
`track_total_hits_up_to` and `timeout_ms` work in either location; `include_source` remains
body-only. Aliases are mutually exclusive, and a control in both body and query string is a 400
even when the values agree. `timeout` is a non-negative integer plus `nanos`, `micros`, `ms`, `s`,
`m`, `h`, or `d`. Boolean `track_total_hits` is deliberately rejected: `true` would promise an
uncapped exact count and `false` would suppress count work, neither of which matches this
endpoint's bounded threshold contract. Unknown query parameters, malformed values, or unsupported
ES/OS search controls are also 400s.

`complete=true` means the exact best K was computed over the selected visibility scope; it does not
mean every true match appears in the page. Winner order is always `(score desc, _id asc)` and
integer addition saturates at the `i64` bounds. Totals are exact while unique matches do not exceed
the selected total threshold; after the next distinct match the result is
`{"value": threshold, "relation": "gte"}`. `size=0` returns no hits but still computes the
thresholded total. `took` is whole milliseconds and `timed_out` is always false on a successful
response; a deadline returns a structured 408 instead of partial hits. `took_ms` is Reverse Rusty's
higher-precision extension. Native v2 keeps numeric `_id` and does not synthesize an `_index`,
because stored queries are logical IDs rather than resources in a caller-selected index.

Defaults are `result_mode="top_k"`, `query_scope="standard"`, `size=100`, `static_v1` ranking with
typed `priority`,
`track_total_hits_up_to=10000`, `include_source=true`, `explain=false`,
`allow_partial_results=false`, and `timeout_ms=5000`. Hard limits are `size <= 10000` and
`track_total_hits_up_to <= 10000`. A native `filter` uses the same tag predicate as compatibility
percolation. Requested source or explanation lookup is fail-closed. The timeout is compute-armed and
includes waiting for the dedicated ranked-search permit; timeout returns 408 and cooperative matching
receives the same deadline.

In cluster mode, ADR-109 ownership is applied before each shard's heap. Every routed logical position
returns at most K sorted owned hits; the coordinator validates disjointness, performs the exact global
merge, and reports routed positions in `_shards` (physical replicas do not inflate the count). Exact
shard totals are summed; `eq` is returned only when every shard is exact and the global sum remains
within the threshold. The coordinator then fetches **current** source only for final winners, grouped
by owning position, and compiles explanations locally. A shard/fetch failure, missing source,
placement-generation drift, timeout, or malformed reply fails the whole response—partial hits never
escape. A source/explanation request takes a request-scoped mutation-frozen cluster view across
matching, winner fetch, and explanation; a same-ID replacement cannot splice its source onto an
older hit. Source-free requests remain concurrent. Enrichment is current-view even under a PIT
(ADR-113): matching, scores, order, and totals are snapshot-stable, but `_source` text is read from
the live store as it exists when the request obtains that fence. A winner deleted before that point
fails its enriched page typed (`include_source: false` pages stay fully pinned).

Winner source text is charged once against `--max-ranked-enrichment-bytes` (default 16 MiB), even when
both `_source` and explanation use it. Exceeding the cap returns `413 rank_enrichment_limit` with no
partial response. Cluster transport/protocol failures return 502; stale placement or unavailable
cluster configuration returns 503. `allow_partial_results=true` remains a 400. Malformed JSON uses
the same structured 400 envelope; missing/wrong content type remains 415 and an oversized body
remains 413 rather than being flattened into a generic validation status.

The optional rank program accepts `profile`, `priority_field="priority"`, and additive integer tag
boosts. `profile` defaults to the built-in `static_v1`; operator-loaded `linear` and
`tree_ensemble` profiles add title-dependent relevance, producing
`profile relevance + priority + matching boosts`. Profile arithmetic is saturating integer math and
ties remain `_id` ascending. Unknown profiles return `unknown_rank_profile`; unknown priority fields
return `unsupported_rank_field`. K bounds retained and returned hits, but every confirmed match is
still scored. Non-static profiles are supported in single-node and in-process cluster modes; a
remote/gRPC coordinator fails with `501 rank_profile_transport_unsupported` rather than changing
scores. See [ADR-162](../../decisions/adr-162-versioned-cpu-ranking-profiles.md).
`result_mode="all"` or `"terminated"`,
`allow_partial_results=true`, `from`, `documents`, and `query` return explicit 400s.

## `POST /v2/_pit`, `DELETE /v2/_pit` — Point-in-time cursor pagination (ADR-113/129/130)

Deep pagination over `/v2/_search` without deep `from`: open a PIT, page with `search_after`
cursors over ONE frozen view, never mixing generations.

```text
POST /v2/_pit?keep_alive=1m             -> open response (below)
POST /v2/_search {..., "pit": {"id": "<pit_id>"}}          -> page 1 + "next_cursor"
POST /v2/_search {..., "cursor": "<next_cursor>"}          -> page N (resend the same request)
DELETE /v2/_pit {"id": "<pit_id>"}      -> ES close spelling
DELETE /v2/_pit {"pit_id": ["<id-1>", "<id-2>"]}           -> OS batch spelling
```

PIT creation accepts the ES/OpenSearch `keep_alive` time value or the native `keep_alive_s`
seconds control, in either the query string or JSON body. An alias pair or one effective control
in both locations is a 400. Time values are non-negative integers followed by `nanos`, `micros`,
`ms`, `s`, `m`, `h`, or `d`. A truly empty body is valid and uses the configured default;
otherwise the body must be JSON. Native `allow_partial_results`, Elasticsearch
`allow_partial_search_results`, and OpenSearch `allow_partial_pit_creation` are equivalent:
`false` is accepted and `true` is a named 400 because a successful PIT always pins every required
logical shard. Unknown body/query controls, malformed values, or unsupported index/routing controls
are 400s. Malformed JSON is a structured 400; a non-JSON body is 415. PIT request bodies over
64 KiB are rejected with 413 before JSON decoding.

A successful open returns one additive response compatible with both identity spellings:

```json
{
  "id": "<opaque token>",
  "pit_id": "<same opaque token>",
  "creation_time": 1785123456789,
  "_shards": {"total": 3, "successful": 3, "skipped": 0, "failed": 0}
}
```

Elasticsearch callers use `id`; OpenSearch and existing Reverse Rusty callers use `pit_id`.
`creation_time` is Unix epoch milliseconds. `_shards` counts logical positions, not physical
replicas: local mode reports one, while a successful in-process cluster open reports every pinned
position. Reverse Rusty retains the unscoped `/v2/_pit` path because it exposes one stored-query
corpus, not caller-selected indices.

PIT close requires exactly one JSON field: Elasticsearch `id`, or OpenSearch/native `pit_id`.
Either spelling accepts one token or a non-empty token array; the array length cannot exceed
`--max-open-pits`. All tokens are authenticated before any registry mutation, so a malformed or
foreign token fails the whole request without closing an earlier valid token. Missing, null,
conflicting, duplicate JSON fields, wrong-type, unknown, and query-string controls are structured
400s; a foreign-process token is 409 `stale_cursor`, a missing or wrong content type is 415, and
a body over the PIT-specific 64 KiB limit is 413 before a scalar or array is materialized.
Delete-all is deliberately unsupported: callers must name the PITs they own, so one client cannot
discard every other client's pinned view.

A successful close returns one additive ES/OpenSearch/native response:

```json
{
  "closed": true,
  "succeeded": true,
  "num_freed": 1,
  "pits": [
    {"successful": true, "pit_id": "<id-1>"}
  ]
}
```

`closed` is the original aggregate result and is true only when every requested PIT was live and
closed. `pits` preserves request order and reports that result per token. `succeeded` means every
existing search context named by the request was closed; a structurally valid, already-gone PIT
therefore returns HTTP 200 with `closed: false`, `succeeded: true`, `num_freed: 0`, and
`successful: false`. `num_freed` counts contexts actually released: one per live PIT locally, or
one per logical shard position per live PIT in coordinator mode; replicas never inflate it.

A PIT pins the engine snapshot (single-node) or every shard position's snapshot (in-process
cluster) for a renew-on-use keep-alive: default `--pit-default-keep-alive-secs` (60), ceiling
`--pit-max-keep-alive-secs` (600, over-ask is a 400), at most `--max-open-pits` (64) concurrently
open — a breach is **429 `pit_limit_exceeded`**, never an eviction. Every use (open, page,
cursor) renews the deadline; abandoned PITs expire; `DELETE` frees immediately (`closed: false`
when already gone — the goal state either way). Open PITs retain memory (the pinned memtable
copy) and, after compaction, disk (unlinked-but-mapped segments) until released; the `open_pits`
gauge tracks them.

Cursor rules: a FULL page (`hits.length == size`, `size > 0`) returns `next_cursor`; a short page
ends the stream (no cursor). The client resends the **same** `document`/`query_scope`/`rank`/
`filter` with each cursor — matcher semantics and any non-static title-feature tuple are
fingerprinted into the token, and a drifted resend is a 400
`cursor_mismatch`; `size`, `timeout_ms`, and `track_total_hits_up_to` may vary per page. Totals
are page-invariant (every page of one PIT reports the identical total). `pit` + `cursor` together
is a 400. Concatenating pages yields exactly the one-shot ranked result over the same PIT — no
duplicates, no gaps.

Fail-closed staleness — **409 `stale_cursor`** (the one deliberate read-surface 409; the pinned
generation is unrecoverable, so open a new PIT and restart rather than retrying): an expired or
closed PIT, a server restart (tokens are HMAC-signed with a per-process key), and — in cluster
mode — any placement change (`resize`, vocabulary rebuild) or a primary failover (PIT reads are
primary-only, never silently failed over). Structurally garbled tokens are 400s. A remote/gRPC
coordinator assembly refuses PIT entirely with **501 `pit_unsupported`** (wire PIT is a later
increment; page via an in-process cluster or single-node mode). Both endpoints ride the open
search auth allowlist.

## `POST /_percolate/jobs` — Exact exhaustive delivery (ADR-114/131)

An exhaustive result can be arbitrarily large, so `result_mode="all"` is a background job with
bounded provisional chunks and a required terminal completion record. The minimal request is:

```json
{
  "document": {"title": "2024 North Star Wireless Mouse Pro New"}
}
```

The full native shape remains available:

```json
{
  "event_id": "listing-123/version-7",
  "document": {"title": "2024 North Star Wireless Mouse Pro New"},
  "query_scope": "with_broad",
  "result_mode": "all",
  "filter": {"tenant": ["acme"]},
  "rank": {
    "priority_field": "priority",
    "boosts": [{"key": "tier", "value": "gold", "boost": 25000}]
  },
  "sink": {"type": "grpc_stream"},
  "timeout_ms": 60000,
  "allow_partial_results": false
}
```

One `document` is required. `result_mode` defaults to `"all"` and no other value is supported;
`sink` defaults to the HTTP `"ndjson_stream"` and may explicitly name `"ndjson_stream"` or the
historic `"grpc_stream"` alias. `query_scope` defaults to `"standard"`; `rank` and `filter` are
optional. The JSON request schema and its typed nested objects are strict: unknown fields,
duplicate schema controls, explicit nulls, malformed JSON, and wrong types are structured 400
errors. Missing/wrong JSON content type is 415, and the endpoint rejects a body larger than 1 MiB
with 413 before JSON deserialization.

Named non-static profiles execute in single-node and in-process cluster jobs. A remote/gRPC job
cannot carry the model program: it may be admitted asynchronously, then records a terminal
`rank_profile_transport_unsupported` failure before any rank RPC. Use `static_v1` in that topology.

The execution deadline can be supplied in the body or query string as native `timeout_ms` or as
ES/OpenSearch-style `timeout`, but aliases and locations are mutually exclusive. `timeout` is a
non-negative integer followed by `nanos`, `micros`, `ms`, `s`, `m`, `h`, or `d`; the effective job
timeout must be positive and no greater than `--exhaustive-job-timeout-secs`. Native
`allow_partial_results` and Elasticsearch `allow_partial_search_results` are likewise
body-or-query aliases: false or omission is accepted, while true is rejected because exhaustive
delivery cannot certify a partial set.

Async-search controls `wait_for_completion_timeout`, `keep_alive`, and `keep_on_completion` are
recognized only to return a clear 400. Jobs always return immediately, and their in-memory status
is retained by a bounded record-count policy rather than a caller-selected duration. In remote
mode, every shard independently rejects a remaining budget above its server-owned
`--max-exhaustive-stream-secs` ceiling (default 300 seconds), before claiming a node worker permit.
Set that shard ceiling at least as high as the coordinator job ceiling.

A successful admission returns `202 Accepted`:

```json
{
  "id": "7fcaa575-beb7-4c6f-a27c-9be901aa7d86",
  "job_id": "7fcaa575-beb7-4c6f-a27c-9be901aa7d86",
  "event_id": "listing-123/version-7",
  "state": "running",
  "is_running": true,
  "is_partial": true,
  "start_time_in_millis": 1785096000123,
  "snapshot_generation": 987654321012345678,
  "status_url": "/_percolate/jobs/7fcaa575-beb7-4c6f-a27c-9be901aa7d86",
  "stream_url": "/_percolate/jobs/7fcaa575-beb7-4c6f-a27c-9be901aa7d86/stream",
  "reused": false
}
```

`id` is the ES/OpenSearch-familiar alias of native `job_id`. `state` retains the native lowercase
enum. `is_running` is true only in `running`; `is_partial` is true until and unless the completion
frame commits the exact result, including for failed/cancelled jobs. `start_time_in_millis` is Unix
epoch milliseconds. There is no `expiration_time_in_millis`: execution timeout is not retained
result expiry.

`event_id` is optional. When supplied, it must contain 1–512 bytes and is the POST idempotency key
while the record is retained. Repeating the same effective request returns the same job/generation
with `reused=true`; defaults and unordered collections are canonicalized first. When omitted, the
server generates and returns one; a retry made before receiving that generated value starts a
different job, so clients that need retry safety should supply their own stable key. For example,
omitted versus explicit
`query_scope: "standard"`, default priority/default timeout, reordered filter values or effective
boosts, and the accepted `grpc_stream`/`ndjson_stream` spellings are equivalent. Reusing an event
id for different execution semantics returns `409 event_id_conflict`. Canonicalization uses stable
raw tag key/value groups and a last-write-wins boost map, so interning a previously unknown tag
after the first request does not change a retained event's identity. Distinct boost pairs that
resolve to the same synthetic tag id are rejected as ambiguous with 400. Exhaustive execution uses
a dedicated worker pool and non-queuing permit: no permit is `503 exhaustive_capacity`; a registry
full of active jobs is `429 exhaustive_registry_full`.
Rejected admission never evicts retained history: the server claims an execution permit before it
prunes a terminal record to make room for the admitted replacement.
`snapshot_generation` is an opaque boot-namespaced `u64`, not a counter clients should order or
predict. A fresh process starts from a new random namespace so a retry after restart cannot reuse
the prior process's member idempotency keys.

### Status, stream, and cancellation

`GET /_percolate/jobs/{id}` returns one strict, no-store status response. It preserves native
`job_id`, `event_id`, lowercase `state`, `query_scope`, `snapshot_generation`,
`created_unix_ms`, and terminal `completed_unix_ms`, and adds familiar `id == job_id`,
`is_running`, `is_partial`, `start_time_in_millis`, and terminal
`completion_time_in_millis`. A running response is partial. Only a successfully completed job is
non-partial and carries `exact_total`, `chunk_count`, and `checksum`; failed/cancelled jobs instead
carry native `failure` plus a structured `error`.

```json
{
  "id": "7ae2...",
  "job_id": "7ae2...",
  "event_id": "catalog-refresh-42",
  "state": "completed",
  "is_running": false,
  "is_partial": false,
  "query_scope": "standard",
  "snapshot_generation": 987654321012345678,
  "start_time_in_millis": 1785148000123,
  "created_unix_ms": 1785148000123,
  "completion_time_in_millis": 1785148000456,
  "completed_unix_ms": 1785148000456,
  "exact_total": 2,
  "chunk_count": 1,
  "checksum": {"xor": 1190750903085048104, "sum": 8313222029812487130}
}
```

The optional `wait_for_completion_timeout=<integer><unit>` query (`nanos`, `micros`, `ms`, `s`,
`m`, `h`, or `d`) waits for terminal status up to the configured exhaustive-job maximum; omission
or `0s` returns immediately. It never claims the stream. A job whose terminal frame has no
concurrent consumer therefore remains `running` until the wait expires. `keep_alive` returns 400
because status retention is in memory and count-bounded, not client-selected time retention.
Unknown, duplicate, malformed, and over-limit query controls return the standard structured 400
envelope. No `expiration_time_in_millis` or `completion_status` is emitted because neither has a
truthful native value (ADR-132).

`completed` is published only after the stream dequeues its terminal completion frame, not when
the worker merely places those bytes in the bounded queue. If a claimed response is dropped while
that frame is still queued, the job becomes `failed` with no summary; the retained event may not
misrepresent a truncated single-consumer stream as complete.
Cancellation, deadline expiry, and terminal dequeue are arbitrated by one terminal transition:
once cancellation or expiry wins, a concurrent dequeue cannot expose completion bytes; once
delivery wins, a later cancellation is a no-op. Any other earlier invalidation is equally final:
for example, DELETE cannot relabel an already-dropped completion frame from `not consumed` to
`cancelled`.

`GET /_percolate/jobs/{id}/stream` is a native, single-consumer result protocol rather than an
Elasticsearch/OpenSearch retained async-search response. Those systems return one JSON result and
have no truthful equivalent for provisional chunks committed by a terminal checksum; their
retention and wait controls therefore belong neither on this route nor in its response (ADR-134).

The route accepts no query parameters. A non-empty query string—including malformed encoding—is
rejected with structured 400 validation before the stream is claimed. Every non-GET method is
rejected first with 405 and `Allow: GET`; notably, even a `HEAD` probe with an invalid query cannot
consume the claim. An unknown id returns `404 job_not_found`; a second GET returns
`409 stream_already_claimed`. A successful GET has already claimed the one consumer and returns `200`,
`Content-Type: application/x-ndjson`, and `Cache-Control: no-store`. Every frame is one UTF-8 JSON
object terminated by `\n`:

```json
{"type":"match_chunk","job_id":"...","sequence":0,"members":[
  {"logical_id":42,"score":1050,"idempotency_key":"<sha256-hex>"},
  {"logical_id":91,"idempotency_key":"<sha256-hex>"}
]}
{"type":"completion","job_id":"...","exact_total":2,"snapshot_generation":987654321012345678,"chunk_count":1,"checksum":{"xor":1190750903085048104,"sum":8313222029812487130}}
```

Sequences start at zero and are contiguous. A member has `score` only when the request supplied a
rank program. Its idempotency key is derived from
`(event_id, snapshot_generation, logical_id)`. Chunks are provisional and have no global ordering
guarantee: a consumer deduplicates by key, verifies the exact total/checksum, and commits **only**
after `completion`. A stream may end after provisional chunks because of cancellation, deadline,
disconnect, shard/protocol failure, or server restart; none of those cases emits completion.
Dropping a claimed response drops the receiver; if completion has not been dequeued, status becomes
`failed` and exposes no exact summary.
The checksum includes score presence as a separate domain, so an absent score cannot attest as any
valid signed score value.
The optional best-effort `failure` frame is diagnostic only—the status endpoint is authoritative.

`DELETE /_percolate/jobs/{id}` accepts no query parameters. For a running job it requests
cooperative cancellation and returns the prior native status snapshot plus `id == job_id`,
`acknowledged: true`, and `deleted: false`:

```json
{
  "acknowledged": true,
  "deleted": false,
  "id": "7ae2...",
  "job_id": "7ae2...",
  "event_id": "catalog-refresh-42",
  "state": "running",
  "query_scope": "standard",
  "snapshot_generation": 987654321012345678,
  "created_unix_ms": 1785148000123
}
```

Poll GET until the state becomes `cancelled` or another terminal state; a running state in the
immediate DELETE response means the worker has not reached its next bounded poll yet. DELETE that
terminal record again to atomically remove its retained job and event-id entries. The terminal
response preserves its final native fields and returns `acknowledged: true`, `deleted: true`;
subsequent status, stream, and DELETE calls return `404 job_not_found`. Releasing the event entry
allows a later POST to reuse that event id for a new request. Successful responses are
`Cache-Control: no-store`; unknown or malformed query fields return the structured 400 envelope
(ADR-133).

Cancellation is checked even when the match has not emitted a chunk, while waiting for the cluster
write barrier, and inside large candidate postings or a long legacy duplicate-version scan. With
bearer auth enabled,
create/status/stream are read surfaces (unless
`--auth-protect-reads` is set), while DELETE is protected.

Jobs and stream buffers are in memory. Restart loses them. The repository provides the
`BrokerPublisher`/`publish_at_least_once` library seam for an external integration, but the shipped
server has no Kafka, Pub/Sub, SQS, JetStream, or other durable broker adapter or broker-selection
flag. An integration using that seam retries the same key and payload so consumers can deduplicate.
In cluster mode, ownership makes shard streams disjoint and every shard summary is validated before
the terminal job completion. The coordinator mutation barrier serializes successful shard mutations
and repair re-drives across that exact execution view (including direct library callers), so a long
or backpressured exhaustive job can delay cluster writes; size the dedicated quota and timeout
accordingly. Mutations and repair re-drives acquire that barrier before any logical-id stripe, so
an exhaustive writer cannot form a lock-order cycle with `resync`. Full HTTP/gRPC channel waits are
bounded by that job/request deadline. Shard nodes independently admit a bounded number of
`PercolateAll` workers before spawning them; direct excess receives gRPC `RESOURCE_EXHAUSTED`
rather than consuming the global blocking pool. While a blocking closure is still queued, its
response sender is revocable on deadline/disconnect but its permit remains attached to the
closure until Tokio schedules it. The configured concurrency bound therefore also bounds dormant
closures in the global blocking pool instead of letting expired requests recycle permits and
enqueue unbounded replacements. Once a closure starts, an explicit signal drops the watcher
sender so a successful terminal summary is followed immediately by EOF.

Remote exact delivery also requires an exclusive coordinator assembly:
`connect_remote_exclusive` / `connect_replicated_exclusive` with one non-zero ID retained across
retries. The server's HTTP cluster connector selects this mode automatically. The first validated
exclusive `AdoptDict`/`AddShard` claims each node, all replies attest that identity, and every later
RPC from another or unstamped coordinator fails with `FAILED_PRECONDITION`. A pre-lease shard
binary attests zero and is refused. This cluster-wide fence is required because two fresh
process-local barriers could otherwise both certify the same empty shard set. The historical
library builders `connect_remote` / `connect_replicated` stay unleased for compatibility, but
their exhaustive call fails before its first chunk; once an exclusive coordinator claims a node,
those unleased clients are fenced there too.

The owner lease is renewable and bounded (30 seconds): every admitted owner RPC renews it. A
different ID is rejected while that lease is live; after it expires, an explicit claim handshake
may replace it only after all already-admitted response bodies and streams drain. A stateless
coordinator restart can therefore require retries through the bounded lease window plus the drain
time of already-admitted work, instead of leaving nodes permanently pinned to the prior boot ID. A
durable shard-process restart clears its
process-local lease; an existing `RemoteShard` automatically performs a claim-stamped,
read-only `DictFingerprint` handshake, verifies the restored node configuration, and retries the
rejected RPC once. That recovery never creates an empty slot. These lifecycle repairs do not make
a fresh in-memory coordinator's convergence history authoritative: rebuild fresh slots from the
authoritative corpus before that restarted coordinator requests exact delivery.
A cluster with
queued partial-apply repairs is not ownership-disjoint: the job fails
without completion (and, when the repair was already queued, without provisional chunks). Run
`POST /_cluster/resync`, verify `pending_repairs=0`, and retry. A newly restarted in-memory
coordinator attached to already-populated remote shards is also refused even when that fresh
counter is zero: it cannot attest that an earlier coordinator left no partial apply. `resync`
cannot reconstruct unknown history in that shape; rebuild fresh shard slots from the authoritative
corpus before requesting exact exhaustive completion.

## `GET|POST /_search` — Percolate titles

Match a single title against all stored queries. Both methods accept the same JSON body; `POST` is
usually friendlier to proxies, while `GET` matches the Elasticsearch/OpenSearch search-method
surface.

```bash
curl -X POST localhost:9200/_search \
  -H 'Content-Type: application/json' \
  -d '{"document": {"title": "Dell XPS 15 Laptop 16GB RAM 512GB SSD New"}}'
```

```json
{
  "took": 0,
  "timed_out": false,
  "took_ms": 0.42,
  "hits": {
    "total": 1,
    "hits": [
      {"_index": "queries", "_id": 1, "_source": {"query": "dell laptop"}}
    ]
  }
}
```

The JSON body accepts these Reverse Rusty controls:

| Field | Default | Description |
|---|---|---|
| `timeout_ms` | 30000 | Native millisecond timeout alias; returns 408 on expiry. Mutually exclusive with `timeout`. |
| `timeout` | `30s` | ES/OS integer time value with `nanos`, `micros`, `ms`, `s`, `m`, `h`, or `d`; returns 408 on expiry. |
| `size` | 1000 | Maximum number of hits to return (per slot in multi-doc mode). |
| `from` | 0 | Offset into the result set for pagination. |
| `rank` | – | Optional ranking block (ADR-059) — order hits by a priority tag and/or request boosts before `from`/`size`. See [Ranking](#ranking-adr-059). |
| `include_broad` | server default (`--include-broad`) | Per-request override: evaluate class C and accepted class D for this request. Class H remains always visible. |
| `include_source` / `_source` | `true` single-node; `false` cluster | Include original query text in each hit. These are aliases and cannot both be present. An explicit `true` works for an in-process cluster; a remote/gRPC cluster returns 501 because its source-fetch wire is not implemented. |
| `explain` | `false` | Attach `_explanation` to each hit for a single native/ES `document`. Multi-document requests return 400 because one union hit can match several input titles. Cluster mode currently returns 400. |
| `profile` | `false` | Include top-level candidate/posting statistics. Multi-document profile statistics are the sum of the per-slot `stats`. |

The ES/OS controls `from`, `size`, `explain`, `profile`, `_source`, and `timeout` may instead be
placed in the query string. A control cannot appear in both locations, even with the same value.
Unknown body fields, query parameters, `rank` fields, boost fields, and document fields return a
structured 400 instead of being silently ignored. `rank`, `include_broad`, `include_source`, and
`timeout_ms` are body-only Reverse Rusty extensions.

`hits.total` is deliberately the legacy integer rather than the newer ES object; it always reflects
the full match count, while `hits.hits` is the paginated window. Every hit carries the stable
`_index: "queries"` identity. `took` is whole milliseconds, `took_ms` is a higher-precision Reverse
Rusty extension, and `timed_out` is always false on a successful response because expiry returns 408
without partial hits. `_shards` is omitted: the compatibility endpoint does not synthesize ES shard
accounting from content-routed positions.

Set `_source: false` (or `include_source: false`) to skip query text lookup. Compatibility cluster
endpoints default it to false so remote clusters remain usable without a source-fetch round trip.
If source or explanation enrichment was requested but is unavailable for a confirmed hit, the whole
request fails (`500 source_unavailable`/`explanation_unavailable` locally; a missing in-process
cluster source is `502`). Matching, ranking, source, and explanation use one exact snapshot
generation. A concurrent replacement can therefore make old enrichment unavailable, but can never
splice the replacement's source onto the older match. In coordinator mode, a request that asks for
sources takes the core mutation-frozen read view through matching and source cloning. Direct
`ClusterEngine` writes and REST writes both wait for that short view; source-free searches keep the
unfenced concurrent-read path.

> **An explicit `timeout` or `timeout_ms` is also a compute budget (ADR-099/123).** On expiry the
> request returns `408` as always, and — when the request set either timeout
> explicitly — the dispatched match work now **cancels itself cooperatively** at
> per-title/segment boundaries and at a fixed interval through dense posting,
> candidate, and canonical-body loops instead of burning the Rayon pool to completion.
> Results are never partial: a cancelled match returns nothing (the same
> 408), never a truncated union. Requests that omit both controls keep the implicit
> 30 s **response** deadline only (the unarmed sampler compiles away and the hot path
> carries zero deadline reads);
> the kill-switch is the dynamic `cooperative_cancel` setting. To bound *how many*
> searches occupy the pool at once, start the server with
> `--max-concurrent-searches N` (excess requests queue within their own timeout).
> Cancellations are counted in `match_cancellations_total{endpoint}`. The same
> applies to `/_mpercolate`.

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
  "took": 0,
  "timed_out": false,
  "took_ms": 0.87,
  "hits": {
    "total": 2,
    "hits": [
      {"_index": "queries", "_id": 1, "_source": {"query": "dell laptop"}},
      {"_index": "queries", "_id": 2, "_source": {"query": "leather jacket"}}
    ]
  },
  "slots": [
    {
      "slot": 0,
      "total": 1,
      "hits": [
        {"_index": "queries", "_id": 1, "_source": {"query": "dell laptop"}}
      ],
      "stats": {
        "unique_candidates": 15,
        "broad_candidates": 0,
        "postings_scanned": 47,
        "matches": 1,
        "probes_attempted": 28,
        "probes_skipped": 12
      }
    },
    {
      "slot": 1,
      "total": 1,
      "hits": [
        {"_index": "queries", "_id": 2, "_source": {"query": "leather jacket"}}
      ],
      "stats": {
        "unique_candidates": 9,
        "broad_candidates": 0,
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
from the index (`broad_candidates` is the subset that came from the quarantined broad lane), how many
posting lists were scanned, how many bloom-filter probes were skipped, and how many candidates
survived to become confirmed matches. See [`../design/matching.md`](../../design/matching.md) §6
for per-query match tracing.
For a stored quoted clause, `_explanation.required_phrases` or
`_explanation.forbidden_phrases` contains its analyzed `positions` and
`arcs: [{start, end, alternatives}]`. A separated required path reports
`required_phrase[N] not contiguous`; a present forbidden path reports
`forbidden_phrase[N] present` (ADR-120).

### Filtered percolation (ADR-049)

The dominant production read pattern is *"percolate, then narrow to one category."* Attach a tag filter
to a percolate request to keep only the matches whose stored query carries the requested
[metadata tags](documents.md#per-query-metadata-tags-adr-049). The filter is a **conjunction across
keys** (AND) of **value sets** (OR within a key). It intentionally narrows the exact Boolean-match
set and is evaluated during verification; it never participates in semantic candidate retrieval.
A filter value never seen at ingest matches nothing (the safe `terms` semantics). Filter values
take the **same canonical scalar coercion as ingest** (ADR-073):
strings, numbers, and bools are accepted everywhere a value is (`{"category": 7}` matches a tag
ingested as `7` or `"7"`); a `null`, object, or nested array anywhere in a filter is a loud **400** —
an unanswerable predicate is never silently dropped (which would *widen* the result set). Two
equivalent shapes are accepted:

**Native** — a `filter` block alongside `document`/`documents`:

```bash
curl -X POST localhost:9200/_search -H 'Content-Type: application/json' -d '{
  "document": {"title": "Dell XPS 15 Laptop 16GB RAM New"},
  "filter": {"category": ["electronics", "computers"], "status": "active"}
}'
```

**Elasticsearch `bool`/`terms` percolate envelope** — for compatibility with existing percolate
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

Only the `percolate` + `bool.must`/`bool.filter(terms/term)` subset is supported. The percolate clause
must name `field: "query"` and exactly one of `document` or `documents`; each document contains only
a string `title`. A `term`/`terms` clause names exactly one tag field, and `terms` values are an array.
The native `document`/`documents`/`filter` shape cannot be mixed with `query`. Unsupported siblings,
options, or clauses (for example `should`, `must_not`, `match`, or `range`) return **400** rather than
silently widening or changing the request. `/_mpercolate` accepts the same `filter` block and ES
envelope (applied to every document in the batch).

### Ranking (ADR-059)

By default hits come back in the engine's order (a boolean candidate set — the engine is a recall-first
matcher, not a ranker). Attach an optional `rank` block to **order** the hits before pagination. Ranking
is a pure post-match step: it only reorders + paginates the already-final set — it never adds or drops a
match. A `rank` block has two optional parts:

- **`priority_key`** — the name of a [tag](documents.md#per-query-metadata-tags-adr-049) whose **numeric
  value** is the query's base priority (a query tagged `priority=50` scores 50; a non-numeric or absent
  value scores 0). An empty string means "no priority term" — identical to omitting the field — on every
  path (single-node, in-process cluster, and over gRPC, whose wire encodes the absent key as `""`).
- **`boosts`** — a list of `{key, value, boost}` entries; a query scores `+boost` for each `(key, value)`
  tag it carries.

The score is **additive** — `score = Σ matched boosts + priority` — and hits are ordered by `score`
descending, ties broken by ascending `_id` (a stable, repeatable order for pagination). Each hit then
carries a `_score` field (present only when a `rank` block was supplied). Want a boost to always
outrank priority? Choose boost magnitudes above your priority range.

```bash
curl -X POST localhost:9200/_search -H 'Content-Type: application/json' -d '{
  "document": {"title": "2020 Acme Labs Chrome Pro New"},
  "filter": {"category": "items"},
  "size": 20,
  "rank": {
    "priority_key": "priority",
    "boosts": [{"key": "tier", "value": "gold", "boost": 100}]
  }
}'
```

```json
{
  "took": 0,
  "timed_out": false,
  "took_ms": 0.31,
  "hits": {
    "total": 3,
    "hits": [
      {"_index": "queries", "_id": 1, "_score": 110, "_source": {"query": "acme labs chrome"}},
      {"_index": "queries", "_id": 3, "_score": 100, "_source": {"query": "acme labs chrome pro"}},
      {"_index": "queries", "_id": 2, "_score": 50,  "_source": {"query": "acme labs chrome new"}}
    ]
  }
}
```

`rank` works on `/_search` (single + multi-document) and `/_mpercolate` (each document's hits ranked
independently), composes with `filter`, and is **opt-in**: with no `rank` block the response is
identical to the unranked path — no `_score` field, engine order preserved. Compatibility cluster endpoints
use ADR-075 rank-at-shard/full-union merge; `/v2/_search` uses ADR-110's bounded exact merge.

## `POST /v2/_mpercolate` — Exact bounded ranked batch (ADR-112/128)

The batch counterpart to `/v2/_search`: one shared parameter set + `documents[]`, one exact bounded
top-K result per document (`responses[i]` corresponds to `documents[i]`), evaluated through the
columnar batch kernel — in coordinator mode with ONE `PercolateTopKBatch` call per involved shard
instead of a per-document fan.

```bash
curl -X POST localhost:9200/v2/_mpercolate \
  -H 'Content-Type: application/json' \
  -d '{
    "documents": [{"title": "2024 north star wireless mouse pro new"},
                  {"title": "generic unmatched listing"}],
    "query_scope": "standard",
    "size": 10,
    "track_total_hits": 10000,
    "rank": {"priority_field": "priority"},
    "_source": true,
    "timeout": "30s",
    "allow_partial_search_results": false
  }'
```

Response: `{took, took_ms, complete, query_scope, responses: [{timed_out, status, _shards, hits:
{total, hits: [{_id, _score, _source?}]}}]}` — `took` is the whole-millisecond batch duration and
`took_ms` is the higher-precision extension. A successful slot reports `timed_out: false` and
`status: 200`; per-slot `_shards.total` is that document's routed fan-out, and totals carry the same
`eq`/`gte` honesty as `/v2/_search`. Empty `documents` is a 200 with empty `responses`.

Semantics and bounds:

- **Shared options.** `query_scope`, `size`, `track_total_hits_up_to`, `rank`, `filter`,
  `include_source`, and `timeout_ms` apply to every slot (per-document options are a named 400;
  heterogeneous-K callers split batches). Numeric `track_total_hits`, Boolean `_source`, and
  time-value `timeout` are mutually-exclusive ES/OS aliases for the corresponding native controls.
  `allow_partial_search_results: false` aliases native `allow_partial_results: false`; `true` is a
  named 400 because the endpoint never returns partial success. Defaults match `/v2/_search`, except
  timeout defaults to 30 seconds (the v1 batch default).
- **Strict boundary.** Unknown top-level, document, rank, or boost fields and every query-string
  parameter are structured 400s. Malformed/type-invalid JSON is a structured 400; body-size and
  content-type failures retain 413 and 415. Boolean `track_total_hits`, `_source` field filters, and
  duplicate alias pairs are rejected rather than approximated.
- **`explain: true` is not supported here** (a named 400; `false` is accepted) — per-(document,
  winner) explanation compilation is antithetical to the throughput path; use `/v2/_search` for
  one document.
- **`pit`/`cursor` are not supported here** (named 400s, ADR-113) — batch cursor pagination is a
  [roadmap item](../../roadmap.md#api-and-operator-ergonomics); page per title via `/v2/_search`.
- **Admission**: batch length ≤ min(`max_percolate_batch`, 10 000) and `size × documents ≤ 2^20`
  (the aggregate collector heap budget), both rejected as `rank_admission_rejected` before any
  matching.
- **Winner `_source`** is fetched once per distinct winner across the whole batch and charged per
  DELIVERED occurrence against the same 16 MiB credit as `/v2/_search`
  (`--max-ranked-enrichment-bytes`); overflow is a whole-request 413.
- **No partial results**: one absolute deadline covers routing, matching, merge, and enrichment —
  expiry is a whole-batch 408; any shard/enrichment failure fails the whole request (the same
  status mapping as `/v2/_search`).
- **ES/OS boundary:** this native endpoint deliberately keeps a JSON `documents[]` envelope and one
  shared option set, not the alternating NDJSON metadata/search lines or independent per-search
  failures of Elasticsearch/OpenSearch `_msearch` (ADR-128). Source-enriched cluster batches hold
  one mutation-frozen read view across matching and union fetch; source-free batches stay
  concurrent.
- **Auth boundary:** when a bearer token is configured, this POST currently requires it even with
  `--auth-protect-reads=false`; unlike `/v2/_search`, it is not on the read-via-POST allowlist.

## `POST /_mpercolate` — Batch percolate (high throughput)

The full-result throughput counterpart to `/_search` (ADR-135). It accepts one strict JSON request
with a shared option set and returns one ordered `responses[i]` slot per input document.
In **standalone mode**, it evaluates the broad lane once per title batch with the columnar kernel
(ADR-026), so a hot anchor's large posting is scanned once for the batch rather than once per title.
Coordinator mode preserves the same exact per-slot semantics but fans out one per-title match; it
does not claim the standalone columnar amortization.

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
    "_source": true,
    "timeout": "2s",
    "allow_partial_search_results": false,
    "profile": true
  }'
```

```json
{
  "took": 0,
  "took_ms": 0.91,
  "responses": [
    {
      "timed_out": false,
      "status": 200,
      "hits": {
        "total": 1,
        "hits": [{"_index": "queries", "_id": 1, "_source": {"query": "dell laptop"}}]
      }
    },
    {
      "timed_out": false,
      "status": 200,
      "hits": {
        "total": 1,
        "hits": [{"_index": "queries", "_id": 2, "_source": {"query": "leather jacket"}}]
      }
    },
    {"timed_out": false, "status": 200, "hits": {"total": 0, "hits": []}}
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

The request must choose exactly one document shape:

- Native: `documents: [{"title":"..."}, ...]`, optionally with a top-level native `filter`.
- ES/OS-familiar: the strict `query.percolate` or `query.bool` subset documented for
  [`/_search`](#getpost-_search--percolate-titles), using `field: "query"` and either `document`
  or `documents`.

The shapes cannot be mixed. The top-level body, each native document, the percolate query, the
ranking block, and the query string are strict: unsupported fields and every query parameter return
a structured `400` instead of being ignored. The media type is `application/json`; malformed JSON
returns structured `400`, an oversized payload preserves `413`, and a missing/wrong JSON content type
preserves `415`.

Shared request fields:

| Field | Default | Description |
|---|---|---|
| `include_broad` | server default (`--include-broad`) | Per-request override: evaluate class C and accepted class D for this batch. Class H remains always visible |
| `include_source` / `_source` | `true` standalone; `false` cluster | Boolean aliases controlling stored query text. Specify at most one. An explicit `true` works for an in-process cluster; a remote/gRPC cluster returns 501 |
| `size` | 1000 | Maximum hits per document |
| `from` | 0 | Per-document offset into each document's hits for pagination |
| `rank` | – | Optional ranking block (ADR-059), applied per document — see [Ranking](#ranking-adr-059) |
| `timeout_ms` / `timeout` | 30000 ms | Native milliseconds or an ES/OS time value such as `250ms` or `2s`; specify at most one. Expiry returns whole-request 408 and an explicit value arms cooperative cancellation (ADR-099) |
| `profile` | false | Standalone only: include the top-level columnar `broad` summary. A coordinator returns `501 profile_unsupported` for `true`; `false` is accepted |
| `explain` | false | `false` is accepted; `true` returns 400 and directs the caller to `/_search` per document |
| `allow_partial_search_results` | false | `false` names the actual fail-closed contract; `true` returns 400 |

Every successful slot has `timed_out: false`, `status: 200`, and a `hits` object. Its exact matched
IDs, total, ranking, page, and source projection are the same as a corresponding per-title search.
Standalone source enrichment stays on the exact snapshot used for matching and fails with
`source_unavailable` rather than attaching text from a concurrent replacement. Cluster source
enrichment retains its mutation-fenced read view. An empty native `documents` array is a valid no-op
(`200` with `responses: []`); a missing document shape is a `400`.

This is **not** the Elasticsearch/OpenSearch multi-search wire format. Their current `_msearch`
endpoints use alternating NDJSON metadata/query lines and may return independent slot errors.
Current ES/OS multi-document percolation also returns union hits with
`_percolator_document_slot`; Reverse Rusty deliberately returns one independent response per input
so the standalone batch kernel can share work. NDJSON, per-document control sets, and partial slot
success are rejected rather than imitated incompletely.

**When to use which.** Use standalone `/_mpercolate` for high-throughput batch/streaming
percolation, especially with broad queries enabled. Both endpoints support `size`/`from` and
`rank`; use `/_search` for rich per-document `stats`, explanations, or profiles. The standalone
batch endpoint deliberately exposes only its aggregate broad summary, while the coordinator names
that unavailable columnar profile instead of returning misleading zeros.
