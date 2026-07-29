# `POST /_percolate/jobs` — Exact exhaustive delivery (ADR-114/131)

> [Percolation & delivery APIs](../percolate.md) · [REST API hub](../../api.md)

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
    "profile": "linear_v1",
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

The full-shape example assumes `linear_v1` has been loaded from the checked-in example registry.
Named-profile selection and fail-closed terminal attestation follow the canonical
[ranking contract](../../ranking.md); profile or transport failures never produce a successful partial
completion.

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
