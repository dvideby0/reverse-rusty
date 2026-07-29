# ADR-161: Checkpoint REST API contract

> [Ingestion, storage & durability decisions](areas/ingestion-storage-and-durability.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

`POST /_checkpoint` is the in-process cluster's full durability commit: it seals or reseals every
logical shard position, atomically commits the coordinator manifest and source/segment registry,
advances the checkpoint epoch, and only then permits the captured mutation-log prefix and orphaned
segments to be reclaimed (ADR-031/032/121). The original REST handler ran that blocking disk work
directly on a Tokio request worker, accepted unchecked query parameters and the server-wide 100 MiB
body allowance, returned only `acknowledged` and `epoch`, and had no endpoint metrics, cache policy,
bounded admission, slow-body deadline, or disconnect-safe completion reporting.

The small response also hid a deployment boundary. `ClusterEngine::checkpoint` deliberately takes
a nondurable path when the coordinator has no `data_dir`: an in-memory cluster performs only
process-local logical-ID maintenance, while a stateless remote coordinator neither seals remote
shard nodes nor creates a cross-shard recovery point. Returning the same unexplained
`acknowledged: true` in all three cases invited clients to infer durability that did not exist.

## Compatibility boundary

Elasticsearch and OpenSearch expose `GET`/`POST /_flush`, with `force` and `wait_if_ongoing`
controls and a shard-result response
([Elasticsearch Flush API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-flush),
[OpenSearch Flush API](https://docs.opensearch.org/latest/api-reference/index-apis/flush/)).
Reverse Rusty already maps that contract honestly to `/_flush` (ADR-137). Its checkpoint additionally
commits Reverse Rusty-specific coordinator state and log cursors; neither product has a corresponding
indexless endpoint. Keep the native `/_checkpoint` spelling and do not fabricate a second flush
alias or adopt flush controls that would misdescribe general cluster-write serialization.

## Decision

- Admit only `POST`. Reject query parameters and nonempty bodies before durability admission.
  Bound body extraction at 64 KiB with a 250 ms read deadline, retain structured 413 handling, and
  return a structured 405 with `Allow: POST` for every other method.
- Keep the operation synchronous. Reverse Rusty has no persistent task/status resource through which
  an asynchronous checkpoint result could be recovered, so it does not accept
  `wait_for_completion=false`. Once admitted, disconnecting the caller does not cancel the commit.
- Share the cluster's one owned durability permit with `POST /_backup`, because backup performs a
  checkpoint under the same writer boundary. Acquire the permit asynchronously; then move the
  permit, writer-lock wait, cluster read guard, and complete checkpoint onto a blocking worker.
  This prevents either endpoint from accumulating detached blocking workers behind the other.
- Supervise the blocking worker from an independently spawned completion task. Successful or failed
  work is logged and counted even when the request future has been dropped.
- Preserve `acknowledged` and `epoch`, add integer `took`, precise `took_ms`, `durable`, and
  `shards_checkpointed`. `durable: true` means this request committed an in-process coordinator
  manifest; only then does `shards_checkpointed` report the number of sealed logical positions.
  A no-`data_dir` success explicitly returns `durable: false`, `shards_checkpointed: 0`, epoch 0,
  and an explanatory message.
- Mark every route-reached response `Cache-Control: no-store`. Count and time all outcomes under the
  fixed `checkpoint` endpoint label.
- Preserve the storage layer's fail-loud behavior. Any shard-seal or coordinator-manifest failure
  returns the typed error status (a persistence failure is 503 `durability_unavailable`) and never
  advances or acknowledges a new checkpoint epoch.

## Consequences

Durable in-process clients can distinguish a committed recovery point from an acknowledged
nondurable maintenance no-op without inspecting startup flags. Remote operators still must quiesce
writes and snapshot all shard/control volumes as one set; the endpoint deliberately does not claim
to coordinate that procedure.

The endpoint can wait behind an already admitted checkpoint, backup, or serialized cluster
mutation. There is no operation timeout: abandoning the HTTP connection is not a safe cancellation
primitive for an atomic durability commit, and no truthful task-recovery API exists yet.

## Safety and proof

The change affects only transport, scheduling, and response projection around the existing
`ClusterEngine::checkpoint` commit order. It does not alter query compilation, matching, placement,
manifest contents, or log truncation.

Handler tests cover durable and nondurable results, strict method/query/body handling, body size and
read deadlines, no-store telemetry, closed admission, off-runtime writer-lock waiting, shared
checkpoint/backup admission, and independently reported completion after request cancellation. A
read-only shard-segment failure proves the REST response remains a fail-loud 503 and the epoch does
not advance. The cluster durability oracles continue to prove checkpoint/reopen equivalence,
tombstone non-resurrection, tags/ranking/vocabulary preservation, replication, resize, and
backup/restore.
