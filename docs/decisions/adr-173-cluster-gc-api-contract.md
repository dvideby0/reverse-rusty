# ADR-173: Cluster-GC REST API contract and supervised cleanup

> [Clustering — elasticity & repair decisions](areas/clustering-elasticity-and-repair.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

ADR-096 built a correct guarded orphan-slot collector and exposed a thin one-shot HTTP trigger. The
trigger accepted ignored query and body input under the server-wide 100 MiB limit, ran on Tokio's
shared blocking pool, had no admission/start deadline, and detached after HTTP cancellation without
independent completion or shutdown joining. Repeated requests could accumulate blocking workers
behind the engine move ledger.

The response also used `GcReport::is_clean`, which considered only per-slot failures. It could
therefore say `acknowledged: true` when whole nodes were unreachable or incompatible. It exposed raw
mesh errors, omitted timing and final control-state identity, and treated a rename-to-trash whose
physical deletion failed as fully reclaimed. Static and in-process assembly could acknowledge a
clean no-op even though no authoritative remote-node inventory had been swept.

## Compatibility boundary

Elasticsearch and OpenSearch dangling-index deletion targets one explicit index UUID, requires
`accept_data_loss=true`, and supports manager and overall response timeouts
([Elasticsearch dangling-index delete](https://www.elastic.co/docs/api/doc/elasticsearch/v8/operation/operation-dangling-indices-delete-dangling-index),
[OpenSearch dangling indexes](https://docs.opensearch.org/latest/api-reference/index-apis/dangling-index/)).
Reverse Rusty's operation instead enumerates registered data nodes and automatically drops only
physical slots proven outside both durable placement and live routing. Keep the native
`POST /_cluster/gc` path. Adopt only `master_timeout` / `cluster_manager_timeout`, whose
admission/start meaning maps exactly. Do not add a `/_dangling` alias or accept
`accept_data_loss`, named UUIDs, dry-run, or overall cancellation timeout.

## Decision

- Accept only bodyless `POST`, with `Allow: POST`. Cap transport at 64 KiB and 250 ms so a bodyless
  route cannot be used to buffer or slow-deliver the server-wide allowance. Reject every other
  query control and duplicate timeout aliases before topology access.
- Require a distributed assignment-routed remote coordinator. Accept CLI-seeded and resolve-only
  assembly because GC never changes assignments and its live keep set protects current backings.
  Reject static assembly because its committed node directory may be incomplete; reject in-process
  assembly because it has no remote slots.
- Share the single reconcile/GC maintenance slot with the unattended reconcile+GC loop. Hold the
  topology and cluster read guards for the full sweep; the engine's all-node move-ledger reservation
  remains the underlying interlock with handoff and reassignment.
- Default/max manager wait to 30 seconds; exact zero is a non-waiting admission and lock probe. The
  deadline covers shared admission, dedicated-worker dispatch, topology/cluster access, and an
  atomic start gate. A pre-start timeout/disconnect guarantees no later deletion. Once started, the
  sweep runs to its exact terminal report.
- Run the synchronous RPC sweep on an independently supervised OS thread. The worker retains
  admission after HTTP disconnect, and graceful shutdown joins the shared slot before durability
  cleanup. Keep detailed slot/node failures in server logs.
- Add `GcReport::pending_disk_cleanup`. A drop that left the serving namespace but did not finish
  trash deletion remains visible and incomplete until node boot cleanup. Define completion as no
  pending disk cleanup, unassigned skip, per-slot failure, or skipped node; a live-routed keep is an
  intentional terminal outcome. Keep `is_clean` as a compatibility spelling of this stronger rule.
- Return HTTP 200 for a terminal partial report, with `acknowledged == completed`, final control
  `version`, `took`/`took_ms`, stable sanitized partial reasons, `Cache-Control: no-store`, and fixed
  `cluster_gc` request/duration telemetry.

## Consequences

The route no longer claims whole-cluster success when any node or slot was unclassified or when
disk reclaim remains pending. Operators can retry safe partial work without receiving internal
mesh details, and an HTTP disconnect cannot make deletion look cancelled while it continues. Manual
GC cannot overlap or queue behind the loop's reconcile/GC maintenance pass.

The endpoint remains intentionally native. Clients that require ES/OpenSearch dangling-index UUID
selection or explicit data-loss deletion cannot treat it as that API. Cross-coordinator destructive
serialization remains bounded by the v1 single-active-coordinator posture recorded in ADR-096.

## Safety and proof

The ADR-096 gRPC oracles continue to prove relocation cleanup, co-located sibling preservation,
flip-without-commit keeps, restarted-unfenced orphan removal, idempotence, durable restart behavior,
and zero false negatives. Core tests cover all classification classes and the stronger completion
predicate. HTTP tests cover strict method/query/body handling, 64 KiB and 250 ms transport bounds,
feature/topology gating, zero/positive/closed admission, terminal timing/version, no-store metrics,
and sanitized partial reports. The supervisor regression proves receiver disconnect cannot cancel a
started worker.
