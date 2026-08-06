# ADR-172: Cluster-reconcile REST API contract and supervised convergence

> [Clustering — elasticity & repair decisions](areas/clustering-elasticity-and-repair.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

ADR-092 shipped an idempotent whole-cluster convergence primitive, an opt-in server loop, and a
thin manual HTTP trigger. The trigger buffered against the server-wide 100 MiB limit, accepted
unknown/null/zero controls, exposed internal mesh errors, used Tokio's shared blocking pool, had no
admission/start deadline, and detached after client cancellation without independent completion or
shutdown joining. Repeated requests and the background loop could therefore accumulate overlapping
`O(corpus)` passes.

The route also ran on static and CLI-seeded remote topologies. Static backings do not follow the
committed map, so that map cannot safely select movement sources. CLI-seeded assignment routing is
live-authoritative, but changing its map makes the next position-preserving endpoint-list guard
reject restart. The background loop required only `--route-by-assignments` and inherited the same
restart trap.

Finally, the report described every `reconciled` position as physically moved and claimed an
uncommitted result left the old source serving reads. ADR-171 changed RF=1 recovery: a prior failed
commit leaves the new target as live authority, and retry commits it without stale recopy. The
running coordinator is exact, but restart from the old durable assignment is unsafe after newer
writes.

## Compatibility boundary

Elasticsearch and OpenSearch `POST /_cluster/reroute` operate on named-index allocation commands
and allocation deciders. `retry_failed=true` runs one extra allocation retry for shards blocked by
prior failures. They also expose commands, simulation/explanation, metric filtering, a manager
timeout, and an overall timeout
([Elasticsearch reroute](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-reroute),
[OpenSearch reroute](https://docs.opensearch.org/latest/api-reference/cluster-api/cluster-reroute/)).

Reverse Rusty has one global matcher and this operation computes every difference between a durable
numeric-position map and deterministic HRW placement, physically converging each position while
continuing after failures. Keep the native `/_cluster/reconcile` path. Adopt only
`master_timeout` / `cluster_manager_timeout`, whose admission/start semantics map exactly. Reject
reroute commands, `retry_failed`, simulation, metric filtering, and overall cancellation timeout.

## Decision

- Accept only `POST`, with `Allow: POST`. Accept an empty body or a strict JSON object containing
  only positive-integer `max_parallel`; cap it at 64 KiB and 250 ms. Reject unknown, duplicate,
  null, zero, non-object, wrong-media-type, and unsupported query input before topology access.
- Require a distributed resolve-only assignment-routed remote coordinator. Reject static,
  CLI-seeded, and in-process topologies before admission. Require the unattended loop to use the
  same resolve-only assembly at startup.
- Share one reconcile admission slot between manual and unattended passes. Conflict-free wave
  width remains operator-sized inside one pass; the endpoint introduces no hidden parallelism cap.
- Accept one manager-timeout alias, default/max 30 seconds. Zero is a non-waiting admission and lock
  probe. The deadline covers shared admission, dedicated-worker dispatch, topology/cluster access,
  and an atomic start gate. A pre-start timeout/disconnect guarantees no later mutation. Once
  started, wait for the exact terminal report and reject overall `timeout`.
- Run the synchronous pass on an independently supervised OS thread. The worker retains admission
  through terminal completion after HTTP disconnect. The loop's blocking reconcile and optional GC
  epilogue retain the same permit after loop-task abort. Graceful shutdown acquires the permit before
  durability cleanup.
- Preserve continue-past-position-failure semantics and HTTP 200 for an explicit partial report.
  Add final control `version` and `took`/`took_ms`; set `acknowledged == converged`; attach
  `Cache-Control: no-store` and fixed `cluster_reconcile` request/duration telemetry.
- Define `reconciled` as desired positions committed during the pass, including RF=1 commit-only
  recovery. Describe `uncommitted` truthfully: live routing reaches the target while the durable map
  remains stale, so prompt retry before restart is required. Keep detailed failures in server logs
  and return stable, sanitized per-position reasons to clients.

## Consequences

One coordinator no longer fans duplicate corpus copies out through repeated HTTP calls or a racing
background pass. A response timeout guarantees the pass never started; a started pass cannot be
mistaken for cancellation and remains joined by shutdown. Operators receive a final durable-state
version and a complete, deterministic resume set without internal endpoint leakage.

The endpoint remains intentionally native. Clients that require ES/OpenSearch allocation commands,
allocation-decider explanations, or `retry_failed` semantics cannot treat it as `/_cluster/reroute`.
The durable move-then-commit restart window and cross-coordinator conditional-transition gap remain
tracked in the roadmap; this API reports that limitation instead of masking it.

## Safety and proof

Core reconcile oracles continue to prove idempotence, continued safe progress, replicated group
movement, packed multi-position placement, concurrent writes, epoch invariance after convergence,
and zero false negatives across resolve-only restart. HTTP tests cover strict method/query/media/body
handling, 64 KiB and 250 ms transport bounds, feature gating, topology rejection before admission,
zero-time admission, terminal version/timing fields, no-store telemetry, and sanitized response
shape. Worker permit ownership and shared loop admission make disconnect and shutdown completion
structural rather than dependent on the request future.
