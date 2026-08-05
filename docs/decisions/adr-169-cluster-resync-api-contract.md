# ADR-169: Cluster-resync REST API contract

> [Clustering — elasticity & repair decisions](areas/clustering-elasticity-and-repair.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

ADR-047 added the correct library mechanism for a remote cross-position mutation that is durably
logged but reaches only some target positions: retain the failed positions in an in-memory queue and
re-drive them through `ClusterEngine::resync`, with log replay as the authoritative recovery
backstop. Its HTTP wrapper remained a thin bodyless Axum handler. It inherited the server-wide
100 MiB body limit, ignored query controls and non-empty bodies, executed synchronous shard RPCs and
parking-lot locks on a Tokio request worker, had no admission/start timeout or disconnect supervisor,
and emitted neither route-specific timing/count telemetry nor cache controls. Its response exposed
only two unexplained counters.

The repair algorithm is idempotent and well tested, but one request can traverse every pending
mutation and wait on unavailable remote peers. The old transport could therefore stall an async
runtime worker, allow redundant passes to accumulate, and leave automation unable to distinguish a
pass that never started from one that continued after its client disappeared.

## Compatibility boundary

Elasticsearch and OpenSearch use `POST /_cluster/reroute?retry_failed=true` to retry failed **shard
allocation**. Their reroute API changes or simulates allocation, accepts allocation commands,
returns cluster-state material, and supports `dry_run`, `explain`, and metric selection
([Elasticsearch reroute](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-reroute),
[OpenSearch reroute](https://docs.opensearch.org/latest/api-reference/cluster-api/cluster-reroute/)).

Reverse Rusty resync changes no topology. It replays already-logged query mutations against their
original failed logical positions. Aliasing it to reroute or accepting `retry_failed` would make
allocation automation appear compatible while performing a different mutation-repair operation.
Keep the native route. Adopt only `cluster_manager_timeout` plus the `master_timeout` alias for the
pre-start coordination wait and an additive `acknowledged` response field.

## Decision

- Admit only bodyless `POST`, return structured `Allow: POST`, reject unknown/duplicate query
  controls, cap delivery at 64 KiB and 250 ms, and return `Cache-Control: no-store` on every
  route-reached response.
- Share the single expensive corpus-administration admission slot used by stats, vocabulary,
  membership work, and resize. This prevents redundant repair workers from queuing behind the same
  REST-writer boundary and makes the existing shutdown quiescence cover detached resync work.
- Accept one manager-timeout spelling, default/max 30 seconds. Zero performs non-waiting admission
  and lock probes. A positive value bounds admission plus the `write_serial` and cluster-read waits
  until an atomic start gate opens. A deadline or disconnect before start cancels queued work and
  guarantees the pass will not mutate later.
- Run the full synchronous pass on an independently supervised OS thread. Once started, ignore the
  manager deadline and complete even after disconnect: a pass may already have repaired some
  positions, so cancellation could not provide an unchanged-state guarantee. Reject the overall
  `timeout` rather than advertise cancellation the mechanism cannot provide.
- Preserve the existing `repaired` and `still_pending` counters, add `took`, `took_ms`, and
  `acknowledged:true`, and define acknowledgement narrowly: the requested pass completed and the
  returned counters are terminal for that pass. It does not mean all targets recovered;
  `still_pending` remains the explicit convergence signal.
- Record every transport and terminal response under the fixed `cluster_resync` metric label. Keep
  individual target failures in the existing typed durability events/logs; they are expected pass
  outcomes represented by `still_pending`, not transport failures.

## Consequences

The route is bounded before work, cannot block Tokio on synchronous mesh operations, cannot build an
unbounded detached worker queue, and gives automation an exact never-started timeout versus a
terminal pass report. Existing clients that read only `repaired` and `still_pending` remain
compatible; the new timing and acknowledgement fields are additive.

The endpoint remains native and process-scoped. A no-op response proves only that the current
coordinator has no queued repairs. It cannot attest that a previous stateless coordinator did not
lose its in-memory queue, and it does not replace durable log replay or the roadmap's future
cross-write fencing/quorum work.

## Safety and proof

The library repair algorithm is unchanged: it drains the queue, serializes per logical id, skips a
drained entry when a fresher mutation exists, re-drives only failed positions, and re-queues any
remaining failures. The REST worker retains `write_serial` around the entire pass, preserving its
existing same-coordinator ordering against document and bulk writes. The shared admission permit is
retained through worker completion and is already quiesced before shutdown's final checkpoint.

Focused handler tests prove strict method/query/body controls, size and absolute delivery deadlines,
zero/positive/closed admission, exclusive-writer deadline cancellation, fixed no-store telemetry,
and dispatch independent of Tokio's shared blocking pool. The supervisor test proves a disconnected
receiver does not stop an admitted worker. ADR-047's core fault-injection tests continue to prove
successful convergence, retry retention while a shard remains failed, same-id freshness, delete
reservation release, and the durable-log recovery backstop.
