# ADR-162: Cluster-state REST API contract

> [Clustering — replication & control plane decisions](areas/clustering-replication-and-control-plane.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

`GET /_cluster/state` returned the committed `ClusterState` document directly from an Axum `get`
route. The prototype accepted unchecked query parameters and the server-wide 100 MiB body
allowance, had no explicit HEAD/cache/metrics contract, performed the cluster-lock wait and
potentially remote linearizable control-plane RPC on a Tokio request worker, serialized there, and
returned backend/transport details to callers on failure.

The state itself is load-bearing. It contains registered nodes and voters, position-to-node
assignments, ring parameters, feature-model identity, application epoch, and placement generation.
A remote coordinator obtains it from the control-plane leader; returning stale local state or
silently partial state would make the endpoint misleading and could encourage automation to act on
the wrong topology.

## Compatibility boundary

Elasticsearch and OpenSearch use the same path for an internal document that includes index
metadata, mappings, index-shard routing, blocks, cluster identity, and state UUID. Both expose
metric/index path selectors, local-versus-manager reads, metadata-version waiting, settings
projection, and manager timeouts
([Elasticsearch cluster state](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-state-2),
[OpenSearch cluster state](https://docs.opensearch.org/latest/api-reference/cluster-api/cluster-state/)).
Elasticsearch explicitly warns that its response is an unstable internal representation and can be
expensive enough to destabilize a large cluster.

Reverse Rusty has no index namespace, index metadata, state UUID, or local coordinator copy.
Fabricating those sections, accepting an index target, or treating the feature-model counter as an
Elasticsearch metadata version would be false compatibility. Keep the stable native document while
adopting only the exact overlap: `GET`/`HEAD`, `_all`, `version`, authoritative `local=false`,
representation-neutral `flat_settings`, and the `cluster_manager_timeout`/`master_timeout` aliases.

## Decision

- Admit only `GET` and `HEAD`. Bound the body-free route at 64 KiB with a 250 ms read deadline;
  reject nonempty bodies, unknown or duplicate query fields, and other methods with a structured
  405 plus `Allow: GET, HEAD`.
- Return the complete native document from `/_cluster/state` and `/_cluster/state/_all`. Add
  top-level `version` as an exact alias for native `epoch`. `/_cluster/state/version` returns only
  that alias. Reject every other metric or target path with a structured validation error.
- Accept `local=false`; reject `local=true` because every successful read is authoritative and
  linearizable. Accept `flat_settings` as representation-neutral because the native document has no
  settings section. Accept exactly one of `cluster_manager_timeout` and `master_timeout`, including
  unitless zero, with a 30-second default and maximum. A positive timeout covers admission and the
  read; zero performs nonblocking admission and, when admitted, executes one authoritative read
  rather than guaranteeing a timeout. Metadata-version waiting and index controls remain
  unsupported and fail loud as unknown input.
- Share the single stats/introspection permit. Acquire it asynchronously, then move the permit,
  cluster-lock wait, authoritative control read, and JSON serialization to a blocking worker.
  A positive caller deadline covers admission and execution. A timed-out read is idempotent and may
  finish detached; it retains the permit until it does, preventing an unbounded blocking-worker
  queue. A zero-timeout request never queues for that permit; an admitted synchronous read runs to
  completion off the request worker.
- Cap the encoded response at 8 MiB. A control read, serialization, join, admission, timeout, or
  size failure is structured and no-store. Detailed control backend/endpoint errors are logged
  server-side but not returned to clients.
- Mark every route-reached response `Cache-Control: no-store`; count and time all outcomes under the
  fixed `cluster_state` endpoint label. HEAD executes the same availability check but strips the
  response body.

## Consequences

Automation gets a stable, bounded native topology document and one exact familiar version selector
without being invited to depend on nonexistent index state. `version`/`epoch` is the application
control-state version; it remains distinct from the checkpoint epoch, Raft term/log index,
feature-model `model_version`, and logical `placement_generation`.

A slow or unavailable manager cannot occupy a Tokio request worker or keep a positive-timeout HTTP
request open past its deadline. The underlying synchronous read cannot be force-cancelled, so one
detached read may continue holding the shared introspection slot; later requests remain bounded by
their own deadlines instead of spawning more blocked work. Zero is the familiar no-wait admission
form, not a cancellation deadline for a read that was able to start.

## Safety and proof

The change does not modify the consensus document, state-machine transitions, placement, routing,
query data, or matching. It changes only HTTP validation, scheduling, projection, and failure
handling around the existing linearizable `ClusterEngine::control_state` read.

Handler tests cover the full and version-only projections, GET/HEAD behavior, exact familiar
controls including zero-timeout immediate and occupied-admission behavior, unsupported metric/target
and metadata/local shapes, method/query/body strictness, body size and read deadlines, no-store
telemetry, shared admission, off-request-thread execution, deadline-detached completion, closed
admission, sanitized control failure, and the response-size ceiling. Existing in-memory/OpenRaft
differential, durable restart, coordinator wiring/failover, health, CAT shards, and routing oracles
continue to prove the underlying state semantics.
