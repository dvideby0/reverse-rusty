# ADR-164: Node-registration REST API contract

> [Clustering — replication & control plane decisions](areas/clustering-replication-and-control-plane.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

`POST /_cluster/nodes` used Axum's generic JSON extractor and synchronously called
`ClusterEngine::register_node` on a Tokio request worker. It accepted the server-wide 100 MiB body
ceiling, unknown JSON and query fields, absent or arbitrary addresses, and node id zero. It had no
body or control-write deadline, explicit method/cache/telemetry contract, admission bound, or
committed version in the response. Backend errors—including manager endpoint and transport
details—were returned to callers.

The mutation is load-bearing. `AddNode` upserts a descriptor by logical id in consensus. Later
assignment-authoritative routing resolves assigned ids through those addresses, so accepting an
invalid endpoint or accidentally replacing bootstrap id zero can make a topology unroutable.
Registration is also easy to overread: manager role and Raft voter membership are separate, and
membership registration does not assign or move shard data.

## Compatibility boundary

Elasticsearch and OpenSearch expose node information, statistics, usage, hot-thread, and related
observational APIs, but no REST operation that inserts a node descriptor into cluster state
([Elasticsearch nodes info](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-nodes-info),
[OpenSearch Nodes APIs](https://docs.opensearch.org/latest/api-reference/nodes-apis/index/)).
Their nodes join through product lifecycle and transport discovery. Reusing `/_nodes`, returning a
fabricated ES/OpenSearch node document, or implying transport discovery would be false
compatibility.

Keep the native path and body. Adopt only `cluster_manager_timeout` and its
`master_timeout` alias: this operation really is one authoritative manager write, so their waiting
semantics map without invention.

## Decision

- Admit only `POST` with a structured 405 and `Allow: POST`. Require JSON or `+json`, reject unknown
  and duplicate query/body fields, cap the body at 64 KiB, and bound delivery at 250 ms.
- Require a positive `u64` id; reserve zero for the bootstrap in-process manager. Require `addr` as
  an HTTP(S) endpoint origin, bounded to 2 KiB, with a host, an optional valid nonzero port, and no
  credentials, fragment, path, or query. Do not perform a reachability probe: registration is
  declarative, while serving checks belong to health and data-moving operations. Keep `role` strict
  to lowercase `data` (default) or `manager`.
- Preserve the existing upsert-by-id state-machine behavior. A new address or role replaces the
  descriptor; an identical retry remains state-idempotent but commits another transition. Document
  that an assigned id must only be redirected to the same recovered logical node, or moved through
  the data-moving topology flow first.
- Return `{acknowledged, version, node}`. Change `ClusterEngine::register_node` to return the exact
  `StateVersion` produced by `ControlPlane::propose`, rather than discarding it. Serialize the
  response role in the lowercase REST dialect.
- State explicitly that registration changes only `nodes`: it neither changes `voters` nor
  `assignments`, performs no rebalance/recovery/data movement, and does not attest endpoint
  reachability. `manager` remains eligibility metadata; Raft voter changes stay on the separate
  joint-consensus operation.
- Accept exactly one manager-timeout spelling, with a 30-second default and maximum. Zero performs
  non-queuing admission and, once admitted, runs one write to completion. A positive timeout covers
  shared administrative admission and proposal execution.
- Move the administrative permit, cluster-lock wait, and synchronous consensus proposal to a
  blocking worker. An atomic queued/start gate prevents blocking-pool delay from beginning the
  proposal after its deadline. A timeout that cancels queued work says no proposal began; a timeout
  after start reports an unknown outcome and directs callers to inspect cluster state before
  retrying. The non-cancellable detached proposal retains admission until completion.
- Return no-store structured responses and fixed-label request telemetry on every route-reached
  outcome. Log detailed control failures server-side, but return only a sanitized fail-loud
  `control_plane_error`.

## Consequences

Automation can distinguish an acknowledged commit from a request rejected before proposal start,
and can correlate the response with the authoritative cluster-state version. It cannot mistake node
registration for physical joining, voter membership, placement, data recovery, or reachability.

Endpoint replacement remains available for recovered-node workflows, but its operational
precondition is explicit. The server cannot cancel a synchronous control write without risking an
ambiguous partial protocol interaction, so a deadline after proposal start is necessarily
outcome-unknown. Because `AddNode` is an upsert, retrying the same descriptor is state-safe after
the caller checks cluster state, though it advances the version again.

The shared administrative slot means a slow manager write can temporarily delay state/stats
introspection. It cannot create an unbounded blocking-worker queue, and no blocking consensus or
cluster-lock wait occupies a Tokio request worker.

## Safety and proof

The consensus transition remains the established deterministic `AddNode`; this change does not
alter assignment, routing, matching, query data, or Raft voter membership. Reserving id zero and
validating a dialable URI shape remove two ways the REST boundary could corrupt topology metadata.
Returning the proposal's own version avoids a racy follow-up state read.

Focused handler tests prove exact version/descriptor responses, data and manager roles, unchanged
voters and assignments, replace-by-id semantics, strict query/media/JSON/id/address/method/body
handling, size and body deadlines, no-store telemetry, zero/positive/closed admission behavior,
off-runtime proposal execution, deadline-detached completion with retained admission and unknown
outcome, and sanitized control failure. Existing control-plane differential, durable Raft restart,
topology-resolution, data-moving reassignment, health, and allocator suites continue to prove the
underlying state and serving semantics.
