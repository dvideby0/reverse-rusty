# ADR-171: Cluster-reassign REST API contract and live-authority reconciliation

> [Clustering — elasticity & repair decisions](areas/clustering-elasticity-and-repair.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

ADR-090 composed live handoff and assignment commit into `reassign_and_move`. Its HTTP wrapper was
still a thin serde extractor over the server-wide body limit. It accepted unknown fields and query
controls, ran synchronous move work on Tokio's shared blocking pool, had no admission or start
deadline, could finish after disconnect without independent terminal observation, and was not
joined by graceful shutdown. It also admitted static and CLI-seeded remote topologies whose next
restart could not safely follow a changed committed map.

Two result/correctness gaps were more serious than transport ergonomics:

- An idempotent retry returned `committed:false` even when the live and durable authorities already
  named the requested node.
- `reassign_and_move` planned recovery from the committed owner without attesting the current live
  primary. A preceding raw handoff or a prior move whose commit failed could leave live routing on
  the target while the durable map still named the old source. New writes then made that source
  stale. Retrying by copying from it could overwrite the live target from stale authority.

The core also narrowed `usize` position to `u32` with `as`, so a library caller on a 64-bit host
could wrap an out-of-range position onto a different logical shard.

## Compatibility boundary

Elasticsearch and OpenSearch `POST /_cluster/reroute` is a multi-command named-index allocation API.
It supports move, cancel, replica and primary allocation commands plus allocation deciders,
dry-run/explain, failed-allocation retry, metric selection, manager timeout, and overall timeout
([Elasticsearch reroute](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-reroute),
[OpenSearch reroute](https://docs.opensearch.org/latest/api-reference/cluster-api/cluster-reroute/)).

Reverse Rusty has one global matcher and this operation moves one global position to a numeric
membership node while synchronously waiting for physical recovery and the durable commit. Keep the
native `/_cluster/reassign` path. Adopt `shard` for `position`, `to_node` for `node`, and
`master_timeout` / `cluster_manager_timeout` because those concepts map exactly. Reject reroute's
command envelope, index identity, allocation controls, simulation, and overall `timeout` rather than
fabricating support.

## Decision

- Accept only `POST` with a structured `Allow: POST` response. Require strict JSON or `+json`, cap
  the body at 64 KiB and 250 ms, reject unknown/duplicate/missing/null fields, accept only a `u32`
  `position` (`shard` alias), and accept a `u64` numeric membership `node` (`to_node` alias) as a JSON
  integer or decimal string. Reject out-of-range library positions before narrowing.
- Require an authoritative resolve-only remote topology. Reject static endpoint-order routing,
  CLI-seeded assignment routing, and in-process placement before admission; the in-process safe
  alternative is whole-cluster `/_cluster/rebalance`.
- Plan the complete committed-owner, current-live-primary, and target endpoint footprint; reserve
  it in the move ledger; then re-read and attest the committed entry, membership resolutions, and
  live primary under that ticket. Seed physical recovery from the attested live primary, never
  blindly from the durable map.
- Treat only agreement between live routing and the committed assignment on the target as an exact
  no-op. Report it as `acknowledged:true`, `moved:false`, `committed:true`. If live routing already
  names the target while the committed map does not, skip data copy and commit the attested live
  owner, returning `reconciled:true`. A fully converged retry performs neither operation.
- Preserve move-then-commit. A pre-flip failure auto-unfences and commits nothing. A post-flip
  commit failure keeps exact live routing on the target and returns a typed uncommitted outcome, but
  no longer describes the stale durable map as restart-safe. After newer writes, restarting before
  reconciliation can route to stale data. The response sets `acknowledged:false`,
  `committed:false`, preserves whether this invocation physically moved, and directs a prompt retry
  before restart.
- Accept exactly one manager-timeout spelling, default/max 30 seconds. Zero is one immediate
  attempt. The deadline bounds the single REST admission slot, topology/cluster access, and atomic
  move-ledger start gate. A pre-start timeout guarantees no later move or commit. Once started, wait
  for the exact terminal result; reject overall `timeout` because cancellation after fencing cannot
  promise unchanged state.
- Run the complete synchronous workflow on an independently supervised OS thread. Retain the
  one-operation admission permit through terminal completion after HTTP disconnect, and have
  graceful coordinator shutdown acquire that permit before durability cleanup.
- Return terminal `took`/`took_ms`, `acknowledged`, `moved`, `committed`, `reconciled`, position,
  node, and handoff generation. Attach `Cache-Control: no-store` and fixed `cluster_reassign`
  request/duration telemetry to every route-reached outcome. Preserve typed failure classes for
  clients while keeping detailed mesh endpoints and transport errors in server logs.

## Consequences

An exact retry can no longer claim that an already-committed assignment is uncommitted. More
importantly, recovery cannot be seeded from a stale durable owner after live routing moved
elsewhere. The retry for an uncommitted move is now cheaper and safer: it attests the existing live
target and commits that authority without a second corpus copy.

Move-then-commit still prevents the control plane from ever naming an empty target. It cannot make
the gap after a successful live flip and a failed durable proposal restart-safe forever. The
running coordinator remains exact, but later writes advance only the live target. Operators must
restore control-plane write availability and retry before coordinator restart. A durable move
intent with restart recovery, or an atomic conditional assignment protocol that participates in
the routing transition, is tracked in the roadmap rather than hidden by the current response.

This remains a single-active-coordinator contract. The local ledger and best-effort durable-primary
recheck do not form a cross-coordinator compare-and-set. The existing group-aware path continues to
own replicated positions; this single-target HTTP route refuses them.

## Safety and proof

The core plan → reserve → revalidate sequence holds the committed endpoint, live endpoint, and
target against conflicting moves. When the target is not live, the existing handoff proof recovers
from the attested live primary, fences and drains it, and flips only after convergence. When the
target is already live, committing it requires no recopy and cannot replace newer target writes
with old-source state. A checked conversion closes the position-aliasing bug.

The localhost gRPC regression performs an uncommitted raw flip, writes newer data to the live
target, retries reassignment, observes a commit-only `Reconciled` result, proves the newer data
survives, verifies a fully converged no-op, and reconstructs a fresh assignment-routed coordinator
from the committed target. Core tests prove out-of-range rejection. Lean/distributed HTTP tests
prove strict method/query/media/body/alias handling, body bounds and deadlines, unsafe-topology
rejection before admission, zero/positive/closed admission, topology-lock deadlines, and truthful
terminal flags. The supervisor test proves terminal completion after response receiver disconnect.
