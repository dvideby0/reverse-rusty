# ADR-170: Cluster-handoff REST API contract

> [Clustering — elasticity & repair decisions](areas/clustering-elasticity-and-repair.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

ADR-044/048 established the zero-false-negative live-handoff mechanism: copy under a retention
lease, fence the source, drain to convergence, atomically flip live routing, and auto-unfence on an
aborted move. ADR-072 exposed it as `POST /_cluster/handoff` for the Compose lifecycle harness. The
HTTP wrapper remained a thin JSON extractor over the server-wide 100 MiB body limit. It accepted
unknown fields and arbitrary query controls, trusted the caller's claimed source endpoint, ran on
Tokio's shared blocking pool, had no admission/start deadline, could outlive a disconnected request
without independent completion observation, and was not joined before shutdown durability cleanup.

The source trust was a correctness issue, not transport polish. Recovery addressed the requested
position on whatever same-dictionary endpoint the caller supplied. An old/orphan slot could
therefore seed a fresh target even though it was not the position's current live primary. The
handoff would then fence the wrong process and could flip routing onto stale data.

The route also changed only live routing. It never committed the membership assignment, so a
resolve-by-assignments restart could route back to the old source. The old documentation recommended
`/_cluster/reassign` but did not require clients to acknowledge or machine-read that boundary.

## Compatibility boundary

Elasticsearch and OpenSearch `POST /_cluster/reroute` accepts logical allocation commands over an
index, shard, source node, and target node. Allocation deciders apply the command to durable cluster
state; the API also supports dry runs, explanations, allocation retry, response metric selection,
and an overall timeout
([Elasticsearch reroute](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-reroute),
[OpenSearch reroute](https://docs.opensearch.org/latest/api-reference/cluster-api/cluster-reroute/)).

Reverse Rusty raw handoff names physical mesh endpoints and intentionally skips the durable
assignment commit. An alias would tell existing allocation automation that a restart-stable move
occurred when only one coordinator's live backing changed. Keep the native path. Adopt `shard` as
an alias for the global position and the manager-timeout spellings, because those concepts map
exactly. Leave logical-node movement to `/_cluster/reassign` and whole-cluster planning to
`/_cluster/rebalance`.

## Decision

- Accept only `POST` with a structured `Allow: POST` response. Require strict JSON or `+json`, cap
  the body at 64 KiB and 250 ms, reject unknown/duplicate/missing/null fields, and validate both
  endpoints as bounded absolute HTTP(S) authorities without paths, queries, or credentials.
- Require `allow_uncommitted:true`. A successful response always includes `committed:false` and an
  actionable warning naming `/_cluster/reassign`. This preserves the harness primitive without
  allowing a client to mistake it for durable placement.
- Under the endpoint move-ledger reservation, attest that the requested source equals the current
  live primary and that the target is not already in the live replica set. Formatting-only
  scheme/host case and trailing-slash differences normalize for identity. A target that already is
  the live primary is an idempotent exact retry and returns `moved:false` at the current generation.
- Add a deadline-aware all-or-nothing ledger reservation. `cluster_manager_timeout` and its
  `master_timeout` alias default to and are capped at 30 seconds; zero is one immediate attempt.
  The deadline bounds the single REST admission slot, topology/cluster access, and conflicting
  endpoint reservation until an atomic start gate opens. Timeout or disconnect before that gate
  wins guarantees recovery will not begin later.
- Once the start gate opens, complete recovery/fencing/flip regardless of the manager deadline or
  HTTP disconnect. Run it on an independently supervised OS thread, retain admission through the
  terminal result, and make coordinator shutdown acquire that permit before final flush/checkpoint.
  Reject overall `timeout`, because cancellation after fencing cannot promise unchanged state.
- Return terminal timing, `acknowledged`, `moved`, `committed:false`, position, and live generation.
  Count every route-reached outcome under fixed `cluster_handoff` telemetry and attach
  `Cache-Control: no-store`.

## Consequences

The raw operation can no longer recover from an arbitrary endpoint, target an existing replica,
start after reporting a pre-start timeout, accumulate duplicate detached workers, block a Tokio
runtime worker, or be abandoned silently during shutdown. An exact retry after a lost success is
safe and reports that no second move occurred.

The request is intentionally breaking for the one shipped caller: the deployment harness now sends
`allow_uncommitted:true`. That friction is the safety boundary. Production automation should not
use the raw route; `/_cluster/reassign` resolves membership, moves data, and commits the new owner.
The native path remains useful for the black-box handoff-under-load proof and controlled topology
transitions where an external operator deliberately owns the later configuration change.

The core source attestation applies to every direct `execute_handoff` caller, including tests and
future low-level integrations. Higher-level reassign/group moves keep their existing plan → reserve
→ revalidate logic and call the already-reserved inner move directly.

## Safety and proof

Source and target identity are checked after acquiring the same move-ledger ticket held through the
flip. A concurrent move touching the source or target cannot change the live backing between
attestation and recovery. A second move of the same position waits on the shared current source;
after the first flip, it either becomes the exact no-change retry or fails the stale-source check.
Target-replica refusal prevents fencing only one member while the previous primary continues to
accept writes.

Focused core tests prove stale-source rejection, live-replica target rejection, normalized
idempotent retry, explicit primary ownership when the deterministic replica keep set sorts a replica
first, and deadline reservation without partial endpoint ownership. Handler tests prove strict
method/query/media/body/endpoint controls, body size and absolute delivery deadlines,
zero/positive/closed admission, topology-lock deadline cancellation, `shard` aliasing, and
non-distributed refusal. The supervisor test proves completion after receiver disconnect. The
existing gRPC handoff oracle and the secured Compose handoff-under-load leg continue to prove
convergence, auto-unfence, accepted-write recall, and zero false negatives across a real routing
flip.
