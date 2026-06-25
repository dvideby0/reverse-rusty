# ADR-086: Coordinator routes by committed assignments + multi-control-endpoint failover

**Status:** Accepted (2026-06-24)

**Context.** ADR-083 wired a deployed coordinator to the durable openraft quorum as a thin
`RemoteControlPlane` client, making the cluster-state *document* durable + HA. Two pieces of the
roadmap Tier-3 "control-plane wiring residue" remained (named in ADR-083's scope boundary, ADR-084's
deferrals, and `STATUS.md` "Current limitations"):

1. **Routing was not driven by the committed shard→node assignments.** The coordinator read the
   committed `ClusterState` only to *validate* its ring params, then routed by its static
   `--shard-endpoint` CLI list — the deployment topology lived in every coordinator's flags, not the
   durable quorum.
2. **The control client used only the first `--control-endpoint`** (`control_endpoints.first()`),
   following a follower's `ForwardToLeader` redirect but with no failover if that first endpoint's
   node was down.

**The correctness trap (why "just resolve from assignments" is unsafe today).** `rebalance`
(`control_plane.rs`, HRW `allocator::plan_assignments`) and `reassign_shard` change the committed map
**without moving data** (the docstrings are emphatic: *"the local shards do not move"*; physical
movement is a later increment). HRW does not preserve position→node identity. So a coordinator that
naively resolved `shards[i]` from `assignments[i]` would, after an operator `rebalance` + a restart,
point `shards[0]` at a server holding a *different* position's data — a silent, shard-sized **false
negative manufactured by the feature itself**. Routing-by-assignments is only safe *and* load-bearing
once a reassignment also **moves the data** (live handoff) — a larger follow-on.

**Decision.** Ship the safe half now: make the committed quorum the topology source of truth at
boot/connect time (position-preserving, guarded, opt-in), plus full multi-endpoint failover. Defer
data-moving reassignment / live re-pointing. All `distributed`-gated; the in-process / in-memory and
the non-opt-in remote paths stay byte-identical.

- **Multi-control-endpoint failover (`RemoteControlPlane`, always on).** A new
  `connect_failover(endpoints, …)` tries the whole list in order, keeping the first reachable as the
  eager primary and retaining the full list. `connect(endpoint)` is a one-element delegate (the
  ADR-083 oracle is unchanged). `call` gains a second resilience layer **orthogonal** to the existing
  `ForwardToLeader` follow: on a transport/`Backend` error from the primary it redials the remaining
  endpoints in order (each at most once per call) — failover finds a *reachable* node, ForwardToLeader
  finds the *leader* among reachable nodes. Control ops are off the matching hot path, so a per-call
  redial on failure is cheap; methods stay `&self` (no sticky-endpoint interior mutability). All
  endpoints down ⇒ fail loud, never a swallowed stale read. The coordinator's `--control-endpoint`
  (already a `Vec`) now passes its full list instead of `.first()`. **Only idempotent reads fail
  over; writes (`Propose`/`ChangeMembership`) never do** — a write that reached the leader may have
  committed before a lost response, and resubmitting it to a fallback endpoint could double-apply a
  non-idempotent op (e.g. `BumpModelVersion` increments the model version per commit), so a failed
  write surfaces loud and converges via an operator/restart retry (the same "writes never retry"
  stance as ADR-085's shard transport). The single `ForwardToLeader` follow stays safe (a follower
  redirects without applying). Consequence: while a coordinator's *primary* control node is down,
  routing-relevant reads fail over but admin *writes* fail loud until the coordinator restarts onto a
  live endpoint — acceptable since control writes are rare admin ops, not the hot path.

- **Topology resolution + seeding (`coordinator/topology.rs`, lean core).** Two pure `ControlPlane`
  free functions — no `ClusterEngine`, no gRPC, no feature gate, so they unit-test against
  `InMemoryControlPlane`. They speak a `ShardGroup`-free `ShardEndpoints = (primary, replicas)` shape;
  the binary maps it to/from `ShardGroup`.
  - `seed_position_preserving(control, topology)` derives a position-preserving map from a deploy
    topology: one logical `Data` node per distinct endpoint URL, `position i →` the nodes for
    `topology[i]`. **Idempotent** (proposes only the diff — a clean restart is a no-op), overwriting
    the genesis "every position → addr-less `NodeId(0)`" map — closing the bootstrap gap so resolution
    reads the topology back.
  - `resolve_topology(control, num_shards)` maps `position → assignments[position] → NodeId →
    nodes.addr`. **Fail-closed:** an unassigned position or an addr-less node errors rather than
    yielding a silently-unrouted shard.

- **Coordinator boot reorder + `--route-by-assignments` (opt-in).** When the flag is set the
  coordinator attaches the control plane (with failover) **before** building shards, then decides the
  shard groups by **reading the committed map first** (load-bearing — seeding first would overwrite a
  rebalanced map and silently defeat the guard): a *genesis* (unseeded) quorum is seeded
  position-preservingly from `--shard-endpoint`; a *populated* quorum is authoritative. It then
  resolves the topology and **guards** it — if `--shard-endpoint` was also given and the resolved
  topology differs, it **fails loud** ("the committed map is not position-preserving — a
  non-data-moving rebalance?"). This defuses the HRW trap for the both-flags case. The resolve-only
  case (flag set, no `--shard-endpoint`) trusts the committed map, so a coordinator can boot without
  `--shard-endpoint` (it still sizes the ring from `--shards` and re-mints its dict from `--load-file`,
  both validated against the quorum on attach; an unseeded quorum here fails loud). The unchanged shard
  builders consume the *resolved* groups instead of the CLI groups. The flag requires
  `--control-endpoint`. (The remote-connect path moved to a new `cluster_mode/remote_connect.rs`
  submodule, keeping both files within the size budget.)

**Consequences / scope.**

- **Default byte-identical.** Without `--control-endpoint` (or with it but without
  `--route-by-assignments`), routing is the unchanged positional `--shard-endpoint` path. On first
  boot with both flags, the resolved topology *equals* the CLI list by construction, so matching is
  byte-identical until an assignment actually moves — and the guard refuses any map that isn't
  position-preserving, so it can never silently route a position to the wrong server.
- **Zero-FN.** The matching hot path is untouched: routing indexes `self.shards[s]` by ring position
  exactly as before; only the *source* of the shards' connect endpoints changed (CLI vs the committed
  document). The control plane is still read only at boot/admin time, never per-title.
- **Explicitly deferred:** a committed assignment change *driving* `execute_handoff` (peer-recovery +
  `HandoffShard` swap) so a reassignment moves data and routing follows **live** while the coordinator
  runs. The scaffolding exists (`autoscale.rs::drive_autoscaled_handoff`, `execute_handoff`); wiring an
  assignment-watch → re-point loop with a zero-FN proof under concurrent writes is the next increment.
  Until then the documented contract is: do not `rebalance` a populated remote cluster expecting
  routing to follow.
- **Proven:** lean unit tests (`cluster_control_plane_oracle`) — seed→resolve round-trips
  position-preserving, re-seed is a no-op (idempotent), a non-position-preserving map resolves
  differently (the guard's trigger); a distributed redirect oracle (`cluster_grpc_oracle::routing`) —
  two real `ShardServer`s, a coordinator resolves to A then a reassigned-to-B coordinator routes to B
  (a sentinel query added only to A is seen by the first coordinator and not the second, proving the
  committed map drives routing) while both match the brute oracle; and a distributed failover oracle
  (`cluster_control_wiring_oracle`) — connect-time skip of a dead leading endpoint, per-call failover
  to a surviving leader when the primary's node is aborted, and all-down ⇒ fail loud. The full
  `distributed` oracle stays green.
- **Deploy:** `compose.cluster.yml` wires all three `--control-endpoint`s + `--route-by-assignments`
  on the coordinator (the quorum is now load-bearing, not "durable but idle"); the Helm coordinator
  emits one `--control-endpoint` per control ordinal + an opt-in `routeByAssignments` value; the
  runbook §11 documents the new behavior and the deferred-handoff caveat.
