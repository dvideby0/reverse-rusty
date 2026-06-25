# ADR-090: Data-moving live reassignment (`reassign_and_move` / `rebalance_and_move`)

**Status:** Accepted (2026-06-25)

**Context.** ADR-086 shipped the *boot-time* half of "route by the committed shard→node map": a
coordinator can resolve its topology from the durable quorum (`--route-by-assignments`,
position-preserving, guarded) and fail over across control endpoints. It deliberately deferred the
*runtime* half — a committed assignment change that actually **moves the data** and re-points routing
live — and recorded the contract: *do not `rebalance` a populated remote cluster expecting routing to
follow*, because the two relevant operations were disjoint:

- [`execute_handoff`](../../engine/src/cluster/coordinator/distributed.rs) (ADR-044/043/048) MOVES a
  shard's data and atomically flips live routing (peer-recover target → fence source → drain to
  convergence → `HandoffShard::swap_backing`), fail-closed (auto-unfence on abort). It never touches
  the committed map.
- [`reassign_shard`](../../engine/src/cluster/coordinator/control_plane.rs) / `rebalance` COMMIT a new
  map but move **no data** (HRW `rebalance` permutes the map without moving segments).

So on a populated remote cluster routing could not follow a reassignment: the
[`route_topology`](../../engine/src/cluster/coordinator/topology.rs) boot guard refuses any committed
map that isn't position-preserving (it would route a position to a node holding different data — a
shard-sized false negative). This is the ADR-086 "data-moving reassignment" deferral (roadmap Tier 3).

**Decision.** Compose the two existing primitives into ONE operation that keeps **committed-map ⟺
live-routing ⟺ physical-data-location** consistent, with a zero-false-negative proof under concurrent
writes and across a coordinator restart. All `distributed`-gated; the in-process / RF=1 default path
never compiles it and is byte-identical. New `distributed`-gated submodule
`engine/src/cluster/coordinator/reassign.rs`.

- **`reassign_and_move(position, to: NodeId, handle)`** — the per-position primitive. Resolve `from`
  (the current committed primary) and `to` to endpoints from membership (fail-closed — never silently
  skip an unroutable node), short-circuit a no-op when `from == to` or both resolve to one endpoint,
  then **move-then-commit**: run `execute_handoff` FIRST, and only on success commit
  `AssignShard{position, primary: to}` — **preserving the position's existing `replicas`** (an
  `AssignShard` replaces the whole entry, so a primary-only assignment would silently drop the
  committed replica set). Returns a typed `ReassignOutcome`: `NoChange`, `Moved`, or
  `MovedButNotCommitted`.

- **`rebalance_and_move(rf, handle)`** — the data-moving analogue of `rebalance`: recompute the HRW
  desired map and `reassign_and_move` each position whose **primary** changes, **sequentially**, in
  position order. Returns a `RebalanceMoveReport { moved, failed, not_attempted }`.

**Why move-then-commit (the zero-FN ordering).** Two routing oracles exist: *live routing*
(`shards[position]`'s `HandoffShard` backing, read on the hot path) and the *committed map* (consulted
only on a coordinator restart via `resolve_topology`). The invariant: every oracle a future reader
could consult must point at a node that holds the data **and serves reads**. The linchpin is that the
source fence is **write-only** — the recovery/read RPCs deliberately do not check it
(`server.rs`). So in the window after the flip but before the commit, the committed map still names
`from`, which still holds the data and still serves reads → a crash + restart resolving the committed
map lands on a reads-serving, data-holding node. The opposite order (commit-then-move) is unsafe: a
crash after the commit but before the move points routing at an empty `to` — a silent false negative
(the exact ADR-086 trap). The crash-window table (every window read-safe):

| Window | Live routing | Committed map (a restart resolves here) | FN? |
|---|---|---|---|
| pre-move / mid-move / post-fence pre-flip | `from` (holds data, reads served) | `from` | No |
| **post-flip, pre-commit** | `to` (converged copy) | `from` (fenced → reads still served, holds data) | **No** |
| commit fails | `to` | `from` (as above) | No |
| post-commit | `to` | `to` | No |

**Fail-closed at every step.** A failed move propagates `Err` and commits nothing (the source
auto-unfenced, routing + the committed map untouched — a consistent rollback). A failed commit AFTER a
successful flip returns `MovedButNotCommitted` (NOT a bare `Err` — the data did move) and emits a
`DurabilityFailure` event; the committed map still names the data-holding, reads-serving source, so a
restart is still correct, and re-running `reassign_and_move` is idempotent (a fenced source still
serves the read-only recovery RPCs, so the retry re-converges the already-populated target and
re-commits). A multi-position `rebalance_and_move` stops on the first failure and reports
`{moved, failed, not_attempted}` — each already-moved position is individually consistent, so a partial
rebalance is a valid, resumable state (fail-forward, no auto-rollback).

**Serialization (the concurrency guard).** `reassign_and_move`, `rebalance_and_move`, AND the
autoscaler-driven handoff all hold a new engine-level `reassign_serial: Mutex<()>` (gated) for the
whole move-then-commit, so two concurrent moves of one position cannot interleave their flip + commit
and invert the map vs routing — the autoscaler's `tick` drives handoffs outside the REST `write_serial`,
so a REST-only lock would not have covered it. A compare-and-set on the committed primary just before
the commit is defense-in-depth for a future multi-coordinator shared control plane. The guard never
touches the hot path (percolate/ingest), so a long segment copy here never stalls reads or writes.
**Sequential** multi-position moves are required (not just an optimization): an HRW reshuffle can chain
(position `p`: F→T while position `q`: T→U), and running them concurrently would have T serve as a
handoff target and source at once — the drain-to-convergence proof assumes a quiescent fenced source.

**Autoscaler unification.** `drive_autoscaled_handoff` now routes through `reassign_and_move` instead
of a bare `execute_handoff`, so an autoscaler-driven move ALSO commits the new owner (closing its prior
latent divergence — it moved data without committing the map) and rides the same guard. Existing
autoscaler tests are unaffected (the in-process ones compile the driver out; the distributed one uses
endpoint-less nodes, so the move is skipped and nothing commits either way).

**Operator surface.** `POST /_cluster/reassign {position, node}` — the map-aware, higher-level
companion to the raw-endpoint `/_cluster/handoff`: resolves the target endpoint from membership, then
move-then-commit; reports `committed:false` (zero-FN safe, retry to reconcile) on a `MovedButNotCommitted`.
`POST /_cluster/rebalance` gains an optional `{"move": true}` (`#[serde(default)]` ⇒ an empty body is
`false` = today's map-only HRW rebalance, backward compatible) that drives `rebalance_and_move`. Both
distributed-gated with a non-distributed 501-with-reason; neither holds `write_serial` (a move runs
concurrently with ingestion by design). After a data-moving reassign the committed map diverges from
the original `--shard-endpoint` order, so a restarting coordinator must boot **resolve-only**
(`--route-by-assignments` + `--control-endpoint`, no `--shard-endpoint`) to trust the quorum — the
ADR-086 guard correctly fails loud on a *stale* CLI.

**Consequences / scope.**

- **Default byte-identical.** Everything is `distributed`-gated (the new submodule, the `reassign_serial`
  field, the autoscaler change). The lean / in-process / RF=1 path has one node owning every position,
  so `from == to` short-circuits to a no-op; the matching hot path is untouched (routing still indexes
  `shards[s]` by ring position; the control plane is read only at admin/restart time).
- **Closes the ADR-086 deferral.** A reassignment now moves data and routing follows — live and across
  a restart. The bare map-only `rebalance` still must not be used alone to re-point a populated remote
  cluster (use `reassign`/`rebalance {move:true}`).
- **Proven.** `cluster_grpc_oracle::reassign` (real gRPC servers): the primary proof — move under a
  concurrent writer, the committed map names the target, and a fresh coordinator resolving from it
  routes to the new owner with zero FN (a simulated restart); the crash-window proof — flip without
  commit, a coordinator resolving the still-old map reads zero-FN from the fenced (reads-serving)
  source; fail-closed — a forced abort moves nothing, commits nothing, auto-unfences. Plus lean unit
  tests for the `rebalance_targets` diff/ordering. The full `distributed` oracle stays green.
- **Explicitly deferred:** **parallel** multi-position moves (today sequential), and an **automated
  assignment-watch → re-point controller** that reconciles the committed map to physical reality
  unattended (this increment is operator/autoscaler-driven and manually triggered).
