# ADR-095: Parallel multi-position moves — the busy-endpoint move ledger + conflict-free waves

**Status:** Accepted (2026-07-01)

**Context.** Every data-moving sweep was strictly sequential. ADR-090 introduced
`reassign_serial: Mutex<()>` — a whole-coordinator lock every move held for its full
move-then-commit — because an HRW reshuffle can **chain** (position `p`: F→T while position `q`:
T→U): run concurrently, T is a handoff target and a fenced source at once, and the
drain-to-convergence proof assumes a quiescent fenced source. ADR-092 and ADR-094 both re-deferred
the relaxation ("a conflict-graph rework of `reassign_serial` into a busy-node guard; a throughput
optimization, not a capability gain"). But the global mutex over-serializes: a `reconcile` /
`rebalance_and_move` sweep of K diverged positions runs K `O(corpus)` copies back-to-back even when
the moves touch entirely disjoint nodes, so a large-cluster convergence is bounded by the SUM of
its moves rather than its longest conflict chain. Separately, the raw REST
`POST /_cluster/handoff` path (`execute_handoff` with caller-supplied endpoints) took **no guard at
all** — a latent race against a concurrent `reassign_and_move` of the same position.

**Decision.** Replace the global mutex with a per-node **busy-endpoint move ledger** and run sweep
moves in **conflict-free waves**. All `distributed`-gated; the default (`max_parallel_moves = 1`)
is the sequential path byte-identically — singleton waves execute INLINE on the calling thread,
zero threads spawned.

- **`MoveLedger`** (new [`reassign/ledger.rs`](../../engine/src/cluster/coordinator/reassign/ledger.rs)):
  `Mutex<HashSet<String>>` + `Condvar`, keyed by **resolved endpoint strings**, not `NodeId`s — the
  physical conflicts (the per-slot fence, the slot map, the dict-adopt/`AddShard` handshake) are per
  *server process*, and two `NodeId`s may alias one endpoint (`reassign_and_move` tolerates that as
  a no-op), which a `NodeId` key would let race. `reserve(&[&str]) -> MoveTicket` blocks until the
  WHOLE set is free, **all-or-nothing** (a waiter holds no partial set ⇒ no hold-and-wait; each
  move holds at most one ticket ⇒ structurally deadlock-free). The RAII ticket releases on every
  exit path including unwind. Conflicting operator calls block exactly as under `reassign_serial`;
  **disjoint operator calls now proceed concurrently** — the deliberate relaxation this ADR ships,
  confined to the rare admin/autoscaler move path (the ledger is never touched by percolate/ingest).
- **Reserve sets.** Single move: `{from, to}` (from = the committed primary). Group move (ADR-094):
  `{cp} ∪ endpoints(D)` — the fenced source plus every fresh AND retained member; dropped members
  `C ∖ D` are never contacted, so they are not reserved. Why this covers every conflict class:
  chained reshuffle and shared source/destination share a node by definition; **two moves of one
  position both reserve its committed primary** (the flip-vs-commit interleave `reassign_serial`
  was built for — serialized by construction); a replicated install holds all of D, so no second
  move touches a member mid-assembly.
- **Plan → reserve → revalidate.** Each move resolves its footprint from a committed read, reserves
  it, then **re-reads and confirms the position's committed entry did not change** while it waited
  (the conflicting move it waited on may have committed this very position); a change re-plans from
  the fresh state (bounded, typed error past `PLAN_ATTEMPTS`). The pre-commit CAS stays as the
  final backstop, unchanged.
- **Waves** (new [`reassign/parallel.rs`](../../engine/src/cluster/coordinator/reassign/parallel.rs)):
  `plan_waves` greedily partitions `rebalance_group_targets` output in position order — a target
  joins the current wave iff its footprint is disjoint from every admitted one and the wave is
  under `max_parallel_moves`. **Scheduling-only**: every move still self-reserves, so a stale/wrong
  plan degrades to two moves briefly blocking on the ledger, never to an unguarded conflict.
  `execute_move_wave` runs a multi-move wave on named **scoped `std` threads** (each bridging async
  via the cluster's tokio `Handle` — the safe plain-thread `block_on` case; deliberately NOT rayon,
  which the long-blocking moves would starve and which nests `block_on` hazardously). An OS spawn
  failure degrades that move to inline + an event; a panicking move thread is contained to a
  per-position error (its ticket released by RAII).
- **Knob + API.** `ReconcileConfig.max_parallel_moves` (default **1**);
  `ClusterEngine::reconcile_with(rf, max_parallel, handle)` /
  `rebalance_and_move_with(rf, max_parallel, handle)` with the existing signatures delegating at 1;
  server `--reconcile-max-parallel N`; optional `{"max_parallel": N}` bodies on
  `POST /_cluster/reconcile` and `/_cluster/rebalance {"move": true}`. Reports are
  position-sorted (a no-op at the default, where waves are singletons in target order).
  Sweep semantics preserved: `reconcile` continues past per-position failures across ALL waves;
  `rebalance_and_move` completes the failing wave and does not start the next (`failed` = the
  lowest-position failure; additional same-wave failures fold into `not_attempted` — attempted,
  rolled back cleanly, retried by a re-run).
- **The `execute_handoff` hole fix.** The public `execute_handoff` now reserves
  `{source, target}` itself; the reassign/group paths call a new unguarded
  `execute_handoff_inner` under their own covering ticket (re-reserving would self-deadlock). A raw
  REST handoff can no longer race a reassign of the same position — closing the pre-existing gap.

**Consequences.** A K-position convergence is now bounded by its longest conflict chain (at the
operator-chosen width) instead of the sum of all moves; a packed-origin topology (every move off
one node) legitimately stays sequential — the ledger enforces exactly the ADR-090 constraint, no
more. Each parallel move costs one OS thread + its own gRPC connections for an `O(corpus)` copy;
the knob is operator-sized (no hidden cap). A move needing a busy node can in principle be starved
by a stream of narrower moves — acceptable on an admin path whose passes are already
min-interval-throttled; the sweeps themselves cannot starve (waves drain `remaining` to empty).
The autoscaler's tick keeps `max_parallel = 1` this increment (its moves still self-reserve, so a
tick landing mid-sweep blocks per-node instead of globally). Cross-coordinator atomicity is
unchanged (best-effort CAS; the conditional-propose primitive stays the ADR-090 deferral, still
single-active-coordinator).

**Proof.** `cluster_grpc_oracle::parallel` — a 4-node two-pair topology whose pair-swapped seeding
makes waves deterministically REAL (cross-pair moves disjoint, same-pair moves conflicting):
`reconcile_with(1, 2, ..)` under a firehose writer converges every position (no slot lost, ≡ brute,
idempotent epoch-invariant second parallel pass, resolve-only restart routes zero-FN) +
`rebalance_and_move_with` fixpoint. Unit: 5 ledger tests (disjoint no-block / overlap blocks /
RAII + panic release / all-or-nothing) + 9 wave-planner tests (chained reshuffle, shared
source/destination serialize; disjoint parallelize; caps; cap-1 ≡ sequential order; group
footprint excludes dropped members; determinism; addr-less targets still scheduled) + parallel
sweep semantics (continue-past-failure at width 2; in-process clean no-op, epoch invariant). The
full 47-test distributed oracle stays green — the default path's regression proof.
