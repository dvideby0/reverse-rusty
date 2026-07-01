# ADR-094: Group-aware data-moving reassignment — RF>1 reconcile (`reassign_group_and_move`)

**Status:** Accepted (2026-07-01)

**Context.** Every data-moving sweep was RF=1-only. [`execute_handoff`](../../engine/src/cluster/coordinator/distributed.rs)
(ADR-044) swaps a position's backing to a **single** `RemoteShard` for the target, so a replicated
position's move would **de-replicate** it — the live group collapses to one copy while the committed
map still advertises the old replicas, and a later failover could read a replica that no longer
receives writes (a stale-read false negative). `reassign_and_move` therefore rejected replicated
clusters (ADR-090), and the ADR-092 landing hardened both `rebalance_and_move` and `reconcile` to
also reject `rf > 1` *requests* (a codex finding: an rf>1 request on a bare cluster planned RF-2
placements, moved primaries only, and reported convergence with the replicas silently never
created). Meanwhile ADR-093 Stages 1–3 built exactly the machinery a replicated move needs: per-slot
fence/recovery/`shard_<id>/` storage, `connect_and_adopt`'s idempotent slot handshake,
`connect_replicated` group composites behind the `HandoffShard` swap seam, and the committed
document already carries full groups (`ShardAssignment { position, primary, replicas }`) end-to-end
through seed/resolve/boot. This ADR closes the ADR-090 RF>1 deferral on that foundation.

**Decision.** Add the replicated **group move** and dispatch the sweeps by *shape*. All
`distributed`-gated; the in-process / lean / RF=1 default paths are byte-identical (the RF=1 sweep
still runs the proven single-shard `reassign_and_move`, byte-for-byte).

- **`rebalance_group_targets(state, rf) -> Vec<(u32, ShardAssignment)>`** (replacing the primary-only
  `rebalance_targets`, whose rf=1 behavior it reproduces exactly): positions whose committed GROUP
  diverges from `plan_assignments(rf)` — primary by identity, **replicas as a SET**. The set-compare
  is load-bearing: `seed_position_preserving` commits replicas in CLI order while the plan emits HRW
  rank order, so a `Vec` compare would flag every healthy cluster as diverged and drive K spurious
  `O(corpus)` moves on the first pass. Replica *order* is only the composite's failover try-order,
  never placement.
- **`ClusterEngine::reassign_group_and_move(position, desired: ShardAssignment, handle)`** (new
  submodule [`reassign/group.rs`](../../engine/src/cluster/coordinator/reassign/group.rs)) — the
  move-then-commit generalization, under `reassign_serial` + one retention lease like
  `execute_handoff`. With C = committed group, D = desired, cp = C.primary:
  1. *Plan:* fail-closed endpoint resolution; C == D under the set-compare ⇒ `NoChange`. The
     recovery source is **always cp** — it is write-authoritative, so it alone provably holds every
     acked write without trusting replica in-sync state; a primary-down position fails loudly
     (degraded repair stays `peer_recover_replica`'s job).
  2. *Pre-fence:* establish **fresh** members `F = D ∖ C` (in no composite ⇒ never serving ⇒ safe to
     bulk-replace): adopt → `RecoverFrom` → bounded drain, writes still flowing.
  3. *Fence cp's slot only.* The composite write path is primary-first and never falls over for
     writes (`replica/shard_impl.rs`), so **one fence write-quiesces the whole group**; fence-window
     writes queue as pending repairs and re-drive post-swap through the swapped backing (`resync`).
  4. *Freeze-probe:* loop `translog_tail` until an empty pass (bounded) — the fenced tail is finite,
     so a stable read marks the frozen high-water. Needed as its own step: promotion / replica-only
     moves have `F = ∅` (no fresh member whose drain would witness convergence), and with several
     members per-member drains alone establish no *common* frozen point.
  5. *Drain F to the frozen tail* (per-member catch-up to stability; abort past the cap ⇒
     auto-unfence + `Err`, the ADR-048 rollback — routing + map untouched).
  6. *Re-establish retained members* `R = (D ∩ C) ∖ {cp}` — **only post-freeze**: adopt (no-op
     handshake) → `RecoverFrom` → a verify catch-up that must return no tail. `RecoverFrom`
     REPLACES the slot state, which is exactly right here: the source **seals before streaming**
     and the install is one atomic per-slot store, so a copy of the fenced-and-frozen source is
     **complete at install** — a silently-desynced committed replica is deterministically *healed*
     without the coordinator ever reading the composite's private in-sync state. Pre-freeze this
     would be unsafe (an R member is live in the old composite; a segments-at-`P` install would
     serve a state missing the tail). cp ∈ D is never recovered — it IS the frozen authority, so
     promotion/demotion fall out of the uniform algorithm with no special case.
  7. *Assemble + swap:* the new backing in D's shape (`ReplicatedShard` over the per-member
     connections; bare `RemoteShard` when D has no replicas — an rf *reduction* falls out free),
     with the coordinator's observer installed as its event sink **before** `swap_backing`
     (`set_observer` fans sinks only at install time — a later-swapped composite would otherwise
     buffer its `ReplicaDesync` events forever, a gap this ADR closes).
  8. *Unfence cp iff cp ∈ D, after the swap* — earlier would reopen the write window on the old
     composite; skipping it would make the retained/demoted cp fail its first fan-out and silently
     desync. cp ∉ D stays fenced forever (serve-then-drop + stale-coordinator write protection,
     the ADR-090 posture). Orphan slots on `C ∖ D` nodes are unrouted post-swap and post-restart
     (routing is the swapped composite; `resolve_topology` reads the committed map); GC deferred.
  9. *Move-then-commit:* manual compare on the **full group** (strictly stronger than the RF=1
     primary-only compare) then `AssignShard(desired)` with bounded retries. Outcomes reuse
     `ReassignOutcome` (`from`/`to` = the primaries); `MovedButNotCommitted` re-drives idempotently.
- **Dispatch by shape.** `rebalance_and_move` and `reconcile` compute group targets and route each
  position: committed AND desired bare ⇒ `reassign_and_move` (the proven RF=1 path,
  byte-identical); anything touching replicas ⇒ the group move. Their rf>1 request rejects are
  removed; `reconcile` keeps its continue-past-failure report semantics at RF>1 unchanged.
- **`reassign_and_move`'s guard narrows to per-position:** reject only a position whose *committed*
  assignment has replicas (a single-target move of a group is ambiguous; the message points here) —
  a bare position on a replicated cluster is a plain single-shard move.

**Crash-window table** (generalizing ADR-090's; every row zero-FN). *Pre-fence crash:* map + routing
untouched; orphan target slots only (the aborted-handoff residue). *Post-fence, pre-swap:* the map
names C; cp is fenced write-only and **serves reads**; a restart boots C from the committed map; the
stranded fence needs the ADR-048 manual-unfence story. *Post-swap, pre-commit:* the map names C,
whose cp still holds every acked write and serves reads — the restart routes zero-FN; live routing
serves D. *Commit failed:* `MovedButNotCommitted`; live on D, durable map on the reads-serving C;
the next pass re-drives idempotently.

**Cost (deliberate).** The fence window includes an `O(corpus)` re-copy per **retained** member —
the price of provable completeness without in-sync introspection (a pure promotion re-copies a
member that is almost certainly already complete). Writes during the window are never lost: they
return `PartiallyApplied` and re-drive via `resync` into the new group. Deferred optimizations,
recorded here: an in-sync-snapshot + content-fingerprint protocol to skip provably-complete members,
and a server-side staged recovery (shadow install, atomic promote) that moves retained-member copies
out of the fence window entirely.

**Consequences / scope.**

- Closes the ADR-090 RF>1 data-moving deferral: `reconcile` + `rebalance_and_move` now converge
  replicated clusters (the reconcile loop / REST already pass the cluster's real
  `replication_factor` — no server-surface change). The boot-time in-sync presumption for committed
  replicas (`connect_replicated` marks all boot replicas in-sync) is pre-existing and unchanged —
  this ADR guarantees completeness **at commit time**.
- **Still deferred:** cross-coordinator conditional-propose (the commit compare stays best-effort
  single-active-coordinator, ADR-090); orphan-slot GC; degraded-source moves (primary-down → fail
  the position; a failover controller is a separate increment); parallel multi-position moves
  (sequential, the chained-reshuffle constraint); the two fence-window cost optimizations above.
- **Proven** (`tests/cluster_grpc_oracle/reconcile_replicated.rs`, real gRPC servers): the packed
  RF=2 headline — every group converges to the HRW plan (set-compare), zero-FN vs brute, idempotent
  second pass (epoch + generations invariant), an RF=2 coordinator restart boots from the resolved
  committed map, and **the de-replication kill shot** — stopping a moved position's NEW primary
  node still serves every title from the new replicas, live and on the restarted coordinator; the
  same under a firehose writer with fence-window writes re-driven into the new group; the
  replica-only and pure-promotion single-position shapes (each with a node-kill failover proof of
  the newly-placed/retained member, incl. the unfence-after-swap); the forced freeze-probe abort
  (map + epoch + routing untouched, source auto-unfenced, zero-FN); and continue-past-a-downed
  -target at RF>1 with a self-healing second pass. Plus `reassign/group/tests.rs` unit tests
  (the group-diff matrix: primary/replica/both divergence, replica-order-insensitive no-op,
  manager/addr-less exclusion, rf clamp, missing-assignment ⇒ diverged) and the reworked
  dispatch/continue-past-failure tests in `reconcile.rs`/`reassign.rs`. The full distributed
  oracle (44 tests) stays green — the RF=1 paths are unperturbed by the dispatch.
