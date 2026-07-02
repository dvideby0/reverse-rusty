# ADR-096: Orphan-slot GC — `ListShards`/`DropShard` + the coordinator sweep

**Status:** Accepted (2026-07-02)

**Context.** Serve-then-drop (ADR-090) deliberately never tears the old owner down: after a
data-moving move, the source node's slot stays in its slot map — fenced, still serving reads and
recovery RPCs — because in the `MovedButNotCommitted` crash window the committed map still names
that source and a restart must land on a data-holding, reads-serving node. But nothing ever
finished the "drop": the slot's engine stayed resident, its `shard_<id>/` dir stayed on disk, and
a durable restart (`restore_durable_slots`) re-attaches **every** `shard_<id>/` dir it finds — so
disk and memory grew with every move, forever (ADR-094 recorded the deferral). Three facts shape
the fix (all code-verified): **fences are not durable** (`ShardSlot.fenced_at_generation` is a
plain `AtomicU64`, rebuilt 0 on restart — a restarted orphan comes back unfenced); **the committed
map alone is an unsafe keep-set** (the flip-without-commit states — a raw `POST /_cluster/handoff`
and `MovedButNotCommitted` — leave live routing pointing at a node the map does not name; the
oracle proves reads serve from exactly such nodes); and **an in-place `remove_dir_all` can brick a
node** (interrupted mid-delete it leaves a live-named dir whose checkpoint sidecar lists
already-deleted segments — the reopen fails loud and the node cannot boot).

**Decision.** A coordinator-driven sweep over two new guarded RPCs. All `distributed`-gated,
default-off; the in-process / lean / default paths are byte-identical.

- **RPCs** (additive; an old peer answers `Unimplemented` and the sweep skips that node):
  `ListShards` — the node's slot inventory (per slot: fence generation, live count, unexpired
  retention leases) plus the node's dict/tag-dict fingerprints, the sweep's identity check.
  `DropShard{shard_id, expected_fence_generation, fingerprints}` — remove the slot from the map +
  reclaim its dir. Per-slot (not a keep-set RPC): one-slot blast radius per call, individually
  fence-armable, and the server stays a dumb executor.
- **The `DropShard` guard ladder** (`server/service/gc.rs`): node fingerprints match → a zero arm
  is `InvalidArgument` (a cold drop is structurally refused — destroying data requires the
  deliberate fence-then-drop two-step) → absent slot ⇒ `dropped=false` (idempotent) → the slot
  must be fenced at **exactly** the armed generation → no unexpired retention lease (an in-flight
  recovery's pinned source is never destroyed; `LocalShard::has_unexpired_retention_leases`
  reaps-first so a crashed recovery's stale lease cannot block GC forever) → the fence is
  **re-checked under the slot-map write lock** at removal (the CAS — an interleaving
  fence/unfence by a newer handoff fails the drop loud). In-flight RPCs holding the old slot
  `Arc` complete against it; a post-check lease race is data-safe (open fds/mmaps survive the
  rename).
- **Disk reclaim = rename-to-trash, then delete** (`server/durable.rs`): rename
  `shard_<id>/` → `shard_<id>.dropped.<nanos>` (one atomic step; the trash name does not parse as
  a slot, so a restart can never re-attach it) + fsync the parent, then best-effort
  `remove_dir_all`. `open_durable` sweeps leftover trash at boot (never fails boot — the
  ADR-078/079 posture). Rejected: in-place delete (the brick-the-boot scenario above);
  rename-only (leaks the disk the feature exists to reclaim).
- **The sweep** (`coordinator/gc.rs`, `gc_orphan_slots -> GcReport`): reserve **every addr'd data
  node** in the ADR-095 move ledger for the whole sweep — strictly coarser than any move's
  footprint, so no move-then-commit *and no raw handoff* can interleave (composing with ADR-095's
  `execute_handoff` guard closes that race structurally, not by documentation) — then re-read the
  committed state under the reservation and classify every hosted slot (pure `classify_slot`,
  unit-tested): position unassigned ⇒ fail-safe skip; committed to this node (primary or replica —
  covers `MovedButNotCommitted`) ⇒ keep; this node's endpoint in the position's
  **`Shard::live_endpoints`** ⇒ keep (the new trait introspection: `RemoteShard` reports its
  connect endpoint, `ReplicatedShard` primary + all replicas — in-sync or not, conservative —
  `HandoffShard` forwards to its backing; endpoint compare normalized, ambiguity keeps); else
  orphan ⇒ probe `fence(0)`, arm a restart-orphan at `max(epoch,1)`, `DropShard` at exactly that
  generation. Per-slot failures land in the report and the sweep continues (the reconcile
  posture); a second sweep is idempotent.
- **Wire-up** (default byte-identical): `ReconcileConfig.gc_orphans` (default `false`) runs the
  sweep as a reconcile-loop epilogue **only after a fully-converged pass** (belt on top of the
  keep-set); `--reconcile-gc-orphans`; one-shot `POST /_cluster/gc` (mutating ⇒ behind the
  ADR-062 auth gate).

**Consequences.** Moves stop leaking: the steady state after a converged reconcile + sweep is
exactly the committed placement's slots, on disk and in memory. The stale-coordinator write
protection serve-then-drop provided is *strengthened*: a dropped slot answers `not_found` (fail
loud) instead of silently serving stale data. A deliberately malicious/stale second coordinator
running the full fence-then-drop two-step could still destroy data — unchanged from the ADR-090
single-active-coordinator posture (`clear_stale_fence` has the same scope). The truly-unassigned
fail-safe branch cannot be constructed through the public APIs (`connect_remote` seeds a default
assignment per position) — it is unit-proven; the oracle proves the live-routed keeps instead.

**Proof.** `cluster_grpc_oracle::gc` — the reclaim-after-relocation primary (exactly the moved-away
slot dropped, dir gone, the co-located sibling byte-identical, ≡ brute, idempotent second sweep, a
durable restart re-attaches ONLY the survivor); the **flip-without-commit keep-set kill shot** (the
raw-handoff state loses nothing — source committed-kept, target live-routing-kept; committing makes
the source a true orphan the next sweep reclaims); the **unfenced restart-orphan** arm-and-drop
(fences are not durable); the committed-elsewhere-but-live-routed keep. Server units: the guard
ladder (zero-arm, mismatch, divergent fingerprints, held lease → release → drop), the durable
end-to-end drop (dir + trash gone, idempotent re-run), the boot trash sweep. Coordinator units:
the four classification classes + the in-process clean-no-op (epoch invariant). Plus the
`live_endpoints` forwards regression (HandoffShard) and the `gc_orphans` default-off config test.
Full 51-test distributed oracle green.
