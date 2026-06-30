# ADR-092: Unattended re-point reconciler (`reconcile` + `--reconcile-interval-secs`)

**Status:** Accepted (2026-06-30)

**Context.** [ADR-090](adr-090-data-moving-reassignment.md) shipped data-moving reassignment
(`reassign_and_move` / `rebalance_and_move`, move-then-commit, zero-FN), but it must be **manually
triggered** — an operator `POST /_cluster/reassign|rebalance{move:true}`, or the autoscaler's
event-driven hooks. Two gaps remained, both recorded as ADR-090 deferrals:

- **No steady-state watcher.** Nothing reconciles the committed shard→node map to the desired HRW
  placement *unattended* — a membership change converges only when a human (or the event-driven
  autoscaler) acts.
- **A latent ADR-086 false-negative trap in the autoscaler.** The autoscaler's membership-drift arm
  executed `Rebalance` via the **map-only** [`rebalance`](../../engine/src/cluster/coordinator/control_plane.rs)
  (`self.rebalance(rf)`), which permutes the committed map **without moving data**. On a
  `--route-by-assignments` remote cluster that manufactures exactly the shard-sized false negative ADR-086
  warns about (the committed map names a node that does not hold the position's data; the boot guard then
  refuses it). The skew-driven `Handoff` arm was already data-moving (ADR-090's
  `drive_autoscaled_handoff`); only the membership-drift arm was unsafe.

**Decision.** Add the unattended re-point controller on top of ADR-090's primitives, plus the autoscaler
safety fix. All engine code is `distributed`-gated; the loop is off by default; the in-process / lean /
RF=1 path is byte-identical.

- **`ClusterEngine::reconcile(rf, handle) -> ReconcileReport`** (new `distributed`-gated submodule
  `engine/src/cluster/coordinator/reconcile.rs`). Reads the committed state, computes the positions whose
  **primary** diverges from the HRW-desired map (the **reused** `rebalance_targets`), and drives the
  data-moving `reassign_and_move` for each — **sequentially**, position order (the same chained-reshuffle
  constraint `rebalance_and_move` obeys). It differs from `rebalance_and_move` in two deliberate ways: it
  is the **unattended** primitive, so it **continues past per-position failures** (recording them and
  retrying next pass — each position is independent and individually move-then-commit, so making maximum
  safe progress beats stalling), and it returns a richer `ReconcileReport { reconciled, skipped,
  uncommitted, failed }`. Empty (a clean no-op) for an in-process / genesis cluster (no addr'd data
  nodes). **RF>1 rejected** (same reason as ADR-090).

- **Autoscaler fix.** `tick`'s membership-drift `Rebalance` arm now drives the **data-moving**
  `rebalance_and_move` when `self.handle.is_some()` (a gRPC-built remote cluster), and keeps the map-only
  `self.rebalance` otherwise. The `handle.is_some()` gate makes the in-process / lean path byte-identical
  (only a remote cluster carries a runtime handle); the remote arm swaps the unsafe map-only rebalance for
  the safe data-moving one. Best-effort (a partial/failed sweep emits an event and is retried by the next
  tick or the reconcile loop), never failing the `tick` — mirroring `drive_autoscaled_handoff`. The
  autoscaler (event-driven) and the reconcile loop (steady-state) become two idempotent triggers for the
  same safe primitive.

- **The unattended driver — an opt-in server-layer loop.** `--reconcile-interval-secs N` on the
  coordinator server spawns a background tokio task (`engine/src/bin/server/cluster_mode/reconcile_loop.rs`)
  that periodically runs `reconcile` on the blocking pool (`execute_handoff` does `block_on` internally —
  it must run off a runtime worker). The **engine stays thread-free and clock-free**: the loop owns the
  runtime and the wall-clock min-interval; the engine method is a pure, idempotent state transition. It
  reuses the existing graceful-shutdown signal (aborted before the durability flush, so a pass never races
  the checkpoint — an in-flight pass on the blocking pool finishes its current move-then-commit safely,
  the handoff tolerating concurrent flushes per ADR-044). Fails loud at startup if set without
  `--route-by-assignments` (reconciling a map the coordinator does not route by is a footgun).

- **REST `POST /_cluster/reconcile`** — a one-shot manual trigger of one `reconcile` pass (the controller's
  continue-past-failures semantics without enabling the loop), `distributed`-gated with a non-distributed
  501-with-reason. A thin `control_version()` pass-through lets the loop cheaply poll the committed epoch
  (no full-document clone) to skip a pass when nothing changed since the last fully-converged one.

**Hysteresis — two layers, cleanly separated.** (1) *Controller idempotence (the engine):* a converged
map yields no targets ⇒ no proposals ⇒ the control-plane epoch is invariant; back-to-back passes commit
nothing. This is the correctness hysteresis. (2) *Wall-clock min-interval (the driver loop only):* each
move is `O(corpus)`, so a membership-flap storm must not re-move on every edge; the loop sleeps at least
`min_interval` (default 30s) between passes. This wall-clock state lives ONLY in the loop — never in the
engine.

**`MovedButNotCommitted` is an idempotent re-drive, not a commit-only fast-path.** Both ADR-090 trigger
sites (a CAS loss to a concurrent move; a persistent commit failure under a down quorum) self-heal on the
next pass: `rebalance_targets` either no longer lists the position (already converged) or still lists it,
and re-running `reassign_and_move` re-converges the already-populated target (the fenced source still
serves the read-only recovery RPCs — cheap) and re-commits. A bare re-commit without re-running
`execute_handoff` would be unsafe under a second coordinator (only the fence + drain-to-convergence proves
the target holds every acked write), so we re-drive the whole move. The report's `uncommitted` field is
observability only.

**Consequences / scope.**

- **Default byte-identical.** The `reconcile` submodule, the `ReconcileConfig`/`ReconcileReport` types,
  the loop, and the autoscaler's remote arm are all `distributed`-gated and/or `handle.is_some()`-gated;
  the loop is off unless `--reconcile-interval-secs` is set. The matching hot path is untouched (the
  control plane is read only at admin/restart time; the reconcile loop runs off the hot path).
- **Closes the ADR-090 "unattended assignment-watch controller" deferral** and removes the latent
  autoscaler false-negative on a route-by-assignments cluster. **Parallel** multi-position moves remain
  deferred (moves stay sequential — the chained-reshuffle hazard + the per-`ShardServer` fence make safe
  parallelism a conflict-graph problem reworking `reassign_serial`; it is a throughput optimization, not a
  capability gain, and is the next increment). RF>1 + cross-coordinator conditional-propose stay deferred
  (ADR-090).
- **Proven.** `cluster_grpc_oracle::reconcile` (real gRPC servers): the headline — a diverged committed
  map converges to the HRW-desired owner under a concurrent writer, a second pass is a no-op (idempotence /
  epoch invariant), and a fresh coordinator routing by the committed map is zero-FN; plus the autoscaler
  fix — `tick` on a remote cluster drives a data-moving rebalance (the generation bumps — not a map-only
  rebalance) and stays zero-FN live + across a restart. `tests/cluster_reconcile_oracle.rs` (in-process):
  `reconcile` is a clean no-op, percolate byte-identical (broad on + off), epoch invariant, idempotent.
  Plus `reconcile.rs` unit tests (the report helpers + the disabled-by-default config). The full
  `distributed` oracle + the existing autoscale oracle stay green.
