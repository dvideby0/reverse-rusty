# ADR-047: Remote live-write partial-apply — observe, fail-closed, repair (`resync`) + the `block_on` thread-context contract

> [Back to the decisions index](../DECISIONS.md)


- **Status:** Accepted + **implemented** (distributed-layer hardening from an external review). The
  in-process / RF=1 default path is **byte-identical** (its `LocalShard` writes are infallible, so no
  partial apply is ever recorded) — `tests/cluster_oracle.rs` + `tests/cluster_durability_oracle.rs` stay
  green unchanged. Proven by `partial_apply_is_detected_then_resync_converges` +
  `resync_requeues_when_shard_still_failing` (`cluster/coordinator/tests.rs`, deterministic, lean core) and
  `grpc_partial_apply_is_detected_and_queued` + `remote_single_target_percolate_safe_from_tokio_worker`
  (`tests/cluster_grpc_oracle.rs`, real wire).
- **Context:** A selective (class-A / class-B-any-of) query is placed on **2+ shards**; the coordinator's
  `apply_add` fanned the inserts out in a loop with `?`, and `apply_remove` summed a `Result` iterator. With
  **remote** shards (the experimental `distributed` layer), shard A's insert can succeed and shard B's RPC
  then fail — leaving the method returning `Err` with shard A already mutated, **no signal and no repair**:
  a silent partial mutation. Because writes are **log-first** (ADR-031), the mutation is durably committed,
  so `ClusterEngine::open`'s replay re-drives every target shard and the divergence **self-heals on reopen**
  — but a *live* cluster stays divergent until then (a transient **false-negative window** on the
  un-applied shard). Separately, the `RemoteShard` sync→async bridge used `Handle::block_on` directly; on
  the single-target read path (`targets.len() <= 1`, the sequential branch) that runs on the *caller's*
  thread, so a future async coordinator probing `percolate` from a tokio worker would hit the
  nested-runtime **panic** the rayon fan-out path happens to avoid. (Surfaced by an external review; the
  in-process core, durable reopen, fan-out bench, and status honesty were verified accurate and need no
  change.)
- **Decision:**
  1. **Detect, don't bail.** `apply_add`'s `Selective` branch and `apply_remove` now **try every target
     shard and collect per-shard failures** instead of bailing on the first error. An empty failure set is
     byte-identical to the old loop (the default path).
  2. **Observe + fail-closed.** A non-empty failure set queues the failed shards for repair (keyed by
     logical id, so a later mutation supersedes an earlier pending one), emits a
     `DurabilityFailure { op: ClusterPartialApply }` event (`is_data_at_risk = true` — a missed match is
     this system's worst outcome), and returns the honest `ShardError::PartiallyApplied { logical, applied,
     failed, detail }`. The error is distinct from a clean `Remote`/`Log` failure so a higher layer can act,
     and documents that the mutation is **committed** (re-`add_query` would double-log; recover via repair).
  3. **Repair (`resync`).** `ClusterEngine::resync()` drains the queue and re-drives each mutation against
     **only its still-failed shards** via the existing `apply_mutation` seam — converging without a full
     reopen. Re-driving touches only failed shards: an Add there is a clean first insert (they never
     received it), a Remove is idempotent — so converged shards are untouched. Idempotent + re-queues a
     still-unreachable shard. The autoscaler `tick` calls it opportunistically (a cheap no-op when empty).
  4. **`block_on` thread-context contract.** All `RemoteShard` RPCs route through `block_on_in_context`,
     which dispatches on the caller's context: off any runtime → plain `block_on` (the rayon-fan-out / build
     path, unchanged); on a **multi-thread** runtime worker → `task::block_in_place(|| block_on)` (the
     documented re-entry; `Runtime::new()` / tonic / axum are all multi-thread); on a current-thread runtime
     → offload to a scoped non-runtime thread.
- **Correctness (load-bearing):** the **durable cluster log stays authoritative** — a reopen replays it in
  order, so `resync` is a *liveness* optimization, not the correctness backstop. `resync` can only **add**
  matches on a lagging shard (closing the FN window), never remove a true match, so it cannot introduce a
  false negative. The in-process / RF=1 path never records a partial apply (infallible writes) ⇒ zero
  behavior change ⇒ the v1 zero-false-negative contract is untouched.
- **Scope / remaining gap (this is the experimental distributed layer, not Cluster v1):** a **single-shard**
  failure (the replicated lane, or a 1-shard selective whose write totally fails) is a clean `Err` that
  converges on **reopen**, not live `resync`. There is still **no cross-write fencing / quorum** — two
  concurrent writers to overlapping shards, or a `resync` racing a same-id write, resolve last-writer-wins
  in memory and authoritatively by the log on reopen; production multi-machine use needs a real fence +
  durable-multi-node rolling-restart harness. The current-thread-runtime `block_on` offload is a fallback,
  not the shipped servers' path (they are multi-thread).
- **Alternatives declined:** *compensating rollback* (delete from already-applied shards on partial
  failure) — fights the log-first model (the mutation is committed; rollback then a reopen-replay would
  resurrect it, an inconsistency between the returned `Err` and the post-reopen state); *two-phase commit /
  quorum write* — the right end state for production, but heavyweight for an experimental layer and a larger
  design (control-plane coupling); *silent self-heal on reopen only* (the pre-ADR behavior) — leaves a live
  FN window with no signal and no live remedy.
- **Consequence:** a mid-fan-out remote write failure is now **visible** (typed error + event + a
  `pending_repairs()` gauge) and **repairable live** (`resync`, plus `tick` auto-heal), instead of a silent
  partial mutation healed only by a full restart; and the `block_on` bridge is safe from any caller thread.
  The honesty the review asked for, now backed by code. Cost: a per-write uncontended mutex touch on the
  (empty) repair queue — negligible, and off the match hot path.
- **See also:** ADR-031 (log-first cluster writes — why a partial apply is *committed*, not lost), ADR-027
  (placement: which queries are multi-shard selective), ADR-029 (the `RemoteShard` gRPC bridge this hardens),
  ADR-044 (the handoff fence this reuses as the test's deterministic write-failure injector), ADR-021 (the
  `EngineEvent`/`DurabilityOp` observability this extends). Code sites:
  `src/cluster/coordinator/ingest.rs` (`apply_add`/`apply_remove`/`note_partial`/`clear_pending`/`resync`/
  `pending_repairs`), `src/cluster/coordinator.rs` (`PendingRepair`/`ResyncReport`/the queue field),
  `src/cluster/coordinator/autoscale.rs` (`tick`), `src/cluster/shard.rs` (`ShardError::PartiallyApplied`),
  `src/events.rs` (`DurabilityOp::ClusterPartialApply`), `src/cluster/remote.rs` (`block_on_in_context`).

