# ADR-048: Reliability hardening — auto-unfence-on-abort, translog-lease TTL, autoscaler-driven handoff

> [Back to the decisions index](../DECISIONS.md)


- **Status:** Accepted + **implemented** (distributed-layer reliability hardening — closes three
  explicitly-deferred items from ADR-040/044/045). The in-process / RF=1 default path is **byte-identical**:
  the lease TTL defaults generous and reaps only a stalled lease (no live recovery is affected); the unfence
  RPC + drain-cap knobs and the autoscaler→handoff wiring are all behind `#[cfg(feature = "distributed")]`,
  so the lean build and `tests/cluster_oracle.rs` + `tests/cluster_durability_oracle.rs` are untouched.
  Proven by `retention_lease_tests::*` + `ttl_reaps_a_stuck_lease_so_the_seal_reclaims_the_tail_and_emits`
  (`src/cluster/{shard.rs,replica/tests.rs}`, deterministic, lean core),
  `grpc_handoff_abort_unfences_source` + `grpc_autoscaler_tick_drives_handoff_resolution_and_preserves_matching`
  (`tests/cluster_grpc_oracle.rs`, real wire), and `tick_emits_handoff_under_skew_without_perturbing_matching`
  (`tests/cluster_autoscale_oracle.rs`).
- **Context:** Three reliability gaps in the experimental distributed layers each left a cluster in a state
  needing **manual operator recovery** or left a control loop **open**:
  1. **Auto-unfence-on-abort (closes ADR-044's deferred note).** `execute_handoff` FENCES the source
     (write-quiesce), drains its tail to the new owner, then flips routing. If the post-fence drain fails to
     converge within its cap — or any post-fence RPC errors — it aborts **fail-closed but leaves the source
     permanently fenced**, rejecting writes until a manual restart. The fence was monotonic (`fetch_max`) with
     **no inverse** (no Unfence RPC).
  2. **Translog-lease TTL (closes ADR-040's "no time/size cap on a stuck lease yet").** A peer-recovery
     retention lease pins the source's translog tail so a concurrent seal can't trim it. With no expiry, a
     **crashed recovering node leaves its lease held forever**, so the source can never GC its tail.
  3. **Autoscaler-driven handoff (closes ADR-045's "load-driven handoff is advisory only").** `node_skew`
     emits a `Handoff` recommendation, but the `tick` driver executed only `Rebalance`; the recommendation
     was returned and **never acted on**.
- **Decision:**
  1. **`Unfence` RPC + auto-unfence (item 1).** Add an `Unfence` RPC that lifts a fence held at EXACTLY a
     given generation — a **compare-and-swap from `generation` back to 0**, so it preserves the Fence
     monotonic-safety story: a stale Unfence, or a node since re-fenced at a higher generation by a newer
     handoff, is a safe no-op. `execute_handoff` wraps the **entire post-fence section** (drain loop +
     convergence check) so that *any* failure after the fence calls `source.unfence(new_gen)` before
     returning the original error; if the unfence RPC itself fails, that is surfaced as a
     `DurabilityFailure { op: ReplicaDesync }` event (the source then truly needs manual recovery) but does
     not mask the original abort cause. The hardcoded drain caps become `ClusterConfig`
     knobs (`handoff_drain_passes` / `handoff_final_drain_cap`, defaults 8 / 1024) — operator-tunable, and a
     test sets the final cap to 0 to force the abort deterministically.
  2. **Lease heartbeat + TTL reap (item 2).** Each retention lease carries a `last_renewed: Instant`;
     **`renew` refreshes it** (it is the recovery's heartbeat, called every catch-up pass), and
     `seal_for_checkpoint` reaps any lease not renewed within `retention_lease_ttl_secs` (default 1800;
     `0` disables — byte-identical to ADR-040) before computing the trim floor. A reap surfaces a
     `DurabilityFailure { op: ReplicaDesync }` event so an abandoned recovery is observable, not silent —
     for which a plain `LocalShard` now honors the coordinator's event sink (it ignored it before). The TTL
     reap takes an explicit `now`, so `seal_for_checkpoint` delegates to a clock-injectable
     `seal_for_checkpoint_at(now)` and the whole seal path is deterministically testable.
  3. **Wire `Handoff` → `execute_handoff` (item 3).** The `tick` driver retains the runtime handle the gRPC
     builders connected on, resolves the `Handoff`'s `from`/`to` node ids to endpoints via
     `control_state().nodes`, re-validates the source still owns the position, and calls `execute_handoff`.
     **Ordering guard:** a handoff is driven only when **no `Rebalance` ran this tick** — a rebalance moves
     placement and would make the pre-rebalance `from`/`to` stale; the handoff is re-evaluated next tick. A
     failed move is **logged-and-continued** (it self-heals via item 1's auto-unfence), never failing the
     tick. The whole arm is `#[cfg(feature = "distributed")]`, so the lean build returns the recommendation
     without acting (a `Handoff` can't even arise in-process — `node_skew` needs ≥2 loaded nodes).
- **Why these are safe (no false negative):**
  - *Unfence CAS:* clearing the fence only at the exact generation this handoff set means a concurrent /
    newer handoff is never disturbed; on the success path the old owner stays fenced/dropped (serve-then-drop
    is unchanged). Reads were never fenced.
  - *Lease reap:* `renew` is a heartbeat, so a live recovery (renewing every pass) is never reaped — only a
    recovery that stopped heartbeating for > TTL (a generous 30-min default = effectively dead). A
    reaped-then-incomplete replica is gated out by the existing read-failover "in-sync only" check before it
    can serve a read, so no client sees a short answer. (Known edge: the initial segment-stream phase before
    the first catch-up does not renew, so a single-shard recovery streaming for > TTL could be reaped
    mid-stream — acceptable under a generous default; renewing during streaming is a follow-on.)
  - *Autoscaler handoff:* `execute_handoff` is itself lossless (peer-recover → fence → drain-to-convergence →
    flip, ADR-044) and now self-heals on abort (item 1); the ordering guard avoids a stale-target move. The
    oracle asserts `percolate ≡ brute` across the tick.
- **Scope / remaining gap (still the experimental distributed layer):** driving a handoff *cleanly to
  completion* from `tick` over gRPC additionally needs the control-plane node→endpoint map to match the
  actual per-shard endpoints — `node_skew` selects an existing primary as the move target, and the
  one-server-per-shard transport can't host a moved shard on a busy endpoint, so a real load move needs
  either multi-shard-per-node endpoints or a spare-node target. That is **deployment-model maturity** (Tier-3
  residue), so the gRPC oracle proves the driver's **resolution + fail-safe skip + zero-FN** path (the move's
  happy path is proven separately by `grpc_live_handoff_under_sustained_writes`, a direct `execute_handoff`).
  Untouched residue: auto-split + `recommended_shard_count`, replicate-broad-to-all, TLS/auth, and the
  durable-multi-node rolling-restart harness.
- **Alternatives declined:** *clear the fence with a plain `store(0)`* — drops the monotonic-safety guard
  (a stale Unfence could lift a legitimately-newer fence); the CAS keeps it. *Reap leases on a background
  timer thread* — adds a thread + wakeups to a single-node engine; the seal is the natural, already-periodic
  reap point. *Make the autoscaler handoff fail the tick on error* — a transient move failure would block
  the (idempotent, useful) rebalance path; log-and-continue + auto-unfence is the resilient choice.
- **Consequence:** an aborted handoff self-heals (the source resumes serving), a crashed recovery's lease
  expires so the source reclaims its tail, and the autoscaler's load-balancing loop is closed end-to-end —
  all observable via `EngineEvent`s, all proven `percolate ≡ brute`. Cost: one `Instant` per lease, a CAS on
  unfence, and a retained runtime handle on a gRPC cluster — all off the match hot path.
- **See also:** ADR-040 (retention leases — the TTL closes its deferred cap), ADR-043/044 (the fence + live
  handoff — auto-unfence closes 044's deferred note), ADR-045 (the autoscaler policy — this drives its
  advisory `Handoff`), ADR-021 (the `EngineEvent`/`DurabilityOp` observability reused), ADR-039 (the seal/trim
  the reap hooks into). Code sites: `engine/grpc/proto/shard.proto` (`Unfence`), `src/cluster/server/service.rs`
  (`unfence` handler), `src/cluster/remote.rs` (`RemoteShard::unfence`), `src/cluster/coordinator/distributed.rs`
  (`execute_handoff` auto-unfence + drain caps), `src/cluster/coordinator/autoscale.rs` (`tick` +
  `drive_autoscaled_handoff`), `src/cluster/shard.rs` (`RetentionLeases` TTL + `seal_for_checkpoint_at` + the
  `LocalShard` event sink), `src/config.rs` (`retention_lease_ttl_secs`), `src/cluster/coordinator.rs`
  (`ClusterConfig` drain caps + the retained handle).

