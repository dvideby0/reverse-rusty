# ADR-040: Translog retention leases + finalize under sustained writes (clustering step 5d)

> [Back to the decisions index](../DECISIONS.md)


- **Status:** Accepted.
- **Context:** ADR-039 made peer recovery no-quiesce by streaming a peer's segments at position `P` then
  replaying the translog tail (> `P`). Its own *Honest scope* flagged two coupled gaps that 5d closes:
  1. **A latent false negative under a concurrent seal.** ADR-039's `seal_for_checkpoint` trims the translog
     unconditionally to its checkpoint `P`. If a recovery has snapshotted segments at `P_snap` and is still
     replaying the tail, a *concurrent* seal (another recovery's `FetchSegments`, a checkpoint) trims the source
     past `P_snap`, moving those ops into NEW segments the recovering node never copied — so its
     `translog_tail(P_snap)` silently loses them. The no-quiesce oracle didn't hit this only because it ordered
     snapshot → write → catch-up with no second seal; concurrent recoveries from one source are a real
     deployment shape.
  2. **No bounded finalize under sustained writes.** A single seal→copy→catch-up leaves the replica caught up
     to a high-water, but writes that landed during the catch-up are still behind. Promoting the replica
     into the in-sync set without a final reconciliation would silently miss those writes.
- **Decision — retention leases (the Elasticsearch *peer-recovery retention lease*), `src/cluster/{shard,replica,server,remote,coordinator}.rs`:**
  - **A lease registry on the recovery source.** `LocalShard` holds `Mutex<RetentionLeases>` (`lease_id →
    retained_pos`). `seal_for_checkpoint` now trims to **`min(P, leases.floor())`** instead of `P`; with no
    lease the floor is absent and it trims to `P` — **byte-identical to ADR-039**. The sidecar's
    `local_checkpoint` stays `P` (segments still capture ≤ `P`); any retained ops in `(trim_to, P]` are
    redundant with the segments and position-filtered out on replay (`replay(P)` ⇒ ops > `P`), so the
    self-restart path is unchanged. Three new `Shard` methods (defaults: a no-op lease at `LogPos(0)`, so
    in-memory / remote-less shards and every non-recovery caller are untouched): `acquire_retention_lease()
    -> (id, pos)` pins at the current high-water; `renew_retention_lease(id, to)` advances it (monotonic) as
    a consumer catches up so the prefix can GC; `release_retention_lease(id)` drops it. `ReplicatedShard`
    delegates all three to its primary (the recovery source).
  - **Why a lease and not "don't trim during recovery."** A single global "recovering" flag breaks under
    concurrent recoveries; leaving the translog untrimmed unbounds it. The lease set takes the MIN across
    holders (correct for N concurrent recoveries) and trims freely the instant the last lease drops (bounded
    GC). The acquire's read-then-register is benign under a racing seal: a seal that trims to `L' > at` before
    the lease registers also *sealed* `(at, L']` into segments, so a recovery copying segments at `P ≥ L'`
    still has them; once registered, no later seal trims past `at`.
  - **The finalize (bounded quiesce), `ReplicatedShard::add_recovered_replica` + `ClusterEngine::add_replica`
    / `peer_recover_replica`.** Hold ONE lease across the whole flow: peer-recover a snapshot + initial tail,
    then **loop** `catch_up_replica` (renewing the lease each pass) until the tail stops advancing — shrinking
    the residual a final quiesce must cover toward zero. In-process, the promotion drains the last residual
    and inserts the replica into the in-sync set **under the composite `write_lock`** (so no write slips
    between the final drain and the in-sync insertion — an atomic promotion); `replicas` became
    `Mutex<Vec<Arc<ReplicaSlot>>>` to allow this runtime growth, with reads/fan-out snapshot-cloning the `Arc`
    handles so a slow probe never holds the lock. **Correctness never depends on the loop converging** — the
    lease keeps the tail safe regardless; only the residual *window size* does (`max_passes` bounds it).
  - **Over gRPC.** A `RetentionLease(op, lease_id, pos, dict_fingerprint)` RPC (op 0/1/2 = acquire/renew/
    release; dict-fingerprint-guarded like `FetchTranslog`) plumbs the three methods to the server's shard;
    `peer_recover_replica` acquires the lease before the segment copy, holds it across the convergence loop,
    and releases on completion (a release failure on an otherwise-good recovery is surfaced as a
    `ReplicaDesync` event, never conflated with the recovery outcome). Additive proto, zero `build.rs` change.
- **Honest scope.** The in-process finalize promotes atomically under `write_lock` (fully lease-protected
  end-to-end). The gRPC `catch_up_recovered_replica` is a *lease-free* manual pass for callers that have
  externally quiesced — a concurrent seal during it could still strand it, so the retention-safe gRPC entry is
  `peer_recover_replica` (which holds the lease across its own convergence loop); a true cross-node in-sync
  *promotion* of a remote replica (vs. the test's separate verify cluster) routes through the allocator
  (ADR-042) and is not yet wired. Retention is keyed by lease only — there is no time/size cap on a stuck
  lease yet (a crashed recovering node leaves its lease until the source restarts or a future lease-expiry
  policy lands). Deferred unchanged: TLS/auth, bounded-memory streaming of a very large tail.
- **Consequence:** A concurrent seal can no longer trim away an in-flight recovery's tail (the latent FN is
  closed), the translog GCs the moment no recovery needs it (no unbounded growth), and a replica can be grown
  into a live position at runtime without pausing writes — the quiesce window is the residual delta, not the
  whole copy. Default in-memory / RF=1 paths are byte-identical (no lease ⇒ trim to `P`), so every prior
  oracle is unchanged and green. Proven by: `replica.rs::seal_honors_retention_lease_so_concurrent_seal_keeps_the_recovery_tail`
  (a second seal during a held lease keeps the tail; releasing it lets the source GC) and
  `::add_recovered_replica_promotes_an_in_sync_set_equal_replica` (runtime growth → an in-sync, set-equal
  replica that receives post-promotion writes); `tests/cluster_grpc_oracle.rs::grpc_peer_recovery_converges_under_sustained_writes`
  (a writer thread streams adds CONCURRENTLY with the recovery; the lease keeps the racing writes safe and the
  target converges to live source ≡ brute over the final set). Full `check.sh` green. The lease registry +
  finalize are std-only (lean core); the `RetentionLease` RPC is `distributed`-gated.
- **See also:** ADR-039 (the no-quiesce translog whose two scope gaps this closes), ADR-036/035 (the gRPC +
  in-process peer recovery the lease protects), ADR-031 (the `LogPos`/`ClusterLog` machinery), ADR-042 (the
  allocator that will drive cross-node promotion), ADR-033 (shared-nothing),
  `src/cluster/{shard,replica,coordinator,server,remote}.rs`, `engine/grpc/proto/shard.proto` (`RetentionLease`).

