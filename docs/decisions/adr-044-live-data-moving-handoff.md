# ADR-044: Live data-moving handoff — the cross-node move that drives the swap (clustering step 6b)

> [Back to the decisions index](../DECISIONS.md)


- **Status:** Accepted.
- **Context:** ADR-043 made a shard position's backing **runtime-swappable** (the routing flip + a generation
  fence stamp), but nothing *drove* a swap. The allocator (ADR-042) **decides** the shard→node map; peer
  recovery (ADR-036/039) **moves** the bytes; ADR-043 **flips** routing — this increment wires
  decide→move→flip into one **live** move: a position's owner changes while the cluster keeps serving reads
  and (almost all) writes. This is §9's serve-then-drop + epoch-fencing step.
- **Decision (`ClusterEngine::execute_handoff` + a new `Fence` RPC):**
  - **A write fence on the old owner.** A new `Fence(generation)` RPC sets a monotonic
    `fenced_at_generation` on the `ShardServer`; once fenced, the data-mutating writes
    (`insert`/`delete`/`ingest`) return `failed_precondition`, while **reads + the recovery RPCs**
    (`FetchSegments`/`FetchTranslog`/`RetentionLease`) stay served. **Write-only by design**, so an in-flight
    READ never hits the fence — which dissolves the ADR-043 in-flight-probe caveat (a demoted owner keeps
    serving reads until the coordinator stops routing to it = serve-then-drop). Dict-fingerprint-guarded and
    monotonic (a stale, lower-generation fence never un-fences). `RemoteShard::fence` is inherent (not a
    `Shard` method) — only the handoff orchestrator fences a specific old owner, by endpoint.
  - **`execute_handoff(position, source_ep, target_ep)` under one retention lease** held on the source for
    the whole move (ADR-040 — so the segment-copy seal, or any concurrent seal, can't strand the tail): (1)
    **no-quiesce bulk recover** the target from the source (segments at `P` + drain the translog tail — the
    source keeps serving + accepting writes); (2) **fence** the source (the position's brief write-quiesce
    begins); (3) **drain to CONVERGENCE** — loop `catch_up` until the source's high-water stops advancing;
    (4) **flip** the `HandoffShard` backing source→target (serve-then-drop) and release the lease.
  - **Why fence-late, not fence-first.** Fencing before the copy would write-quiesce the source for the
    *whole* segment copy — exactly the ADR-036 whole-copy quiesce that 5c/5d eliminated. Fencing *after* the
    no-quiesce bulk copy keeps that property; only the brief converge-then-flip is write-quiesced.
  - **Why convergence, not a single final catch-up.** A write that passed the source's fence check *just
    before* the fence took effect can still append *after* a single catch-up reads the tail (a TOCTOU). But a
    fenced source accepts no new writes, so its tail is finite and frozen: looping `catch_up` until the
    high-water stops advancing captures every op it ever accepted. Convergence — not a fixed pass count — is
    what makes the flip lossless; a generous cap guards only a misbehaving (still-accepting) source.
- **Honest scope.** Single-coordinator: the flip is serialized, so the fence is the *cross-node / future
  multi-coordinator* guard (defense-in-depth today). A write rejected in the fence→flip window is **fail-closed**
  (rejected + retryable — the caller retries onto the new owner; it never silently vanishes). On non-convergence
  (a misbehaving source) `execute_handoff` **aborts the flip fail-closed** and leaves the source fenced — a
  *stuck position* needing operator attention, never a lost write (auto-unfence-on-abort is a refinement).
  "Drop the old owner" = drop it from **routing**, not teardown — its server keeps running (tearing it down is a
  separate ops step). RF > 1 *group* relocation (moving a whole primary+replica set) reuses the same swap (the
  backing can be a `ReplicatedShard`), but the oracle covers the single-owner move; the **autoscaler** that
  *triggers* a handoff on a membership/rebalance event is step 6c (design-only).
- **Consequence:** a shard can be moved between owners **live**, under concurrent writes, with **zero false
  negatives** and **uninterrupted reads** — the missing decide→move→flip wiring (the allocator decides, peer
  recovery moves, the 6a `HandoffShard` flips, the fence guards). Proven by
  `tests/cluster_grpc_oracle.rs::grpc_live_handoff_under_sustained_writes` (reassign a position source→target
  under a concurrent writer that retries the brief fence-window rejections; the SAME cluster — its position
  re-pointed to the new owner — ≡ the brute oracle over the final live set; the handoff generation bumps; every
  add converges onto the new owner) + `src/cluster/server.rs::fence_rejects_writes_but_serves_reads` (writes
  rejected, reads served, monotonic, fingerprint-guarded). Full `check.sh` green; no new dependency (the `Fence`
  RPC reuses tonic; no `proto.rs` mapper needed for its scalar messages).
- **See also:** ADR-043 (the swappable `HandoffShard` backing this drives), ADR-042 (the shard→node map a
  reassignment acts on), ADR-036/039/040 (peer recovery + the per-shard translog + retention leases — the byte
  mover, the no-quiesce tail, and the lease this holds), ADR-033 (shared-nothing — the move is peer recovery,
  no shared storage), `src/cluster/{coordinator,server,remote,handoff}.rs`, `grpc/proto/shard.proto`,
  `tests/cluster_grpc_oracle.rs`.

