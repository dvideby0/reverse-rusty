# ADR-039: Durable + replicated per-shard query log (the translog) + no-quiesce peer recovery (clustering step 5c)

> [Back to the decisions index](../DECISIONS.md)


- **Status:** Accepted.
- **Context:** ADR-036's gRPC peer recovery copies a *point-in-time snapshot* of a shard's `.seg` files, so
  writes to the position had to be **quiesced** for the whole copy window (documented in `server.rs`,
  `coordinator.rs`, `replica.rs`). The reason was structural: a remote/gRPC cluster uses `NullClusterLog` (the
  ADR-031 coordinator log is the *in-process* story), so there was no durable tail to replay the writes that
  land during the copy. This is the Elasticsearch **translog** gap — the last data-plane hole before a real
  multi-node deployment. A correction carried from ADR-037/038: the control plane does **not** close this gap
  (it holds the cluster-state *doc*, never query mutations); closing it needs the per-shard *query* log.
- **Decision (`src/cluster/translog.rs` + `shard.rs` + `replica.rs` + the gRPC surface):**
  - **Reuse, don't reinvent.** The translog reuses ADR-031's proven log machinery verbatim: the
    `ClusterMutation { Add{logical,version,dsl}, Remove{logical} }` op (logical-id + raw DSL — the ADR-029
    DSL-on-wire invariant, re-compilable against the frozen dict → byte-identical placement), the opaque
    `LogPos`, and the CRC-framed `FileClusterLog` / in-memory `NullClusterLog` backends (torn-tail forward-scan
    recovery, atomic tmp+rename checkpoint, `fsync` knob). `translog.rs` is the thin per-shard wiring. **Not**
    the engine WAL: its tombstone is a per-shard *physical* `(seg_idx, local_id)`, un-replayable on a peer whose
    local ids differ (replicas are set-equal, not byte-identical) — the same reason ADR-031 declined it.
  - **Owned by the durable `LocalShard`.** Each durable shard (an in-process replica *or* a gRPC data node)
    keeps its own dense, monotonic translog rooted in its data dir; in-memory shards keep a `NullClusterLog` →
    byte-identical to pre-ADR-039. Writes are **log-first / fail-closed**: `insert_extracted` /
    `delete_by_logical_id` append the mutation under the engine lock (so log order == apply order) BEFORE
    applying, rejecting the write on an append failure (the per-shard analogue of `add_query`). Bulk
    `ingest_extracted` goes straight to a durable base segment (no translog). **Replication rides the existing
    primary→replica fan-out** — each in-sync replica appends to its own translog (the ES model; no new transport).
  - **The position boundary is the zero-false-negative lynchpin.** `seal_for_checkpoint` (flush memtable →
    reseal base tombstones) captures `P = last_pos` under the write lock and trims the translog to `P`, so the
    segments hold exactly ops ≤ `P` and the tail exactly ops > `P`. Recovery streams segments (≤ `P`) then
    replays the tail (> `P`): no overlap, no double-apply — the property `ClusterEngine::open` already relies on,
    pushed to the shard. (`add_compiled` is append-only, so correctness rests on the position bound, never on add
    idempotency.)
  - **No-quiesce recovery, both paths.** In-process `peer_recover` seals the primary at `P`, copies its
    segments, attaches, then replays the primary's translog tail (> `P`) into the new replica — the writes that
    landed during the copy, recovered rather than lost; a re-runnable `catch_up_replica` drains any further tail.
    Over gRPC: `FetchManifest.up_to_seqno` carries `P`, a new server-streaming `FetchTranslog(after_seqno)` RPC
    serves the un-sealed tail (read-only — no seal, so the source keeps accepting writes), and the coordinator's
    `peer_recover_replica` recovers segments then replays the tail through the SAME apply funnel (re-derived from
    DSL). The documented quiesce notes are deleted. The wire `TranslogEntry { seqno, oneof{ AddItem add; uint64
    remove_logical } }` reuses `AddItem` (typed, not opaque — keeps the wire DSL-bearing + oracle-assertable);
    all additive, zero `build.rs` change.
  - **Data-node self-restart (§6).** A durable shard records a per-shard checkpoint **sidecar** (`shard.ckpt`:
    `next_seg_id` + `local_checkpoint P` + segment list + dict fingerprint, CRC + atomic tmp+rename) at each
    seal — AFTER the segments are durable, BEFORE the translog is trimmed, so a crash in between just replays an
    already-captured, position-filtered prefix. `new_durable` finds the sidecar on restart and attaches the
    committed segments + replays the translog tail (engine-only, since the ops are already in the log), so a
    `shardserver --data-dir` survives its own crash with no coordinator manifest (the remote coordinator is
    non-durable). The sidecar's dict-fingerprint guard refuses attaching segments built for a divergent space.
- **Honest scope.** Recovery is deterministic-by-ordering in the oracles (snapshot → write → tail catch-up),
  which exercises the exact path concurrent writes take during the copy; under *sustained* writes, full
  convergence still needs a brief finalize (the quiesce window shrinks from the whole copy to the residual
  delta — `catch_up_replica` is the loop). Translog **retention/GC** for a slow recovering replica (keep the
  tail back to the slowest follower) is a policy not yet set — 5c seals the source fresh, so the copy's `P` is
  current. Deferred (unchanged from prior steps): TLS/auth (plaintext localhost); bounded-memory streaming of a
  very large tail; an allocator acting on the shard→node map; add-as-upsert / version-LWW (replay preserves op
  order). For the in-process *durable cluster*, the coordinator `ClusterLog` remains the authoritative
  crash-rebuild source (`open` resets the per-shard translog) — the per-shard translog is the recovery tail +
  the data-node durability; unifying the two logs is a future cleanup.
- **Consequence:** A coordinator can bring a fresh node up from a live peer WITHOUT quiescing the source's
  writes, and a durable data node self-recovers after its own crash. The default in-memory / RF=1 / in-process
  paths are byte-identical, so every prior oracle is unchanged and green — the acceptance signal. Proven by:
  `tests/cluster_grpc_oracle.rs::grpc_peer_recovery_without_quiescing` (a fresh node recovers segments at `P`,
  writes land after `P`, the translog tail catches them up, recovered ≡ live source ≡ brute oracle over the
  final live set — zero false negatives across the wire); `replica.rs::peer_recover_replays_tail_without_quiescing`
  (the in-process analogue) and `::durable_shard_self_restarts_from_translog` (§6); plus `translog.rs` unit tests
  (fresh/reset, torn-tail via the reused `clog` backend, the sidecar round-trip). Full `check.sh` green (fmt +
  clippy ×3 incl. lean-core + tests ×2 incl. distributed + audit + deny). `translog.rs` is std-only (lean core);
  the gRPC pieces are `distributed`-gated.
- **See also:** ADR-031 (the `ClusterLog` seam + CRC framing + one-`apply`-funnel this reuses and re-homes per
  shard), ADR-036 (the gRPC peer recovery whose quiesce gap this closes), ADR-035 (the in-process `ReplicatedShard`
  + `peer_recover` this extends), ADR-032 (the per-shard durable segments the translog tails), ADR-029/034 (the
  transport + DSL-on-wire + dict shipping the `FetchTranslog` wire reuses), ADR-037/038 (the control plane — the
  cluster-state doc, explicitly NOT the query mutations this log carries), ADR-033 (shared-nothing — local
  segments + per-node durable log, no object store), `src/cluster/{translog,shard,replica,server,remote,coordinator}.rs`,
  `engine/grpc/proto/shard.proto` (`FetchTranslog`/`TranslogEntry`), `tests/cluster_grpc_oracle.rs`.

