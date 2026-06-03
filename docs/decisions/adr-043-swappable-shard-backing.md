# ADR-043: Swappable shard backing — the live-handoff routing-flip mechanism (clustering step 6a)

> [Back to the decisions index](../DECISIONS.md)


- **Status:** Accepted.
- **Context:** ADR-042's allocator commits the *desired* shard→node map but explicitly does **not** move
  data; §9 calls for **serve-then-drop + epoch fencing** on a live move, and §4.3 names the data-moving
  handoff as the next step. The coordinator routes by ring **position index** into `shards: Vec<Box<dyn
  Shard>>` and never reads the shard→node map on the hot path (`route`/`percolate_inner`), so in-process a
  reassignment is a no-op for matching — the handoff is meaningful only over gRPC, where a position's
  `RemoteShard` must be **re-pointed at a new owner** at runtime. That re-point needs a position's backing to
  be atomically swappable. This increment ships exactly that mechanism (the routing flip + the fence stamp);
  the cross-node move that *drives* a swap is ADR-044 (step 6b).
- **Decision (`src/cluster/handoff.rs`, `distributed`-gated):**
  - **A `HandoffShard` wrapper, mirroring `ReplicatedShard`.** A `Shard` whose backing is one boxed shard in
    an `ArcSwap<Box<dyn Shard>>` plus an `AtomicU64` generation. `swap_backing(new, gen)` re-points the slot
    atomically — **backing stored first, generation published with `Release` after**, so a reader/fencer that
    `Acquire`-observes the new generation also observes the new backing (no "demoted but still serving"
    window). **Serve-then-drop falls out of `arc_swap` for free:** an in-flight probe holds its loaded `Guard`
    (the old backing) and completes correctly; the old backing drops only when the last `Guard` releases — no
    read-path lock, safe under the coordinator's rayon probe fan-out. The same `ArcSwap`-for-lock-free-reads
    pattern `LocalShard`/`ShardServer` already use (ADR-016).
  - **`impl Shard for Arc<HandoffShard>` (not the bare type)** so the SAME `Arc` clones into both `shards[i]`
    (boxed) and the coordinator's typed `handoffs: Vec<Arc<HandoffShard>>` side-table; the `wrap_handoff`
    helper builds both views from one allocation, so they share one object **by construction** (a swap through
    the handle is instantly visible to reads through `shards[i]`). Step 6b reaches the typed handle to flip a
    position with **no downcast** and **no `Shard`-trait change** (every method forwards to the live backing,
    including the *defaulted* ones — omitting one would silently inherit the wrong default for a wrapped
    `ReplicatedShard`; a unit test guards this).
  - **Representation:** `ArcSwap<Box<dyn Shard>>`, not `Arc<dyn Shard>` — `arc_swap`'s `RefCnt` is implemented
    only for `Arc<T: Sized>` and `dyn Shard` is unsized, but a `Box<dyn Shard>` is a Sized fat pointer, so
    `Arc<Box<dyn Shard>>` qualifies; auto-deref still reaches `dyn Shard` for the forwards, so the extra hop is
    invisible.
  - **Gated + opt-in.** The whole module and the coordinator's `handoffs` field are behind `distributed`, so
    the lean core and the in-process/RF=1 **default path never compile it and stay byte-identical** (and there
    is no lean dead-code lane to satisfy). The gRPC builders (`connect_remote`/`connect_replicated`) wrap each
    position via `wrap_handoff`; `ClusterEngine::handoff_generations()` exposes the per-position fence stamps
    (read-only introspection).
  - **The generation** is the committed control-plane epoch (`ClusterState::epoch`) the backing was installed
    under. **Inert in 6a** (nothing compares it); it is the fence token ADR-044 reads to tell a demoted owner
    "you are fenced at generation N" before dropping it.
- **Scope — mechanism, not (yet) the move.** 6a ships the swappable backing + the fence stamp + serve-then-drop
  and proves them with unit tests. The cross-node move (`execute_handoff` = `peer_recover_replica` → a final
  catch-up under a brief write quiesce so the new owner ≡ the source at the flip instant → `swap_backing` →
  fence the old owner via a new `Fence` RPC → drop it) is **ADR-044 (step 6b)**, proven over the gRPC oracle.
  **Honest scope:** epoch fencing is load-bearing for the *multi-coordinator* future; with today's single
  coordinator the flip is serialized, so correctness rests on serve-then-drop + (6b's) quiesce, and the fence
  is defense-in-depth. The in-flight-probe-vs-fence race (a probe that loaded the old owner can hit its fence
  and surface `ShardError::Remote`) is ADR-044's to handle (swap → drain → fence → drop).
- **Consequence:** a position's backing can be re-pointed at runtime without a read-path lock and without
  touching the default path — the missing routing-flip half of the live handoff (the byte mover, peer
  recovery, already exists). Proven by six `handoff.rs` unit tests: a swap to a set-equal backing is
  byte-identical (ids + stats); an in-flight read serves the old backing while a fresh read sees the new one;
  the generation tracks swaps and is co-visible with the backing; concurrent readers survive repeated swaps;
  and both writes and the defaulted `set_event_sink` forward to the backing. Full `check.sh` green; no new
  dependency (reuses `arc-swap`, already lean-core).
- **See also:** ADR-042 (the shard→node map a reassignment acts on), **ADR-044** (the cross-node move that
  drives `swap_backing` — the next increment), ADR-035/036/039 (peer recovery — the byte mover a handoff
  re-points to), ADR-016 (the `ArcSwap` lock-free-snapshot pattern reused), ADR-027 (the position routing this
  flips), `src/cluster/{handoff,coordinator}.rs`, clustering-and-scaling.md §9/§4.3/§10.

