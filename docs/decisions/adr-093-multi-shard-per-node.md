# ADR-093: Multi-shard-per-node (a node hosts many shards)

**Status:** Accepted (2026-06-30) — design of record (direction decided); implementation is staged
(Stages 1–4 below). **Stage 1 (foundation) is built** (2026-06-30, branch `feat/multi-shard-stage1`):
proto `shard_id`, a shard-keyed `ShardServer` slot map, and per-shard fence/recovery/`shard_<id>/`
storage — fixing the codex P1, with the 1:1 deployment preserved. **Stage 2 (co-location) is built**
(2026-07-01, branch `feat/multi-shard-stage2`): the `AddShard` RPC + a per-endpoint adoption dedup in
`connect_remote` let several positions share one endpoint (fewer pods than shards) without re-shipping
the dict — oracle-proven K-positions-on-N&lt;K-servers ≡ single-node ≡ brute. Stages 3–4 remain staged.
ADR-092 (the unattended reconciler) is **parked** on the `feat/unattended-reconciler` branch — it is
*not* on `main`, so it is referenced here as plain text, not a link, until it lands (Stage 4).

## Context

The distributed deployment today is **one shard per process**: a `ShardServer` wraps exactly one
`Engine`, its `RecoverFrom` RPC **replaces** that engine's whole state, and its write-fence is a single
per-server `AtomicU64`. The coordinator's `connect_remote`/`connect_replicated` hardcode
`endpoints.len() == num_shards` — endpoint *i* is shard position *i*, 1:1.

This collides with the rest of the stack, which is **already multi-shard-per-node aware**:

- **The allocator** (`allocator::plan_assignments`, HRW) maps `position → NodeId` with **many positions
  per node** — it has no 1:1 assumption (its own test places 4000 positions on 4 nodes).
- **The control plane** (`ClusterState`/`ShardAssignment`/`NodeDescriptor`) stores `position → NodeId`
  and `NodeId → addr` separately — multiple positions can share a `NodeId`/endpoint.
- **`resolve_topology`** already resolves several positions to the same endpoint, with no complaint.
- **The durable coordinator layout** is already per-position: `shard_dir(base, position)` →
  `shard_<position>/segments/…`, and `ClusterManifest.segment_registry` is indexed by position.

So a code review (codex, on the parked reconciler branch) correctly flagged that any HRW-driven
data-moving rebalance — `rebalance_and_move` (ADR-090) and the unattended reconciler
(ADR-092, parked on `feat/unattended-reconciler`, not yet on `main`) — **silently overwrites data**: HRW packs several
positions onto one node, but a one-shard `ShardServer` can only hold one, and the second `RecoverFrom`
clobbers the first. And at RF=1 a node loss is unrecoverable (no replica), so the genuinely useful
unattended scenarios (failover, rebalance) have no safe home today.

**The fix is not to constrain those features — it is to make the deployment match the model the rest of
the stack already assumes.** This is the Elasticsearch model: a node hosts many shards, each
independently recoverable, relocatable, and fenced; rebalancing relocates one shard without touching the
node's others; failover promotes a replica.

## Decision

Make a `ShardServer` (node) host **many shards, keyed by a shard-id**, where **`shard_id` is the global
shard position** (0..K). A node hosting positions {2, 5, 7} runs one `ShardServer` serving shard-ids
{2, 5, 7}, with on-disk `shard_002/`, `shard_005/`, `shard_007/` subdirs — the *same* layout the
coordinator already uses. The change is concentrated entirely in the **gRPC transport + the
`ShardServer`**; the allocator, control plane, `resolve_topology`, and durable coordinator layout are
unchanged.

### What stays unchanged (confirmed by the seam survey)

- `allocator::plan_assignments`, `changed_assignments` — already position→node, multi-per-node.
- `control.rs` — `ClusterState`, `ShardAssignment`, `NodeDescriptor`; no 1:1 assumption.
- `coordinator/topology.rs` — `resolve_topology`/`route_topology` already map many positions → one
  endpoint.
- The coordinator's durable layout + `ClusterManifest` (per-position registry).
- The lean / in-process / `server`-default builds — everything here is `distributed`-gated.
- `trait Shard` — each impl is *implicitly* one shard; a `RemoteShard` carries its `shard_id` in `self`,
  so the trait methods are untouched.

### Per-node vs per-shard split (the core of the refactor)

| State | Scope | Today | Multi-shard |
|---|---|---|---|
| `norm`, `config`, `security`, `client_security`, `health_addr`, `data_dir` (root) | **per-node** | `ShardServer` fields | unchanged |
| adopted `dict` / `tag_dict` (frozen) | **per-node, shared** | inside the one `ServerState` | a node-scope `ArcSwapOption<(Arc<Dict>, Arc<TagDict>)>`; every slot references the same `Arc`s |
| the `Engine`/`LocalShard` | **per-shard** | `ServerState.shard` | one per slot |
| `fenced_at_generation` | **per-shard** | one `AtomicU64` on `ShardServer` (codex P1) | one per slot |
| retention-lease state | **per-shard** | inside `LocalShard` | unchanged (already per-shard) |
| durable segments / translog / sidecar | **per-shard** | at `data_dir` root | at `data_dir/shard_<id>/` |

`ShardServer.state: ArcSwapOption<ServerState>` becomes
`shards: <concurrent map><ShardId, Arc<ShardSlot>>`, where

```rust
struct ShardSlot {
    state: ArcSwapOption<ServerState>, // the Engine + the shared dict/tag-dict Arcs
    fenced_at_generation: AtomicU64,   // per-shard fence (fixes codex P1)
}
```

(Concurrency primitive TBD in Stage 1 — an `RwLock<HashMap<…>>` keeps the lean dependency philosophy;
the read path takes a slot `Arc` clone out and releases the lock, so it never holds across an RPC.)

### Proto changes (`grpc/proto/shard.proto`)

Add `uint32 shard_id` to every **per-shard** request — `Percolate`, `NumQueries`, `ClassCounts`,
`Ingest`, `Insert`, `Delete`, `Flush`, `FetchSegments`, `RecoverFrom`, `FetchTranslog`,
`RetentionLease`, `Fence`, `Unfence`. proto3 defaults the field to 0, so the wire stays
forward/backward-decodable. `AdoptDict` gains a `shard_id` to name the slot it creates;
`DictFingerprint`/`AdoptDict` otherwise stay node-level (the dict/tag-dict fingerprints are a *node-wide*
content invariant, **not** per-shard — they are not duplicated per slot).

The generated `grpc/` crate rebuilds from the `.proto` via pure-Rust `protox` (nothing checked in), so
this is a recompile, not a vendored-code edit.

### `RemoteShard` changes (`cluster/remote.rs`)

Add a `shard_id: u32` field; the constructors (`connect`/`connect_with_security`/`connect_and_adopt`
/…`_with_security`) take it; every request build sets `shard_id: self.shard_id`. The instrumented `call`
seam (ADR-085) is **unchanged** — `shard_id` flows via `self`, not the call. The `dict_fp`/`tag_dict_fp`
fields stay (node-wide, carried on the recovery RPCs for content verification).

### `ShardServer` changes (`cluster/server.rs` + `server/service/*`)

- Hold the shard map + a node-scope adopted-dict cell. Each RPC handler resolves `req.shard_id` → its
  slot (`not_found` if absent), then operates on that slot's `ServerState`/fence — the *only* change to
  most handlers is one map lookup replacing `self.loaded()?`.
- **`fence`/`unfence`** act on `slot.fenced_at_generation` (per-shard) — **this is codex's P1 fix**: two
  handoffs fencing different shards on one node no longer race a shared atomic.
- **`recover_from(shard_id)`** recovers into `data_dir/shard_<id>/segments` and stores **only that
  slot's** `state` — a recovery never clobbers the node's other shards.
- **`adopt_dict(shard_id)`** adopts the dict at node scope once (idempotent on fingerprint), then creates
  the named slot referencing the shared `Arc<Dict>`/`Arc<TagDict>` (so the dict is deserialized once per
  node, not once per shard).
- **`open_durable`** scans `data_dir/shard_*/` and restores every slot the node previously held.

### Coordinator / builder changes (`cluster/coordinator/distributed.rs`)

- `connect_remote`/`connect_replicated` change from `endpoints[i] = position i` to a `position →
  (endpoint, shard_id)` mapping (multiple positions may share an endpoint). The natural source is
  `resolve_topology` (already returns `position → endpoint`) plus `shard_id = position`. A node's first
  connect ships+adopts the dict (`AdoptDict(shard_id)`); subsequent slots on the same node reuse the
  node's adopted dict (a lightweight `AddShard`/repeat-`AdoptDict` — Stage 2).
- `execute_handoff`/`peer_recover_replica` thread the `shard_id` into the source/target `RemoteShard`
  calls (fence/recover/lease the *right* slot). The `HandoffShard` wrapper is orthogonal and unchanged.

## Staged implementation (each stage ships green; lean/default builds untouched throughout)

1. **Foundation. ✅ BUILT (2026-06-30).** Proto `shard_id`; `RemoteShard(endpoint, shard_id)`;
   `ShardServer` shard-keyed map with **per-shard fence/recovery/storage** + node-scope dict;
   `AdoptDict(shard_id)` slot creation; builders send `shard_id = position`. **The 1:1 deployment is
   preserved** — each node still hosts one slot (its position). This alone **fixes codex P1's root
   cause** (per-shard fence) and is the load-bearing transport PR. gRPC oracle: the existing K-servers
   tests now address shard-ids; added a `per_shard_fence_isolation` unit. One in-flight adjustment vs
   the plan: `peer_recover_replica`/`catch_up_recovered_replica` took a `shard_id` **parameter** (not a
   hardcoded 0) — the replication oracle recovers position 1, so the caller must name the slot.
2. **Co-location. ✅ BUILT (2026-07-01).** Builders let several positions share one endpoint; a node
   hosts many slots; the new `AddShard` RPC creates a co-located slot over the node's already-adopted
   dict (no dict re-ship / re-deserialize). Compose/Helm can run fewer pods than shards (expressed by
   repeating an endpoint — no CLI change). gRPC oracle (`colocation.rs`): K=4 positions on N=2 servers ≡
   single-node ≡ brute, both broad on/off, + `shard_query_counts` proving all K co-located slots are
   independently populated; four `add_shard` server unit tests (no-adopt / after-adopt / wrong-fp /
   idempotent). **Key finding:** RF=1 co-location already worked via repeated `AdoptDict` (the
   `endpoints.len() == num_shards` check means "one entry per position", repeats allowed — it stays), so
   the delta was purely the `AddShard` optimization + per-endpoint dedup + the oracle, not a rewrite.
   `connect_replicated` (RF&gt;1) co-location + its replica-promotion oracle are deferred to Stage 3;
   per-node `/_metrics` aggregation over co-located slots and a Compose/Helm worked example are small
   follow-ons.
3. **Per-shard relocation + failover.** `execute_handoff` moves one slot between nodes without touching
   the node's other shards ⇒ `rebalance_and_move`/HRW become **safe** (the collision codex flagged is
   gone — every move targets a distinct slot); RF&gt;1 replica promotion across multi-shard nodes. gRPC
   oracle: relocate one of several co-located shards, assert the others are untouched + zero-FN.
4. **Rebase the reconciler (ADR-092, the parked branch) + the autoscaler** onto the
   now-safe foundation — the parked branch returns, correct, with the route-by-assignments gate (its P2)
   and no collision hazard (its P1, now structurally impossible).

## Backward-compat / migration

- **Wire:** the new `shard_id` fields default to 0 (proto3), so a mixed old/new mesh decodes; but the
  distributed layer is **experimental / localhost-proven** (STATUS), so the supported story is "deploy a
  matched coordinator + shardserver version," not rolling a mixed mesh.
- **On-disk:** the `ShardServer` data layout moves from `data_dir/segments/…` to
  `data_dir/shard_<id>/segments/…`. Because the distributed shard store holds no production data yet
  (experimental), Stage 1 adopts the subdir layout directly (no in-place migration). The single-node
  `Engine` on-disk format and the in-process cluster's durable layout are **untouched**.
- **No control-plane / manifest format change** — the coordinator already speaks per-position.

## Testing strategy

- Extend `tests/cluster_grpc_oracle` (feature `distributed`): per-shard fence isolation (Stage 1);
  multiple slots per server ≡ single-node ≡ brute (Stage 2); relocate one co-located shard leaves the
  node's others intact + zero-FN, under a concurrent writer + across a resolve-only restart (Stage 3).
- The existing single-node oracle (`tests/oracle.rs`) and the in-process cluster oracles are unaffected
  (no front-end / placement change).
- `check.sh` stays the gate; every stage is green before merge.

## Consequences

- **Enables safe rebalancing + failover**, unblocking ADR-090's `rebalance_and_move`, the ADR-092
  reconciler, RF&gt;1 in Helm, and the autoscaler's data-moving path — all of which are unsafe today
  *because* of the one-shard limit.
- **Fewer nodes than shards** becomes a supported topology (cost: run K positions on N pods).
- **Concentrated blast radius:** the allocator/control-plane/durable-coordinator layers — the parts that
  are hardest to change safely — need no change; the work is the transport + `ShardServer`, which is
  `distributed`-gated and oracle-fenced.
- **Cost:** a multi-PR program (Stages 1–4) and a per-shard concurrency primitive in `ShardServer`. The
  dict stays shared per node (no per-shard memory blowup). Per-node metrics/health become *aggregate over
  the node's shards* (a follow-on to ADR-091's per-node `/_metrics`).

## Risks & open questions

- **Concurrency primitive for the shard map.** Prefer a std `RwLock<HashMap<ShardId, Arc<ShardSlot>>>`
  (lean-dependency philosophy) over a new `dashmap` dependency; the read path clones the slot `Arc` and
  drops the lock immediately, so the RPC never holds it. To confirm in Stage 1.
- **Slot creation lifecycle.** `AdoptDict(shard_id)` (Stage 1) vs a dedicated `AddShard`/`RemoveShard`
  (Stage 2/3) — and whether removal also GCs the on-disk `shard_<id>/`.
- **Per-node observability.** ADR-091's `/_metrics` becomes per-node-aggregate-over-shards (+ optional
  `{shard="…"}` labels) — a small follow-on, not a blocker.
- **deploy topology.** Compose/Helm gain a "positions-per-pod" notion (Stage 2); v1 can keep 1:1 and
  still benefit from the per-shard fence fix.

## Alternatives considered

- **Constrain the reconciler/rebalance to empty-destination moves** (the collision guard). Rejected as
  the *primary* path: it makes the unattended controller refuse realistic HRW reshuffles (it would be a
  near-no-op), and leaves the underlying model mismatch in place. (A fail-loud guard is still worth
  adding to ADR-090's `rebalance_and_move` as defense-in-depth until Stage 3 lands — see the parked
  reconciler review.)
- **Keep one-shard-per-node, lean on k8s StatefulSet identity + RF&gt;1 + resize** for all operations.
  Viable for node *replacement* (StatefulSets already handle it), but it forecloses cross-node
  rebalancing and shrinking node count — capabilities the allocator already models and a commercial
  product is expected to have. The user chose to build the capability rather than design around its
  absence.
