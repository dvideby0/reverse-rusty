# ADR-080: Replicate-broad-to-all + the cluster class-D always-candidate lane

**Status:** Accepted (2026-06-20)

**Context.** ADR-065 criterion 8 — *"either replicate [broad queries] to all nodes or record the ADR
for why the RF-replicated shard-0 lane suffices at v1."* Today the broad lane — cost class **C**
(hot-only anchor) and class **B-arity-2** (all-hot required pair) — lives on **shard 0 only**, and
every title unconditionally probes shard 0 (`route()` does `targets.push(0)`), evaluating broad there
(`include_broad && s == 0`). ADR-027 named this *"the in-process stand-in for the design's 'replicate
the broad lane to every node' (§7)."* Two costs come due on this criterion: (1) **shard 0 is a broad
hotspot** — every title hits it regardless of content; and (2) **class D is rejected cluster-wide** —
ADR-068 built the single-node always-candidate lane (negation-only queries stored under
`util::universal_sig()`, gated by `EngineConfig.accept_class_d`, probed once per segment inside
`include_broad`) but parked cluster support here, because `placement_of` returns `Target::Reject` for
class D. The reference workload contains negation-only "base"/"raw" entities, so the cluster reject is a
real drop-in divergence.

**Decision: R-shard.** Replicate the broad lane (class C + B-arity-2 + opt-in class D) to **every
shard**, and evaluate it on **exactly one** shard per title — that title's *broad-eval shard*, picked
by a stable hash so the load spreads and no shard is a broad hotspot. This graduates ADR-027's shard-0
stand-in and unblocks class D, reusing the entire existing shard/replication/durability/gRPC stack.

- **Placement.** `Target::Replicated` now means **every shard** (was shard 0). `placement_of` gains an
  `accept_class_d` parameter: class D maps to `Replicated` (the broad lane, under the universal
  signature) when the knob is on, else stays `Reject`. Class C and B-arity-2 already mapped to
  `Replicated`. The decision is re-derived identically on log replay (same frozen dict + same config),
  so live ≡ replay.
- **Write fan-out.** Every site that sent a `Replicated` query to shard 0 — `bucket_and_ingest`, build
  pass B, `rebuild_from_live` (resize / `set_vocab`), `apply_add`, `apply_upsert` — now fans it to all
  shards (and every replica copy). `apply_add`'s broad arm reuses the same `applied`/`failed`/
  `note_partial` collection as the selective arm (shared `insert_on_shards` helper), so a mid-fan-out
  remote failure is queued for ADR-047 repair, not a silent partial. `apply_remove` already fanned to
  all shards; `replay_apply` funnels through the same `apply_*`, so live ≡ replay.
- **Routing — the broad-eval shard.** `route()` drops the unconditional `targets.push(0)`. It computes
  the selective targets (`ring.lookup(f)` for each non-hot title feature) and picks one broad-eval
  shard: `targets[ fnv1a64(title) % targets.len() ]` when the title has selective targets (free-rides
  an already-probed shard — **zero extra fan-out**), else `fnv1a64(title) % num_shards` (a single
  hashed probe — the title has no selective anchor, all-hot or empty). The percolate gate becomes
  `include_broad && s == broad_eval_shard` (in both `percolate_inner` and `percolate_filtered_ranked`).
  Because broad evaluates on exactly one shard, a broad query is counted once; the existing
  union+dedup-by-logical-id merge is the backstop. The hash spreads the broad-eval shard across titles
  — the shard-0 always-probe hotspot is gone.
- **Durability — the class-D rollback fence.** Cluster shards are segments-only durable (ADR-032, no
  per-shard manifest), so the single-node manifest-v4 fence (ADR-068) has no per-shard home; it lives
  at the **`ClusterManifest`**. A commit registering any class-D-bearing shard writes manifest **v5**
  (layout-identical to v4; the version word *is* the fence — set from `any(shard.class_counts()[3] >
  0)`); a pre-ADR-080 binary accepts only v2..=4 and fails `ClusterEngine::open` outright on v5. A
  class-D-free commit keeps writing v4 byte-identically.
- **The clog needs no new op markers.** Unlike the single-node WAL (which logs *before* classifying, so
  it needed `InsertClassD`/`UpsertClassD` markers to replay legacy frames under the old gate), the
  coordinator clog logs raw DSL and re-derives placement deterministically on replay, reproducing the
  accept decision from config. A class-D entry already sealed into a shard's segment base (captured at
  `snapshot_pos`) is attached on `open` and stays matchable regardless of a later knob flip — the knob
  gates *acceptance*, never *visibility*; only the un-checkpointed clog tail is re-placed under the
  reopen-time knob (knob-off ⇒ drop, never resurrect — the safe direction). The server supplies
  `accept_class_d` consistently via its CLI flag across build and reopen.
- **gRPC.** The in-process path is wire-transparent (the protocol carries DSL + `include_broad` + tags,
  never the placement decision), so it lands and is oracle-proven first. The coordinator's
  `accept_class_d` drives placement; each remote shard server runs its *own* engine's gate, so the
  **operator contract** is that every shard server runs with the same `--accept-class-d` (else a
  class-D add fanned to a knob-off shard is silently dropped). Coordinator-mode startup warns when the
  flag is set in remote mode; a connect-time accept echo is the documented follow-on.

**Rejected: R-coord (broad on the coordinator, evaluated locally).** §7's literal *"every matcher node…
locally"* predates this codebase's split of "matcher node" into a routing-only coordinator + stateful
shards. R-coord's one real benefit — no network hop for broad eval — is unrealizable until a hardened
multi-machine, multi-coordinator deployment exists (out of scope for v1), and it would stand up a
*second, separately-durable, separately-replicated* matcher on the coordinator (which holds no segments
today), disjoint from the shard delete fan-out and absent from the peer-recovery/translog stack — the
worst way to widen the zero-false-negative surface. R-shard inherits the existing HA stack wholesale and
delivers both wins (hotspot removed, class-D unblocked). R-coord is the deferred multi-coordinator
refinement.

**Why this is safe (the correctness contract).** For any title `T` that could satisfy a broad-lane
query `Q` (class C, B-arity-2, or accepted D): (1) every shard holds the **complete** broad lane (write
fan-out sends every replicate-all query + the universal-sig class-D entry to every shard); (2)
`broad_eval_shard ∈ probed targets` by construction, so `T` always probes a shard that runs broad; (3)
that shard's `match_into(include_broad=true)` runs the full broad block — arity-1 broad anchors over
`P(T)` **and** the title-independent universal-sig probe — so an accepted class-D entry fires for `T`
regardless of `T`'s features; (4) exact verify enforces forbidden features; (5) broad evaluates on
exactly one shard and the union is deduped, so no double-count and no shard boundary can drop a match. A
dropped/unreachable broad-eval shard probe errors loud (propagates), never silently shrinks the union.
At K=1 the broad-eval shard is always 0 ⇒ byte-identical to single-node. The class A / B-any-of cover
argument is unchanged (anchor is a required/any-of feature, routes to `ring.lookup(anchor)`). Default
behavior is result-identical for every existing corpus (the broad match *set* is unchanged — only its
physical placement and the evaluating shard move); the new behavior is opt-in class D.

**Proven.** `tests/cluster_oracle/class_d.rs` — the lane-on differential `cluster ≡ single-node(lane on)
≡ brute(accepting class D)` across K∈{1,3,8,16} × broad on/off; lane-off pins `RejectedClassD` +
`class_counts()[3]==0`; broad-off quarantines class D; and the replicate-to-all distinction encoded as a
test (storage fan-out = N — class C/D summed counts are multiples of K — while read fan-out stays
bounded and at least one title's fan-out omits shard 0, proving the hotspot is gone).
`tests/cluster_durability_oracle/class_d.rs` — a durable class-D cluster reopens ≡ pre-crash ≡ brute
across K∈{1,3,8} (segment base + clog tail), the build commit writes the v5 fence, and a knob-off reopen
keeps sealed always-candidates matchable while rejecting a new class-D add. `storage::manifest` unit —
the v5 fence round-trips and an unknown future version fails loud. The full existing cluster +
durability oracles stay green (replicate-to-all is result-identical for the broad set).

**See also:** ADR-027 (the multi-shard core + the shard-0 stand-in this graduates), ADR-068 (the
single-node class-D lane + the manifest-v4 fence this mirrors at the cluster manifest), ADR-026 (the
broad lane), ADR-032 (segments-only durability — why the fence lives at the cluster manifest), ADR-047
(the partial-apply repair the broad fan-out reuses), ADR-065 criterion 8 (the requirement),
[`clustering-and-scaling.md`](../design/clustering-and-scaling.md) §7. Deferred: R-coord (coordinator-local
broad for multi-coordinator deployments); a connect-time `accept_class_d` handshake echo for remote
shard servers.
