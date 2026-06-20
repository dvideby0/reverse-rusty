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
- **Durability — the v5 replicate-to-all layout fence (two-way).** Cluster shards are segments-only
  durable (ADR-032, no per-shard manifest), so the layout marker lives at the **`ClusterManifest`**:
  every ADR-080 durable cluster writes manifest **v5** (layout-identical to v4 — the version word *is*
  the marker, `broad_replicate_all`). It fences both directions, each load-bearing for zero false
  negatives: (1) **rollback** — a pre-ADR-080 binary accepts only v2..=4 and fails `open` outright on
  v5, so it never writes broad onto shard 0 only (which the rotating routing would mis-read) nor
  silently drops class-D (it has no universal-signature probe); (2) **forward** — this binary refuses to
  `open` a v<5 cluster, whose broad lives on shard 0 only and would be mis-routed by the rotating
  broad-eval shard (a silent FN on the upgrade path — a codex-review catch). A pre-ADR-080 durable
  cluster must be rebuilt with this binary.
- **The clog needs no new op markers.** Unlike the single-node WAL (which logs *before* classifying, so
  it needed `InsertClassD`/`UpsertClassD` markers to replay legacy frames under the old gate), the
  coordinator clog logs raw DSL and re-derives placement deterministically on replay, reproducing the
  accept decision from config. A class-D entry already sealed into a shard's segment base (captured at
  `snapshot_pos`) is attached on `open` and stays matchable regardless of a later knob flip — the knob
  gates *acceptance*, never *visibility*; only the un-checkpointed clog tail is re-placed under the
  reopen-time knob (knob-off ⇒ drop, never resurrect — the safe direction). The server supplies
  `accept_class_d` consistently via its CLI flag across build and reopen.
- **Lifecycle correctness (codex review).** Two front-door edge cases the cluster must mirror from the
  single node: (a) an **effectively-empty** class-D query (no positives AND no forbidden) is rejected at
  placement *before* fan-out even with the lane on (`accept_class_d && !ex.forbidden.is_empty()`) — so
  an `upsert` to one cannot tombstone the prior version in pass 1 and then store nothing in pass 2 (a
  failed replace never deletes, ADR-067 parity); (b) a **rebuild** (`resize` / `set_vocab`) re-ingests
  ALREADY-STORED queries, so its fresh shards force `accept_class_d` **on** — a cluster reopened with the
  knob off keeps its sealed always-candidates through the next rebuild instead of silently dropping them
  (the cluster analogue of ADR-068 mech 2; the coordinator still gates *new* class-D adds via the
  unchanged `self.per_shard.accept_class_d`, which runs before any shard sees the query).
- **gRPC.** The in-process path is wire-transparent (the protocol carries DSL + `include_broad` + tags,
  never the placement decision), so it lands and is oracle-proven first. The coordinator's
  `accept_class_d` drives placement; each remote shard server runs its *own* engine's gate, so the
  **operator contract** is that every shard server runs with the same `--accept-class-d` (else a
  class-D add fanned to a knob-off shard is silently dropped). Coordinator-mode startup warns when the
  flag is set in remote mode. The in-process **forward layout fence has no remote analogue yet**: a
  `distributed` coordinator reconnecting to *populated pre-ADR-080* shard servers (broad on shard 0
  only) would mis-route under the rotating broad-eval shard, with no layout handshake to catch it (a
  codex-review catch). A connect-time handshake carrying both the `accept_class_d` decision and the
  broad-layout version — so the coordinator refuses a legacy remote shard rather than mis-routing it —
  is the documented follow-on; until then a cross-version *remote* upgrade is unsupported (rebuild the
  cluster). The in-process v1 core (the production-relevant path) is fully fenced.

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
across K∈{1,3,8} (segment base + clog tail), the build commit writes the v5 marker, a knob-off reopen
keeps sealed always-candidates matchable while rejecting a new class-D add, plus the three lifecycle
guards (codex review): the **forward fence** refuses a downgraded (v4) manifest loudly, a **knob-off
resize** preserves sealed class-D ≡ brute, and (in the oracle) an **empty class-D** rejects + a failed
upsert never deletes the prior version. `storage::manifest` unit — the v5 replicate-to-all marker
round-trips and an unknown future version fails loud. The full existing cluster + durability oracles stay
green (replicate-to-all is result-identical for the broad set).

**See also:** ADR-027 (the multi-shard core + the shard-0 stand-in this graduates), ADR-068 (the
single-node class-D lane + the manifest-v4 fence this mirrors at the cluster manifest), ADR-026 (the
broad lane), ADR-032 (segments-only durability — why the fence lives at the cluster manifest), ADR-047
(the partial-apply repair the broad fan-out reuses), ADR-065 criterion 8 (the requirement),
[`clustering-and-scaling.md`](../design/clustering-and-scaling.md) §7. Deferred (all in the experimental `distributed` path): R-coord (coordinator-local broad for
multi-coordinator deployments); a connect-time handshake carrying the `accept_class_d` decision **and**
the broad-layout version (the remote analogue of the in-process forward fence — refuse a populated
pre-ADR-080 shard server instead of mis-routing it); and class-D under remote partial-apply `resync`
(ADR-047), where re-driving a queued class-D mutation must likewise force accept.
