# Status — what's built and what's next

Current state of the Rust engine (`engine/`) and the prioritized roadmap — **the canonical home for
what's implemented vs design-only**. Component detail lives in the [design docs](design/README.md) and
the [ADRs](DECISIONS.md); the per-file index is the module map in [`../CLAUDE.md`](../CLAUDE.md), and
dependency versions in [`../engine/Cargo.toml`](../engine/Cargo.toml). The full suite passes —
differential oracle, unit, server, coverage-gap, error-path, hardening, and persistence suites (the
last now covering durability-failure events + recovery-event buffering), plus doc-tests and the
pressure/soak suite (`tests/stress.rs` — now committed and run by `cargo test`; its 10M soak is
`#[ignore]`d). Run `cargo test --release` for the current count. GitHub Actions runs the full
`check.sh` gate + benchmarks on every PR — see [`testing.md`](testing.md) and [`DECISIONS.md`](DECISIONS.md) ADR-024.

---

## Implemented (working, tested)

- **Core pipeline** — DSL parser (`dsl.rs`), shared query/title normalizer with daachorse +
  `NormalizerBuilder` (`normalize.rs`), feature dictionary + 64-hot common mask (`dict.rs`),
  signature-cover optimizer + cost classes A/B/C/D (`compile.rs`), adaptive candidate index
  (inline → Vec → roaring, `index.rs`), integer-only SoA exact matcher (`exact.rs`).
- **LSM engine** (`segment.rs`) — immutable base segments + mutable memtable, `flush()`,
  `bulk_ingest()`, tombstone update/delete, ClickHouse-inspired score-based compaction
  (`compact` / `compact_all` / `compact_range`) with auto-triggers (`maybe_compact` / `maybe_flush`).
- **Persistence** — mmap'd `.seg` segment format with frozen hash tables (ADR-012), write-ahead log
  with CRC framing + crash recovery and configurable fsync policy (ADR-013), durable all-or-nothing
  bulk ingest (ADR-017), `Engine::open()` manifest + WAL recovery, query source store + `sources.dat`
  (ADR-014).
- **Read concurrency** — snapshot reads via `ArcSwap<EngineSnapshot>` + `parking_lot::Mutex` writer
  (ADR-016): lock-free reads, zero reader/writer contention.
- **Skip filter** — per-segment cache-line blocked bloom over signature keys (ADR-011), checked before
  each probe; `MatchStats` reports probe skip rate.
- **Runtime config** (`config.rs`) — `EngineConfig` knobs (segment cap, flush/holes thresholds,
  compaction cost, query complexity limits, WAL fsync policy) with startup validation. Dynamic knobs
  are runtime-tunable via the ES-style `GET/PUT /_settings` API (ADR-022); the config rides in the
  lock-free snapshot as `Arc<EngineConfig>`.
- **Observability** (`events.rs`) — `EngineEvent` / `EngineMetrics` / `CompactionTrigger` via a
  zero-dependency observer; wired to `tracing` structured logs + `prometheus` export. Durability
  degradation (WAL/manifest/segment/source-store write failures, corrupt-segment-skip on recovery)
  is routed through `EngineEvent::DurabilityFailure { op, detail, error }` instead of stderr, so the
  server logs it (`error!`/`warn!` by severity) and increments an alertable `durability_failures_total{op}`
  counter; recovery-time failures (pre-observer) are buffered and replayed on `set_observer` (ADR-021).
- **Vocabulary** (`vocab.rs`) — `Vocab` learn-from-any-of-groups + JSON persistence (ADR-015), runtime
  swap with vocab-epoch staleness tracking, per-segment reverse index for O(segments) delete.
- **HTTP server** (`bin/server.rs`) — ES-style REST (`/_doc`, `/_search` with explain/profile,
  `/_bulk` per-item status ADR-018, `/_stats`, `/_cat/stats`, `/_cat/segments` per-segment detail
  (text table + `?format=json`, ADR-023), `/_health`, `/_metrics`, `/_vocab*`,
  `/_settings` GET/PUT with dynamic-vs-static enforcement + `include_defaults` — ADR-022),
  graceful shutdown, production hardening (body/concurrency limits, request IDs, slow-query log,
  segment CRC, complexity limits).
- **Error handling** — typed `ParseError` / `NormalizerError`, fallible deserialization, zero
  panicking `unwrap()` in library code.
- **Tooling** — explain (`explain.rs`), seeded data generator (`gen.rs`), NPMI corpus learner
  (`bin/learn.rs`), title introspection (`bin/norm.rs`), benchmark + read-amplification harnesses
  (`bin/bench.rs`, `bin/segbench.rs`), CSV/JSONL loader (`loader.rs`).
- **Correctness** — randomized differential oracle (brute force vs engine): zero false negatives &
  zero false positives over 100k+ matches, across single-build / multi-segment / compaction configs.
- **Resident-memory reduction (ADR-020)** — per-component resident accounting
  (`dict`/`query_store`/`logical_index`/`alive` in `EngineMetrics`); lazy on-disk source store
  (`SourceStore`, `sources.dat` v2 sorted index+blob+CRC, `EngineConfig::retain_source`); flat mmap'd
  logical-index columns (`.seg` v2, binary-searched, v1-reconstruct back-compat). Resident drops from
  ~148 → ~4.5 B/query (`retain_source=false`). Both formats keep v1 read paths; oracle unchanged.
- **Broad-lane batch / columnar evaluation (ADR-026)** — the broad lane (`segment/broad_batch.rs`)
  now runs once per title-batch instead of per-title: a per-batch inverted index (feature → title
  bitmap), one probe per broad anchor per batch, and bitmap-algebra verification (`exact::eval_batch`,
  the transpose of `verify`), plus a pure-anchor skip-verify fast path. Exposed as `match_titles_batch`
  (Engine + snapshot) and `POST /_mpercolate` (ES `_msearch`-shaped). Byte-identical to the per-title
  path (`tests/broad_batch.rs` + batch oracle); broad postings scanned amortize ~1/batch_size (29× at
  256). Four dynamic knobs (`broad_batch_size`/`broad_columnar`/`broad_materialize`/`max_percolate_batch`)
  + broad Prometheus counters; `broad_columnar=false` is the inline kill-switch.
- **Cluster scope frame — read before the cluster entries below.** **Cluster v1** (shippable) = the
  in-process multi-shard core + durable local reopen + dynamic vocabulary — **built and oracle-proven,
  zero false negatives (Roadmap Tier 0, now complete)**. The gRPC / replication / control-plane /
  handoff / autoscaler layers in the entries below are **built and oracle-proven _in-process / on
  localhost_ but experimental** — not yet hardened for real multi-machine deployment (no TLS/auth,
  write-quiesce windows, advisory-only autoscaler, no auto-split). Each entry's *Honest scope* note
  records the per-feature boundary **as of that increment** (some items it flags as design-only were
  built in a later entry below).
- **In-process multi-shard core (ADR-027)** — the first, dependency-free step of clustering
  (`src/cluster/`): a `ClusterEngine` coordinator over K `Shard`s (each a `Shard`-wrapped `Engine` +
  `ArcSwap` snapshot), a consistent-hash `HashRing` over the query's **anchor `FeatureId`**, and content
  routing that sends a title only to its ~2–5 anchor shards (not all N) plus a designated replicated lane
  (shard 0) for class-C / class-B-arity-2 queries that have no rare anchor. One authoritative `Dict` is
  built over the whole corpus, frozen, and shared read-only into every shard, so `sig_key`s and hotness
  are globally consistent — a shard's indexing matches the coordinator's placement by construction.
  `compile::anchor_plan` (refactored out of `build_signatures`, byte-identical) is the placement SSOT.
  Proven by `tests/cluster_oracle.rs`: cluster ≡ single-node ≡ independent brute-force oracle across
  K∈{1,3,8,16} × broad on/off, zero false negatives / false positives, every placement class + small
  fan-out asserted. This is build-path steps 1–2 plus step 1's gRPC transport (ADR-029): behind the
  off-by-default `distributed` feature a `ShardServer` + gRPC `RemoteShard` carry a shard over the
  network — proven by `tests/cluster_grpc_oracle.rs` (gRPC cluster ≡ single-node ≡ brute, broad on/off).
  The remaining distributed layers are built later in this list (ADR-029→045) — oracle-proven
  _in-process / on localhost_ but experimental; see the **Cluster scope frame** above and Tier 3.
  ([`design/clustering-and-scaling.md`](design/clustering-and-scaling.md) §3/§7/§10.)
- **Durable cluster coordinator log (ADR-031)** — clustering build-path step 3a: the `ClusterEngine`
  coordinator now has durability of its own. A `trait ClusterLog` (`cluster/clog.rs`) with a CRC-framed
  `FileClusterLog` + in-memory `NullClusterLog`, a coordinator-level manifest + base snapshot (`storage.rs`),
  and log-first/fail-closed `add_query`/`remove_query` make an in-process cluster built with a `data_dir`
  rebuildable from disk alone: `ClusterEngine::open` re-derives byte-identical placement (zero false
  negatives) from manifest + snapshot + replayed log, and `checkpoint()` compacts the log. Raw DSL is the
  logged source of truth; one `apply` funnel serves both live writes and replay (the Raft state-machine
  apply in disguise — the seam is shaped for a Raft-backed log later). Dependency-free (lean core); proven
  by `tests/cluster_durability_oracle.rs` (rebuild ≡ pre-crash ≡ brute across K∈{1,3,8} × broad, +
  checkpoint, torn-tail, fail-closed, two-backend differential, fsync parity).
- **Per-shard durable segments — attach-and-mmap reopen (ADR-032)** — clustering build-path step 3b
  (local dir). Each shard is now a segments-only durable engine (`shard_<i>/segments/*.seg`, no per-shard
  WAL or manifest, built over the one shared frozen dict). `ClusterEngine::open` **attaches-and-mmaps** each
  shard's committed compiled segments and replays only the log tail — **no re-ingest/recompile of the
  corpus** (the cost that re-ingest paid at 100M). The coordinator manifest (v2) is the single atomic
  commit point recording the per-shard segment registry + per-shard `next_seg_id` + log cursor (the raw-DSL
  base snapshot + coordinator `live` set are gone). `checkpoint()` re-seals tombstoned base segments so a
  truncated `Remove` can't resurrect a query, and a missing/corrupt committed segment fails `open` loud
  (no silent shard-sized false negative). Crash-safety mirrors 3a (manifest = sole commit point; pre-commit
  crash ⇒ old registry authoritative + orphan segments recovered via log replay). Dependency-free; proven by
  the extended `tests/cluster_durability_oracle.rs` (the existing rebuild ≡ pre-crash ≡ brute, plus
  attach-with-no-log, the checkpoint-after-removing-a-build-time-query bug-catcher, orphan-ignored, and
  corrupt-segment-fails-loud). The durable base is **local-disk** segments — the shared-nothing model
  (ADR-033); a **Raft-backed** `ClusterLog` still drops in behind the same seam. Still design-only:
  cross-process/remote coordinator durability.
- **Cross-process dict shipping (ADR-034)** — completes the gRPC transport so a data node need not rebuild a
  byte-identical dict from the corpus out-of-band. The coordinator **ships** its frozen dict to each server
  at connect (new `AdoptDict` RPC over the existing `serialize_dict`/`Dict::fingerprint`); a server can start
  **pending** (dict-less) via `ShardServer::pending` and adopt it. Contract: empty shard → adopt; same
  fingerprint → idempotent no-op; **non-empty shard under a divergent dict → refuse** (surfaced as
  `ShardError::DictMismatch`), so the ADR-030 silent-FN guard is preserved where it matters. `connect_remote`
  ships by default (identical-dict shipping is a no-op, so pre-built callers are behavior-preserved). Behind
  the `distributed` feature; proven by `tests/cluster_grpc_oracle.rs` (a new dict-less-servers oracle ≡
  single-node ≡ brute, plus the updated divergence test) + a `server.rs` adoption-contract unit test. Scope:
  ships the **dict**; the normalizer is still a shared-vocab assumption (`default_vocab()` today) — vocab
  shipping is the next hardening. This is the first shared-nothing multi-node step (ADR-033 roadmap).
- **Per-shard replication + peer recovery — in-process (ADR-035)** — clustering build-path step 4, the
  Elasticsearch/Cassandra HA primitive. A `ReplicatedShard` composite (`src/cluster/replica.rs`) wraps one
  shard position's **primary + N replicas** behind the existing `trait Shard`, so the coordinator is
  unchanged (RF copies live inside one `Box<dyn Shard>`): writes fan out to the in-sync replicas, reads
  **fail over** to an in-sync replica on a transport error (never a stale one → no false negative), and
  aggregation + durability present the **primary's** view (so `num_queries`/`class_counts`/remove counts are
  rf-independent). A fresh replica is brought up by **peer recovery** — seal the primary, copy its `.seg`,
  attach-and-mmap (the in-process analogue of "stream segments from a peer"). `ClusterConfig::replication_factor`
  (default 1 = byte-identical to before) drives it; replicas are HA copies rebuilt from the primary on `open`,
  so the durable manifest is unchanged (primary + log remain the durable truth). Dependency-free; proven by
  `tests/cluster_oracle.rs` (RF∈{2,3}×K ≡ single-node ≡ brute, counts not inflated, live add/remove) and
  `tests/cluster_durability_oracle.rs` (durable RF=2 reopen ≡ pre-crash ≡ brute; checkpoint seals primaries
  only) + `replica.rs` unit tests. The **gRPC multi-node lift** (replicas as remote shards + a streaming
  segment-fetch RPC) is built in ADR-036.
- **gRPC multi-node replication + peer recovery (ADR-036)** — lifts ADR-035 onto the gRPC transport.
  `ClusterEngine::connect_replicated(groups)` wraps each position's primary + replica `RemoteShard`s in a
  `ReplicatedShard` (coordinator unchanged; reads fail over, writes fan out); `ShardServer` gains durable
  ctors (`pending_durable`/`new_durable`) and `AdoptDict` builds a durable shard when a `data_dir` is set.
  Two new RPCs — server-streaming `FetchSegments` (seal → manifest frame → chunked `.seg` runs; the receiver
  rejects a truncated stream rather than attaching a subset) and target-driven `RecoverFrom` (the recovering
  node pulls a peer's segments + attaches — the Elasticsearch model), orchestrated by `peer_recover_replica`.
  One new distributed-only dep (`tokio-stream`). Proven by `cluster_grpc_oracle.rs`'s
  `grpc_replicated_failover_and_peer_recovery` (K×RF servers ≡ brute; primary-stop failover; fresh-node peer
  recovery). **Honest scope:** recovery quiesces writes (there is no durable remote coordinator log to replay
  a tail from — that couples to the Raft step); shard→node placement / membership and TLS/auth stay design-only.
- **Cluster-state control-plane seam (ADR-037)** — clustering build-path step 5a, the dependency-free first
  increment of the quorum/Raft control plane. A `trait ControlPlane` (`src/cluster/control.rs`) — the
  document-mutation + linearizable-read sibling of `ClusterLog` — holds the small, low-rate cluster-state
  document (`ClusterState`: ring params + the **shard→node map** + membership + feature-model version +
  epoch), with an in-memory `InMemoryControlPlane` backend (the `NullClusterLog` analogue + fast differential
  backend). `ClusterEngine` carries it as a `Box<dyn ControlPlane>` defaulted to one logical node owning every
  shard, so the RF=1 / in-process path is **byte-identical**; new introspection `control_state()` /
  `assignment_for()` / `reassign_shard()`. The seam's shape (membership distinct from `propose`, a
  `ForwardToLeader` error, snapshot-read not watch, an app epoch distinct from the Raft term) is fixed so the
  **openraft** backend drops in behind it (step 5b) without touching the coordinator — and openraft is
  `distributed`-gated, so the lean core never sees it. Consensus holds the cluster-state doc **only**: query
  mutations stay on `ClusterLog` + the per-shard primary→replica path, the segment registry stays in the local
  manifest. Dependency-free; proven by `tests/cluster_control_plane_oracle.rs` (default ≡ brute across K×RF;
  document well-formed; reassignment preserves correctness; two-backend differential) + `control.rs` unit
  tests. **Honest scope:** the control plane alone does **not** lift the ADR-036 recovery-quiesce window — that
  needs a durable/replicated *per-shard query log* (step 5c, distinct from the control-plane doc); the openraft
  backend (5b), multi-process elections, and an allocator acting on the map are design-only.
- **openraft control-plane backend (ADR-038)** — clustering build-path step 5b: the real consensus engine
  behind the ADR-037 seam. A `RaftControlPlane` (`src/cluster/control_raft.rs`) implements `trait ControlPlane`
  over openraft's `Raft<C>` — `propose` → `client_write`, `change_membership` → `Raft::change_membership`,
  `cluster_state` → `ensure_linearizable` + state-machine read, openraft's `ForwardToLeader` mapped 1:1 — so the
  coordinator changes **no call site** and its default backend stays in-memory (every existing oracle byte-
  identical). The state machine routes each committed `Normal` entry through the SAME `control::apply` funnel as
  the in-memory backend (live ≡ replay) and derives `voters` from Raft membership entries. Cross-process consensus
  rides a new gRPC `ControlService` (3 RPCs carrying an **opaque serde envelope** — the proto never mirrors
  openraft's message types) added to the existing `shard.proto`, with a tonic `RaftNetwork` + a `ControlServer`
  and a `controlserver` manager bin. openraft is pinned `=0.9.24`, `optional`, and **`distributed`-gated — absent
  from the lean (`--no-default-features`) dependency graph**. Proven by `tests/cluster_control_raft_oracle.rs`:
  a 3-node in-process cluster (genuine elections + replication + quorum commit) converges to the in-memory
  backend's document (voters/nodes/assignments/model — not the epoch, which openraft's own Blank/Membership
  commits perturb), a follower `propose` returns `ForwardToLeader`, `change_membership` routes to Raft, and —
  over real gRPC servers on localhost — the cluster **survives its leader being killed** (re-elects from quorum,
  preserves the committed document, accepts a fresh write). **Honest scope:** this does **not** close the ADR-036
  recovery-quiesce window (that is step 5c — a durable/replicated per-shard *query* log); a durable Raft log
  (CRC-framed, reusing `storage::crc32`), TLS/auth, and an allocator acting on the shard→node map remain
  design-only.
- **Durable + replicated per-shard query log (translog) + no-quiesce peer recovery (ADR-039)** — clustering
  build-path step 5c, closing the ADR-036 recovery-quiesce gap. Each durable shard owns a per-shard **translog**
  (`src/cluster/translog.rs`) — the Elasticsearch translog, reusing ADR-031's CRC-framed `FileClusterLog` /
  `NullClusterLog` + the logical-id-and-DSL `ClusterMutation` + `LogPos` verbatim, re-homed per shard. Writes are
  log-first/fail-closed; `seal_for_checkpoint` captures the snapshot position `P` and trims the tail (segments
  hold ops ≤ `P`, the translog holds exactly the un-sealed ops > `P` — the no-double-apply boundary). Peer
  recovery now streams a peer's segments at `P` **then replays the translog tail (> `P`)** — the writes that land
  during the copy window are recovered rather than lost, so recovery need **not quiesce** writes, both in-process
  (`peer_recover` + `catch_up_replica`) and over gRPC (a new server-streaming `FetchTranslog(after_seqno)` RPC +
  `FetchManifest.up_to_seqno` + the coordinator's `peer_recover_replica`). A durable data node also **self-restarts**
  from a per-shard checkpoint sidecar (`shard.ckpt`: committed segments + `P` + dict fingerprint), attaching its
  segments + replaying its translog tail after its own crash (no coordinator manifest on the remote path). The
  default in-memory / RF=1 / in-process paths are byte-identical (a `NullClusterLog` translog), so every prior
  oracle is unchanged. Proven by `tests/cluster_grpc_oracle.rs::grpc_peer_recovery_without_quiescing` (recovered
  ≡ live source ≡ brute over the final live set across the wire), `replica.rs::peer_recover_replays_tail_without_quiescing`
  + `::durable_shard_self_restarts_from_translog`, and `translog.rs` unit tests. `translog.rs` is std-only (lean
  core); the gRPC pieces are `distributed`-gated. **Honest scope (closed by ADR-040 below):** retention/GC and the
  finalize loop landed in step 5d; TLS/auth and an allocator acting on the shard→node map remain design-only.
- **Translog retention leases + finalize under sustained writes (ADR-040)** — clustering build-path step 5d,
  closing ADR-039's two scope gaps. (1) **Retention leases** (the Elasticsearch peer-recovery retention lease):
  the recovery source (`LocalShard`) holds a lease set, and `seal_for_checkpoint` now trims the translog to
  `min(P, lease_floor)` instead of `P` — so a **concurrent** seal (another recovery's `FetchSegments`, a
  checkpoint) can no longer trim away the tail an in-flight recovery still needs (a latent false negative in
  ADR-039's no-quiesce path), and with no lease held it trims to `P` (**byte-identical** to ADR-039) so the
  translog GCs the moment no recovery needs it. Three `Shard` methods (acquire/renew/release, default no-op);
  over gRPC a `RetentionLease` RPC. (2) **Finalize:** recovery holds one lease across a **convergence loop**
  (`catch_up_replica` until the tail stops advancing), then promotes the replica into the in-sync set under a
  brief write quiesce — the window shrinks to the residual delta, not the whole copy. `ReplicatedShard` gained
  runtime replica growth (`add_recovered_replica`) and `ClusterEngine::add_replica` exposes it; correctness
  never depends on the loop converging (the lease keeps the tail safe), only the window size does. Default
  in-memory / RF=1 paths byte-identical; proven by `replica.rs` unit tests (retention-keeps-tail-across-a-
  concurrent-seal, runtime in-sync promotion) + `tests/cluster_grpc_oracle.rs::grpc_peer_recovery_converges_under_sustained_writes`
  (a writer thread streams adds concurrently with the recovery; recovered ≡ live source ≡ brute over the final
  set). Lean-core retention + finalize; the RPC is `distributed`-gated. **Honest scope:** a stuck lease has no
  time/size expiry yet; cross-node in-sync promotion of a remote replica routes through the allocator (design-only);
  TLS/auth deferred.
- **Durable Raft log + control-plane restart recovery (ADR-041)** — clustering build-path step 5e, making the
  ADR-038 openraft backend survive a restart. A new `src/cluster/control_store.rs` is the byte-level durable
  substrate: a CRC-framed append-only record log (reusing the `clog`/`wal` forward-scan / torn-tail pattern +
  `storage::crc32`) for the Raft entries, plus atomic single-value files (tmp + fsync + rename) for the **vote**
  (election safety), the **committed** log id (`save_committed`, so a restart re-applies `(snapshot.last,
  committed]`), the last-purged id, and the state-machine **snapshot**. The state machine is NOT persisted
  per-apply — openraft rebuilds it on restart from the snapshot + the replayed log (so `apply` stays the
  in-memory `control::apply` ⇒ live ≡ replay unchanged). `LogStore`/`StateMachine` gained `in_memory()` (the
  ADR-038 path — byte-identical) + `open(dir, fsync)`; `build_node` takes `Option<&Path>`, `in_process_cluster`
  stays in-RAM, and `start_grpc_node` + the new `controlserver --data-dir` flag make a manager node durable. A
  `RaftControlPlane::shutdown()` releases the files for a clean restart. Proven by
  `tests/cluster_control_raft_oracle.rs::durable_node_recovers_committed_document_after_restart` (commit → shutdown
  → rebuild from disk → the committed doc survives + a fresh write commits) + `control_store.rs` unit tests. All
  `distributed`-gated (the lean core never compiles openraft); no new dependency. **Honest scope:** an end-to-end
  durable-multi-node rolling-restart harness + TLS/auth remain design-only.
- **Shard→node allocator (ADR-042)** — clustering build-path step 5f, the decision layer that fills the
  control-plane shard→node map. `src/cluster/allocator.rs` plans a placement via **rendezvous (HRW)** hashing
  (`util::fnv1a64` over `(position, node)`): for each shard position the top-RF nodes by weight (primary +
  replicas) — balanced, deterministic, and **minimal-movement** (a node add/remove reassigns ≈1/N of positions,
  not all, like Elasticsearch/Cassandra rebalance). `ClusterEngine` gained `register_node`/`deregister_node`
  (membership via the control plane) + `rebalance(rf)`, which commits only the changed positions
  (`changed_assignments`) as `AssignShard` proposals — idempotent (no membership change ⇒ 0 moves), fail-closed,
  a no-op on the single-node default. Lean core, dependency-free. **Scope:** this commits the *desired* map;
  physically relocating a shard's segments on a reassignment reuses peer recovery (ADR-036/039) and is the
  deployment wiring on top (in-process the map is advisory — matching is unaffected). Proven by `allocator.rs`
  unit tests (distinct primary+replicas, RF clamp, determinism, ≈1/N movement, balance, the diff) +
  `tests/cluster_allocator_oracle.rs` (register → rebalance ⇒ a balanced fully-assigned map; idempotent; a
  deregistered node drops out; `percolate` byte-identical before/after every rebalance ⇒ zero-FN preserved).
  Foundation for the autoscaler (ADR-045, built) + auto-split (step 6).
- **Swappable shard backing — the live-handoff routing-flip mechanism (ADR-043)** — clustering build-path
  step 6a: the routing half of the data-moving handoff (the byte mover, peer recovery, already exists). A
  `HandoffShard` (`src/cluster/handoff.rs`) wraps one shard position's backing in an `ArcSwap<Box<dyn Shard>>`
  + a generation stamp and implements `Shard` on `Arc<HandoffShard>`, so the gRPC builders
  (`connect_remote`/`connect_replicated`) wrap each position and keep a typed handle (a `handoffs` side-table)
  that step 6b uses to **re-point a position at a new owner at runtime** without downcasting `dyn Shard`.
  **Serve-then-drop falls out of `arc_swap`:** an in-flight probe completes against the *old* backing
  (lock-free, safe under the coordinator's rayon fan-out) while a swap re-points the slot atomically; the
  generation is the epoch-fence stamp step 6b reads (inert here). The whole capability is `distributed`-gated,
  so the lean core and the in-process/RF=1 default path are **byte-identical** (every prior oracle unchanged).
  Proven by six `handoff.rs` unit tests (a swap to a set-equal backing is byte-identical in ids + stats; an
  in-flight read serves the old backing while a fresh read sees the new one; the generation tracks swaps;
  concurrent readers survive repeated swaps; writes and the defaulted `set_event_sink` forward to the backing).
  The cross-node move that *drives* the swap is step 6b (ADR-044, below).
- **Live data-moving handoff — the cross-node move (ADR-044)** — clustering build-path step 6b: wires
  decide→move→flip into one **live** shard move (a position's owner changes while the cluster keeps serving).
  A new `Fence` RPC + a monotonic, write-only server-side fence demote the old owner: once fenced, its
  data-mutating writes (`insert`/`delete`/`ingest`) return `failed_precondition` while **reads + the recovery
  RPCs stay served** (so an in-flight read never hits the fence — serve-then-drop). `ClusterEngine::execute_handoff`
  orchestrates it under one retention lease: no-quiesce bulk peer-recover the target → **fence** the source
  (the position's brief write-quiesce begins) → **drain to convergence** (the fenced source's tail is finite +
  frozen, so looping the catch-up captures every op it ever accepted — closing the TOCTOU a single final
  catch-up would leave) → **flip** the 6a `HandoffShard` backing source→target. Fence-LATE (after the
  no-quiesce copy) keeps the no-quiesce property; only the converge-then-flip is write-quiesced. The byte
  mover is peer recovery (ADR-036/039); the lease (ADR-040) pins the tail so a concurrent seal can't strand
  it. Proven by `tests/cluster_grpc_oracle.rs::grpc_live_handoff_under_sustained_writes` (reassign a position
  source→target under a concurrent writer that retries the brief fence-window rejections; the SAME cluster,
  re-pointed to the new owner, ≡ the brute oracle over the final live set — zero false negatives, reads never
  paused) + `src/cluster/server.rs::fence_rejects_writes_but_serves_reads`. `distributed`-gated; no new
  dependency. **Honest scope:** single-coordinator (the fence is the multi-coordinator guard; the flip is
  serialized); a fence-window write is fail-closed + retryable (never lost); a non-converging source aborts
  the flip fail-closed (the source stays fenced — a stuck position, never a lost write); "drop the old owner" =
  drop from routing, not teardown; RF>1 group relocation reuses the same swap but the oracle covers the
  single-owner move. A non-converging (or any post-fence) abort now **auto-unfences the source** so it
  resumes serving instead of staying stuck (ADR-048); the drain caps are `ClusterConfig` knobs. **Still
  design-only:** auto-split.
- **Autoscaler — the policy/trigger layer (ADR-045)** — clustering build-path step 6c: the policy that
  *decides when* to drive the built mechanisms. A pure, deterministic `cluster::autoscale::evaluate(snapshot,
  config)` over a `LoadSnapshot` (membership + the shard→node map + per-shard corpus — the only load signal
  that crosses the `Shard` seam, so it behaves identically in-process and across nodes) emits `ScalingAction`s:
  **membership drift → `Rebalance` (executable)**, **per-node skew → `Handoff` (advisory)**, **per-shard corpus
  over a threshold → `RecommendSplit` (advisory)**. The thin `ClusterEngine` driver (`coordinator::autoscale`)
  `tick(config)` collects the snapshot, runs `evaluate`, executes the executable subset (each `Rebalance` → the
  idempotent `rebalance(rf)`), and returns the full decision incl. advisories; `on_node_joined`/`on_node_left`
  are the event-driven entries. The membership trigger is coarse (it never recomputes HRW — `evaluate` stays a
  pure function of the snapshot); the idempotent `rebalance` is the truth for the exact moves, so there is **no
  clock / hysteresis** (idempotence *is* the hysteresis). `AutoscaleConfig::default()` is **disabled** ⇒ `tick`
  is a no-op ⇒ every prior oracle is byte-identical. Lean core, no new dependency. **Honest scope:** auto-split
  is **advisory only** (no split mechanism — the ring's `num_shards` is fixed at construction; it needs ring
  re-keying + a `recommended_shard_count` signal); load-driven handoff is now **driven** — `tick` calls
  `execute_handoff` for a `Handoff` (gRPC-gated, ADR-048), guarded so it never runs in the same tick as a
  rebalance; QPS/compute-replica autoscaling (HPA-style) is out of engine scope. Proven by
  `src/cluster/autoscale.rs` unit tests (the deterministic policy) + `tests/cluster_autoscale_oracle.rs`
  (`tick` ≡ a manual `rebalance`; `percolate` byte-identical before/after a tick ⇒ zero-FN; a second tick
  commits nothing; a disabled config is a no-op; a split advisory mutates nothing).
- **Remote live-write partial-apply: observe + fail-closed + repair (ADR-047)** — distributed-layer hardening
  from an external review. A selective query placed on 2+ remote shards could see one insert succeed and the
  next RPC fail, leaving a **silent partial mutation** (a transient false-negative window until reopen).
  Now: `apply_add`/`apply_remove` **try every target shard and collect failures**; a partial failure emits a
  `DurabilityFailure { op: ClusterPartialApply }` event, returns the honest `ShardError::PartiallyApplied`,
  and **queues the failed shards for repair**; `ClusterEngine::resync()` re-drives only the still-failed
  shards (the autoscaler `tick` calls it opportunistically). The `RemoteShard` `block_on` bridge is now
  **thread-context-safe** (`block_on_in_context`: `block_in_place` on a multi-thread runtime worker, plain
  `block_on` off-runtime) so a future async coordinator can't hit the nested-runtime panic on the
  single-target read path. The **in-process / RF=1 default is byte-identical** (infallible writes ⇒ no partial
  apply ever recorded) — the Cluster-v1 oracles stay green unchanged. **Honest scope:** still **no cross-write
  fencing / quorum** (concurrent overlapping writers + a `resync`/same-id-write race resolve last-writer-wins
  in memory, authoritatively by the log on reopen); single-shard (replicated-lane) failures converge on reopen,
  not live `resync`; the durable log remains the correctness backstop, `resync` a liveness optimization. Proven
  by `cluster/coordinator/tests.rs` (deterministic detect→resync→converge + requeue-while-failing) +
  `tests/cluster_grpc_oracle.rs` (wire-level detection + the single-target `block_on` guard).
- **Reliability hardening: auto-unfence-on-abort + translog-lease TTL + autoscaler-driven handoff (ADR-048)** —
  closes three explicitly-deferred items (ADR-040/044/045) that each left a cluster needing manual recovery or
  a control loop open. (1) **Auto-unfence-on-abort:** a new CAS-guarded `Unfence` RPC (lift the fence only at
  the exact generation this handoff set — preserving the Fence monotonic-safety story); `execute_handoff` now
  lifts the fence on *any* post-fence failure, so an aborted move resumes serving instead of staying
  write-quiesced. The drain caps became `ClusterConfig` knobs. (2) **Translog-lease TTL:** each retention lease
  carries a `last_renewed` heartbeat (`renew` refreshes it), and `seal_for_checkpoint` reaps a lease idle past
  `retention_lease_ttl_secs` (default 1800; `0` = disabled) so a crashed recovery's tail is reclaimable — a
  reap is surfaced as a `DurabilityFailure { op: ReplicaDesync }` (a plain `LocalShard` now honors the
  observer's sink). (3) **Autoscaler-driven handoff:** the `tick` driver resolves a `Handoff`'s nodes to
  endpoints and calls `execute_handoff`, guarded so it never runs in the same tick as a rebalance (stale
  target) and self-healing on failure (item 1). All three are **`distributed`-gated / byte-identical on the
  lean + in-process path**; the lease TTL default never reaps a live (heartbeating) recovery. Proven by
  `retention_lease_tests::*` + `ttl_reaps_a_stuck_lease_…` (lean core), `grpc_handoff_abort_unfences_source` +
  `grpc_autoscaler_tick_drives_handoff_resolution_and_preserves_matching` (real wire), and
  `tick_emits_handoff_under_skew_without_perturbing_matching`. **Honest scope:** driving a load move to
  *completion* over gRPC additionally needs the control-plane node→endpoint map to match the shard endpoints
  (one-server-per-shard can't host a moved shard on a busy endpoint) — deployment-model maturity (Tier-3
  residue); the oracle proves the driver's resolution + fail-safe skip + zero-FN, the move's happy path is
  `grpc_live_handoff_under_sustained_writes`.
- **Cluster module restructured for agent-friendliness (no behavior change)** — the four largest `src/cluster/`
  files were split into focused submodules following the `segment.rs` pattern (root = the type *defs*, `impl`
  blocks split by responsibility): `coordinator.rs` (1,698 lines → `coordinator/{lifecycle,ingest,matching,
  control_plane,distributed,tests}`), `replica.rs` (1,205 → `replica/{shard_impl,test_support,tests}`),
  `control_raft.rs` (1,091 → `control_raft/{log_store,state_machine,network,builders}`), and `server.rs`
  (1,004 → `server/{service,tests}`) — no cluster file now exceeds ~600 lines. A pure refactor: cross-sibling
  private items widen to `pub(in crate::cluster::<mod>)`, public API + matching are unchanged, proven by the
  full cluster oracle suite + `check.sh` staying green. The per-area router is the module map in
  [`../CLAUDE.md`](../CLAUDE.md).

## Measured

Headline figures only. Full tables, p99s, and the 100M extrapolation are the canonical record in
[`performance/results.md`](performance/results.md); the machine-independent regression invariants live
in [`performance/benchmark-results.txt`](performance/benchmark-results.txt).

- Selective path **~158k–710k titles/sec/core** (1M–5M queries; ~256 B/query), **~3.8× on 4 threads**.
- Flat **~54 candidates/title**, independent of corpus size.
- **~750k updates/sec/core** with immediate (epoch) visibility; build **~650k queries/sec/core**.
- LSM read-amplification stays bounded as segments grow (1→8): candidates/title flat, throughput ~2×
  off, filter skip rate climbing toward ~87% — table in [`performance/results.md`](performance/results.md) §7.
- **Resident memory (mmap profile, ADR-020):** ~148 → **~4.5 B/query** with `retain_source=false`
  (source store + reverse index both off-heap) — ~33× (~14.5 GB → ~0.45 GB extrapolated to 100M).

---

## Roadmap at a glance

The full prioritized roadmap — design-only items, the **Cluster v1** acceptance gate, the
operational-polish backlog, and the evaluated-and-declined list — lives in **[`roadmap.md`](roadmap.md)**.
Tiers, highest-leverage first:

- **Tier 0 — Cluster v1 acceptance gate (complete).** In-process multi-shard core + durable local
  reopen + dynamic vocabulary (ADR-046) — built + oracle-proven, the shippable milestone.
- **Tier 1 — highest-leverage bottlenecks.** Broad-lane batch evaluation (✅ ADR-026) + resident-memory
  reduction (✅ ADR-020) — both shipped.
- **Tier 2 — feature-model quality & self-tuning.** Compaction-that-improves, wire the NPMI learner,
  confidence-gated alias learning.
- **Tier 3 — scale & production maturity.** Feature-model versioning + blue/green; hardening the
  (experimental) distributed multi-node layers; aspects-first ingestion.
- **Tier 4 — ES/OS percolator parity.** Per-query metadata + filtered percolation (✅ built single-node,
  ADR-049); ranking + `/_mpercolate` pagination; byte-cleaning + bulk-alias APIs.

See **[`roadmap.md`](roadmap.md)** for the per-tier detail, the Nice-to-have / operational-polish
backlog, and the Evaluated & declined list.

---

## Current limitations

- **Not yet a hardened multi-node deployment.** The full shared-nothing multi-node stack is **built and
  oracle-proven _in-process / on localhost_ (experimental)** — sharding + content routing, the gRPC `ShardServer`/`RemoteShard` transport with coordinator
  dict shipping, a durable coordinator log, per-shard local durable segments (attach-and-mmap reopen), per-shard
  replication + peer recovery (in-process + gRPC), a durable openraft control plane (multi-process elections +
  leader failover + restart recovery), a per-shard translog (no-quiesce peer recovery + retention/finalize),
  a rendezvous-hash shard→node allocator, a runtime-swappable shard backing (the live-handoff routing-flip
  mechanism, ADR-043), the **live data-moving handoff** itself (the cross-node move under concurrent
  writes — peer-recover → fence → drain-to-convergence → flip, ADR-044), and the **autoscaler** policy that
  drives `rebalance` on membership/skew events (ADR-045) — with reliability hardening (auto-unfence-on-abort,
  translog-lease TTL, autoscaler-driven handoff; ADR-048) (ADR-027, 029, 031–048; per-ADR detail
  in [Implemented](#implemented-working-tested) above). But it is exercised **single-process / on localhost** by
  the oracles — not yet deployed and hardened across real machines. **Remaining for production multi-node** (all
  design-only; ADR-033 — no object store / cloud dependency anywhere): **auto-split** + `recommended_shard_count`
  (and the clean node→endpoint move a real load-driven handoff implies),
  **cross-process dynamic vocabulary** (the in-process piece is the now-complete Tier-0 v1 item — [research
  spike](research/dynamic-vocabulary.md)), **replicate-broad-to-all**, **TLS/auth** on the (currently plaintext)
  transports, and an end-to-end durable-multi-node rolling-restart harness — see Tier 3.
  **Correctness caveat (ADR-029/030/034):** cross-process dict identity is handled — the coordinator **ships**
  its frozen dict at connect (ADR-034) and the ADR-030 fingerprint handshake fails loud
  (`ShardError::DictMismatch`) if a *populated* server holds a divergent dict, so a diverged dict can never drop
  matches *silently*. The **normalizer** must still match on both sides (`default_vocab()` today — absorbing
  new vocabulary after the dict is frozen is **done in-process (Tier 0, ADR-046)**; shipping learned aliases
  *cross-process* to a remote shard's normalizer remains deferred), and the transport is
  unauthenticated/plaintext. Treat the gRPC surface as correctness-safe, not yet a hardened multi-process deployment.
- **Empty default vocabulary.** `default_vocab()` ships no domain terms; vocabulary is supplied at
  runtime via the `Vocab` system or `NormalizerBuilder`. Auto-deriving it from the corpus is the
  NPMI-wiring item in Tier 2. Absorbing vocabulary that first appears *after* the dict is frozen
  (live writes against a cluster's shared frozen dict) is the **Tier-0 dynamic-vocabulary** item.
- **Validated on synthetic data only.** The differential oracle and the benchmarks run against the
  seeded synthetic generator ([`gen.rs`](../engine/src/gen.rs)), which is deliberately adversarial
  (ADR-008); one design-validation pass ran ~20 real eBay titles through the normalizer
  ([`research/real-data-findings.md`](research/real-data-findings.md)). What has **not** been done is a
  false-negative / false-positive audit (or throughput run) against a *real saved-search corpus* with
  messy listing titles. Synthetic data cannot stand in for the long tail of real text, so this is the
  highest-leverage step for external credibility — and a prerequisite before quoting the headline
  numbers as production guarantees rather than design-target evidence.

The former production-hardening audit's medium-priority items — metrics gaps (P2-2), response-envelope
consistency (P2-8), and bulk-ingest lock scope (P2-14) — are now resolved (2026-05-29): P2-2 and P2-8
were implemented and P2-14 was closed as stale/by-design (reads are lock-free since ADR-016). The
audit no longer exists as a separate document; its surviving lower-priority items live in the
Nice-to-have backlog above and in the relevant ADRs.
