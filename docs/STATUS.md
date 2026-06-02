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
  The remaining distributed layers stay design-only (see Tier 3).
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
  Foundation for autoscale/auto-split (step 6).

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

## Roadmap (design-only, prioritized)

Priority follows the bottleneck analysis ([`performance/results.md`](performance/results.md) §9): the
selective match path is already ~255× the spec target with a flat ~54 candidates/title, so the leverage
is in the **broad lane**, **memory/footprint**, and the **durability + scale** story — not in shaving
the selective candidate count further.

### Tier 1 — highest leverage (the measured bottlenecks)

- ~~**Broad-lane batch / columnar evaluation.**~~ **✅ Shipped (ADR-026).** The broad lane now runs
  once per title-batch (columnar): per-batch feature→title inverted index, one probe per broad anchor
  per batch, bitmap-algebra verification, and a pure-anchor skip-verify fast path (the
  materialized-subscription analog). Exposed as `match_titles_batch` + `POST /_mpercolate`; byte-identical
  to the per-title path; broad postings scanned amortize ~1/batch_size (29× at batch 256, ~2.4× end-to-end
  throughput over the inline path). The "metered to a higher cost class" intent is satisfied by the new
  broad `MatchStats`/Prometheus meters. The single biggest matching-performance lever — now resolved.
  Remaining follow-ups: class-C ingest warnings/rewrite suggestions (its own feature), SIMD posting
  intersection. ([`design/matching.md`](design/matching.md) §4; details in the Implemented section above.)
- ~~**Memory: resident-footprint reduction.**~~ **✅ Shipped (ADR-020).** Phase-0 measurement showed
  resident RAM (once the SoA/index are mmap'd) is dominated by the **source store** (91 B/q) and the
  **reverse index** (53 B/q), *not* the dict. Both are now off-heap — lazy on-disk source store +
  flat mmap'd logical-index columns — dropping resident from **~148 → ~4.5 B/query** (~33×; ~14.5 GB →
  ~0.45 GB at 100M). Deferred as not worth it *for memory*: dict arena/mmap (bounded, ~3.5 B/q — its
  separate un-versioned-manifest correctness hazard is future work) and tighter SoA packing (paged —
  helps disk/throughput, not resident RAM).

### Tier 2 — feature-model quality & self-tuning

- **Compaction-that-improves.** The merge mechanic is done; add the "improve" phase — recompute stats
  and re-anchor queries whose anchor drifted hot, repacking covers during a merge that's already
  happening. ([`design/ingestion-and-updates.md`](design/ingestion-and-updates.md) §7.)
- **Wire the NPMI learner as the runtime vocab source.** The `learn.rs` corpus learner and the `Vocab`
  runtime plumbing both exist but aren't connected; wiring them lets the feature model self-derive from
  the corpus. ([`research/corpus-feature-learning.md`](research/corpus-feature-learning.md).)
- **Alias / equivalence learning** (e.g. `UD` ≡ `Upper Deck`) with the precision-first safety rail
  (expansion-not-collapse, feedback-validated, reversible) — the one feature-learning sub-problem that
  can affect correctness, so it stays confidence-gated.

### Tier 3 — scale & production maturity (larger builds)

- **Feature-model versioning + blue/green re-materialize.** Frozen common-mask across minor versions;
  a major model change is replayed from the log into a parallel index, then an atomic alias/epoch swap.
- **Clustering — the 100M horizontal-scale story** (built on the **shared-nothing** model: local segments +
  per-node/coordinator WAL + replication + a quorum control plane — **no object store, no cloud dependency**;
  ADR-033). The stack is **built and oracle-proven through build-path step 5f** — in-process multi-shard core,
  the gRPC transport + dict shipping, the durable coordinator log + per-shard local segments, replication +
  peer recovery, a durable openraft control plane, a per-shard translog with no-quiesce recovery +
  retention/finalize, and a rendezvous-hash shard→node allocator (ADR-027, 029, 031–042). **Per-ADR detail is
  in [Implemented](#implemented-working-tested) above**; the build path + cross-shard correctness argument are
  in [`design/clustering-and-scaling.md`](design/clustering-and-scaling.md) §10 (hashing-variant survey:
  [`research/clustering-prior-art.md`](research/clustering-prior-art.md)). **Still design-only** — the
  production multi-node residue (step 6 + hardening): an **autoscaler** that drives `rebalance`/`register_node`
  on membership events; the **live data-moving handoff** on a reassignment (serve-then-drop + epoch fencing —
  the allocator decides the map, peer recovery already moves the bytes); **auto-split** +
  `recommended_shard_count`; **normalizer/vocab shipping** (dict shipping landed in ADR-034; the normalizer is
  still a shared `default_vocab()` assumption); **replicate-broad-to-all** (in-process uses the shard-0 lane
  only); **TLS/auth** on the gRPC + control transports; and a translog-lease TTL + an end-to-end
  durable-multi-node rolling-restart harness.
- **Aspects-first ingestion.** Use eBay structured item-specifics as features instead of relying only
  on title parsing — higher feature quality, but a larger domain integration.

### Tier 4 — ES/OS percolator parity (not fully verified — based on initial gap analysis)

These items would close the remaining gaps between Reverse Rusty's DSL/normalizer and what
production ES/OS percolator deployments typically rely on. They are based on a preliminary
comparison with a real-world percolator workload; the scope of each may shrink or grow once
implementation begins.

- **Byte-cleaning: punctuation-equivalence rules.** `clean_into` currently maps all
  non-alphanumeric, non-marker characters to a space. Production title corpora treat
  mid-word hyphens (`-`), apostrophes (`'`, `'`), slashes (`/`), and periods differently
  — e.g. `O'Brien`, `O-Brien`, and `OBrien` should all normalize to the same token. Add a
  configurable punctuation-folding table to the byte-cleaning pass so callers can declare
  which characters collapse vs. become word boundaries.
  ([`normalization.md`](design/normalization.md) §2.)
- **`NormalizerBuilder`: bulk synonym / alias registration API.** The builder already
  supports phrases and single-token synonyms, but real deployments need to register
  hundreds of equivalences (abbreviation → canonical, variant spellings, term expansions
  like `auto` ≡ `{autograph, autographed, signature, signed}`). Add a batch registration
  method and/or a file-based vocabulary loader so large synonym tables are easy to maintain
  outside of code.
- **Metadata-aware result filtering.** ES/OS percolator queries are typically stored alongside
  structured metadata (entity type, category, status) and filtered at search time via bool
  clauses. Reverse Rusty today returns raw query-ID sets with no metadata awareness. Options:
  per-query tag storage with post-match filtering, or partitioned indices. Design TBD —
  the goal is to support the common pattern of "percolate title, then narrow by category"
  without requiring a separate metadata lookup.
- **Match scoring / ranking hooks.** ES/OS percolator returns `_score` from the stored
  query's relevance model; production consumers use `function_score` wrappers to boost
  results by metadata (e.g. status priority). Reverse Rusty currently returns binary
  match/no-match. Add an optional scoring callback or rank-annotation layer so callers
  can order results without a separate pass.

### Polish / niche

- **SIMD intersection** for medium/large (mostly broad-lane) roaring postings — a micro-optimization
  best folded into the broad-lane work above.

### Evaluated & declined

- **Query-family / shared-prefix DAG** (subtree pruning). Implicit anchor-sharing already captures the
  near-duplicate-clustering benefit, the selective path isn't the bottleneck, and the
  mmap-serialization + compaction-rebuild cost wasn't justified. See [`DECISIONS.md`](DECISIONS.md)
  ADR-019.

---

## Nice-to-have / operational polish backlog

Low-priority polish, ergonomics, and micro-optimizations — none are production blockers (moved here
from the audit's former P3 list). Roughly grouped:

**API / ops ergonomics**
- **No CORS headers** — browser-based tools can't hit the API. Add `tower-http::CorsLayer`.
- **No `--version` flag** in the CLI.
- **No Dockerfile or k8s manifests.**
- ~~**No segment detail endpoint** (`/_cat/segments`).~~ **✅ Shipped (ADR-023).** `GET /_cat/segments`
  returns per-segment detail — kind (memory/mmap/memtable), entries/alive/deleted, holes ratio, vocab
  epoch + stale flag, and a resident-vs-overhead byte split — as a text table or `?format=json`, read
  lock-free from the snapshot. Two follow-ups it deliberately deferred are tracked as their own items
  below (per-segment filter FP rate; `_cat` verbose/column-selection flags).
- **No thread-pool introspection** (`/_cat/thread_pool` equivalent).
- **No per-segment filter FP rate in `/_cat/segments`** (deferred from ADR-023). The anchor filter doesn't
  retain its inserted key count, and the mmap arm doesn't expose the filter's block count through the
  `BaseSegment` wrapper — so an honest, *symmetric* false-positive-rate column (real for both memory and
  mmap segments) needs a small change first: have `SegmentFilter` retain `n` at build time and expose
  block count on `MmapSegment`. Then add a `filter_fp_pct` column to the endpoint.
- **`_cat` endpoints lack ES `?v` / `?h` / `?help` flags** (noted in ADR-023). `/_cat/*` returns a fixed
  text table (always with a header) or `?format=json`; ES also supports a verbose toggle, column
  selection, and a help listing. Low-value polish, listed for completeness.
- **`took_ms` uses raw f64** — yields values like `0.003284000000000001`. Use integer ms or round to 2 dp.
- **No pre-warming** for mmap'd segments on cold start.

**Memory / hot-path micro-optimizations**
- **`alive: Vec<bool>`** uses 8× the memory of a bitvec (1 byte vs 1 bit per entry).
- **`seg_lens` Vec allocated on the match hot path** — could be a fixed-size array.
- **WAL `append_insert` allocates a Vec per write** — production WALs use pre-allocated write buffers.
- **Byte-at-a-time CRC-32** for manifest writes — table-based would be ~10× faster.

**Robustness / build hygiene**
- **Durable-ingest segment-write failures surface only as `ingest_rollback`, not `segment_write`.** ADR-021
  routes the *flush* path's segment write through a precise `DurabilityOp::SegmentWrite`, but the durable
  build/bulk path (`build_durable_base`) returns the `io::Error` up to the infallible wrapper, which emits
  `IngestRollback` with the OS error in the `error` field — so the operator sees the cause but not the
  precise op label (unlike a manifest failure, which emits both `manifest_write` + `ingest_rollback`).
  Optional refinement: emit `SegmentWrite`/`SegmentMmap` from inside `build_durable_base` for symmetric
  labeling. Low priority — the underlying error is already visible.
- **Dict format not versioned** — adding a new `FeatureKind` variant would silently corrupt deserialization.
- ~~**`GET /_vocab` acquires the write mutex.**~~ **✅ Fixed.** `EngineSnapshot` now carries the vocab as
  an `Arc<Vocab>` (the `Engine` holds `Option<Arc<Vocab>>`, `Arc::clone`d into each snapshot — O(1) per
  publish), and `get_vocab` reads `state.snapshot.load().vocab()` instead of locking the engine. Vocab
  reads are now lock-free like every other read endpoint, closing the last ADR-016 violation. (No new
  ADR — this completes ADR-016's stated design.)
- ~~**Server/observability deps are not feature-gated.**~~ **✅ Fixed (ADR-028).** The nine
  HTTP/observability crates (`axum`/`tokio`/`clap`/`parking_lot`/`tower`/`uuid`/`tracing`/
  `tracing-subscriber`/`prometheus`) are now `optional` behind a default-on `server` feature, and the
  server bin carries `required-features = ["server"]`. `cargo build --no-default-features` yields the
  lean embeddable core (daachorse/memmap2/rayon/roaring/arc-swap/serde/serde_json + transitives),
  enforced by the new `clippy (lean core)` lane in `check.sh`. `serde`/`serde_json` stay core (Vocab
  JSON, `EngineConfig`, `ExplainDetail`, JSONL loader are all library code).
- ~~**Durability/persistence failures log to stderr, not the observability stack.**~~ **✅ Shipped
  (ADR-021).** All 14 durability/persistence failure sites in
  `src/segment/{lifecycle,ingest,persistence}.rs` (WAL init/append/checkpoint/reset, manifest write,
  segment write/mmap fallback, source-store write/re-map/load, corrupt-segment-skip and torn-WAL-tail
  on recovery) now emit `EngineEvent::DurabilityFailure { op: DurabilityOp, detail, error }` instead of
  `eprintln!`. The server's observer logs each through `tracing` (`error!` for data-at-risk ops, `warn!`
  for display-only/benign ones — `DurabilityOp::is_data_at_risk`) and increments
  `durability_failures_total{op}` for alerting. Construction/recovery failures predate the observer, so
  they are buffered and replayed when `set_observer` is called.

---

## Current limitations

- **Not yet a hardened multi-node deployment.** The full shared-nothing multi-node stack is **built and
  oracle-proven** — sharding + content routing, the gRPC `ShardServer`/`RemoteShard` transport with coordinator
  dict shipping, a durable coordinator log, per-shard local durable segments (attach-and-mmap reopen), per-shard
  replication + peer recovery (in-process + gRPC), a durable openraft control plane (multi-process elections +
  leader failover + restart recovery), a per-shard translog (no-quiesce peer recovery + retention/finalize), and
  a rendezvous-hash shard→node allocator (ADR-027, 029, 031–042; per-ADR detail in
  [Implemented](#implemented-working-tested) above). But it is exercised **single-process / on localhost** by the
  oracles — not yet deployed and hardened across real machines. **Remaining for production multi-node** (all
  design-only; ADR-033 — no object store / cloud dependency anywhere): an **autoscaler** driving `rebalance` +
  the **live data-moving handoff** on a reassignment (serve-then-drop + epoch fencing), **auto-split**,
  **normalizer/vocab shipping**, **replicate-broad-to-all**, and **TLS/auth** on the (currently plaintext)
  transports — see Tier 3.
  **Correctness caveat (ADR-029/030/034):** cross-process dict identity is handled — the coordinator **ships**
  its frozen dict at connect (ADR-034) and the ADR-030 fingerprint handshake fails loud
  (`ShardError::DictMismatch`) if a *populated* server holds a divergent dict, so a diverged dict can never drop
  matches *silently*. The **normalizer** must still match on both sides (`default_vocab()` today — vocab shipping
  is the next step), and the transport is unauthenticated/plaintext. Treat the gRPC surface as correctness-safe,
  not yet a hardened multi-process deployment.
- **Empty default vocabulary.** `default_vocab()` ships no domain terms; vocabulary is supplied at
  runtime via the `Vocab` system or `NormalizerBuilder`. Auto-deriving it from the corpus is the
  NPMI-wiring item in Tier 2.
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
