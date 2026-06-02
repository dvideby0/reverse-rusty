# CLAUDE.md — agent context for Reverse Rusty

**Agent entry point — read this first.** It carries the safety rails (the correctness contract + the
invariants you must not break) and a router to the *one* doc for any task. It is deliberately **not** a
reference manual: status, performance numbers, dependency versions, and component design live in their
canonical docs, linked from the router below.

> Human/product overview → [`README.md`](README.md) · Full docs index + conventions →
> [`docs/README.md`](docs/README.md)

## What this project is

Reverse Rusty is a high-performance **reverse product-query matcher** for eBay-style listings.
Given millions of stored product-intent queries and an incoming listing title, it finds which
queries match ("percolation"). Written in Rust; a single-node engine whose **in-process
multi-shard core** (entity-anchor sharding + content routing) is built and oracle-proven
([ADR-027](docs/DECISIONS.md)). The **distributed multi-node layers** — a gRPC shard transport with dict
shipping (ADR-029/034), durable per-shard segments + coordinator log (ADR-031/032), replication + peer
recovery, a **shared-nothing** Raft control plane (no object store; ADR-033), an allocator, live handoff,
and an autoscaler (through ADR-045) — are built and oracle-proven **in-process / on localhost, but
experimental** (not yet hardened for real multi-machine deployment). The current **Cluster v1** correctness
item is **dynamic vocabulary** — absorbing new terms after the shared dict is frozen
([docs/STATUS.md](docs/STATUS.md) Tier 0). It gates candidates on **semantic signatures** (not raw terms), verifies
with **integer-only match plans**, quarantines broad queries, and supports frequent updates —
with a hard guarantee of **zero false negatives**. (Selective path ≈250× the spec target, a flat
~54 candidates/title, zero false negatives — see [`docs/performance/`](docs/performance/README.md).)

## The correctness contract (the thing that must never break)

> **Lossless signature cover:** if a title `T` could satisfy query `Q`'s positive semantics,
> then `T` must generate at least one signature that retrieves `Q` from the candidate index.

This guarantees zero false negatives. False-positive *candidates* are allowed (the exact
matcher rejects them). Verified by a randomized differential oracle in `tests/oracle.rs`; the formal
statement + construction proof obligation are in [`docs/design/README.md`](docs/design/README.md) §2.

## Critical invariants — do not violate these

- **Never gate on MUST_NOT features.** Gating on a negative lets an absent feature drop a
  real match. Forbidden features are checked *only* in exact verification. The signature
  optimizer literally cannot see them — this is enforced structurally.
- **No strings, regex, allocation, or AST interpretation on the match hot path.** All of that
  is pushed into compile time. The hot path is dumb, branch-predictable integer work.
- **No panicking `unwrap()` in library code.** Errors are typed (`ParseError`, `NormalizerError`).
- **Same normalizer for queries and titles.** The feature spaces must line up; any normalizer
  change must apply to both sides or correctness breaks.
- **Signatures are built only from required features and required any-of groups.** This is
  what makes the lossless cover provably correct.
- **Postings are append-only within a segment.** Local IDs are issued in order, so postings
  are sorted by construction — no per-insert sort/dedup.

## How to approach implementation work

The design docs describe *goals and constraints*, not mandated solutions. When picking up a
roadmap item, **research first, implement second:**

1. Identify what problem the item is really solving (e.g., "skip wasted segment probes").
2. Look at how peer systems and state-of-the-art literature solve that same problem — RocksDB,
   ClickHouse, Lucene, DuckDB, academic papers, whatever is relevant. Don't limit yourself to
   what the design doc suggested.
3. Evaluate the candidates against Reverse Rusty's specific constraints (the invariants above, the
   hot-path budget, the dependency philosophy).
4. Then implement the winner.

The design docs may suggest a specific approach (e.g., "xor/binary-fuse filters") but that's a
starting hypothesis, not a requirement. If research shows a better fit, use it. Example: the
design docs suggested binary fuse filters for segment skip-filtering, but research into RocksDB's
history showed that cache-line blocked bloom was a better match for our 1-memory-access budget
(see [`docs/DECISIONS.md`](docs/DECISIONS.md) ADR-011).

## Build, test, run

- **Language:** Rust 2021 edition, std-only core. **17 dependencies** — the lean core
  (`cargo build --no-default-features`) needs only `daachorse`, `memmap2`, `roaring`, `rayon`,
  `arc-swap` (snapshot reads), `serde`/`serde_json` (vocab/config/loader JSON); the rest are
  server/observability crates behind the default-on `server` feature ([ADR-028](docs/DECISIONS.md);
  lean build enforced by a `check.sh` lane). The optional `distributed` feature adds `tonic`/`prost`
  (via the `engine/grpc/` workspace member) for the gRPC `ShardServer` ([ADR-029](docs/DECISIONS.md)),
  `tokio-stream` for the peer-recovery segment stream ([ADR-036](docs/DECISIONS.md)), and `openraft`
  for the cluster-manager control-plane backend ([ADR-038](docs/DECISIONS.md)).
  **Versions are pinned in
  [`engine/Cargo.toml`](engine/Cargo.toml) — that file is authoritative; do not restate pins here** (it
  also documents the one default-feature exclusion — `prometheus`).
- **Build:** `cd engine && export CARGO_TARGET_DIR=/tmp/reverse-rusty-target && cargo build --release`
- **Test:** `cargo test --release` (oracle + parser + error-path + persistence + hardening + coverage-gap + pressure/stress suites). How-we-test guide → [`docs/testing.md`](docs/testing.md).
- **Lint/gate:** `engine/check.sh` (fmt + clippy + test + audit + deny) — the local gate; `--fast` runs fmt + clippy only. **CI runs this same script**, so a green `check.sh` locally means a green PR.
- **Git hooks:** `./setup-hooks.sh` once per clone — pre-commit runs the fast gate, pre-push runs the full gate (bypass with `--no-verify`; CI is the backstop).
- **CI:** GitHub Actions ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs `check.sh` + benchmarks on every PR and push to `main`; the 10M soak is on-demand (`workflow_dispatch` → `run_soak`). Rationale → [`docs/DECISIONS.md`](docs/DECISIONS.md) ADR-024.
- **Toolchain:** pinned in [`engine/rust-toolchain.toml`](engine/rust-toolchain.toml) (rustc + rustfmt + clippy) so local and CI builds match.
- **Demo:** `cargo run --release --bin demo` (worked example end-to-end with explain output)
- **Benchmark:** `cargo run --release --bin bench -- [num_queries] [num_titles] [broad_frac] [skew] [seed]` (run-and-print; regression gate → [`docs/performance/benchmark-results.txt`](docs/performance/benchmark-results.txt))
- **Server:** `cargo run --release --bin server -- [--port 9200] [--data-dir ./data] [--load-file queries.csv] ...`
  (all flags + endpoints: [`docs/reference/api.md`](docs/reference/api.md))
- **Shard server (gRPC):** `cargo run --release --bin shardserver --features distributed -- [127.0.0.1:50051]`
  — serves one shard's `ShardService`. The `distributed` feature (gRPC `ShardServer`/`RemoteShard`) is off
  by default; its differential oracle runs under `cargo test --features distributed` (and the full `check.sh`).
- **Control server (gRPC):** `cargo run --release --bin controlserver --features distributed -- <NODE_ID> <BIND_ADDR> [--peer ID=URL ...] [--bootstrap]`
  — a cluster-manager node serving the openraft `ControlService` (ADR-038); `--bootstrap` forms the initial cluster.
- **Build profile:** LTO, codegen-units=1, opt-level=3, panic=abort

## Architecture at a glance

Two phases, sharply separated (full diagram: [`docs/design/README.md`](docs/design/README.md) §1):

```
COMPILE TIME (per stored query, off hot path — allowed to be expensive)
  query DSL → parse → AST → normalize → CompiledQuery
    → signature-cover optimizer → candidate_signatures (lossless cover)
    → cost classification (A/B/C/D) → append to segment

MATCH TIME (per incoming title, the hot path — allocation-free)
  raw title → normalize → dense feature IDs → title signatures
    → probe candidate index → union of candidate IDs
    → integer-only exact verification → emit matches
```

## Module map

| File | Purpose | Design doc |
|---|---|---|
| `src/lib.rs` | Library root, public API re-exports | — |
| `src/dsl.rs` | Query DSL parser → AST (compile-time only) | [normalization.md](docs/design/normalization.md) §1 |
| `src/normalize.rs` | Shared query/title normalizer (daachorse automaton) + `NormalizerBuilder` | [normalization.md](docs/design/normalization.md) §2–4 |
| `src/dict.rs` | Feature dictionary, frequency tracking, 64-bit common mask, synthetic-ID hashing for out-of-dict terms (dynamic vocab, ADR-046) | [normalization.md](docs/design/normalization.md) §5 |
| `src/compile.rs` | Signature-cover optimizer + cost classes A/B/C/D + read-only compile path for explain + `anchor_plan` (pre-hash anchor groups — the placement SSOT for clustering) | [matching.md](docs/design/matching.md) §1; ADR-027 |
| `src/config.rs` | `EngineConfig` — runtime-tunable knobs for compaction, flush, merge scoring, and the broad-lane batch evaluator (`Serialize`; dynamic subset updatable at runtime via `/_settings`) | ADR-022, ADR-026 |
| `src/filter.rs` | Per-segment anchor filter (cache-line blocked bloom, 512-bit blocks) | [ingestion-and-updates.md](docs/design/ingestion-and-updates.md) §6 |
| `src/index.rs` | Candidate index: sig key → posting list (inline/Vec/Roaring) | [matching.md](docs/design/matching.md) §2 |
| `src/exact.rs` | Integer-only SoA exact verification (common-mask gate) + columnar batch verification (`eval_batch`, the bitmap transpose of `verify`) + pure-anchor derivation | [matching.md](docs/design/matching.md) §3–4 |
| `src/events.rs` | `EngineEvent` (incl. `DurabilityFailure`/`DurabilityOp`), `EngineMetrics`, `CompactionTrigger`, `SegmentInfo`/`SegmentKind` (per-segment introspection) — zero-dependency observability | ADR-021, ADR-023 |
| `src/storage.rs` | Mmap'd segment file format: frozen hash tables, `MmapSegment` (incl. `class_counts` over the persisted class bytes), `BaseSegment`, engine manifest, Dict serialization, query source persistence (`sources.dat`), and the coordinator `ClusterManifest` v2 — per-shard segment registry + `next_seg_id`s + dict, the cluster's atomic commit point | ADR-012, ADR-014, ADR-031, ADR-032 |
| `src/wal.rs` | Write-ahead log: append-only CRC-framed entries, crash recovery replay | ADR-013 |
| `src/segment.rs` + `src/segment/` | LSM engine (module). Root holds the shared type *defs* (`Engine`, `Segment`, `BaseSegment`, `EngineSnapshot`, `BatchMatchOptions`/`BroadStrategy`, report types); `impl` blocks split into submodules — `seg`/`base`/`snapshot` (the data/read types) and `lifecycle`/`ingest`/`compaction`/`matching`/`persistence`/`metrics` (the `Engine` controller), plus `broad_batch` (the columnar broad-lane batch evaluator behind `match_titles_batch`). Same responsibilities: memtable + flush + bulk_ingest + tombstones + compaction + auto-trigger policy + persistence. A **segments-only durable** mode (cluster shards, ADR-032): `with_shared_segments_only` (data_dir, no WAL, no own manifest) + `open_shared_segments` (attach an explicit mmap'd file list against the shared dict, fail-loud) + `reseal_tombstoned_segments` (bake base-segment tombstones to disk at checkpoint). Submodule-internal helpers are `pub(in crate::segment)`. A `#[cfg(test)]` `wal_failure_tests` submodule holds WAL-failure integration tests for the write path. | [ingestion-and-updates.md](docs/design/ingestion-and-updates.md); broad lane → [matching.md](docs/design/matching.md) §4 |
| `src/cluster.rs` | **Multi-shard core — module root.** Submodule decls + public re-exports + the module-level correctness model (ONE shared frozen dict ⇒ globally-consistent `FeatureId`s ⇒ lossless cross-shard cover). **Cluster v1** = this in-process core + durable reopen + **dynamic vocabulary** (absorbing new terms after the dict is frozen — STATUS Tier 0, → ADR-046); the `distributed`-gated multi-node layers are built but **experimental / localhost-proven** (not yet hardened for real multi-machine deployment). The cluster is **shared-nothing** (local segments + coordinator log + per-shard translog + replicas + Raft control plane — no object store). The areas below split what was one file per concern; everything behind the off-by-default `distributed` feature is so noted, and the in-process/RF=1 default path is byte-identical. | [clustering-and-scaling.md](docs/design/clustering-and-scaling.md) §3/§7/§10; ADR-027/029/031–045 (ADR-033 shared-nothing) |
| `src/cluster/coordinator.rs` + `coordinator/` | **`ClusterEngine`** — placement (writes) + content routing (reads) + cross-shard merge. Root holds the type *defs* (`ClusterConfig`/`AddOutcome`/`ClusterEngine`) + free helpers; the `impl` splits into `lifecycle` (build/from_parts/open/checkpoint), `ingest` (add/remove + the shared `apply`/`replay_apply` funnel + bulk), `matching` (route/percolate/counts), `control_plane` (membership/assignment/rebalance/observer), `autoscale` (the autoscaler driver — `tick`/`on_node_*`), `vocab` (the runtime vocabulary change — `set_vocab` blue/green rebuild + re-placement + `learn_and_apply`, ADR-046 mech 2), and the gated `distributed` (gRPC builders + peer recovery + `execute_handoff`). | [clustering-and-scaling.md](docs/design/clustering-and-scaling.md) §3; ADR-027 |
| `src/cluster/shard.rs`, `replica.rs` + `replica/`, `handoff.rs` | **The `Shard` seam + composites.** `shard.rs` = the local↔remote `trait Shard` + `LocalShard` (wraps an `Engine` + `ArcSwap<EngineSnapshot>`) + `ShardError` + `apply_mutation`. `replica/` = `ReplicatedShard` (one position's primary + N replicas — read failover to in-sync only, primary-authoritative writes, `peer_recover`/`catch_up_replica`; `shard_impl` holds the `Shard` impl, `test_support`/`tests` the units). `handoff.rs` (gated) = `HandoffShard` (runtime-swappable backing — `ArcSwap<Box<dyn Shard>>` + generation, serve-then-drop). | ADR-027/035/043 |
| `src/cluster/control.rs`, `control_raft.rs` + `control_raft/`, `control_store.rs`, `control_server.rs` | **Control plane** — holds the cluster-state document ONLY (never query mutations). `control.rs` = the dependency-free `trait ControlPlane` + `ClusterState` + `InMemoryControlPlane` (lean; the default ⇒ byte-identical). Gated: `control_raft/` = the openraft `RaftControlPlane` (`log_store`/`state_machine`/`network`/`builders`, reusing the ONE `control::apply` funnel ⇒ live ≡ replay), `control_store` = its durable CRC-framed Raft log + vote/committed files, `control_server` = the gRPC `ControlService` (opaque serde envelope). | ADR-037/038/041 |
| `src/cluster/clog.rs`, `translog.rs` | **Durable query logs** (CRC-framed `FileClusterLog`/`NullClusterLog` + `ClusterMutation`/`LogPos`): `clog` = the coordinator's ordered mutation log (the `ClusterEngine::{open,checkpoint}` source of truth, log-first/fail-closed); `translog` = the per-shard ES-style translog (recovery streams segments at `P` then replays the tail > `P`, so peer recovery need not quiesce writes). | ADR-031/039/040 |
| `src/cluster/remote.rs`, `server.rs` + `server/`, `proto.rs` | **gRPC transport** (gated). `remote.rs` = the `RemoteShard` client (sync→async `block_on` bridge; `connect_and_adopt` ships the frozen dict, ADR-034). `server.rs` = `ShardServer` (`server/service` holds the `ShardService` RPC handlers — percolate/ingest/insert/delete/flush + `AdoptDict`/`FetchSegments`/`RecoverFrom`/`FetchTranslog`/`RetentionLease`/`Fence`). `proto` = mappers over the generated `grpc/` crate. | ADR-029/034/036/044 |
| `src/cluster/ring.rs`, `allocator.rs`, `autoscale.rs` | **Placement primitives + elasticity policy.** `HashRing` = the consistent-hash ring over anchor `FeatureId` (entity-anchor sharding ⇒ ~2–5 shard fan-out, never all N). `allocator` = the rendezvous (HRW) shard→node planner (balanced, deterministic, ≈1/N minimal-movement; drives `register_node`/`rebalance`). `autoscale` = the pure policy (`evaluate`: membership/skew/corpus → `ScalingAction`s; split/handoff advisory) driven by `ClusterEngine::tick` (the driver lives in `coordinator/autoscale.rs`). | [clustering-and-scaling.md](docs/design/clustering-and-scaling.md) §3/§8; ADR-042, ADR-045 |
| `grpc/` (member `reverse-rusty-shard-proto`) | Workspace member holding the generated gRPC `ShardService` (protobuf messages + tonic client/server). Built only under `distributed`; codegen via pure-Rust `protox` in `build.rs` (no system `protoc`), nothing checked in. | ADR-029 |
| `src/explain.rs` | Debug/explain tooling (first-class, not bolt-on) + structured `ExplainDetail` for API | [matching.md](docs/design/matching.md) §6 |
| `src/gen.rs` | Synthetic data generator (deterministic, seeded) | — |
| `src/vocab.rs` | Runtime vocabulary learning from query any-of groups, `Vocab` struct, JSON persistence | ADR-015 |
| `src/error.rs` | Typed `ParseError` with `ParseErrorKind` enum | — |
| `src/loader.rs` | Query file loader (CSV + JSONL auto-detection) | — |
| `src/util.rs` | FNV-1a hash (stable across runs), FastMap alias | — |
| `tests/oracle.rs` | Differential correctness oracle (brute force vs engine; per-title AND batch path) | — |
| `tests/broad_batch.rs` | Broad-lane batch≡scalar equivalence matrix (the load-bearing batch correctness deliverable) | [matching.md](docs/design/matching.md) §4 |
| `tests/cluster_oracle.rs` | Multi-shard differential oracle: cluster ≡ single-node ≡ brute, K∈{1,3,8,16} × broad on/off, all placement classes + fan-out asserted | ADR-027 |
| `tests/cluster_grpc_oracle.rs` | gRPC differential oracle (feature `distributed`): K real `ShardServer`s on localhost, cluster-over-gRPC ≡ single-node ≡ brute + live add/percolate/remove RPCs; **dict shipping (ADR-034)** — a dict-less-`pending`-servers variant + the divergent-dict-refused-on-a-populated-server guard; **replication + peer recovery (ADR-035/036)** — K×RF durable servers via `connect_replicated` ≡ brute, primary-stop **failover**, and fresh-node **peer recovery** over `FetchSegments`/`RecoverFrom`; **no-quiesce recovery (ADR-039)** — `grpc_peer_recovery_without_quiescing`: snapshot at `P`, writes land after `P`, the `FetchTranslog` tail catches them up, recovered ≡ live source ≡ brute over the final live set (the in-process + self-restart analogues are `replica.rs` unit tests) | ADR-029, ADR-034, ADR-036, ADR-039 |
| `tests/cluster_durability_oracle.rs` | Cluster durability oracle: a `data_dir` cluster rebuilt from manifest + per-shard segments + log ≡ pre-crash ≡ brute, K∈{1,3,8} × broad on/off, + checkpoint, torn-tail recovery, append-fails-closed, two-backend differential, fsync parity, fail-loud guards. Step-3b additions: attach-with-the-log-deleted, the checkpoint-after-removing-a-build-time-query bug-catcher, orphan-segment-ignored-and-GC'd, corrupt-segment-fails-loud | ADR-031, ADR-032 |
| `tests/cluster_control_raft_oracle.rs` | openraft control-plane oracle (feature `distributed`): a 3-node in-process Raft cluster (real elections + replication + quorum commit) converges to the in-memory backend's document (voters/nodes/assignments/model — NOT the epoch, which openraft's Blank/Membership commits perturb); a follower `propose` → `ForwardToLeader`; `change_membership` routes to Raft; and over real gRPC `ControlService` servers on localhost, **survive-the-leader-being-killed** (re-elect from quorum, committed doc persists, fresh write commits) | ADR-038 |
| `tests/cluster_autoscale_oracle.rs` | Autoscaler oracle (ADR-045): over a real in-process cluster, `tick` commits the same shard→node map a manual `rebalance` does; **`percolate` byte-identical before/after a tick** (zero false negatives); a second tick commits nothing (epoch-invariant); a disabled config is a no-op; a corpus-over-threshold `RecommendSplit` advisory mutates nothing | ADR-045 |
| `tests/error_paths.rs` | API error handling regression tests | — |
| `tests/persistence.rs` | Persistence tests: segment round-trip, WAL recovery, mmap compaction | — |
| `tests/hardening_fixes.rs` | Integration tests: vocab epoch, fallible deser, reverse-index delete | — |
| `tests/coverage_gaps.rs` | Regression tests closing specific coverage gaps | — |
| `tests/stress.rs` | Pressure/soak suite: mixed read/write/delete churn, par==seq under mutation; one `#[ignore]`d 10M-query soak | [testing.md](docs/testing.md) |
| `src/bin/demo.rs` | Worked example end-to-end | — |
| `src/bin/clusterdemo.rs` | Cluster worked example: per-class placement + content-routed fan-out | ADR-027 |
| `src/bin/shardserver.rs` | Deployable shard node: serves `ShardService` over gRPC (feature `distributed`); `--pending` starts dict-less (ADR-034), `--data-dir` makes it **durable** — a recovering/replica node that can serve/accept peer recovery (ADR-036) | ADR-029, ADR-036 |
| `src/bin/controlserver.rs` | Deployable cluster-manager node: serves the openraft `ControlService` over gRPC (feature `distributed`); `--bootstrap` forms the initial cluster from `--peer ID=URL` members; `--data-dir` makes it **durable** (ADR-041 — persists its Raft log/vote/committed/snapshot, resumes its committed cluster-state doc on restart) | ADR-038, ADR-041 |
| `src/bin/bench.rs` | Benchmark harness | — |
| `src/bin/learn.rs` | Corpus feature learner (NPMI) | [corpus-feature-learning.md](docs/research/corpus-feature-learning.md) |
| `src/bin/norm.rs` | Title introspection tool | — |
| `src/bin/segbench.rs` | Read-amplification vs segment count harness | — |
| `src/bin/snapbench.rs` | Snapshot read/publish concurrency benchmark | ADR-016 |
| `src/bin/server.rs` | HTTP server (axum) — ES-style REST API (incl. batch `/_mpercolate`), snapshot-based concurrency, structured logging, Prometheus metrics, graceful shutdown. Endpoint reference: [`docs/reference/api.md`](docs/reference/api.md) | ADR-014, ADR-016, ADR-021, ADR-022, ADR-023, ADR-026 |

*(All test files above are committed and run by `cargo test --release`. `tests/stress.rs`'s one 10M-query soak is `#[ignore]`d — run it explicitly or via the CI `run_soak` dispatch input. How-we-test guide: [`docs/testing.md`](docs/testing.md).)*

## Where to go — find the ONE doc for your task

| Your task / question | Go to (one hop) |
|---|---|
| Understand the whole system fast | [`docs/design/README.md`](docs/design/README.md) §1 (mental model) |
| "Will my change cause a false negative?" | [`docs/design/README.md`](docs/design/README.md) §2 + the invariants above |
| Edit the DSL parser / normalizer / dictionary | [`docs/design/normalization.md`](docs/design/normalization.md) (+ `src/dsl.rs`, `normalize.rs`, `dict.rs`) |
| Edit the signature optimizer / candidate index / exact matcher | [`docs/design/matching.md`](docs/design/matching.md) (+ `src/compile.rs`, `index.rs`, `exact.rs`) |
| Edit the broad lane (class C) | [`docs/design/matching.md`](docs/design/matching.md) §4; evidence: [`docs/performance/results.md`](docs/performance/results.md) §9 |
| Edit segments / flush / compaction / WAL / mmap | [`docs/design/ingestion-and-updates.md`](docs/design/ingestion-and-updates.md) (+ `src/segment/`, `storage.rs`, `wal.rs`) |
| Edit the HTTP server / REST endpoints | [`docs/reference/api.md`](docs/reference/api.md) (+ `src/bin/server.rs`) |
| Query DSL syntax / vocabulary | [`docs/reference/dsl.md`](docs/reference/dsl.md) |
| Add/understand a config knob or `/_settings` | [`docs/DECISIONS.md`](docs/DECISIONS.md) ADR-022; `src/config.rs` |
| "Is X built or just designed?" / what to work on next | [`docs/STATUS.md`](docs/STATUS.md) |
| "Why was it done this way?" / "why was X NOT built?" | [`docs/DECISIONS.md`](docs/DECISIONS.md) (ADR index; declined → ADR-019) |
| Performance numbers / 100M extrapolation | [`docs/performance/results.md`](docs/performance/results.md); regression gate: `benchmark-results.txt` INVARIANTS |
| Run/change tests, benchmarks, pressure tests, hooks, or CI | [`docs/testing.md`](docs/testing.md) (gate: `engine/check.sh`; CI: `.github/workflows/ci.yml`) |
| Clustering / sharding / scale-out | [`docs/design/clustering-and-scaling.md`](docs/design/clustering-and-scaling.md); **shared-nothing** model (ADR-033, no object store). **Cluster v1** = in-process core + durable reopen + **dynamic vocabulary** (Tier 0, → ADR-046 — the immediate priority). The **distributed layers, built but experimental / localhost-proven**: gRPC transport + dict shipping + replication + Raft control plane (durable, restart-recoverable) + **per-shard translog / no-quiesce peer recovery + retention/finalize** + **shard→node allocator** + **live data-moving handoff** (swappable backing + cross-node move + write fence) + **autoscaler** (membership/skew → `rebalance`; split/handoff advisories) → `src/cluster/`, `engine/grpc/`, [`docs/DECISIONS.md`](docs/DECISIONS.md) ADR-027 + ADR-029 + ADR-034 + ADR-035/036 + ADR-037/038 + ADR-039/040/041 + ADR-042/043/044 + ADR-045 |
| Prior art (Lucene / ES / Tantivy) | [`docs/research/prior-art.md`](docs/research/prior-art.md) |
| Dependency versions / why a crate | [`engine/Cargo.toml`](engine/Cargo.toml) |
| Full docs index + where-new-info-goes rules | [`docs/README.md`](docs/README.md) |

## Conventions

**Code conventions:**
- SoA (struct-of-arrays) layout for cache efficiency in exact match.
- Segment-local `u32` IDs on hot path; global `u64` IDs resolved only on confirmed match.
- Typed errors (`ParseError { kind, pos }`), never `unwrap()` in library code.
- **Library code never writes to stderr.** Operational failures surface as typed errors, or — for
  best-effort/degraded paths with no caller — as an `EngineEvent` the observer turns into logs +
  metrics (see `DurabilityFailure`, ADR-021). `eprintln!`/`println!` are for CLI bins and
  test/bench diagnostics only, never `src/*.rs` production paths.
- Deterministic data generation (seeded PRNG) so benchmarks and oracle are reproducible.
- Three-tier adaptive postings: inline (≤8) → Vec (≤256) → RoaringBitmap (>256).

**Where new information goes** (full rules + SSOT registry in [`docs/README.md`](docs/README.md)):
decision → `docs/DECISIONS.md` (new ADR, never renumber); component design → `docs/design/<topic>.md`;
"is it built / what's next" → `docs/STATUS.md`; benchmark numbers → `docs/performance/`; dependency
version → `engine/Cargo.toml`; new `src/` file → update the module map above.

## When modifying this file

This file is the *safety + orientation* layer, not a mirror of the docs. So:
- **Inline here (safety — an agent must not have to hop to stay correct):** the correctness-contract
  sentence and the critical-invariants list. Keep them byte-identical to
  [`docs/design/README.md`](docs/design/README.md) §2.
- **Never inline here (link the one canonical home instead):** performance numbers (→ `docs/performance/`),
  dependency versions (→ `engine/Cargo.toml`), full implemented/roadmap status (→ `docs/STATUS.md`),
  per-component design (→ `docs/design/`), decision rationale (→ `docs/DECISIONS.md` by ADR number).
- **Update the module map** when files are added/removed/renamed.
- If you're about to paste a number, a version, or a paragraph that already lives in one of those
  homes, write a one-line pointer instead.
