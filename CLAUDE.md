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
an autoscaler, and remote partial-apply repair (through ADR-047) — are built and oracle-proven **in-process
/ on localhost, but experimental** (not yet hardened for real multi-machine deployment). **Cluster v1** — the in-process
core + durable reopen + **dynamic vocabulary** (absorbing new terms after the shared dict is frozen,
ADR-046) — is **built and oracle-proven**, zero false negatives including across reopen
([docs/STATUS.md](docs/STATUS.md) Tier 0). It gates candidates on **semantic signatures** (not raw terms), verifies
with **integer-only match plans**, quarantines broad queries, and supports frequent updates —
with a hard guarantee of **zero false negatives**. (Selective path ≈250× the spec target, a flat
~54 candidates/title, zero false negatives — see [`docs/performance/`](docs/performance/README.md).)

## The correctness contract (the thing that must never break)

> **Lossless signature cover:** if a title `T` *could* satisfy query `Q`'s positive semantics, then
> `T` must generate at least one signature that retrieves `Q` from the candidate index.

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

- **Language:** Rust 2021 edition, std-only core. **A deliberately lean dependency tree** — the lean core
  (`cargo build --no-default-features`) needs only seven crates: `daachorse`, `memmap2`, `roaring`, `rayon`,
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
- **Lint/gate:** `engine/check.sh` (fmt + clippy + test + audit + deny) — the local gate; `--fast` runs fmt + clippy only. **CI runs this same script**, so a green `check.sh` locally means a green PR. It also prints a non-failing >600-line file-size advisory (a refactor nudge that never blocks the gate).
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
| `src/normalize.rs` | Shared query/title normalizer (daachorse automaton) + `NormalizerBuilder` + the configurable byte-cleaning `PunctClass` table (`Split`/`Fold`/`Keep`/`Marker`; declare `'`/`-` as `Fold` to collapse `O'Brien`/`O-Brien`/`OBrien`, ADR-058) + the configurable **number-context word list** (default `["pop"]` demotes an adjacent number to a generic term; empty = parity-mode position-insensitive number typing, ADR-069) + the `Side`-asymmetric **alias-mode phrases** (`PhraseMode::Alias`: query-side collapse / title-side additive) + overlapping automaton + `match_features_dual` producing the two title views (`N(T)`/`P(T)`) for multi-word aliases (ADR-061) | [normalization.md](docs/design/normalization.md) §2–4 |
| `src/dict.rs` | Feature dictionary, frequency tracking, 64-bit common mask, synthetic-ID hashing for out-of-dict terms (dynamic vocab, ADR-046), transient `EquivMap` for compile-time equivalence expansion (ADR-054) | [normalization.md](docs/design/normalization.md) §5 |
| `src/tagdict.rs` | Per-query metadata **tag** dictionary — interns `(key,value)` tags to dense `TagId`s (a space disjoint from `FeatureId`), with the same synthetic-ID escape hatch as `dict.rs`. Tag strings die here; the filter is integers-only (filtered percolation, ADR-049). In the cluster, ONE frozen `TagDict` is shared into every shard like the frozen `Dict` (`fingerprint`/`mark_finalized`/`get_or_synthetic` for the cross-shard apply path, ADR-055) | [matching.md](docs/design/matching.md) §5.1 |
| `src/compile.rs` | Signature-cover optimizer + cost classes A/B/C/D + read-only compile path for explain + `anchor_plan` (pre-hash anchor groups — the placement SSOT for clustering; class D = ONE empty broad group, the **universal signature** behind the opt-in always-candidate lane, ADR-068) + `Extracted::expand_equivalences` (FN-safe alias expansion: required→any-of, ADR-054) | [matching.md](docs/design/matching.md) §1; ADR-027 |
| `src/config.rs` | `EngineConfig` — runtime-tunable knobs for compaction, flush, merge scoring, and the broad-lane batch evaluator (`Serialize`; dynamic subset updatable at runtime via `/_settings`) | ADR-022, ADR-026 |
| `src/filter.rs` | Per-segment anchor filter (cache-line blocked bloom, 512-bit blocks) | [ingestion-and-updates.md](docs/design/ingestion-and-updates.md) §6 |
| `src/index.rs` | Candidate index: sig key → posting list (inline/Vec/Roaring) | [matching.md](docs/design/matching.md) §2 |
| `src/exact.rs` | Integer-only SoA exact verification (common-mask gate) + columnar batch verification (`eval_batch`, the bitmap transpose of `verify`) + pure-anchor derivation + the SoA **tag column** & request `TagPredicate` (verify-stage tag filter, never gates — ADR-049) + the **`TitleView`** two title-feature views (positive superset `P(T)` for required/any-of, negative canonical `N(T)` for forbidden — multi-word aliases, ADR-061) | [matching.md](docs/design/matching.md) §3–5 |
| `src/rank.rs` | Post-match **ranking** (ADR-059): `RankSpec`/`CompiledRankSpec` + the additive `score` (`Σ request-boosts + priority-tag value`). Optional out-of-core layer over the final id set — reorders + paginates only, never gates; consumed by `EngineSnapshot::rank` + the percolate handlers | [matching.md](docs/design/matching.md) §5.4 |
| `src/events.rs` | `EngineEvent` (incl. `DurabilityFailure`/`DurabilityOp`), `EngineMetrics`, `CompactionTrigger`, `SegmentInfo`/`SegmentKind` (per-segment introspection) — zero-dependency observability | ADR-021, ADR-023 |
| `src/storage.rs` + `storage/` | Persistence + on-disk formats (module). Root holds the shared binary primitives (CRC-32, atomic rename, LE scalar read/write) + public re-exports; submodules split by concern: `segment` (the `.seg` format **v3** / **v4** — v4 is layout-identical, written only for a segment holding class-D always-candidates as a rollback fence (ADR-068); `write_segment` + the mmap-backed `MmapSegment` read view, incl. `class_counts`, frozen hash tables, and the per-query **tag column** (ADR-049); v1/v2 read back untagged), `dict` (feature-dict (de)serialization — `RDCT` **v1**, magic+version header, strict `kind_from_tag` decode, fail-loud + legacy header-less read, ADR-057), `tagdict` (tag-dict (de)serialization — `RTGD` **v1**, same versioning; empty blob = no tags), `manifest` (engine `Manifest` **v3**/**v4** — incl. the per-segment dead-locals bitmaps + WAL-seq watermark making base-segment tombstones durable at the commit point, ADR-066; v4 = layout-identical, written only while a class-D-bearing segment is registered — the loud rollback fence, ADR-068 — + coordinator `ClusterManifest` **v4** — per-shard segment registry + `next_seg_id`s + dict + serialized vocab + serialized tag dict, the cluster's atomic commit point), `sources` (`SourceStore` query-source persistence, `sources.dat`) | ADR-012, ADR-014, ADR-031, ADR-032, ADR-046, ADR-049, ADR-057, ADR-066 |
| `src/wal.rs` | Write-ahead log: append-only CRC-framed entries (**v3**: deletes log one address-free `DeleteByLogical` frame, replayed via the live path's funnel — compaction-renumber-immune, ADR-066; **v4**: the `Upsert` frame — atomic replace-by-id, both halves recover or neither, ADR-067; **v5**: `InsertClassD`/`UpsertClassD` op markers — accepted-under-the-lane frames replay stored while legacy logged-before-classifying frames keep the old reject gate, ADR-068), crash recovery replay | ADR-013, ADR-066, ADR-067 |
| `src/segment.rs` + `src/segment/` | LSM engine (module). Root holds the shared type *defs* (`Engine`, `Segment`, `BaseSegment`, `EngineSnapshot`, `BatchMatchOptions`/`BroadStrategy`, report types); `impl` blocks split into submodules — `seg`/`base`/`snapshot` (the data/read types) and `lifecycle`/`ingest`/`compaction`/`matching`/`persistence`/`metrics` (the `Engine` controller), plus `broad_batch` (the columnar broad-lane batch evaluator behind `match_titles_batch`). Same responsibilities: memtable + flush + bulk_ingest + tombstones + compaction + auto-trigger policy + persistence. A **segments-only durable** mode (cluster shards, ADR-032): `with_shared_segments_only` (data_dir, no WAL, no own manifest) + `open_shared_segments` (attach an explicit mmap'd file list against the shared dict, fail-loud) + `reseal_tombstoned_segments` (bake base-segment tombstones to disk at checkpoint). Submodule-internal helpers are `pub(in crate::segment)`. A `#[cfg(test)]` `wal_failure_tests` submodule holds WAL-failure integration tests for the write path. | [ingestion-and-updates.md](docs/design/ingestion-and-updates.md); broad lane → [matching.md](docs/design/matching.md) §4 |
| `src/cluster.rs` | **Multi-shard core — module root.** Submodule decls + public re-exports + the module-level correctness model (ONE shared frozen dict ⇒ globally-consistent `FeatureId`s ⇒ lossless cross-shard cover). **Cluster v1** = this in-process core + durable reopen + **dynamic vocabulary** (new terms absorbed after the dict is frozen — **built + oracle-proven**, STATUS Tier 0, ADR-046); the `distributed`-gated multi-node layers are built but **experimental / localhost-proven** (not yet hardened for real multi-machine deployment). The cluster is **shared-nothing** (local segments + coordinator log + per-shard translog + replicas + Raft control plane — no object store). The areas below split what was one file per concern; everything behind the off-by-default `distributed` feature is so noted, and the in-process/RF=1 default path is byte-identical. | [clustering-and-scaling.md](docs/design/clustering-and-scaling.md) §3/§7/§10; ADR-027/029/031–045 (ADR-033 shared-nothing) |
| `src/cluster/coordinator.rs` + `coordinator/` | **`ClusterEngine`** — placement (writes) + content routing (reads) + cross-shard merge. Holds the ONE frozen `Dict` + `TagDict` shared into every shard (ADR-055). Root holds the type *defs* (`ClusterConfig`/`AddOutcome`/`ClusterEngine`) + free helpers; the `impl` splits into `lifecycle` (build/`build_with_tags`/from_parts/open/checkpoint), `ingest` (add/`add_query_with_tags`/remove + the shared `apply`/`replay_apply` funnel + `ingest`/`ingest_with_tags`), `matching` (route/percolate/`percolate_filtered`/`compile_tag_predicate`/counts), `control_plane` (membership/assignment/rebalance/observer), `autoscale` (the autoscaler driver — `tick`/`on_node_*`), `vocab` (the runtime vocabulary change — `set_vocab` blue/green rebuild + re-placement + `learn_and_apply`, ADR-046 mech 2; refuses a tagged cluster, ADR-055), and the gated `distributed` (gRPC builders that ship the dict + tag dict + peer recovery + `execute_handoff`). | [clustering-and-scaling.md](docs/design/clustering-and-scaling.md) §3; ADR-027/055 |
| `src/cluster/shard.rs`, `replica.rs` + `replica/`, `handoff.rs` | **The `Shard` seam + composites.** `shard.rs` = the local↔remote `trait Shard` + `LocalShard` (wraps an `Engine` + `ArcSwap<EngineSnapshot>`) + `ShardError` + `apply_mutation`. `replica/` = `ReplicatedShard` (one position's primary + N replicas — read failover to in-sync only, primary-authoritative writes, `peer_recover`/`catch_up_replica`; `shard_impl` holds the `Shard` impl, `test_support`/`tests` the units). `handoff.rs` (gated) = `HandoffShard` (runtime-swappable backing — `ArcSwap<Box<dyn Shard>>` + generation, serve-then-drop). | ADR-027/035/043 |
| `src/cluster/control.rs`, `control_raft.rs` + `control_raft/`, `control_store.rs`, `control_server.rs` | **Control plane** — holds the cluster-state document ONLY (never query mutations). `control.rs` = the dependency-free `trait ControlPlane` + `ClusterState` + `InMemoryControlPlane` (lean; the default ⇒ byte-identical). Gated: `control_raft/` = the openraft `RaftControlPlane` (`log_store`/`state_machine`/`network`/`builders`, reusing the ONE `control::apply` funnel ⇒ live ≡ replay), `control_store` = its durable CRC-framed Raft log + vote/committed files, `control_server` = the gRPC `ControlService` (opaque serde envelope). | ADR-037/038/041 |
| `src/cluster/clog.rs`, `translog.rs` | **Durable query logs** (CRC-framed `FileClusterLog`/`NullClusterLog` + `ClusterMutation`/`LogPos`): `clog` = the coordinator's ordered mutation log (the `ClusterEngine::{open,checkpoint}` source of truth, log-first/fail-closed); `translog` = the per-shard ES-style translog (recovery streams segments at `P` then replays the tail > `P`, so peer recovery need not quiesce writes). | ADR-031/039/040 |
| `src/cluster/remote.rs`, `server.rs` + `server/`, `proto.rs`, `security.rs` | **gRPC transport** (gated). `remote.rs` = the `RemoteShard` client (sync→async `block_on` bridge; `connect_and_adopt` ships the frozen dict, ADR-034; `_with_security` variants ride the mesh TLS+token). `server.rs` = `ShardServer` (`server/service` holds the `ShardService` RPC handlers — percolate/ingest/insert/delete/flush + `AdoptDict`/`FetchSegments`/`RecoverFrom`/`FetchTranslog`/`RetentionLease`/`Fence`; `with_security` applies TLS + the token verifier). `proto` = mappers over the generated `grpc/` crate. `security.rs` = the ADR-071 mesh-security module shared by BOTH transports/sides: `ServerSecurity`/`ClientSecurity` shapes, `resolve_mesh_token` (ADR-062 rules), the token inject/verify interceptors (constant-time, default-deny over the whole service), `configure_endpoint` (client TLS). | ADR-029/034/036/044, ADR-071 |
| `src/cluster/ring.rs`, `allocator.rs`, `autoscale.rs` | **Placement primitives + elasticity policy.** `HashRing` = the consistent-hash ring over anchor `FeatureId` (entity-anchor sharding ⇒ ~2–5 shard fan-out, never all N). `allocator` = the rendezvous (HRW) shard→node planner (balanced, deterministic, ≈1/N minimal-movement; drives `register_node`/`rebalance`). `autoscale` = the pure policy (`evaluate`: membership/skew/corpus → `ScalingAction`s; split/handoff advisory) driven by `ClusterEngine::tick` (the driver lives in `coordinator/autoscale.rs`). | [clustering-and-scaling.md](docs/design/clustering-and-scaling.md) §3/§8; ADR-042, ADR-045 |
| `grpc/` (member `reverse-rusty-shard-proto`) | Workspace member holding the generated gRPC `ShardService` (protobuf messages + tonic client/server). Built only under `distributed`; codegen via pure-Rust `protox` in `build.rs` (no system `protoc`), nothing checked in. | ADR-029 |
| `src/explain.rs` | Debug/explain tooling (first-class, not bolt-on) + structured `ExplainDetail` for API | [matching.md](docs/design/matching.md) §6 |
| `src/gen.rs` | Synthetic data generator (deterministic, seeded) + the opt-in **messy mode** (`messify_title`/`messify_query`/`messify_dataset` — seeded adversarial surface noise: case/diacritics/whitespace runs/punctuation/unicode junk/OOV tokens; separate functions, so every clean corpus stays byte-identical, ADR-063) | — |
| `src/vocab.rs` + `vocab/` | Runtime vocabulary (module: root holds the `Vocab` + entry type defs; `learn`/`methods`/`alias` submodules). The any-of synonym learner + `Vocab` struct + JSON persistence (ADR-015); `CorpusLearnConfig`/`learn_vocab_from_corpus` compose the opt-in NPMI phrase learner under it (ADR-053); `Vocab.equivalences` + `resolve_equivalences` + `learn_equivalences_from_queries` drive the expansion-not-collapse alias path (ADR-054); `Vocab.punctuation` (`PunctRule`s) persists the byte-cleaning punctuation-fold table into `to_normalizer` (ADR-058); `Vocab.number_context` persists the number-context word list the same way (`None` = the `["pop"]` default, `Some([])` = the parity mode, ADR-069); `vocab/alias.rs` (+ `alias/classify.rs`/`solr.rs`) is the **`AliasRegistry`** governance layer over equivalence expansion (provenance/kind/confidence/status; conservative single-token auto-activation; Solr import; `effective_equivalence_groups` + the `intern_equivalence_forms` ID-stability fix + the `demote_unexpressible` install-seam self-heal, ADR-060; **multi-word activation** + `active_alias_forms` registering alias-mode phrases via `to_normalizer`, ADR-061) | ADR-015, ADR-053, ADR-054, ADR-058, ADR-060, ADR-061, ADR-069 |
| `src/corpus.rs` | NPMI collocation core (`tokenize`/`learn_phrases`/`apply_phrases`) + `learn_phrases_from_text` → `Vocab` of induced entity phrases; lean-core, shared by `bin/learn.rs` + `vocab.rs` | ADR-053; [corpus-feature-learning.md](docs/research/corpus-feature-learning.md) |
| `src/error.rs` | Typed `ParseError` with `ParseErrorKind` enum | — |
| `src/loader.rs` | Query file loader (CSV + JSONL auto-detection) | — |
| `src/util.rs` | FNV-1a hash (stable across runs), FastMap alias | — |
| `tests/oracle.rs` | Differential correctness oracle (brute force vs engine; per-title AND batch path) — incl. the **messy-corpus** passes (`messy.rs`, the contract over adversarial surfaces), the **degenerate-input** differential (`degenerate.rs`), ADR-063, and the **class-D vacuous-accept** differential (`class_d.rs`: always-candidates ≡ brute incl. durability + knob-flip replay, ADR-068) | — |
| `tests/adversarial.rs` | **Reference-free property suite** (ADR-063): self-match diagonal (clean / messy-query / perturbed-title), metamorphic set-identity under surface noise, ADR-054/058/060/061 cross-form matrices (incl. the codex-R11 whitespace-run regression), unicode-soup fuzz (no-panic + determinism + `P(T) ⊇ N(T)` + `match_features == N(T)`) — covers the front-end divergence the shared-front-end differential cannot see | [testing.md](docs/testing.md) |
| `tests/broad_batch.rs` | Broad-lane batch≡scalar equivalence matrix (the load-bearing batch correctness deliverable) | [matching.md](docs/design/matching.md) §4 |
| `tests/ranking.rs` | Engine-level ranking (ADR-059): `EngineSnapshot::rank` additive scoring + newest-live-copy tag precedence + the ranked-set ≡ unranked-set recall guard | [matching.md](docs/design/matching.md) §5.4 |
| `tests/cluster_oracle.rs` | Multi-shard differential oracle: cluster ≡ single-node ≡ brute, K∈{1,3,8,16} × broad on/off, all placement classes + fan-out asserted; + **filtered percolation** (ADR-055) — tagged corpus, cluster filtered ≡ single-node ≡ brute across K×RF + filtered ⊆ unfiltered + synthetic-tag cross-shard consistency | ADR-027, ADR-055 |
| `tests/cluster_grpc_oracle.rs` | gRPC differential oracle (feature `distributed`): K real `ShardServer`s on localhost, cluster-over-gRPC ≡ single-node ≡ brute + live add/percolate/remove RPCs; **dict shipping (ADR-034)** — a dict-less-`pending`-servers variant + the divergent-dict-refused-on-a-populated-server guard; **replication + peer recovery (ADR-035/036)** — K×RF durable servers via `connect_replicated` ≡ brute, primary-stop **failover**, and fresh-node **peer recovery** over `FetchSegments`/`RecoverFrom`; **no-quiesce recovery (ADR-039)** — `grpc_peer_recovery_without_quiescing`: snapshot at `P`, writes land after `P`, the `FetchTranslog` tail catches them up, recovered ≡ live source ≡ brute over the final live set (the in-process + self-restart analogues are `replica.rs` unit tests); **filtered percolation over the wire (ADR-055)** — `grpc_filtered_percolation_matches_single_node_and_oracle`: `AdoptDict` ships the tag dict + fingerprint handshake, tagged bulk load + a live tagged add, filtered percolate ≡ single-node ≡ brute | ADR-029, ADR-034, ADR-036, ADR-039, ADR-055 |
| `tests/cluster_durability_oracle.rs` | Cluster durability oracle: a `data_dir` cluster rebuilt from manifest + per-shard segments + log ≡ pre-crash ≡ brute, K∈{1,3,8} × broad on/off, + checkpoint, torn-tail recovery, append-fails-closed, two-backend differential, fsync parity, fail-loud guards. Step-3b additions: attach-with-the-log-deleted, the checkpoint-after-removing-a-build-time-query bug-catcher, orphan-segment-ignored-and-GC'd, corrupt-segment-fails-loud; + the **cluster-upsert** module (ADR-070): single-frame `Upsert` replays ≡ live through both the log-tail and checkpoint reopen paths ≡ brute | ADR-031, ADR-032, ADR-070 |
| `tests/cluster_control_plane_oracle.rs` | Control-plane seam oracle (lean core, ADR-037): the default `InMemoryControlPlane` perturbs nothing, a reassignment preserves zero false negatives, and every backend converges to the same committed cluster-state doc — the acceptance gate for the `ControlPlane` seam | ADR-037 |
| `tests/cluster_control_raft_oracle.rs` | openraft control-plane oracle (feature `distributed`): a 3-node in-process Raft cluster (real elections + replication + quorum commit) converges to the in-memory backend's document (voters/nodes/assignments/model — NOT the epoch, which openraft's Blank/Membership commits perturb); a follower `propose` → `ForwardToLeader`; `change_membership` routes to Raft; and over real gRPC `ControlService` servers on localhost, **survive-the-leader-being-killed** (re-elect from quorum, committed doc persists, fresh write commits) | ADR-038 |
| `tests/cluster_autoscale_oracle.rs` | Autoscaler oracle (ADR-045): over a real in-process cluster, `tick` commits the same shard→node map a manual `rebalance` does; **`percolate` byte-identical before/after a tick** (zero false negatives); a second tick commits nothing (epoch-invariant); a disabled config is a no-op; a corpus-over-threshold `RecommendSplit` advisory mutates nothing | ADR-045 |
| `tests/cluster_allocator_oracle.rs` | Allocator oracle (lean core, ADR-042): the rendezvous/HRW shard→node map is balanced + deterministic, `rebalance` is idempotent (minimal movement), and **`percolate` is byte-identical before/after a rebalance** (zero false negatives) | ADR-042 |
| `tests/error_paths.rs` | API error handling regression tests | — |
| `tests/persistence.rs` | Persistence tests: segment round-trip, WAL recovery, mmap compaction | — |
| `tests/hardening_fixes.rs` | Integration tests: vocab epoch, fallible deser, reverse-index delete | — |
| `tests/coverage_gaps.rs` | Regression tests closing specific coverage gaps | — |
| `tests/stress.rs` | Pressure/soak suite: mixed read/write/delete churn, par==seq under mutation; one `#[ignore]`d 10M-query soak | [testing.md](docs/testing.md) |
| `src/bin/demo.rs` | Worked example end-to-end | — |
| `src/bin/clusterdemo.rs` | Cluster worked example: per-class placement + content-routed fan-out | ADR-027 |
| `src/bin/shardserver.rs` | Deployable shard node: serves `ShardService` over gRPC (feature `distributed`); `--pending` starts dict-less (ADR-034), `--data-dir` makes it **durable** — a recovering/replica node that can serve/accept peer recovery (ADR-036); `--tls-cert`/`--tls-key` + `--cluster-token` secure the mesh (ADR-071) | ADR-029, ADR-036, ADR-071 |
| `src/bin/controlserver.rs` | Deployable cluster-manager node: serves the openraft `ControlService` over gRPC (feature `distributed`); `--bootstrap` forms the initial cluster from `--peer ID=URL` members; `--data-dir` makes it **durable** (ADR-041 — persists its Raft log/vote/committed/snapshot, resumes its committed cluster-state doc on restart); `--tls-cert`/`--tls-key` + `--tls-ca`/`--tls-domain` + `--cluster-token` secure the mesh both directions (ADR-071) | ADR-038, ADR-041, ADR-071 |
| `src/bin/bench.rs` | Benchmark harness | — |
| `src/bin/clusterbench.rs` | Cluster fan-out benchmark: shards-probed/title (avg/p95/p99), candidate structure + broad share, fan-out-vs-K sweep (machine-independent) | ADR-027 |
| `src/bin/learn.rs` | Corpus feature-learner CLI — a thin demo over `corpus.rs` (prints NPMI entity/selectivity tables) | [corpus-feature-learning.md](docs/research/corpus-feature-learning.md) |
| `src/bin/norm.rs` | Title introspection tool | — |
| `src/bin/segbench.rs` | Read-amplification vs segment count harness | — |
| `src/bin/snapbench.rs` | Snapshot read/publish concurrency benchmark | ADR-016 |
| `src/bin/server/` | HTTP server (axum, **module**) — ES-style REST API (incl. batch `/_mpercolate`), snapshot-based concurrency, structured logging, Prometheus metrics, graceful shutdown. `server/main.rs` is the entry point (CLI parse, engine build, router wiring, shutdown); submodules split by concern — `cli` (flags), `auth` (opt-in bearer-token gate for mutating/admin endpoints — default-deny on non-GET/HEAD, ADR-062), `metrics` (Prometheus registry + the `EngineEvent`→counter bridge), `state` (`AppState` + the cluster-mode `ClusterAppState` + the `RequestCtx` middleware seam + request-id/in-flight middleware), `dto` (cross-handler response types — the error envelope + `_source`), `handlers/` (endpoint handlers grouped by family — `doc`/`search`/`admin`/`vocab`/`alias`, each owning its endpoint-specific DTOs + co-located tests), **`cluster_mode`** (coordinator-mode startup: assemble in-process build/reopen or `distributed`-gated remote connect, cluster router, durability shutdown — ADR-070) and **`handlers/cluster/`** (the coordinator-mode handler family — `doc` = cluster-atomic upsert + bulk, `search` = filtered percolate + per-request `include_broad`, `admin` = stats/health/shards/checkpoint + `_cluster/*` ops, `vocab` = `set_vocab`-backed vocab/alias admin; 501-with-alternative for single-node-only surfaces). Endpoint reference: [`docs/reference/api.md`](docs/reference/api.md) | ADR-014, ADR-016, ADR-021, ADR-022, ADR-023, ADR-026, ADR-062, ADR-070 |

*(All test suites above are committed and run by `cargo test --release`; suites that outgrew the size limit are now `tests/NAME/` folders, so the `tests/NAME.rs` names here are the cargo `--test NAME` suite names. The stress suite's one 10M-query soak is `#[ignore]`d — run it explicitly or via the CI `run_soak` dispatch input. How-we-test guide: [`docs/testing.md`](docs/testing.md).)*

## Where to go — find the ONE doc for your task

| Your task / question | Go to (one hop) |
|---|---|
| Understand the whole system fast | [`docs/design/README.md`](docs/design/README.md) §1 (mental model) |
| "Will my change cause a false negative?" | [`docs/design/README.md`](docs/design/README.md) §2 + the invariants above |
| Edit the DSL parser / normalizer / dictionary | [`docs/design/normalization.md`](docs/design/normalization.md) (+ `src/dsl.rs`, `normalize.rs`, `dict.rs`) |
| Edit the signature optimizer / candidate index / exact matcher | [`docs/design/matching.md`](docs/design/matching.md) (+ `src/compile.rs`, `index.rs`, `exact.rs`) |
| Edit the broad lane (class C) | [`docs/design/matching.md`](docs/design/matching.md) §4; evidence: [`docs/performance/results.md`](docs/performance/results.md) §9 |
| Per-query tags / filtered percolation (the `TagPredicate`, `tagdict.rs`, REST filter) | [`docs/design/matching.md`](docs/design/matching.md) §5 + [`docs/DECISIONS.md`](docs/DECISIONS.md) ADR-049 (single-node) / ADR-055 (through the cluster) (+ `src/tagdict.rs`, `exact.rs`, `bin/server/handlers/` — `doc`/`search`; cluster: `cluster/coordinator/{lifecycle,ingest,matching}.rs` + `clog.rs` + `shard.rs` + the gated `remote.rs`/`server/`) |
| Edit segments / flush / compaction / WAL / mmap | [`docs/design/ingestion-and-updates.md`](docs/design/ingestion-and-updates.md) (+ `src/segment/`, `storage.rs`, `wal.rs`) |
| Edit the HTTP server / REST endpoints | [`docs/reference/api.md`](docs/reference/api.md) (+ `src/bin/server/` — `handlers/{doc,search,admin,vocab}.rs`) |
| Query DSL syntax / vocabulary | [`docs/reference/dsl.md`](docs/reference/dsl.md) |
| Add/understand a config knob or `/_settings` | [`docs/DECISIONS.md`](docs/DECISIONS.md) ADR-022; `src/config.rs` |
| "Is X built or just designed?" / what to work on next | [`docs/STATUS.md`](docs/STATUS.md) (built vs design-only); the prioritized roadmap → [`docs/roadmap.md`](docs/roadmap.md) |
| "Why was it done this way?" / "why was X NOT built?" | [`docs/DECISIONS.md`](docs/DECISIONS.md) (the ADR index → one file per ADR in [`docs/decisions/`](docs/decisions/); declined → ADR-019) |
| Performance numbers / 100M extrapolation | [`docs/performance/results.md`](docs/performance/results.md); regression gate: `benchmark-results.txt` INVARIANTS |
| Run/change tests, benchmarks, pressure tests, hooks, or CI | [`docs/testing.md`](docs/testing.md) (gate: `engine/check.sh`; CI: `.github/workflows/ci.yml`) |
| Clustering / sharding / scale-out | [`docs/design/clustering-and-scaling.md`](docs/design/clustering-and-scaling.md); **shared-nothing** model (ADR-033, no object store). **Cluster v1** = in-process core + durable reopen + **dynamic vocabulary** (Tier 0, ADR-046 — built + oracle-proven). The **distributed layers, built but experimental / localhost-proven**: gRPC transport + dict shipping + replication + Raft control plane (durable, restart-recoverable) + **per-shard translog / no-quiesce peer recovery + retention/finalize** + **shard→node allocator** + **live data-moving handoff** (swappable backing + cross-node move + write fence) + **autoscaler** (membership/skew → `rebalance`; split/handoff advisories) + **remote partial-apply repair** (observe + fail-closed + `resync`, ADR-047) → `src/cluster/`, `engine/grpc/`, [`docs/DECISIONS.md`](docs/DECISIONS.md) ADR-027 + ADR-029 + ADR-034 + ADR-035/036 + ADR-037/038 + ADR-039/040/041 + ADR-042/043/044 + ADR-045 + ADR-047 |
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
decision → a new `docs/decisions/adr-NNN-slug.md` file + an index row in `docs/DECISIONS.md` (never
renumber); component design → `docs/design/<topic>.md`; "is it built" → `docs/STATUS.md`, "what's next"
→ `docs/roadmap.md`; benchmark numbers → `docs/performance/`; dependency version → `engine/Cargo.toml`;
new `src/` file → update the module map above.

## When modifying this file

This file is the *safety + orientation* layer, not a mirror of the docs. So:
- **Inline here (safety — an agent must not have to hop to stay correct):** the correctness-contract
  sentence and the critical-invariants list. Keep them byte-identical to
  [`docs/design/README.md`](docs/design/README.md) §2.
- **Never inline here (link the one canonical home instead):** performance numbers (→ `docs/performance/`),
  dependency versions (→ `engine/Cargo.toml`), implemented status (→ `docs/STATUS.md`) + the roadmap
  (→ `docs/roadmap.md`), per-component design (→ `docs/design/`), decision rationale
  (→ `docs/DECISIONS.md` + `docs/decisions/`).
- **Update the module map** when files are added/removed/renamed.
- If you're about to paste a number, a version, or a paragraph that already lives in one of those
  homes, write a one-line pointer instead.
