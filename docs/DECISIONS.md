# Architecture Decision Records

Lightweight record of the key design decisions in Reverse Rusty. Each entry captures the context,
the decision, and the consequences — so that future contributors (human or agent) understand
*why* things are the way they are, not just *what* they are.

**This file is the index.** Each ADR's full text lives in its own file under
[`decisions/`](decisions/) (`decisions/adr-NNN-slug.md`); the tables below give a one-line summary and
link to each. To find a decision, scan the relevant theme below, then open the one file — you should
never have to read all of them. (Implementation status of each decision is tracked in
[`STATUS.md`](STATUS.md), not here.)

ADRs are **append-only and never renumbered** — a superseded or reversed decision is marked in place,
not deleted (the record of *why something was not done* is as load-bearing as the rest).

> **Adding an ADR:** create a new `decisions/adr-NNN-slug.md` (next free number, a short kebab-case
> slug) and add one row to the matching theme table below. Never renumber, rewrite, or delete an
> existing record; supersede it in place and note the superseding ADR.

## Matching & verification

| ADR | Decision | Summary | Status |
|---|---|---|---|
| [001](decisions/adr-001-semantic-signatures.md) | Semantic signatures over term-level gating | Gate candidates on 2–3 *semantic* feature combinations from a domain-aware normalizer, not raw terms → flat ~54 candidates/title at any corpus size. | Accepted |
| [002](decisions/adr-002-integer-exact-verification.md) | Integer-only exact verification | Push all parsing/AST work to compile time; the match hot path is pure `u64`-mask + sorted-`u32` work — no strings/regex/alloc. | Accepted |
| [003](decisions/adr-003-broad-query-quarantine.md) | Broad-query quarantine via cost classes | Classify queries A–D at compile time; route non-selective class C to a batch lane, reject unconstrained class D — keep the selective path fast. | Accepted |
| [006](decisions/adr-006-forbidden-features-never-gate.md) | Forbidden features never gate (structural) | MUST_NOT features are invisible to the signature optimizer and checked only in exact verification — gating on an absent feature would be a false negative. | Accepted |
| [011](decisions/adr-011-cache-line-blocked-bloom.md) | Cache-line blocked bloom skip-filter | Per-segment anchor skip-filter is a 512-bit cache-line blocked bloom (1 memory access), chosen over binary-fuse/u64-blocked to fit the probe budget. | Accepted |
| [019](decisions/adr-019-query-family-factoring-declined.md) | Query-family factoring evaluated and declined | Declined the shared-prefix/family DAG — it optimizes a non-bottleneck at high format/rebuild cost; implicit anchor-sharing already prunes near-duplicates. Reversible. | **Declined** |
| [025](decisions/adr-025-query-complexity-limits.md) | Wire query-complexity limits into the parser | Thread configured max length/clauses/any-of-size into the parser so the flags + `/_settings` actually enforce; WAL replay keeps the compiled-in ceiling. | Accepted |
| [026](decisions/adr-026-broad-lane-batch-evaluation.md) | Broad-lane batch / columnar evaluation | Evaluate the broad lane once per title-batch via columnar bitmap algebra (`/_mpercolate`); byte-identical to per-title, removes the broad bottleneck. | Accepted |

## Normalization & vocabulary

| ADR | Decision | Summary | Status |
|---|---|---|---|
| [010](decisions/adr-010-normalizer-builder-fallible.md) | NormalizerBuilder + fallible construction | Replace hardcoded vocab + `.expect()` with a fluent `NormalizerBuilder` + fallible `default_vocab()`; domain-agnostic, zero panics, Debug/Send/Sync everywhere. | Accepted |
| [015](decisions/adr-015-runtime-vocabulary-learning.md) | Runtime vocabulary learning from any-of groups | Learn synonyms from query any-of groups at runtime (`Vocab` + `/_vocab`); a `vocab_epoch` counter tracks segments compiled under a now-stale normalizer. | Accepted |
| [046](decisions/adr-046-dynamic-vocabulary.md) | Dynamic vocabulary (Cluster v1) | Absorb terms after the dict is frozen — feature-hashing for unknown tokens (no coordination, bounded FP, never FN) + runtime normalizer learning for aliases (blue/green rebuild). Built. | Accepted |
| [053](decisions/adr-053-corpus-phrase-vocab-source.md) | NPMI corpus phrase induction as a runtime vocab source | Wire the `learn` binary's NPMI collocation miner into a library `corpus::learn_phrases_from_text` → `Vocab`, composed under the ADR-015 any-of learner via an opt-in `CorpusLearnConfig`/`learn_and_apply_with`. Phrases only (no aliases) ⇒ same-normalizer gluing ⇒ oracle-equivalent, zero FN; default-off ⇒ byte-identical. | Accepted |
| [054](decisions/adr-054-equivalence-expansion.md) | Equivalence (alias) learning via expansion, not collapse | First-class `Vocab.equivalences` + a compile-time `Extracted::expand_equivalences` that widens a required feature into an any-of over its group (query-side, via a transient `dict::EquivMap`). Structurally FN-safe (match set only grows; wrong alias ⇒ bounded FP). Declared + any-of-learned sources (opt-in); distributional/match-feedback discovery deferred behind the same seam. | Accepted |
| [058](decisions/adr-058-punctuation-equivalence-folding.md) | Configurable punctuation-equivalence folding | Make byte-cleaning's per-character behavior a configurable `PunctClass` table (`Split`/`Fold`/`Keep`/`Marker`) on the shared normalizer; declaring `'`/`-` as `Fold` collapses `O'Brien`/`O-Brien`/`OBrien` to one token, closing a recall gap. Same table over queries + titles ⇒ cover holds. Default reproduces the historical behavior (byte-identical); opt-in, persisted via `Vocab`. | Accepted |

## Ingestion, storage & durability

| ADR | Decision | Summary | Status |
|---|---|---|---|
| [004](decisions/adr-004-lsm-write-path.md) | LSM write path over full rebuild | Log-structured writes (immutable segments + memtable + tombstones, atomic epoch swap) instead of full rebuild — ~750k updates/sec/core, immediate visibility. | Accepted |
| [009](decisions/adr-009-score-based-compaction.md) | ClickHouse-style score-based compaction | Score-based greedy compaction (not RocksDB leveled) because percolation must probe *every* segment — directly minimizes time-integrated segment count. | Accepted |
| [012](decisions/adr-012-mmap-segment-format.md) | mmap'd segment file format + frozen hash tables | Custom `.seg` format with frozen open-addressing hash tables, mmap'd for zero-copy reads; the precondition that makes the anchor skip-filter pay off. | Accepted |
| [013](decisions/adr-013-write-ahead-log.md) | Write-ahead log (WAL) for crash recovery | Append-only CRC-framed WAL for the memtable; WAL-first (durability before visibility), a failed append rejects the write, fsync policy configurable. | Accepted |
| [014](decisions/adr-014-query-source-store.md) | Engine-level query source store | Keep original query text in an engine-level `sources.dat` (not in `.seg`), so source text never touches the match hot path. | Accepted |
| [016](decisions/adr-016-snapshot-read-path-arcswap.md) | Snapshot read path (ArcSwap) over RwLock | Lock-free reads via `ArcSwap<EngineSnapshot>` + write-only `Mutex`; structural sharing makes publish O(1) (PUT 82 ms → ~2 µs at 1M queries). | Accepted |
| [017](decisions/adr-017-durable-bulk-ingest.md) | Durable bulk ingest (segment = artifact, manifest = commit) | Bulk ingest is durable-or-rejected (RocksDB IngestExternalFile model); no silent in-memory fallback, manifest write is the atomic commit point. | Accepted |
| [018](decisions/adr-018-bulk-ingest-per-item-outcomes.md) | Bulk ingest reports per-item outcomes (ES-style) | `/_bulk` returns per-item statuses (which queries were dropped + why), not just an aggregate count. | Accepted |
| [020](decisions/adr-020-resident-memory-reduction.md) | Production-scale resident-memory reduction | Lazy on-disk source store + flat logical-index columns cut resident memory ~148 → ~96 B/query (→ ~4.5 with both, opt-in) ahead of sharding. | Accepted |
| [051](decisions/adr-051-fail-closed-flush-compaction.md) | Fail-closed flush, compaction & reseal | Extend ADR-017's durable-or-rejected discipline to flush/compaction/reseal/recompile: build the replacement durable before destroying what it replaces, gate WAL-advance/file-deletion on the commit point. No silent restart data loss on disk failure. | Accepted |
| [056](decisions/adr-056-compaction-reanchoring.md) | Compaction-that-improves (re-anchoring drifted queries) | Opt-in `compaction_reanchor`: a merge re-derives each alive query's cover with current frequencies (decoding the stored SoA, reusing `anchor_plan`) so a drifted anchor moves to a more-selective one — shrinking hot postings + fan-out. FN-safe by the `anchor_plan`/`match_into` matched pair (SoA copied verbatim); a no-op in a cluster (frozen dict); default-off ⇒ byte-identical. Closes the Tier-2 item. | Accepted |
| [057](decisions/adr-057-frozen-dict-format-versioning.md) | Version + harden the frozen-space serializations (feature dict + tag dict) | Add a `magic + version` header (`RDCT`/`RTGD`) to the two previously-unversioned binary formats so a layout change or newer-build blob fails loud instead of misparsing; strict `dict::kind_from_tag` decode (unknown tag ⇒ error, never silent `Generic`); fully-fallible parse (no panic on truncation). Legacy header-less blobs still read; content-based fingerprint untouched ⇒ gRPC handshake byte-identical. Closes the "dict format not versioned" robustness item. | Accepted |

## Engine, errors, dependencies & ops

| ADR | Decision | Summary | Status |
|---|---|---|---|
| [005](decisions/adr-005-typed-errors.md) | Typed errors over stringly-typed Results | `ParseError { kind, pos }` + `IngestReport` instead of `Result<_, String>` and silent drops — inspectable errors; no `unwrap()` in library code. | Accepted |
| [007](decisions/adr-007-three-production-dependencies.md) | Three production dependencies | Adopt daachorse / roaring / rayon for alias matching, large postings, and data-parallel matching once the std-only design was validated. | Accepted |
| [008](decisions/adr-008-deterministic-data-generation.md) | Deterministic data generation (seeded PRNG) | Seeded SplitMix64 PRNG (no crates) so benchmarks + the oracle are reproducible and adversarial patterns are configurable parameters. | Accepted |
| [021](decisions/adr-021-durability-failures-observable.md) | Durability failures are observable events | Route the ~14 durability-failure sites through a structured `EngineEvent::DurabilityFailure` (op + severity), not stderr — operators alert from metrics/logs. | Accepted |
| [022](decisions/adr-022-runtime-settings-api.md) | ES-style runtime settings API (`/_settings`) | `GET/PUT /_settings` reads the live config lock-free and updates the dynamic subset at runtime with all-or-nothing per-key validation; static keys rejected. | Accepted |
| [023](decisions/adr-023-per-segment-introspection.md) | Per-segment introspection (`/_cat/segments`) | Expose per-segment holes / memory-split / staleness (text or JSON), read lock-free from the snapshot. | Accepted |
| [024](decisions/adr-024-ci-github-actions.md) | CI via GitHub Actions mirroring `check.sh` | CI runs `check.sh` itself (one source of truth); commit the pressure suite + benchmark baseline; benchmarks run-and-print, never gate (hardware variance). | Accepted |
| [028](decisions/adr-028-lean-core-feature-gate.md) | Feature-gate the server stack (lean core) | Gate the server/observability stack behind a default-on `server` feature (Cargo-level, zero `#[cfg]`); a lean-core clippy lane keeps server crates out of library code. | Accepted |
| [050](decisions/adr-050-golden-front-end-tests.md) | Oracle front end pinned by spec-authored golden tests | The differential oracle shares the engine's parse/normalize/extract front end (and runs empty-vocab), so a front-end bug would hide; pin those three stages with hand-authored golden tests + a vocab-rich oracle pass. | Accepted |
| [052](decisions/adr-052-external-review-hardening.md) | External-review hardening (batch) | Six small review fixes: reject `-` + space in the parser; reserve 0 in `sig_key` (frozen-table sentinel); apply `max_percolate_batch` to multi-doc `/_search`; bounds-validate segment sections before the unsafe cast; document `timeout_ms` as response-only; default the HTTP bind to `127.0.0.1` + `--host`. | Accepted |

## Clustering — core & transport

| ADR | Decision | Summary | Status |
|---|---|---|---|
| [027](decisions/adr-027-in-process-multi-shard-core.md) | In-process multi-shard core | K-shard coordinator: one shared frozen dict → globally-stable `FeatureId`s, a feature-anchor ring (~2–5 fan-out), broad lane on a replicated shard. The no-false-negative heart of clustering. | Accepted |
| [029](decisions/adr-029-grpc-shardserver-shard-seam.md) | gRPC `ShardServer` + local↔remote `trait Shard` | Lift the shard behind a `trait Shard` + a tonic `ShardServer` (off-by-default `distributed`); ships DSL not feature-ids; the fallible seam preserves zero-FN. | Accepted |
| [030](decisions/adr-030-dict-fingerprint-handshake.md) | Dict-fingerprint handshake + fallible construction | Connect-time `Dict::fingerprint` handshake turns a divergent cross-process dict from a silent false-negative into a loud `DictMismatch`; construction made fully fallible. | Accepted |
| [031](decisions/adr-031-externalized-coordinator-log.md) | Externalized coordinator log (`trait ClusterLog`) | A durable CRC-framed (+ null) ordered, log-first mutation log so the whole cluster is rebuildable from the log alone. | Accepted |
| [032](decisions/adr-032-per-shard-durable-segments.md) | Per-shard durable compiled segments | Reopen by attach-and-mmap per-shard `.seg` files (not re-ingest); coordinator manifest is the atomic commit point; checkpoint re-seals tombstoned base segments. | Accepted |
| [033](decisions/adr-033-shared-nothing-storage.md) | Shared-nothing cluster storage | Supersede the Aurora/object-store framing — shared-nothing (local segments + per-node WAL + peer recovery + Raft control plane), like ES/Cassandra/Kafka. | Accepted |
| [034](decisions/adr-034-cross-process-dict-shipping.md) | Cross-process dict shipping over gRPC | Ship the frozen dict to each server at connect (`AdoptDict`); a data node starts empty/pending instead of rebuilding the dict from the whole corpus. | Accepted |

## Clustering — replication & control plane

| ADR | Decision | Summary | Status |
|---|---|---|---|
| [035](decisions/adr-035-per-shard-replication-peer-recovery.md) | Per-shard replication + peer recovery (`ReplicatedShard`) | One primary + N replicas (in-process): primary-authoritative writes, in-sync-only read failover, peer recovery by streaming segments. Set-equality is the basis. | Accepted |
| [036](decisions/adr-036-grpc-replication-peer-recovery.md) | gRPC multi-node replication + peer recovery | Lift replication + peer recovery onto gRPC — remote replicas that fail over + cross-node segment streaming (`FetchSegments`/`RecoverFrom`); servers become durable. | Accepted |
| [037](decisions/adr-037-control-plane-seam.md) | Control-plane seam (`trait ControlPlane`) | Dependency-free seam + in-memory backend holding the cluster-state doc (ring params + shard→node map + membership + epoch); shaped for openraft, byte-identical by default. | Accepted |
| [038](decisions/adr-038-openraft-control-service.md) | openraft backend + gRPC `ControlService` | A real openraft backend behind the seam; consensus holds only the cluster-state doc, never query mutations; survives leader death. | Accepted |
| [039](decisions/adr-039-durable-translog-no-quiesce-recovery.md) | Durable translog + no-quiesce peer recovery | Per-shard durable+replicated query log lets recovery stream segments at P then replay the tail > P — recovery without quiescing writes; data nodes self-restart. | Accepted |
| [040](decisions/adr-040-translog-retention-leases.md) | Translog retention leases + finalize | Leases pin the translog tail across a recovery (min over holders) so a concurrent seal can't strand it; a bounded finalize loop grows a replica in-sync without pausing writes. | Accepted |
| [041](decisions/adr-041-durable-raft-log-recovery.md) | Durable Raft log + control-plane restart recovery | Make the openraft log/vote/committed/snapshot durable so a `controlserver --data-dir` survives a crash and rejoins quorum; `apply` stays pure in-memory. | Accepted |

## Clustering — elasticity & repair

| ADR | Decision | Summary | Status |
|---|---|---|---|
| [042](decisions/adr-042-shard-node-allocator.md) | Shard→node allocator (rendezvous hashing) | HRW hashing computes a balanced, minimal-movement shard→node map; `rebalance` commits only changed positions; in-process the map is advisory. | Accepted |
| [043](decisions/adr-043-swappable-shard-backing.md) | Swappable shard backing (`HandoffShard`) | Make a position's backing atomically swappable (`ArcSwap` + generation fence stamp), serve-then-drop for free — the routing-flip half of a live handoff. | Accepted |
| [044](decisions/adr-044-live-data-moving-handoff.md) | Live data-moving handoff | `execute_handoff` wires decide→move→flip: no-quiesce bulk recover → fence the source (writes only) → drain to convergence → flip routing. A shard moves owners live, zero FN. | Accepted |
| [045](decisions/adr-045-autoscaler.md) | Autoscaler (policy/trigger over rebalance) | A pure `evaluate` policy (membership→rebalance; skew→handoff advisory; corpus→split advisory) + a `tick` driver; idempotence is the hysteresis; disabled by default. | Accepted |
| [047](decisions/adr-047-remote-partial-apply-resync.md) | Remote live-write partial-apply repair (`resync`) | A mid-fan-out remote write failure is detected (typed `PartiallyApplied` + event) and repaired live (`resync`) instead of silently partial; + a safe `block_on` thread-context contract. | Accepted |
| [048](decisions/adr-048-reliability-hardening.md) | Reliability hardening | Auto-unfence-on-abort (`Unfence` CAS), translog-lease TTL reap, and wiring the autoscaler's `Handoff` advisory through to `execute_handoff`. | Accepted |

## Percolator parity

| ADR | Decision | Summary | Status |
|---|---|---|---|
| [049](decisions/adr-049-percolator-parity-tags.md) | Per-query metadata, filtered percolation, ranking | Per-query integer metadata tags in the SoA + filtered percolation pushed into verify (never gating, mirroring ADR-006) + optional out-of-core ranking. Built single-node + oracle-proven. | Accepted |
| [055](decisions/adr-055-cluster-tags-filtered-percolation.md) | Tags + filtered percolation through the cluster | Thread tags end-to-end (in-process + gRPC): one shared frozen `TagDict` (like the `Dict`), raw tags in the log + read-only `get_or_synthetic` resolution (never `intern`), filter resolved once + shipped as `TagId` groups, tag-dict shipping + fingerprint handshake. Additive APIs ⇒ untagged path byte-identical. Built + oracle-proven. | Accepted |
| [059](decisions/adr-059-percolate-ranking-pagination.md) | Percolate ranking + pagination (ADR-049 dp-4, single-node) | Opt-in post-match ranking: a new `rank.rs` scorer + `EngineSnapshot::rank` order the already-final id set by `Σ boosts + priority-tag value` (additive), tie-broken by `_id`; the handler sorts + applies `from`/`size` + emits `_score`. Adds `from` to `/_mpercolate` and per-slot truncation (closes the ADR-052 #3 tail). Touches neither index nor verifier ⇒ zero-FN; default byte-identical. Cluster ranking still deferred. Built + tested. | Accepted |
| [060](decisions/adr-060-learned-alias-evolution.md) | Learned-alias evolution — Phase 1 (safe single-token activation) | A governing `AliasRegistry` (provenance / kind / confidence / status) over ADR-054 expansion: a structural classifier auto-activates only single-token spelling/abbreviation variants; learned category alternatives `(psa,bgs,sgc)`, multi-word, and mixed-kind groups stay review candidates. Solr/Lucene import + group-level any-of learning + the **alias-ID-stability fix** (intern active forms before resolving, so a later insert can't flip a synthetic→dense id and silently kill the alias). Active groups feed `effective_equivalence_groups`; live apply via `set_vocab`+recompile; REST `GET/POST /_vocab/aliases*`. Single-node; multi-word = Phase 2. Zero-FN; default byte-identical. Built + oracle-proven. | Accepted |
| [061](decisions/adr-061-token-graph-multiword-aliases.md) | Token-graph multi-word aliases — Phase 2 (positive/negative title feature views) | Activates the multi-word candidates ADR-060 recorded. The wall: a title emits ONE feature set used for both required and forbidden checks, but multi-word retrieval needs the overlapping superset (unsafe for negation). Fix: **two title-side feature views** — positive superset `P(T)` (overlap-aware retrieval + required + any-of) and canonical leftmost-longest `N(T)` (forbidden only), threaded as `TitleView` through `verify`/`match_into`. Query side collapses an alias phrase to its entity (ADR-054 expansion widens it); title side is additive + overlapping. The equivalence machinery is reused unchanged (a collapsed form resolves to one entity). Forbidden policy = canonical leftmost-longest (recall-safe). Multi-word now auto-activates when declared/manual. Single-node; broad lane routes to the two-view inline path while aliases active. Zero-FN (oracle incl. forbidden-over-multi-word from day one); default byte-identical. Built + oracle-proven. | Accepted |

---

*Conventions for editing docs (and where each fact lives) → [`README.md`](README.md).*
