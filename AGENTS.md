# AGENTS.md — agent context for Reverse Rusty

Read this before changing the repository. It owns the safety rails and routes each task to its
canonical documentation; it is deliberately not a second reference manual.

> Product overview → [`README.md`](README.md) · Documentation map and ownership rules →
> [`docs/README.md`](docs/README.md)

## What this project is

Reverse Rusty is a Rust reverse product-query matcher: millions of stored product-intent queries are
matched against incoming listing titles. It retrieves candidates through semantic signatures, then
checks them with integer-only match plans.

The single-engine and in-process multi-shard paths are built and oracle-proven, including durable
reopen and dynamic vocabulary. The feature-gated distributed stack—gRPC shards, replication and peer
recovery, a Raft control plane, data-moving reassignment/reconciliation, co-location, and exhaustive
delivery—is built and tested in-process, over localhost, and in single-host container networks. It
remains experimental until the real multi-machine corpus and failure exercise in
[`docs/roadmap.md`](docs/roadmap.md) is complete.

Current behavior belongs in [`docs/design/`](docs/design/README.md),
[`docs/reference/`](docs/reference/api.md), and
[`docs/operations/`](docs/operations/deployment-modes.md). Shipped history belongs in
[`docs/CHANGELOG.md`](docs/CHANGELOG.md); unfinished work belongs in
[`docs/roadmap.md`](docs/roadmap.md).

## The correctness contract

> **Lossless signature cover:** if a title `T` *could* satisfy query `Q`'s positive semantics, then
> `T` must generate at least one signature that retrieves `Q` from the candidate index.

This guarantees zero candidate false negatives. Extra candidates are allowed; exact verification
rejects them. The proof obligation lives in
[`docs/design/README.md`](docs/design/README.md#2-the-correctness-contract-the-thing-that-must-never-break),
and the principal differential suites live under `engine/tests/oracle/` and
`engine/tests/independent_oracle/`.

## Critical invariants

- **Never gate on forbidden (`MUST_NOT`) features.** Negatives are checked only in exact
  verification; the signature optimizer must not be able to see them.
- **Cost movement must never imply visibility movement** (ADR-105). Default-visible,
  opt-in-broad, and rejected are visibility states; realtime, hot-columnar, broad-columnar, and
  universal are scheduling states. A cost-driven move may change only the latter.
- **Keep the class-C boundary tied to the frozen top-64 mask, never the runtime hot threshold.**
  Class H remains default-visible even though it uses the columnar evaluator.
- **Build signatures only from positive requirements and required any-of branches.** A proxy used
  for retrieval remains a necessary condition; full compound and phrase predicates stay in exact
  verification.
- **Use the same feature model on both sides.** Query compilation and title normalization must share
  compatible normalizer, dictionary, vocabulary, punctuation, and compiler semantics. Positive
  matching uses `P(T)`; forbidden checks use canonical `N(T)`.
- **Keep the matching core integer-only and allocation-free.** Strings, regex, AST interpretation,
  and source lookup belong outside candidate retrieval and exact verification.
- **Apply metadata filters, unique-emission ownership, ranking, pagination, and delivery only after
  Boolean matching.** These layers may intentionally select or order confirmed matches; they must
  not weaken semantic candidate retrieval.
- **Distributed exact reads fail loud.** Do not return a successful partial top-K or exhaustive
  result when a required shard, winner source, completion summary, or ownership attestation is
  missing.
- **Durable and wire incompatibilities fail loud.** Never silently reinterpret a newer segment,
  manifest, log, compiler-semantics stamp, placement generation, or mesh capability.
- **Postings are append-only within a segment.** Segment-local IDs are issued in order, so postings
  remain sorted without per-insert sort/dedup.
- **No panicking `unwrap()`/`expect()` in library code.** Return typed errors. Library code also
  reports operational failures through typed results or `EngineEvent`, not stdout/stderr.

## How to approach implementation work

Design and roadmap pages state problems and constraints, not compulsory implementations.

1. Identify the actual workload or correctness problem.
2. Research peer systems and relevant literature.
3. Compare candidates against the invariants, hot-path budget, durability model, and lean
   dependency policy.
4. Implement the best fit and record architecture/compatibility choices in an ADR.

Keep the dependency tree deliberate. Crate features and versions are authoritative only in
[`engine/Cargo.toml`](engine/Cargo.toml).

## Build, test, and run

Run Rust commands from `engine/`:

```bash
export CARGO_TARGET_DIR=/tmp/reverse-rusty-target
cargo build --release
cargo test --release
cargo test --release --features distributed
./check.sh
```

- `./check.sh --fast` runs the short format/lint path; the full script is the local CI-equivalent
  gate and prints a non-failing file-size advisory.
- `./setup-hooks.sh` installs the repository hooks.
- `cargo run --release --bin demo` runs the worked example.
- `cargo run --release --bin server -- --help` lists the HTTP-server flags.
- The gRPC node binaries are deliberately low-level; use
  [`docs/operations/cluster-deployment.md`](docs/operations/cluster-deployment.md) for supported
  command lines rather than inferring flags from their parsers.
- Test organization, ignored soaks, benchmarks, and CI ownership are documented in
  [`docs/testing.md`](docs/testing.md).
- Endpoint and deployment commands are documented in
  [`docs/reference/api.md`](docs/reference/api.md) and
  [`docs/operations/build-and-smoke.md`](docs/operations/build-and-smoke.md).

The toolchain is pinned in `engine/rust-toolchain.toml`; dependency versions are pinned in
`engine/Cargo.toml`. Do not copy either into prose.

## Architecture skeleton

```text
COMPILE (off the hot path)
  DSL → AST → shared normalization → extracted positive/negative predicates
      → lossless anchor plan → class A/B/C/D/H → mutable segment

MATCH
  title → P(T) + N(T) → title signatures
        → main/hot/broad/universal candidate lanes
        → integer exact verification
        → metadata + unique ownership
        → optional integer ranking
        → all/top-K/exhaustive delivery
```

The full architecture and class definitions live in
[`docs/design/README.md`](docs/design/README.md) and
[`docs/design/matching.md`](docs/design/matching.md).

## Repository map

This is a responsibility map, not a duplicated API reference.

| Area | Primary paths | Canonical doc |
|---|---|---|
| DSL, normalization, feature/tag dictionaries, vocabulary | `engine/src/{dsl,normalize,dict,tagdict,vocab,corpus}.rs` and matching submodules | [`docs/design/normalization.md`](docs/design/normalization.md) |
| Compilation, anchors, candidate index, exact verification, explain | `engine/src/{compile,index,filter,exact,explain}.rs` and submodules | [`docs/design/matching.md`](docs/design/matching.md) |
| Ranking, collectors, ownership, PIT, exhaustive delivery | `engine/src/{rank,collect,result,ownership,pit,delivery,broker}.rs` and submodules | [`docs/design/matching.md`](docs/design/matching.md), [`docs/reference/ranking.md`](docs/reference/ranking.md), [`docs/reference/api/percolate.md`](docs/reference/api/percolate.md) |
| Mutable engine, snapshots, broad/hot batches, compaction | `engine/src/segment.rs`, `engine/src/segment/` | [`docs/design/ingestion-and-updates.md`](docs/design/ingestion-and-updates.md) |
| Durable formats, sources, backup, WAL | `engine/src/storage.rs`, `engine/src/storage/`, `engine/src/wal.rs`, `engine/src/wal/` | [`docs/design/ingestion-and-updates.md`](docs/design/ingestion-and-updates.md), [`docs/operations/rolling-upgrade.md`](docs/operations/rolling-upgrade.md) |
| Cluster placement, routing, durability, control, transport, recovery | `engine/src/cluster.rs`, `engine/src/cluster/` | [`docs/design/clustering-and-scaling.md`](docs/design/clustering-and-scaling.md) |
| HTTP and node binaries | `engine/src/bin/server/`, `engine/src/bin/{shardserver,controlserver}.rs` | [`docs/reference/api.md`](docs/reference/api.md), [`docs/operations/deployment-modes.md`](docs/operations/deployment-modes.md) |
| Correctness, durability, distributed, and pressure suites | `engine/tests/` | [`docs/testing.md`](docs/testing.md) |
| Images, Compose, Helm, smoke and lifecycle harnesses | `deploy/`, `.github/workflows/` | [`docs/operations/build-and-smoke.md`](docs/operations/build-and-smoke.md) |

Aim for roughly 600 lines per Rust or Markdown file as a soft maintainability signal, not a gate.
Split at coherent responsibility boundaries: roots own shared types/re-exports and children own
implementations or test families. Do not add indirection solely to satisfy the number.

## Task router

| Task | Read first |
|---|---|
| Understand the system or assess false-negative risk | [`docs/design/README.md`](docs/design/README.md) |
| Change parsing, normalization, aliases, or feature IDs | [`docs/design/normalization.md`](docs/design/normalization.md) |
| Change anchors, classes, candidate retrieval, exact matching, or tags | [`docs/design/matching.md`](docs/design/matching.md) |
| Change ranking profiles, features, scoring, or loading | [`docs/design/matching.md`](docs/design/matching.md), [`docs/reference/ranking.md`](docs/reference/ranking.md) |
| Change ingest, flush, compaction, persistence, backup, or formats | [`docs/design/ingestion-and-updates.md`](docs/design/ingestion-and-updates.md) |
| Change sharding, routing, replication, control, recovery, or resize | [`docs/design/clustering-and-scaling.md`](docs/design/clustering-and-scaling.md) |
| Change the REST API or DSL contract | [`docs/reference/api.md`](docs/reference/api.md), [`docs/reference/dsl.md`](docs/reference/dsl.md) |
| Change deployment, security, sizing, recovery, or alerts | [`docs/operations/deployment-modes.md`](docs/operations/deployment-modes.md) |
| Change tests, benchmarks, hooks, gates, or CI | [`docs/testing.md`](docs/testing.md) |
| Inspect measurements or add a benchmark capture | [`docs/performance/README.md`](docs/performance/README.md) |
| Understand why a choice was made | [`docs/DECISIONS.md`](docs/DECISIONS.md) |
| See shipped changes | [`docs/CHANGELOG.md`](docs/CHANGELOG.md) |
| Pick or refine unfinished work | [`docs/roadmap.md`](docs/roadmap.md) |
| Review prior art | [`docs/research/README.md`](docs/research/README.md) |

## Documentation ownership

Follow [`docs/README.md`](docs/README.md) when moving or adding documentation.

- Current behavior → design/reference/operations.
- Shipped outcome → `docs/CHANGELOG.md`.
- Unfinished idea, with its full proposal → `docs/roadmap.md`.
- Architecture or compatibility rationale → a numbered ADR under `docs/decisions/` plus one row in
  the appropriate `docs/decisions/areas/` catalog.
- Measurements → `docs/performance/benchmark-results.txt`, then interpretation in
  `docs/performance/results.md`.
- Dependency/toolchain versions → authoritative configuration files only.

When work ships, remove its roadmap entry instead of marking it complete. Keep ADRs as immutable
decision history, updating only explicit status/outcome/cross-reference sections when later facts
change. If a path or heading moves, repoint every inbound link in the same change.

One owner-authorized exception applies to the 2026-07 domain-neutral prototype reset: obsolete
product-specific examples and specialized implementation details may be sanitized in place so the
public tree contains no deployment-domain vocabulary. Preserve decision numbers, status,
architecture constraints, evaluated alternatives, measurements, and revision outcomes in generic
form. New decisions and later maintenance return to the immutable-history rule above.
