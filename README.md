<p align="center">
  <img height="280" alt="reverse_rusty" src="https://github.com/user-attachments/assets/ab6aeedb-0934-445e-8cb3-de6b726b19a0" />
</p>


# Reverse Rusty

<p align="center">
  <a href="https://github.com/dvideby0/reverse-rusty/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/dvideby0/reverse-rusty/actions/workflows/ci.yml/badge.svg" /></a>
</p>

Reverse Rusty is a reverse product-query matcher written in Rust. It stores Boolean product-intent
queries and, for each incoming listing title, returns the IDs of the queries whose semantics match.
Search normally finds documents for one query; this repository implements the inverse operation,
usually called **percolation**.

The engine is designed around short product titles, large query sets, and frequent query updates.
Queries are parsed and compiled when they are registered. Matching retrieves candidates through
semantic signatures and then checks those candidates with integer-only match plans.

> **Building on the code or contributing (including with an AI agent)?** Start at
> [`AGENTS.md`](AGENTS.md) for the correctness contract, invariants, and task→doc router, then browse
> [`docs/`](docs/README.md). [`CLAUDE.md`](CLAUDE.md) is a compatibility shim for tools that
> discover that filename.

## Scope and maturity

The single-engine and in-process multi-shard paths are built and covered by differential oracles,
including durable reopen and dynamic vocabulary. The feature-gated distributed stack is also
implemented and exercised in-process, over localhost, and in single-host container networks, but the
project still treats real multi-machine operation as experimental. Representative-corpus validation
and a real Kubernetes failure exercise remain open in the
[`roadmap`](docs/roadmap.md#priority-1--real-world-acceptance-evidence). The exact packaged modes and
their constraints are documented in
[`deployment-modes.md`](docs/operations/deployment-modes.md).

Within the documented query language and normalization semantics, candidate retrieval follows one
correctness contract: a title that could satisfy a query must retrieve that query for exact
verification. Extra candidates are allowed; missing candidates are not. This lossless-signature
property is checked by both engine-coupled and independent differential oracles. The formal statement,
test locations, and invariants are in [`AGENTS.md`](AGENTS.md).

## Architecture overview

There are two phases: **compile time**, when a query is registered, and **match time**, when a title
arrives. Parsing, normalization, and signature selection happen during compilation. Matching
normalizes the title, probes the candidate index, and runs exact verification.

```
                        COMPILE TIME (per stored query)
                        ══════════════════════════════

  ┌──────────────┐     ┌───────────┐     ┌────────────┐     ┌──────────────┐
  │  Query DSL   │────▶│   Parse   │────▶│ Normalize  │────▶│   Extract    │
  │              │     │           │     │            │     │  Features    │
  │ "vintage     │     │ AST with  │     │ Canonical  │     │              │
  │  (leather,   │     │ terms,    │     │ feature    │     │ required: [] │
  │  suede)      │     │ groups,   │     │ IDs from   │     │ forbidden:[] │
  │  -replica"   │     │ negations │     │ shared     │     │ any-of:   [] │
  └──────────────┘     └───────────┘     │ dictionary │     └──────┬───────┘
                                         └────────────┘            │
                                                                   ▼
  ┌──────────────┐     ┌───────────────────────────────────────────────────┐
  │  Append to   │◀────│          Signature-Cover Optimizer                │
  │  Segment     │     │  Pick the smallest set of signatures that         │
  │              │     │  guarantees retrieval. Classify placement/work:   │
  │ • Index      │     │  A/B: selective       C/D: opt-in broad lane      │
  │ • ExactStore │     │  H: visible hot tier  D: universal signature      │
  └──────────────┘     └───────────────────────────────────────────────────┘


                        MATCH TIME (per incoming title)
                        ═════════════════════════════

  ┌──────────────┐     ┌───────────┐     ┌────────────┐     ┌──────────────┐
  │  Raw Title   │────▶│ Normalize │────▶│  Generate  │────▶│    Probe     │
  │              │     │ (same     │     │  Title     │     │  Candidate   │
  │ "Vintage     │     │  pipeline │     │ Signatures │     │    Index     │
  │  Leather …"  │     │  as query)│     │            │     │ sig → [IDs]  │
  └──────────────┘     └───────────┘     └────────────┘     └──────┬───────┘
                                                                   ▼
  ┌──────────────┐     ┌──────────────────┐     ┌────────────────────────────┐
  │   Returned   │◀────│ Optional Rank +  │◀────│ Exact Integer Verification │
  │  Query IDs   │     │ Bounded Delivery │     │ mask → required → forbidden │
  │  [42, 17]    │     │ score ↓, ID ↑    │     │ → any-of / phrase checks    │
  └──────────────┘     └──────────────────┘     └────────────────────────────┘
```

*The authoritative engineering rendering of this pipeline lives in
[`docs/design/README.md`](docs/design/README.md) §1.*

### Key techniques

Each links to the design doc that details it:

- **Signature-cover optimizer** — selects a *lossless* minimal set of signature keys per query, so any
  title that could match generates a retrieving signature. This is the candidate-retrieval
  correctness invariant. ([design/matching.md](docs/design/matching.md) §1)
- **Common-mask gate** — the 64 hottest features get a bit in a `u64` mask; two bitwise checks reject
  many candidates before touching their variable-length verification columns.
  ([design/matching.md](docs/design/matching.md) §3)
- **Three-tier adaptive postings** — inline (≤8) → sorted `Vec` (≤256) → roaring bitmap (>256), chosen
  by cardinality. ([design/matching.md](docs/design/matching.md) §2)
- **Two-axis placement classes (A/B/C/D/H)** — visibility and scheduling are independent: selective
  A/B work stays on the normal path, C/D work is opt-in broad, and H keeps default-visible hot
  anchors in a separately scheduled tier.
  ([design/matching.md](docs/design/matching.md) §4)
- **Cache-line blocked bloom filters** — each segment carries a 512-bit-block filter answering "could
  this signature exist here?" in one cache-line fetch. ([design/ingestion-and-updates.md](docs/design/ingestion-and-updates.md) §6)
- **LSM write path** — the WAL, memtable, immutable mmap'd segments, and compaction provide the write,
  recovery, and snapshot-read model.
  ([design/ingestion-and-updates.md](docs/design/ingestion-and-updates.md) §3)
- **Versioned CPU ranking profiles** — optional static, linear, or bounded tree scoring runs only
  after Boolean truth, with deterministic integer scores and exact top-K delivery in every topology.
  ([ranking reference](docs/reference/ranking.md))

The lossless-signature contract is stated in [`AGENTS.md`](AGENTS.md) and developed in
[`docs/design/README.md`](docs/design/README.md) §2.

## Measurements

Versioned benchmark captures, machine details, workload definitions, and regression checks live in
[`docs/performance/`](docs/performance/README.md). Treat those results as measurements of the
documented workloads rather than capacity claims for an arbitrary corpus or deployment.

## Quickstart

The repository uses Rust 2021 and pins its toolchain in
[`engine/rust-toolchain.toml`](engine/rust-toolchain.toml).

```bash
cd engine
cargo build --release      # build
cargo test  --release      # run the default test suite
cargo run   --release --bin demo     # worked example end-to-end with explain output
```

Run the HTTP server:

```bash
cargo run --release --bin server          # listens on :9200
curl -X PUT localhost:9200/_doc/1 -H 'Content-Type: application/json' \
  -d '{"query": "(laptop,notebook) 16gb -refurbished"}'
curl -X POST localhost:9200/_search -H 'Content-Type: application/json' \
  -d '{"document": {"title": "Dell XPS 15 Laptop 16GB RAM 512GB SSD New"}}'
```

Full endpoint and flag reference: [`docs/reference/api.md`](docs/reference/api.md). Query language:
[`docs/reference/dsl.md`](docs/reference/dsl.md). The four documented deployment modes
(single-node, in-process cluster, Compose, and Helm), their bring-up commands, and their constraints:
[`docs/operations/deployment-modes.md`](docs/operations/deployment-modes.md).
For title-dependent ordering of confirmed matches, see the
[`ranking-profile reference`](docs/reference/ranking.md); the built-in `static_v1` profile requires
no additional configuration.

Use it as a library:

```rust
use reverse_rusty::{Engine, Normalizer};

let norm = Normalizer::default_vocab().unwrap();
let mut engine = Engine::new(norm);

let queries = vec![(1u64, "(laptop,notebook) 16gb -refurbished".to_string())];
engine.build_from_queries(&queries);

let mut scratch = reverse_rusty::segment::MatchScratch::new();
let mut out = Vec::new();
engine.match_title("Dell XPS 15 Laptop 16GB RAM 512GB SSD New", &mut scratch, &mut out, true);
// out contains the matching query IDs: [1]
```

See [`docs/reference/dsl.md`](docs/reference/dsl.md) for loading and learning vocabulary.

## Repository map

| Path | Purpose |
|---|---|
| [`engine/src/`](engine/src/) | Library, HTTP server, persistence, and cluster implementation |
| [`engine/tests/`](engine/tests/) | Integration, differential-oracle, durability, and stress coverage |
| [`engine/grpc/`](engine/grpc/) | Protobuf definitions and generated gRPC workspace member |
| [`engine/ref-matcher/`](engine/ref-matcher/) | Independent reference matcher used by correctness tests |
| [`deploy/`](deploy/) | Container image, Compose topology, Helm chart, and deployment smoke scripts |
| [`.github/workflows/`](.github/workflows/) | CI and release workflows |
| [`docs/`](docs/README.md) | Documentation hub, ownership rules, and the full documentation index |

Start with [`AGENTS.md`](AGENTS.md) before changing code. It contains the correctness invariants and
routes implementation tasks to the relevant design and operational pages. The primary documentation
areas are:

| Area | Contents |
|---|---|
| [`docs/reference/`](docs/reference/) | Query language, HTTP API, configuration, and compatibility contracts |
| [`docs/design/`](docs/design/README.md) | Current architecture and correctness model |
| [`docs/operations/`](docs/operations/) | Build, deployment, recovery, upgrade, sizing, and alerting procedures |
| [`docs/performance/`](docs/performance/README.md) | Benchmark captures, analysis, and measurement runbooks |
| [`docs/DECISIONS.md`](docs/DECISIONS.md) | ADR hub and area catalogs |
| [`docs/CHANGELOG.md`](docs/CHANGELOG.md) | Shipped project history |
| [`docs/roadmap.md`](docs/roadmap.md) | Unfinished work only |

## Build features

Feature definitions and dependency versions are authoritative in
[`engine/Cargo.toml`](engine/Cargo.toml).

| Command | Build |
|---|---|
| `cargo build --release` | Core library and default HTTP server |
| `cargo build --release --no-default-features` | Lean library build without the server stack |
| `cargo build --release --features distributed` | Default build plus the gRPC and Raft cluster stack |

## License

Licensed under the [MIT License](LICENSE). The crate is not published to crates.io
(`publish = false` in [`engine/Cargo.toml`](engine/Cargo.toml)).
