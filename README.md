<p align="center">
  <img height="280" alt="reverse_rusty" src="https://github.com/user-attachments/assets/ab6aeedb-0934-445e-8cb3-de6b726b19a0" />
</p>


# Reverse Rusty

<p align="center">
  <a href="https://github.com/dvideby0/reverse-rusty/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/dvideby0/reverse-rusty/actions/workflows/ci.yml/badge.svg" /></a>
</p>

A high-performance reverse query matching engine written in Rust. Given millions of stored
queries and an incoming document title, Reverse Rusty finds every query that matches — with
**zero false negatives guaranteed**.

Traditional search finds documents that match a query. Reverse Rusty inverts that: it finds
queries that match a document. This is called **percolation**, and it's useful any time you
need to monitor a stream of content against a large set of standing interest expressions.

> **Building on the code or contributing (incl. AI agents)?** Start at [`CLAUDE.md`](CLAUDE.md)
> for the architecture, invariants, and a task→doc router, then browse [`docs/`](docs/README.md).

## Use Cases

**Marketplace alerts** — Users save searches like `(laptop,notebook) 16gb -refurbished`.
When a new listing appears, Reverse Rusty checks it against millions of saved searches and
notifies every user whose query matches. No polling, no fan-out query storm.

**Content classification** — Define category rules as queries. Feed incoming product titles
through the engine and get back which categories apply. Handles synonyms, negations, and
required-any-of groups natively.

**Ad targeting** — Advertisers define targeting expressions; incoming page content is matched
against all active campaigns simultaneously. Sub-millisecond per-title latency makes this
viable at ad-serving speed.

**Compliance monitoring** — Regulatory rules expressed as queries are matched against a
stream of transaction descriptions, flagging anything that hits a pattern. The zero-false-negative
guarantee means nothing slips through.

**Price tracking** — Shoppers define product-intent queries. As prices update across
retailers, each update is percolated to find which watchers care about that product.

## How It Works

Reverse Rusty is modeled after Elasticsearch's percolate query, but purpose-built for short
product titles (5-20 words) rather than full-text documents.

There are two distinct phases: **compile time** (when queries are registered) and **match
time** (when documents arrive). All the expensive work — parsing, normalization, signature
optimization — happens at compile time. Match time is a tight, allocation-free integer
pipeline.

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
  │              │     │  guarantees retrieval. Classify query cost:       │
  │ • Index      │     │  A: rare anchor   ← ideal   C: broad ← quarantine │
  │ • ExactStore │     │  B: paired anchor ← ok      D: no anchor ← reject │
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
  ┌──────────────┐     ┌───────────────────────────────────────────────────┐
  │   Matched    │◀────│  Exact Verification (integer-only, per candidate): │
  │  Query IDs   │     │  1. common-mask gate (2 ops, ~80% reject)         │
  │  [42, 17]    │     │  2. required  3. forbidden  4. any-of groups      │
  └──────────────┘     └───────────────────────────────────────────────────┘
```

*The authoritative engineering rendering of this pipeline lives in
[`docs/design/README.md`](docs/design/README.md) §1.*

### Key techniques

Each links to the design doc that details it:

- **Signature-cover optimizer** — selects a *lossless* minimal set of signature keys per query, so any
  title that could match always generates a retrieving signature. This is what guarantees zero false
  negatives. ([design/matching.md](docs/design/matching.md) §1)
- **Common-mask gate** — the 64 hottest features get a bit in a `u64` mask; two bitwise ops reject
  ~80% of candidates before any other memory access. ([design/matching.md](docs/design/matching.md) §3)
- **Three-tier adaptive postings** — inline (≤8) → sorted `Vec` (≤256) → roaring bitmap (>256), chosen
  by cardinality. ([design/matching.md](docs/design/matching.md) §2)
- **Broad-query cost classes (A/B/C/D)** — low-selectivity queries are detected at compile time and
  quarantined to a separate lane instead of poisoning candidate selectivity.
  ([design/matching.md](docs/design/matching.md) §4)
- **Cache-line blocked bloom filters** — each segment carries a 512-bit-block filter answering "could
  this signature exist here?" in one cache-line fetch. ([design/ingestion-and-updates.md](docs/design/ingestion-and-updates.md) §6)
- **LSM write path** — WAL + memtable + immutable mmap'd segments + score-based compaction give
  immediate visibility and crash recovery without blocking readers.
  ([design/ingestion-and-updates.md](docs/design/ingestion-and-updates.md) §3)

The non-negotiable correctness contract behind all of this (the *lossless signature cover*) is stated
in [`CLAUDE.md`](CLAUDE.md) and proven in [`docs/design/README.md`](docs/design/README.md) §2.

## Performance

Selective path **≈710k titles/sec/core** at 1M queries (≈255× the spec target), a flat **~54
candidates/title** regardless of corpus size, and **zero** false negatives — verified by a differential
oracle. Full methodology, the 100M-query extrapolation, and the machine-independent regression
invariants are in [`docs/performance/`](docs/performance/README.md).

## Quickstart

Uses the 2021 edition; the toolchain is pinned in [`engine/rust-toolchain.toml`](engine/rust-toolchain.toml)
(rustup auto-installs the pinned rustc). The release profile enables LTO, a single codegen unit, and
`opt-level=3` for maximum match-path throughput.

```bash
cd engine
cargo build --release      # build
cargo test  --release      # run the differential oracle + parser/error-path tests
cargo run   --release --bin demo     # worked example end-to-end with explain output
```

**Run the server** (Elasticsearch-style REST API):

```bash
cargo run --release --bin server          # listens on :9200
curl -X PUT localhost:9200/_doc/1 -H 'Content-Type: application/json' \
  -d '{"query": "(laptop,notebook) 16gb -refurbished"}'
curl -X POST localhost:9200/_search -H 'Content-Type: application/json' \
  -d '{"document": {"title": "Dell XPS 15 Laptop 16GB RAM 512GB SSD New"}}'

# Exact bounded local top-K ranking (ADR-107/108):
curl -X PUT localhost:9200/_doc/2 -H 'Content-Type: application/json' \
  -d '{"query":"dell laptop 16gb","rank_fields":{"priority":50}}'
curl -X POST localhost:9200/v2/_search -H 'Content-Type: application/json' \
  -d '{"document":{"title":"Dell XPS 15 Laptop 16GB RAM"},"size":10}'
```

Full endpoint and flag reference: [`docs/reference/api.md`](docs/reference/api.md). Query language:
[`docs/reference/dsl.md`](docs/reference/dsl.md). Deploying it — the four supported modes
(single-node · in-process cluster · Compose · Helm) with exact commands and the v1 constraints:
[`docs/operations/deployment-modes.md`](docs/operations/deployment-modes.md).

**Use as a library:**

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

## Documentation

| Doc | What's in it |
|---|---|
| [`CLAUDE.md`](CLAUDE.md) | Architecture, correctness invariants, and a task→doc router (start here to build on the code) |
| [`docs/`](docs/README.md) | Documentation hub — index, single-source-of-truth registry, conventions |
| [`docs/reference/api.md`](docs/reference/api.md) · [`dsl.md`](docs/reference/dsl.md) | REST API and query-DSL reference |
| [`docs/design/`](docs/design/README.md) | How it works: normalization, matching, ingestion/LSM, clustering |
| [`docs/performance/`](docs/performance/README.md) | Measured results, bottleneck analysis, benchmark runbook |
| [`docs/STATUS.md`](docs/STATUS.md) · [`roadmap.md`](docs/roadmap.md) · [`DECISIONS.md`](docs/DECISIONS.md) | What's built (STATUS), what's next (the prioritized roadmap), and the architecture decision records |

## Dependencies

Reverse Rusty is built on a minimal dependency set (versions pinned in
[`engine/Cargo.toml`](engine/Cargo.toml)):

| Crate | Purpose |
|---|---|
| `daachorse` | Double-array Aho-Corasick automaton for multiword alias matching |
| `memmap2` | Memory-mapped segment files for zero-copy reads |
| `roaring` | Compressed bitmaps for large posting lists |
| `rayon` | Parallel matching across titles |
| `arc-swap` | Lock-free snapshot reads (zero reader/writer contention) |
| `axum` + `tokio` | HTTP server |
| `serde` + `serde_json` | JSON serialization |
| `clap` | CLI argument parsing |
| `tracing` | Structured logging |
| `prometheus` | Metrics export |

The lean core (`cargo build --no-default-features`) depends only on `daachorse`, `memmap2`, `roaring`,
`rayon`, `arc-swap`, and `serde`/`serde_json`; the `axum`/`tokio`/`clap`/`tracing`/`prometheus` server and
observability crates sit behind the default-on `server` feature (ADR-028; the lean build is enforced by a
`check.sh` lane). The optional `distributed` feature adds the gRPC/Raft cluster stack. See
[`docs/STATUS.md`](docs/STATUS.md) and [`engine/Cargo.toml`](engine/Cargo.toml).

## License

Licensed under [the MIT License](LICENSE) — a permissive, widely-used license that allows reuse
with attribution. It remains unpublished to crates.io (`publish = false`; see the note in
[`engine/Cargo.toml`](engine/Cargo.toml)).
