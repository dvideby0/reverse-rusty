<p align="center">
  <img height="280" alt="reverse_rusty" src="https://github.com/user-attachments/assets/ab6aeedb-0934-445e-8cb3-de6b726b19a0" />
</p>


# Reverse Rusty

A high-performance reverse query matching engine written in Rust. Given millions of stored
queries and an incoming document title, Reverse Rusty finds every query that matches — with
**zero false negatives guaranteed**.

Traditional search finds documents that match a query. Reverse Rusty inverts that: it finds
queries that match a document. This is called **percolation**, and it's useful any time you
need to monitor a stream of content against a large set of standing interest expressions.

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

### The Big Picture

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
  │  Segment     │     │                                                   │
  │              │     │  Pick the smallest set of signatures that         │
  │ • Index      │     │  guarantees retrieval. Classify query cost:       │
  │ • ExactStore │     │                                                   │
  │ • Filter     │     │  A: rare anchor (1 sig)     ← ideal               │
  │              │     │  B: paired anchor (1 sig)   ← acceptable          │
  │              │     │  C: broad (fan-out sigs)    ← quarantined         │
  └──────────────┘     │  D: no anchor possible      ← rejected            │
                       └───────────────────────────────────────────────────┘


                        MATCH TIME (per incoming title)
                        ═════════════════════════════

  ┌──────────────┐     ┌───────────┐     ┌────────────┐     ┌──────────────┐
  │  Raw Title   │────▶│ Normalize │────▶│  Generate  │────▶│    Probe     │
  │              │     │           │     │  Title     │     │  Candidate   │
  │ "Vintage     │     │ Same      │     │ Signatures │     │    Index     │
  │  Leather     │     │ pipeline  │     │            │     │              │
  │  Jacket XL"  │     │ as query  │     │ All combos │     │ sig → [IDs]  │
  └──────────────┘     └───────────┘     │ of feature │     └──────┬───────┘
                                         │ pairs      │            │
                                         └────────────┘            ▼
  ┌──────────────┐     ┌───────────────────────────────────────────────────┐
  │   Matched    │◀────│           Exact Verification                      │
  │  Query IDs   │     │                                                   │
  │              │     │  For each candidate, integer-only checks:         │
  │  [42, 17]    │     │                                                   │
  │              │     │  1. Common-mask gate  (2 bitwise ops, ~80% reject)│
  │              │     │  2. Required features (binary search)             │
  │              │     │  3. Forbidden features (binary search)            │
  │              │     │  4. Any-of groups     (binary search per group)   │
  └──────────────┘     └───────────────────────────────────────────────────┘
```

### Key Algorithms

#### Signature-Cover Optimizer

The core insight: instead of probing every stored query against every title, we only need to
check queries that *could* match. The signature-cover optimizer selects a minimal set of
**signature keys** for each query such that any title satisfying the query's positive
requirements will generate at least one matching signature.

```
  Query: "vintage (leather,suede) jacket -replica"
  Required features: [vintage, jacket]
  Any-of features:   [leather, suede]

  Optimizer picks the rarest required feature as the anchor:
    If "vintage" is rare  → signature = hash(vintage)        → Cost Class A
    If both are common    → signature = hash(vintage,jacket) → Cost Class B
    If all are very common → broad lane                      → Cost Class C
    If no positive features → rejected                       → Cost Class D
```

This is what guarantees **zero false negatives**: the signature set is *lossless*. Any title
that could match will always generate a signature that retrieves the query as a candidate.
False-positive candidates are fine — the exact matcher filters them cheaply.

#### Common-Mask Gate

The 64 most frequent features in the dictionary each get a bit position in a 64-bit mask.
Every stored query has two mask words: one for its required features, one for its forbidden
features. Every title computes its own mask word during normalization.

```
  Title mask:     0b...1101_0110
  Query req mask: 0b...0100_0010
  Query forb mask:0b...0000_1000

  Check 1: (req_mask & title_mask) == req_mask?   → all required bits present?
  Check 2: (forb_mask & title_mask) == 0?          → no forbidden bits present?

  Two bitwise operations. Rejects ~80% of candidates before touching any
  other memory. The remaining candidates fall through to binary-search
  verification on the full feature lists.
```

#### Three-Tier Adaptive Postings

Each signature key maps to a posting list of query IDs. The posting list representation
adapts based on cardinality to balance memory and speed:

```
  Tier 1: Inline (≤8 IDs)
  ┌──────────────────────────────────┐
  │ [u32; 8] array on the stack     │  No heap allocation.
  │ Direct iteration.               │  Most postings live here.
  └──────────────────────────────────┘

  Tier 2: Vec (9–256 IDs)
  ┌──────────────────────────────────┐
  │ Sorted Vec<u32> on the heap     │  Sorted by construction
  │ Binary search for membership.   │  (append-only, monotonic IDs).
  └──────────────────────────────────┘

  Tier 3: Roaring Bitmap (>256 IDs)
  ┌──────────────────────────────────┐
  │ Compressed bitmap (roaring)     │  Efficient union/intersection.
  │ Handles millions of IDs.        │  ~2 bytes/ID typical.
  └──────────────────────────────────┘
```

Postings are append-only within a segment. Since local IDs are issued in order, postings are
sorted by construction — no per-insert sort or dedup is needed.

#### Cache-Line Blocked Bloom Filters

Each immutable segment has a bloom filter over its signature keys. Before probing a segment's
index, the filter answers "could this signature exist here?" in a single cache-line fetch.

```
  Filter layout (512-bit blocks = 64 bytes = 1 cache line):

  ┌────────────────────────────────────────────────────┐
  │ Block 0: [u64; 8]  ← 512 bits, one cache line     │
  ├────────────────────────────────────────────────────┤
  │ Block 1: [u64; 8]                                  │
  ├────────────────────────────────────────────────────┤
  │ Block 2: [u64; 8]                                  │
  ├────────────────────────────────────────────────────┤
  │ ...                                                │
  └────────────────────────────────────────────────────┘

  Lookup: hash(key) → block index → 6 probe bits within that block
  Total memory accesses: 1 (the entire check fits in one cache line)
  False positive rate: ~1% at 10 bits/key
  Memory: ~1.25 bytes per key
```

This design follows RocksDB's Full Filter approach. Classic bloom filters scatter probes
across memory, causing multiple cache misses — slower than just checking the hash map
directly. By confining all probes to a single cache line, the filter stays within the
one-memory-access budget of the hot path.

#### LSM Write Path

Queries flow through a log-structured merge (LSM) pipeline that provides immediate
visibility and crash recovery without blocking readers:

```
  ┌───────────┐
  │  Incoming  │
  │  Queries   │
  └─────┬─────┘
        │
        ▼
  ┌───────────┐     Write-ahead log (WAL) ensures
  │    WAL    │     crash recovery. CRC-framed
  │  (append) │     entries, sequential writes.
  └─────┬─────┘
        │
        ▼
  ┌───────────┐     In-memory, mutable. Queries are
  │ Memtable  │     searchable immediately after insert.
  │ (active)  │     Flushes when size threshold is hit.
  └─────┬─────┘
        │ flush
        ▼
  ┌───────────┐     Immutable on-disk segments with
  │ Segment 0 │     frozen hash tables (mmap'd).
  └───────────┘     Each has its own index, exact
  ┌───────────┐     store, and bloom filter.
  │ Segment 1 │
  └───────────┘
  ┌───────────┐
  │ Segment 2 │◄── compaction merges segments,
  └───────────┘    reclaims tombstones, controls
                   read amplification.

  Deletes: tombstone in memtable → reclaimed during compaction.
  Reads:   probe memtable + all segments, union results.
  Compaction: score-based merge selection (inspired by ClickHouse).
```

#### Exact Verification Pipeline

When a candidate query ID is retrieved from the index, it passes through a four-stage
verification pipeline. Each stage is progressively more expensive, but earlier stages reject
the vast majority of candidates cheaply:

```
  Candidate ID
       │
       ▼
  ┌─────────────────────────────┐
  │ Stage 1: Common-Mask Gate   │  2 bitwise ops on u64 masks.
  │ (required bits present?     │  Rejects ~80% of candidates.
  │  forbidden bits absent?)    │  Cost: ~1 nanosecond.
  └─────────┬───────────────────┘
            │ pass
            ▼
  ┌─────────────────────────────┐
  │ Stage 2: Required Tail      │  Binary search for each non-mask
  │ (remaining required         │  required feature in the title's
  │  features all present?)     │  sorted feature list.
  └─────────┬───────────────────┘
            │ pass
            ▼
  ┌─────────────────────────────┐
  │ Stage 3: Forbidden Tail     │  Binary search for each non-mask
  │ (no remaining forbidden     │  forbidden feature. Reject on
  │  features present?)         │  first hit.
  └─────────┬───────────────────┘
            │ pass
            ▼
  ┌─────────────────────────────┐
  │ Stage 4: Any-Of Groups      │  For each OR group, binary search
  │ (each group has ≥1 member   │  for any member. Reject if an
  │  present?)                  │  entire group has no hits.
  └─────────┬───────────────────┘
            │ pass
            ▼
       ✓ MATCH
```

All four stages operate on dense integer feature IDs — no strings, no regex, no allocation.
The struct-of-arrays (SoA) memory layout keeps related data contiguous for cache efficiency:
mask words are packed together, required-feature blobs are packed together, and so on.

## Performance

Measured on Apple M-series, 4 cores, 3.8 GiB:

| Metric | Value |
|---|---|
| Throughput (selective) | **710k titles/sec/core** @ 1M queries |
| Parallel speedup | ~3.8x on 4 threads |
| Candidates per title | ~54 mean, 112 p99 (flat across corpus sizes) |
| False negatives | **0** (verified by differential oracle) |
| Update throughput | ~750k queries/sec/core |
| Build throughput | ~650k queries/sec/core |
| Memory per query | ~256 bytes |

## Installation

### Requirements

- Rust 1.70+ (2021 edition)
- Cargo

### Build

```bash
cd engine
cargo build --release
```

The release profile enables LTO, single codegen unit, and `opt-level=3` for maximum
throughput on the match path.

### Run Tests

```bash
cd engine
cargo test --release
```

This runs the differential correctness oracle (brute-force vs engine) plus parser and
error-path regression tests.

## Query DSL

Queries are written in a simple DSL that supports required terms, phrases, any-of groups,
and negations. All top-level clauses are implicitly ANDed together.

### Operators

| Syntax | Meaning | Example |
|---|---|---|
| `word` | Required term (AND) | `laptop` |
| `"a b"` | Required phrase (AND) | `"running shoes"` |
| `(a,b,c)` | Any-of group (OR — at least one must match) | `(red,blue,green)` |
| `-word` | Must not contain (NOT) | `-refurbished` |
| `-"a b"` | Must not contain phrase (NOT) | `-"for parts"` |
| `-(a,b,c)` | Must not contain any of (NOT + OR) | `-(used,open box,returned)` |

### Combining Operators

Every top-level element is required (AND logic). Use groups for OR within that structure,
and prefix with `-` for exclusion.

```
# All of these terms are required (AND):
vintage leather jacket

# At least one color required (OR), plus a required term:
(brown,tan,cognac) leather jacket

# Required terms with exclusions (AND + NOT):
vintage leather jacket -wallet -belt

# Full example using all operators:
vintage (leather,suede) "bomber jacket" (brown,tan,black) -womens -(replica,faux,vegan)
```

This last query matches titles that contain: `vintage`, either `leather` or `suede`, the
phrase `bomber jacket`, at least one of `brown`/`tan`/`black` — but rejects any title
containing `womens`, `replica`, `faux`, or `vegan`.

### Normalization

Both queries and titles pass through the same normalization pipeline before matching:

- **Case folding and diacritic removal** — `Café` becomes `cafe`, `Jokić` becomes `jokic`
- **Number disambiguation** — years, quantities, model numbers, and other numeric types are
  classified separately based on context
- **Domain-agnostic by default** — the normalizer ships with no hardcoded vocabulary. All
  domain knowledge (phrases, synonyms, graders) is supplied via vocabulary configuration

Because the same normalizer processes both queries and titles, synonyms and aliases work
automatically — a query containing `sneakers` will match a title containing `running shoes`
if those are configured as equivalent in the vocabulary.

### Vocabulary

The engine's domain knowledge is managed through a **vocabulary** — a JSON-serializable
collection of phrases, synonyms, grader keywords, and grade words. Vocabulary can come from
three sources:

1. **Learned from queries** — the engine scans any-of groups in your query corpus to discover
   synonym relationships. If many queries contain `(rookie,rc)`, the engine learns that
   `rookie ≈ rc` and maps both to the same canonical feature.

2. **Manual configuration** — add phrases, synonyms, graders, and grade words through the
   `Vocab` API or the `PUT /_vocab` REST endpoint.

3. **File-based** — load a vocabulary JSON file at startup with `--vocab-file`, or save/load
   at runtime. Vocabularies are composable via `merge()`.

```json
{
  "synonyms": [
    {"token": "rc", "canonical": "term:rookie", "kind": "category"},
    {"token": "ud", "canonical": "term:upper_deck", "kind": "generic"}
  ],
  "phrases": [
    {"tokens": ["upper", "deck"], "canonical": "term:upper_deck", "kind": "generic"}
  ],
  "graders": ["psa", "bgs", "sgc"],
  "grade_words": ["gem", "mint", "pristine"]
}
```

The `NormalizerBuilder` API remains available for programmatic vocabulary construction when
you need fine-grained control.

## Usage

### As a Server

Start the HTTP server:

```bash
cd engine
cargo run --release --bin server
```

Options:

| Flag | Default | Description |
|---|---|---|
| `--port` | 9200 | Port to listen on |
| `--data-dir` | *(in-memory)* | Persistence directory for segments and WAL |
| `--load-file` | — | Pre-load queries from a CSV or JSONL file at startup |
| `--vocab-file` | — | Load vocabulary from a JSON file at startup |
| `--threads` | *(physical cores)* | Number of rayon worker threads |
| `--include-broad` | false | Include broad-lane (class C) queries in results |
| `--drain-timeout` | 30 | Graceful shutdown timeout in seconds |
| `--log-format` | pretty | `pretty` for human-readable, `json` for structured |
| `--slow-query-threshold-ms` | 1000 | Log searches exceeding this at `warn` level (0 disables) |
| `--max-segments` | 8 | Max base segments before compaction triggers |
| `--memtable-flush-threshold` | 100000 | Memtable entries before auto-flush |
| `--max-query-length` | 10000 | Maximum query string length in bytes |
| `--max-query-clauses` | 256 | Maximum clauses per query |
| `--max-anyof-group-size` | 64 | Maximum members in an any-of group |

Example with persistence, vocabulary, and pre-loaded queries:

```bash
cargo run --release --bin server -- \
  --port 9200 \
  --data-dir ./data \
  --vocab-file vocab.json \
  --load-file queries.csv \
  --threads 8 \
  --log-format json
```

The server handles SIGINT/SIGTERM gracefully — it drains in-flight requests, flushes the
memtable, and syncs the WAL before exiting.

### As a Library

```rust
use percolator::{Engine, EngineConfig, Normalizer, Vocab};

// Option 1: Empty vocabulary (domain-agnostic, relies on exact token matching)
let norm = Normalizer::default_vocab().unwrap();
let mut engine = Engine::new(norm);

// Option 2: Load vocabulary from a file
let vocab = Vocab::load_json("my-domain.json".as_ref()).unwrap();
let mut engine = Engine::with_vocab(vocab, EngineConfig::default()).unwrap();

// Option 3: Learn vocabulary from query data, then build the engine
let queries = vec![
    (1, "(laptop,notebook) 16gb -refurbished".to_string()),
    (2, "vintage leather jacket -(replica,faux)".to_string()),
];
let learned = percolator::vocab::learn_from_queries(&queries, 2);
let mut engine = Engine::with_vocab(learned, EngineConfig::default()).unwrap();

// Register queries
engine.build_from_queries(&queries);

// Match a title
let mut scratch = percolator::segment::MatchScratch::new();
let mut out = Vec::new();
engine.match_title("Dell XPS 15 Laptop 16GB RAM 512GB SSD New", &mut scratch, &mut out, true);
// out contains query IDs: [1]
```

### Demo

Run the built-in worked example with explain output:

```bash
cd engine
cargo run --release --bin demo
```

### Benchmarks

```bash
cd engine
cargo run --release --bin bench -- <queries> <titles> <broad_frac> <skew> <reps>

# Example: 1M queries, 5k titles, no broad queries, Zipf skew 2.0, 60 reps
cargo run --release --bin bench -- 1000000 5000 0.0 2.0 60
```

## REST API

The server exposes an Elasticsearch-style REST API.

### `GET /` — API Root

```bash
curl localhost:9200/
```

```json
{
  "name": "percolator",
  "version": "0.1.0",
  "tagline": "you know, for matching"
}
```

### `PUT /_doc/{id}` — Register a Query

```bash
curl -X PUT localhost:9200/_doc/1 \
  -H 'Content-Type: application/json' \
  -d '{"query": "(laptop,notebook) 16gb -refurbished"}'
```

```json
{"_id": 1, "result": "created", "error": null}
```

If the query fails to parse or has no anchorable features (cost class D), the response
includes the error:

```json
{"_id": 1, "result": "rejected", "error": "query has no anchorable feature (cost class D)"}
```

### `GET /_doc/{id}` — Retrieve a Query

```bash
curl localhost:9200/_doc/1
```

```json
{"_id": 1, "found": true, "_source": {"query": "dell laptop"}}
```

If the query ID doesn't exist:

```json
{"_id": 1, "found": false}
```

### `DELETE /_doc/{id}` — Remove a Query

```bash
curl -X DELETE localhost:9200/_doc/1
```

```json
{"_id": 1, "result": "deleted", "deleted_count": 1}
```

If the query ID doesn't exist (or was already deleted):

```json
{"_id": 1, "result": "not_found"}
```

### `POST /_search` — Percolate Titles

Match a single title against all stored queries:

```bash
curl -X POST localhost:9200/_search \
  -H 'Content-Type: application/json' \
  -d '{"document": {"title": "Dell XPS 15 Laptop 16GB RAM 512GB SSD New"}}'
```

```json
{
  "took_ms": 0.42,
  "hits": {
    "total": 1,
    "hits": [{"_id": 1, "_source": {"query": "dell laptop"}}]
  }
}
```

Optional request fields:

| Field | Default | Description |
|---|---|---|
| `timeout_ms` | 30000 | Per-request timeout in milliseconds (returns 408 on expiry) |
| `size` | 1000 | Maximum number of hits to return |
| `from` | 0 | Offset into the result set for pagination |
| `include_source` | true | Include original query text in each hit |

`total` always reflects the full match count; `hits` is the paginated window.
Set `include_source: false` to skip query text lookup for faster responses.

Match multiple titles in a single request:

```bash
curl -X POST localhost:9200/_search \
  -H 'Content-Type: application/json' \
  -d '{
    "documents": [
      {"title": "Dell XPS 15 Laptop 16GB RAM 512GB SSD New"},
      {"title": "Vintage Brown Leather Bomber Jacket Size L"}
    ],
    "timeout_ms": 5000
  }'
```

```json
{
  "took_ms": 0.87,
  "hits": {
    "total": 2,
    "hits": [
      {"_id": 1, "_source": {"query": "dell laptop"}},
      {"_id": 2, "_source": {"query": "leather jacket"}}
    ]
  },
  "slots": [
    {
      "slot": 0,
      "total": 1,
      "hits": [{"_id": 1, "_source": {"query": "dell laptop"}}],
      "stats": {
        "unique_candidates": 15,
        "postings_scanned": 47,
        "matches": 1,
        "probes_attempted": 28,
        "probes_skipped": 12
      }
    },
    {
      "slot": 1,
      "total": 1,
      "hits": [{"_id": 2, "_source": {"query": "leather jacket"}}],
      "stats": {
        "unique_candidates": 9,
        "postings_scanned": 22,
        "matches": 1,
        "probes_attempted": 18,
        "probes_skipped": 8
      }
    }
  ]
}
```

The `stats` object per slot shows how much work the engine did: how many candidates were
retrieved from the index, how many posting lists were scanned, how many bloom-filter probes
were skipped, and how many candidates survived to become confirmed matches.

### `POST /_bulk` — Bulk Ingest

NDJSON format, compatible with Elasticsearch's `_bulk` API:

```bash
curl -X POST localhost:9200/_bulk \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary @- <<'EOF'
{"index": {"_id": 1}}
{"query": "(laptop,notebook) 16gb -refurbished"}
{"index": {"_id": 2}}
{"query": "vintage leather jacket -(replica,faux)"}
{"index": {"_id": 3}}
{"query": "\"running shoes\" (nike,adidas) -used"}
EOF
```

```json
{
  "took_ms": 1.23,
  "errors": false,
  "items": [
    {"index": {"_id": 1, "status": 201, "error": null}},
    {"index": {"_id": 2, "status": 201, "error": null}},
    {"index": {"_id": 3, "status": 201, "error": null}}
  ]
}
```

If any query fails, `errors` is `true` and that item gets a `400` status with the parse
error message. Successfully ingested queries in the same batch are unaffected.

### `POST /_flush` — Flush Memtable

Flush the in-memory memtable to an immutable on-disk segment:

```bash
curl -X POST localhost:9200/_flush
```

```json
{
  "acknowledged": true,
  "total_queries": 3,
  "base_segments": 1
}
```

### `POST /_compact` — Force Compaction

Trigger segment compaction to merge segments and reclaim tombstones:

```bash
curl -X POST localhost:9200/_compact
```

When compaction runs:

```json
{
  "acknowledged": true,
  "segments_merged": 2,
  "entries_before": 150,
  "entries_after": 142,
  "tombstones_reclaimed": 8
}
```

When no compaction is needed:

```json
{
  "acknowledged": true,
  "message": "no compaction needed"
}
```

### `GET /_stats` — Engine Metrics (JSON)

```bash
curl localhost:9200/_stats
```

```json
{
  "total_queries": 3,
  "base_segments": 1,
  "memtable_entries": 0,
  "dict_features": 24,
  "rejected_parse": 0,
  "rejected_class_d": 0,
  "class_counts": {"a": 2, "b": 1, "c": 0, "d": 0},
  "segment_sizes": [3],
  "segment_holes": [0.0],
  "memory": {
    "exact_bytes": 1024,
    "index_bytes": 2048,
    "filter_bytes": 512
  }
}
```

- **class_counts** — how many queries fell into each cost class (A is best, D is rejected)
- **segment_holes** — fraction of tombstoned entries per segment (drives compaction decisions)
- **memory** — breakdown of heap usage across the exact store, candidate index, and bloom filters

### `GET /_cat/stats` — Engine Metrics (Human-Readable)

```bash
curl localhost:9200/_cat/stats
```

```
queries          3
segments         1 (+ memtable: 0)
features         24
class A/B/C/D    2 / 1 / 0 / 0
rejected parse   0
rejected classD  0
memory           3584 bytes (~0.0 MB)

segment  entries  holes
0        3        0.00%
```

### `GET /_health` — Health Check

```bash
curl localhost:9200/_health
```

```json
{
  "status": "green",
  "total_queries": 3,
  "wal_healthy": true,
  "persistence_healthy": true,
  "skipped_segments": 0
}
```

| Status | Meaning |
|---|---|
| `green` | All systems healthy |
| `yellow` | Some segments were skipped during load (data may be incomplete) |
| `red` | WAL or persistence subsystem is unhealthy |

### `GET /_metrics` — Prometheus Metrics

```bash
curl localhost:9200/_metrics
```

Returns metrics in Prometheus text exposition format for scraping by Prometheus, Grafana
Agent, or compatible collectors.

### `GET /_vocab` — Current Vocabulary

```bash
curl localhost:9200/_vocab
```

```json
{
  "synonyms": [
    {"token": "rc", "canonical": "term:rookie", "kind": "generic"}
  ],
  "phrases": [
    {"tokens": ["upper", "deck"], "canonical": "term:upper_deck", "kind": "generic"}
  ],
  "graders": ["psa"],
  "grade_words": ["gem"]
}
```

### `PUT /_vocab` — Replace Vocabulary

Replace the engine's vocabulary. If queries have already been ingested, the response includes
a warning — you should reingest for consistent matching.

```bash
curl -X PUT localhost:9200/_vocab \
  -H 'Content-Type: application/json' \
  -d '{"synonyms": [{"token": "rc", "canonical": "term:rookie", "kind": "category"}], "phrases": [], "graders": [], "grade_words": []}'
```

```json
{
  "acknowledged": true,
  "warning": "normalizer changed with existing queries; reingest for consistent matching"
}
```

### `POST /_vocab/learn` — Learn Vocabulary from Queries

Send raw query text to discover synonym relationships from any-of groups. Returns the
learned vocabulary without applying it — review and then `PUT /_vocab` to use it.

```bash
curl -X POST localhost:9200/_vocab/learn \
  -H 'Content-Type: application/json' \
  -d '{
    "queries": [[1, "(rookie,rc) 2024"], [2, "(rookie,rc) 2023"]],
    "min_count": 2
  }'
```

```json
{
  "synonyms": [
    {"token": "rc", "canonical": "term:rookie", "kind": "generic"}
  ],
  "phrases": [],
  "graders": [],
  "grade_words": []
}
```

The `min_count` parameter (default: 2) controls how many times a synonym pair must appear
across different queries before it's included. Higher values reduce noise.

### All Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/` | GET | Version info |
| `/_doc/{id}` | GET | Retrieve a stored query |
| `/_doc/{id}` | PUT | Register a single query |
| `/_doc/{id}` | DELETE | Remove a stored query |
| `/_search` | POST | Percolate one or more titles |
| `/_bulk` | POST | NDJSON bulk ingest |
| `/_flush` | POST | Flush memtable to immutable segment |
| `/_compact` | POST | Force segment compaction |
| `/_stats` | GET | JSON metrics snapshot |
| `/_cat/stats` | GET | Human-readable metrics |
| `/_health` | GET | Health check (green/yellow/red) |
| `/_metrics` | GET | Prometheus text exposition format |
| `/_vocab` | GET | Current vocabulary as JSON |
| `/_vocab` | PUT | Replace vocabulary |
| `/_vocab/learn` | POST | Learn synonyms from raw query text |

## Dependencies

Reverse Rusty is built on a minimal dependency set:

| Crate | Purpose |
|---|---|
| `daachorse` | Double-array Aho-Corasick automaton for multiword alias matching |
| `memmap2` | Memory-mapped segment files for zero-copy reads |
| `roaring` | Compressed bitmaps for large posting lists |
| `rayon` | Parallel matching across titles |
| `axum` + `tokio` | HTTP server (server binary only) |
| `serde` + `serde_json` | JSON serialization (server binary only) |
| `clap` | CLI argument parsing (server binary only) |
| `tracing` | Structured logging (server binary only) |
| `prometheus` | Metrics export (server binary only) |

The core matching library depends only on daachorse, memmap2, roaring, and rayon.

## License

See [LICENSE](LICENSE) for details.
