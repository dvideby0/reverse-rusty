# Prior Art — Reverse Product-Query Matchers

*Scope: survey the battle-tested systems named in the spec, extract the data structures /
invariants / layouts worth borrowing, and record what we deliberately reject and why. Feeds
directly into the [design docs](../design/README.md). This doc answers "how to gate
selectively"; its complement — hot/broad predicates, duplicate queries, shared evaluation, and
self-tuning classification — is [`broad-scaling-prior-art.md`](broad-scaling-prior-art.md).*

## The problem in one paragraph

We have ~100M *stored queries* expressing product intent ("1994 Upper Deck Michael Jordan
SP Preview PSA 10, not auto, not BGS/SGC"). A stream of ~10M short *listing titles* per
hour arrives (~2.8k titles/sec). For each title we must return the set of stored queries it
satisfies, in near-real-time, with **zero false negatives** for supported semantics, while
queries are updated frequently. This is the classic *reverse search / percolation /
prospective search* shape, but specialized to short product titles rather than arbitrary
documents. The dominant cost in every prior system is the same: **how do you avoid
evaluating all N stored queries against each incoming document?** Everything below is
ultimately about that one question.

---

## 1. Lucene Monitor (and its ancestor, Luwak)

**What it is.** Lucene's `monitor` module holds a set of stored Lucene queries and matches
incoming documents against them. It descends from Flax's *Luwak* library. The central
abstraction is the `Presearcher`: it "reduce[s] the number of queries actually run against a
Document" by defining (a) how stored queries are indexed and (b) how an incoming document is
turned into a query against that index.

**How the presearcher works.** When a document arrives, the `Monitor` builds a tiny in-memory
index over just that one document, reads the document's term dictionary, and constructs a
disjunction (OR) query from those terms. It runs that disjunction against the *query index* to
retrieve the small set of candidate stored queries that could possibly match, and only those
candidates are actually executed against the document.

**The key idea — term extraction by query decomposition.** `TermFilteredPresearcher` runs a
`QueryAnalyzer` that builds a tree representation of each stored query and selects a *minimal
set of terms that uniquely gates* the query. The decisive invariant:

- For a **conjunction** (boolean MUST, phrase, span, interval), you only need to extract **one**
  term — if the document lacks that one anchor term, the whole conjunction cannot match. A phrase
  query on `the quick brown fox` can be gated on just `brown`.
- For a **disjunction** (should/OR), you must extract a term from **every** branch — missing any
  branch could drop a real match.

`MultipassTermFilteredPresearcher` improves selectivity by indexing several terms per query
across multiple "passes" / fields, and supports a minimum term weight so that ultra-common terms
(`the`) are avoided as anchors in favour of rarer, more selective ones.

**What we borrow:**

1. **The two-phase architecture** — *cheap candidate retrieval → exact verification* — is exactly
   right and we adopt it wholesale.
2. **The decomposition invariant** (one anchor per conjunction, all-branches per disjunction) is
   the formal core of our "lossless signature cover" requirement. Our compiler generalizes it from
   single terms to multi-feature *signatures*.
3. **Choosing rare anchors over common ones.** Their "minimum term weight" is the seed of our
   frequency-driven signature optimizer.
4. **Explain/debug.** Monitor exposes presearcher debug info ("why was this query a candidate?").
   We make this a first-class, always-available capability.

**What we reject / improve:**

- Monitor builds a **fresh Lucene index per document**. For 2.8k short titles/sec that per-doc
  indexing overhead (analysis, posting construction, term-dict build, segment teardown) is pure
  waste. We replace it with **allocation-free feature extraction** into dense integer IDs.
- It gates on **raw terms**. Raw terms are a weak signal for product titles: `10` matches grade,
  year suffix, lot count, set number… We gate on **semantic features** (`grade:10`,
  `grader_grade:psa10`) which are far more selective.
- A single anchor per conjunction is *minimal* but not *cost-optimal* — it can still land on a
  hot term. We pick anchors (signatures) by **expected candidate cost**, not just by existence.

---

## 2. Elasticsearch / OpenSearch percolator

**What it is.** The `percolator` field type stores queries; a `percolate` query later matches a
document (or a batch of documents) against the stored set. It is the productionized, distributed
descendant of the same idea as Monitor and shares Lucene's term-extraction machinery.

**Candidate selection.** At index time the engine **extracts query terms and stores them
alongside the percolator query**. At percolate time it selects candidate queries using those
extracted terms before running the in-memory verification — described in the docs as "an
important performance optimization … it can significantly reduce the number of candidate matches
the in-memory index needs to evaluate."

**The crucial failure mode — unsupported queries.** The percolator "cannot extract terms from all
queries (for example the `wildcard` or `geo_shape` query)." When such a query appears in a
**required** position, or is the only clause, **the selection optimization is disabled** and that
query becomes an always-candidate — it is verified against *every* document. Elastic's own
engineering notes and bug history (e.g. `minimum_should_match` miscomputation when nesting
boolean queries, `intervals` queries failing extraction) show how subtle the
"extract a correct, complete gating set" problem is, and how a single mistake turns into either a
false negative (correctness bug) or a query that can never be pruned (performance bug).

**Batching.** The percolate query supports matching **multiple documents in one request**, which
amortizes the in-memory index construction and candidate-set work across a batch.

**Update / refresh implications.** Percolator queries live in a normal Lucene index, so adding /
changing a stored query is a document index + **refresh** before it becomes visible. High update
rates mean frequent refreshes, segment churn, and merge pressure — near-real-time, not real-time.

**What we borrow:**

1. **Extract-and-store at compile time, select at match time.** Same shape as ours.
2. **Batching titles** to amortize per-title fixed costs and improve cache/branch behaviour.
3. **An explicit "unsupported / un-gateable" category.** This is the single most valuable lesson:
   a query you cannot gate cheaply must be *recognized as such and quarantined*, never allowed to
   silently become an always-candidate that everyone pays for. This directly motivates our
   **broad-query classifier and separate broad lane** (cost classes A/B/C/D).

**What we reject / improve:**

- **General Lucene boolean queries.** We don't need arbitrary nested boolean/span/interval/geo. A
  constrained product DSL means *every* supported query is gateable by construction — we can make
  "un-gateable" a **compile-time rejection (class D)** rather than a silent runtime tax.
- **Index-refresh update model.** Segment+refresh latency and merge amplification are too heavy for
  frequent query churn. We use **immutable segments + a small hot delta + tombstones + atomic epoch
  swap** (see the [ingestion design](../design/ingestion-and-updates.md)), giving sub-segment
  update visibility without rebuilding postings in place.
- **Generic query execution for verification.** We compile verification to **integer mask / sorted-
  slice checks**, not a Lucene `Scorer` tree.

**Capability mapping — what a deployed percolator offers vs Reverse Rusty.** Beyond candidate
selection, production percolator deployments lean on a handful of *operational* capabilities. The
abstract reference workload is written up in [`percolator-workload.md`](percolator-workload.md); the
mapping below is the canonical alignment/gap record (statuses tracked in [`../STATUS.md`](../STATUS.md)
Tier 4):

| Capability | Generic ES/OS percolator | Reverse Rusty today | Disposition |
|---|---|---|---|
| Boolean query shape (include / exclude / OR) | Lucene bool / span / interval (arbitrary nesting) | required / forbidden / any-of (CNF) | ✅ parity for the product-DSL subset (ADR-001) |
| Compile-time extract + match-time select | term extraction stored with the query | signature-cover optimizer | ✅ same architecture |
| Un-gateable query handling | silently becomes an always-candidate | compile-time **class-D reject** / **class-C broad lane** | ✅ improves (ADR-003) |
| Recall → verify | over-matches; **caller must exact-re-test** | integer-exact verifier ⇒ output false-positive-free | ✅ **subsumes** — one stage, final matches |
| Per-query **metadata** stored with the query | arbitrary JSON fields | interned `(key,value)` tags in the SoA | ✅ **built (single-node)** — ADR-049 |
| **Filter** results by metadata (bool clauses) | `bool.filter` on stored fields | ES `bool`/`terms` + native filter, pushed into verify | ✅ **built (single-node)** — ADR-049 (the dominant read pattern) |
| `_score` / relevance ranking | per-hit Lucene score | pure boolean (`Vec<u64>`) | ❌ gap → Tier 4 (lower priority) |
| `function_score` boost by metadata | yes | none | ❌ gap → Tier 4 (lower priority) |
| Pagination (`from` / `size`) | yes | `/_search` yes; `/_mpercolate` size-only | ◑ partial → Tier 4 |
| Batch percolate (many docs / request) | yes | `/_mpercolate` (columnar broad lane) | ✅ have (ADR-026) |
| Update / visibility model | index + **refresh** (segment churn) | immutable segments + memtable + epoch swap | ✅ improves (NRT, no refresh stall) |

The **metadata + filter pair** — the workload's dominant read pattern — is **built and oracle-proven on
the single-node engine** (designed in [`../design/matching.md`](../design/matching.md) §5, decided in
[`../DECISIONS.md`](../DECISIONS.md) ADR-049). The remaining Tier-4 rows are ranking / `function_score` and
`/_mpercolate` pagination (lower priority); threading tags through the experimental cluster path is a
separate follow-on.

---

## 3. Tantivy & Quickwit (Rust search internals)

**What they are.** *Tantivy* is a Rust full-text search library inspired by Lucene; *Quickwit* is a
distributed search engine built on Tantivy, notable for **mmap'd immutable split files** and
decoupled storage. We mine them for *layout and lifecycle* ideas, not for query semantics.

**What we borrow:**

1. **Immutable segments + a write-ahead mutable buffer, merged by background compaction.** This is
   the Lucene/Tantivy lifecycle and it is exactly how we handle 100M queries with frequent updates:
   bulk of queries live in immutable, mmap-friendly segments; churn lands in a small in-memory hot
   delta; a background compactor folds delta into new segments.
2. **mmap immutable segment files** with a compact on-disk == in-memory layout, so segment load is
   a near-zero-copy `mmap` rather than a parse. Enables fast restart and OS-managed page cache.
3. **Columnar / "fast field" thinking and struct-of-arrays postings.** We store the exact-match plan
   as parallel arrays (SoA), not arrays-of-structs, for cache-line density and vectorization.
4. **Segment-local doc IDs.** Tantivy uses dense per-segment doc IDs and only maps to global IDs at
   the end. We mirror this exactly: a `u32` `SegmentLocalQueryId` rides the hot path; the `u64`
   `GlobalLogicalQueryId` only materializes on a confirmed match.

**What we reject / improve:**

- Tantivy's tokenizer/analyzer pipeline, scoring (BM25), and inverted-index generality are
  out of scope — we are not ranking and not doing free-text retrieval. We keep the *file lifecycle*
  and *SoA layouts* and drop the search engine on top.

---

## 4. Roaring bitmaps (roaring-rs / CRoaring)

**What it is.** A compressed bitmap that partitions the 32-bit integer space into 2^16 chunks, each
stored by the locally optimal container:

- **Array container** — sorted `u16` array, for sparse chunks (≤ 4096 elements).
- **Bitmap container** — a 65,536-bit bitmap, for dense chunks.
- **Run container** — `(start, length)` pairs, for long consecutive runs.

Containers **convert adaptively** as density changes, and `runOptimize` repacks runs. The result is
"consistently faster and smaller" set operations (intersection/union/iteration) than uncompressed
bitmaps or plain integer arrays across a wide density range.

**What we borrow:**

1. **Adaptive posting representation keyed on cardinality** — this is the model for our candidate
   index postings (`signature → query-id list`):
   - 0–8 IDs → **inline tiny array** (no heap, lives in the bucket header)
   - small → **sorted `u32` array** (branch-predictable, cache-friendly intersection)
   - medium → **SIMD-friendly sorted block array**
   - large → **roaring bitmap** (use the crate directly)
   - huge → **hot-key split / broad lane** (don't store at all in the main index)
2. **Chunked 16-bit partitioning** for fast intersection and union of candidate sets.
3. The empirical lesson that **sorted arrays beat bitmaps when sparse** — most product signatures
   are sparse, so the common case is the cheap case.

**What we reject / improve:** roaring is a *general* integer-set library; for the dominant tiny/small
postings we use even leaner inline representations to avoid container overhead and pointer chasing,
and only fall back to the roaring crate for genuinely large postings.

---

## 5. Aho-Corasick & daachorse (multi-pattern matching)

**What they are.** The Rust `aho-corasick` crate matches many patterns simultaneously in a single
linear pass (NFA/DFA automaton). *daachorse* implements Aho-Corasick over a **compact double-array**
trie: ~12 bytes/state, constant-time transitions, and reportedly **3.0–5.2× faster** matching with
**56–60% less memory** than `aho-corasick` on a 675K-pattern dictionary.

**Why it matters here.** Title normalization must extract **aliases / multi-word entities**
("Upper Deck" → `brand:upper_deck`, "PSA GEM MT 10" → `grader_grade:psa10`, "Michael Jordan" →
`player:michael_jordan`) at ingestion speed over a potentially large alias dictionary, in one pass,
allocation-free.

**What we borrow:**

1. **A double-array Aho-Corasick automaton as the alias/entity extractor**, scanning normalized
   tokens once to emit dense feature IDs — no per-title hash-map lookups per token, no backtracking.
2. **Leftmost-longest match semantics** so "PSA GEM MT 10" wins over "PSA" + "10" when both are
   registered, giving deterministic normalization.
3. The compact-state lesson: keep the automaton small and cache-resident so it stays hot across the
   2.8k-titles/sec stream.

**What we reject / improve:** we run the automaton over **canonicalized token boundaries**, not raw
bytes, so we sidestep Unicode/substring pitfalls and keep matches aligned to word units relevant to
titles. (Reverse Rusty ships daachorse's double-array Aho-Corasick as the alias extractor; an
earlier hand-rolled trie used the identical interface.)

---

## 6. Set-containment join literature

**Why it's the most relevant theory.** "Title `T` matches query `Q`" requires (among other things)
that `Q`'s required-feature set ⊆ `T`'s feature set. Returning all such `Q` for a stream of `T` *is*
a **set-containment join** (find all sets contained-in / containing a probe set). The literature
gives us principled candidate-pruning, not just engineering folklore.

**Key results we use:**

1. **PRETTI** builds an inverted index on one side and a **prefix tree** over the other to share work
   among sets with common prefixes. We evaluated this as the basis for an explicit query-family /
   shared-prefix structure (represent shared required-feature prefixes once; prune whole subtrees when
   the title lacks a shared feature) but **declined to build it** — the implicit anchor-sharing already
   present in our candidate index captures the near-duplicate-clustering benefit, and an explicit
   prefix DAG's cost (mmap serialization, compaction rebuild) was not justified against an already-flat
   ~54 candidates/title on a path that is not the bottleneck. See [`../DECISIONS.md`](../DECISIONS.md)
   ADR-019. **LIMIT+** (the bounded-depth successor to PRETTI) corroborates the concern — it exists
   specifically because PRETTI's full prefix tree grows too large.
2. **Global token ordering by frequency.** Ordering elements by global frequency and filtering on the
   least-frequent elements "drastically reduce[s] candidate sizes." This is precisely why our
   signature optimizer prefers **rare features as anchors**.
3. **Signature- vs prefix-tree tradeoff.** Signature-based methods burn CPU enumerating signatures;
   prefix-tree methods burn memory storing trees. **FreshJoin**'s adaptive compromise — record a
   couple of *least-frequent* elements plus a frequency-tuned hash signature — maps almost one-to-one
   onto our design: anchor on the 1–2 rarest features and size the signature by frequency.

**What we borrow:** prefix filtering, global frequency ordering, least-frequent-element anchoring,
adaptive signature length. (Prefix-tree sharing was surveyed but not adopted — see result 1 above.)
These give our optimizer a defensible theoretical basis rather than ad-hoc heuristics.

**What we reject / improve:** the literature targets static batch joins. We are streaming with
frequent updates and a fixed small probe set (one title at a time, or a small batch), so we
**precompute and persist** the index side and build per-title structures to be allocation-free.

---

## 7. Optional advanced topics (scouted, staged for later)

- **Static membership filters (xor / binary-fuse / ribbon).** ~1.6× smaller than Bloom at the same
  false-positive rate, immutable, branch-light. A natural fit for the **forbidden-feature** pre-check
  ("does this title contain *any* excluded feature for this query?") and for segment-level "could any
  query here match?" gates. Immutability matches our segment model. Staged for a later iteration.
- **SIMD set intersection** (e.g. shuffle/`_mm_cmpestrm`-style on x86, NEON on aarch64). Applies to
  medium sorted-array postings. Reverse Rusty uses sorted-array galloping/merge intersection that
  auto-vectorizes; explicit SIMD kernels are a documented optimization.
- **Learned Bloom filters.** Interesting but add ML inference and model-drift risk on the hot path —
  contrary to the spec's "no ML inference per candidate." Rejected for the hot path.
- **NUMA-aware sharding & mmap.** 100M queries → multiple shards. Pin shards to NUMA nodes, mmap
  immutable segments per node, avoid cross-NUMA shared mutable state. Design-level concern; Reverse Rusty is
  single-node but the segment model is shard-ready.

---

## 8. Synthesis — what the design takes from each

| Source | Borrowed | Rejected for this domain |
|---|---|---|
| Lucene Monitor / Luwak | two-phase candidate→verify; decomposition invariant; rare-anchor selection; explain | per-document indexing; raw-term gating; minimal-not-cost-optimal anchors |
| ES/OpenSearch percolator | compile-time extract + match-time select; title batching; explicit unsupported-query category | Lucene boolean generality; refresh-based updates; generic Scorer verification |
| Tantivy / Quickwit | immutable segments + hot buffer + compaction; mmap layout; SoA postings; segment-local IDs | tokenizer/analyzer; BM25 scoring; inverted-index generality |
| Roaring | adaptive cardinality-keyed posting representations; 16-bit chunking; sparse→array, dense→bitmap | one-size-fits-all containers for tiny postings |
| Aho-Corasick / daachorse | double-array automaton for one-pass alias/entity extraction; leftmost-longest | byte-level matching; per-token hash lookups |
| Set-containment joins | prefix filtering; global frequency order; least-frequent anchoring; adaptive signature length | static batch assumptions; prefix-tree family sharing (surveyed, declined — ADR-019) |

**The thesis carried into the design:** generic percolators win by gating on *extracted terms*. We go
one level deeper — gate on **compiler-selected semantic signatures**, verify with **integer-only match
plans**, and **quarantine broad queries** instead of letting them become always-candidates. Those three
moves are where the order-of-magnitude advantage over generic percolation for this domain comes from.
(Near-duplicate queries do cluster — they share signature anchors implicitly — but we deliberately do
*not* add an explicit family-factoring structure on top; see [`../DECISIONS.md`](../DECISIONS.md) ADR-019.)

---

## Sources

- [Lucene Monitor / TermFilteredPresearcher (Lucene API)](https://lucene.apache.org/core/8_3_1/monitor/org/apache/lucene/monitor/TermFilteredPresearcher.html)
- [Lucene monitor package summary (9.9.2)](https://lucene.apache.org/core/9_9_2/monitor/org/apache/lucene/monitor/package-summary.html)
- [MultipassTermFilteredPresearcher (Lucene 10.2.0)](https://lucene.apache.org/core/10_2_0/monitor/org/apache/lucene/monitor/MultipassTermFilteredPresearcher.html)
- [Luwak — stored queries library (flaxsearch)](https://github.com/flaxsearch/luwak)
- [Elasticsearch percolate query reference](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-percolate-query)
- [Elasticsearch: fix query extraction bugs (PR #29283)](https://github.com/elastic/elasticsearch/pull/29283)
- [`intervals` queries fail percolator term extraction (issue #45639)](https://github.com/elastic/elasticsearch/issues/45639)
- [Tantivy (Rust full-text search engine)](https://github.com/quickwit-oss/tantivy)
- [Roaring bitmaps — consistently faster and smaller (arXiv 1603.06549)](https://ar5iv.labs.arxiv.org/html/1603.06549)
- [Roaring format specification](https://github.com/RoaringBitmap/RoaringFormatSpec/)
- [roaring-rs](https://github.com/RoaringBitmap/roaring-rs)
- [aho-corasick crate](https://crates.io/crates/aho-corasick)
- [daachorse — double-array Aho-Corasick](https://github.com/daac-tools/daachorse)
- [Engineering faster double-array Aho-Corasick automata (arXiv 2207.13870)](https://arxiv.org/abs/2207.13870)
- [Set Containment Join Revisited (arXiv 1603.05422)](https://arxiv.org/abs/1603.05422)
- [FreshJoin: adaptive set containment join](https://link.springer.com/article/10.1007/s41019-019-00107-y)
