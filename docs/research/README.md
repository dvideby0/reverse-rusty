# Research

Prior art, source-level peer studies, and corpus/real-data findings that justify the design.

- [`prior-art.md`](prior-art.md) — battle-tested systems we mine for ideas (Lucene Monitor/Luwak, ES/OpenSearch percolator, Tantivy/Quickwit, roaring bitmaps, Aho-Corasick/daachorse, set-containment joins) + a synthesis table.
- [`clustering-prior-art.md`](clustering-prior-art.md) — consistent-hashing variant comparison (ring+vnodes chosen), content-based pub/sub routing, and the Elasticsearch distributed-percolator contrast that ground the clustering design (→ [`../DECISIONS.md`](../DECISIONS.md) ADR-027).
- [`dynamic-vocabulary.md`](dynamic-vocabulary.md) — **charter** for the Cluster v1 spike on absorbing new vocabulary after the dict is frozen: ES/OS global ordinals + dynamic mapping, Vespa real-time indexing, RocksDB shared-dictionary versioning, Lucene FST, and deterministic feature-hashing — evaluated against the zero-false-negative contract (→ ADR-046).
- [`corpus-feature-learning.md`](corpus-feature-learning.md) — learning the feature extractor from the query corpus (NPMI entity induction), what's safe to learn vs the aliasing safety rail, measured learner results.
- [`real-data-findings.md`](real-data-findings.md) — testing the normalizer against real eBay "PSA 10" titles: bugs found & fixed, and the aspects-first + corpus-learned front-end conclusion.
