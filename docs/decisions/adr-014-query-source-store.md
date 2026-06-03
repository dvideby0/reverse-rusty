# ADR-014: Engine-level query source store (not in segment files)

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


- **Context:** The audit (P1-7) identified that search results returning bare integer IDs with
  no query text made the API effectively write-only. Implementing `GET /_doc/{id}` and rich
  search hits requires storing original query text somewhere. Three options: (A) add a source
  section to `.seg` files, (B) per-segment `.src` sidecar files, (C) engine-level
  `FastMap<u64, String>` persisted as a single `sources.dat` file.
- **Decision:** Option C — engine-level `FastMap` with a separate `sources.dat` binary file
  (atomic tmp+rename). Query text is populated on every ingest path and removed on delete.
  Persisted on flush alongside the manifest. Loaded on `Engine::open()`, with WAL replay
  adding memtable entries on top. Uses `FastMap` (identity hasher) consistent with all other
  u64-keyed maps in the codebase.
- **Consequence:** Query text is never on the match hot path (ADR-002 preserved). Segment
  files stay optimized for matching — no bloat from string data in mmap'd regions. Source
  lookups happen only in response formatting (after matching completes) or `GET /_doc/{id}`.
  Memory overhead is proportional to total query text (~20 bytes/query average). The
  `include_source: false` option lets clients skip source lookup entirely when they only need
  IDs. Trade-off: `sources.dat` is a full rewrite on every flush; at very large scale, a
  per-segment approach (option B) would amortize writes.
- **Alternatives rejected:** (A) Adding to `.seg` format would bloat mmap'd memory with data
  never accessed during matching, violating the hot-path budget. (B) Per-segment sidecars add
  complexity for compaction merging and are premature at current scale.

