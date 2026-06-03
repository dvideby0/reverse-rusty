# ADR-017: Durable bulk ingest — segment file is the artifact, manifest is the commit point

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


- **Context:** `bulk_ingest` / `build_from_queries` compile a batch directly into a fresh base
  segment, deliberately bypassing the WAL (ADR-013). The audit (P1-15) flagged that this path
  could lose acknowledged data silently: on a segment-write or mmap failure, `make_base_segment`
  printed to stderr and *fell back to an in-memory segment*, so the engine reported success
  (`IngestReport { ingested: N }`) for data that was never persisted and would vanish on the next
  restart. The manifest-write result was likewise ignored before returning the report. Source
  text was never persisted by these paths at all (only `flush` wrote `sources.dat`), so even a
  clean restart lost the source text of bulk-ingested queries.
- **Research:** RocksDB's `IngestExternalFile` is the canonical model. It does *not* WAL the
  ingested data — that would be a redundant double-write of data already in a durable file.
  Instead the SST file *is* the durable artifact (fsync'd), and the atomic MANIFEST update that
  references it is the linearization/commit point; ingestion is all-or-nothing ("if the status is
  non-OK, none of the files are ingested"). Our segment file already fsyncs via tmp-write +
  `sync_all` + atomic `durable_rename` (P2-13), and `Engine::open` ignores any segment file not
  listed in the manifest — so we already had RocksDB's two ingredients. The bug was purely in
  error *handling*, not durability mechanics. So the fix is to surface failures and make the
  commit atomic, **not** to WAL bulk entries.
- **Decision:** Add fallible `try_bulk_ingest` / `try_build_from_queries` returning
  `io::Result<IngestReport>`; the existing infallible `bulk_ingest` / `build_from_queries` become
  thin wrappers that log + set `persistence_healthy = false` + return an empty report on `Err`
  (preserving the in-memory-mode and test/demo call sites). A shared `commit_base_segment` makes
  the batch all-or-nothing: (1) `build_durable_base` writes the segment file and mmaps it,
  propagating any I/O error instead of falling back to memory; (2) the segment is appended and the
  manifest written — the atomic commit point, which both references the new file and embeds the
  updated dict; (3) on manifest failure the in-memory segment is popped and the orphan file
  deleted, so nothing is committed. Accepted source text is collected locally and applied to the
  (display-only) query store + persisted to `sources.dat` *after* the commit point — bulk has no
  WAL backstop, so this is the sole point at which bulk source text becomes durable. The server
  maps a bulk persistence failure to HTTP 503 (`/_bulk`) and aborts startup on initial-load
  failure rather than serving a non-durable engine.
- **Consequence:** A bulk batch is now durable-or-rejected: callers never get a success report for
  data that isn't on disk. Match data (segment + manifest) is strictly all-or-nothing; a
  `sources.dat` failure *after* the commit point is surfaced via `persistence_healthy` but does
  not un-commit the already-durable match data (source text is display-only, never on the match
  path). Auxiliary in-memory state interned during a rejected batch (new dict features, rejection
  counters) is not rolled back — like RocksDB, internal stats reflect the attempt; only the
  committed data set is transactional. Cost: each bulk call now also rewrites `sources.dat`
  (O(corpus)); the manifest write (full dict serialization) was already O(corpus) per call, so
  this adds a constant factor, not a new asymptotic class (measured ~63 ms → 340 ms for a
  200-query bulk as the base corpus grows 10k → 100k). Shrinking that per-call cost (chunking,
  incremental sources, I/O outside the write lock) is tracked separately as audit P2-14. Also
  fixed in passing: bulk-ingested segments now carry the current `vocab_epoch` (they were
  defaulting to 0 and so were counted permanently stale once any vocab was set).
- **See also:** ADR-013 (WAL — the live-insert path this deliberately contrasts with), ADR-012
  (segment file format + manifest), ADR-014 (query source store), P2-13 (`durable_rename`),
  [ingestion-and-updates.md](../design/ingestion-and-updates.md)


