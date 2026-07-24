# ADR-013: Write-ahead log (WAL) for crash recovery

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


- **Context:** With mmap'd segments, sealed data is durable on disk. But the mutable memtable
  (hot delta) lives in memory and is lost on crash. Without a WAL, all un-flushed inserts and
  tombstones are lost. The design doc specifies a durable mutation log as the source of truth
  (§3 of ingestion-and-updates.md).
- **Decision:** Simple append-only WAL (`wal.log`) with framed entries. Each entry:
  `[body_len: u32, crc32: u32, seq: u64, op: u8, payload...]`. Operations cover Insert,
  atomic Upsert, positional legacy Tombstone, address-free DeleteByLogical, and
  FlushCheckpoint; accepted class-D inserts/upserts have distinct op markers so replay preserves
  the writer's admission decision. Insert-shaped payloads carry logical id, version, query text,
  raw tags, optional typed priority, and—on engine-owned WAL v7 writes—the ADR-116 internal source
  generation. CRC-32 per entry detects torn writes from crashes.
  Recovery: scan forward, skip entries with bad CRC, replay only entries after the last
  FlushCheckpoint (earlier entries are already materialized in sealed segments).
  WAL is written before memtable mutation (WAL-first = durability before visibility), and
  **a WAL append failure rejects the mutation**: the in-memory state is left untouched and the
  error is surfaced to the caller (`WriteError::Wal` from `try_insert_live`, `io::Result` from
  `tombstone`/`delete_by_logical_id`) — never swallowed (P1-17). Query parsing happens *before*
  the WAL append, so a malformed query (`WriteError::Parse`) never reaches the log. Two fsync
  policies via `EngineConfig::wal_sync_on_write` (P1-17): the default (`false`) `write(2)`s each
  append to the OS page cache and fsyncs only at flush checkpoints — an acknowledged write
  survives a process crash but not power loss until the next checkpoint (RocksDB `sync=false` /
  SQLite `NORMAL`); `true` fsyncs every append before acknowledging, so an acknowledged write
  survives power loss (SQLite `FULL`), at a large per-write latency cost. Checkpoints are always
  fsync'd.

  The v7 source generation is reserved before append and installed unchanged in the exact row and
  `sources.dat`. Recovery never allocates a new generation for that frame; generation-less legacy
  frames remain generation zero. Source-store updates accept only an equal-or-newer generation, so
  replaying an old frame covered by a later same-id bulk manifest cannot replace the newer source.
- **Consequence:** Crash recovery is correct: replaying the WAL after the last checkpoint
  reproduces the exact memtable state. CRC-32 detects partial writes. The WAL is reset after
  compaction + manifest write (all data is in segments). No new dependencies (CRC-32 is
  hand-rolled, ~15 lines). Trade-off (measured): the default checkpoint-only policy costs
  ~3 µs/append (page-cache `write(2)`); enabling `wal_sync_on_write` raises that to ~4 ms/append
  — one device flush per mutation, ~1300x slower — in exchange for power-loss durability. A
  failed WAL append rejects the write rather than degrading durability silently, so callers can
  retry (the server maps `WriteError::Wal` to HTTP 503).
- **See also:** [ingestion-and-updates.md](../design/ingestion-and-updates.md) §3
