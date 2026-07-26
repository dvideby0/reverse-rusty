# ADR-122: Fail-closed positional tombstones

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted

- **Context.** `Engine::tombstone_in(segment, local_id)` is an expert physical-address API.
  It appended a positional WAL frame before checking whether the segment or row existed, and
  then returned `Ok(())` even when no deletion was applied. An invalid or already-dead address
  therefore produced a durable no-op reported as success. Compaction makes this contract especially
  unsafe because it replaces and densifies segments: an old pair can remain in range while naming
  a different live query, turning a stale call into a durable false-negative deletion.
- **Research.** Lucene's analogous `IndexWriter::tryDeleteDocument(reader, docID)` succeeds only
  while the near-real-time reader and its segment are still current. When the segment has been
  merged away it returns a stale result, and the caller must resolve and delete the document again
  by a stable term or query. That is the right boundary here too: a physical address is a conditional
  capability, not a stable document identity.
- **Decision.** A caller first resolves a process-local `SegmentAddress`, supplying the stable
  expected logical id so even a previously-held positional pair is checked before a token can be
  minted. The opaque token carries the installed segment generation, local id, and logical identity;
  it cannot be reconstructed from the two positional numbers. Segment replacement installs a fresh
  generation, while an unchanged segment keeps its generation even if its ordinal shifts. A reseal
  publishes its replacement segments and generations only with the manifest commit; commit failure
  restores both vectors so subsequent positional WAL frames still address the durable layout.
  `tombstone_in(&address)` validates the still-installed generation, local-id bounds, logical
  identity, and liveness before touching the WAL. Rejections return the public typed
  `TombstoneError` (`StaleAddress`, `SegmentNotFound`, `LocalNotFound`, `AlreadyDeleted`, or the
  defensive `SegmentIndexOverflow`) and consume no WAL sequence or bytes. Once validated, preserve
  ADR-013's WAL-first order; `TombstoneError::Wal` leaves the row alive. WAL recovery remains
  deliberately idempotent and tolerant of invalid/already-dead historical frames because replay is
  not a new caller acknowledgement.
- **Consequences.** A live positional delete can no longer acknowledge a durable no-op. Callers can
  distinguish stale addressing from an I/O failure and re-resolve by logical id. No match-hot-path,
  WAL-format, manifest-format, or normal delete-by-logical behavior changes.
- **Testing.** Unit coverage pins invalid segment, invalid local id, already-dead row, zero WAL
  movement on each rejection, and WAL failure before mutation. Persistence coverage pins valid
  WAL-tail replay and the adversarial compaction case where a held pair stays in-range but is reused
  for a different survivor; the generation rejects it without WAL movement, live or after reopen.
  A neighboring-range merge also pins that a token for an unchanged segment follows its new ordinal
  and logs the current WAL address. An injected reseal-manifest failure pins rollback to the old
  ordinal map before a later accepted delete and restart. ADR-066's existing compaction/crash
  regression continues to prove that historical positional frames cannot misfire.
- **See also:** ADR-005 (typed errors), ADR-013 (WAL-first writes), ADR-066 (tombstone durability and
  positional-frame recovery watermark).
