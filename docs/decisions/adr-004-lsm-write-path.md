# ADR-004: LSM write path over full rebuild

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


- **Context:** The naive approach is to rebuild the entire index when queries change. At
  100M queries this is unacceptable (minutes of unavailability or double-buffering cost).
- **Decision:** Log-structured (LSM) write path with immutable segments + a mutable memtable
  (hot delta) + tombstones. Writes append to the memtable and become visible immediately via
  an atomic epoch swap. Segments are never mutated once sealed.
- **Consequence:** ~750k updates/sec/core with immediate visibility. Full rebuild is reserved
  for the initial seed and major feature-model changes (blue/green from the log, not
  stop-the-world). Read amplification grows with segment count — compaction caps it (ADR-009).
- **See also:** [ingestion-and-updates.md](../design/ingestion-and-updates.md)

