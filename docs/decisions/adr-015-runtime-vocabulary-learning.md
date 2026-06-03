# ADR-015: Runtime vocabulary learning from query any-of groups

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


- **Context:** ADR-010 made the normalizer domain-agnostic via `NormalizerBuilder`, but
  vocabulary still had to be supplied manually. For a new domain the operator has no good way
  to bootstrap a vocabulary. Query any-of groups (e.g., `(rc, rookie)`) are an organic source
  of synonym relationships — the query author is asserting that the members are interchangeable
  in their intent. Mining these at runtime avoids the need for an external corpus pipeline.
- **Decision:** Add `Vocab` struct (`src/vocab.rs`) that holds learned synonyms, phrases, and
  graders. `Vocab::learn_from_queries()` extracts synonyms from stored query any-of groups
  using frequency and co-occurrence thresholds. The engine exposes `set_vocab()` to replace
  the normalizer vocabulary at runtime, plus REST endpoints (`GET/PUT /_vocab`,
  `POST /_vocab/learn`). Vocabulary is persisted as JSON via `--vocab-file`.
- **Consequence:** Bootstrapping a new domain requires only ingesting queries — the system
  can learn its own vocabulary. **Hazard:** `set_vocab()` replaces the normalizer without
  recompiling existing queries. Until queries are reingested, the "same normalizer for queries
  and titles" invariant (ADR-001) is violated in practice. **Enforcement:** A monotonic
  `vocab_epoch` counter on the engine is incremented on each `set_vocab()` call. Every
  segment (base and memtable) is stamped with the epoch at which its queries were compiled.
  `Engine::stale_segment_count()` / `has_stale_segments()` reports how many segments are
  out-of-date; `set_vocab()` returns this count. `EngineMetrics::stale_segments` and the
  `/_health` endpoint (yellow status when stale) make staleness visible to operators.
  Compaction preserves the minimum epoch of merged segments (still stale if any source was).
  A production system would additionally need blue/green rematerialization (see design-only:
  feature-model versioning). `serde` becomes a library dependency (via `Vocab` serialization),
  previously it was server-only.
- **See also:** ADR-010 (NormalizerBuilder), [normalization.md](../design/normalization.md),
  [corpus-feature-learning.md](../research/corpus-feature-learning.md)

