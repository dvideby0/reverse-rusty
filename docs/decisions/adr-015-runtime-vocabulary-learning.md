# ADR-015: Runtime vocabulary learning from query any-of groups

> [Normalization & vocabulary decisions](areas/normalization-and-vocabulary.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted


- **Context:** ADR-010 made the normalizer domain-agnostic via `NormalizerBuilder`, but
  vocabulary still had to be supplied manually. For a new domain the operator has no good way
  to bootstrap a vocabulary. Query any-of groups (for example `(package,pkg)`) are an organic source
  of synonym relationships — the query author is asserting that the members are interchangeable
  in their intent. Mining these at runtime avoids the need for an external corpus pipeline.
- **Decision:** Add `Vocab` struct (`src/vocab.rs`) that holds learned synonyms, phrases, and
  aliases. The free function `vocab::learn_from_queries()` extracts synonyms from stored query
  any-of groups using frequency and co-occurrence thresholds. The engine exposes `set_vocab()` to
  replace the normalizer vocabulary at runtime, plus REST endpoints (`GET/PUT /_vocab`,
  `POST /_vocab/learn`). Vocabulary is persisted as JSON via `--vocab-file`.
- **Current outcome:** The REST and cluster vocabulary-replacement paths recompile stored queries
  before publishing the new normalizer, so the query/title feature model stays aligned. Embedded
  callers use the deliberately split sequence `set_vocab()` followed by
  `recompile_stale_segments()` and must not publish a snapshot between those calls. Bootstrapping can
  start from query any-of groups and later combine reviewed aliases and corpus phrases. `serde` is a
  library dependency for vocabulary serialization.
- **See also:** ADR-010 (NormalizerBuilder), [normalization.md](../design/normalization.md),
  [corpus-feature-learning.md](../research/corpus-feature-learning.md)
