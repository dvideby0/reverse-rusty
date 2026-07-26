# Normalization & vocabulary decisions

> [Architecture decision hub](../../DECISIONS.md)

Shared query/title normalization, dictionaries, learned vocabulary, aliases, and feature-space evolution.

| ADR | Decision | Summary | Status |
|---|---|---|---|
| [010](../adr-010-normalizer-builder-fallible.md) | Fallible normalizer builder | Replaces hardcoded vocabulary and panicking construction with a configurable, typed builder. | Accepted |
| [015](../adr-015-runtime-vocabulary-learning.md) | Runtime vocabulary learning | Learns synonyms from any-of groups and tracks the epoch used to compile each segment. | Accepted |
| [046](../adr-046-dynamic-vocabulary.md) | Dynamic vocabulary | Hashes post-freeze terms and rebuilds aliases blue/green so new vocabulary cannot cause false negatives. | Accepted |
| [053](../adr-053-corpus-phrase-vocab-source.md) | Corpus phrase induction | Adds opt-in NPMI phrase learning as another source for the runtime vocabulary. | Accepted |
| [054](../adr-054-equivalence-expansion.md) | Alias expansion | Widens required features into compile-time any-of groups instead of collapsing meanings. | Accepted |
| [058](../adr-058-punctuation-equivalence-folding.md) | Punctuation folding | Makes punctuation treatment configurable and shared across query and title normalization. | Accepted |

---

Shipped changes are recorded in [CHANGELOG.md](../../CHANGELOG.md); unfinished work belongs in
[roadmap.md](../../roadmap.md). Documentation placement rules live in
[the documentation hub](../../README.md).
