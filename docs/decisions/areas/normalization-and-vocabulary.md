# Normalization & vocabulary decisions

> [Architecture decision hub](../../DECISIONS.md)

Shared query/title normalization, dictionaries, learned vocabulary, aliases, and feature-space evolution.

| ADR | Decision | Summary | Status |
|---|---|---|---|
| [010](../adr-010-normalizer-builder-fallible.md) | NormalizerBuilder + fallible construction | Replace hardcoded vocab + `.expect()` with a fluent `NormalizerBuilder` + fallible `default_vocab()`; domain-agnostic, zero panics, Debug/Send/Sync everywhere. | Accepted |
| [015](../adr-015-runtime-vocabulary-learning.md) | Runtime vocabulary learning from any-of groups | Learn synonyms from query any-of groups at runtime (`Vocab` + `/_vocab`); a `vocab_epoch` counter tracks segments compiled under a now-stale normalizer. | Accepted |
| [046](../adr-046-dynamic-vocabulary.md) | Dynamic vocabulary (Cluster v1) | Absorb terms after the dict is frozen — feature-hashing for unknown tokens (no coordination, bounded FP, never FN) + runtime normalizer learning for aliases (blue/green rebuild). Built. | Accepted |
| [053](../adr-053-corpus-phrase-vocab-source.md) | NPMI corpus phrase induction as a runtime vocab source | Wire the `learn` binary's NPMI collocation miner into a library `corpus::learn_phrases_from_text` → `Vocab`, composed under the ADR-015 any-of learner via an opt-in `CorpusLearnConfig`/`learn_and_apply_with`. Phrases only (no aliases) ⇒ same-normalizer gluing ⇒ oracle-equivalent, zero FN; default-off ⇒ byte-identical. | Accepted |
| [054](../adr-054-equivalence-expansion.md) | Equivalence (alias) learning via expansion, not collapse | First-class `Vocab.equivalences` + a compile-time `Extracted::expand_equivalences` that widens a required feature into an any-of over its group (query-side, via a transient `dict::EquivMap`). Structurally FN-safe (match set only grows; wrong alias ⇒ bounded FP). Declared + any-of-learned sources (opt-in); distributional/match-feedback discovery deferred behind the same seam. | Accepted |
| [058](../adr-058-punctuation-equivalence-folding.md) | Configurable punctuation-equivalence folding | Make byte-cleaning's per-character behavior a configurable `PunctClass` table (`Split`/`Fold`/`Keep`/`Marker`) on the shared normalizer; declaring `'`/`-` as `Fold` collapses `O'Brien`/`O-Brien`/`OBrien` to one token, closing a recall gap. Same table over queries + titles ⇒ cover holds. Default reproduces the historical behavior (byte-identical); opt-in, persisted via `Vocab`. | Accepted |

---

Each summary links to the canonical ADR record. Implementation status belongs in
[STATUS.md](../../STATUS.md); documentation placement rules belong in
[the documentation hub](../../README.md).
