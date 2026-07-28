# ADR-069: Caller-defined number-context words

> [Percolator parity decisions](areas/percolator-parity.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted; implementation revised by compiler semantics 5

## Original accepted decision

- **Context.** Four-digit tokens in `1900..=2099` are recognized as years. The prototype had one
  embedded product-specific context word that instead made the following value a generic term.
  That unreachable configuration created a residual parity mismatch for callers whose reference
  matcher treated number typing as position-insensitive.

- **Decision.** Generalize the embedded condition into a number-context word list on the shared
  normalizer:

  1. `NormalizerBuilder::set_number_context_words` and `number_context_words` replace the list.
  2. An empty list disables contextual demotion; a custom list supports catalogs with year-shaped
     model, part, or series identifiers.
  3. The vocabulary document persists the list through files, cluster manifests, and live
     vocabulary replacement.
  4. The same list runs over query compilation and title analysis.

- **Original compatibility posture.** The first implementation represented the vocabulary field as
  optional. An absent field retained the then-current embedded default, while an explicit empty
  list selected position-insensitive typing. This preserved existing prototype state while making
  the behavior configurable.

- **Evaluated and declined: emit both typings.** Emitting both `year:N` and `term:N` on titles looked
  like a recall superset, but one title feature set also serves forbidden checks. The additional
  feature could therefore reject a query that previously matched. Maintaining one shared configured
  typing was simpler and semantically exact.

- **Why configuration rather than more numeric types.** The engine cannot know whether a value is a
  year, model, capacity, dimension, revision, or seller identifier without catalog context. Built-in
  interpretations would make feature semantics category-dependent.

## Compiler-semantics-5 outcome

The public prototype reset removed every embedded context word and the legacy optional-field
distinction. `Vocab.number_context` is now a strict, serde-defaulted `Vec<String>` whose default is
empty. `model 1995` therefore emits `year:1995` unless the caller explicitly declares `model` as
context. Prototype state compiled under the retired feature model is rebuilt from query sources;
there is no in-place compatibility promise.

Changing the list changes the shared feature model. REST and cluster replacement paths recompile
stored queries before publication. Embedded callers must complete the split `set_vocab()` then
`recompile_stale_segments()` sequence before exposing a snapshot. Candidate retrieval and exact
verification are otherwise unchanged.

- **Testing.** Golden tests pin the empty default, a custom `["model"]` list, JSON round-trip,
  strict unknown-field rejection, merge behavior, runtime recompile, and oracle agreement for
  positive and forbidden year clauses.

- **Record preservation.** Obsolete catalog-specific labels and examples from the original
  prototype record were sanitized under the one-time exception documented in
  [`DECISIONS.md`](../DECISIONS.md). The accepted configuration decision, compatibility rationale,
  rejected alternative, and current revision outcome remain recorded.

- **See also:** ADR-058 (punctuation configuration), ADR-046 (vocabulary apply/recompile),
  [`normalization.md`](../design/normalization.md), and
  [`percolator-workload.md`](../research/percolator-workload.md).
