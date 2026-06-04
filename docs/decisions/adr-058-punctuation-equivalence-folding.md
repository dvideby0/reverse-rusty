# ADR-058: Configurable punctuation-equivalence folding in byte-cleaning

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted

- **Context.** The shared normalizer's byte-cleaning pass (`normalize::Normalizer::clean_into`) had three
  hardcoded behaviors for non-alphanumeric characters: `.` was kept in place (so half-grades like `9.5`
  survive), `#` and `/` became standalone marker tokens (so the number logic can tell card-numbers `#2`
  and serials `/199` from grades), and **every other** non-alphanumeric byte became a space (a word
  boundary). That last rule is a recall hazard on real listing text: mid-word punctuation splits a token
  that a query author wrote joined. `O'Brien` and `O-Brien` both tokenize to `[o, brien]`, while `OBrien`
  tokenizes to `[obrien]` — so a stored query `obrien` **misses** a title `O'Brien` entirely. Because this
  engine is a **recall-first** candidate generator (a missed candidate is the cardinal failure; a false
  positive is cheap, the exact verifier drops it), a normalization rule that can drop a real match is the
  highest-value gap in the Tier-4 percolator-parity set. The roadmap flagged it: *"`clean_into` currently
  maps all non-alphanumeric, non-marker characters to a space … add a configurable punctuation-folding
  table so callers can declare which characters collapse vs. become word boundaries."*

- **Decision.** Make per-character byte-cleaning behavior a **configurable table** on the normalizer,
  with a default that reproduces the historical behavior exactly. A character resolves to one of four
  classes (`normalize::PunctClass`):
  - **`Split`** — map to a space (a word boundary). The default for any character not otherwise
    classified.
  - **`Fold`** — delete the character, so the alphanumerics on either side **join** into one token
    (`O'Brien` → `obrien`). This is the new punctuation-equivalence behavior: declaring `'`, the curly
    apostrophe `'` (U+2019), and a mid-word `-` as `Fold` collapses `O'Brien` / `O'Brien` / `O-Brien` /
    `OBrien` onto the same token.
  - **`Keep`** — keep the character literally, in place, inside the surrounding token (`9.5` stays
    `9.5`). The default for `.`.
  - **`Marker`** — emit the character as its own standalone token (` c `). The default for `#` and `/`.

  The table (`PunctTable`) defaults to `.`→`Keep`, `#`→`Marker`, `/`→`Marker`, everything else→`Split` —
  **byte-identical** to the pre-ADR-058 hardcoded logic. It is built through `NormalizerBuilder`
  (`set_punct_class` / `fold_punctuation` / `fold_punctuation_chars` / the fluent `punct`) and persists
  through `Vocab` (a `#[serde(default)] punctuation: Vec<PunctRule>` field → `Vocab::to_normalizer`), so
  rules survive reopen and are settable over the existing `PUT /_vocab` surface. ASCII characters resolve
  through a flat 128-entry array (branchless, no hashing on the per-title path); a rare non-ASCII rule
  falls back to a small map that stays empty unless one is registered.

- **Why it is correctness-safe (the load-bearing property).** Folding lives entirely inside `clean_into`,
  which the compile path (`compile_features` / `compile_features_readonly`) and the match path
  (`match_features`) **both** reach through the single `emit` entry point. So whatever the table says,
  **queries and titles fold identically** — the feature spaces stay aligned, which is the shared-normalizer
  invariant (normalization.md §2) the lossless-cover contract rests on. The differential oracle proves
  this directly: an engine *and* an independent brute-force oracle, both built under a folding normalizer,
  agree exactly (zero false negatives, zero false positives) over punctuated data, including the
  forbidden-term and any-of paths. Folding is **not** an unconditional recall win — it is the operator's
  informed trade: declaring `'` as `Fold` gains the joined-form match (`obrien` now matches `O'Brien`) and
  gives up the split-form one (`brien` alone no longer matches `O'Brien`). Whichever is chosen, the cover
  holds *under that configuration*; the engine never silently drops a match relative to its own normalizer.

- **Why it is byte-identical by default.** The default `PunctTable` seeds exactly the three historical
  special cases and `Split` for all else, so a normalizer built without touching it produces the identical
  feature stream — every existing oracle, golden, and persistence test passes unchanged (the 40k-query
  default differential and the populated-vocab differential are untouched). Old `Vocab` JSON that predates
  the field deserializes to an empty rule set (`#[serde(default)]`), i.e. the default table. The feature
  is **opt-in / default-off**, the same shape as ADR-053 (corpus phrases) and ADR-054 (equivalence
  expansion).

- **Scope.** Single-engine and in-process cluster (the table rides in the shared `Normalizer` /
  `Vocab` like any other vocabulary). Shipping a punctuation table *cross-process* to a remote shard's
  normalizer is the same deferred item as cross-process normalizer/alias shipping (the distributed layers
  use `default_vocab()` today, STATUS "Current limitations") — out of scope here. Reclassifying the
  number-pipeline defaults (`.`/`#`/`/`) is allowed but an operator's responsibility: e.g. folding `.`
  would merge `9.5` → `95` and defeat half-grade detection; the defaults stay as they were precisely so
  the number logic is unaffected unless deliberately overridden.

- **Alternatives.** (1) *Additive multi-emit* (emit the joined form **and** the split components, à la
  Lucene's `WordDelimiterGraphFilter` `preserveOriginal`/`catenateWords`, so folding is a pure recall
  gain) — deferred, not rejected: it widens the token stream and the candidate set, and the roadmap's
  scope is explicitly "collapse vs. word-boundary," a single-form fold. The `PunctClass` seam can grow an
  additive variant later without an API break. (2) *Hardcode a fixed fold set* (always fold `'`/`-`) —
  rejected: the right set is corpus-dependent (a hyphen is mid-word in `O-Brien` but a range in `1-of-1`),
  and a fixed change would silently alter every existing deployment's feature space. Configurable +
  default-off is the only choice that keeps the default byte-identical. (3) *Do it as a post-tokenization
  filter* (an ES token-filter analog) — rejected: byte-cleaning is the natural, cheapest place (one pass,
  no token re-stitching) and the roadmap scoped it there.

- **Testing.** `normalize.rs`: the default still splits `'`/`-` (`O'Brien` → `[term:brien, term:o]`) and
  keeps the `#`/`/`/`.` behaviors (a regression guard mirroring the number-disambiguation matrix); a
  folding normalizer collapses all four `O'Brien` surface forms (ascii + curly apostrophe + hyphen +
  joined) to the single token `term:obrien`; folding merges only *within* a word (`foo - bar` stays two
  tokens); and a `Keep`→`Fold` override on `.` is exercised. `vocab.rs`: punctuation rules JSON
  round-trip and drive folding through `to_normalizer`, old JSON without the field is default behavior,
  and `merge` carries rules first-wins. `tests/oracle.rs`:
  `zero_false_negatives_with_punctuation_folding` — engine ≡ brute (zero FN/FP) under a folding normalizer
  over apostrophe/hyphen data, the joined-form query matches every punctuated variant, and the default
  normalizer is shown to miss it. Full `check.sh` green.

- **Consequences.** Operators can declare a corpus's mid-word punctuation (apostrophes, hyphens) as
  folding so punctuation-only spelling differences stop causing missed candidates — closing a real
  recall gap for the recall-first workload — while every deployment that does not opt in is byte-identical
  to before. The `PunctClass` table generalizes the previously-hardcoded `.`/`#`/`/` handling into one
  data-driven seam, and leaves room for a future additive (preserve-original) fold variant. Closes the
  Tier-4 "byte-cleaning: punctuation-equivalence rules" roadmap item.
