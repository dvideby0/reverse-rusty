# Corpus-Driven Feature Learning — building the "tokenizer" from the queries

*Can we build the feature extractor (the normalizer / tokenizer) **from the supplied queries
themselves**, so we never hand-enumerate that "jo kep" is a player, "upper deck" is a brand, and so
on — given no oracle of every entity that exists, and queries that don't tag fields? Short answer:
yes for the part that controls candidate selectivity (demonstrated and measured in
`engine/src/bin/learn.rs`). The one part not freely learnable without risk is cross-form equivalence
(aliasing); §5 explains exactly why and how to do it safely.*

---

## 1. The reframing that makes this possible

Our matching core is already **semantics-agnostic**. Look at what actually selects candidates in
`compile.rs::build_signatures`: it uses `dict.freq(feature)` and `is_hot(feature)` — pure
**frequency rank**. It never consults the `FeatureKind` ("player" vs "brand" vs "grade") to choose an
anchor. The hand-built vocabulary was only ever doing two jobs:

1. **Gluing multi-token entities** into one feature (`michael jordan` → one token), which *raises
   selectivity* (a two-word unit is rarer than either word).
2. **Canonicalizing equivalent surface forms** (`PSA 10` = `PSA10` = `PSA GEM MT 10`; `UD` =
   `Upper Deck`), which *raises recall of user intent*.

Neither job requires knowing *what* the thing is. And there are two more facts that make the corpus
sufficient:

- **The query corpus defines the entire feature universe.** A token that appears in *no* stored query
  is irrelevant to matching — our `match_features` already drops title tokens that aren't in the
  dictionary. So we never need a global entity list; we need only the tokens/phrases that some query
  actually uses.
- **Selectivity is a measurable statistic**, not a semantic judgment. "jo kep" is a good anchor iff it
  is *rare in the corpus* — which we can count directly.

So the feature extractor we need is: a corpus-learned **(a) tokenizer + (b) multi-token-entity glue +
(c) frequency table**, plus a carefully-bounded **(d) equivalence learner**.

---

## 2. What we built and measured (`learn` binary)

We take raw query text (no `Vocab`, no field taxonomy — the learner sees only strings), tokenize on
whitespace/punctuation, count unigrams and adjacent bigrams, and induce multi-token entities with
**NPMI collocation mining** (the word2vec / Mikolov "New York" phrase trick):

```
NPMI(a,b) = ln( P(ab) / (P(a)·P(b)) ) / ( −ln P(ab) )      merge if NPMI ≥ τ and count ≥ min_count
```

We iterate (bigram → trigram) by rewriting the corpus with merged phrases and re-mining.

**Result on 500,000 synthetic queries, zero hand-coded vocabulary** (`min_count=50, τ=0.30`):

```
--- name-like entities, top by binding strength (npmi) ---
upper_deck       count 63696   npmi 1.073
michael_jordan   count  4239   npmi 1.043
lebron_james     count  1811   npmi 1.038
kobe_bryant      count  1402   npmi 1.037
ken_griffey      count  1162   npmi 1.036
wayne_gretzky    count  1002   npmi 1.035
tom_brady        count   986   npmi 1.035
mike_trout       count   858   npmi 1.035
patrick_mahomes  count   816   npmi 1.034
```

It discovered **every multi-word player and the multi-word brand** — the exact entities the hand-built
vocab encoded — without ever being told players or brands exist. It also learned grader+grade units
(`psa_10`, `psa_9.5`, `bgs_8`) and longer trigrams (`rookie_psa_8`). The feature universe it derived
was ~17,200 distinct unigrams + ~190 learned entities, entirely from co-occurrence.

**Selectivity gain (the payoff for candidate counts):** a learned entity used as the anchor has a
lower document-frequency than either of its parts, so its candidate posting is shorter:

```
learned entity      df(phrase)   min df(part)   gain
psa_10                   24,115        41,092    1.7×
bgs_8                    35,206        71,153    2.0×
sp_psa                   17,194        49,739    2.9×
rookie_psa               17,036        50,408    3.0×
```

Multi-word *names* (rare entities) gain far more — `michael_jordan` (df ≈ 4,239) is a vastly better
anchor than `michael` or `jordan` alone. This is candidate reduction obtained *for free* from the
corpus, with no taxonomy. (Full learner capture in
[`../performance/benchmark-results.txt`](../performance/benchmark-results.txt).)

---

## 3. Which parts are SAFE to learn (and why they can't break the contract)

The zero-false-negative contract requires only that **queries and titles normalize consistently** and
that signatures are built from required features. Against that bar:

- **Tokenization** — deterministic, applied identically to queries and titles. Safe.
- **Entity gluing (phrase induction)** — safe, and this is the important one. The learned phrase set is
  applied identically at compile time and match time. If the learner *wrongly* glues two tokens, the
  worst outcome is a slightly different (still consistent) feature, which can only produce extra
  **candidate** false positives — caught by exact verification, never a false negative. If it *fails*
  to glue a real entity, we just fall back to the unigram anchors (less selective, still correct).
  So phrase learning can only ever change *performance*, not *correctness*.
- **Frequency / selectivity model** — pure counting; feeds the existing optimizer. Safe.

In other words, the entire selectivity-relevant half of normalization is **freely learnable from the
corpus with no correctness risk**, because it only influences which anchor we pick, and the exact
matcher is the source of truth.

---

## 4. The numeric/pattern features come almost for free too

`year:1994`, `grade:10` were pattern rules in the hand normalizer. Two corpus-driven options:

- **Treat numbers as ordinary tokens.** `1994` is just a token with a measured df; the optimizer ranks
  it like anything else. No rule needed. (Range queries — "1990–1995" — would need an explicit
  integer-comparison literal.)
- **Auto-detect token *classes* by behavior**, optionally: tokens that share positional and
  co-occurrence statistics form a latent "field" (all 4-digit 19xx/20xx tokens behave alike; graders
  behave alike). This recovers field structure *without naming it*, via clustering — useful for the
  common-mask assignment, but not required for correctness.

---

## 5. The one risky part — equivalence / aliasing — and how to do it safely

`UD` ≡ `Upper Deck`, `MJ` ≡ `Michael Jordan`, `PSA10` ≡ `PSA 10`. This is the *only* job that touches
**exact-match semantics**, so it's the only one where learning can hurt correctness:

- A **wrong** equivalence (merging two genuinely different things) → false-positive **results**
  (precision loss), not just candidates.
- A **destructive, inconsistent** canonicalization (collapsing a form on one side but not the other) →
  false **negatives** — a contract violation.

Note phrase induction already absorbs the *adjacency* variants safely: `PSA10` (one token) and
`PSA 10` (two adjacent tokens → glued to `psa_10`) both land on the same feature, no equivalence
machinery needed. The genuinely hard cases are **non-adjacent / abbreviation** equivalences (`UD` vs
`upper deck`). Three escalating, precision-first techniques:

1. **Distributional similarity.** Two tokens are equivalence candidates if they appear in highly
   similar query *contexts* (same neighbor distributions). Cheap to compute from the same co-occurrence
   counts; medium precision — propose, don't auto-apply.
2. **Match-feedback validation.** Use the title→query stream: if titles that say `UD` and titles that
   say `upper deck` satisfy the *same* query sets, that's strong, behavioral evidence of equivalence.
   This is self-supervising and high-precision.
3. **Expansion, not collapse.** Implement a confirmed equivalence by *expanding* a query to index under
   *both* surface forms (an any-of over learned-equivalent features) rather than destructively rewriting
   to a canonical token. Expansion only *adds* signatures, so it can never drop a true match; a wrong
   expansion costs candidate false positives, not false negatives. Exact match must also honor the
   equivalence, so equivalences stay **confidence-gated, human-overridable, and reversible**.

Practical recommendation: seed a tiny curated alias set for the few high-value, high-risk equivalences
(graders, the handful of canonical brands), and let the corpus learner propose the long tail under
techniques 1–2, applied via 3. The 80% (entity gluing + selectivity) is fully automatic; the risky
20% (aliasing) is automatic-with-a-safety-rail.

---

## 6. How it slots into the architecture

The learner is an **offline job over the query corpus** that emits a *compiled feature model*:

```
feature_model = {
  token -> dense id,
  learned phrases  -> a daachorse double-array automaton (now DATA-DERIVED, not hand-written),
  df / frequency table,
  optional token-class clusters (latent fields),
  confirmed equivalence classes (with confidence + provenance),
}
```

The engine loads this model as its normalizer — the same `Normalizer::emit` interface, but the phrase
list and frequencies come from the model instead of `default_vocab()`. The daachorse automaton we
already named as the production phrase extractor ([`prior-art.md`](prior-art.md) §5) is the natural
runtime carrier; the only change is that its patterns are *learned* rather than coded.

This also **ties into the "improving compaction" loop** (see the
[design overview](../design/README.md) and [ingestion design](../design/ingestion-and-updates.md)):
each compaction
re-runs the learner on the current query population, re-discovers entities and frequencies, re-ranks
anchors, and rewrites poor signature covers. The feature extractor becomes a living artifact that
tracks the query corpus, instead of a static dictionary that goes stale.

---

## 7. Bottom line

The hand-built vocabulary was never load-bearing for *correctness* or for the *core selectivity
mechanism* — the matcher ranks anchors by frequency, and the query corpus is the feature universe.
We demonstrated, on 500k queries with zero hand-coded vocabulary, that NPMI collocation mining
recovers exactly the entities we had encoded by hand (all multi-word players, the brand, grader+grade
units) and delivers 1.7–3× selectivity gains, purely from co-occurrence. So:

- **Build the tokenizer/feature-extractor from the queries** — entity gluing and frequencies:
  fully automatic, zero correctness risk, and it directly optimizes candidate counts.
- **Keep a thin safety rail only around aliasing** — expansion-not-collapse, feedback-validated,
  confidence-gated — because that is the only sub-problem that can affect result correctness.
- **Wire it into compaction** so the feature model self-updates with the query population.

Run it: `cargo run --release --bin learn -- 500000 50 0.30`.
