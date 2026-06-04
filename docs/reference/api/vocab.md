# Vocabulary — REST API

> Part of the [REST API reference](../api.md). Query language: [`dsl.md`](../dsl.md).

## `GET /_vocab` — Current vocabulary

```bash
curl localhost:9200/_vocab
```

```json
{
  "synonyms": [
    {"token": "rc", "canonical": "term:rookie", "kind": "generic"}
  ],
  "phrases": [
    {"tokens": ["upper", "deck"], "canonical": "term:upper_deck", "kind": "generic"}
  ],
  "graders": ["psa"],
  "grade_words": ["gem"],
  "equivalences": [["ud", "upper deck"]]
}
```

## `PUT /_vocab` — Replace vocabulary

Replace the engine's vocabulary. If queries have already been ingested, the response includes a
warning — you should reingest for consistent matching.

```bash
curl -X PUT localhost:9200/_vocab \
  -H 'Content-Type: application/json' \
  -d '{"synonyms": [{"token": "rc", "canonical": "term:rookie", "kind": "category"}], "phrases": [], "graders": [], "grade_words": []}'
```

```json
{
  "acknowledged": true,
  "warning": "normalizer changed with existing queries; reingest for consistent matching"
}
```

**Declaring equivalences (ADR-054).** The optional `equivalences` block is a list of groups of
surface forms treated as the same entity (e.g. `[["ud", "upper deck"], ["rc", "rookie"]]`). Unlike
`synonyms` (which *collapse* a form to a canonical via the normalizer), equivalences are applied by
**expansion**: a query requiring one form is widened to an any-of over the group, so it matches a
title bearing any form. Expansion only grows a query's match set, so it is **false-negative-safe** —
a wrong/uncertain equivalence can only add bounded false positives, never drop a true match. Each form
should resolve to a single entity (glue a multi-token form as a phrase first); a form that doesn't is
skipped. Applying the change recompiles existing queries through the expansion.

## `POST /_vocab/learn` — Learn vocabulary from queries

Send raw query text to discover synonym relationships from any-of groups. Returns the learned
vocabulary without applying it — review and then `PUT /_vocab` to use it.

```bash
curl -X POST localhost:9200/_vocab/learn \
  -H 'Content-Type: application/json' \
  -d '{
    "queries": [[1, "(rookie,rc) 2024"], [2, "(rookie,rc) 2023"]],
    "min_count": 2
  }'
```

```json
{
  "synonyms": [
    {"token": "rc", "canonical": "term:rookie", "kind": "generic"}
  ],
  "phrases": [],
  "graders": [],
  "grade_words": []
}
```

The `min_count` parameter (default: 2) controls how many times a synonym pair must appear across
different queries before it's included. Higher values reduce noise. See [`dsl.md`](../dsl.md#vocabulary)
for how vocabulary affects matching.

**Opt-in NPMI corpus phrase induction (ADR-053).** Add `"corpus_phrases": true` to ALSO induce
multi-token entity **phrases** (e.g. `upper deck` → `upper_deck`) from the supplied query text via NPMI
collocation mining, on top of the any-of synonyms. Phrases only — never aliases. They are applied
**additively** (a match emits the phrase feature AND keeps the component features), so a query
referencing a component never loses a candidate — important because this is a recall-first
candidate generator. (A phrase-*form* query does tighten to requiring the adjacent phrase; for genuine
entities, which appear adjacent in real titles, that is negligible — but it is why this is opt-in and
reviewable.) Tunable:
`npmi_min_count` (min adjacent co-occurrence, default 3), `npmi_tau` (binding-strength threshold,
default 0.30), `npmi_iterations` (bigram→trigram passes, default 2). Absent ⇒ any-of learning only,
exactly as before. Add `"learn_equivalences": true` to instead learn the any-of groups as
**equivalence groups** applied via FN-safe expansion (ADR-054) rather than collapse synonyms.

```bash
curl -X POST localhost:9200/_vocab/learn \
  -H 'Content-Type: application/json' \
  -d '{"queries": [[1,"upper deck 1994"],[2,"upper deck rookie"]],
       "corpus_phrases": true, "npmi_min_count": 2}'
```

## `POST /_vocab/learn_and_apply` — Learn from stored queries and apply

Learn synonyms from the engine's **own** already-ingested queries and apply them in one step (unlike
`POST /_vocab/learn`, which only returns synonyms learned from caller-supplied queries for review). The
engine re-mints its vocabulary, recompiles every stored query under the new normalizer, and atomically
swaps — so both surface forms of each learned alias match immediately, with zero false negatives
(ADR-046). The change is durable (it survives reopen).

```bash
curl -X POST 'localhost:9200/_vocab/learn_and_apply?min_count=2'
```

```json
{
  "acknowledged": true,
  "recompiled": 1280
}
```

`min_count` (query parameter, default: 2) is the minimum any-of occurrences before a synonym pair is
learned; `recompiled` is the number of stored queries rebuilt under the new vocabulary.

Add `?corpus_phrases=true` to ALSO self-derive entity **phrases** from the engine's own live query text
via NPMI corpus phrase induction (ADR-053), applied through the same recompile/blue-green rebuild with
zero false negatives. Tunable via `npmi_min_count` (default 3), `npmi_tau` (default 0.30), and
`npmi_iterations` (default 2). Add `?learn_equivalences=true` to learn the any-of groups as
**equivalence groups** applied via FN-safe expansion (ADR-054) instead of collapse synonyms.
Absent ⇒ any-of synonym learning only (byte-identical to before).

```bash
curl -X POST 'localhost:9200/_vocab/learn_and_apply?corpus_phrases=true&npmi_min_count=3'
```

