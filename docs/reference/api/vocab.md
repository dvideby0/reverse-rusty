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
  "grade_words": ["gem"]
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
collocation mining, on top of the any-of synonyms. Phrases only — never aliases — so applying them is
lossless-cover safe (the same normalizer glues both queries and titles; zero false negatives). Tunable:
`npmi_min_count` (min adjacent co-occurrence, default 3), `npmi_tau` (binding-strength threshold,
default 0.30), `npmi_iterations` (bigram→trigram passes, default 2). Absent ⇒ any-of learning only,
exactly as before.

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
`npmi_iterations` (default 2). Absent ⇒ any-of learning only (byte-identical to before).

```bash
curl -X POST 'localhost:9200/_vocab/learn_and_apply?corpus_phrases=true&npmi_min_count=3'
```

