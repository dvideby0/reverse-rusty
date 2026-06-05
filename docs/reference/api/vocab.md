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
  "equivalences": [["ud", "upper deck"]],
  "punctuation": [{"ch": "'", "class": "fold"}, {"ch": "-", "class": "fold"}]
}
```

## `PUT /_vocab` — Replace vocabulary

Replace the engine's vocabulary. Existing stored queries are **automatically recompiled** under the
new normalizer — under the same lock, before the new snapshot is published — so the change takes
effect immediately with zero false negatives. `recompiled` reports how many queries were rebuilt.

```bash
curl -X PUT localhost:9200/_vocab \
  -H 'Content-Type: application/json' \
  -d '{"synonyms": [{"token": "rc", "canonical": "term:rookie", "kind": "category"}], "phrases": [], "graders": [], "grade_words": []}'
```

```json
{
  "acknowledged": true,
  "recompiled": 1280
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

**Declaring punctuation rules (ADR-058).** The optional `punctuation` block reclassifies how individual
characters are handled in byte-cleaning. Each rule is `{"ch": "<char>", "class": "<fold|split|keep|marker>"}`:
`fold` deletes the character so its neighbors **join** into one token (so `O'Brien`, `O-Brien`, and
`OBrien` all become `obrien` — closing a recall gap for punctuation-only spelling differences), `split`
makes it a word boundary, `keep` leaves it literally in place, and `marker` emits it as its own token. The
default — `.` is `keep`, `#`/`/` are `marker`, everything else is `split` — is reproduced exactly when the
block is omitted (so older vocab payloads are unchanged). The same table applies to both queries and
titles, so the lossless-cover contract is preserved under any configuration.

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


---

## Learned-alias registry (ADR-060)

A governance layer over equivalence expansion (ADR-054): a registry of alias **candidates** with
provenance, a structural **kind**, a confidence score, and a lifecycle **status** (`candidate` /
`active` / `rejected`). Only **active** single-token groups affect matching (via FN-safe expansion);
candidates are recorded for review and never change results. Conservative by construction —
single-token spelling/abbreviation variants auto-activate, while learned multi-form category
alternatives (`(psa, bgs, sgc)`), multi-word aliases (a Phase-2 token-graph feature), and mixed-kind
groups are recorded as candidates, **never silently active**. Single-node (like ADR-054).

### `GET /_vocab/aliases`

Returns the full registry (for review) plus a status summary. Lock-free (reads the `ArcSwap`
snapshot, ADR-016).

```bash
curl 'localhost:9200/_vocab/aliases'
```

```json
{
  "aliases": {
    "entries": [
      { "forms": ["autograph", "autographs"], "provenance": "learned_from_queries",
        "kind": "single_token_variant", "status": "active", "confidence": 0.6 },
      { "forms": ["bgs", "psa", "sgc"], "provenance": "learned_from_queries",
        "kind": "single_token_distinct", "status": "candidate", "confidence": 0.5 }
    ]
  },
  "summary": { "active": 1, "candidate": 1, "rejected": 0 }
}
```

### `POST /_vocab/aliases/import`

Import a Solr/Lucene synonym file (the format ES's `synonyms_path` consumes) into the registry and
apply it live. Comma lists are one equivalence group; `a, b => c, d` mappings are unioned into one
**bidirectional** group (RR equivalences are bidirectional — a recall-safe over-approximation); `#`
comments and `\,` escapes are honored. Safe single-token groups auto-activate; multi-word groups are
recorded as candidates.

```bash
curl -X POST localhost:9200/_vocab/aliases/import \
  -H 'Content-Type: application/json' \
  -d '{"synonyms": "autograph, autographs\nrc => rookie card"}'
```

```json
{ "acknowledged": true, "activated": 1, "recompiled": 1280,
  "summary": { "active": 1, "candidate": 1, "rejected": 0 } }
```

`activated` is the number of groups switched to active; `recompiled` is the number of stored queries
rebuilt in place so the change takes effect immediately (no restart), with zero false negatives.

### `POST /_vocab/aliases/learn_and_apply`

Learn alias candidates from the engine's OWN stored queries (any-of co-occurrence) into the registry
and apply. Conservative: only clear single-token variants auto-activate; everything else lands as a
candidate (inspect via `GET /_vocab/aliases`, then declare via an import to activate). `?min_count=N`
(default 2). Response shape matches `import`.
