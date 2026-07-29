# `POST /_vocab/learn` — Learn vocabulary from queries

> [Vocabulary & alias APIs](../vocab.md) · [REST API hub](../../api.md)

Send raw query text to discover synonym relationships from any-of groups. Returns the learned
vocabulary without applying it — review and then `PUT /_vocab` to use it. This is a native
review-first API: Elasticsearch manages named Solr-rule synonym sets and OpenSearch configures
synonyms through analyzer token filters, but neither surface learns this vocabulary document from a
query corpus.

```bash
curl -X POST localhost:9200/_vocab/learn \
  -H 'Content-Type: application/json' \
  -d '{
    "queries": [[1, "(package,pkg) 2024"], [2, "(package,pkg) 2023"]],
    "min_count": 2
  }'
```

```json
{
  "synonyms": [
    {"token": "pkg", "canonical": "term:package", "kind": "generic"}
  ],
  "phrases": [],
  "equivalences": [],
  "punctuation": [],
  "aliases": {"entries": []}
}
```

The `min_count` parameter (default: 2) controls how many times a synonym pair must appear across
different queries before it is included. It must be at least 1; higher values reduce noise. Query
IDs must be unique, because a repeated ID is not evidence from a different query. See
[`dsl.md`](../../dsl.md#vocabulary) for how vocabulary affects matching.

**Opt-in NPMI corpus phrase induction (ADR-053).** Add `"corpus_phrases": true` to ALSO induce
multi-token entity **phrases** (e.g. `north star` → `north_star`) from the supplied query text via NPMI
collocation mining, on top of the any-of synonyms. Phrases only — never aliases. They are applied
**additively** (a match emits the phrase feature AND keeps the component features), so a query
referencing a component never loses a candidate — important because this is a recall-first
candidate generator. A phrase-*form* query does tighten to requiring the adjacent phrase; for genuine
entities, which appear adjacent in real titles, that is negligible — but it is why this is opt-in and
reviewable. Explicit DSL quotes use the same analyzed adjacency contract; see
[`dsl.md#quoted-phrases`](../../dsl.md#quoted-phrases) and ADR-120. Tunable:
`npmi_min_count` (min adjacent co-occurrence, default 3), `npmi_tau` (binding-strength threshold,
default 0.30), `npmi_iterations` (bigram→trigram passes, default 2). Absent ⇒ any-of learning only,
exactly as before. Add `"learn_equivalences": true` to instead learn the any-of groups as
**equivalence groups** applied via FN-safe expansion (ADR-054) rather than collapse synonyms.

```bash
curl -X POST localhost:9200/_vocab/learn \
  -H 'Content-Type: application/json' \
  -d '{"queries": [[1,"north star 2024"],[2,"north star wireless mouse"]],
       "corpus_phrases": true, "npmi_min_count": 2}'
```

Contract:

- POST is the only method. Query parameters, unknown JSON fields, a missing `queries` field,
  malformed tuples, duplicate query IDs, and invalid query DSL are rejected with the standard JSON
  error envelope. `application/json` and `application/*+json` are accepted.
- The caller-supplied corpus is required in both standalone and coordinator mode; the endpoint never
  substitutes stored engine or cluster queries. Use `POST /_vocab/learn_and_apply` for the
  own-corpus operation. An empty `queries` array is a valid dry run and returns an empty vocabulary.
- A request may contain at most 100,000 queries and 16 MiB of JSON. Each query uses the public DSL
  ceilings: 10,240 bytes, 256 clauses, and 64 members per any-of group. The request body must
  complete within five seconds. The corpus may expand to at most 100,000 potential any-of
  relationship observations; phrase induction accepts at most 100,000 corpus tokens.
- `npmi_tau`, `npmi_min_count`, and `npmi_iterations` are accepted only with
  `corpus_phrases: true`. Tau must be within `[-1, 1]`, minimum count must be at least 1, and
  iterations must be within `1..=8`.
- Success is one complete, bare, round-trippable `Vocab` document with
  `Content-Type: application/json` and `Cache-Control: no-store`. JSON decoding, corpus validation,
  learning, and serialization run off the async request workers under the shared one-at-a-time
  administrative-work admission. A result is limited to 100,000 entries and 16 MiB, so it remains
  acceptable to `PUT /_vocab`; exceedance is a 400 asking the caller to raise learning thresholds.
  A closed admission gate returns `503 vocab_unavailable`.
