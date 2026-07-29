# `POST /_vocab/learn_and_apply` — Learn from stored queries and apply

> [Vocabulary & alias APIs](../vocab.md) · [REST API hub](../../api.md)

Learn vocabulary from the server's **own** already-ingested queries and apply it synchronously. This
is the mutating counterpart to review-first `POST /_vocab/learn`: it gathers the current live source
corpus, learns, merges the result under the installed vocabulary, recompiles every stored query, and
publishes the coherent replacement. Existing rules win collisions and previously installed learned
rules remain until removed with an edited `PUT /_vocab`; use the dry run first when operator review
or exact replacement is required.

```bash
curl -X POST 'localhost:9200/_vocab/learn_and_apply?min_count=2'
```

```json
{
  "took": 37,
  "took_ms": 37.428,
  "acknowledged": true,
  "recompiled": 1280
}
```

Standalone and coordinator modes return this same shape. `took` is whole-route elapsed time in
integer milliseconds, `took_ms` preserves fractional milliseconds, and `recompiled` is the number
of unique live query sources rebuilt under the learned vocabulary.

Controls are strict query parameters:

- `min_count` defaults to 2 and must be at least 1. It counts distinct stored queries supporting an
  any-of relationship, not repeated clauses within one query.
- `corpus_phrases=true` also self-derives entity phrases from the live text via NPMI corpus phrase
  induction (ADR-053). Only then may `npmi_min_count` (default 3, minimum 1), `npmi_tau` (default
  0.30, finite and within `[-1, 1]`), and `npmi_iterations` (default 2, range `1..=8`) be sent.
- `learn_equivalences=true` learns any-of relationships as widening equivalence groups (ADR-054)
  instead of collapse synonyms. It can be combined with phrase induction.

Any-of equivalence expansion is structurally monotone: it can add matches but not remove them.
Induced phrases preserve component emissions and the lossless cover for the active feature model,
but a query written in the induced phrase form can intentionally tighten to adjacency; phrase
induction is therefore opt-in. With neither opt-in, behavior is any-of synonym learning only.

```bash
curl -X POST 'localhost:9200/_vocab/learn_and_apply?corpus_phrases=true&npmi_min_count=3'
```

The transport is bodyless and bounded:

- POST is the only method. Unknown, duplicate, malformed, or out-of-range query controls and any
  request body are rejected through the standard JSON error envelope.
- Body extraction has a 64 KiB ceiling and a 250 ms deadline even though only an empty body is
  valid. Every route-reached response has `Cache-Control: no-store`; unsupported methods are 405
  with `Allow: POST`.
- The request waits asynchronously for the shared one-at-a-time administrative-work slot. Corpus
  gathering, learning, lock acquisition, recompilation, persistence, and snapshot publication run
  on one blocking worker that owns the permit. There is no execution timeout after admission: a
  disconnect does not cancel a partially completed feature-model change.

The O(live corpus) rebuild has the same correctness and durability boundary as `PUT /_vocab`.
Standalone verifies that every canonical live source was recompiled and no stale segment remains.
A successful durable cluster checkpoint is included before acknowledgement. If a coherent
standalone rebuild becomes live but its durable commit fails, the new snapshot is published but the
response is `503 persistence_unavailable`, never `acknowledged: true`; a cluster checkpoint failure
is likewise not acknowledged. A single-node operator must save `GET /_vocab` to the configured
`--vocab-file` before restart, while a durable cluster stores it in the coordinator manifest.
Closed admission is 503, invalid controls/vocabulary or a non-local coordinator are 400, and a
blocking-worker or impossible incomplete rebuild is 500.

This remains a native API. Elasticsearch can create/update explicit named synonym sets and reload
eligible search analyzers; OpenSearch configures explicit synonyms through analyzer token filters
and can refresh updateable search analyzers. Neither product learns a vocabulary from stored reverse
queries and atomically recompiles that corpus, so an alias to those endpoints would misrepresent the
operation.

---
