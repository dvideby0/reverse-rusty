# Vocabulary — REST API

> Part of the [REST API reference](../api.md). Query language: [`dsl.md`](../dsl.md).

## `GET` / `HEAD /_vocab` — Current vocabulary

```bash
curl localhost:9200/_vocab
curl -I localhost:9200/_vocab
```

```json
{
  "synonyms": [
    {"token": "pkg", "canonical": "term:package", "kind": "generic"}
  ],
  "phrases": [
    {"tokens": ["north", "star"], "canonical": "brand:north_star", "kind": "brand"},
    {"tokens": ["wireless", "mouse"], "canonical": "entity:wireless_mouse", "kind": "entity"}
  ],
  "equivalences": [["ns", "north star"]],
  "punctuation": [{"ch": "'", "class": "fold"}, {"ch": "-", "class": "fold"}],
  "number_context": ["model"],
  "aliases": {"entries": []}
}
```

The GET response is the one complete installed `Vocab` document. It can be saved as the
single-node `--vocab-file` or sent back to `PUT /_vocab` without projection or reconstruction.
`HEAD` performs the same snapshot capture and serialization but returns no body. Every outcome
reached through the read route includes `Cache-Control: no-store`; success is
`Content-Type: application/json`.

The read is strict: it accepts no query parameters or request body. GET/HEAD body extraction has a
64 KiB ceiling and a 250 ms read deadline, independent of the write operation's 16 MiB allowance.
Errors use the standard JSON envelope: invalid query/body is 400, a stalled body is 408, oversized
input is 413, closed read admission is 503, and serialization/worker failure is 500. Other methods
are 405 with `Allow: GET, HEAD, PUT`.

Standalone mode captures one immutable lock-free engine snapshot. Coordinator mode clones the
installed vocabulary while briefly holding the cluster read guard on a blocking worker, releases
the guard, and serializes afterward. Both share the server's single bounded administrative-read
slot, so concurrent large documents cannot multiply clone/serialization work; waiting for that
slot is asynchronous.

This is deliberately a native API. Elasticsearch
[`GET /_synonyms/{id}`](https://www.elastic.co/guide/en/elasticsearch/reference/current/get-synonyms-set.html)
and
[`PUT /_synonyms/{id}`](https://www.elastic.co/guide/en/elasticsearch/reference/current/put-synonyms-set.html)
operate on one named, pageable Solr-rule set, while OpenSearch exposes synonyms through
[analyzer token-filter configuration](https://docs.opensearch.org/latest/analyzers/token-filters/synonym/).
Reverse Rusty's document also owns phrases, equivalences, punctuation, numeric context, and the
governed alias registry, so neither the standard path nor its response shape is an honest alias.

## `PUT /_vocab` — Replace vocabulary

Replace the engine's vocabulary. Existing stored queries are **automatically recompiled** under the
new normalizer before the new snapshot is published, so the change takes effect immediately with
zero false negatives. Standalone mode performs the replacement and recompile under its engine writer
lock. Coordinator mode performs one blue/green re-placement under the cluster write lock and
checkpoints a durable cluster before success. Both return the same response shape;
`recompiled` reports how many live queries were rebuilt.

The request is strict and synchronous:

- only query-free `PUT` is accepted;
- a body and `Content-Type: application/json` (or an `application/*+json` vendor media type) are
  required;
- the complete `Vocab` object rejects malformed JSON and unknown top-level fields;
- body extraction is capped at 16 MiB and must complete within 5 seconds; and
- every response includes `Cache-Control: no-store` and uses the standard JSON error envelope.

The decoded request waits asynchronously for the server's single administrative-work slot. The
O(corpus) replacement then runs on a blocking worker, so lock acquisition and compilation do not
occupy a Tokio request worker. The operation has no cancellable execution timeout: after admission,
it runs to a terminal result and publishes a coherent snapshot even if the client disconnects.
Concurrent stats and vocabulary read/write operations serialize through the same bounded slot.

> **Durability:** on a successful response, the recompiled queries have committed like a flush.
> The single-node vocabulary **object** itself still lives in memory: `--vocab-file` is the restart
> source (ADR-015). Save the same JSON there (for example, capture `GET /_vocab`) after a successful
> REST replacement; reopening with a stale or absent file would desynchronize title normalization
> from the persisted queries. A cluster persists its vocabulary in the coordinator manifest and
> does not have this file caveat. If a standalone recompile becomes live but its durable commit
> fails, the process publishes that coherent live state but returns
> `503 persistence_unavailable` instead of `acknowledged: true`; the old manifest remains
> authoritative for restart. A durable coordinator checkpoint failure is likewise not
> acknowledged even though the blue/green state may already be live; inspect `GET /_vocab` before
> deciding how to recover.

```bash
curl -X PUT localhost:9200/_vocab \
  -H 'Content-Type: application/json' \
  -d '{"synonyms":[{"token":"pkg","canonical":"term:package","kind":"generic"}],"phrases":[],"equivalences":[],"punctuation":[],"number_context":[],"aliases":{"entries":[]}}'
```

```json
{
  "took": 37,
  "took_ms": 37.428,
  "acknowledged": true,
  "recompiled": 1280
}
```

`took` is the whole-route elapsed time in integer milliseconds and `took_ms` preserves fractional
milliseconds. Invalid transport or JSON is 400/408/413/415 as applicable, an invalid vocabulary is
400 `vocab_error`, closed admission or unhealthy/degraded persistence is 503, and a blocking-worker
or impossible incomplete rebuild failure is 500. A non-local coordinator still returns 400 because
the current remote shard protocol does not ship the replacement normalizer.

**Declaring equivalences (ADR-054).** The optional `equivalences` block is a list of groups of
surface forms treated as the same entity (e.g. `[["ns", "north star"], ["pkg", "package"]]`). Unlike
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
block is omitted. The same table applies to both queries and
titles, so the lossless-cover contract is preserved under any configuration.

**Number-context words (ADR-069).** The optional `number_context` array lists tokens that demote an
immediately-following number to a generic term (`model 1995` → `term:1995`, never `year:1995`).
Omitted or empty means position-insensitive typing, so a four-digit year is `year:N` everywhere.
Like every vocabulary change, applying a custom list recompiles stored queries under the new typing;
the same list runs over queries and titles.

## `POST /_vocab/learn` — Learn vocabulary from queries

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
[`dsl.md`](../dsl.md#vocabulary) for how vocabulary affects matching.

**Opt-in NPMI corpus phrase induction (ADR-053).** Add `"corpus_phrases": true` to ALSO induce
multi-token entity **phrases** (e.g. `north star` → `north_star`) from the supplied query text via NPMI
collocation mining, on top of the any-of synonyms. Phrases only — never aliases. They are applied
**additively** (a match emits the phrase feature AND keeps the component features), so a query
referencing a component never loses a candidate — important because this is a recall-first
candidate generator. A phrase-*form* query does tighten to requiring the adjacent phrase; for genuine
entities, which appear adjacent in real titles, that is negligible — but it is why this is opt-in and
reviewable. Explicit DSL quotes use the same analyzed adjacency contract; see
[`dsl.md#quoted-phrases`](../dsl.md#quoted-phrases) and ADR-120. Tunable:
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

## `POST /_vocab/learn_and_apply` — Learn from stored queries and apply

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

## Learned-alias registry (ADR-060/061/102/103)

The registry governs equivalence expansion with provenance, a structural **kind**, confidence,
optional feedback evidence, and a lifecycle **status** (`candidate`, `active`, or `rejected`).
Candidates and rejected entries are metadata only. Active, expressible groups widen positive
matching through the false-negative-safe equivalence path.

Current activation policy:

| Source and kind | Default status |
|---|---|
| Operator-imported or manually edited single-token or multi-word group | `active` |
| Any-of-learned clear single-token spelling/abbreviation variant | `active` |
| Any-of-learned distinct-token or multi-word group | `candidate` |
| Distributionally discovered group, of any kind | `candidate` |
| Mixed-feature-kind or otherwise unexpressible group | `candidate`; it cannot affect matching |

Multi-word aliases are implemented, not deferred: ADR-061 supplies query-side collapse plus the
two title feature views, and ADR-076 makes cluster routing positive-view-aware. Import,
learn-and-apply, and an edited registry installed through `PUT /_vocab` work in single-node and
cluster modes. The lower-confidence discovery-record and match-feedback workflows remain
single-node where noted below.

### `GET` / `HEAD /_vocab/aliases`

Returns the governed registry for review. GET and HEAD accept optional non-negative integer
`from` and `size` parameters, matching the familiar paging controls on Elasticsearch's
[get-synonym-set API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-synonyms-get-synonym).
`count` is the total number of stored entries before paging; `summary` likewise describes the whole
registry, not only the returned page. Omitting `size` preserves the historical full-registry
response. `size=0` or an offset at or beyond `count` returns an empty `entries` array.

```bash
curl 'localhost:9200/_vocab/aliases?from=0&size=100'
```

```json
{
  "count": 2,
  "aliases": {
    "entries": [
      { "forms": ["package", "packages"], "provenance": "learned_from_queries",
        "kind": "single_token_variant", "status": "active", "confidence": 0.6 },
      { "forms": ["new", "refurbished"], "provenance": "learned_from_queries",
        "kind": "single_token_distinct", "status": "candidate", "confidence": 0.5 }
    ]
  },
  "summary": { "active": 1, "candidate": 1, "rejected": 0 }
}
```

Entry order is the registry's stable stored order. Offset pages do not pin a snapshot across
requests: if another call replaces the registry between pages, the next page can reflect the new
registry. Fetch without `size`, or fetch `GET /_vocab` once, when one coherent full review is
required.

The transport is strict and bodyless. Unknown, duplicate, or malformed parameters and non-empty
bodies return structured 400 errors; stalled bodies return 408; bodies over the GET-specific
64 KiB limit return 413; unsupported methods return 405 with `Allow: GET, HEAD`. Every
route-reached response has `Cache-Control: no-store` and fixed `vocab_aliases_get` telemetry. HEAD
returns the corresponding paged GET headers and `Content-Length` with no body.

Registry capture, paging, and JSON serialization share the one administrative blocking-work slot.
Standalone mode captures one immutable snapshot without an engine lock. Coordinator mode clones
the registry under a brief cluster read lock inside the blocking worker and releases the lock
before serialization. Closed admission returns `503 aliases_unavailable`; worker or serialization
failure returns the same type with 500.

This remains a native governance API ([ADR-150](../../decisions/adr-150-alias-registry-read-api-contract.md)).
Elasticsearch/OpenSearch
[aliases](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-alias)
route indices and data streams, while analyzer synonym rules do not carry this registry's
provenance, kind, confidence, evidence, and lifecycle status. Reverse Rusty therefore does not
expose this data through `/_alias`, `/_cat/aliases`, or a fabricated named synonym-set path.

### `POST /_vocab/aliases/import`

Strictly import Solr/Lucene synonym rules into the one governed registry and apply them
synchronously. The native body accepts complete file text:

```bash
curl -X POST 'localhost:9200/_vocab/aliases/import?refresh=true' \
  -H 'Content-Type: application/json' \
  -d '{"synonyms":"package, pkg\nwireless mouse => cordless mouse",
       "format":"solr","expand":true}'
```

The body may instead use the familiar Elasticsearch
[synonym-set rule envelope](https://www.elastic.co/guide/en/elasticsearch/reference/current/put-synonyms-set.html):

```json
{
  "synonyms_set": [
    { "id": "package-rule", "synonyms": "package, pkg" },
    { "id": "mouse-rule", "synonyms": "wireless mouse => cordless mouse" }
  ]
}
```

`synonyms_set` may be one rule object or an array. Each object contains exactly one rule. Optional
IDs must be unique, non-empty, and at most 256 bytes; they are accepted as request metadata but are
not persisted because Reverse Rusty's registry keys canonical form groups, not named rules or
sets. This route does not claim Elasticsearch's `/_synonyms/{id}` set isolation, retrieval,
deletion, or analyzer-reload model.

Comma lists are one equivalence group. `a, b => c, d` is intentionally unioned into one
**bidirectional** group: Reverse Rusty uses matching-safe expansion and does not implement
directional replacement. Blank lines and `#` comments are ignored, while backslash escapes the next
character (including `,`, `#`, and `\`). Malformed mappings, empty forms, duplicate-only groups,
dangling escapes, and an empty/comment-only file reject the whole import with a line-specific 400
error; no partial registry mutation occurs.

The optional OpenSearch
[synonym-filter](https://docs.opensearch.org/latest/analyzers/token-filters/synonym/) controls
default to `format: "solr"` and `expand: true`. Those are the only supported values: WordNet and
directional `expand: false` fail explicitly. Elasticsearch-style `refresh=true` or omission is
accepted in the query string; `refresh=false` is rejected because every acknowledged import is
synchronously live.

An imported expressible single-token or multi-word group activates because the file is an operator
declaration. An unexpressible or mixed-feature-kind group remains a candidate.

```json
{ "took": 37, "took_ms": 37.42, "acknowledged": true, "result": "updated",
  "rules": 2, "activated": 2, "recompiled": 1280,
  "summary": { "active": 2, "candidate": 0, "rejected": 0 } }
```

`rules` is the accepted rule count, `activated` is the number of groups switched to active, and
`recompiled` is the number of stored queries rebuilt so the change takes effect immediately. An
identical retry returns `result: "noop"`, `activated: 0`, and `recompiled: 0`; it does not rebuild,
checkpoint, or republish an identical snapshot. Standalone and coordinator modes return this same
shape and always use `recompiled` (never a coordinator-only `rebuilt` alias).

The request is strict JSON (`application/json` or `application/*+json`), capped at 16 MiB and five
seconds. At most 10,000 rules and 256 forms per rule are accepted. Unknown or duplicate query
parameters, unknown JSON fields, both/neither input envelopes, malformed JSON, and unsupported
compatibility controls fail explicitly. Unsupported methods return 405 with `Allow: POST`. Every
route-reached response has `Cache-Control: no-store` and fixed `vocab_aliases_import` telemetry.

Admission, engine/coordinator lock waits, parsing apply, and any rebuild run in the shared one-slot
administrative blocking worker. Closed admission returns `503 aliases_unavailable`; worker failure
returns the same type with 500. In standalone durable mode, unhealthy persistence is refused before
mutation. A successful mutation must rebuild every live source and leave no stale segment. If the
coherent live rebuild completes but its durable commit fails, it is published for read consistency
but returns `503 persistence_unavailable` and is never acknowledged. Coordinator mode uses the same
checkpointing vocabulary-rebuild path and typed shard failures.

This contract is recorded in
[ADR-152](../../decisions/adr-152-alias-import-api-contract.md).

### `POST /_vocab/aliases/learn_and_apply`

Learn alias candidates from the engine's own stored queries (any-of co-occurrence) into the registry
and apply. Only clear single-token variants auto-activate; distinct-token and multi-word groups land
as candidates. Inspect them with `GET /_vocab/aliases`, then make an operator declaration through an
import or an edited `PUT /_vocab` document if appropriate. `?min_count=N` defaults to 2. The response
contains `acknowledged`, `activated`, `recompiled`, and `summary`; unlike import it does not include
rule-count or import-result fields.

### `POST /_vocab/aliases/discover`

Run deterministic, distributional discovery (ADR-102) and return review proposals without recording
or activating anything. With no body, single-node mode analyzes its live stored query sources. An
explicit corpus is accepted in either mode:

```json
{
  "queries": [[1, "north star wireless mouse"], [2, "ns wireless mouse"]],
  "min_token_freq": 5,
  "min_similarity": 0.60,
  "max_pairs": 100,
  "max_vocab": 4096,
  "max_cooccurrence_rate": 0.05,
  "glue_phrases": true,
  "include_numeric": false
}
```

The fields shown are the defaults. The response is
`{"count":N,"proposals":[{"forms":["ns","north star"],"similarity":0.91,
"cooccurrence_rate":0.0}]}` in best-first deterministic order. Cluster mode requires the explicit
`queries` corpus because the coordinator has no cross-shard source-gather operation; omitting it is
a 400.

### `POST /_vocab/aliases/discover_and_record`

Single-node only. Run discovery over this engine's own stored queries and record every proposal as a
review `candidate`. The body may override the same knobs as `discover`, but may not supply
`queries`. Nothing activates, matching does not change, and `recompiled` is always zero:

```json
{
  "acknowledged": true,
  "proposed": 12,
  "new_candidates": 8,
  "rediscovered": 3,
  "rejected_sticky": 1,
  "recompiled": 0,
  "summary": {"active": 2, "candidate": 8, "rejected": 1}
}
```

Cluster mode returns 501. Run the dry discovery against an explicit corpus, review it, and install
the resulting registry with `PUT /_vocab` instead.

### Match-feedback validation

ADR-103 can passively compare which queries match titles containing each form of a tracked
two-form candidate. Capture is single-node, in memory, default off, and applies to compatibility
`/_search` and `/_mpercolate` traffic. Enable it with the dynamic settings
`alias_feedback_capture=true`; `alias_feedback_max_pairs` bounds the tracked candidate set.

`GET /_vocab/aliases/feedback` returns the rolling evidence. Threshold query parameters default to
`min_overlap=0.5`, `min_titles=50`, and `min_queries=20`:

```json
{
  "capture_enabled": true,
  "tracked_pairs": 1,
  "min_overlap": 0.5,
  "min_titles": 50,
  "min_queries": 20,
  "pairs": [{
    "forms": ["ns", "north star"],
    "titles_a": 75,
    "titles_b": 81,
    "titles_both": 2,
    "sampled_a": 43,
    "sampled_b": 46,
    "excluded": 4,
    "overlap": 0.78,
    "validated": true
  }]
}
```

`POST /_vocab/aliases/validate_and_apply` with the same thresholds stamps evidence and raises
confidence for validated candidates without changing matching. Add `?activate=true` to explicitly
promote eligible validated candidates through the full recompile path; rejected or mixed-kind
entries are never resurrected by automation. The response reports `validated`, `stamped`,
`activated`, `recompiled`, and the status `summary`.

`POST /_vocab/aliases/feedback/reset` clears the process-local evidence window and returns
`{"acknowledged":true}`. All three feedback endpoints return 501 in cluster mode; run capture on a
single-node replica of the title stream and install reviewed activations through cluster
`PUT /_vocab`.
