# `POST /_vocab/aliases/import` — Import and apply aliases

> [Vocabulary & alias APIs](../vocab.md) · [REST API hub](../../api.md)

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
republish an identical snapshot, or checkpoint an already committed coordinator generation. If the
previous durable coordinator attempt rebuilt live state but failed before committing its manifest,
the identical retry attests or repairs the feature-model control transition and finishes that
checkpoint before acknowledging the no-op. It can replace only the exact pre-import manifest
retained by that live attempt. If the current manifest was renamed but its directory sync failed,
the retry re-attests and syncs that exact next-epoch commit, including its segment registry, next
segment IDs, source sidecars, and log replay cursor. An unreadable, incompatible, divergent, or
otherwise newer manifest fails loud and is not overwritten. For embedded callers, an identical
import encountered between the public `set_vocab` and `recompile_stale_segments` steps completes
the pending rebuild and reports `result: "updated"` with its nonzero `recompiled` count.
Standalone and coordinator modes return this same shape and always use `recompiled` (never a
coordinator-only `rebuilt` alias).

The request is strict JSON (`application/json` or `application/*+json`), capped at 16 MiB and five
seconds. At most 10,000 rules and 256 forms per rule are accepted. Unknown or duplicate query
parameters, unknown JSON fields, both/neither input envelopes, malformed JSON, and unsupported
compatibility controls fail explicitly. Optional rule IDs are limited to 256 raw input bytes, must
remain non-empty after trimming, and must be unique after trimming. Unsupported methods return 405
with `Allow: POST`. Every route-reached response has `Cache-Control: no-store` and fixed
`vocab_aliases_import` telemetry.

Admission, engine/coordinator lock waits, parsing apply, and any rebuild run in the shared one-slot
administrative blocking worker. Closed admission returns `503 aliases_unavailable`; worker failure
returns the same type with 500. In standalone durable mode, unhealthy persistence is refused before
mutation. A successful mutation must rebuild every live source and leave no stale segment. If the
coherent live rebuild completes but its durable commit fails, it is published for read consistency
but returns `503 persistence_unavailable` and is never acknowledged. Coordinator mode uses the same
checkpointing vocabulary-rebuild path and typed shard failures.

This contract is recorded in
[ADR-152](../../../decisions/adr-152-alias-import-api-contract.md).
