# `PUT /_vocab` — Replace vocabulary

> [Vocabulary & alias APIs](../vocab.md) · [REST API hub](../../api.md)

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
