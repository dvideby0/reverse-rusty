# `POST /v2/_pit`, `DELETE /v2/_pit` — Point-in-time cursor pagination (ADR-113/129/130)

> [Percolation & delivery APIs](../percolate.md) · [REST API hub](../../api.md)

Deep pagination over `/v2/_search` without deep `from`: open a PIT, page with `search_after`
cursors over ONE frozen view, never mixing generations.

```text
POST /v2/_pit?keep_alive=1m             -> open response (below)
POST /v2/_search {..., "pit": {"id": "<pit_id>"}}          -> page 1 + "next_cursor"
POST /v2/_search {..., "cursor": "<next_cursor>"}          -> page N (resend the same request)
DELETE /v2/_pit {"id": "<pit_id>"}      -> ES close spelling
DELETE /v2/_pit {"pit_id": ["<id-1>", "<id-2>"]}           -> OS batch spelling
```

PIT creation accepts the ES/OpenSearch `keep_alive` time value or the native `keep_alive_s`
seconds control, in either the query string or JSON body. An alias pair or one effective control
in both locations is a 400. Time values are non-negative integers followed by `nanos`, `micros`,
`ms`, `s`, `m`, `h`, or `d`. A truly empty body is valid and uses the configured default;
otherwise the body must be JSON. Native `allow_partial_results`, Elasticsearch
`allow_partial_search_results`, and OpenSearch `allow_partial_pit_creation` are equivalent:
`false` is accepted and `true` is a named 400 because a successful PIT always pins every required
logical shard. Unknown body/query controls, malformed values, or unsupported index/routing controls
are 400s. Malformed JSON is a structured 400; a non-JSON body is 415. PIT request bodies over
64 KiB are rejected with 413 before JSON decoding.

A successful open returns one additive response compatible with both identity spellings:

```json
{
  "id": "<opaque token>",
  "pit_id": "<same opaque token>",
  "creation_time": 1785123456789,
  "_shards": {"total": 3, "successful": 3, "skipped": 0, "failed": 0}
}
```

Elasticsearch callers use `id`; OpenSearch and existing Reverse Rusty callers use `pit_id`.
`creation_time` is Unix epoch milliseconds. `_shards` counts logical positions, not physical
replicas: local mode reports one, while a successful in-process cluster open reports every pinned
position. Reverse Rusty retains the unscoped `/v2/_pit` path because it exposes one stored-query
corpus, not caller-selected indices.

PIT close requires exactly one JSON field: Elasticsearch `id`, or OpenSearch/native `pit_id`.
Either spelling accepts one token or a non-empty token array; the array length cannot exceed
`--max-open-pits`. All tokens are authenticated before any registry mutation, so a malformed or
foreign token fails the whole request without closing an earlier valid token. Missing, null,
conflicting, duplicate JSON fields, wrong-type, unknown, and query-string controls are structured
400s; a foreign-process token is 409 `stale_cursor`, a missing or wrong content type is 415, and
a body over the PIT-specific 64 KiB limit is 413 before a scalar or array is materialized.
Delete-all is deliberately unsupported: callers must name the PITs they own, so one client cannot
discard every other client's pinned view.

A successful close returns one additive ES/OpenSearch/native response:

```json
{
  "closed": true,
  "succeeded": true,
  "num_freed": 1,
  "pits": [
    {"successful": true, "pit_id": "<id-1>"}
  ]
}
```

`closed` is the original aggregate result and is true only when every requested PIT was live and
closed. `pits` preserves request order and reports that result per token. `succeeded` means every
existing search context named by the request was closed; a structurally valid, already-gone PIT
therefore returns HTTP 200 with `closed: false`, `succeeded: true`, `num_freed: 0`, and
`successful: false`. `num_freed` counts contexts actually released: one per live PIT locally, or
one per logical shard position per live PIT in coordinator mode; replicas never inflate it.

A PIT pins the engine snapshot (single-node) or every shard position's snapshot (in-process
cluster) for a renew-on-use keep-alive: default `--pit-default-keep-alive-secs` (60), ceiling
`--pit-max-keep-alive-secs` (600, over-ask is a 400), at most `--max-open-pits` (64) concurrently
open — a breach is **429 `pit_limit_exceeded`**, never an eviction. Every use (open, page,
cursor) renews the deadline; abandoned PITs expire; `DELETE` frees immediately (`closed: false`
when already gone — the goal state either way). Open PITs retain memory (the pinned memtable
copy) and, after compaction, disk (unlinked-but-mapped segments) until released; the `open_pits`
gauge tracks them.

Cursor rules: a FULL page (`hits.length == size`, `size > 0`) returns `next_cursor`; a short page
ends the stream (no cursor). The client resends the **same** `document`/`query_scope`/`rank`/
`filter` with each cursor — matcher semantics and any non-static title-feature tuple are
fingerprinted into the token, and a drifted resend is a 400
`cursor_mismatch`; `size`, `timeout_ms`, and `track_total_hits_up_to` may vary per page. Totals
are page-invariant (every page of one PIT reports the identical total). `pit` + `cursor` together
is a 400. Concatenating pages yields exactly the one-shot ranked result over the same PIT — no
duplicates, no gaps.

Fail-closed staleness — **409 `stale_cursor`** (the one deliberate read-surface 409; the pinned
generation is unrecoverable, so open a new PIT and restart rather than retrying): an expired or
closed PIT, a server restart (tokens are HMAC-signed with a per-process key), and — in cluster
mode — any placement change (`resize`, vocabulary rebuild) or a primary failover (PIT reads are
primary-only, never silently failed over). Structurally garbled tokens are 400s. A remote/gRPC
coordinator assembly refuses PIT entirely with **501 `pit_unsupported`** (wire PIT is a later
increment; page via an in-process cluster or single-node mode). Both endpoints ride the open
search auth allowlist.
