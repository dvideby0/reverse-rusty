# ADR-147: Vocabulary replacement REST API contract

> [Engine, errors, dependencies & ops decisions](areas/engine-quality-and-operations.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

`PUT /_vocab` correctly installed a new normalizer and recompiled stored queries before publishing
the next standalone snapshot, while coordinator mode used the existing atomic blue/green rebuild.
The HTTP boundary was still a prototype. It silently accepted all query parameters, inherited the
global 100 MiB ingest ceiling, had no body-read deadline, and exposed Axum extractor rejections
instead of the standard error envelope. It lacked cache policy, timing, and route telemetry.
Standalone returned `recompiled`, coordinator returned undocumented `rebuilt`, and both acquired
blocking `parking_lot` guards and performed O(corpus) work on Tokio request workers.

The standalone engine deliberately keeps a coherent in-memory rebuilt segment if its source,
segment, or manifest commit fails, while leaving the old durable commit authoritative and marking
persistence unhealthy. The old handler nevertheless returned `acknowledged: true`, so HTTP success
could claim durability that had not occurred.

[Elasticsearch's create-or-update synonym-set API](https://www.elastic.co/guide/en/elasticsearch/reference/current/put-synonyms-set.html)
replaces one named set of Solr-format rules and may reload analyzers. OpenSearch configures synonyms
through
[analyzer token filters](https://docs.opensearch.org/latest/analyzers/token-filters/synonym/) and
can refresh updateable search analyzers. Reverse Rusty's one complete `Vocab` also owns entity
phrases, equivalence expansion, punctuation classes, number context, and governed aliases, and a
change must recompile reverse-query exact plans. A `/_synonyms/{id}` alias or named-set request
shape would therefore misrepresent both scope and apply semantics.

## Decision

- Keep the native `PUT /_vocab` path and complete `Vocab` document. Do not add a synonym-set alias
  or accept analyzer-refresh controls. Success remains synchronous because publishing a new title
  normalizer before every stored query is rebuilt could create false negatives.
- Accept only query-free PUT with `application/json` or an `application/*+json` vendor media type.
  Reject missing/unsupported media types, malformed JSON, unknown vocabulary fields, and empty
  input through the standard JSON error envelope.
- Give PUT its own 16 MiB body ceiling and five-second body-read deadline, independent of the
  GET/HEAD request limit and global bulk-ingest ceiling. Return structured 400/408/413/415 errors.
- Attach `Cache-Control: no-store` to every route-reached outcome. Count and time every outcome
  under the fixed `vocab_put` endpoint label, beginning before transport validation.
- Share the single administrative-work permit with vocabulary reads and stats. Wait for admission
  asynchronously, move the owned permit into a blocking worker, and acquire engine/cluster locks
  only there. The worker owns standalone snapshot publication, so a disconnected request cannot
  leave a completed coherent replacement unpublished.
- Return the same standalone/coordinator success shape:
  `{took, took_ms, acknowledged: true, recompiled}`. `took` is whole-route integer milliseconds;
  `took_ms` retains fractional precision. The coordinator no longer substitutes `rebuilt` for the
  documented field on this endpoint.
- In standalone mode, capture the expected distinct live logical/source count before replacement;
  physical row counts are intentionally unsuitable because recompilation canonicalizes supported
  additive histories. After recompiling, require exactly that count and no stale segments before
  success. Refuse a durable update whose persistence is already unhealthy with 503.
- If recompilation produced a complete coherent live state but its durable commit degraded
  persistence, publish that live state to preserve query/title feature-space consistency but
  return `503 persistence_unavailable`, explicitly saying that it is live and not durably
  acknowledged. The prior manifest remains the restart authority. An impossible count/staleness
  mismatch returns a sanitized 500 and is never published as the new snapshot.
- A coordinator rebuild error is described as "not acknowledged," not "refused": its blue/green
  swap can precede a control-plane or durable-checkpoint failure. The typed cluster error still
  chooses the status, and clients can inspect `GET /_vocab` before recovery.
- Keep the established coordinator refusal for remote or handoff-wrapped shards: the current wire
  protocol does not ship the replacement normalizer, so accepting it would risk false negatives.

## Consequences

Clients get deterministic JSON validation, familiar synchronous timing, one response shape in both
local modes, and an explicit distinction between coherent live application and durable
acknowledgement. Large or slow request bodies are bounded, and corpus rebuilds no longer block
Tokio workers or form an unbounded blocking-pool queue.

The operation remains deliberately native. Clients built for Elasticsearch named synonym sets or
OpenSearch analyzer configuration must translate explicitly. Vocabulary replacement serializes
with stats and vocabulary reads, and there is no cancellable execution timeout after admission:
dropping the HTTP request does not safely roll back a partially completed durable operation.
Single-node operators must still update `--vocab-file` after a successful REST replacement.

## Safety and proof

The rebuild keeps query compilation and title normalization on one vocabulary and verifies that
every live query was recompiled before acknowledging success. A durable commit failure preserves
the coherent in-memory exact plans and title normalizer together while leaving the older manifest
authoritative, so neither the live process nor restart mixes feature spaces. The lossless
signature-cover contract is unchanged.

Standalone route tests pin strict query/media/JSON handling, request bounds and body deadline,
vendor JSON, no-store responses, timing and telemetry, asynchronous admission, off-runtime engine
locking, closed admission, exact recompile count, snapshot publication, alias matching, and an
injected source-sidecar failure that returns 503 without leaving stale live plans. Coordinator
tests pin the shared transport/admission behavior and the mode-consistent `recompiled` response.
