# ADR-152: Alias-import REST API contract

> [Engine, errors, dependencies & ops decisions](areas/engine-quality-and-operations.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

`POST /_vocab/aliases/import` accepted only one loose JSON object containing raw Solr synonym text.
Malformed rules such as missing mapping sides, repeated arrows, empty forms, duplicate-only groups,
and trailing escapes could be silently dropped or partially interpreted. The endpoint inherited the
server-wide 100 MiB body limit, had no body deadline or method fallback, performed O(corpus) rebuilds
and engine/coordinator lock waits on Tokio request workers, published a standalone snapshot even
after failure, and did not verify durable acknowledgement or a complete rebuild. Standalone and
coordinator responses also disagreed on `recompiled` versus `rebuilt`, and an identical import
needlessly rebuilt every stored query.

[Elasticsearch's synonym-set update API](https://www.elastic.co/guide/en/elasticsearch/reference/current/put-synonyms-set.html)
uses named `/_synonyms/{id}` resources, `synonyms_set` rule objects with optional rule IDs, a 10,000
rule limit, synchronous analyzer reload by default, and a `refresh` control. OpenSearch configures
Solr or WordNet rules, plus expansion behavior, on its
[synonym token filter](https://docs.opensearch.org/latest/analyzers/token-filters/synonym/).
Reverse Rusty instead has one governed registry whose entries carry provenance, structural kind,
confidence, review evidence, and lifecycle state. Its matching-safe equivalences are always
bidirectional expansion groups. It cannot honestly claim named-set CRUD, WordNet parsing,
directional replacement, or deferred activation.

## Decision

- Keep the native `POST /_vocab/aliases/import` path and the single governed registry. Do not expose
  `/_synonyms/{id}`: accepting a set ID would imply isolation, retrieval, deletion, and reload
  semantics the engine does not have.
- Accept exactly one strict JSON input:
  - native `{"synonyms":"<Solr file text>"}`; or
  - familiar Elasticsearch `{"synonyms_set":{"id":"optional","synonyms":"one rule"}}` or an array
    of those rule objects.
  Optional rule IDs must be unique, non-empty after trimming, and at most 256 raw input bytes. They
  are request metadata only; the registry continues to key canonical form groups and does not
  fabricate persistent named rules.
- Accept OpenSearch's body controls only as `format: "solr"` and `expand: true`, with those values as
  the defaults. Reject WordNet and `expand: false`. Continue to union either side of `a => b` into
  one bidirectional group; this is a documented recall-safe expansion, not directional replacement.
- Accept Elasticsearch's `refresh=true` query spelling or omission. Reject `refresh=false` because
  every acknowledged import is synchronously live; do not pretend to queue a deferred reload.
  Reject every other, duplicate, or malformed query parameter and every unknown JSON field.
- Parse the whole Solr input before changing the registry. Ignore blank/comment lines, preserve
  backslash escapes, and reject malformed mappings, empty forms, duplicate-only groups, dangling
  escapes, and empty files with line-specific typed errors. Bound one import at 10,000 rules and
  one rule at 256 forms. An Elasticsearch rule object contains exactly one rule.
- Bound the HTTP body at 16 MiB and five seconds. Require `application/json` or an
  `application/*+json` media type. Unsupported methods return the standard error envelope and
  `Allow: POST`. Every route-reached response is `Cache-Control: no-store`.
- Return the same timed response in standalone and coordinator modes:
  `took`, `took_ms`, `acknowledged`, `result`, `rules`, `activated`, `recompiled`, and `summary`.
  `result` is `updated` when the registry was installed and `noop` when an identical import changed
  nothing. A no-op returns `activated: 0`, `recompiled: 0`, does not publish a new snapshot, and does
  not rebuild. It also avoids a checkpoint when the current coordinator vocabulary generation is
  already committed; an identical retry after a failed durable checkpoint must retry that commit
  before acknowledging the otherwise unchanged registry. If an embedded single engine stopped
  between `set_vocab` and `recompile_stale_segments`, the identical import completes that pending
  rebuild instead of reporting a no-op. A coordinator retry likewise attests or repairs its
  feature-model control transition before acknowledgement. It may checkpoint over only the exact
  pre-import manifest retained by that attempt; a fully published next-epoch manifest is re-synced
  and adopted if the earlier directory sync reported failure and its complete recovery identity
  matches the attempted commit, including the segment registry, next segment IDs, source sidecars,
  and log replay cursor. An unreadable or incompatible manifest, or one with any other
  divergent/newer commit identity, fails loud and is never overwritten.
- Wait asynchronously for the shared one-slot administrative permit, then move the permit, engine
  or coordinator lock wait, parsing apply, rebuild, and standalone publication onto a blocking
  worker. A disconnected request cannot release admission while mutation work continues.
- Before a standalone durable mutation, refuse unhealthy persistence. After an actual mutation,
  require every live source to have been recompiled and no stale segment to remain. A coherent live
  rebuild whose durable commit fails is published for read consistency but returns an explicit 503,
  never an acknowledgement. Coordinator mutation continues through its checkpointing `set_vocab`
  path and typed shard errors.
- Count and time every route outcome under fixed `vocab_aliases_import` labels, beginning before
  transport validation.

## Consequences

Elasticsearch clients can reuse the familiar rule-object envelope and synchronous refresh spelling,
while OpenSearch users can state the Solr/expansion controls they expect. Unsupported semantics fail
loud instead of being accepted and ignored. Operators receive one deterministic contract and
response in both local modes, and identical retries are cheap, observable no-ops.

The embedded `AliasRegistry::import_solr` and `Vocab::import_solr_aliases` APIs now return the typed
parse failure rather than silently discarding invalid rules. Callers must handle that result. The
additional strictness deliberately treats a malformed file as one atomic failed declaration.

## Safety and proof

Parsing completes before registry mutation. Applied groups still pass the existing structural
classification policy: expressible operator-declared single-token and multi-word groups activate,
while mixed or unexpressible groups remain candidates. Active groups still widen positive
requirements through the same shared query/title feature model; forbidden features remain invisible
to candidate retrieval. The lossless signature-cover contract is unchanged.

Parser tests cover comments, escapes, directional-input union, every malformed rule family, line
diagnostics, and rule/form bounds. Standalone route tests cover both JSON dialects, synchronous
matching, true no-op publication behavior, strict method/query/media/JSON/body handling, deadlines,
telemetry, off-runtime admission and locking, closed admission, and fail-loud durable commit failure.
Coordinator tests cover response and matching parity, no-op behavior, durable retry recommit,
control-transition repair, incompatible-manifest refusal, telemetry, method handling, asynchronous
admission, and off-runtime write-lock contention over a real multi-shard cluster. Embedded lifecycle
tests prove an identical import completes a pending split-apply rebuild.
