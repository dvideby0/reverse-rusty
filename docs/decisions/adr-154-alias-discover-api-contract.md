# ADR-154: Alias discovery REST API contract

> [Engine, errors, dependencies & ops decisions](areas/engine-quality-and-operations.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

`POST /_vocab/aliases/discover` computes deterministic distributional alias proposals from either
the standalone engine's stored queries or an explicit caller corpus. It returns similarity and
co-occurrence evidence without recording or activating anything. The original route inherited the
server-wide 100 MiB body limit, accepted ignored query parameters and unknown JSON fields, silently
skipped malformed DSL, accepted unbounded or nonsensical discovery controls, and performed parsing,
engine locking, O(corpus) discovery, and serialization on Tokio request workers. Standalone and
coordinator handlers also bypassed shared administrative admission, route-wide timing/counters, and
no-store response handling.

[Elasticsearch synonym APIs](https://www.elastic.co/guide/en/elasticsearch/reference/current/put-synonym-rule.html)
manage explicit rules in named synonym sets, while OpenSearch configures explicit rules through a
[synonym token filter](https://docs.opensearch.org/latest/analyzers/token-filters/synonym/) and can
[refresh search analyzers](https://docs.opensearch.org/latest/im-plugin/refresh-analyzer/index/).
None of those contracts mines a reverse-query corpus for review-only distributional proposals.
Using their paths or response shapes would therefore imply resource and activation semantics this
operation does not have.

## Decision

- Keep the native `POST /_vocab/aliases/discover` path and its compute-only, review-first meaning.
  Do not expose an Elasticsearch/OpenSearch synonym-management alias.
- Accept no query parameters. The request body is optional in standalone mode; an omitted or empty
  body selects a snapshot of live stored query sources. A non-empty body must be
  `application/json` or `application/*+json`, reject unknown fields, and may supply an explicit
  `queries` corpus plus discovery knob overrides. Coordinator mode requires explicit `queries`
  because it has no cross-shard source-gather operation.
- Reject malformed tuples, repeated query IDs, and invalid DSL instead of silently treating them as
  absent evidence. Bound explicit corpora at 100,000 queries, one million positive context-token
  observations, 16 MiB of JSON, and a five-second body read.
- Validate discovery controls before computation: `min_token_freq` is positive;
  `min_similarity` and `max_cooccurrence_rate` are finite within `[0,1]`; `max_pairs` is at most
  100,000; and `max_vocab` is within `1..=4096`. The 4,096 ceiling preserves ADR-102's intended
  `N²/2` pair-key bound.
- Return additive whole-route `took` and `took_ms` fields with the existing deterministic `count`
  and best-first `proposals`. Bound the serialized response at 16 MiB and ask the caller to lower
  `max_pairs` or raise thresholds if it would exceed that limit.
- Wait asynchronously for the shared one-slot administrative permit. Move parsing, validation,
  stored-source capture, discovery, and serialization onto a blocking worker that owns the permit
  until completion. Standalone briefly holds the engine guard only while cloning live sources, then
  releases it before distributional work. Explicit-corpus coordinator discovery takes no cluster
  guard.
- Return the standard JSON error envelope, `Allow: POST` for unsupported methods, and
  `Cache-Control: no-store` for every route-reached outcome. Count and time all outcomes under the
  fixed `vocab_aliases_discover` telemetry label, beginning before transport validation.

## Consequences

Alias discovery is now a strict, bounded, observable dry run with the same explicit-corpus behavior
in standalone and coordinator modes. A client can distinguish transport failure from a legitimate
empty proposal set, and a malformed query can no longer disappear from the evidence base.

Stored-corpus discovery remains available only on a single engine until a truthful cross-shard
source-gather operation exists. The endpoint does not record candidates; that separate mutation
remains `POST /_vocab/aliases/discover_and_record`.

## Safety and proof

The ADR-102 signal, ordering, co-occurrence defense, numeric default, phrase glue, and never-active
governance policy are unchanged. This route still modifies no engine, vocabulary, registry,
dictionary, segment, or snapshot state. Rejecting malformed evidence and bounding caller-controlled
work do not change the lossless signature-cover contract.

Standalone route tests cover explicit and stored corpora, the planted substitute, timed no-store
output, compute-only registry state, fixed telemetry, strict method/query/media/JSON behavior, body
size and deadline, duplicate IDs, invalid DSL, control bounds, corpus cardinality, asynchronous
admission, and closed admission. Coordinator tests cover the same planted explicit-corpus result,
timing/header/telemetry parity, the fail-loud missing-corpus boundary, shared validation, and method
fallback.
