# `POST /_vocab/aliases/discover` — Discover alias candidates

> [Vocabulary & alias APIs](../vocab.md) · [REST API hub](../../api.md)

Run deterministic, distributional discovery (ADR-102) and return review proposals without recording
or activating anything. With no body, standalone mode analyzes a snapshot of its live stored query
sources. An explicit corpus is accepted in either mode:

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

The knob values shown are the defaults. A successful response is timed and ordered best-first
deterministically:

```json
{
  "took": 12,
  "took_ms": 12.34,
  "count": 1,
  "proposals": [{
    "forms": ["ns", "north star"],
    "similarity": 0.91,
    "cooccurrence_rate": 0.0
  }]
}
```

The transport and work bounds are strict:

- POST is the only method, and query parameters are not accepted. Unsupported methods return 405
  with `Allow: POST`; every route-reached response has `Cache-Control: no-store`.
- The body may be empty in standalone mode. A non-empty body requires `application/json` or an
  `application/*+json` media type, rejects unknown fields and malformed tuples, is capped at
  16 MiB, and must complete within five seconds.
- Explicit query IDs must be unique and every DSL string must parse within the public 10,240-byte,
  256-clause, and 64-member any-of ceilings. The corpus may contain at most 100,000 queries and one
  million positive context-token observations. An empty explicit corpus is a valid dry run.
- `min_token_freq` must be at least 1. `min_similarity` and `max_cooccurrence_rate` must be finite
  within `[0,1]`. `max_pairs` may be zero but cannot exceed 100,000. `max_vocab` must be within
  `1..=4096`; this bounds the algorithm's pair-key space. `glue_phrases` and `include_numeric` are
  Boolean.
- The request waits asynchronously for the shared one-at-a-time administrative-work slot. JSON
  decoding, corpus validation, discovery, and serialization run on the blocking worker that owns
  the permit. Standalone holds the engine guard only long enough to clone stored sources, then
  releases it before analysis. There is no execution timeout after admission.
- The serialized response is capped at 16 MiB. Exceedance is a 400 asking the caller to lower
  `max_pairs` or raise thresholds. Closed admission is 503; a blocking-worker or serialization
  failure is 500. All failures use the standard JSON envelope and fixed
  `vocab_aliases_discover` telemetry.

Coordinator mode requires the explicit `queries` corpus because it has no cross-shard source-gather
operation; omitting it is a fail-loud 400 with the alternative. Explicit-corpus computation takes
no cluster guard and otherwise returns the same response.

This remains a native review-first API. Elasticsearch manages explicit rules in named
[synonym sets](https://www.elastic.co/guide/en/elasticsearch/reference/current/put-synonym-rule.html);
OpenSearch configures explicit rules through
[synonym token filters](https://docs.opensearch.org/latest/analyzers/token-filters/synonym/) and
[refreshes search analyzers](https://docs.opensearch.org/latest/im-plugin/refresh-analyzer/index/).
Neither API discovers review proposals from a reverse-query corpus, so an alias would misrepresent
the input, resource, and activation semantics. The strict contract is recorded in
[ADR-154](../../../decisions/adr-154-alias-discover-api-contract.md).
