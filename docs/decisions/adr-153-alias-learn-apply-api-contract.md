# ADR-153: Alias learn-and-apply REST API contract

> [Engine, errors, dependencies & ops decisions](areas/engine-quality-and-operations.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

`POST /_vocab/aliases/learn_and_apply` learns governed alias relationships from co-occurrence in the
engine's stored queries and immediately rebuilds those queries under the resulting registry. The
original route accepted loose query parameters, including `min_count=0`, inherited the server-wide
100 MiB body limit, had no body deadline or method fallback, and performed engine/coordinator lock
waits plus O(corpus) learning and rebuild work on Tokio request workers. It always published a
standalone snapshot even after failure, did not verify a complete or durable rebuild, and returned
different `recompiled` and `rebuilt` fields in standalone and coordinator modes.

[Elasticsearch synonym APIs](https://www.elastic.co/guide/en/elasticsearch/reference/current/put-synonym-rule.html)
manage explicit rules in named synonym sets and reload affected analyzers. OpenSearch likewise
configures explicit rules through a
[synonym token filter](https://docs.opensearch.org/latest/analyzers/token-filters/synonym/) and can
[refresh search analyzers](https://docs.opensearch.org/latest/im-plugin/refresh-analyzer/index/).
Neither contract learns governed relationships from stored reverse queries. Reverse Rusty's
operation therefore has no honest Elasticsearch or OpenSearch route alias.

## Decision

- Keep the native `POST /_vocab/aliases/learn_and_apply` path. Do not expose `/_synonyms` or an
  analyzer-reload alias for an operation whose input, governance, and rebuild semantics differ.
- Accept only an optional positive `min_count` query parameter, defaulting to 2. It measures
  distinct stored-query evidence. Reject zero, unknown, duplicate, and malformed query parameters.
- Keep the operation bodyless. Bound request transport at 64 KiB and 250 milliseconds, reject a
  non-empty body, and return the standard error envelope plus `Allow: POST` for unsupported methods.
  Every route-reached response is `Cache-Control: no-store`.
- Return the same timed response in standalone and coordinator modes:
  `took`, `took_ms`, `acknowledged`, `activated`, `recompiled`, and `summary`.
- Wait asynchronously for the shared one-slot administrative permit, then move the permit,
  engine/coordinator lock waits, learning, rebuild, durable commit, and standalone publication onto
  a blocking worker. The worker owns admission until it finishes, even if the client disconnects.
- Before a standalone durable mutation, refuse unhealthy persistence. Require the rebuild count to
  equal the number of live sources and require no stale segment to remain. A coherent live rebuild
  whose durable commit fails is still published for read consistency, but returns
  `503 persistence_unavailable` and is never acknowledged. Coordinator mode continues through the
  checkpointing vocabulary-rebuild path and propagates typed shard or durability failures.
- Preserve the existing conservative governance policy: only clear single-token variants
  auto-activate; distinct-token and multi-word relationships remain candidates for operator review.
- Count and time every route outcome under fixed `vocab_aliases_learn_apply` labels, beginning
  before transport validation.

## Consequences

The stored-corpus learner is strict, bounded, observable, and consistent across both local
deployment modes. Operators can distinguish a live durable success from a failed commit, while
unsupported Elasticsearch/OpenSearch expectations fail loud instead of implying named synonym
resources or analyzer reload behavior.

The embedded engine method remains available independently of HTTP transport. This decision tightens
the REST contract without changing the learned relationship policy or turning review candidates
into active aliases.

## Safety and proof

Learning continues to use distinct stored queries rather than repeated occurrences within one query.
Active learned groups still widen only positive requirements through the shared query/title feature
model; forbidden features remain invisible to candidate retrieval. Distinct-token and multi-word
groups remain review candidates, so learning does not bypass alias governance. The lossless
signature-cover contract is unchanged.

Standalone route tests cover synchronous learned matching, response shape, snapshot publication,
strict method/query/body handling, body limits and deadlines, telemetry, asynchronous admission,
off-runtime engine locking, closed admission, and a durable commit failure that remains live but
unacknowledged. Coordinator tests cover matching and response parity, strict transport, telemetry,
admission, and off-runtime cluster-lock contention.
