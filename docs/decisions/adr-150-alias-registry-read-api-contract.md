# ADR-150: Alias-registry read REST API contract

> [Engine, errors, dependencies & ops decisions](areas/engine-quality-and-operations.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

`GET /_vocab/aliases` returned every governed alias entry and a lifecycle summary, but its HTTP and
execution boundaries remained prototype quality. Unknown, duplicate, and malformed query
parameters were ignored. Arbitrary bodies were accepted under the server-wide 100 MiB limit, with
no body deadline. Unsupported methods returned the router's generic response. The route had no
cache policy or request metrics, and its potentially large JSON serialization ran on a Tokio
request worker. Coordinator mode also acquired its blocking cluster read lock there.

[Elasticsearch's get-synonym-set API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-synonyms-get-synonym)
pages one named set with `from` and `size` and reports a total `count`. Elasticsearch
[index aliases](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-alias)
instead describe index/data-stream routing, filters, and write-index selection. OpenSearch likewise
uses `alias` for
[index aliases](https://docs.opensearch.org/latest/api-reference/cat/cat-aliases/) and configures
explicit synonym rules through an
[analyzer token filter](https://docs.opensearch.org/latest/analyzers/token-filters/synonym/).
Reverse Rusty's one alias registry carries provenance, structural kind, confidence, review
evidence, and lifecycle status; only active expressible entries affect reverse-query matching. It
is neither an index alias nor a named analyzer synonym set.

## Decision

- Keep the native `/_vocab/aliases` path and the existing nested `aliases.entries` plus whole-registry
  `summary` response. Do not add `/_alias`, `/_cat/aliases`, or `/_synonyms/{id}` aliases that would
  claim unrelated routing or named-set semantics.
- Accept strict GET and HEAD. Permit only optional non-negative integer `from` and `size` query
  parameters, matching the familiar Elasticsearch synonym-set paging controls. Reject unknown,
  duplicate, and malformed parameters. Omitting `size` preserves the historical complete-registry
  response; out-of-range `from` and `size=0` return an empty page.
- Add top-level `count`, the total registry cardinality before paging. Keep `summary` over the whole
  registry rather than the selected page. Entry order remains the registry's stable stored order.
- Require an empty request body. Bound extraction at 64 KiB and 250 ms even though any non-empty
  body is invalid. Return the standard JSON error envelope for 400/405/408/413/500/503 outcomes,
  `Allow: GET, HEAD` for unsupported methods, and `Cache-Control: no-store` on every route-reached
  response.
- Return the same representation headers for GET and HEAD, including the corresponding paged JSON
  `Content-Length`, while removing the HEAD body through normal HTTP routing semantics.
- Wait asynchronously for the server's one administrative-work permit, then move that permit and
  serialization onto a blocking worker. A disconnected request cannot release admission while the
  work continues. Closed admission is a sanitized `503 aliases_unavailable`; serialization and
  worker failure are sanitized 500 responses.
- In standalone mode, capture one immutable engine snapshot before dispatch. In coordinator mode,
  acquire the cluster read lock only inside the blocking worker, clone the registry under that
  brief guard, release it, and then page and serialize. No cluster lock wait or JSON work runs on a
  Tokio worker.
- Count and time every route outcome under fixed `vocab_aliases_get` labels, starting before
  transport validation.

## Consequences

Operators can retain the historical full review or page large registries with familiar controls
and a stable total. Counts remain meaningful on every page, HEAD is useful for metadata checks, and
all responses are cache-safe and observable. Large registries cannot fan out unbounded clone and
serialization work, and coordinator lock contention does not stall an async request worker.

Paging is intentionally offset-based over stored order, not a mutation-stable cursor. A concurrent
registry replacement can change later pages, so clients that require one coherent review should
retrieve the complete response or retrieve `GET /_vocab` once and inspect its embedded registry.
The route remains native because mapping its governance metadata onto index aliases or analyzer
synonym rules would be lossy and operationally misleading.

## Safety and proof

The operation reads one immutable or read-guarded registry and cannot change normalization,
candidate retrieval, exact verification, or lifecycle status. The lossless signature-cover
contract is unchanged.

Standalone route tests pin full and paged results, total count, whole-registry summary, content
headers, GET/HEAD parity, strict query/body/method handling, size and time bounds, no-store
telemetry, asynchronous admission, and closed-admission failure. Coordinator tests pin the same
page shape and telemetry over a real multi-shard cluster, asynchronous admission, and that
deliberate cluster-lock contention does not stall a single-worker Tokio runtime.
