# `POST /_vocab/aliases/learn_and_apply` — Learn from stored queries and apply

> [Vocabulary & alias APIs](../vocab.md) · [REST API hub](../../api.md)

Learn governed alias relationships from distinct-query any-of co-occurrence in the engine's own
stored queries and synchronously rebuild those queries under the resulting registry:

```bash
curl -X POST 'http://127.0.0.1:8080/_vocab/aliases/learn_and_apply?min_count=2'
```

```json
{
  "took": 3,
  "took_ms": 3.42,
  "acknowledged": true,
  "activated": 1,
  "recompiled": 12,
  "summary": {
    "active": 1,
    "candidate": 1,
    "rejected": 0
  }
}
```

`min_count` defaults to 2 and must be positive. It counts distinct stored queries that support a
relationship, not repeated occurrences within one query. Only clear single-token variants
auto-activate; distinct-token and multi-word groups land as candidates. Inspect them with
`GET /_vocab/aliases`, then make an operator declaration through an import or an edited `PUT /_vocab`
document if appropriate.

The route is strictly bodyless. It rejects unknown, duplicate, and malformed query parameters,
caps request-body collection at 64 KiB and 250 milliseconds, returns `Allow: POST` for other
methods, and marks every route response `Cache-Control: no-store`. Admission, engine/coordinator lock
waits, corpus learning, rebuild, durable commit, and standalone publication use the shared one-slot
administrative blocking worker. The same timed response is returned in standalone and coordinator
modes.

In standalone durable mode, unhealthy persistence is refused before mutation. A successful response
means every live source was recompiled, no stale segment remains, and the rebuilt state committed.
If a coherent live rebuild finishes but the durable commit fails, the live snapshot is published for
read consistency but the request returns `503 persistence_unavailable` and is not acknowledged.
Coordinator mode uses the same checkpointing vocabulary-rebuild path and propagates typed shard or
durability failures.

This is a native stored-corpus learning operation. Elasticsearch manages explicit rules in named
synonym sets, while OpenSearch configures explicit synonym filters and analyzer refresh; neither
contract represents learned, governed relationships from stored reverse queries. The contract is
recorded in [ADR-153](../../../decisions/adr-153-alias-learn-apply-api-contract.md).
