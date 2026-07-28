# ADR-146: Vocabulary read REST API contract

> [Engine, errors, dependencies & ops decisions](areas/engine-quality-and-operations.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

`GET /_vocab` returned the installed vocabulary, but the transport remained a prototype. It
silently accepted every query string and request body, inherited the global 100 MiB ingest limit,
had no body-read deadline, cache policy, route telemetry, or documented HEAD behavior, and relied on
the router's empty method rejection. Serialization ran on a Tokio request worker. Coordinator mode
also cloned the vocabulary while acquiring a blocking `parking_lot` cluster read guard on that
worker. A large vocabulary or an in-progress blue/green vocabulary write could therefore consume
async executor capacity, and many concurrent reads could duplicate large clone/serialization
allocations.

[Elasticsearch's get-synonym-set API](https://www.elastic.co/guide/en/elasticsearch/reference/current/get-synonyms-set.html)
returns one named set of Solr-format synonym rules and paginates those rules. Reverse Rusty's
vocabulary is one complete, replaceable normalizer document: synonyms, entity phrases,
equivalences, punctuation classes, number context, and a governed alias registry. OpenSearch
documents synonyms as
[an analyzer token-filter configuration](https://docs.opensearch.org/latest/analyzers/token-filters/synonym/)
rather than an equivalent cluster-wide vocabulary document. Adding `/_synonyms` or adopting its
set/rule response would therefore misrepresent both scope and apply semantics.

## Decision

- Keep the native `/_vocab` path and complete `Vocab` JSON shape. Do not add `/_synonyms` or
  fabricate named synonym sets. The response remains directly deserializable and installable
  through `PUT /_vocab`; pagination is rejected because a partial document would not be
  round-trippable.
- Accept only query-free, bodyless GET and HEAD for the read operation. Give those methods a
  GET-specific 64 KiB extraction ceiling and a separate 250 ms body-read deadline. The limit is
  layered before adding PUT to the method router, so it cannot reduce the existing vocabulary-write
  allowance.
- Return structured 400/405/408/413 errors. Unsupported path methods report
  `Allow: GET, HEAD, PUT`, reflecting both the audited read and the existing replacement operation.
  Remove every HEAD response body, attach `Cache-Control: no-store` to every route-reached outcome,
  and record request counts plus whole-route duration under the fixed `vocab_get` label.
- Share the existing single administrative read permit with stats, health dependency collection,
  and coordinator metrics. Wait for admission asynchronously and move the owned permit into one
  blocking worker so cancellation cannot release it while clone or serialization work continues.
  A closed admission gate returns a sanitized `503 vocab_unavailable`.
- In standalone mode, capture one immutable `ArcSwap` snapshot and serialize its vocabulary on the
  blocking worker. An engine built directly from the stock normalizer returns the empty/default
  `Vocab` document, which describes that same absence of configured rules.
- In coordinator mode, acquire the cluster read guard only inside the blocking worker, clone one
  installed vocabulary while guarded, release the guard, and then serialize the clone. This keeps a
  consistent document without holding the writer-excluding guard through JSON encoding.
- Return `application/json` on success. A serialization or worker failure is logged with its detail
  and returned as a sanitized `500 vocab_unavailable`; no partial JSON is emitted.

## Consequences

Operators get one cache-safe, round-trippable vocabulary snapshot and a bodyless HEAD contract.
Malformed, oversized, or stalled input is bounded and visible. Large snapshots cannot fan out
unbounded blocking work, and coordinator lock acquisition no longer blocks Tokio request workers.
Vocabulary replacement keeps its prior request-size allowance and semantics.

The endpoint remains deliberately native. Clients expecting Elasticsearch named synonym sets,
per-rule identifiers, or pagination must translate explicitly. A read may wait behind another
shared administrative collection, and an already admitted read retains its permit until its
blocking work completes even if the client disconnects.

## Safety and proof

The operation reads one immutable or read-guarded vocabulary and never changes normalization,
candidate retrieval, or exact verification, so the lossless signature-cover contract is unchanged.
Standalone and coordinator route tests pin complete JSON round-tripping, content type, no-store
behavior, GET/HEAD parity, strict query/body/method handling, GET-only body limits, the body-read
deadline, whole-route telemetry, asynchronous shared admission, sanitized closed-admission
failure, and continued large-body availability for PUT.
