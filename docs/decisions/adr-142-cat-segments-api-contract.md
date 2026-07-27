# ADR-142: CAT segments API contract

> [Engine, errors, dependencies & ops decisions](areas/engine-quality-and-operations.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

ADR-023 added truthful lock-free `SegmentInfo` rows, but the REST projection had remained a
prototype: it accepted bodies under the global 100 MiB ceiling, silently treated unknown formats
and query controls as the default table, always printed headers, exposed different names and value
types in text and JSON, and had only pure rendering tests. It therefore looked like
`GET /_cat/segments` without providing the common CAT contract clients expect.

[Elasticsearch CAT segments](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-segments)
and [OpenSearch CAT segments](https://docs.opensearch.org/latest/api-reference/cat/cat-segments/)
both define header, column, help, sorting, format, and byte-unit controls. Their rows describe
Lucene segments belonging to index shards. Reverse Rusty instead has one native LSM corpus with
immutable memory/mmap base segments plus a mutable memtable, so copying index, shard, node, Lucene,
commit, or disk-size fields would be false compatibility.

## Decision

- `GET /_cat/segments` is a strict bodyless GET. A route-local 64 KiB ceiling bounds rejected
  bodies; other methods return structured 405 with `Allow: GET`; invalid extraction, unknown
  fields, and unsupported values return structured 400/413 errors. Every response, including
  errors and the coordinator's 501, carries `Cache-Control: no-store`.
- The route uses the CAT conventions that map exactly: headerless text by default, `v`,
  `format=json`, `h` selection/reordering with aliases and simple wildcards, schema-only `help`,
  stable multi-column `s`, and the binary `bytes` units supported by Elasticsearch/OpenSearch.
  JSON contains canonical selected column names in requested order and presentation-string values.
- CAT stats and CAT segments share one parser/renderer so flag parsing, wildcard selection, ordered
  JSON, help, alignment, and sorting cannot drift. Cells retain typed sort values: counts and byte
  totals sort numerically, hole ratios sort as decimals, and booleans sort as booleans regardless
  of their rendered strings.
- Native rows remain base segments oldest-first followed by the memtable. Exact familiar names are
  used where honest: `docs.count` is the live-row count, `docs.deleted` is the tombstoned-row count,
  and `size.memory` is the saturating sum of attributed resident payload and overhead. Native
  columns retain physical entries, kind, hole percentage, vocabulary epoch, staleness, payload
  memory, and overhead memory. Legacy prototype names remain column aliases.
- `memory.payload` is zero for mmap-backed payloads because those pages are file-backed, while
  `memory.overhead` remains resident. The API does not invent an ES/OS on-disk `size`, Lucene
  generation/version, commit/searchability flag, index selector, shard identity, or node identity.
- Collection stays O(number of segments) from one lock-free snapshot and remains on the request
  worker; it does not perform the corpus-wide scans that require `/_stats` admission. Coordinator
  mode validates the same transport and then returns the existing fail-loud 501 with
  `/_cat/shards` as the supported alternative.

## Consequences

Operators get one predictable CAT dialect across native stats and standalone segment inspection,
including machine-selectable columns and byte units, without confusing Reverse Rusty's storage
model with Lucene's. The default table intentionally becomes headerless and CAT JSON values become
strings; callers that relied on the prototype's always-present header or typed JSON must use `v` or
adapt to the documented CAT contract. Stable typed automation should prefer `GET /_stats`.

Coverage now drives the real Axum route: physical/live/deleted arithmetic, memtable-last order,
numeric sorting, aliases/wildcards, requested JSON key order, byte totals, help, no-store headers,
body/method/size failures, and the coordinator 501 boundary. Existing engine tests continue to
prove dense ordinals, per-row arithmetic, deletion holes, and engine/snapshot agreement.
