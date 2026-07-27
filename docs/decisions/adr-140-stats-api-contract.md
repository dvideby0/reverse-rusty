# ADR-140: Stats REST API contract — truthful native metrics and bounded collection

> [Engine, errors, dependencies & ops decisions](areas/engine-quality-and-operations.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

- **Context.** `GET /_stats` returned useful engine-specific counters, but it silently ignored
  every query parameter and request body, inherited the 100 MiB ingest limit, returned no timing
  or shard result, allowed dynamic responses to be cached, and did not account for its own status
  or latency. More importantly, standalone collection scanned persisted class columns and
  collected plus sorted every posting length directly on a Tokio request worker. Concurrent
  scrapes could repeat that corpus-wide allocation without admission. Coordinator collection
  synchronously fanned out from the request worker and fetched every shard count twice—once for
  the aggregate and again for the per-position array. The reference also called physical rows
  “queries” without making tombstones and content-driven multi-position copies explicit, and
  omitted resident-memory and WAL metrics already present in `EngineMetrics`.

- **Compatibility boundary.** Elasticsearch and OpenSearch use `GET /_stats` for index
  statistics, with named-index and metric paths, a `level` selector, `primaries`/`total`
  aggregations, and Lucene-specific metric groups
  ([Elasticsearch index stats](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-stats),
  [OpenSearch Index Stats API](https://docs.opensearch.org/latest/api-reference/index-apis/stats/)).
  Reverse Rusty has one implicit percolator corpus whose physical rows may be copied to multiple
  semantic-signature positions, plus native cost classes, candidate lanes, LSM tombstones, and
  retained source structures. Mapping those rows to index/primary/replica document statistics
  would be false. The endpoint therefore remains explicitly native: no named-index, metric-path,
  or index-stats query controls are accepted. Familiar fields are additive only where their
  meaning is exact.

- **Decision — strict transport.** Both modes accept exactly a query-free, bodyless `GET`.
  Non-empty query strings and bodies are structured 400s, oversized bodies are structured 413s
  under a 64 KiB route limit, and other methods are structured 405s with `Allow: GET`. Dynamic
  success and error responses carry `Cache-Control: no-store`. Every terminal outcome increments
  the `stats` HTTP status counter and the full request—including admission wait—is observed by the
  endpoint duration histogram.

- **Decision — bounded execution.** Each server owns one stats-admission permit. A caller waiting
  for it consumes no blocking worker and disappears cleanly if cancelled. Once admitted, the
  permit moves into `spawn_blocking`, so disconnecting the request cannot release admission while
  the corpus scan still runs. Standalone class and posting collection uses one immutable
  `EngineSnapshot`; reads and writes continue against their normal snapshot/mutex model.
  Coordinator collection owns its cluster read guard inside the blocking worker, derives
  `total_queries` from the one `shard_queries` pass, and makes only the remaining class-count
  pass. Required-shard failure still fails the whole response.

- **Decision — truthful response.** Both modes add integer `took`, precise `took_ms`, and the
  exact `_shards {total, successful, failed}` projection. Standalone adds `mode`,
  `live_queries`, and `tombstoned_queries` while preserving physical `total_queries`; the three
  are tied by an equality checked in route tests. `class_counts` remains a physical-row tally so
  in-memory and mmap representations stay identical. `memory` now reports every resident
  component already owned by `EngineMetrics` plus their saturating total. `translog.operations`
  and `translog.size_in_bytes` are exact familiar projections of the native WAL pending count and
  file size.

- **Decision — cluster count semantics.** `shard_queries`, their `total_queries` sum, and
  `class_counts` are the primary physical-row view. They include tombstones and count a query once
  for every content-derived logical position that stores it, but do not multiply replica copies.
  They are capacity/placement signals, not a distinct live document count. Reverse Rusty does not
  expose a misleading ES `docs.count` until every supported coordinator assembly can attest that
  logical quantity.

- **Safety and proof.** Stats collection is read-only and never participates in candidate
  retrieval or exact verification, so match-set correctness is unchanged. Route tests pin strict
  query/body/method/size handling, no-store caching, timing and shard fields, live/tombstone
  arithmetic, complete memory/WAL projections, endpoint accounting, asynchronous cancellable
  admission, and standalone/coordinator parity. Existing cluster tests continue to pin fail-loud
  shard errors.
