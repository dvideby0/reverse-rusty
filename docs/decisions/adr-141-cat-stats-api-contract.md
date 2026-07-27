# ADR-141: CAT stats API contract — native rows with familiar controls

> [Engine, errors, dependencies & ops decisions](areas/engine-quality-and-operations.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

- **Context.** `GET /_cat/stats` emitted a useful standalone text summary, but it accepted and
  silently ignored every query parameter and request body, inherited the 100 MiB ingest limit,
  exposed no status/latency telemetry, and allowed its dynamic output to be cached. It also ran
  the same persisted class-column scan and posting-length collection/sort as `GET /_stats`
  directly on a Tokio request worker without the stats-admission permit introduced by ADR-140.
  Its `queries` line did not distinguish physical, live, and tombstoned rows, and its memory total
  omitted the dictionary, retained sources, logical-id indexes, and liveness overlays.

- **Compatibility boundary.** Elasticsearch and OpenSearch have no `/_cat/stats` operation. Their
  closest summary surfaces are CAT count/indices, and their CAT families share text-by-default,
  `format`, `v`, `h`, `help`, and `s` controls
  ([Elasticsearch CAT count](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-count),
  [OpenSearch CAT APIs](https://docs.opensearch.org/latest/api-reference/cat/index/)). Reverse
  Rusty cannot call physical LSM rows Elasticsearch/OpenSearch documents or indices (ADR-140), so
  the native path remains. It adopts only the common table mechanics that have exact meanings.

- **Decision — table and controls.** The response is a two-column `metric` / `value` table with
  one row per native statistic. Text remains the default; `format=json` returns an array whose
  selected fields are strings, matching CAT's presentation-oriented contract. `v` adds text
  headings, `h` selects/reorders `metric` and `value` by full name, alias (`m` / `v`), or simple
  wildcard, `help` describes the columns without collecting corpus statistics, and `s` performs
  lexical multi-column `asc` / `desc` sorting. Bare `v` and `help` flags are accepted. Unknown
  parameters, columns, sort fields/directions, formats, duplicate fields, and ambiguous help
  combinations fail as structured 400s rather than being ignored.

- **Decision — truthful rows.** `queries.physical`, `queries.live`, and
  `queries.tombstoned` replace the ambiguous `queries` label. Classes remain physical-row
  tallies. Posting, dedup, broad-lane, and per-base-segment telemetry remain available as named
  rows. Memory exposes all seven resident components and their saturating total; WAL backlog is
  projected as `translog.operations` / `translog.size_in_bytes`. `took_ms` includes validation,
  asynchronous admission, and collection through the point where rows are ready to render.

- **Decision — strict bounded transport.** The route accepts only bodyless `GET`, uses the same
  64 KiB ceiling as `/_stats`, returns structured 400/405/413 errors, sets `Allow: GET` on method
  errors, and marks every success/error `Cache-Control: no-store`. Every terminal handler outcome
  increments `http_requests_total{endpoint="cat_stats",status}` and the whole request is measured
  by the `cat_stats` duration histogram.

- **Decision — shared collection.** CAT stats loads one immutable engine snapshot, acquires the
  same single stats permit as `/_stats`, and owns that permit inside `spawn_blocking`. Concurrent
  calls across either endpoint therefore cannot multiply the corpus-wide posting allocation or
  class scan, and a disconnected request cannot release admission while its worker continues.
  Help is schema-only and deliberately bypasses admission. Cluster mode remains a documented 501:
  `GET /_stats` and `GET /_cat/shards` are the truthful coordinator views.

- **Safety and proof.** The endpoint is read-only and cannot affect candidate retrieval or exact
  verification. Route tests pin physical/live/tombstone arithmetic, complete memory/WAL rows,
  common CAT controls (including bare flags, wildcard column selection, JSON, help, and sorting),
  strict method/query/body/size errors, no-store/status accounting, help's admission bypass, and
  shared cancellable stats admission.
