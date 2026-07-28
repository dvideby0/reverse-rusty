# ADR-143: CAT shards API contract

> [Engine, errors, dependencies & ops decisions](areas/engine-quality-and-operations.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

The coordinator's `GET /_cat/shards` route had remained a prototype. It accepted bodies under the
global 100 MiB ceiling, ignored unknown controls and unsupported formats, always printed a header,
returned different value types in text and JSON, blocked a Tokio request worker while probing
possibly remote shards, and exposed no route metrics or cache policy. More seriously, it converted
any failed control-plane read into an empty assignment map and rendered `-` for every node. That
made an unavailable topology look like a successful unassigned cluster.

[Elasticsearch CAT shards](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-shards)
and [OpenSearch CAT shards](https://docs.opensearch.org/latest/api-reference/cat/cat-shards/)
define header, column, help, sorting, and format controls. Their rows describe every primary and
replica copy of a Lucene index shard, including live documents, storage, state, IP, and node.
Reverse Rusty's coordinator instead owns logical shard positions, a physical stored-query count per
position, and a separately committed primary-plus-replica node assignment. Inventing index,
per-copy readiness, live-document, disk-size, or IP fields would be false compatibility.

## Decision

- `GET /_cat/shards` is a strict bodyless GET with a route-local 64 KiB ceiling. Other methods
  return structured 405 with `Allow: GET`; invalid extraction, unknown controls, unsupported
  values, and oversized bodies return structured 400/413 errors. Every response carries
  `Cache-Control: no-store` and increments the `cat_shards` request metric.
- The endpoint uses the common CAT mechanics that map exactly: headerless text by default, `v`,
  `format=json`, `h` selection/reordering with aliases and simple wildcards, schema-only `help`,
  and stable multi-column `s`. JSON uses selected canonical keys in requested order and
  presentation-string values.
- Rows remain native and position-level: numeric `shard`, numeric `queries`, and textual `nodes`.
  `queries` counts physical stored rows, including tombstones and content-driven copies, so it is
  deliberately not named or aliased `docs`. `nodes` renders the committed primary first followed
  by `+`-separated replicas.
- Collection reads the committed control state and one count from every logical position in one
  admitted blocking job. It shares the single stats permit with `/_stats` and `/_cat/stats`; help
  does not acquire that permit. Shard failures, control-plane failures, ring/state count mismatch,
  missing assignments, duplicate assignments, and out-of-range assignments fail the whole request
  loudly rather than returning a partial or fabricated table.
- The route does not accept index selectors, `bytes`, `time`, `local`, or cluster-manager timeout
  controls. Reverse Rusty has no index namespace, shard storage/time columns, or equivalent
  local-versus-manager state read to which those controls could map honestly.

## Consequences

Operators get the same strict CAT dialect used by native stats and standalone segment inspection,
without confusing logical Reverse Rusty positions with Lucene shard copies. The default table
becomes headerless and JSON counts become strings; callers of the prototype must use `v` or adapt
to the documented CAT contract.

The assignment column is a committed desired-placement view, not a live per-replica readiness
attestation. A successful response guarantees that every serving position answered and had one
well-formed committed assignment, but the two distributed observations are not a transactional
cluster snapshot. Stable typed automation should use `GET /_stats` and `GET /_cluster/state`.
