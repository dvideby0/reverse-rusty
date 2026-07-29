# REST API reference

The Reverse Rusty server (`engine/src/bin/server/`) exposes an Elasticsearch-style REST API over
HTTP. Use this hub to choose an area, scan that area's method/path catalog, and open the focused
contract for the API you need.

For stored-query syntax, see the [query DSL](dsl.md). Ranking-profile scoring, configuration,
features, and topology behavior are canonical in the [ranking reference](ranking.md). Engine
internals live in the [matching](../design/matching.md) and
[ingestion](../design/ingestion-and-updates.md) design references.

## API areas

| Area catalog | Scope |
|---|---|
| [Server & shared behavior](api/server.md) | Start-up flags, security, the API root, and coordinator-mode behavior shared across endpoints. |
| [Documents](api/documents.md) | Register, replace, retrieve, existence-check, and delete stored queries. |
| [Percolation & delivery](api/percolate.md) | Compatibility search, exact bounded search, batches, PIT pagination, and exhaustive delivery. |
| [Ingest & lifecycle](api/ingest.md) | Bulk ingest, flush, checkpoint, compaction, force merge, and backup. |
| [Observability](api/observability.md) | JSON statistics, CAT tables, authoritative cluster state, readiness, and Prometheus metrics. |
| [Vocabulary & aliases](api/vocab.md) | Vocabulary reads and replacement, learning, governed aliases, discovery, and feedback validation. |
| [Settings](api/settings.md) | Read and update live engine settings. |
| [Cluster control](api/cluster.md) | Membership and topology-changing cluster operations. |

## Finding and maintaining APIs

- **Know the task?** Open the matching category catalog above.
- **Know the path?** Search this directory for the route, then open its focused contract page.
- **Need deployment or recovery steps?** Use the [operations guides](../operations/deployment-modes.md);
  API pages own request and response behavior, while operations pages own procedures.
- **Need rationale?** Use the [architecture decision hub](../DECISIONS.md).
- **Adding an API?** Add one focused contract page and one row to its category catalog. Add a new
  category here only when none of the existing areas is coherent.

Every current API behavior has one canonical reference. Category catalogs summarize and route;
focused pages own parameters, examples, responses, strictness, errors, and topology differences.

---

Documentation placement and single-source-of-truth rules live in the
[documentation hub](../README.md).
