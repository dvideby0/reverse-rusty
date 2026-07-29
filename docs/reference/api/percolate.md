# Percolation & delivery APIs

> [REST API hub](../api.md) · [Query DSL](../dsl.md) · [Ranking profiles](../ranking.md)

Match incoming titles against stored queries through compatibility, exact bounded, cursor, batch,
or exhaustive-delivery contracts.

| API | What it does | Availability |
|---|---|---|
| [`POST /v2/_search`](percolate/v2-search.md) | Exact bounded top-K for one title, with strict ranking, thresholded totals, enrichment, and no partial results. | Single-node and coordinator modes |
| [`POST\|DELETE /v2/_pit`](percolate/pit.md) | Open and close a point-in-time used for stable cursor pagination. | Single-node and in-process cluster; remote coordinator returns 501 |
| [`POST /_percolate/jobs`](percolate/exhaustive-jobs.md) | Create an exact exhaustive job with bounded admission and optional idempotency. | Single-node and coordinator modes |
| [`GET /_percolate/jobs/{id}`](percolate/exhaustive-jobs.md#status-stream-and-cancellation) | Inspect retained status, optionally using bounded wait polling. | Single-node and coordinator modes |
| [`DELETE /_percolate/jobs/{id}`](percolate/exhaustive-jobs.md#status-stream-and-cancellation) | Cancel running work or remove a retained terminal result. | Single-node and coordinator modes |
| [`GET /_percolate/jobs/{id}/stream`](percolate/exhaustive-jobs.md#status-stream-and-cancellation) | Claim the single bounded, terminally attested NDJSON result stream. | Single-node and coordinator modes |
| [`GET\|POST /_search`](percolate/search.md) | Compatibility percolation for one or more titles, including filtering, ranking, paging, explain, and profile controls. | Single-node and coordinator modes |
| [`POST /v2/_mpercolate`](percolate/v2-mpercolate.md) | Strict shared-options exact bounded top-K batch with whole-request exactness. | Single-node and coordinator modes |
| [`POST /_mpercolate`](percolate/mpercolate.md) | Strict full-result compatibility batch with ordered slots. | Single-node and coordinator modes; some profiling differs |
