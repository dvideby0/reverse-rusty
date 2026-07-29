# `GET` / `HEAD /_health` — Native readiness

> [Observability APIs](../observability.md) · [REST API hub](../../api.md)

```bash
curl 'localhost:9200/_health?wait_for_status=green&timeout=30s'
```

Standalone response:

```json
{
  "status": "green",
  "mode": "standalone",
  "timed_out": false,
  "total_queries": 3,
  "wal_healthy": true,
  "persistence_healthy": true,
  "skipped_segments": 0,
  "stale_segments": 0
}
```

| Status | Meaning |
|---|---|
| `green` | Single-node durability is healthy, or every cluster position answers with no queued repair |
| `yellow` | Single-node load skipped/stale segments, or cluster partial applies are queued for resync |
| `red` | Single-node WAL/persistence failure, or a required cluster shard/control/topology check failed |

Cluster health uses a deliberately smaller native payload:

```json
{
  "status": "green",
  "mode": "cluster",
  "timed_out": false,
  "shards": 8,
  "pending_repairs": 0
}
```

A yellow or red response also includes `reason`. Detailed shard/control-plane errors are logged but
the unauthenticated response uses a stable generic red reason.

The route is a strict, bodyless GET/HEAD with a 64 KiB extraction ceiling. It rejects unknown query
parameters and unsupported values, returns structured 400/405/413 errors, sends
`Allow: GET, HEAD` on 405, strips the body for HEAD, and includes `Cache-Control: no-store` on every
response. `GET` and `HEAD` remain open even with `--auth-protect-reads` so orchestrator probes do
not need bearer credentials. Admission occurs before body buffering and allows at most eight
concurrent health requests; additional work fails immediately with 429, `Retry-After: 1`, and a
structured `rejected_execution_exception`. The cap therefore also covers slow request bodies, and
body buffering has an independent 250 ms deadline. A body that does not complete by then returns
408 with a structured `request_timeout`, releasing its health permit. Health duration telemetry
starts before method validation, admission, and body extraction, so it includes all transport work
and rejections as well as successful probes and status waits.

Supported query controls:

| Control | Contract |
|---|---|
| `wait_for_status=red\|yellow\|green` | Wait until the observed status reaches at least this ordered color; yellow accepts yellow or green |
| `timeout=<time>` | Bound the wait, stats admission, and coordinator probe result wait; default `30s`; units are `nanos`, `micros`, `ms`, `s`, `m`, `h`, or `d` |
| `level=cluster` | Accepted familiar spelling for this cluster-level native response; index/shard levels are rejected |

Green and yellow return HTTP 200. Red returns 503. If coordinator collection cannot complete by the
deadline, or `wait_for_status` is not reached, the latest response returns 408 with
`"timed_out":true`. The response preserves the last completed observation rather than replacing it
with a synthetic failure at the deadline, and a status first seen after the deadline remains timed
out. The coordinator rechecks the wall clock after each blocking result rather than relying only on
the async timeout race. Explicit status waits and dependency-probe deadlines have distinct stable
reasons. A coordinator request that times out cannot forcibly stop already-running blocking/network
work; that work retains its single shared stats permit until its own transport bounds complete.

Coordinator green requires a successful committed control-state read, a count from every logical
serving position, matching committed/ring shard counts, and exactly one in-range committed
assignment per position. The probe shares bounded stats admission with `/_stats` and CAT stats and
runs off the async request workers.

This is deliberately not Elasticsearch/OpenSearch `/_cluster/health`. Those APIs describe Lucene
index-shard allocation; Reverse Rusty has no honest equivalent for their index, active-primary,
relocating, or unassigned-shard fields, so no `/_cluster/health` alias is exposed (ADR-144).
