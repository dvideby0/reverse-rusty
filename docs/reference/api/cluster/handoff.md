# `POST /_cluster/handoff` — Move live routing between endpoints

> [Cluster control APIs](../cluster.md) · [REST API hub](../../api.md)

The raw-endpoint handoff primitive peer-recovers one logical shard position, fences and drains its
current source, then atomically flips live routing to a fresh target. It does **not** update the
control-plane assignment. Use [`POST /_cluster/reassign`](reassign.md) for a normal,
restart-stable placement change.

## Compatibility boundary

Elasticsearch and OpenSearch expose `POST /_cluster/reroute`. A reroute `move` command identifies a
logical index, shard, source node, and target node; allocation deciders apply it to durable cluster
state. It also supports simulation, explanations, allocation retry, and cluster-state projections
([Elasticsearch reroute](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-reroute),
[OpenSearch reroute](https://docs.opensearch.org/latest/api-reference/cluster-api/cluster-reroute/)).

Reverse Rusty handoff names physical gRPC endpoints and changes only the current coordinator's live
routing. Mapping it to `/_cluster/reroute` would let allocation automation believe a durable
assignment was committed when it was not. The route therefore remains native. It adopts only the
compatible `shard` request alias and manager-timeout spellings.

## Request

```sh
curl -X POST 'localhost:9200/_cluster/handoff?cluster_manager_timeout=30s' \
  -H 'Content-Type: application/json' \
  -d '{
    "position": 2,
    "source": "https://source.example:50051",
    "target": "https://target.example:50051",
    "allow_uncommitted": true
  }'
```

The strict JSON object has four required fields:

| Field | Contract |
|---|---|
| `position` | Non-negative global shard position. `shard` is an alias; specifying both is an error. |
| `source` | Absolute `http://` or `https://` authority for the position's current live primary. Paths, queries, credentials, and values over 2,048 bytes are rejected. |
| `target` | A different absolute endpoint that is not already the live primary or a live replica for the position. |
| `allow_uncommitted` | Must be `true`, acknowledging that this low-level operation does not commit the durable assignment. |

Unknown, duplicate, missing, null, and incorrectly typed fields are rejected. The media type must
be `application/json` or `application/*+json`. The body is limited to 64 KiB and must finish
arriving within 250 ms.

`source` is not trusted as a command. After reserving the source/target footprint in the move
ledger, the coordinator compares it with the position's current live primary. A stale source or an
already-live replica target fails before recovery or fencing. This prevents an old same-dictionary
slot from seeding a new live owner.

## Query parameters and start timeout

| Parameter | Contract |
|---|---|
| `cluster_manager_timeout` | Time allowed to obtain handoff admission, topology/cluster access, and the conflict-aware endpoint-ledger reservation. Default and maximum `30s`; `0` is one non-waiting attempt. |
| `master_timeout` | Alias for `cluster_manager_timeout`. Supplying both is an error. |

Supported units are `nanos`, `micros`, `ms`, `s`, `m`, `h`, and `d`; bare `0` is also accepted.
Unknown and duplicate controls are rejected. `timeout` is deliberately unsupported: once recovery
or fencing begins, cancellation cannot safely promise that routing and source fencing stayed
unchanged.

A pre-start deadline returns 408 and guarantees that no handoff from that request will begin later.
Once started, the request waits for the exact terminal result even if the manager deadline passes.
If the client disconnects, the independently supervised worker still completes, retains its single
REST handoff admission slot, and records the outcome. Coordinator shutdown waits for active handoff
workers before final durability cleanup.

## Execution and retries

Under the move-ledger reservation the operation:

1. attests that `source` is the current live primary and `target` is outside the live replica set;
2. peer-recovers the target while the source continues serving reads and writes;
3. fences source writes and drains the finite tail to convergence;
4. atomically flips the position's live backing to the target; and
5. releases the source retention lease.

Failures before the flip leave routing unchanged and attempt to auto-unfence the source. The brief
fence window can reject writes explicitly; a rejected write is never silently accepted. An exact
retry after a lost successful response is idempotent: if the requested target is already the live
primary, the route returns `moved:false` with its current generation.

The source remains outside live routing after success but the durable assignment still names its
previous owner. Update external static topology before restart, or—preferably—use `/_cluster/reassign`
instead of this primitive. `allow_uncommitted:true`, `committed:false`, and the response warning
make that boundary machine-visible.

## Response

```json
{
  "took": 418,
  "took_ms": 418.73,
  "acknowledged": true,
  "moved": true,
  "committed": false,
  "position": 2,
  "generation": 7,
  "warning": "live routing changed without committing the control-plane assignment; use POST /_cluster/reassign for restart-stable placement"
}
```

`acknowledged:true` means the requested live-routing operation reached a terminal success.
`moved:false` means the target already owned routing. `committed` is always false on this endpoint;
the generation is the live handoff fence/routing generation, not a control-state version.

Every route-reached response carries `Cache-Control: no-store` and fixed `cluster_handoff`
request/duration telemetry. The route is protected by the normal mutating-endpoint bearer-token
policy.

## Errors and availability

- **400 `validation_error`** — invalid query/body/endpoint, stale source, live-replica target, bad
  position, or a coordinator not assembled with handoff-capable remote backings.
- **408 `handoff_timeout`** — admission, topology/cluster access, or endpoint-ledger reservation did
  not complete before the manager deadline; no movement started.
- **413 `payload_too_large`** — body exceeds 64 KiB.
- **415 `validation_error`** — JSON media type is missing or unsupported.
- **500/502/503** — supervised-worker, remote mesh, fingerprint, recovery, fencing, or admission
  failure; inspect logs and cluster health before retrying.
- **501 `not_supported_in_cluster_mode`** — server was built without the `distributed` feature.

The endpoint requires a distributed build. It is a low-level deployment/test primitive, not the
production placement API. Cross-topology assembly rules live in
[coordinator mode](../server/coordinator-mode.md); movement invariants live in
[clustering and scaling](../../../design/clustering-and-scaling.md).
