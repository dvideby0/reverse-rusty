# `GET` / `HEAD /_cluster/state` — Authoritative control state

> [Observability APIs](../observability.md) · [REST API hub](../../api.md)

Coordinator mode exposes the small committed control-plane document used for membership, logical
position placement, and feature/ring identity:

```bash
curl 'localhost:9200/_cluster/state'
```

```json
{
  "version": 0,
  "epoch": 0,
  "nodes": [{"id": 0, "addr": null, "role": "Data"}],
  "voters": [0],
  "assignments": [{"position": 0, "primary": 0, "replicas": []}],
  "num_shards": 1,
  "vnodes": 128,
  "dict_fingerprint": 123,
  "model_version": 0,
  "placement_generation": 1
}
```

`version` is an exact familiar alias for `epoch`, the monotonically committed application-state
version. It is not the local checkpoint epoch, the Raft term/log index, `model_version`, or
`placement_generation`. The other fields retain their native meanings:

| Field | Meaning |
|---|---|
| `nodes` | Registered logical nodes, optional transport addresses, and data/manager eligibility |
| `voters` | Current Raft manager voter ids |
| `assignments` | One logical position's committed primary and replica node ids |
| `num_shards`, `vnodes` | Ring parameters |
| `dict_fingerprint`, `model_version` | Frozen feature-model identity and model transition counter |
| `placement_generation` | Logical row-placement identity, changed only by model/ring rebuilds |

The exact Elasticsearch/OpenSearch version selector is available without transferring the rest of
the document:

```bash
curl 'localhost:9200/_cluster/state/version'
```

```json
{"version": 7}
```

`/_cluster/state/_all` is equivalent to the base path. `local=false` is accepted;
`local=true` is rejected because every successful response comes from the authoritative,
linearizable control plane. `cluster_manager_timeout` and `master_timeout` are mutually exclusive
aliases. Positive values bound admission plus the read (default and maximum 30 seconds). `0` is a
non-queuing probe: it returns 408 if shared introspection admission is occupied; when admitted, it
executes one authoritative read off the request worker. It is not a cancellation deadline for that
already-started synchronous read. `flat_settings` is accepted but representation-neutral because
this document contains no settings section.

Other ES/OpenSearch metric or index-target paths, metadata-version waiting, and index-expansion
controls fail with a validation error. Reverse Rusty has no index metadata, mapping, index-shard
routing table, state UUID, or local coordinator state to return, so those shapes are not
fabricated.

The route accepts only GET/HEAD and an empty body, with a 64 KiB request ceiling and 250 ms body-read
deadline. It shares the single stats/introspection work slot; lock waiting, a remote linearizable
RPC, and JSON serialization run off Tokio request workers. Responses are capped at 8 MiB and always
carry `Cache-Control: no-store`. Backend details stay in server logs when a control read fails.
HEAD performs the same availability check and returns no body.
