# `POST /_cluster/resize` — Resize an in-process cluster

> [Cluster control APIs](../cluster.md) · [REST API hub](../../api.md)

This strict native operation rebuilds an in-process cluster under a fresh consistent-hash ring and
atomically replaces the serving shard set (ADR-078/167). Every live query is re-extracted from its
stored source and re-placed under the new ring; vocabulary, the frozen feature/tag spaces, query
tags, ranking values, and Boolean semantics are preserved.

```bash
curl -X POST \
  'localhost:9200/_cluster/resize?cluster_manager_timeout=5s' \
  -H 'Content-Type: application/json' \
  -d '{"num_shards":16}'
```

The required `num_shards` is an integer from 1 through 1024. It may grow or shrink the ring by an
arbitrary amount; it need not be a factor or multiple of the current count. Repeating an already
attested current count is an acknowledged no-op in memory. A same-count retry repairs a prior
post-swap control-count failure before acknowledgement; on a durable cluster it also re-checkpoints
and repairs the on-disk shard-directory set.

A successful response is terminal:

```json
{
  "acknowledged": true,
  "shards_acknowledged": true,
  "version": 47,
  "old_num_shards": 8,
  "num_shards": 16,
  "rebuilt": 1200000
}
```

- `acknowledged` means the blue/green rebuild, atomic serving swap, control-state shard-count
  proposal, and required durable checkpoint all completed.
- `shards_acknowledged` is the familiar ES/OpenSearch field and is always true on a 200: unlike
  their target-index operations, this synchronous endpoint does not return while target shards are
  unassigned or recovering.
- `version` is the final observed `ClusterState` application version, shared with
  [`GET /_cluster/state`](../observability/cluster-state.md). It is not a Raft term/log index,
  checkpoint epoch, feature-model version, or placement generation. Before returning it, resize
  also attests that the committed placement generation exactly matches the serving shards.
- `old_num_shards` and `num_shards` report the serving ring transition. `rebuilt` is the number of
  unique live logical queries rebuilt; it is zero for a same-count retry.

## Execution and timeout contract

The rebuild is `O(corpus)` and temporarily needs blue and green state. One shared administrative
slot admits it alongside stats and vocabulary work. After admission, one independently supervised
OS thread acquires exclusive topology, REST-write, and cluster guards, rebuilds the corpus, swaps
the ring/shards, commits control state, checkpoints when durable, and reads the final version. Tokio
request workers never wait on those blocking locks or perform the rebuild.

Supported query controls:

- `cluster_manager_timeout` is the OpenSearch-inclusive spelling.
- `master_timeout` is the Elasticsearch and legacy OpenSearch spelling.

They are aliases; specify at most one. Values use `nanos`, `micros`, `ms`, `s`, `m`, `h`, or `d`,
default to 30 seconds, and cannot exceed 30 seconds. Exact `0` performs a non-waiting admission and
exclusive-lock probe. A positive value covers admission, dedicated-worker dispatch, and lock
waiting until the rebuild atomically starts.

A deadline before start returns `408 resize_timeout` and guarantees no delayed resize can begin.
Once all exclusive guards are held and the rebuild starts, the manager timeout does not cancel it:
arbitrary cancellation could strand a swapped in-memory ring, control state, and durable manifest
at different generations. The request waits for the exact terminal result. If the client
disconnects after start, the supervised worker retains admission and completes; graceful shutdown
acquires and retains the shared corpus-administration admission slot before its final checkpoint,
so an admitted worker cannot start after cleanup. Inspect `/_health` and `/_cluster/state` after any
connection loss before retrying.

A failed control proposal can occur after the serving swap. The next request first repairs only
that exact one-generation resize predecessor; it cannot advance to a different shard count until
the prior serving/control transition is committed and attested. Any other control/live divergence
fails loud instead of being reinterpreted as a resize retry.

The familiar overall `timeout`, `wait_for_active_shards`, asynchronous task controls, and target
index settings are rejected because their ES/OpenSearch meanings do not match this synchronous
in-place rebuild.

## Strictness, topology, and errors

The route accepts only `POST`, requires `application/json` or `application/*+json`, caps the body at
64 KiB, and gives body delivery 250 ms. It requires exactly one object with exactly one
`num_shards` field; unknown/duplicate/null fields, non-object JSON, fractional/string counts, zero,
and counts above 1024 are rejected before admission. Every route-reached response is structured
JSON, `Cache-Control: no-store`, and observed under the fixed `cluster_resize` metric label.

Only an in-process cluster is supported. A static, CLI-seeded assignment-routed, or resolve-only
remote coordinator returns `501 not_supported_in_cluster_mode` before admission. Changing a remote
ring without first rebuilding and attesting every remote position would make routing disagree with
stored placement and create silent false negatives. Use the documented separate-cluster
blue/green procedure instead; online remote resize remains a
[roadmap item](../../../roadmap.md#automatic-and-remote-cluster-resize).

Invalid input is 400, a pre-start deadline is 408, an oversized body is 413, a missing/wrong media
type is 415, closed/failed worker admission is 503, and the remote-topology boundary is 501.
Underlying rebuild, control, or durability failures fail loud with a typed non-200 response and a
sanitized reason; inspect server logs, health, and cluster state before retrying.

## Elasticsearch/OpenSearch boundary

Elasticsearch and OpenSearch resize a named source index into a distinct target index through
`/{index}/_split/{target}` or `/{index}/_shrink/{target}`. Their split/shrink operations preserve the
source, constrain the target shard count to a multiple/factor, require index-state preparation, and
may acknowledge cluster-state creation before target recovery completes
([Elasticsearch shrink](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-shrink),
[OpenSearch split](https://docs.opensearch.org/latest/api-reference/index-apis/split/),
[OpenSearch shrink](https://docs.opensearch.org/latest/api-reference/index-apis/shrink-index/)).

Reverse Rusty has one reverse-query corpus rather than named source/target indices. Its operation
mutates the serving ring in place, accepts arbitrary bounded shard counts, remains synchronous, and
does a full source rebuild instead of hard-linking Lucene segments. Aliasing it to `_split` or
`_shrink`, accepting target-index settings, or returning a fabricated index name would therefore be
false compatibility. The native path stays explicit; only manager-timeout spellings and
`shards_acknowledged` are shared where their semantics are exact.

Cross-topology assembly rules are documented in
[coordinator mode](../server/coordinator-mode.md). The operator procedure lives in
[cluster deployment](../../../operations/cluster-deployment.md#5-scaling); cluster design and
failure invariants are canonical in
[clustering and scaling](../../../design/clustering-and-scaling.md).

---

Back to the [REST API reference](../../api.md).
