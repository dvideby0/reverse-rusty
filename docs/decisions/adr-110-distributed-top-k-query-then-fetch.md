# ADR-110: Distributed top-K and query-then-fetch

- **Status:** Accepted
- **Date:** 2026-07-16

## Context

ADR-108 connected exact Boolean matching directly to a bounded local `TopKCollector`, but cluster
ranking still used ADR-075's compatibility path: every routed shard returned every scored match and
the coordinator materialized, deduplicated, sorted, and paginated the full union. ADR-109 supplied
the missing prerequisite for an exact bounded merge by making every logical match emit at one
deterministic routed shard position.

Returning sources or explanations in the phase-one shard response would undo the row bound and spend
network and compilation work on local contenders that lose globally. Distributed delivery also needs
one deadline and fail-closed behavior across permit wait, fan-out, merge, enrichment, and explanation;
otherwise a timeout or missing source could leak a plausible but incomplete response.

## Decision

### Ownership before bounded collection

The snapshot top-K implementation is generalized over the existing monomorphized `EmissionPolicy`.
Standalone calls instantiate `EmitAll`. A shard instantiates ADR-109 `UniqueOwner`, after exact
verification and per-member aliveness/tag checks but before scoring enters `TopKCollector`.

`ClusterEngine::compile_rank_program` compiles the typed priority flag and request boosts once against
the coordinator's authoritative frozen `TagDict`. `try_percolate_filtered_top_k` then fans the same
integer-only program, filter, `TopKOptions`, ownership context, and absolute deadline to every routed
position. Each shard returns at most K owned rows, strictly ordered by
`(score desc, logical_id asc)`, plus its thresholded total, match/rank statistics, and bounded/
ownership/configuration attestations.

The coordinator rejects an oversized, unordered, stale, or unattested reply and rejects any logical
ID emitted by more than one position. It merges the disjoint rows with the same total order and
truncates to K. This is exact: if a global winner were below its owner's local K, that one owner would
already contain K rows that globally outrank it, a contradiction.

`ClusterRankedMatch` reports the winners, merged total, merged match/rank stats, routed-position count,
actual shard rows received, remote protobuf bytes, and the phase-one placement identity. The hard row
bound is `shard_rows_received <= K × routed_shards`, including K=0.

### Exact thresholded totals

Shard totals count owned logical matches. The coordinator sums them only while every shard relation is
`eq` and the sum is representable. It returns `eq` only when that exact sum is at most the request
threshold. Otherwise it returns exactly `{value: threshold, relation: gte}`. Thus individually exact
shards whose global sum crosses the threshold cannot accidentally produce a globally exact response.

### Query-then-fetch enrichment

After the global winners are fixed, `fetch_ranked_sources` groups their IDs by owning logical shard
position. The private `Shard` seam gains one batched current-source read; `ReplicatedShard` and
`HandoffShard` apply their ordinary in-sync read failover/current-backing behavior. Over gRPC,
`FetchMatches` is server-streaming and returns exactly one current source per requested winner.

This increment deliberately uses the **current view**, not a PIT or snapshot token. Phase two validates
the phase-one placement generation and shard count at the coordinator, request, and every streamed
message. A missing source, generation/configuration drift, malformed stream, or fetch failure
invalidates the whole response. Explanations never cross the wire: the coordinator recompiles each
fetched source with its authoritative normalizer/dictionary and builds `ExplainDetail` locally.

Cluster HTTP ingest uses ADR-108's strict `rank_fields.priority` parser and mirrors the signed value
into the already-durable raw `tags.priority`. Cluster shard ingest reconstructs the typed value from
that raw tag, including post-freeze synthetic tag IDs. This preserves signed priority over live,
replay, recovery, and remote paths without changing a durable format.

### Transport, deadlines, and bounds

The shard protobuf adds `PercolateTopK` and server-streaming `FetchMatches`. `PercolateTopK` carries a
typed compiled rank program, compiled filter, ownership context, K, total threshold, and remaining
microseconds. The client also sets `grpc-timeout`. Every retry recomputes both from the same original
absolute deadline; backoff, transport, shard cooperative `DeadlineAt`, stream drain, merge, source
fetch, and explanation all consume that one budget. No partial response is supported.

A pre-ADR-110 peer returns `UNIMPLEMENTED`; missing bounded/ownership echoes or mismatched placement
metadata fail closed. Existing all-ID `Percolate` remains wire-compatible.

Two static response limits bound enrichment and compatibility traffic:

- `server --max-ranked-enrichment-bytes`, default 16 MiB, charges winner source text once even when
  both `_source` and explanation use it. Local and cluster `/v2/_search` return
  `413 rank_enrichment_limit` before emitting any partial response.
- `shardserver --max-grpc-result-bytes`, default 4 MiB and constrained to `1..=4 MiB`, checks exact
  protobuf encoded size for compatibility percolate replies, top-K replies, and each fetch-stream
  item. Overflow is gRPC `RESOURCE_EXHAUSTED`; the cap can be lowered but never raised past tonic's
  default receive bound.

### REST and observability

Cluster mode now registers `POST /v2/_search` with the same DTOs, defaults, validation, rank program,
winner-enrichment budget, and dedicated bounded-search permit as local mode. Successful `_shards`
counts are routed logical positions. Shard, timeout, placement, source, and cap failures return no hits;
partial results remain unsupported. Compatibility HTTP routes are unchanged.

Bounded counters cover shard hits/result bytes, source-fetch bytes, total relation, cancellation, cap
rejection, coordinator rows/result bytes, and enrichment rejection. Transport metrics add top-K/fetch
outcomes, and the shard latency histogram adds the two RPC methods. All labels remain fixed-cardinality.

### Persistence

No segment, WAL, manifest, coordinator-log, translog, or adopted-space format changes are needed.
ADR-109 placement identity and existing raw source/tag durability contain all state required by both
phases. Flush, checkpoint/reopen, backup/restore, replication, recovery, and handoff reuse their
existing formats and lifecycle rules.

## Correctness and verification

Ownership filtering is downstream of exact matching and cannot affect candidate cover, routing,
negative handling, or tag filtering. It selects exactly one emitter before a bounded collector; the
local-K/global-K contradiction above proves that the coordinator's bounded merge equals collect-all,
full-sort, truncate. Scores are integer-only and the shared `(score desc, id asc)` order makes ties
deterministic. Summed owned totals count every logical match exactly once.

Differential tests compare distributed top-K with single-node collect-all/full-sort across K and total
thresholds, signed scores, ties, filters, A/B/C/D/H, canonical bodies, and dynamic vocabulary. They pin
replicated high-score rows, one-owner winners, global threshold overflow, K=0, empty results, stale
placement, and missing sources. Real-gRPC tests cover mixed-version refusal, one absolute deadline,
exact caps, source streaming, post-freeze priority, RF>1 failover/recovery, and live handoff. Handler
tests cover shared defaults and 400/408/413/502/503 no-partial failures. Durability tests cover flush,
checkpoint/reopen, and backup/restore. `rankbench` measures actual shard rows, coordinator collect+
merge time, fetch bytes/time, asserts the K×routed bound, and retains every prior semantic checksum.

## Consequences and deferred work

Network rows are now bounded by routed positions × K, and source/explanation work is winner-only. An
enrichment race is intentionally resolved to current state and can fail the response; clients needing
snapshot-consistent paging must wait for PIT/cursors.

Title batching, PIT/cursors, exhaustive jobs/streams, partial results, and competitive pruning remain
the later ranked-delivery increments. The compatibility all-ID gRPC method remains available but is
now safely bounded by the static result cap.
