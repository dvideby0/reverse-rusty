# ADR-108: Typed priority and local bounded ranked percolation

- **Status:** Accepted
- **Date:** 2026-07-16

## Context

ADR-107 separated exact Boolean matching from result delivery and introduced a bounded collector,
but Increment 1 deliberately stopped before storage and serving. The compatibility ranking path
still materialized every match and parsed the selected priority tag outside the match loop. That
preserved behavior, but `size` bounded only the eventual response—not collection memory or ranking
work—and a tag string was not a durable typed scoring column.

Column-oriented search engines solve the same problem with numeric doc-value/fast-field columns:
Lucene/Elasticsearch doc values and Tantivy fast fields keep sortable values beside document rows,
away from the inverted-index vocabulary. Reverse Rusty needs the same shape without adding a
dependency, interpreting strings on the match path, or changing the lossless signature cover.

## Decision

### One typed column and strict ingest

`ExactStore` gains one parallel signed `i64` column, represented by `RankValues { priority }`.
`rank_fields.priority` on `PUT /_doc/{id}` and `POST /_bulk` accepts only an integer JSON value or a
signed decimal string that fits `i64`; floats, booleans, null, containers, overflow, and unknown rank
fields fail with a typed 400. The value is mirrored into the canonical `priority` tag. If callers
provide both forms, exactly one numerically-equal legacy value is required.

Legacy `tags.priority` remains permissive and behavior-compatible. A numeric legacy value lowers into
the typed column; a malformed value continues to score zero. `TagDict` caches the parsed integer for
dense priority tags, so old segments require no string lookup or parse while matching. Synthetic
legacy priority tags continue to score zero, matching ADR-075.

### Bounded local API

Single-node mode registers `POST /v2/_search` for one document and `result_mode=top_k` only. Defaults
are `query_scope=standard`, `size=100`, `track_total_hits_up_to=10_000`, typed priority ranking,
source inclusion, exact/no-partial results, and a compute-armed five-second timeout. Both K and the
total threshold are capped at 10,000. `all`, `terminated`, `from`, cursors, batching, and the cluster
route remain deferred and fail loudly.

`EngineSnapshot::try_match_title_top_k` connects `TopKCollector` to the scalar local matcher. The
collector retains `O(K + total-threshold)` state and orders winners by `(score desc, logical_id asc)`.
Scores use saturating integer addition. Exact totals are reported while the distinct count is at or
below the threshold; the next distinct match switches the result to `{value: threshold,
relation: gte}`. Winner source/explanation lookup happens only after finalization and fails the whole
request when requested data is unavailable.

Scoring resolves typed priority and tags from the **newest live physical copy for the logical ID**;
the physical copy that happened to emit the match cannot determine rank. `MatchSink::on_match(id)` is
unchanged, so compatibility collection and the unranked path acquire no metadata work. Existing
`RankSpec`, `CompiledRankSpec`, `EngineSnapshot::rank`, `/_search`, `/_mpercolate`, cluster ranking,
and protobuf messages remain unchanged.

A dedicated v2 semaphore defaults to the Rayon worker count. Permit wait and cooperative matching
share the same request deadline. Ranked metrics use bounded labels for outcome/scope, total relation,
and admission reason, plus counters for score evaluations, heap replacements, source bytes, and
reported true-match lower bounds.

### Persistence and migration

The priority column follows every exact row through body-dedup membership, flush, both compaction
paths, upsert/delete, vocabulary rebuild, cluster resize carry-through, backup, and restore.

- `.seg` v6 appends an aligned `i64` array to the exact section and validates its count at open. It is
  written only when at least one typed value is non-zero; all-zero files retain their prior v3/v4/v5
  version and layout. v1–v5 read with no column and use the cached legacy-tag fallback. Compaction
  materializes that fallback, so rewrites migrate naturally.
- WAL v6 appends an optional `i64` to existing insert/upsert payloads without new opcodes. The WAL
  header version is informational and old readers stop after the tag section, safely ignoring the
  extension while retaining the mirrored tag. New readers derive legacy frames from cached tags.
- Manifest, cluster-manifest, source-store, and protobuf versions do not change: none of their
  layouts gains a field, and segment capability is already self-described and fail-loud.

## Correctness and verification

Ranking remains strictly post-verification. It cannot influence signatures, candidate retrieval,
MUST_NOT handling, tag filtering, or visibility, so the lossless-cover proof is unchanged.

The bounded path is differentially checked against collect-all → newest-live scoring → full sort →
truncate across K/threshold boundaries, signed values, saturating boosts, ties, filters, visibility
scope, duplicate copies, and canonical-body members. Persistence tests cover WAL tails, v6 mmap,
legacy fallback and compaction migration, corruption refusal, and reopen. Handler tests cover strict
ingest, defaults, bounds, threshold relations, deferred modes, source failure, and permit-deadline
queuing. `rankbench` retains ADR-107's four semantic checksums as compatibility pins.

## Consequences and deferred work

Local top-K result memory is bounded independently of the true match count, and ranking is integer
only after request compilation. A non-zero typed priority costs eight bytes per exact row on disk and
in memory; all-zero persistent corpora retain old segment versions.

Distributed top-K ownership/merge, protobuf transport, multi-document ranking, cursor/PIT, exhaustive
delivery, explicit approximate termination, and competitive candidate pruning remain Increment 3+
work. Compatibility ranking deliberately remains collect-all and tag-string-semantic.
