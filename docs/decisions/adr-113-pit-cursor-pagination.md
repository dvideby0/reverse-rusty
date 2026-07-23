# ADR-113: Point-in-time snapshots and cursor pagination

- **Status:** Accepted
- **Date:** 2026-07-18

## Context

Every ranked increment since ADR-107 deliberately served the **current view**: `/v2/_search`
returns the exact top K under one deadline, `from` and `cursor` are reserved 400s, and clients
needing snapshot-consistent paging were told to wait for PIT/cursors (ADR-110 §Consequences,
ADR-112). Deep pagination with offset `from` re-matches per page and is not point-stable across
writes (the legacy ADR-059 surface keeps that behavior, documented). The ranked-percolation
program's Increment 6 is this increment: deep pagination that never mixes generations, fails
closed, and adds nothing to the match hot path.

## Decision

### A PIT is a pinned `Arc<EngineSnapshot>`; the registry is dumb serving state

A published `EngineSnapshot` is already structurally immutable — every write goes through
`Arc::make_mut` copy-on-write on the engine's own handles, so a held Arc keeps serving its alive
bitmaps, memtable copy, and mmap segments (which stay mapped after compaction unlinks their
files) untouched. Pinning that Arc IS the point-in-time; there is no versioned-read machinery.
The lean core gains one primitive, `PitRegistry<T>` (`src/pit.rs`): a TTL'd map with an injected
clock (the `RetentionLeases` pattern — lazy reap on every touch, no background thread),
renew-on-use keep-alives, and a cap that **rejects** (429) rather than evicts — an evicted PIT
would silently break someone else's cursor. Ids are process-monotonic and never reused; the
registry dying with the process is the designed restart semantics.

### `search_after` is a strict boundary in the ONE collector choke point

`TopKOptions` gains `search_after: Option<(i64, u64)>` (default `None` ⇒ byte-identical).
`TopKState::observe` skips rows the boundary does not strictly beat (`ranked_beats`, the ADR-110
total order) — after scoring, before the heap, and after `totals.observe`, so **totals stay
corpus-wide and page-invariant** (the ES `search_after` semantics; every page of one PIT reports
the identical total, a pinned test invariant). Strictness makes the last row of one page the
exclusive boundary of the next: no duplicate, no gap; concatenating pages equals
collect-all-sort-truncate (the property test) and the one-shot ranked snapshot (the engine,
HTTP, and cluster exit gates). Batch requests reject a boundary loudly
(`BatchSearchAfterUnsupported`) — per-title batch cursors are a named deferral.

### The client surface: explicit ES-style `/v2/_pit` + an opaque signed cursor

`POST /v2/_pit {"keep_alive_s"?}` opens a PIT (defaults 60s, ceiling 600s, 64 open —
`--pit-default-keep-alive-secs` / `--pit-max-keep-alive-secs` / `--max-open-pits`);
`DELETE /v2/_pit {"pit_id"}` closes it. Page one binds a search with `"pit": {"id": ...}`; a
FULL page (`hits.len() == size`, `size > 0`) returns `next_cursor`; the client resends the same
request with `"cursor"` to continue; a short page ends the stream (`next_cursor` absent). Every
use renews the keep-alive. `pit` + `cursor` together is a 400 (the cursor names its PIT).

Tokens are HMAC-SHA256-signed hex (new `hmac` + `sha2` deps, **server-feature-only** — the lean
core stays at seven crates; hex via a local helper, no base64 crate). The MAC key is per-process
(two `uuid` v4 draws): a restarted server has lost its registry, so a token that cannot even
authenticate is exactly as stale as the state it referenced. The cursor carries the pit id, the
`(score, logical_id)` boundary, and a SHA-256 **fingerprint** of the request's page-invariant
semantics — the normalized title (the pinned snapshot's normalizer), `query_scope`, the rank
program, and the resolved filter (canonicalized; `size`/`timeout_ms`/`track_total_hits_up_to`
may vary per page). The client must resend the same query; a mismatch is a 400
`cursor_mismatch` instead of silently wrong pages.

### The status contract — and the one deliberate read-surface 409

| condition | status |
|---|---|
| structurally garbled token | 400 `validation_error` |
| authenticated but unknown / expired / closed / restarted / placement-drifted | **409 `stale_cursor`** |
| valid cursor, different resent query | 400 `cursor_mismatch` |
| registry at cap | 429 `pit_limit_exceeded` |
| PIT on a remote/gRPC assembly | 501 `pit_unsupported` (+ the alternative) |
| `from` on v2, `pit`/`cursor` on `/v2/_mpercolate` | named 400s (unchanged / newly named) |

ADR-110 made the ranked read surface 409-free (same conditions as the write surface's 409s map
to 503 retry-later). ADR-113 amends that rule for exactly one condition: a stale PIT is **not**
retry-later — the pinned generation is gone forever, and the client's correct move is to open a
new PIT and restart. The `http_status.rs` doc-table records the amendment and pin tests hold
both surfaces to it (`PitNotFound` is the divergence-free row: 409 on both).

### Cluster: index-wide PITs, a placement-identity gate, and one fan body

`Shard` gains `open_pit` / `close_pit` / `percolate_top_k_owned_pit`, defaults fail-closed
(`PitUnsupported`) so a shard that cannot pin can never silently serve a current-view page into
a cursor stream. `LocalShard` holds a plain pin map (the coordinator's registry owns TTL/caps
and fans closes). `ClusterEngine::open_pit` pins EVERY position (title-independent, the ES
index-wide shape) fail-closed, recording `(placement_generation, num_shards)`;
`try_percolate_filtered_top_k_pit` gates on that identity, then runs the ONE extracted
`top_k_core` fan with each shard reading its pinned snapshot — the current-view and pit paths
cannot fork. `validate_part` additionally attests every shard's first row sits strictly after
the boundary (a dishonest pre-boundary row would duplicate an earlier page).

Invalidation: `resize` and `set_vocab` (the only placement-generation bumps) drop the old
shards — and their pins — and the coordinator `clear()`s its registry entries **without
resetting the id counter** (a reused id would let a stale cursor alias a post-rebuild PIT of
the new generation). Reassign/rebalance move data without bumping the generation and correctly
leave PITs alone in-process. A durable reopen serves no prior PIT (in-memory registry).
`ReplicatedShard` delegates pit ops to the **primary only** — a PIT does not survive failover
(the replica has no pin ⇒ `PitNotFound` ⇒ 409), deliberately: silent failover would serve a
different engine's view mid-cursor. A `search_after` boundary without a PIT is refused
(`PitUnsupported`) — the wire cannot carry it and a current-view boundary would mix
generations across pages.

### Enrichment stays current-view fail-closed

`SourceStore` is interior-mutable (`insert(&self)`), so a pinned snapshot does **not** freeze
`_source` text: matching/scoring/order/totals are snapshot-stable; winner enrichment reads
current sources, and a winner deleted between pages fails the page typed (`source_unavailable`
500 local / 502 cluster — the same race ADR-110 documented). `include_source: false` pages are
fully snapshot-stable. Snapshot-consistent sources (SourceStore versioning) are a named
deferral.

## Consequences

- Pagination is exact and generation-pure: pages concatenate to one frozen ranked snapshot with
  no duplicates and no gaps, proven at the collector (property), engine (held snapshot across
  delete/flush/compact), HTTP (both modes), and cluster (K×RF sweep ≡ single-node) seams.
- A PIT retains memory/disk: the pre-publish memtable copy and any compaction-unlinked segments
  stay live until the last pin drops (unix: unlinked-but-mapped). `--max-open-pits` × TTL bound
  it; the `open_pits` gauge exposes it.
- The default path is byte-identical (`search_after: None`, no PIT): rankbench canonical
  checksums unchanged; every suite green.
- Deferred, in the ADR-110 §Consequences pattern: wire/gRPC PIT (`RemoteShard` refuses typed
  with the alternative; the coordinator refuses before fanning), batch cursors, SourceStore
  versioning, and the
  pre-existing `/v2/_mpercolate` auth-allowlist asymmetry (`/v2/_pit` joins the open search
  allowlist; `/v2/_mpercolate` still requires a token when one is configured — flagged, not
  changed here). `result_mode=all` was also deferred at this decision and is now delivered by
  ADR-114.
- `evaluations` rank counters legitimately differ across pages (the scorer runs before the
  boundary check); oracles compare winners + totals only (the ADR-112 rule).
