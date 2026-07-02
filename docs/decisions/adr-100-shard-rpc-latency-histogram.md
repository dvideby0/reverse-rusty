# ADR-100: Per-shard RPC latency histograms in the lean `/_metrics` (Tier 5 M3 residual)

> [Back to the decisions index](../DECISIONS.md)

- **Status:** **Done (2026-07-02).** `reverse_rusty_shard_rpc_duration_seconds` — a native
  Prometheus histogram family in the shard node's `/_metrics` exposition, per
  `{shard, method}`.

- **Context:** ADR-091 gave the deploy bins lean per-node `/_metrics` (gauges only) and
  explicitly deferred **per-shard p95/p99 latency** — "needs per-request hot-path timing hooks; a
  shard sees only successful in-process operations; the coordinator already has RPC latency via
  ADR-085." The coordinator's ADR-085 transport metrics record per-method **summed** nanos
  (average only, and the *client's* view — network + queueing included); the shard's own service
  time was invisible. The `prometheus` crate (which has real histograms) stays behind the
  `server` feature by the ADR-028/091 lean stance — the `distributed` deploy bins must not gain
  it.

- **Decision:**
  1. **Time at the gRPC handler boundary, never the engine.** One `Instant` pair around the
     engine call inside the `ShardService` handlers — `percolate`, `percolate_ranked`, `ingest`
     — success paths only (error paths `?`-return before the observe, matching ADR-091's "a
     shard sees only successful in-process operations"; failure rates are already client-side in
     ADR-085). Splitting ranked from unranked mirrors ADR-085's client-side method labels, so
     `client latency − shard service latency ≈ network + queueing` is computable per method.
     The match hot path is untouched. Recording is unconditional: ~two `Instant` reads + three
     relaxed `fetch_add`s per RPC — unmeasurable next to any RPC; the exposition is only
     reachable via the opt-in `--metrics-addr` listener.
  2. **A lean, std-only fixed-bucket histogram** (`node_metrics/latency.rs`): 22 finite `le`
     bounds on a 1–2.5–5 log ladder from **2.5 µs to 30 s** — the bottom decade resolves the
     selective path (in-process p99 is single-digit µs, `performance/results.md` §1), the middle
     the broad lane, and 10 s/30 s align with the ADR-085 read/write client deadlines. Bounds are
     `(nanos, "le-label")` const pairs so bound and label cannot drift. `AtomicU64` buckets
     (non-cumulative, cumulated at render), `Relaxed` ordering — the `transport_metrics`
     pattern. ~576 B per slot (3 methods × 24 words).
  3. **Torn-scrape safety by clamping, not ordering.** `Relaxed` gives no cross-counter
     ordering, so a concurrent scrape could see a bucket increment whose count increment it
     misses — rendering `le="+Inf"` below a finite cumulative bucket (a malformed histogram
     `histogram_quantile()` mishandles). Rather than pay acquire/release fences on the RPC path,
     the renderer clamps: `+Inf` and `_count` render `max(count, Σ buckets)`. Every scrape is
     well-formed; a one-observation skew self-corrects next scrape. (>30 s observations land in
     no finite bucket and surface via `count` — the Prometheus `+Inf` contract.)
  4. **Histograms live on the `ShardSlot`,** not the swappable `ServerState`: an in-place
     `recover_from` state swap keeps the series continuous; the totals reset only when the slot
     itself is replaced (adopt-on-empty / `AddShard`) or the process restarts — ordinary
     Prometheus counter resets.
  5. **Native histogram exposition, quantiles are Prometheus's job.** One family,
     `reverse_rusty_shard_rpc_duration_seconds`, with `_bucket{shard,method,le}` (cumulative) +
     `_sum` (seconds) + `_count`, header-once grouped across slots × methods (the ADR-093
     Stage-3 exposition rule). Nothing precomputes p95/p99 server-side:
     `histogram_quantile(0.95, sum by (le, shard) (rate(…_bucket{method="percolate"}[5m])))`.
     A loaded slot that has served no RPCs renders the full all-zero family, so the series exist
     from the first scrape. Controlserver gets nothing (Raft RPC internals belong to openraft;
     the control plane is off the match path).

- **Safety.** All `distributed`-gated: the lean and `server` builds are byte-identical; no wire
  change; no new dependency. The renderer stays lock-free off the engine write lock (it keeps
  the slot `Arc`, mirroring the RPC handlers). Zero-FN untouched — nothing on the match path
  changed.

- **Proven.**
  - Unit (`node_metrics/latency.rs` + `node_metrics/tests.rs`): `le` is inclusive (at-bound vs
    one-nano-over; >30 s bumps count only); the ladder is strictly increasing; the torn-read
    clamp; per-RPC independence; the rendered family (header-once, one line per bound + `+Inf`
    per method, cumulative monotone, `+Inf == _count`, `_sum` in seconds, all-zero methods still
    render); multi-slot header-once with both `{shard=…}` series.
  - Integration (`tests/node_metrics.rs`): a populated server that has served no RPCs renders
    the zero family (first-scrape series continuity).
  - gRPC end-to-end (`tests/cluster_grpc_oracle/transport.rs`): on the existing ADR-085
    workload, the server-side histogram `_count{method="percolate"}` **equals the client-side
    `percolate.calls`** (two-sided consistency: every client-recorded call completed
    successfully server-side), same for `ingest`, and `+Inf` mirrors `_count`.

- **Alternatives considered.**
  - **Precomputed p50/p95/p99 gauges** — rejected: quantiles cannot be aggregated across shards
    or time windows; native `le` buckets are the Prometheus-correct surface.
  - **Acquire/Release ordering for scrape consistency** — rejected: fences on every RPC to
    protect a metrics read; the render-time clamp is free and strictly safer.
  - **Timing inside `LocalShard`/the engine** — rejected: the ADR-091 residual asks for the
    *service* view, and instrumenting the engine puts hooks near the hot path for no added
    signal at RPC granularity.
  - **Per-method metric families** (`…_percolate_duration_seconds`, …) — rejected: N families
    vs one method label; diverges from ADR-085's method-labeled precedent.

- **Deferred follow-ons.** The coordinator-side upgrade of ADR-085's summed `latency_nanos` to
  the same histogram type (client-view percentiles; touches the ungated transport-metrics
  surface — a separate small PR); timing `insert`/`delete`/`flush` (one enum variant + one label
  each when needed); per-shard broad-lane batch cost (the other ADR-091 residual, still open);
  Grafana/alert examples (the Tier 5 M3 ops-docs item).
