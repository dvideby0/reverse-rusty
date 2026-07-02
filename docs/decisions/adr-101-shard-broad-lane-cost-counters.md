# ADR-101: Per-shard broad-lane cost counters in the lean `/_metrics` (Tier 5 M3 residual)

> [Back to the decisions index](../DECISIONS.md)

- **Status:** **Done (2026-07-02).** `reverse_rusty_broad_{candidates,postings_scanned,queries_evaluated,batches}_total{shard}`
  — four native Prometheus counter families in the shard node's `/_metrics` exposition.

- **Context:** ADR-091 deferred per-shard **broad-lane batch cost** — "stays coordinator-side
  (ADR-026 already tracks it there); per-shard gets a proxy via `class_queries{class="c"}`" — and
  ADR-100 re-deferred it. The coordinator's registry counts `broad_*_total` cumulatively across
  *all* its requests, so a single hot shard soaking broad-lane work is invisible: the per-shard
  cost signal existed per-call (every percolate reply already carries `MatchStats` with five
  broad-lane counters) but was aggregated nowhere on the serving side.

- **Decision:**
  1. **Accumulate at the gRPC handler boundary, never the engine.** The `percolate` handler
     already holds the engine-returned `MatchStats` on both its branches (plain + `rank`); a
     `SlotBroadCost::record(&stats)` there is four relaxed `fetch_add`s on the success path —
     the match hot path is untouched, and recording is unconditional (an `include_broad=false`
     call carries all-zero broad fields; `fetch_add(0)` is branch-free noise). `ingest` produces
     no `MatchStats`; there is no other match RPC.
  2. **Same wire names as the coordinator, `{shard}`-labeled.** A shard IS an engine (the
     ADR-091 wire-name rule), so the families are the coordinator registry's exact names —
     `reverse_rusty_broad_candidates_total`, `…_broad_postings_scanned_total`,
     `…_broad_queries_evaluated_total`, `…_broad_batches_total` — with the additive `{shard}`
     label; existing dashboards keyed on the bare names match per-pod. `broad_anchors_scanned`
     is deliberately not exposed (no coordinator counterpart — keep the family set symmetric).
  3. **The columnar-only fields render 0 today, deliberately.** The shard wire is per-title
     `Percolate` only, and the columnar batch evaluator runs only under `match_titles_batch`
     (the coordinator's local `/_mpercolate` path) — so `queries_evaluated`/`batches` are
     structurally 0 on a shard node. They are accumulated + rendered anyway: name symmetry,
     first-scrape series continuity (the ADR-100 precedent), and a future batch-percolate RPC
     lights them up without a naming change. The live per-shard broad signals are
     `broad_candidates_total` + `broad_postings_scanned_total`; broad share per title is a
     PromQL job:
     `rate(reverse_rusty_broad_candidates_total[5m]) / rate(reverse_rusty_shard_rpc_duration_seconds_count{method=~"percolate.*"}[5m])`.
  4. **Counters live on the `ShardSlot`** (`node_metrics/broad_cost.rs`, mirroring ADR-100's
     `SlotLatency`): an in-place `recover_from` state swap keeps the totals continuous; a
     whole-slot replacement or restart is an ordinary Prometheus counter reset. Unlike the
     histogram there is no cross-counter invariant to protect at render time — each total is an
     independent monotone counter, so relaxed increments + relaxed reads are the whole story (no
     clamp machinery). ~4 words per slot.

- **Safety.** All `distributed`-gated: the lean and `server` builds are byte-identical; no wire
  change; no new dependency; zero-FN untouched (nothing on the match path changed). The same
  full metric names appear unlabeled on the coordinator and `{shard}`-labeled on shard pods —
  different scrape targets, the deliberate ADR-091 posture.

- **Proven.**
  - Unit (`node_metrics/broad_cost.rs` + `node_metrics/tests.rs`): monotonic accumulation,
    field independence, zero-default; the rendered families (typed `counter` header once across
    co-located slots, one `{shard=…}` series per slot, exact values, the columnar-only zeros
    asserted).
  - Integration (`tests/node_metrics.rs`): a populated server that has served no percolate
    renders the all-zero families (first-scrape series continuity).
  - gRPC end-to-end (`tests/cluster_grpc_oracle/broad_cost.rs`): a broad-OFF workload leaves
    all four counters exactly 0; a broad-ON workload (plain + ranked branches both driven) makes
    the rendered `candidates`/`postings_scanned` totals **equal the client-summed per-call
    `MatchStats` exactly** (two-sided consistency on the happy path — errors/timeouts/retries
    asserted 0 first, the ADR-100 precedent), with the columnar-only families still 0.

- **Alternatives considered.**
  - **`shard_`-prefixed names** (`reverse_rusty_shard_broad_*`) — rejected: breaks the ADR-091
    "same names as a single-node engine" dashboard contract for no disambiguation gain (the
    `{shard}` label already disambiguates).
  - **Only the two families that can move today** — rejected: asymmetric with the coordinator's
    four, and a future batch RPC would force a naming decision under pressure; the cost of two
    all-zero series is near nil and the caveat is documented + test-asserted.
  - **Broad-lane *time* (a second histogram or a broad/selective split of ADR-100's)** —
    rejected for now: needs engine-internal timing hooks near the hot path (the exact thing
    ADR-091/100 declined); the counter family + the RPC histogram already separate "how much
    broad work" from "how slow".

- **Deferred follow-ons.** A shard-side batch-percolate RPC (would light up
  `queries_evaluated`/`batches`); the ADR-100 deferrals stand unchanged.
