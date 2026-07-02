# Resource sizing

How to size a Reverse Rusty deployment (roadmap Tier 5 M3). This page gives the *method*; the
measured numbers it plugs in live in **[`../performance/results.md`](../performance/results.md) §5
(memory) and §1–4 (throughput)** — that file is the SSOT, this one rounds and links.

> TL;DR — sizing is **memory-driven, not CPU-driven**. Budget ~256 B of resident memory per stored
> query at today's baseline (production-optimization target 80–140 B — results.md §5), give each
> node ~2× steady-state headroom, and pick the shard count from the corpus size — throughput takes
> care of itself (the selective path clears the spec target by 57–255× per core).

## 1. Memory: the shard-count formula

Steady-state resident memory scales linearly with stored queries (measured flat 1M→5M —
results.md §5). Size from the corpus:

```
corpus_bytes ≈ num_queries × bytes_per_query        (≈256 B baseline today)
K_min        ≈ corpus_bytes / (node_memory × 0.5)   (0.5 = the headroom factor, §2)
```

Worked example (the results.md §5 extrapolation): 100M queries ≈ 25.6 GB ⇒ **8 shards of
~12.5M queries (~3.3 GB each) on commodity 8–16 GB nodes**, or ~16 shards for more headroom +
better cache residency (§3). Don't size shards above a few GB of index each even when memory
allows — §3 is why.

Two refinements:

- **Broad share matters more than count alone:** class-C (broad) queries cost far more per title
  than selective ones. Watch the live cost-class split (`reverse_rusty_class_queries{class="c"}`)
  and the per-shard broad counters (ADR-101) rather than assuming the benchmark mix.
- **Trust the gauges over the formula:** `reverse_rusty_memory_bytes{component=…}` on every
  shard's `/_metrics` (ADR-091) is the real number, broken down by component. The formula gets
  you a starting topology; the gauge tells you when it was wrong.

## 2. Headroom: why ~2×

Steady-state is not peak. Transients that ride on top of resident index memory:

- **Flush + compaction** build the next segment while the old ones still serve — briefly holding
  both (bounded by segment size, but real).
- **Blue/green operations** (vocab rebuild, resize — ADR-046/078) build a second engine before
  swapping: an in-process cluster doing `set_vocab` needs ~2× its own footprint for the rebuild
  window.
- **Ingest bursts** grow the memtable ahead of the flush trigger.

Halving the usable node memory in the formula (`× 0.5`) covers these plus the OS page cache the
mmap'd segments want. If you never run blue/green ops on the node, 0.6–0.7 is defensible;
`reverse_rusty_memory_bytes` over a week of real traffic is the tiebreaker.

## 3. Cache residency: why more, smaller shards

Per-core throughput *declines* as one engine's hot structures outgrow L2/L3 (measured 710k → 437k
titles/s/core from 1M → 5M queries — results.md §5's cache-residency note). Sharding is the fix:
each shard's postings + SoA stay cache-resident, recovering the higher per-core rate. This is why
the 100M plan says 8–16 shards, not "one big node with 32 GB".

**Positions vs pods (ADR-093):** shard *positions* (K) and *pods* need not be 1:1 — a pod can host
several co-located positions (`shard_id`-keyed slots). So you can pick K for cache residency and
data-movement granularity, and separately pick how many pods to spread K over
([`kubernetes-deployment.md`](kubernetes-deployment.md); the chart models 1:1 by default).
Entity-anchor routing keeps read fan-out at ~2–5 shards per title regardless of K — fan-out does
not argue against more shards.

## 4. CPU and throughput

CPU is not the constraint: the measured selective path clears the 2,778 titles/s spec target by
**57–255× per core** (results.md §1/§4), and matching parallelizes across titles. Provision CPU
for: the broad lane if your corpus is broad-heavy (watch the ADR-101 counters; the batch endpoint
`/_mpercolate` amortizes it — results.md §9), ingest/compaction background work, and the
coordinator's fan-out merge. A handful of cores per shard node is typically plenty; the
`reverse_rusty_shard_rpc_duration_seconds` p99 (ADR-100) tells you when it isn't.

## 5. The other components

- **Coordinator:** stateless in the remote topologies; memory = the frozen dict + tag space +
  request buffers (small next to shards). Scale horizontally behind a load balancer for HTTP
  concurrency, not memory.
- **Control plane:** tiny (a Raft log of rare membership/assignment ops). The chart's defaults are
  fine; give it stable storage, not size.
- **Disk per shard:** segments ≈ resident index bytes (the mmap'd files ARE the resident data) +
  translog + one compaction transient (≈ the largest segment being rewritten). 2× the shard's
  memory budget in disk is a comfortable start; `persistence.size` in
  [`values.yaml`](../../deploy/helm/reverse-rusty/values.yaml) (default 10 Gi) is the knob.
- **Kubernetes requests/limits:** set `resources.requests.memory` to the shard budget from §1–2
  and the limit modestly above it (the engine's footprint is stable; a limit close to the request
  catches leaks loudly). The chart ships `resources: {}` — set it per environment.

## 6. When the numbers move

The bytes-per-query baseline improves with the planned production optimizations (string interning,
mmap-resident dicts, SoA tightening — the Tier 3 "memory headroom" roadmap item; target 80–140
B/query). This page deliberately quotes rounded anchors and defers to
[`../performance/results.md`](../performance/results.md) — when that file's §5 numbers change,
re-derive K from the formula, don't re-tune prose here.
