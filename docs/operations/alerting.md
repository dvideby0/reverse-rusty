# Alerting

What to alert on and why (roadmap Tier 5 M3). The **expressions live in one place** —
[`deploy/prometheus-alerts.yml`](../../deploy/prometheus-alerts.yml), a loadable Prometheus rule
file validated by `promtool` in CI — and this page explains each rule *by name*: what it means,
why that threshold, what to do. Tune thresholds in the yml; the metric inventory itself is in
[`cluster-deployment.md` §9](cluster-deployment.md) (ADR-091/100/101).

> Scope note: the rules select by **metric name only** (`reverse_rusty_*`) — no `job`/`instance`
> selectors baked in. Scrape labels depend on your setup (the Helm chart emits
> `prometheus.io/scrape` pod annotations; the compose network exposes ports 9100/9101 — runbook
> §9). Add your own job scoping if one Prometheus watches several clusters, and alert on scrape
> absence (`up == 0` for your jobs) — a dead target must not read as "no alerts".

## Reading the severities

- **page** — correctness or availability is at risk *now* (durability failure, no leader, shard
  down, fail-loud reads firing).
- **warn** — cost/capacity drift or an operation that didn't finish; investigate within the day.

## Shard rules

### RRShardNotReady
`reverse_rusty_shard_ready == 0` for 5m. The slot is pending (no dict adopted) or lost its state.
On a healthy roll this flickers for seconds; 5 minutes means a shard is not coming back on its
own. **Do:** `kubectl describe pod` / `rrc ps` for the pod state; if the volume is gone, the
DR flow ([`disaster-recovery.md` §3.1](disaster-recovery.md)).

### RRShardCompactionBacklog
`tombstoned_entries > 1M` for 30m — deletes are outpacing compaction. Cost problem first (dead
entries burn memory + scan time), disk-pressure problem later. **Do:** check compaction is
running (single-node: `flush/compaction_total` counters; cluster: the backlog should sawtooth,
not climb), lower the compaction trigger thresholds (`/_settings`, ADR-022), and check disk
headroom — compaction needs transient space ([`sizing.md` §5](sizing.md)).

### RRShardStaleSegments
`stale_segments > 0` for 15m — segments still compiled under an older vocab epoch. A vocab
change (`set_vocab` / alias activation) recompiles synchronously, so sustained staleness means a
rebuild was interrupted. Matching stays zero-FN under the OLD vocab semantics for those segments;
the new vocab's widenings just aren't live there yet. **Do:** re-apply the vocab (`PUT /_vocab` —
idempotent) and watch the gauge drain.

### RRShardPercolateP99High
`histogram_quantile(0.99, …rate(…_bucket{method="percolate"}…))` > 250ms for 10m — shard-side
*service* time (ADR-100), network excluded. In-process p99 is single-digit µs, so sustained
hundreds of ms means the shard is starved (CPU/memory — check
`reverse_rusty_memory_bytes` vs the node, [`sizing.md`](sizing.md)) or the workload went
broad-heavy (next rule). The number to compare against client-observed latency: the gap is
network + queueing (ADR-085 transport metrics).

### RRShardBroadShareHigh
ADR-101 counters ÷ the ADR-100 request count: broad-lane candidates per percolate > 500 for 15m.
The selective path holds ~54 candidates/title *flat*; hundreds-per-title sustained means the
corpus mix shifted toward class-C (broad) queries — a cost regression, not a correctness problem.
**Do:** check the class split (`reverse_rusty_class_queries{class="c"}` — did someone bulk-load
broad queries?), consider `include_broad: false` defaults for callers that don't need the broad
lane, and batch broad-heavy traffic through `/_mpercolate` (the columnar lane —
[`../performance/results.md`](../performance/results.md) §9).

## Control-plane rules

### RRControlNoLeader
`control_leader_known == 0` for 1m on any member. No leader ⇒ no admin writes commit (reads and
matching are unaffected — the control plane is off the hot path, ADR-083). One minute covers a
normal re-election several times over. **Do:** check member health/connectivity; if a majority is
gone, [`disaster-recovery.md` §3.2](disaster-recovery.md).

### RRControlLeaderCountWrong
`sum(is_leader) != 1` for 5m. Zero is covered above (this catches it too); more than one for
minutes means members disagree about terms — a partition symptom. **Do:** check inter-node
connectivity (the mesh ports), then per-member logs.

### RRControlTermChurn
Term advanced > 3× in 10m — repeated elections: a flapping member (crash-looping pod, disk stall)
or lossy network between control nodes. **Do:** find the member whose restarts line up with the
term bumps.

### RRControlApplyLag
`last_log_index − last_applied > 100` for 5m on a member — its state machine is stuck (disk).
Control entries are rare, so ANY sustained lag is anomalous. **Do:** per-member disk health; the
member self-heals on restart from its durable log (ADR-041).

## Coordinator rules

### RRDurabilityFailure
`increase(durability_failures_total[5m]) > 0` — **zero-tolerance page.** A write could not be
made durable (WAL append / segment write / manifest commit failed — ADR-021/051). The engine
fails closed, but the underlying cause (disk full, volume failure) compounds. **Do:** check disk
space/health immediately; do not take a backup onto the same failing disk; once resolved, verify
with a sentinel write and take a fresh backup.

### RRTransportErrors
Shard-RPC errors from the coordinator sustained for 5m — the fan-out is failing **loud** against
some shard, i.e. clients are seeing `502`s rather than silently short results (ADR-072/085).
Effectively "a shard is unreachable" as seen from the routing layer. **Do:** find the failing
endpoint in the coordinator's structured logs; then the shard rules above.

### RRTransportTimeouts
Same posture as errors but deadline-shaped: the shard answers, just not within the per-call
deadline (ADR-085). Usually load (see the p99 + broad-share rules) or network. Warn, not page —
retried reads mask isolated cases; sustained rates precede error pages.

### RRAuthFailures
Rejected bearer tokens > 1/s for 5m: a misconfigured client (rotated token not rolled out) or
probing (ADR-062). **Do:** correlate source IPs in the request logs; rotate the token if it's
probing.

### RRSlowQueries
Searches past `--slow-query-threshold-ms` sustained. The single-node/coordinator-side cousin of
the shard p99 rule — catches slowness the shard histogram can't see (merge, rank, response
assembly). Same triage as RRShardPercolateP99High.

### RRCancellationSpike
Cooperative cancellations (ADR-099) > 1/s for 10m: clients are hitting their `timeout_ms` and the
engine is stopping the wasted work. The client experience is already degraded (408s). **Do:**
capacity (p99/broad rules), or raise the client `timeout_ms` if it is unrealistically tight for
the corpus.

### RRIngestRejects
Sustained parse failures / refused class-D on ingest. Not data loss — rejects are acked as
errors to the writer — but a producer is sending queries the engine won't store. **Do:** sample
the rejects from the ingest logs; if they are class-D (match-everything) rejects, that's the
guardrail working (ADR-068 documents the opt-in lane if those queries are intentional).

## What is deliberately not alerted

- **`wal_size_bytes` / `wal_pending_entries` growth** — checkpoint/flush cadence varies by
  deployment; the durability-failure counter is the actual failure signal.
- **Coordinator memory** — stateless and small; alert at the platform layer if at all.
- **Per-request HTTP latency quantiles** (`http_request_duration_seconds`) — client-side SLOs
  belong to the caller's dashboards; the engine-side signals above localize the cause faster.
