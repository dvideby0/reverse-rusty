# ADR-091: Per-node Prometheus metrics — shard/control `/_metrics`

**Status:** Accepted (2026-06-30)

**Context.** Observability stopped at the coordinator. The single-node / `--cluster` HTTP server
exposes a rich Prometheus `/_metrics` (engine gauges + HTTP/event counters + the ADR-085 gRPC
transport metrics), but the deployable `shardserver` and `controlserver` binaries expose **no metrics
surface at all** — only the `grpc.health.v1.Health` service on `--health-addr` (ADR-084). ADR-084
recorded this explicitly as **deferral (b)** ("health ≠ metrics"). An operator running a remote
cluster is therefore blind to the per-node signals that matter for capacity and incident response:
per-shard stored-query count, resident memory by component, compaction backlog, cost-class
distribution, and — on the manager nodes — Raft term / leadership / log progress / membership. This is
the named prerequisite for any autoscaling signal (roadmap Tier 5 M3).

The enabling observation from the code: **every number is already there and reachable lock-free.**
`EngineSnapshot::metrics()` and `::segment_infos()` / `::class_counts()` read off the shard's
`ArcSwap<EngineSnapshot>` without touching the engine write `Mutex`; `Raft::metrics()` returns a
cheap-clone `watch` of `RaftMetrics`. So this is purely an *exposure* task — no new engine
instrumentation, no hot-path change.

**The dependency constraint.** The `prometheus` crate is behind the **`server`** feature, not
`distributed` ([`engine/Cargo.toml`](../../engine/Cargo.toml)). The shard/control bins are
`distributed`-gated and do not pull `server`. Rather than entangle the features (dragging the
`prometheus` registry + its transitive deps into every distributed build), we follow the project's
lean-dependency philosophy (ADR-028) and the existing std-only `transport_metrics.rs` precedent.

**Decision.** Add an opt-in, per-node Prometheus `/_metrics` endpoint on a SEPARATE plaintext
`--metrics-addr` port — mirroring the ADR-084 `--health-addr` posture exactly (plaintext, pod-local,
non-sensitive, never the TLS + token mesh data port). Unset ⇒ no listener, byte-identical.

- **A new lean, std-only `cluster/node_metrics.rs`** (`distributed`-gated) hand-rolls (a) a tiny
  Prometheus text-exposition renderer (`# HELP` / `# TYPE` / `name{labels} value`, with label-value
  escaping), and (b) `serve_metrics(addr, render)` — a minimal blocking HTTP/1.1 listener on a
  dedicated `std::thread` (non-blocking accept poll + a stop flag for clean test teardown). No new
  dependency, no tokio worker consumed, decoupled from the gRPC runtime. The render closure is
  invoked per scrape, so every gauge is read fresh — no cumulative-counter registry and no
  `EngineEvent` observer wiring on the deploy bins.

- **`ShardServer::metrics_source()`** returns a `ShardMetricsSource` holding a shared clone of the
  server's `Arc<ArcSwapOption<ServerState>>` (the same cell `AdoptDict` writes, so it reports live
  numbers across the pending→adopted flip). Its `render()` reads ONE lock-free snapshot and emits the
  shard gauges; a pending (not-yet-adopted) server reports only `reverse_rusty_shard_ready 0`.
  **`ControlServer::metrics_source()`** returns a `ControlMetricsSource` holding a cheap-clone `Raft`
  handle; `render()` projects `raft.metrics()` through `control_view` onto the control gauges.

- **Wire-name consistency.** A shard *is* an engine, so it emits the SAME `reverse_rusty_*` gauge
  names a single-node server emits (`total_queries`, `base_segments`, `memtable_entries`,
  `dict_features`, `memory_bytes{component}`, `wal_size_bytes`, `wal_pending_entries`,
  `stale_segments`) — existing dashboards work per-pod unchanged — plus shard extras:
  `tombstoned_entries` (Σ segment `deleted` = compaction backlog), `class_queries{class="a|b|c|d"}`
  (the broad lane is class `c`), and `shard_ready` (0/1). Control nodes emit `reverse_rusty_control_*`:
  `term`, `is_leader`, `leader_known`, `state{state="leader|follower|candidate|learner|shutdown"} 1`,
  `last_log_index`, `last_applied`, `voters`, `snapshot_last_index`.

- **The bins** parse `--metrics-addr <ADDR>` exactly like `--health-addr`, capture the metrics source
  BEFORE `serve` consumes the server, and spawn the listener. A bind failure is **fatal** (an explicit
  observability request must not start silently); the listener serves for the process lifetime.

- **Coordinator complement.** The cluster-mode `/_metrics` additionally exposes
  `reverse_rusty_cluster_shard_queries{shard="N"}` from the already-available
  `ClusterEngine::shard_query_counts()` (best-effort, set on scrape) — a cluster-wide per-shard view
  without per-pod scraping, using the existing `prometheus` registry.

- **Deploy artifacts.** The Helm chart gains a `metrics.enabled` toggle + `ports.{shard,control}Metrics`,
  passing `--metrics-addr` to both StatefulSets, declaring the `containerPort`, and adding
  `prometheus.io/scrape|port|path` pod annotations. The production Compose adds `--metrics-addr` to
  every shard/control command (bound on the mesh network, not published to the host).

**Why segments-only shards read `wal_* = 0`.** A cluster shard is a segments-only engine (no WAL,
ADR-032), so `wal_size_bytes` / `wal_pending_entries` are honestly 0 on a shard. That is correct, not a
bug — the live LSM-pressure signal on a shard is `tombstoned_entries` + `stale_segments`. The names are
kept for parity with the single-node engine (where they are meaningful).

**Safety (zero-FN).** Default-off and `distributed`-gated, so the lean / server builds are
byte-identical and a node without `--metrics-addr` is byte-identical to before. The renderer reads only
the lock-free snapshot / the cheap-clone Raft handle — it never takes the engine write lock and is off
every match/ingest hot path, so it cannot perturb correctness.

**Scope-outs (deferred, not silently dropped).**
- **Per-shard p95/p99 latency** — needs per-request hot-path timing hooks; a shard sees only
  successful in-process operations. The coordinator already has RPC-level latency via the ADR-085
  transport metrics. Tracked as the residual M3/M4 metrics item.
- **Broad-lane batch cost** stays coordinator-side (ADR-026 already tracks it there); per-shard gets a
  proxy via `class_queries{class="c"}` (the class-C / broad count), which is free from `class_counts`.

**Consequences / scope.**

- **Closes ADR-084 deferral (b).** The deployable shard + control nodes now expose Prometheus metrics
  on a per-pod scrape target; the autoscaling-signal prerequisite is met.
- **Lean.** Zero new dependencies; the renderer + listener are std-only, gated to `distributed`. The
  one `metrics_snapshot` accessor on `LocalShard` is `distributed`-gated so the lean/server builds see
  no dead code.
- **Proven.** `node_metrics` unit tests cover the renderers (golden substrings, label escaping) and
  the HTTP layer (a bound `:0` listener round-trips `GET /_metrics` → 200 text, a bad path → 404).
  `tests/node_metrics.rs` proves the end-to-end adapter path over the public API: a populated
  `ShardServer` reports `reverse_rusty_total_queries` == the ingested count (numbers are real, not
  zeros) and a pending shard reports not-ready; a real single-node Raft `ControlServer` reports
  `is_leader 1` / `state{state="leader"} 1` / `voters 1` (exercising `control_view` against an actual
  `RaftMetrics`). Helm renders are `kubeconform -strict`-validated across the value matrix; Compose
  passes `docker compose config`.
- **Not in scope:** the per-shard p95/p99 latency + broad-lane cost above; a `harness.sh` curl-`/_metrics`
  assertion (a follow-on); Grafana dashboards / alert rules (the M3 ops-docs item).
