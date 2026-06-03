# ADR-021: Durability failures are observable events, not stderr writes

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


- **Context:** The engine emits structured lifecycle events through an optional observer
  ([`EngineEvent`] + `emit()`), which the server translates into `tracing` logs and Prometheus
  counters — the library stays observability-stack-agnostic (no `tracing`/`log` dependency). But the
  *durability failure* paths predated that discipline: ~14 sites across
  `src/segment/{lifecycle,ingest,persistence}.rs` wrote to **stderr** via `eprintln!` (segment
  write/mmap fell back to in-memory; WAL init/append/checkpoint/reset failed; manifest write failed;
  `sources.dat` write/re-map/load failed; a corrupt segment or torn WAL tail was skipped on recovery).
  Each already set the right health flag (`wal_healthy`/`persistence_healthy`, surfaced via `/_health`)
  and took the consistency-preserving action (reject the write, roll the batch back, or fall back to
  memory) — but the *failure signal itself* never reached `--log-format json` structured logs or
  Prometheus. An operator running the server could not **alert** on degraded durability: stderr is not
  scraped, and `/_health` is a coarse liveness gate, not a per-failure counter. The working precedent
  was already in the tree: `EngineEvent::SegmentCleanupFailed` routes a best-effort cleanup miss through
  the observer. Durability failures are strictly *more* important than a leaked file, yet were *less*
  observable.
- **Decision:** Add one structured event, `EngineEvent::DurabilityFailure { op: DurabilityOp, detail:
  String, error: String }`, and route all 14 sites through `emit()`.
  - **`DurabilityOp`** is a `Copy` discriminator (`WalInit`, `WalAppend`, `WalCheckpoint`, `WalReset`,
    `SegmentWrite`, `SegmentMmap`, `SegmentRecovery`, `ManifestWrite`, `SourceStoreWrite`,
    `SourceStoreRemap`, `SourceStoreLoad`, `WalTornTail`, `IngestRollback`) with a stable snake_case
    `as_str()` for metric labels and an `is_data_at_risk()` predicate. Folding the kind into one
    enum-carrying variant (rather than 14 top-level `EngineEvent` variants) keeps the server's match
    arms — and every other observer — small, while still giving operators a precise, matchable label.
    This mirrors the existing `CompactionTrigger`/`FeatureKind`/`ParseErrorKind` enum-as-discriminator
    pattern.
  - **Severity is derived, not stored:** `is_data_at_risk()` returns true for failures that mean match
    data may be lost or was never durably committed (segment/manifest/WAL-append/init, ingest rollback,
    recovery skip) and false for display-only (`_source`) failures and benign WAL housekeeping
    (checkpoint/reset/torn-tail). The server logs the former at `error!` and the latter at `warn!`, and
    increments `durability_failures_total{op}` for both — so alerting rules can page on
    `op=~"segment_write|manifest_write|wal_append|wal_init|ingest_rollback|segment_recovery"` and merely
    record the rest.
  - **Recovery-time failures are buffered.** `with_config`/`open` run *before* an observer can be
    attached (`set_observer` is called after construction), so emitting there would be a no-op. Those
    sites push onto a bounded `pending_events: Vec<EngineEvent>`; `set_observer` drains and delivers them
    synchronously on attach, then clears. The runtime `emit()` path is unchanged (drops events when no
    observer is set, exactly as before) — only construction buffers, so there is no unbounded-growth
    path.
  - The per-site `*_healthy` flags and rollback/​fallback control flow are **untouched** — this change
    only adds an observable signal; it does not alter what the engine *does* on failure.
- **Consequence:** An operator can now alert on degraded durability from metrics alone, and every
  failure (including silent recovery skips) appears in structured logs with a kind, a human-readable
  consequence, and the underlying error. The compiler enforces completeness: `EngineEvent`'s observer
  matches have no wildcard arm, so any future event variant forces both the Prometheus and `tracing`
  paths to handle it. No match semantics change → the differential oracle is unchanged and green; two
  new persistence tests cover a runtime failure (read-only `segments/` → `SegmentWrite` event) and the
  buffer-and-replay (corrupt segment on reopen → `SegmentRecovery` delivered on `set_observer`). Cost is
  contained to `events.rs` (the type), a thin `segment/` wiring layer, and the server observer.
- **See also:** ADR-013 (WAL — the durability mechanism whose failures this surfaces), ADR-016
  (`/_health` exposes `wal_healthy`/`persistence_healthy` from the snapshot — the coarse gate this
  complements), ADR-017 (durable bulk ingest — the all-or-nothing rollback now emits `IngestRollback`),
  ADR-020 (the resident-memory work that introduced the lazy source store whose write/remap/load
  failures are among the routed sites), `STATUS.md` (operational-polish backlog).

