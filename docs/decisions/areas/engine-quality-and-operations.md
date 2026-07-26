# Engine, errors, dependencies & ops decisions

> [Architecture decision hub](../../DECISIONS.md)

Cross-cutting engine APIs, typed failures, dependency boundaries, observability, testing, and delivery mechanics.

| ADR | Decision | Summary | Status |
|---|---|---|---|
| [005](../adr-005-typed-errors.md) | Typed errors over stringly-typed Results | `ParseError { kind, pos }` + `IngestReport` instead of `Result<_, String>` and silent drops — inspectable errors; no `unwrap()` in library code. | Accepted |
| [007](../adr-007-three-production-dependencies.md) | Three production dependencies | Adopt daachorse / roaring / rayon for alias matching, large postings, and data-parallel matching once the std-only design was validated. | Accepted |
| [008](../adr-008-deterministic-data-generation.md) | Deterministic data generation (seeded PRNG) | Seeded SplitMix64 PRNG (no crates) so benchmarks + the oracle are reproducible and adversarial patterns are configurable parameters. | Accepted |
| [021](../adr-021-durability-failures-observable.md) | Durability failures are observable events | Route the ~14 durability-failure sites through a structured `EngineEvent::DurabilityFailure` (op + severity), not stderr — operators alert from metrics/logs. | Accepted |
| [022](../adr-022-runtime-settings-api.md) | ES-style runtime settings API (`/_settings`) | `GET/PUT /_settings` reads the live config lock-free and updates the dynamic subset at runtime with all-or-nothing per-key validation; static keys rejected. | Accepted |
| [023](../adr-023-per-segment-introspection.md) | Per-segment introspection (`/_cat/segments`) | Expose per-segment holes / memory-split / staleness (text or JSON), read lock-free from the snapshot. | Accepted |
| [024](../adr-024-ci-github-actions.md) | CI via GitHub Actions mirroring `check.sh` | CI runs `check.sh` itself (one source of truth); commit the pressure suite + benchmark baseline. Deep exploratory benchmarks remain advisory; ADR-124 adds a pinned, variance-tolerant merge-blocking subset. | Accepted |
| [028](../adr-028-lean-core-feature-gate.md) | Feature-gate the server stack (lean core) | Gate the server/observability stack behind a default-on `server` feature (Cargo-level, zero `#[cfg]`); a lean-core clippy lane keeps server crates out of library code. | Accepted |
| [050](../adr-050-golden-front-end-tests.md) | Oracle front end pinned by spec-authored golden tests | The differential oracle shares the engine's parse/normalize/extract front end (and runs empty-vocab), so a front-end bug would hide; pin those three stages with hand-authored golden tests + a vocab-rich oracle pass. | Accepted |
| [052](../adr-052-external-review-hardening.md) | External-review hardening (batch) | Six small review fixes: reject `-` + space in the parser; reserve 0 in `sig_key` (frozen-table sentinel); apply `max_percolate_batch` to multi-doc `/_search`; bounds-validate segment sections before the unsafe cast; document `timeout_ms` as response-only; default the HTTP bind to `127.0.0.1` + `--host`. | Accepted |

---

Each summary links to the canonical ADR record. Implementation status belongs in
[STATUS.md](../../STATUS.md); documentation placement rules belong in
[the documentation hub](../../README.md).
