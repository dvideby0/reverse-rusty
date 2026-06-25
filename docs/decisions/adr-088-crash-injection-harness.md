# ADR-088: Real-process SIGKILL crash-injection harness (the Phase 0 durability torture)

> [Back to the decisions index](../DECISIONS.md)

- **Status:** **Built + passing (2026-06-25).** A new lean-core bin `src/bin/crashwriter.rs` and a new
  integration suite `engine/tests/crash_injection/` that spawns the bin, SIGKILLs it mid
  durable-operation, reopens the data dir in-process, and diffs the recovered engine against the
  front-end-independent reference matcher (ADR-087). Five scenarios (WAL append / flush / compaction /
  backup / churn), `#[ignore]`d and run by a new `check.sh` `crash injection` lane. Mutation-validated
  3/3 (FN, FP, kill).

- **Context:** This is **Phase 0, item 3** of the reality/adversarial audit — net-new, prioritized
  above every product-roadmap tier, building on item 2's independent oracle (ADR-087) as ground truth.
  Reverse Rusty's cardinal guarantee is **zero false negatives** ([`design/README.md`](../design/README.md)
  §2), including across a crash + restart. The durability design is real — WAL-first writes (ADR-013),
  atomic manifest commit via tmp→fsync→rename (ADR-017), the ADR-066 delete-recovery watermark, the
  ADR-067 atomic upsert — and it is **extensively tested, but only under *simulation***: the persistence
  and cluster-durability suites inject faults by `chmod`-ing a directory read-only, appending garbage to
  a log tail, flipping a CRC bit, or dropping a backing file. None of them actually **kills the OS
  process mid-syscall**. A real `SIGKILL` during a `write(2)`/`fsync(2)`/`rename(2)` is the one failure
  mode the in-process simulations structurally cannot reproduce — the process is never actually torn
  down with in-flight kernel state — and it is exactly what a `kill -9`, an OOM kill, a node power-loss,
  or a `docker kill` does in production. The deploy harness (ADR-072) does `docker kill -s KILL` at the
  *container* level, but only *between* operations, only on the cluster, and diffed against a self-captured
  baseline rather than an independent oracle.

- **Decision:** A **dedicated, ungraceful crash-writer process** driven by an in-process parent that
  kills it and verifies recovery against the independent oracle.

  1. **`crashwriter` — a tiny lean-core bin.** It opens an `Engine` on `--data-dir` and runs ONE
     durable workload (selected by `--workload`), reading its queries from a parent-written
     `id<TAB>dsl` TSV (so the writer and the reference see byte-identical queries with no regeneration
     drift). It installs **no signal handler and no flush-on-exit** — death is ungraceful by design
     (SIGKILL is uncatchable anyway), which is the whole point. Built directly on `Engine` (no HTTP /
     port / async) so the kill tears the real durability seam, not transport machinery; declared as a
     `[[bin]]` so the integration test reaches it via `env!("CARGO_BIN_EXE_crashwriter")`.

  2. **The ACK protocol is the durability signal.** After each mutation the engine *accepts*, the
     writer prints a flushed `ACK <id>` (or `TOMB <id>` for an inserted-then-deleted id). The `try_*`
     mutators run their WAL durability sync (`sync_after_append`) BEFORE returning `Ok`, so a line the
     parent has **read** is a happens-before proof the write is durable — to the OS page cache under
     the default `wal_sync_on_write=false` (which survives a process SIGKILL), or to disk under
     `RR_CRASH_FSYNC=1` (which also survives power loss). A query killed mid insert-or-delete emits
     neither ACK nor TOMB, so it is cleanly **don't-care**.

  3. **The parent delivers a real external SIGKILL.** It reads the ACK stream until a trigger (N acks,
     or the backup workload's `FLUSHED` marker), sleeps a deterministic per-iteration jitter, then
     `Child::kill()` (real `SIGKILL` on Unix — zero new dependencies) and reaps. The verdict **never
     depends on where the kill lands** — the ACK set defines exactly which ids MUST survive; the jitter
     only sweeps the durable window across iterations. The parent asserts it *actually killed* (not a
     graceful finish), so the suite can never silently degrade into a clean-shutdown round-trip.

  4. **Verification is a true differential against the independent oracle.** The parent reopens the
     data dir in-process with `Engine::open` (a clean `Ok` is itself asserted — recovery must not
     error), then diffs every title: **zero false negatives** over the acked-only reference (every
     durably-acked query's matches must be present — the cardinal sin), and **zero false positives**
     over the full-corpus-minus-tombed reference (every engine match must be some real, non-deleted
     query's legitimate match; a recovered surplus in-flight id is allowed, a fabricated or resurrected
     match is not). The references are `reverse-rusty-ref-matcher` (ADR-087), which shares no front-end
     code with the engine — so a crash that recovered *corrupted* state would be caught even if the
     corruption is in the parser/normalizer/extractor.

  5. **Five scenarios, each a `--workload` steering the kill into one durable window:** `wal_append` (a
     WAL frame's write+sync; run in BOTH durability modes), `flush` (a segment write + manifest commit),
     `compaction` (a merge + manifest swap + WAL reset), `backup` (staging + atomic rename — the source
     must reopen fully intact while a torn dest is discarded), and `churn` (insert + interleaved
     `DeleteByLogical` — the delete-recovery path, where a resurrected delete is a false positive).

  6. **Gating.** The scenarios are `#[ignore]`d (they spawn + SIGKILL real processes and do real
     fsyncs, so they are slower and more environment-sensitive than pure in-process tests) and run by a
     new full-gate-only `check.sh` lane (`--test-threads=1` so concurrent kills don't thrash). The lane
     honors `RR_CRASH_ITERS` (small default) so the same code is a quick PR smoke and a nightly soak.

- **Self-validation (the harness must BITE).** A crash test that always passes is worthless, so each
  check was mutation-tested and confirmed to turn the suite RED:
  - **Recovery drops writes** — making `replay_wal_tail` skip 1/3 of recovered inserts produced
    **FN=174** (the zero-FN assert fired).
  - **Delete replay neutered** — skipping `apply_delete_by_logical` on replay resurrected the churn
    scenario's deleted canaries → **FP=16** (the corruption assert fired). To make resurrection
    observable independent of title overlap, the churn corpus prepends self-matching **canary** queries
    that the writer always deletes.
  - **No real kill** — making the parent not kill produced a "writer finished before the kill" failure
    (proving the suite exercises a real SIGKILL, not a graceful round-trip).

  (An "ACK before the durable write" mutation is *not* a reliable bite for this design: the writer's
  loop is sequential, so the WAL `write_all` is the statement immediately after the ack and the
  un-durable window is sub-microsecond. The sync-before-ack ordering is asserted structurally by the
  protocol instead.)

- **Consequences.**
  - The real-SIGKILL-mid-syscall gap is closed for the single-node engine across the WAL, flush,
    compaction, backup, and delete-recovery windows, under an independent oracle — proving the
    durability machinery is real, not plausible scaffolding.
  - Purely additive: no engine-source change; the lean/server/distributed builds are byte-identical
    (the bin is lean-core, the suite is a dev-only test target). No new external dependency
    (`Child::kill()` provides SIGKILL).
  - The default config's `wal_sync_on_write=false` is *process-crash* durable (page cache survives a
    SIGKILL) but not *power-loss* durable; the harness asserts the acked-set guarantee in both modes,
    but true page-cache loss (power loss) remains the domain of the existing torn-tail / CRC
    simulations, which this harness complements rather than replaces.

- **Alternatives considered.**
  - **Self-`abort()` at an injected program point** (rejected as the primary): it runs at a Rust
    statement boundary *after* the current syscall returned, so it does not tear a real `write`/`fsync`/
    `rename` — it is the same fidelity as the in-process fault injection we already have. External
    SIGKILL is the documented gap.
  - **Driving the HTTP server over the wire** (rejected): needs port management, an HTTP client, and a
    readiness dance, and tests the transport rather than the durability seam; the deploy harness already
    covers the container/HTTP angle.
  - **In-process power-loss emulation** (out of scope): SIGKILL cannot drop the page cache, so true
    power loss is not reproducible here; it stays covered by the torn-tail / CRC simulations.

- **Deferred follow-ons.** A **cluster** real-SIGKILL-mid-write leg (kill a `shardserver` *during* a
  write loop, not between ops, in `deploy/harness.sh`); an **upsert** crash scenario (ADR-067's atomic
  replace); and a **crash-loop / multi-reopen** scenario that would exercise the ADR-066 `ensure_seq_after`
  watermark under repeated real kills (the single-reopen churn scenario covers `DeleteByLogical` replay,
  but the post-reopen-append watermark hazard needs a second reopen — today covered by the simulation
  oracle).
