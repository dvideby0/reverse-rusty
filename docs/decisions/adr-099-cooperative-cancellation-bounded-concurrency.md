# ADR-099: Cooperative match cancellation + bounded search concurrency (Tier 5 M3)

> [Back to the decisions index](../DECISIONS.md)

- **Status:** **Done (2026-07-02).** The ADR-052 deferral ("cooperative cancellation on the match
  path — weigh a coarse per-segment deadline check against simply bounding concurrency"), closed
  with **both**, as one combined design.

- **Context:** `timeout_ms` on `/_search` / `/_mpercolate` was a **response deadline only**:
  `tokio::time::timeout` raced the `spawn_blocking` future, so on expiry the client got its 408
  but the match work kept burning the rayon pool to completion. There was also **no bound on
  concurrent match work** (the tower `ConcurrencyLimit(256)` caps HTTP requests of every kind,
  not pool occupancy; the in-flight gauge is advisory). The compounding failure: under a burst of
  expensive searches, queued jobs whose clients already timed out still execute in full when
  dequeued — the pool ends up 100% busy on dead work, starving live requests. Prior art is
  exactly this pair: Lucene/ES (`TimeLimitingCollector` checks at collector/segment boundaries +
  a bounded search pool) and Postgres (`statement_timeout` via `CHECK_FOR_INTERRUPTS` at safe
  points).

- **Decision:**
  1. **A monomorphized deadline seam in the match bodies** (`segment.rs`: `trait DeadlineCheck`
     with `NoDeadline` / `DeadlineAt`). `MatchView::match_title` and the broad-batch
     `match_batch_chunk` are generic over it; the **unarmed monomorph's error type is
     `Infallible`**, so the compiler erases every check and every `Err` arm — the
     byte-identical-default claim is **structural, not empirical** (and the bench INVARIANTS
     reproduce). Checks sit at COARSE boundaries only — entry (a match that spent its whole
     budget queued dies before doing any work), each base-segment probe, the memtable probe, each
     Phase-0 title, each columnar segment block — never per candidate (the hot-path invariant).
     This is **bounded staleness, not preemption**: worst case, one segment's work runs past the
     deadline. `exact.rs`, `BatchMatchOptions`, `trait Shard`, and `ClusterEngine` are untouched.
  2. **Typed, partial-proof cancellation.** Armed matchers return
     `Result<_, MatchCancelled>`; every cancelled path **clears its output buffer before
     returning**, and the par/batch collects short-circuit the whole request — a cancelled match
     can never masquerade as a successful empty/short result, and `/_mpercolate` never returns a
     partially-filled `responses[]` (a missing slot is indistinguishable from an empty match set —
     the fail-loud rule). New `EngineSnapshot::try_match_title_filtered` /
     `try_match_titles_par_filtered` / `try_match_titles_batch_with_stats_filtered`
     (`deadline: Option<Instant>`; `None` delegates to the unarmed path).
  3. **The outer contract is UNCHANGED**: 408 on timeout, results discarded — cancellation only
     stops the wasted work. A cooperative cancellation racing ahead of the tokio timer maps to
     the **same** 408 arm (never an empty 200). Partial-results-with-a-`timed_out`-flag was
     REJECTED: a partial union is a false-negative-shaped response — anti-contract (the same
     reasoning as ADR-085's fail-loud reads).
  4. **The arming rule: explicit `timeout_ms` only**, gated by a **dynamic kill-switch**
     (`EngineConfig.cooperative_cancel`, default `true`, updatable via `PUT /_settings` — the
     `broad_columnar` precedent). Deriving a deadline from the implicit 30 s default would put
     clock reads on every unarmed title; explicit-arm keeps the default path free of them and
     gives cancellation to exactly the requests that declared a budget. (Arming default-timeout
     requests is a recorded follow-on.)
  5. **Bounded search concurrency**: `--max-concurrent-searches N` (default 0 = unbounded,
     byte-identical). `Some` ⇒ `/_search` + `/_mpercolate` (both modes) **wait** on a
     `tokio::sync::Semaphore` INSIDE the existing timeout race — a request that never gets a
     permit 408s at its own deadline and its dropped acquire consumes nothing. The permit is
     moved **into** the `spawn_blocking` closure, so it is released when the blocking work
     actually ends (not when an abandoned join handle drops at response-timeout) — the semaphore
     reflects true pool occupancy, and cancellation is what recycles permits fast under overload.
     Server-scoped CLI flag (like `--threads`; a tokio semaphore cannot shrink at runtime —
     resize deferred).
  6. **The cluster path** (`percolate_blocking`): a handler-local `enum PercFail { Shard → 502,
     Cancelled → 408 }` adds a **per-title** deadline check to the rayon fan-out — a shard
     failure is never masked by cancellation (each title maps to its own variant). Within one
     title, a remote shard's work is already bounded client-side by the ADR-085 per-RPC deadline;
     **shard-server-side** cancellation (a wire deadline + shard-local checks) is the stated
     deferral, as is threading a deadline through `trait Shard` (blast radius across
     Local/Replicated/Remote/Handoff + proto).
  7. **Observability**: `match_cancellations_total{endpoint}` — incremented inside the blocking
     closure, so it counts even after the handler already answered 408 (the "work actually
     stopped" signal, distinct from `http_requests_total{status="408"}` which also counts
     un-armed response-deadline timeouts) — and `search_permits_in_use`. No `EngineEvent`: the
     typed error is the channel and the handler is the caller (the ADR-021 rule).

- **Proven.**
  - `tests/error_paths.rs::cancellation` — the zero-FN guard: armed-but-unexpired (far-future
    deadline) is **byte-identical** to the infallible paths (ids AND `MatchStats`, per-title /
    par / batch × broad on/off); expired-at-entry errs near-immediately with an **empty** output
    (pre-seeded junk cleared, never returned).
  - `tests/stress/cancellation.rs` — **proves-work-stopped**, self-calibrating: over a
    broad-heavy corpus (600k queries, 80% broad, inline strategy; uncancelled ≈ 0.5–0.6 s), a
    deadline of `T_full/20` errs in ≪ `T_full/4` (measured ~30 ms vs ~540 ms) on both the batch
    and the parallel paths — machine speed cannot flake it, only cancellation failing to cancel.
  - Handler tests (both modes): an explicit `timeout_ms: 0` **408s and records the cancellation
    counter** (deterministic — the deadline is expired before the closure's first check); no
    explicit `timeout_ms` ⇒ never armed, counter stays 0, serving unchanged; **one permit +
    two concurrent searches** ⇒ both queue and succeed, `search_permits_in_use` returns to 0;
    cluster: the cancelled multi-doc percolate 408s + counts while unarmed serving stays intact.
  - The full oracle suites run unchanged (no result-affecting path was touched); bench
    INVARIANTS reproduce (the unarmed default carries zero deadline reads).

- **Alternatives considered.**
  - **Deadline-only** — stops per-request overrun but N concurrent runaways still pin the pool
    for their full budgets. **Semaphore-only** — bounds how many, but every admitted runaway
    still burns to completion after its 408; queued-then-dequeued jobs are wasted from the first
    instruction. Combined: the semaphore bounds *how many* jobs exist, the deadline bounds *how
    long a dead one lives* (including spent-whole-budget-queued — the entry check fires on
    dequeue).
  - **`Option<Instant>` parameter instead of the monomorphized trait** — rejected: puts a branch
    on every boundary of the unarmed hot path and makes "no cost by construction" an empirical
    claim instead of a structural one.
  - **Partial results with `timed_out: true`** — rejected (fail-loud; see Decision 3).
  - **429-on-saturation instead of queueing** — rejected for v1: the semaphore wait is naturally
    bounded by each request's own timeout; a bounded-queue/429 QoS layer is deferred.

- **Deferred follow-ons.** Shard-server-side gRPC cancellation (wire deadline + shard-local
  checks); a deadline through `trait Shard::percolate_filtered`; cross-request QoS /
  bounded-queue-429; runtime resize of the permit count; arming implicit-default-timeout
  requests.
