# ADR-052: External-review hardening — parser, signature-key, segment-bounds, request-cap, timeout, and network-posture fixes

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


- **Context:** A 2026-06 external review raised 7 source-level findings. Each was verified against
  code (file:line) and deduped against the docs. Finding #1 (fail-open flush/compaction) is its own
  decision, [ADR-051](adr-051-fail-closed-flush-compaction.md). The remaining six are individually
  small, low-risk, and share one origin (the review), so they are batched into a single hardening
  pass and recorded here (cf. [ADR-048](adr-048-reliability-hardening.md), which bundled three
  reliability items). Two close genuine — if low-probability — correctness/safety gaps (#5, #6); two
  close an intent/amplification gap (#4, #3); two are honesty/posture changes (#2, #7).
- **Decision (one item per finding):**
  - **#4 — Parser rejects whitespace after `-`.** `foo - bar` previously parsed as
    `foo AND NOT "" AND bar` — a negated *empty* term plus a stray *positive* `bar`, silently
    flipping the user's intent. This is harmless to the zero-false-negative contract (an empty
    `MUST_NOT` compiles to zero forbidden features, and forbidden features never gate retrieval —
    [ADR-006](adr-006-forbidden-features-never-gate.md)), but it is a parse-cleanliness bug. A `-` must now
    be *immediately* followed by the atom it negates; a following space errors `TrailingDash`,
    exactly like an end-of-input trailing dash. `-bar` (no space) still negates normally —
    whitespace-significant negation matches the rest of the grammar (`dsl.rs`).
  - **#6 — `sig_key` reserves 0.** The frozen on-disk hash tables (`storage::segment`) use a slot
    key of 0 as the empty-slot sentinel, but `sig_key` is a hash whose range *includes* 0. A query
    whose key hashed to 0 would be invisible in the frozen table — its posting list never retrieved,
    a real match silently dropped (a zero-false-negative contract violation). Probability ~2⁻⁶⁴ per
    distinct key, and frozen-table-only (the in-memory index is a real `HashMap` with no sentinel),
    so it would manifest only after a flush/reopen. `sig_key` now folds 0→1 (`if h == 0 { 1 }`,
    `util.rs`). **Not** `h | 1`: that remaps half the keyspace and would silently change the on-disk
    `.seg` key set (a format break needing a reindex). Because the avalanche fixes 0
    (`avalanche(0) == 0`), the fold only ever perturbs the single input that accumulates to 0 — every
    existing segment's keys are byte-unchanged, so **no format bump / no reindex.**
  - **#3 — Multi-doc `/_search` honors `max_percolate_batch`.** The batch cap was enforced only on
    `/_mpercolate`; a multi-document `/_search` was bounded only by the 100 MB body limit, so one
    large body could schedule millions of parallel matches. The cap now applies to both (reject with
    `400` before any work is scheduled). The inverse gap (`/_mpercolate` lacks `from`/offset
    pagination) is a separate, already-tracked roadmap item and is not changed here. Per-slot
    `slots[*].hits` remain complete-per-slot by design (slots are the per-title observability view).
  - **#5 — Segment section bounds are validated before the unsafe cast.** The mmap typed-slice
    readers (`read_u16_slice`/`read_u32_slice`/`read_u64_slice`/`parse_frozen_index`) built a `&[T]`
    via `from_raw_parts` from a `count`/`cap` read *out of* the file, justified only by the trailing
    CRC. CRC proves byte integrity, **not** that a length is structurally valid — a
    CRC-consistent-but-malformed segment (a writer bug, a torn write that happens to re-pass CRC, or
    tampering) could make the cast construct a slice that overruns the mapping (undefined behavior).
    A shared `checked_section_end` now validates each section's extent (no integer overflow, no
    overrun past the mmap) before every cast, turning that into a fail-loud `InvalidData` error —
    matching the corrupt-segment-fails-loud contract ([ADR-032](adr-032-per-shard-durable-segments.md)).
    Likelihood is low (the engine writes its own segments and CRC catches random corruption), but the
    `unsafe` blocks' SAFETY contracts were *unsound as written* (they asserted in-bounds purely from
    CRC), so this is defense-in-depth plus a corrected invariant.
  - **#2 — `timeout_ms` is documented as a response deadline, not a compute budget.** `/_search`
    and `/_mpercolate` race the matching `spawn_blocking`/Rayon work against a Tokio timeout and
    return `408` on expiry, but the work is **not** cancelled — `spawn_blocking` cannot be
    interrupted and the match path has no cooperative cancellation (it is kept branch-predictable and
    allocation-free by the hot-path invariant). This is resource-only: the work is read-only over an
    immutable `Arc<EngineSnapshot>`, so an abandoned match wastes CPU but cannot corrupt state or
    affect correctness. We document the semantics (response-timeout-only; bound load with the
    request-concurrency limit) rather than thread a deadline through the hot path.
  - **#7 — HTTP server defaults to loopback.** The server hardcoded a bind to `0.0.0.0` (all
    interfaces) with no authentication on its mutating/admin endpoints, while only the *gRPC/control*
    transports' TLS-auth gap was documented. The default bind is now `127.0.0.1` (matching the
    `shardserver`/`controlserver` loopback default) with an opt-in `--host` flag; binding `0.0.0.0`
    is now a deliberate operator choice, for a trusted network or behind an authenticating reverse
    proxy. The REST API still has no built-in auth.
- **Consequence:** Six small, independently-tested fixes. #5/#6 close latent correctness/safety holes
  with no format change (the `sig_key` fold is backward-compatible; the bounds checks reject only
  structurally-invalid segments, which valid round-trips never hit). #4/#3 remove a silent
  intent-flip and an unbounded fan-out vector. #2/#7 align the docs and the network posture with
  reality. The **one outward-facing behavior change** is the bind default: a server previously
  reachable on all interfaces is now loopback-only unless started with `--host 0.0.0.0` — operators
  who relied on the implicit all-interfaces bind must now opt in. **Deferred** (not in this pass): an
  optional bearer-token / API-key gate for mutating endpoints (the heavier half of #7), cooperative
  cancellation on the match path (#2), and `from`/offset pagination + per-slot hit truncation on the
  percolate endpoints (#3's tail).
- **See also:** [ADR-051](adr-051-fail-closed-flush-compaction.md) (#1, durability — separate PR),
  [ADR-006](adr-006-forbidden-features-never-gate.md) (forbidden features never gate — why #4 is harmless),
  [ADR-012](adr-012-mmap-segment-format.md) / [ADR-032](adr-032-per-shard-durable-segments.md)
  (segment format + corrupt-segment-fails-loud — #5), [ADR-026](adr-026-broad-lane-batch-evaluation.md)
  (`max_percolate_batch` — #3), [ADR-011](adr-011-cache-line-blocked-bloom.md) (frozen hash tables — #6)
