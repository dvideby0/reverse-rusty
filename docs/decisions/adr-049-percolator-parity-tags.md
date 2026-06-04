# ADR-049: Per-query metadata, filtered percolation, and optional ranking (percolator parity)

> [Back to the decisions index](../DECISIONS.md)


- **Status:** **Built (single-node) + oracle-proven (2026-06-03)** — the lead item (decision points 1–3:
  per-query metadata + filtered percolation) is implemented end-to-end on the single-node engine: tag
  interning (`tagdict.rs`), the SoA tag column + verify-stage filter (`exact.rs`), `.seg` v3 + WAL v2
  persistence, the Engine/snapshot filtered match API, and the REST surface (ES `bool`/`terms`/`percolate`
  envelope + a native `filter` block, with ES-style sibling-tag ingest). Proven by the filtered
  differential oracle (`tests/oracle.rs` — zero false negatives/positives + the "filtering only removes"
  monotonicity property), the batch≡scalar-under-filter matrix (`tests/broad_batch.rs`, incl. the
  pure-anchor materialization path), and tagged `.seg`/WAL reopen (`tests/persistence.rs`). Decision point 4
  (ranking + `/_mpercolate` `from` pagination) is **now also built single-node**
  ([ADR-059](adr-059-percolate-ranking-pagination.md)); cluster ranking remains deferred. The single-node server is the drop-in
  target and is complete; threading tags through the **cluster** (in-process coordinator + durable log +
  the `distributed` gRPC wire-format) follows the experimental-path cadence. Originally accepted as a
  design-only direction — the framing below preserved the correctness contract *by construction* through
  the build.
- **Context:** Reverse Rusty matches titles to stored queries and returns a bare set of matched
  `logical_id`s. Real percolator deployments — captured abstractly in
  [`research/percolator-workload.md`](../research/percolator-workload.md) — do more: each stored query
  carries **structured metadata** (a category, a status, secondary keys), and the **dominant read
  pattern is "percolate, then narrow to one category"** (sometimes one status), occasionally followed by
  ranking. Today a caller wanting "matches in category X" must run a separate metadata store or
  post-filter outside the engine — and RR does not even retain the metadata to filter on. Three gaps
  follow: (1) no per-query metadata; (2) no result filtering by metadata; (3) no scoring/ranking, and
  only partial pagination (`/_search` has `from`/`size`, `/_mpercolate` is size-only). (1)+(2) is the
  high-value pair (the dominant pattern); (3) is lower-priority — in the reference workload only a
  public search surface ranks, the core matching jobs do not.
- **Decision:**
  1. **Metadata = interned integer tags in the SoA.** A stored query may carry a small set of
     `(key,value)` tags; intern each to a dense `TagId` at compile time (as features become `FeatureId`s,
     `dict.rs`) and store them as one more exact-match SoA column (`tag_off/tag_len` → sorted `tag_blob`),
     persisted in the `.seg` format. No strings on the match path.
     ([`design/matching.md`](../design/matching.md) §5.1; [`design/ingestion-and-updates.md`](../design/ingestion-and-updates.md) §11.)
  2. **Filtered percolation pushes into verify (baseline).** A request-supplied tag predicate (a
     conjunction of "key ∈ {values}") compiles to required `TagId`s and is checked during exact
     verification of each candidate — a sorted-slice test reusing the existing SoA cursor.
     Tag-partitioned segment-skip is a **deferred** optimization for the dominant single-key filter, and
     must be filter-driven + fail-open. ([`design/matching.md`](../design/matching.md) §5.2, §5.5.)
  3. **Tags are checked only post-candidate, never in signature gating.** Structurally identical to
     "forbidden features never gate" (ADR-006): signatures stay built only from required features +
     any-of groups. ([`design/matching.md`](../design/matching.md) §5.3.)
  4. **Ranking is an optional out-of-core layer.** Matching stays boolean and complete; ranking sorts the
     already-final `Vec<u64>` by a priority tag and/or a request boost, and applies top-K / `from` (also
     closing the `/_mpercolate` pagination gap). It touches neither the candidate index nor the verifier.
     ([`design/matching.md`](../design/matching.md) §5.4.)
  5. **Identity is reused, not added.** The caller-supplied `logical_id: u64` already serves as the
     entity foreign key the workload needs; no separate id field.
- **Why this is safe (no false negative):** the lossless-cover contract
  ([`design/README.md`](../design/README.md) §2) is about which signatures a *title* generates to retrieve
  queries; tags never participate in that, so the cover is unchanged. A tag filter only *removes* queries
  the caller did not request — it cannot drop a query the caller wanted, so it adds no false negative
  within the requested tag scope. Ranking runs after verification and changes only order, never
  membership. This mirrors the MUST_NOT invariant (ADR-006) exactly, which makes the safety argument
  **structural** rather than test-dependent.
- **Scope / remaining design questions (for the build):** the request grammar for tag predicates and the
  REST surface (a `filter`/`rank` block on `/_search` + `/_mpercolate`, [`reference/api.md`](../reference/api.md));
  the `.seg` format's versioned tag section + backward-compatible read; whether a metadata/status-only
  update can rewrite just the tag column (a workload-aligned optimization) vs re-compile; and the eventual
  tag-partitioned segment-skip. None affect the correctness contract.
- **Alternatives declined:** *post-match external filter* (return all, filter outside) — what callers do
  today; still verifies everything and needs an external store, strictly worse than pushdown once tags
  are in the SoA. *Tags as part of the signature/anchor* — rejected outright: it couples a caller filter
  to the cover proof and risks dropping wanted matches (the MUST_NOT lesson). *Scoring inside the
  verifier* — rejected: it would entangle a presentation concern with the boolean core; the workload
  shows ranking is a separate, optional surface.
- **Consequence:** once built, "percolate then narrow by category" — the dominant production read
  pattern — is a single in-engine call with an integer-only filter on the hot path, no external metadata
  lookup, and the zero-false-negative guarantee intact. The engine moves from a bare matcher toward
  percolator parity while keeping its core invariants. Until built, this ADR is the spec and the engine
  is unchanged.
- **See also:** ADR-006 (forbidden-never-gates — the invariant this mirrors), ADR-001/002 (semantic
  signatures + integer-exact verify — the two-stage subsumption), ADR-026 (`/_mpercolate` batch — where
  pagination/ranking attach), ADR-046 (the feature dictionary whose interning is reused for tag ids).
  Design: [`design/matching.md`](../design/matching.md) §5,
  [`design/ingestion-and-updates.md`](../design/ingestion-and-updates.md) §11. Workload:
  [`research/percolator-workload.md`](../research/percolator-workload.md). Roadmap:
  [`STATUS.md`](../STATUS.md) Tier 4. Would-be code sites (when built): `src/dict.rs` (tag interning),
  `src/exact.rs` (the tag SoA column + verify-stage filter), `src/compile.rs` (carry tags through
  compile), `src/storage/segment` (the `.seg` tag section), `src/bin/server/` + `reference/api.md`
  (the filter/rank request surface).
