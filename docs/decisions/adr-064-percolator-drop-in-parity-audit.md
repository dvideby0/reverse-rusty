# ADR-064: Drop-in percolator parity — audit findings and work package

> [Back to the decisions index](../DECISIONS.md)


- **Status:** **Accepted (2026-06-10) — a program ADR.** Each numbered item below ships under its own
  ADR/PR; this file records the audit that produced them and the acceptance bar. Tracked in
  [`roadmap.md`](../roadmap.md) Tier 4 (the work package) + the polish backlog — **the roadmap copy is
  the live tracker** (completion marks land there); this ADR records the decision-time scope.
- **Context:** A deep **drop-in-replacement audit** (2026-06) against the reference percolator
  deployment documented in [`research/percolator-workload.md`](../research/percolator-workload.md) —
  this time a full semantic + operational gap analysis, not the earlier capability mapping. Method:
  (1) requirements extracted from the reference deployment's actual read/write paths (with their
  application-side matcher's regex-level semantics as the contract); (2) an **empirical parity PoC** —
  real stored queries translated into the RR DSL under a documented **parity configuration**, percolated
  against pinned titles, with ground truth computed by *executing the reference deployment's own
  application-side precision matcher*; (3) an adversarial review hunting false-negative cases in the
  translation rules; (4) live verification of the REST seam semantics against a running server. Result:
  **zero false negatives on every pinned precision-stage-accepting pair** (38/38, plus the exclude-only
  side-list case), every false positive predicted in advance by the translation model — and a set of
  concrete divergences, each now a decided work item below. The full parity configuration + translation
  contract is recorded in [`percolator-workload.md`](../research/percolator-workload.md) §Drop-in parity.
- **Findings → decisions (the work package):**
  1. **`PUT /_doc/{id}` becomes an atomic upsert.** Today a re-PUT inserts a second live copy *without*
     tombstoning the old one — the id keeps matching under **either** version's semantics until an
     explicit DELETE (live-verified: re-PUT then DELETE reports `deleted_count: 2`), and the
     DELETE-then-PUT replace recipe leaves a brief no-match window across two snapshot publishes.
     ES `index` semantics — and the reference write path — are replace-by-id. Decision: PUT tombstones
     prior live copies and inserts the new version under one writer-lock critical section and **one**
     snapshot publish; WAL framing versioned accordingly; decide + document 201-created vs 200-updated.
     (Today's behavior: `segment/ingest.rs` pure insert, `segment/snapshot.rs` multi-live-copy note,
     `handlers/doc.rs` `try_insert_live_with_tags`.)
  2. **Class-D (negation-only) queries get an opt-in accept-and-quarantine lane.** ES/OS `query_string`
     rewrites a pure-negative query to **match-all-except** (`fixNegativeQueryIfNeeded` adds a match-all
     clause; the percolator evaluates such queries blindly per document via `MemoryIndex`), and the
     reference workload *contains* them — "base"/"raw" entities defined entirely by exclusions. RR
     rejects them at ingest (cost class D), which silently diverges. Decision: an opt-in lane (default
     off = today's loud reject) that accepts a positive-less query as an **always-candidate** — a member
     of every title's candidate set, its forbidden features enforced only in exact verification —
     consistent with "never gate on MUST_NOT" (the cover of an empty positive set is the universal
     signature). Rides the broad-lane batching for amortization (the analogue of OS's blind per-doc
     evaluation). Interim integration contract: callers side-list class-D rejections as always-candidates.
  3. **A parity-mode normalizer knob disables the `pop` number-context demotion.** The hard-coded rule
     (a 4-digit 1900–2099 token immediately after `pop` emits `term:N`, not `year:N`) makes number
     typing **position-sensitive** — the *one residual false-negative class* the audit demonstrated in
     both directions against a position-insensitive reference matcher (a query-side year vs a title-side
     `pop`-adjacent year, and vice versa). Decision: a vocab-persisted knob disabling the context rule
     (and evaluate emitting both typings title-side in parity mode — recall-superset, FP-only); default
     = current behavior, byte-identical.
  4. **Non-string tag values fail loud.** Ingest silently drops a non-string tag value
     (`{"tags": {"priority": 7}}` ingests with *no* priority tag and no error) and a filter value array
     silently drops non-string elements, while a scalar non-string filter value 400s — an inconsistent
     surface whose silent half corrupts filtering invisibly (the query becomes unreachable by any filter
     on that key). Decision: reject — or canonically coerce; pick one and document it — on **both**
     paths, ingest and filter arrays.
  5. **The HTTP PUT path wires `maybe_flush`.** `put_doc` → `try_insert_live_with_tags` bypasses the
     only `maybe_flush` call site, so `memtable_flush_threshold` is **inert for REST single-doc writes**
     — memtable + WAL grow until a manual `/_flush` or shutdown. Durability is unaffected (the WAL
     replays on restart) but the knob lies. Straight bug fix.
  6. **Per-request broad control on `/_search`.** `/_search` honors only the server-wide
     `--include-broad` and **silently ignores** an `include_broad` body field (serde unknown-field
     tolerance), while `/_mpercolate` has the per-request override — with broad off, class-C queries are
     silently absent from hits, which reads as missing data. Decision: accept the per-request override on
     `/_search` (and consider rejecting unknown body fields).
  7. **Smaller items (→ polish backlog):** a *measured* reopen/restart-time number at ≥1M queries
     (currently inferred, never captured); a documented + tested **backup/restore** procedure — a *live*
     hot-copy is **not** safe as-is (a concurrent flush/compaction can commit a new manifest and delete
     superseded segments mid-copy, so the copied manifest can reference files the copy missed; the
     procedure needs write-quiescing, a filesystem snapshot, or a file-pinning protocol — designing and
     testing that is the item); an opaque **original-expression passthrough** (store a caller-supplied
     source string verbatim alongside the compiled query and return it with hits, so a consumer whose
     precision stage re-parses the source gets the *original*, not a widened translation — see the
     §Drop-in parity round-trip caveat in `percolator-workload.md`); optional **tag read-back**
     (`GET /_doc` returning a query's tags) for metadata audits. A `should`/`must_not` TagPredicate
     extension is **declined for now**: structurally FN-safe to add later (filters only remove,
     post-candidate), and the known cross-key-OR pattern is covered by two filtered calls + a
     client-side union.
- **Why this is safe:** items 1–2 change write-path semantics behind an explicit opt-in or toward the
  ES-aligned behavior, and neither touches signature gating; item 3 is opt-in + vocab-persisted; items
  4–6 turn silent behavior loud. Nothing alters the lossless-cover contract — the class-D lane *extends*
  it. Each item must keep the full oracle + adversarial suites green and ship with its own tests
  (item 1 needs a re-PUT-narrower-then-percolate pin; item 2 a vacuous-accept differential; item 3
  golden pins for the `pop` cases).
- **See also:** ADR-049/055/059 (the percolator-parity family), ADR-046 (synthetic IDs — the audit
  leaned on both-sides hashing), ADR-026 (the broad lane — the class-D lane's home), ADR-018 (`/_bulk`
  per-item statuses — verified matching the reference bulk contract), ADR-065 (Distributed v1 — the
  cluster-side program), [`research/percolator-workload.md`](../research/percolator-workload.md) (the
  workload + the verified parity contract).
