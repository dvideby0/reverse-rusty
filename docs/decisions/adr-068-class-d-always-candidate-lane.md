# ADR-068: The class-D always-candidate lane (opt-in accept-and-quarantine for negation-only queries)

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted

- **Context.** [ADR-064](adr-064-percolator-drop-in-parity-audit.md) item 2. A **class-D** query has no
  required feature and no any-of group — nothing to anchor a signature on — so the engine rejects it at
  ingest (`InsertOutcome::RejectedClassD`, HTTP 400). But ES/OS accept such queries: `query_string`
  rewrites a pure-negative query to **match-all-except** (`fixNegativeQueryIfNeeded` adds a match-all
  clause), and the Lucene percolator independently has the same concept on its candidate side — a query
  whose term extraction fails is flagged and **evaluated against every document** (the
  `extraction_result: failed` always-candidate bucket). The reference workload *contains* such queries
  ("base"/"raw" entities defined entirely by exclusions), so the loud reject is a real drop-in
  divergence: a caller migrating a stored-query corpus hits 400s on a class of queries the reference
  deployment serves.

- **Decision.** An opt-in lane (`EngineConfig.accept_class_d`, default **off** = today's loud reject)
  that stores a negation-only query as an **always-candidate**: a member of *every* title's candidate
  set, its forbidden features enforced only in exact verification. Five coordinated pieces:

  1. **The universal cover lives in the optimizer, not ad hoc.** `anchor_plan` now returns class D with
     one **empty broad-anchor group** — the cover of an empty positive set *is* the universal signature.
     `build_signatures` hashes it like any group: `sig_key(&[])` (`util::universal_sig()`), the FNV
     basis avalanched — a stable non-zero constant. Because the cover is what the optimizer derives
     (not a side-table), every path that re-derives covers — **compaction re-anchoring (ADR-056), the
     vocab recompile, explain** — reproduces it by construction and can never strand a stored class-D
     entry. The entry rides the existing broad-index machinery: postings, tombstones, the segment
     filter, `.seg` serialization (the v3 format already encodes class D = 3 and reads it back; no
     format change), and compaction remapping, all unchanged.
  2. **The gate stays at ingest, parameterized.** `Segment::add_compiled` takes `accept_class_d`; a
     class-D plan is stored only when the flag is set **and `forbidden` is non-empty**. A query with no
     positives *and* no negatives (an effectively empty query — it would match every title outright) is
     still rejected even with the lane on: ES errors on an empty `query_string` too, and silently
     storing a match-all is an operational footgun, not parity. Live/bulk/build ingest pass the config
     knob; two paths pass `true` **unconditionally**:
     - **WAL replay** — a logged class-D insert was accepted when it was acknowledged; replaying it
       under a since-flipped knob must not drop an acknowledged write (the same trust-the-log rule the
       parse limits already follow: "WAL replay deliberately ignores them").
     - **The vocab recompile** (`set_vocab` blue/green) — a stored query must survive a vocabulary
       change. This also closes a **pre-existing silent-FN hazard**: a query whose positives vanish
       under the new vocab (re-classifying A/B/C → D) was silently dropped from the rebuilt index —
       unmatchable, no error, no counter. Now it is kept as an always-candidate when it still has
       forbidden features (zero FN, bounded FP — the contract's preferred failure mode); only the
       all-empty case still drops.
  3. **The title side probes the universal key once per segment, unconditionally** (within
     `include_broad`): `Segment::match_into`, `MmapSegment::match_into`, and the columnar batch
     kernel's `eval_one_segment` (one `reach(universal_sig)` per batch — the amortization the lane
     exists for). Unconditional because stored entries must stay reachable however the knob is later
     toggled — the knob gates *acceptance*, never *visibility*. When no class-D entry exists the probe
     is one blocked-bloom miss per sealed segment (or one hash miss in the memtable) — only when broad
     is requested, zero when it is not.
  4. **Verification needs no changes — by construction.** `verify`/`verify_slices` on an
     empty-positive entry reduce to exactly the vacuous semantics (mask gate `0 & x == 0` passes,
     required/any-of loops are empty, forbidden checked against `N(T)`), and `eval_batch_slices` is its
     bitmap transpose clause-for-clause. `is_pure_anchor` is structurally false for class D
     (`req_mask == 0` fails `is_power_of_two`), so the skip-verify fast path can never bypass the
     forbidden check, and the batch kernel routes the entry to full bitmap verification.
  5. **Metrics become honest.** `class_counts()[3]` now reports the **stored** class-D count, symmetric
     with A/B/C (it was previously overwritten with the rejection counter as a stand-in, valid only
     while D could never be stored); `rejected_class_d` remains its own counter/field everywhere
     (`EngineMetrics`, `/_stats`, Prometheus). The `/_doc` 400 message now names the knob.

- **Upgrade + rollback are versioned, not vibes (a codex-review catch — both findings real).**
  The first cut replayed every logged insert/upsert as accepted, which is wrong in BOTH directions
  across a binary boundary:
  - **Upgrade (WAL v5: per-frame op markers).** A pre-v5 binary logged a frame *before* classifying,
    so its WAL can hold op-0/op-4 frames whose write was acknowledged as `RejectedClassD`. Replaying
    those as accepted would resurrect a query the caller was told doesn't exist — and the op-4 upsert
    variant would **tombstone the acknowledged-live prior version** (a false negative). Fix: accepted
    class-D writes get their own ops (`InsertClassD` = 5, `UpsertClassD` = 6, payload-identical);
    replay applies op-5/6 as accepted and legacy op-0/4 under the old reject gate — each frame
    reproduces its *writer's* decision, no file-header coupling, mixed files self-describing.
  - **Rollback (segment v4 + manifest v4: a feature-gated fence).** A segment holding a class-D
    entry reads cleanly on a pre-ADR-068 binary — which never probes the universal signature, so
    those acknowledged queries would *silently* stop matching. Such a segment is written as format
    **v4** (layout-identical to v3) — but the segment version alone is NOT the loud gate (a second
    codex catch): recovery's corrupt-segment posture is *skip + event + continue*, so the old binary
    would silently serve without the whole mixed segment. The fence therefore also lives at the
    **manifest**: a commit registering any class-D-bearing segment writes manifest **v4**
    (layout-identical to v3), and an unsupported manifest version fails the old binary's
    `Engine::open` outright — the loud refusal rollback needs. Class-D-free commits keep writing v3
    byte-identically, so rollback stays clean for anyone who never enabled the lane.
    **The fence deliberately does NOT cover an unflushed WAL tail** (a third codex finding,
    triaged and declined): a pre-v5 binary hitting an op-5/6 frame stops and reports the tail as
    skipped bytes (a `DurabilityFailure` event) and continues without it — exactly how a pre-v4/v3
    binary already treats an unflushed ADR-067 upsert / ADR-066 delete frame, the twice-established
    coexistence posture. No code written today can make *yesterday's* binary refuse a newer WAL
    (pre-v5 readers ignore the header version), and the alternative — an eager manifest-fence
    commit on the first class-D accept plus a fence-relax lifecycle tracking WAL-pending state —
    buys little for its complexity when the operator contract for EVERY rollback is already
    **roll back only after a clean flush** (which produces the v4 manifest and trips the fence).
  Both pinned by tests: `legacy_rejected_class_d_frames_replay_under_the_old_gate` (a hand-built
  legacy WAL — the prior version must survive, the rejected frames must not resurrect) and
  `class_d_segments_write_the_v4_rollback_fence` (segment + manifest v4 iff class-D present; both
  v3 byte-identical otherwise; unknown future versions fail loud).

- **Scope.** Single-node (engine + REST). The **cluster keeps rejecting class D** at placement
  (`Target::Reject`) regardless of any shard engine's knob — an always-candidate must be visible to
  every percolate, which under content routing means replicate-to-all placement plus gRPC shipping of
  the accept decision; that work rides the ADR-065 replicate-broad-to-all criterion and ships
  separately (precedent: ADR-055 deferring `set_vocab` on a tagged cluster). The coordinator gates
  before any shard engine sees the query, so a misconfigured shard knob cannot create divergence.
  **Shipped: [ADR-080](adr-080-cluster-replicate-broad-to-all.md)** — the cluster now accepts class D on
  the replicate-to-all broad lane behind `accept_class_d`, with the rollback fence at the v5
  `ClusterManifest`.

- **Why this is safe (the correctness contract).** The lossless-cover contract says: if a title `T`
  *could* satisfy `Q`'s positive semantics, `T` must generate a signature that retrieves `Q`. For a
  class-D query the positive semantics are vacuous — **every** title could satisfy them — so the
  lossless cover of `Q` is a signature every title generates: the universal signature. The lane is the
  contract's natural extension, not an exception to it. "Never gate on MUST_NOT" is untouched —
  forbidden features still never reach an anchor (`anchor_plan` still reads only `required`/`anyof`),
  and the always-candidate's forbidden features are checked only in exact verification, exactly like
  every other query's. False positives are bounded by the broad lane's batch evaluator (the analogue
  of OS's blind per-doc `MemoryIndex` evaluation). Default-off keeps every existing corpus, test, and
  benchmark byte-identical; the only default-on behavior changes are the `class_counts()[3]` semantics
  and the vocab-recompile keep (both strictly more correct, both documented).

- **Testing.** A **vacuous-accept differential** (`tests/oracle/class_d.rs`): corpora seeded with
  forbidden-only queries (`gen.rs::gen_class_d_queries` — a separate opt-in function, the ADR-063
  messy-mode discipline, so every existing corpus stays byte-identical),
  lane on — engine ≡ brute (whose evaluator already computes vacuous truth) per-title **and** batch,
  including tombstone churn, flush → mmap reopen, WAL replay under a flipped knob, compaction (both
  variants), and `set_vocab` survival; lane off — the loud reject pinned (plus the all-empty-query
  reject under lane on). The broad-batch equivalence matrix gains a class-D corpus scenario
  (batch ≡ scalar across columnar/inline × materialize on/off).

- **See also:** ADR-064 (the program), ADR-006 (never gate on MUST_NOT — the invariant this extends),
  ADR-026 (the broad lane), ADR-056 (re-anchoring — why the cover must be optimizer-derived), ADR-065
  (the cluster follow-on home), [`matching.md`](../design/matching.md) §4.
