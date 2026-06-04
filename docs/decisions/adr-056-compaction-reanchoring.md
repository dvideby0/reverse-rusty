# ADR-056: Compaction-that-improves — re-anchoring drifted queries during a merge

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted

- **Context.** A query's *anchor* — the signature/posting list it is retrieved under — is chosen at
  compile time by `anchor_plan(ex, dict)` as a function of feature frequency: the rarest required
  feature (or the most-selective any-of group). In the single-node engine every `insert_live` /
  `bulk_ingest` bumps global frequencies (`Dict::bump_freq`, no finalized guard), so over time a
  query's original anchor can drift to a *more common* feature while a different required feature
  becomes the rarer, more-selective one. Until now `Segment::compact_from` merged segments by
  **mechanically remapping** the old signatures forward — it never re-ran `anchor_plan` — so this
  drift was never repaired: hot postings kept growing and per-title candidate fan-out stayed higher
  than necessary. The roadmap's Tier-2 item "compaction-that-improves"
  ([`ingestion-and-updates.md`](../design/ingestion-and-updates.md) §7.3) is exactly this repair:
  *"a query whose anchor went hot gets a fresh, more-selective signature cover … repaired lazily and
  locally, never by a global rebuild."*

- **Decision.** Add an **opt-in** compaction "improve" pass that re-anchors drifted queries by
  re-deriving each alive query's signature cover with the *current* frequencies, reusing the existing
  optimizer verbatim:
  - **`Segment::compact_from_reanchored(sources, dict)`** (`segment/seg.rs`): merges like
    `compact_from` but, instead of remapping old postings, reconstructs each alive query's
    `(required, anyof)` from the merged exact-store SoA and re-runs `build_signatures(ex, dict)`,
    inserting the fresh `main`/`broad` postings and class. Returns the merged segment plus a
    `reanchored` count (queries whose cover actually changed).
  - **`ExactStore::anchoring_inputs(id, mask_inverse)`** (`exact.rs`) decodes the stored cover:
    masked-required features (kept only as set bits in `req_mask`) are un-masked via
    **`Dict::mask_inverse()`** (`dict.rs`, bit→feature, derived from the frozen mask), combined with
    the non-masked tail and the directly-stored any-of groups. Forbidden is deliberately not
    decoded — `anchor_plan` never reads it.
  - **`EngineConfig::compaction_reanchor`** (default `false`): `do_compact_range` branches to the
    re-anchored variant when set, else the byte-identical `compact_from`. The `reanchored` count
    rides `CompactionReport` → `EngineEvent::Compaction` → the `/_compact` response and structured
    logs.
  - **Three load-bearing obligations.** (O1) entries are processed in ascending old-local-id order
    and each entry's fresh sigs inserted at its (ascending) new id immediately, so postings stay
    sorted by construction (the append-only invariant). (O2) the exact-store entry is copied
    **verbatim** (`copy_entry`) — only postings and the class vector are re-derived — so `verify` /
    `is_pure_anchor` are byte-identical and forbidden features are preserved (rebuilding the SoA from
    the decoded `ex` would drop forbidden ⇒ false positives). (O3) un-masking relies on the mask
    being **frozen**; a query built before the mask was finalized has `req_mask == 0`, so the
    un-masking loop is a natural no-op.

- **Why it is FN-safe (the load-bearing property).** Re-anchoring **cannot introduce a false
  negative** because the new cover is produced by the *same* `anchor_plan` the title side
  (`Segment::match_into`) is matched against, using the *same* dict. A matching title contains all of
  a query's required features (AND-semantics) and ≥1 member of each any-of group, so whatever anchor
  the optimizer re-derives, the title generates the signature that retrieves it: arity-1 per feature;
  arity-2 for the class-B `{hot}×{other}` pair (the escalation always pairs on a hot feature, which
  the title's arity-2 loop emits); broad arity-1 for class C. The anchor choice only governs *which*
  posting list a query lives in; the exact-store data is untouched, so the match set is identical.
  Each segment (and the memtable) is probed independently with the title's full signature set, so a
  re-anchored segment coexisting with non-re-anchored ones is safe. Proven by
  `tests/oracle.rs::compaction_reanchoring_preserves_correctness` (a controlled drift forces a
  guaranteed anchor flip; pre == post == brute oracle across class-A flip / any-of / forbidden /
  broad shapes) and `::compaction_reanchoring_matches_oracle_at_scale` (a realistically-drifted 30k
  corpus re-anchors ~15% of queries with zero FN, per-title **and** through the columnar broad
  batch path).

- **Scope and the frozen-mask limitation (honest).** Re-anchoring works *within* the frozen 64-hot
  common mask. The mask is frozen for the engine's life after the first `finalize_mask`
  ([`ingestion-and-updates.md`](../design/ingestion-and-updates.md) §8: minor versions interoperate
  via a frozen mask so exact-match masks stay comparable across segments). Re-ranking the hot set
  itself remains a **major-version blue/green re-materialization** concern (out of scope here).
  - **Correction to the initial design intuition:** the cost class is *not* invariant. With the mask
    frozen, a feature's *frequency* and its *hotness* diverge as the corpus drifts (a feature can
    reach high frequency yet never gain a mask bit). So a query's rarest-by-current-frequency
    required feature can now be a hot one, escalating it from an arity-1 class-A cover to a
    more-selective arity-2 class-B cover (A→B) — which is precisely the repair. This stays lossless
    by the same matched-pair argument above. A query is never re-anchored to class D (a stored query
    always has a required/any-of feature; `debug_assert`'d).
  - **The one refused transition — main→broad (the demote-guard).** A main-lane (A/B) query is
    **never** demoted into the broad (C) lane. The main index is probed on every percolate, but the
    broad lane is opt-in (the default path is `include_broad = false`), so moving a query main→broad
    would hide it there — a false negative. This crossing happens only when a query's *sole* anchor
    has become hot — e.g. an entry compiled *before* `finalize_mask` (an `insert_live`-then-`flush`
    on an empty engine), still sitting in main with `req_mask == 0`, whose lone feature a later
    `bulk_ingest` makes hot. That is a hotness reclassification, exactly the major-version blue/green
    case this ADR defers — not a silent compaction change — so such an entry keeps its original
    cover. (The reverse, broad→main, only adds findability and is kept.) Caught by
    `tests/oracle.rs::reanchoring_never_demotes_a_main_query_into_the_broad_lane`, which matches with
    `include_broad = false`. *(Found by the Codex pre-PR review; the initial matched-pair argument
    implicitly assumed the broad lane was always probed.)*

- **Cluster safety = no-op by construction.** A cluster shard indexes against the ONE frozen shared
  `Dict` and **never bumps frequency** (the `extract_readonly`/`ingest_extracted` path), so
  `build_signatures(reconstructed_ex, frozen_dict)` reproduces the original cover exactly →
  re-anchoring is a guaranteed no-op (`reanchored == 0`) and can never change a query's shard
  placement or within-shard retrievability. The feature is therefore single-node-only in effect; the
  cluster leaves the knob off. Proven by
  `tests/oracle.rs::reanchoring_is_a_noop_under_a_frozen_dict`.

- **Alternatives.** (1) *Re-extract each query from its source text* (the `recompile_stale_segments`
  pattern) — rejected: it depends on `query_store`, re-parses/re-normalizes, re-runs equivalence
  expansion, and would mis-rebuild an old-version entry from a logical's *latest* text if a logical
  ever had multiple live versions. Decoding the already-indexed SoA is faithful to exactly what is
  stored, integer-only, and avoids all of that. (2) *Union the new cover with the old* (keep both
  anchors) — rejected as unnecessary: losslessness comes from `anchor_plan`/`match_into` being a
  matched pair, not from retaining stale signatures, and the union would defeat the selectivity win.
  (3) *Re-rank the hot mask during compaction* — rejected: the mask must stay comparable across
  non-merged segments and the memtable; re-ranking it is a global, blue/green operation, not a local
  merge.

- **Why opt-in / default-off.** A first cut of a write-path/segment-layout change touching the
  zero-FN core ships behind a knob (like the broad-lane kill-switches) so the default compaction path
  stays byte-identical and the change is fully reversible. Flipping the default on (the design's
  eventual intent, "compaction that improves") is a fast-follow after the oracle + a real-corpus
  validation pass.

- **Testing.** `dict.rs` units (`mask_inverse` round-trips `mask_bit`; empty before finalize);
  `tests/oracle.rs` — the controlled-drift flip (asserts re-anchoring *fired*, all shapes lossless),
  the at-scale realistic-drift oracle (per-title + columnar batch), and the frozen-dict no-op.
  Full `check.sh` green (incl. the cluster + distributed oracles, unaffected).

- **Consequences.** Compaction now repairs frequency drift lazily and locally — drifted queries move
  onto more-selective anchors during a merge that was happening anyway, shrinking hot postings and
  per-title candidate fan-out — closing the concrete Tier-2 "compaction-that-improves" item. The
  zero-false-negative contract is preserved structurally; the default path (and the entire cluster
  path) is byte-identical. The remaining §7 "improve" menu (candidate-survival telemetry,
  `recommended_shard_count`/`recommended_arity`, feature-ID re-ranking for locality, re-running the
  corpus learner) and hot-mask re-ranking via blue/green stay deferred, each its own increment.
