# ADR-105: The always-visible hot tier — frequency-threshold cost reclassification under two-axis placement

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted (2026-07-03)

- **Context.** The Broad-Query Cost Program (its spec:
  [`proposals/broad-cost-program.md`](../proposals/broad-cost-program.md); evidence: ADR-104 + the
  [prior-art survey](../research/broad-scaling-prior-art.md)) diagnosed one corpus-independent
  defect: `is_hot` — membership in the **top-64-by-frequency** common mask — serves two masters.
  As the 64-bit verify-mask assignment it is correctly sized and must stay frozen (`req_mask`
  words are baked into every segment's SoA). As the *cost-classification* predicate it is a rank
  cliff: at a large enough corpus, a feature ranked #65 can carry an arbitrarily fat posting yet
  classify "selective," landing structurally-broad queries in the always-probed realtime lane.
  The per-title selective lane on the 20M broad-bearing bench corpus ran at 13,383 titles/s/core
  vs 437,730 on the broad-free corpus (**32×**,
  [`performance/benchmark-results.txt`](../performance/benchmark-results.txt)).
  Two external reviews of the program spec independently ranked this fix first and mandated the
  **hot tier** shape: cost may move, visibility may not.

- **Decision.** Split `is_hot`'s two roles; give θ-hot-anchored queries a third per-segment
  index, evaluated cheaply but visible always:

  1. **Classification** (`compile/plan.rs::anchor_plan`, now `(ex, dict, theta)`): the verify
     mask is untouched; classification gains
     `is_hot_anchor(f) = is_hot(f) ∨ (θ > 0 ∧ freq(f) ≥ θ)`. A required-branch query whose
     rarest feature is θ-hot **but not top-64** — or an any-of query whose best group has no
     top-64 member but a θ-hot one (the whole group moves; a query lives in exactly ONE index
     per segment) — classifies **class H**, its arity-1 anchors stored in the segment's **hot
     index**. The visibility-affecting boundaries are deliberately **θ-invariant**: class C
     (opt-in broad) still triggers only on top-64 hotness, and the class-B arity-2 pair
     escalation stays keyed to the frozen mask, because its title-side pair loop
     (`{is_hot} × {other}`) must mirror it exactly — extending *that* predicate is lever 3's
     fenced change, not this one's. Enforced structurally in `anchor_plan`, not by review.
  2. **The two-axis placement rule** (adopted from the spec's review outcome as an architecture
     invariant, [`design/matching.md`](../design/matching.md) §4): *visibility* ∈
     {default-visible, opt-in broad, rejected/explicit-universal} × *evaluation* ∈ {realtime
     anchor, columnar hot, columnar broad, universal}. **Cost movement must never imply
     visibility movement.** The hot tier is the first non-trivial cell: default-visible ×
     columnar.
  3. **Match paths.** The scalar path probes the hot index arity-1 with every title feature on
     EVERY request — never `include_broad`-gated — skipped entirely when a segment has no hot
     entries (one branch per segment per title: the tier is structurally free on hot-empty
     corpora, the review's named regression risk). The batch driver instead lifts the tier into
     its **columnar pass** (the ADR-026 machinery, lane-parameterized: `Lane::{Broad, Hot}`
     through the kernel; the universal-signature probe stays broad-only, since class D lives in
     the broad index) — exactly one of the two forms runs, so nothing is double-evaluated.
     `BroadStrategy::Inline` doubles as the hot kill-switch; the ADR-061 multi-word-alias
     two-view forcing routes hot inline exactly like broad. The vacuous accept needed a twin
     predicate: a class-H anchor has **no mask bit**, so it lives in the required *tail*
     (`req_len == 1, req_mask == 0`) and `is_pure_anchor` is structurally false for it —
     `pure_tail_anchor(local, anchor)` (the reaching anchor is known in the kernel loop) makes
     retrieval-is-proof-of-match fire for exactly the single-token population the tier targets.
  4. **The knob.** `EngineConfig.hot_anchor_threshold` (θ; default **0 = off**, byte-identical
     engine), dynamic via `/_settings`; `--hot-anchor-threshold` on `server` and `shardserver`.
     `DEFAULT_HOT_ANCHOR_THETA = 1024` is the recommended value — an **absolute** posting bound
     sitting with wide margin between the two measured 20M populations (selective max main
     posting ~104; the mislabeled fat postings, up to 43,533). The spec's "the roaring-tier
     boundary ~1024" rationale was wrong about the code (`index.rs::ROARING_THRESHOLD` is 256);
     the constant is deliberately NOT tied to the storage tiers. The real corpus refines it
     later (spec §7.2).
  5. **Why θ needs no fence (the safety argument).** A θ change — config drift, a WAL tail
     replayed under a flipped knob, a coordinator/shard mismatch — can only move queries
     between **class A and class H**, and the two are indistinguishable to every consumer:
     both always-visible, both probed on every request, and both place
     `Target::Selective(ring.lookup(anchor))` in the cluster (identical `Target`s — placement
     is provably θ-invariant). So live ≡ replay holds for RESULTS unconditionally; only the
     A/H *counts* can drift, which the oracle asserts explicitly as the benign divergence.
     Consequently: **no WAL op markers** (unlike ADR-068, where acceptance vs rejection
     differed), **no gRPC handshake attestation**, **no title-side change**. The engine
     manifest records θ (v5, forensic — "this corpus's hot entries were classified under
     θ=N"); the live config stays authoritative for new classification.
  6. **Rollback fences (the ADR-068 idiom).** A pre-ADR-105 binary never probes the hot index,
     so hot-bearing data must refuse it loudly: a segment holding class-H entries writes `.seg`
     **v5** (the hot-index section rides the previously-reserved header bytes 72..80; class
     byte 4 = H; open-time validation rejects any class byte past the version's ceiling — the
     old `.min(3)` clamp would have silently counted H as D), and a commit registering one
     writes engine manifest **v5** (single-node recovery skips unreadable segments, so the
     manifest must be the loud half — the ADR-068 reasoning verbatim). Hot-free output stays
     byte-identical v3/v4 (the version ladder picks the highest needed). The **`ClusterManifest`
     is deliberately NOT bumped**: cluster shards attach their registered segments **fail-loud**
     (`open_shared_segments`, no skip-and-continue), so the per-shard v5 `.seg` version word
     alone fences an old binary — pinned both ways by the durability oracle (version stays 5;
     a forged future segment version refuses the whole open). Rejected alternative: an RCMN v6
     θ record — a rebuild-on-rollback penalty purchased for a knob whose divergence is already
     benign.
  7. **Cluster.** `placement_of` gains θ and places class H **selectively** — ring-hashed on its
     non-top-64 anchor(s), exactly like class A; never `Target::Replicated`, so the tier
     structurally avoids the ADR-080 deferred replicated-B-arity-2 shape (a replicated
     always-visible query is re-scanned on every routed shard). Reachability needs **zero
     routing change**: `route()` ring-routes every non-top-64 title feature, and a class-H
     anchor is non-top-64 by definition — with the explicit warning, now a load-bearing
     comment + K-swept oracle, that "upgrading" `route()` to `is_hot_anchor` is the one
     natural-looking edit that would make every class-H query unreachable. Remote shards
     classify locally (`shardserver --hot-anchor-threshold`); the **operator contract** is
     same-θ-everywhere, and divergence is cost-only (a θ=0 shard re-inherits the fat-posting
     scans; proven by the gRPC divergent-server leg). The class-counts wire keeps `counts` at
     exactly 4 and adds the **additive `hot` field** (a pre-ADR-105 reader hard-errors on
     `len != 4`, and the rolling-upgrade order guarantees an old-coordinator × new-shard
     window); `MatchStats` gains six additive hot fields. The autoscaler needs no change —
     class H rides `shard_corpus` and correctly counts toward split pressure (it is ring-placed
     selective load, unlike the discounted replicated C/D).
  8. **Compaction is the migration seam** (ADR-056, extended): the re-anchoring merge — now in
     `segment/merge.rs` — re-derives with θ and moves **A→H** when the anchor's frequency has
     reached θ, and **H→A** only once it has decayed to **≤ θ/2** (the hysteresis margin; an
     anchor wobbling around θ stays put), both directions bounded by
     `hot_migration_max_moves` per merge (a capped merge keeps old, still-correct covers and
     converges over subsequent merges). **{A,B,H}→C is refused** (the ADR-056 demote guard,
     extended to H — the broad lane is opt-in, so the crossing would hide an always-visible
     query) and **C→H is refused** (findability-*adding*, but a silent change to which
     requests see the query; defensive under the frozen mask, since a C anchor cannot lose its
     bit). Requires `compaction_reanchor = true` — structural, not policy: the mechanical
     `compact_from` has no `&Dict` to re-derive with (it now remaps all THREE lanes — dropping
     the hot remap would silently unanchor every class-H entry through an ordinary merge, the
     kind of FN the oracle's compaction legs exist to catch). Fresh ingests classify H at
     ingest with θ alone; the migration serves pre-existing sealed corpora. Cluster shards'
     frozen dict ⇒ stable θ-classification ⇒ re-anchoring stays a no-op there (the ADR-056
     property, preserved).
  9. **Observability.** `MatchStats` gains `hot_{postings_scanned, candidates,
     queries_evaluated, anchors_scanned, batches, prefilter_skipped}` (merged in the ONE shared
     body — the ADR-101 lesson); `class_counts` widens to `[A,B,C,D,H]` with H **appended** (the
     autoscaler and the class-D pins read positionally); `/_stats` gains `class_counts.h` +
     `postings.hot` percentiles; `/_cat/stats` prints the A/B/C/D/H split + the hot posting
     line; `/_compact` reports `hot_promoted`/`hot_demoted`; the ADR-101 shard counters gain
     the `reverse_rusty_hot_*_total{shard}` family (+ the coordinator registry equivalents);
     explain renders class H with the θ-vs-frequency commentary; `bench` prints the H split and
     per-lane hot meters.

- **What the observe-first telemetry found (the measurement that reshaped acceptance).** The
  PR-A `would_be_hot` counter + the 20M baseline capture **falsified the spec §3.1 sizing** of
  the synthetic corpus before enforcement shipped (the counter doing exactly its job):
  the generated broad-intent population is ~130k (~43.5k per shape — `gen.rs` draws the 5%
  branch per *family iteration*), not ~1M; the fat 43,533-entry main posting is the **shared
  class-B arity-2 pair posting of ~43.5k byte-identical "psa 10" queries** — an
  identical-query concentration (the dedup lever's case, spec §5.1, plus the ADR-080 deferred
  B-arity-2 note), NOT a rank-#65 class-A anchor; and the genuine θ-reclassifiable population
  at θ=1024 is `would_be_hot = 782` (Zipf-head players). θ-reclassification does not move
  class B, so **the hot tier alone does not recover the synthetic corpus's 32×** — the top-64
  cliff it fixes is real *by construction* but nearly absent from that specific corpus. Per
  the maintainer's decision, increment 1 therefore ships **paired with dedup Stage A**
  (increment 2's cheap half), and the recovery measurement runs with BOTH in place — the
  combined increment attacks the measured defect honestly. Details + cross-checks:
  the corrected-reading capture block in
  [`performance/benchmark-results.txt`](../performance/benchmark-results.txt).

- **Why this is safe (the correctness contract).** For any title `T` that could satisfy a
  class-H query `Q`: `Q`'s anchors are required-side (or chosen-any-of-group) features of `Q`,
  so `T` contains at least one; the hot index is probed arity-1 with every feature of `P(T)` on
  every request (scalar) or per batch (columnar — the same lossless transpose as ADR-026, with
  the two-view forcing inherited from ADR-061); exact verification is byte-identical (the SoA
  entry is untouched by classification). Forbidden features still never reach an anchor
  (`anchor_plan` reads only required/any-of — the invariant holds structurally for the new
  branch too). Default behavior is byte-identical: θ=0 reproduces the pre-ADR-105 classifier
  exactly, hot-free segments serialize byte-identically, and the empty hot index adds zero
  probes.

- **Proven.** `tests/oracle/hot.rs` — the θ-on differential ≡ brute (per-title + batch, broad
  both ways), the **visibility-invariance** equality (θ-on ≡ θ-off byte-identically on both
  `include_broad` modes; A+H conserved, B/C/D counts θ-invariant), durable v5 reopen (results
  AND counts), θ-flip WAL replay (results identical; the benign count drift asserted), the D5
  mixed-any-of leg, `would_be_hot` == enforced-H (the observe→enforce tie), the margin-gated
  A↔H migration on controlled frequencies (in-band block pinned), the work cap + convergence,
  the C-never-crosses guard, the messy-corpus differential, hot-empty-is-free (zero extra
  probes), and the v5 fence matrix (version ladder, recorded θ, forged class bytes, future
  versions). `tests/broad_batch.rs` — the full equivalence matrix θ-on, the Inline kill-switch,
  materialize on ≡ off **with the fast path proven firing** (the `pure_tail_anchor` trap), and
  the alias forced-inline leg. `tests/cluster_oracle/hot.rs` — K ∈ {1,3,8,16} ≡ single-node ≡
  brute on both modes (the `route()` trap test), class H stored once vs class C ×K, the
  one-shard live placement + broad-off visibility, the cluster θ-flip invariance.
  `tests/cluster_durability_oracle/hot.rs` — durable reopen across K + clog tail, the θ-flip
  reopen, the ClusterManifest-stays-v5 negative pin, the fail-loud forged-segment attach.
  `tests/cluster_grpc_oracle/hot.rs` — the remote differential, the additive wire field, the
  shard hot counters, the θ-divergent-server cost-only leg. Plus the adversarial θ-on
  self-match diagonal and the front-end-independent θ-on sweep (the reference is
  classification-blind — no reference change, which is itself the point). Full `check.sh`
  green; every θ=0 default path byte-identical (the 20-target suite unchanged).

- **Alternatives considered.**
  - *Replicate class H like the broad lane (ADR-080)* — rejected: it inherits the documented
    replicated-B-arity-2 cost bug (an always-visible replicated query re-verifies on every
    routed shard) and buys nothing — the anchor is ring-routable by construction.
  - *Gate the hot tier behind `include_broad`* — rejected outright: the reclassified queries
    were default-visible class A; hiding them is the exact FN the ADR-056 demote guard exists
    to prevent (the naive fix the spec's §3.4 warns against).
  - *WAL op markers / RCMN v6 / a gRPC θ handshake* — rejected as fencing a divergence that is
    correctness-benign by the placement argument (5); the machinery would imply a hazard that
    does not exist. Lever 3 (pair anchors), whose predicate IS match-side-mirrored, is where
    the recorded-snapshot fence becomes load-bearing.
  - *Auto-enable migration whenever θ > 0* — rejected: it would silently switch on the
    ADR-056-opt-in re-anchoring write path; the pairing (θ + `compaction_reanchor`) keeps the
    default compaction byte-identical and the migration an explicit operator action.

- **Deferred.** Per-shard posting-length percentile exposure on `/_metrics` (the single-node
  `/_stats` block ships; a `{shard}` gauge is a follow-on); the C→H relaxation (findability-
  adding, revisit if a real corpus surfaces stranded pre-finalize entries); lever 5b's dense
  candidate bitmap in `reach()` for mega-postings (> 0.40 × segment len), measurement-gated —
  the columnar pass already amortizes posting iteration to once per batch, so the Vespa-lore
  promotion has no cost to remove until `reach()` shows up in a profile.

- **See also:** ADR-104 (the measurement), ADR-026 (the columnar lane this tier rides),
  ADR-056 (the re-anchoring seam + the demote guard this extends), ADR-068 (the fence idiom +
  the lane-addition precedent), ADR-080 (cluster broad placement — the contrast), the program
  spec §5.2/§8, [`matching.md`](../design/matching.md) §4.
