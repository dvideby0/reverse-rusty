# ADR-103: Match-feedback alias validation (behavioral evidence for candidates)

> [Back to the decisions index](../DECISIONS.md)

- **Status:** **Accepted (2026-07-02).** Opt-in passive capture of the live title→query match
  stream, aggregated into per-candidate-pair behavioral evidence
  (`GET /_vocab/aliases/feedback`), stamped into the registry by
  `POST /_vocab/aliases/validate_and_apply` (activation stays explicit).

- **Context:** The Tier 2 roadmap item; technique 2 of
  [`research/corpus-feature-learning.md`](../research/corpus-feature-learning.md) §5 — "if titles
  that say `UD` and titles that say `upper deck` satisfy the *same* query sets, that's strong,
  behavioral evidence of equivalence. Self-supervising and high-precision." ADR-102 (and the
  ADR-060 learners) produce *candidates*; nothing produced *evidence* — a reviewer stared at a
  pair with a similarity number and guessed. The engine is itself the title→query join point:
  every percolate response is a labeled event. No feedback infrastructure of any kind existed
  before this ADR.

- **Decision:**
  1. **Passive capture, opt-in, default OFF.** Two dynamic `EngineConfig` knobs
     (`/_settings`): `alias_feedback_capture: bool = false`,
     `alias_feedback_max_pairs: usize = 256`. When on, the single-node `/_search` +
     `/_mpercolate` handlers feed the aggregator post-match — inside the handler's
     `spawn_blocking` closure, after the ids are known, never inside the engine's match path.
     Default-off ⇒ byte-identical responses and zero added work (the hook is behind the knob).
     An explicit stage-two `POST /_feedback` endpoint (confirmed-match events from the
     downstream filter) is the named deferred extension — it would call the same
     `observe(title_tokens, matched_ids)` seam.
  2. **Validation-of-candidates, not open-ended discovery.** The tracked universe = registry
     entries with `status == Candidate` and exactly two forms (n-form groups deferred), capped
     at `alias_feedback_max_pairs` (confidence desc, forms asc — deterministic), re-synced on
     every snapshot publish (`AppState::publish_snapshot` — the vocab epoch is NOT a sufficient
     dirty signal, because ADR-102's metadata-only install records candidates without bumping
     it), and the sync itself is **gated on the capture knob** so the default-off contract is
     zero-work on the write path — no lock, no registry scan (codex review); flipping the knob
     re-syncs at the next publish. Bounds cardinality by construction and matches the roadmap word: *validation*.
  3. **Bounded evidence: oversampled bottom-k sketches** (`vocab/alias/feedback.rs`,
     std-only). Per pair: `titles_a/b/both` counters + two bottom-k signatures of matched query
     ids — the smallest `splitmix64(id)` values (NOT the engine's FNV — id-assignment patterns
     in its low bits would bias the sample). The raw sketch keeps **4× the sample size**
     (1024 raw for a k = 256 Jaccard sample, stderr ≈ 0.06 against a 0.5 threshold) so the
     report-time exclusion filters BEFORE the sample truncates (codex review: filtering an
     already-capped sketch lets heavily form-referencing populations starve the survivors
     below `min_queries` despite plenty of clean evidence — oversampling keeps the filtered
     bottom-k exact up to 75% exclusion, and beyond that the survivors remain a valid smaller
     sample). Order-independent (deterministic under request interleaving), exact below
     capacity, fixed memory (≤ ~16 KiB/side ⇒ ~8 MiB at the default cap). Title-side classification is by **contiguous
     token run** over `corpus::tokenize` output (multi-word forms = an adjacent run; token
     equality, never substring — `ud` ⊄ `stud`); a title containing BOTH forms is excluded
     (counted `titles_both` — no discriminating signal). Evidence is **not persisted** — a
     rolling operational signal, reset on restart or `POST /_vocab/aliases/feedback/reset`.
  4. **The degenerate-evidence exclusion.** At report time, sketch members whose query source
     text references either form (contiguous-token test over the DSL text; unresolvable ids
     conservatively excluded) are dropped before the overlap is computed. Why: a query
     *requiring* `ud` matches `ud`-titles and structurally cannot match `upper deck` titles
     pre-activation — mechanically depressing a true alias's overlap — while a query already
     bridging the pair through an active equivalence inflates it; the exclusion removes both
     distortions. Report-time keeps capture cheap and is statistically sound (the k smallest
     hashes surviving a filter are a bottom-k sample of the filtered population).
  5. **What the signal can and cannot reject — the pipeline composition.** Overlap of the
     surviving (non-form) matched-query populations validates *demand equivalence*. A pair
     whose forms satisfy disjoint demand is rejected (overlap ≈ 0). An **identical-demand
     co-hyponym** (psa/bgs titles of the same products match the same player queries) would
     PASS this test — which is exactly why the pipeline composes: such pairs are syntagmatic
     and the ADR-102 **co-occurrence penalty keeps them out of the candidate set** (they are
     never tracked), and activation remains gated. Stated plainly in the docs rather than
     oversold: feedback validates demand-equivalence of *discovery's survivors*; discovery's
     paradigmatic filter is the co-hyponym gate; the reviewer is the final gate.
  6. **Validated** = Jaccard overlap ≥ `min_overlap` (default 0.5) AND `titles_a/b ≥
     min_titles` (50) AND surviving sampled queries per side ≥ `min_queries` (20) — request
     parameters with serde defaults (NaN/negative thresholds sanitized server-side), not
     config.
  7. **Surface + governance.** `GET /_vocab/aliases/feedback` (per-pair evidence + echoed
     thresholds + `capture_enabled`). `POST /_vocab/aliases/validate_and_apply` stamps
     validated pairs — `AliasEntry.feedback: Option<FeedbackEvidence>` (a `serde(default)`
     field-addition, the `number_context` precedent) + `confidence = max(old, overlap)` via
     `AliasRegistry::record_feedback` — through ADR-102's metadata-only seam (no recompile,
     nothing activated). With **`activate=true`** (explicit per-invocation, default false) it
     additionally promotes validated pairs via the new `activate_validated` — which, unlike
     the operator-override `activate()`, acts ONLY on a still-`Candidate` entry: `Rejected` is
     refused (an automated pass must never resurrect an operator's rejection), `MixedKind` is
     refused, and an already-`Active` entry returns `false` so a racing or repeated validate
     pass is idempotent and never triggers a spurious full recompile (codex review) — then the
     genuine `set_vocab` + recompile path, since active groups changed. v1 default remains operator-activates:
     automation exists one flag away, reversible via reject, and activation is never a *side
     effect* of capture. Cluster mode: 501-with-alternative (single-node-first, the
     ADR-059/060/102 precedent).

- **Safety.** Capture off ⇒ byte-identical (structural — the hook is behind the knob). Capture
  on: post-match token scans, O(batch × tracked pairs), under one mutex on the server state —
  an operational cost documented here, never a match-path change. Zero-FN untouched: evidence
  and confidence stamps change no matching-relevant state (the ADR-102 seam verifies it
  structurally); activation goes through the proven ADR-054/060 expansion path, which widens
  only (oracle-asserted superset).

- **Alternatives considered.**
  - **An explicit `POST /_feedback` (stage-two confirmations) as v1** — deferred, not
    rejected: higher precision (confirmed rather than candidate-satisfied) but requires an
    integration that exists nowhere yet; passive capture is immediately usable and the
    `observe` seam is exactly what the endpoint would call.
  - **Persisting evidence** — declined: a rolling operational signal; persistence adds a
    format + recovery surface for data whose value is recency.
  - **Capture inside the engine match path** — rejected: the hot-path rule; the handlers
    already hold everything needed.
  - **Auto-activation by default for validated pairs** — rejected: "confidence-gated,
    human-overridable, reversible" (the research doc); `activate=true` is the recorded
    compromise with the roadmap's "highest-precision *automated* signal".
  - **Exact capped id sets (first-N `HashSet`s)** — rejected: order-dependent under request
    interleaving (flaky) and biased; bottom-k is order-independent, fixed, estimator-grade.

- **Proven.** Unit (`vocab/alias/feedback.rs` + `alias/tests.rs`): sketch exactness below k /
  boundedness / permutation-invariance; Jaccard identical = 1.0, disjoint = 0.0, zero-sample =
  0.0 (never NaN), ~1/3 on a half-shared population; token-run classification (multi-word
  runs, `stud`-vs-`ud`, both-forms exclusion); the report-time exclusion drops
  form-referencing ids and a form-queries-only corpus never validates; tracked-pair sync
  (candidates-only, cap determinism, evidence retained across re-syncs); `record_feedback`
  stamps + maxes confidence with a NaN guard; `activate_validated` refuses `Rejected`; the
  `feedback` field round-trips and old JSON reads `None`; the oversampling regression (95%
  form-referencing population still validates — a post-cap filter would starve it); repeated
  automated activation returns `false` (idempotent). Oracle
  (`tests/oracle/alias_feedback.rs`): the full loop on a salted corpus — discovery files the
  candidate, passive capture over a mixed-form title stream validates it (with
  form-referencing queries demonstrably excluded), stamping alone changes NO match result and
  no epoch, `activate=true` makes the cross-form match appear with every prior match preserved
  (widening-only); and a disjoint-demand candidate accumulates plenty of evidence yet never
  validates. Handler/config: the settings patch accepts both knobs.

- **Deferred follow-ons.** The explicit stage-two `POST /_feedback`; n-form group validation;
  evidence-driven *de*-activation suggestions for underperforming active aliases; cluster-mode
  capture (needs a coordinator-side aggregation story).
