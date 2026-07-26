# ADR-087: A front-end and lowering-independent semantic oracle

> [Distributed v1 — the ADR-065 graduation program decisions](areas/distributed-v1-graduation.md) · [Decision hub](../DECISIONS.md)

- **Status:** **Built + passing (2026-06-24; semantic-model hardening 2026-07-25).** New std-only workspace member
  `engine/ref-matcher/` (`reverse-rusty-ref-matcher`) reimplementing the DSL parser, normalizer,
  grammar-preserving semantic predicate model, and direct evaluator from the spec, with **zero**
  dependency on `reverse-rusty`; a differential suite `engine/tests/independent_oracle/` diffs the
  real engine against it; and a `check.sh` lane (`ref-matcher independence`) mechanically enforces
  code independence via `cargo tree`. All run under the default `cargo test --release` + the gate.

  **2026-07-24 amendment (ADR-118):** “independent” here means no shared code or dependencies, not
  guaranteed semantic independence. Both implementations independently interpreted “joint positive
  bare words” as spanning non-bare intervening clauses and therefore agreed on one false-negative
  lowering. ADR-118 fixed it and recorded the residual oracle boundary.

  **2026-07-25 amendment (issue #123):** the reference no longer reproduces production extraction.
  Its former flattened `RefQuery` (including corpus frequencies and rarest-member retrieval
  proxies) is removed. `semantic.rs` now retains an explicit ordered clause tree:
  `RequiredTerm`, `RequiredPhrase`, `RequiredAnyOf`, and their complete forbidden counterparts.
  Any-of members remain full conjunctions, a forbidden clause negates its complete predicate, and
  phrases remain analyzed graphs. The tree evaluates directly against independent canonical
  `P(T)`/`N(T)` title representations. The differential separately classifies any semantic truth
  missed by the final engine as a candidate-cover miss or a post-retrieval verification miss by
  running a candidate-only collector through the real stored posting/filter/lane traversal;
  candidate generation is compared for **recall only**, since extra candidates are legal.

  The first review of that model immediately exposed a production divergence: a negated bare term
  that analyzes to several features (for example `-psa10`) was flattened into independent
  exclusions, rejecting a title that contained only one feature. Production now stores it as one
  complete forbidden conjunction, the human differential pins partial and complete cases, and
  compiler semantics **4** source-rebuilds semantics 0–3 before serving.

- **Context:** This is **Phase 0, item 2** of the reality/adversarial audit — the
  highest-value net-new item, prioritized above every product-roadmap tier. Reverse Rusty's cardinal
  guarantee is **zero false negatives** ([`design/README.md`](../design/README.md) §2): a stage-one
  high-recall candidate generator where a silent miss is the worst failure. The load-bearing
  correctness test is the in-tree differential oracle (`tests/oracle/`), but its independence is
  **structurally partial**: the "brute-force" reference reimplements only candidate retrieval + exact
  verification — for the **front end** it calls the engine's OWN `dsl::parse`, `compile::extract`, and
  `Normalizer` (`tests/oracle/harness.rs`). So a semantic bug in the parser, normalizer, or extractor
  corrupts both sides identically and the oracle stays green (the shared-front-end blind spot,
  **ADR-050**; the reference-free `tests/adversarial/` only partly covers it). ADR-050 narrowed the
  gap with spec-authored golden tests, but golden tests are a finite set of point cases — they cannot
  differentially exercise the front end over millions of (title, query) pairs. The Phase 0 directive
  is precisely to prove which parts are real *under an independent check*; for the front end, that
  check is a from-scratch reference run differentially against the engine.

- **Decision:**
  1. **A separate, std-only, zero-dependency reference crate** — `reverse-rusty-ref-matcher`, a
     workspace member built ONLY as a `[dev-dependencies]` of `engine` (so the lean core / server /
     distributed builds never compile it). It reimplements the whole front end from the spec
     ([`reference/dsl.md`](../reference/dsl.md), [`design/normalization.md`](../design/normalization.md),
     ADR-054/058/060/061/068/069 + the spec-authored golden tests) — **not** copied from
     `normalize/core.rs` / `compile/extract.rs`: the DSL parser (AND clauses, any-of groups, phrases,
     adjacent-`-` negation, the byte/clause/any-of limits), the two-phase normalizer (byte clean +
     diacritic fold + the `PunctClass` table; the grader/grade/number/synonym/generic token pipeline
     with the single-pending grader/grade-context aging windows; the ADR-061 two title views
     `N(T)` / `P(T)` with the force-additive parse-union, the raw-`term:` union, and the overlap
     scan), and a **plain semantic clause tree** from the user-facing grammar. Positive bare terms
     are analyzed only within maximal uninterrupted runs (ADR-118); any-of members remain complete
     conjunctive term predicates (ADR-119); required/forbidden phrases remain ordered graph
     predicates (ADR-120); and forbidden terms/groups negate their complete predicates. Positive
     ADR-054 equivalence alternatives widen leaf requirements in that tree. It contains no
     frequency counter, retrieval proxy, signature, cost class, exact-store column, or singleton
     lowering from the production compiler.
  2. **It reuses none of the engine — provably.** No `reverse-rusty`, no `daachorse`, no `serde`. The
     reference compares matches by **canonical feature string** (`year:1994`, `term:psa`,
     `grade:10`, `grader_grade:psa10`), never the engine's interned `FeatureId` — which is what frees
     it from the dictionary entirely (synthetic hashing included). Phrase matching is a **naive linear
     scan**, not an Aho-Corasick automaton: a test oracle optimizes for correctness + independence, and
     a structurally different second implementation is *more* likely to expose an integration bug than
     reusing the same library would be. Independence is enforced by the `ref-matcher independence`
     `check.sh` lane (`cargo tree` must show no `reverse-rusty` edge), so it cannot silently regress.
  3. **One vocabulary description drives both sides.** The differential harness (which links both
     crates) builds the engine `Normalizer`/`Vocab` AND the reference `RefVocab` from the same
     generator constants / alias declarations — feeding identical vocabulary *data* (not logic) to both,
     exactly as it feeds identical generated query/title *strings* to both. Only the normalization
     *logic* differs.
  4. **Full front-end coverage, proven differentially.** The suite asserts **zero false negatives,
     zero false positives, and zero candidate-cover false negatives** over: the generated corpus
     under the empty default vocab (clean + the
     adversarial messy/surface-noise pass); a populated grader+phrase+synonym vocab; the ADR-061
     multi-word alias two-view path (a controlled mix exercising bidirectional aliases, nested/overlap
     entities, the forbidden-canonical-`N(T)` view, component tokens, any-of, and whitespace runs, plus
     a randomized at-scale alias corpus); a hand-written **gotcha table** asserted against BOTH sides
     (a human-authored expectation is the tiebreaker); and an **env-gated real corpus**
     (`RR_ORACLE_CORPUS=<jsonl>`, skipped when unset, so CI and the public repo never see user-supplied
     real data). Candidate comparison is deliberately one-way: every semantic truth must be retrieved
     through the actual stored indexes, while false-positive candidates remain legal work for exact
     verification.
  5. **Drift policy — the spec is the authority.** The reference is authored from the spec + the
     spec-authored golden tests, never from engine code. On a genuine divergence the triage authority
     is the spec + golden tests, not "trust the engine": spec mandates the reference's answer ⇒ an
     **engine bug** (the high-value catch); spec mandates the engine's ⇒ a reference bug; spec silent
     ⇒ a spec gap (decide intent, add a golden test + gotcha). The finite tables that must match
     exactly (the diacritic fold map, the punct classes, the year `1900..=2099` and grade `1.0..=10.0`
     ranges, the `>3`/`>2` aging windows) are called out in code so a reviewer diffs them against the
     spec.

- **Findings & non-obvious facts (recorded so they aren't re-discovered):**
  - `Normalizer::default_vocab()` has **empty** graders/grade-words (only `number_context = ["pop"]`).
    So under the default vocab the in-tree oracle runs, `psa10` does NOT fuse — it is a single generic
    `term:psa10`, and `psa 10` is `term:psa` + `term:10`. Grader fusion + aging fire only under a
    populated grader vocab. This shaped the default-vocab phase.
  - **The reference has no candidate plan.** Earlier versions selected their own rarest feature for
    every any-of member, which was independent code but the same execution idea as production. That
    could make both implementations agree on an incorrect proxy interpretation. The semantic model
    now stores only complete member predicates. Candidate-cover behavior is observed only on the
    production side and checked against semantic truths for recall.
  - **Result at adoption:** zero FN / zero FP over ~61k default-clean, ~69k default-messy, ~75k
    populated, ~989k at-scale-alias matches, plus every then-current gotcha. ADR-118 later found the
    first shared-semantics miss outside that expectation table: code independence did not prevent
    both implementations from translating the same ambiguous prose incorrectly.

- **Alternatives reconsidered (this revisits ADR-050's declined option):** ADR-050 explicitly
  *declined* "a fully independent reference extractor," on three grounds; each is addressed here:
  - *"A second copy of nontrivial logic that would itself be unverified."* The reference IS verified —
    by the differential against the engine over millions of pairs, by the hand-authored gotcha table
    (asserted against both sides), and by the spec/golden-tests as the named tiebreaker. An unverified
    second copy was the objection; a *cross-checked* second copy is the instrument.
  - *"Would have to be kept in lockstep with every normalizer change."* Accepted as real maintenance
    cost, and the right trade for the FN-safety of the cardinal guarantee. The cost is bounded: the
    reference is std-only and self-contained, and any drift surfaces immediately as a differential
    failure (not a silent gap).
  - *"A divergence could not be attributed to the engine vs. the copy."* Resolved by the drift policy:
    the **spec + golden tests** are the authority, so a divergence is attributable by construction, and
    the gotcha table (human-authored expectations) localizes it.
  This ADR does not weaken ADR-050; the golden tests + vocab-rich pass remain. The independent oracle
  is the differential complement the point-case golden tests cannot be. *Not chosen:* a Python
  reference (a second toolchain CI lacks; `cargo tree` can't prove its independence; std-only Rust
  gets code-level independence without a second runtime). *Not chosen:* an in-tree test module (it
  links `reverse-rusty`, so nothing structurally prevents reusing the front end — the exact way the
  in-tree oracle ended up sharing it).

- **Why this is safe / what it buys:** the reference remains dev-only and zero-dependency. The
  2026-07-25 hardening replaces a reference-only execution-plan model with a smaller grammar tree.
  Its candidate observer is a diagnostic collector over the existing monomorphized traversal; every
  production collector keeps a no-op callback that optimizes away. The one production semantic
  correction reuses the existing integer-only forbidden-conjunction program, and compiler semantics
  4 forces source-driven standalone/cluster migration before an older row can be served. This closes
  both the shared-front-end blind spot and the “same lowering in different code” blind spot for the
  covered grammar, while retaining the ground truth used by Phase 0 item 3 (real-process crash
  injection).

- **See also:** ADR-050 (the shared-front-end blind spot + the golden-test mitigation this completes),
  ADR-063 (the reference-free adversarial suite + the test-audit that motivated Phase 0), ADR-054
  (equivalence expansion), ADR-058 (punctuation folding), ADR-061 (the two-view alias semantics),
  ADR-068 (class-D), ADR-069 (number context), ADR-028 (the lean-dependency philosophy the std-only
  reference honors). Code: `engine/ref-matcher/`, `engine/tests/independent_oracle/`,
  `engine/check.sh` (the `ref-matcher independence` lane). How-we-test:
  [`../testing.md`](../testing.md).
