# ADR-087: A front-end-independent correctness oracle (the Phase 0 reference matcher)

> [Back to the decisions index](../DECISIONS.md)

- **Status:** **Built + passing (2026-06-24).** New std-only workspace member
  `engine/ref-matcher/` (`reverse-rusty-ref-matcher`) reimplementing the DSL parser, normalizer,
  extractor, and match predicate from the spec, with **zero** dependency on `reverse-rusty`; a new
  differential suite `engine/tests/independent_oracle/` diffs the real engine against it; and a new
  `check.sh` lane (`ref-matcher independence`) mechanically enforces the independence via
  `cargo tree`. All run under the default `cargo test --release` + the gate.

- **Context:** This is **Phase 0, item 2** of the reality/adversarial audit — the
  highest-value net-new item, prioritized above every product-roadmap tier. Reverse Rusty's cardinal
  guarantee is **zero false negatives** ([`design/README.md`](../design/README.md) §2): a stage-one
  high-recall candidate generator where a silent miss is the worst failure. The load-bearing
  correctness test is the in-tree differential oracle (`tests/oracle/`), but its independence is
  **structurally partial**: the "brute-force" reference reimplements only candidate retrieval + exact
  verification — for the **front end** it calls the engine's OWN `dsl::parse`, `compile::extract`, and
  `Normalizer` (`tests/oracle/harness.rs`). So a semantic bug in the parser, normalizer, or extractor
  corrupts both sides identically and the oracle stays green (the shared-front-end blind spot,
  **ADR-050**; the reference-free `tests/adversarial.rs` only partly covers it). ADR-050 narrowed the
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
     scan), the extractor (joint normalization of positive bare words; the rarest-by-frequency any-of
     proxy with singleton-collapse; ADR-054 equivalence expansion required→any-of; class-D drop), and
     the match predicate.
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
  4. **Full front-end coverage, proven differentially.** The suite asserts **zero false negatives AND
     zero false positives** over: the generated corpus under the empty default vocab (clean + the
     adversarial messy/surface-noise pass); a populated grader+phrase+synonym vocab; the ADR-061
     multi-word alias two-view path (a controlled mix exercising bidirectional aliases, nested/overlap
     entities, the forbidden-canonical-`N(T)` view, component tokens, any-of, and whitespace runs, plus
     a randomized at-scale alias corpus); a hand-written **gotcha table** asserted against BOTH sides
     (a human-authored expectation is the tiebreaker); and an **env-gated real corpus**
     (`RR_ORACLE_CORPUS=<jsonl>`, skipped when unset, so CI and the public repo never see user-supplied
     real data).
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
  - **One reference simplification, documented:** the engine represents a multi-token any-of member by
    its rarest interned-id proxy on a frequency tie; the reference uses the lexicographically-smallest
    feature on a tie. The two can differ only on a title bearing SOME-but-not-all of a multi-token
    member's tokens — which real surface forms never produce (a title carries a member completely or
    not at all), so it does not arise in the generated / gotcha corpora (both pass). Noted for the
    real-corpus pass.
  - **Result:** zero FN / zero FP everywhere — ~61k default-clean, ~69k default-messy, ~75k populated,
    ~989k at-scale-alias matches, plus every gotcha. The independent reference found **no engine
    front-end bug**: the parser/normalizer/extractor are now confirmed correct under a check the
    in-tree oracle structurally cannot be, not merely assumed correct.

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

- **Why this is safe / what it buys:** purely additive — a new dev-only crate, a new test suite, one
  `check.sh` lane, and a Cargo dev-dependency; no production code changed, and the lean/server/
  distributed builds are byte-identical (the member is dev-only). It gives the front end the
  independent differential check the in-tree oracle structurally cannot, closing the documented
  shared-front-end blind spot for the covered paths, and lays the ground truth that Phase 0 item 3
  (real-process crash injection) will diff a recovered engine against.

- **See also:** ADR-050 (the shared-front-end blind spot + the golden-test mitigation this completes),
  ADR-063 (the reference-free adversarial suite + the test-audit that motivated Phase 0), ADR-054
  (equivalence expansion), ADR-058 (punctuation folding), ADR-061 (the two-view alias semantics),
  ADR-068 (class-D), ADR-069 (number context), ADR-028 (the lean-dependency philosophy the std-only
  reference honors). Code: `engine/ref-matcher/`, `engine/tests/independent_oracle/`,
  `engine/check.sh` (the `ref-matcher independence` lane). How-we-test:
  [`../testing.md`](../testing.md).
