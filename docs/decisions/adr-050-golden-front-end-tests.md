# ADR-050: The differential oracle's front end is verified by spec-authored golden tests, not a second extractor

> [Back to the decisions index](../DECISIONS.md)


- **Status:** **Built + passing (2026-06-03).** Spec-authored golden tests added in-module to `src/dsl.rs`
  (full-AST cases), `src/normalize.rs` (`mod golden`), and `src/compile.rs` (`mod golden`), plus a
  vocab-rich oracle pass (`zero_false_negatives_with_populated_vocab`) in `tests/oracle.rs`. All run under
  the default `cargo test --release` (the `--lib` tests need no `tests/` registration), so the gate
  (`check.sh`) covers them automatically.
- **Context:** The differential oracle (`tests/oracle.rs`) is the load-bearing correctness test — it
  asserts the engine has zero false negatives/positives against a brute-force matcher over millions of
  (title, query) pairs ([`design/README.md`](../design/README.md) §2). Its independence is **partial**, and
  an external review correctly flagged it. The brute reference reimplements only **candidate retrieval +
  exact verification** (a linear scan, with its own `Dict`/`Normalizer` *instances*). For the **front end**
  it calls the engine's *own* production code: `dsl::parse`, `compile::extract`, and the `Normalizer`
  pipeline. So a semantic bug in the parser, the extractor, or the normalization model corrupts the ground
  truth and the engine identically — both agree on the wrong answer and the oracle stays green. Worse, the
  oracle builds both sides with the empty `default_vocab` (ADR-010), so the entire vocab-driven
  normalization path (multiword phrases, synonyms, graders) is **never exercised at all** — the generator
  side-steps it with any-of groups of alternate surface forms. The contract's `positively_matches(T, Q)`
  is *assumed* correct; the oracle proves the index+verify agrees with a brute force over the **same
  extracted features**, not that the extraction itself is right.
- **Decision:**
  1. **Pin the three front-end stages with hand-authored golden tests.** Expected outputs are written from
     the spec — [`reference/dsl.md`](../reference/dsl.md), [`design/normalization.md`](../design/normalization.md)
     §1–§4, [`design/matching.md`](../design/matching.md) §1 — **not** captured from running the code, so a
     human-authored constant cannot inherit a code bug. `dsl.rs` asserts full `Ast` structure per operator;
     `normalize.rs` asserts exact feature-*name* sets (diacritics, the number-disambiguation matrix, the
     vocab-driven phrase/synonym/grader paths, fold-stability + no-drift determinism); `compile.rs` asserts
     exact required/forbidden/any-of name sets (joint multiword normalization, singleton-any-of promotion,
     dedup, the §1 worked example) plus the **forbidden-never-anchors** invariant at the data level.
  2. **Exercise the vocab-driven normalizer end-to-end** with a second oracle pass built on a populated
     `NormalizerBuilder` vocab. Still a coherence check (shared front end), so it *complements* the golden
     tests rather than replacing them — but it makes the phrase/synonym/grader machinery reachable, which
     the empty-vocab oracle never did.
  3. **Author conservatively where the spec is silent.** Cases the docs don't define (serial-vs-year
     precedence, doubled commas, ambiguous `mj`) are omitted or labelled regression-guards, never presented
     as spec-blessed.
  4. **Keep them in-module, not in a new `tests/` file.** They are `--lib` unit tests next to the code they
     pin (matching the existing `dsl.rs`/`dict.rs` convention), so no module-map / suite-table registration
     is needed and a future visibility tightening can't break them.
- **Alternatives declined:** *A fully independent reference extractor* — reimplement parse + normalize +
  extract a second time inside the oracle and diff the two feature representations. Rejected: it is a second
  copy of nontrivial, evolving logic (the daachorse phrase scan, the number-disambiguation rules) that
  would itself be unverified and would have to be kept in lockstep with every normalizer change; a
  divergence could not be attributed to the engine vs. the copy, muddying the zero-false-negative signal.
  Golden constants traceable to a spec section are cheaper, stronger, and localize a failure to one stage.
  *Weakening or removing the empty-vocab oracle* — rejected; the vocab-rich pass is additive, the existing
  oracle is untouched.
- **Why this is safe / what it buys:** the change is purely additive (new tests + a corrected comment + a
  doc note); no production code changed. The golden tests give the front end the independent check the
  differential oracle structurally cannot, and the vocab-rich pass closes the "never even ran it" gap. A
  future semantic regression in parse/normalize/extract now fails a hand-authored assertion instead of
  silently passing the oracle.
- **See also:** ADR-006 (forbidden-never-gates — the data-level invariant the `compile.rs` golden tests
  assert), ADR-008 (seeded determinism the oracle relies on), ADR-010 (the empty `default_vocab` that
  motivates the vocab-rich pass), ADR-046 (synthetic IDs — why the golden helpers use the *mutating*
  compile path so `Dict::name` round-trips), ADR-024 (the one-gate model the `--lib` tests slot into).
  Tests: `src/dsl.rs`, `src/normalize.rs`, `src/compile.rs` (`mod golden`), `tests/oracle.rs`
  (`zero_false_negatives_with_populated_vocab`). How-we-test: [`testing.md`](../testing.md).
