# ADR-063: Adversarial test hardening — messy corpora, reference-free properties, and boundary pins

> [Back to the decisions index](../DECISIONS.md)


- **Status:** **Built + passing (2026-06-09).** A messy-mode layer on the generator (`src/gen.rs`:
  `messify_title`/`messify_query`/`messify_dataset`, opt-in functions — no `GenConfig` change, every
  existing corpus byte-identical), two messy differential passes (`tests/oracle/messy.rs`, per-title +
  columnar batch), a degenerate-input differential (`tests/oracle/degenerate.rs`), a new
  **reference-free property suite** (`tests/adversarial/` — self-match, metamorphic set-identity,
  cross-form matrices, unicode-soup fuzz), posting-tier boundary units (`src/index.rs mod tests`),
  a mid-file WAL bit-flip pin (`tests/persistence/wal.rs`), and a delete-twice/reinsert life-cycle pin
  (`tests/coverage_gaps/edge_cases.rs`). All run under the default `cargo test --release`.
- **Context:** A test-suite audit driven by one empirical observation: **every bug an external review
  (Codex) caught had passed a green suite.** Post-morteming all ~23 review-caught fixes on `main`
  classified the escapes: ~39% *path-simply-untested* (lifecycle seams), ~22% *generated-data-too-clean*,
  ~22% *cross-seam*, the rest error-path / weak-assertion / oracle-shares-code. Two systemic causes stood
  out. (1) **The generator is the easiest possible surface**: lowercase, single-spaced, punctuation-free,
  in-vocabulary ASCII — so the normalizer's hardest code (whitespace-run handling, punctuation classes,
  boundary-aware phrase selection, diacritic folding, synthetic-ID absorption) ran in production paths the
  corpus never touched. The codex-R8/R11/R12 whitespace-run and boundary-selection FNs lived exactly
  there. (2) **The differential oracle cannot see front-end divergence** (ADR-050): its brute reference
  shares `dsl::parse` + `compile::extract` + the `Normalizer`, so a *query-side vs title-side* asymmetry
  bug corrupts both sides identically. Golden tests pin single-stage semantics, and the parse-union
  oracle pins `P(T)` construction, but **no test asserted the end-to-end product contract on a hostile
  surface**: that a query and a title carrying the *same semantics under different bytes* still meet.
- **Decision:**
  1. **Messy mode as opt-in post-processing, not a `GenConfig` field.** `messify_*` wraps any clean
     string in seeded surface noise: case flips, foldable diacritics, whitespace runs (spaces + tabs +
     edge padding), Split-class punctuation around and *inside* tokens, unicode junk (emoji/CJK/™),
     duplicated tokens, fresh out-of-dict tokens, and (rarely) >64-distinct-feature padded titles.
     Query messing is structure-preserving (negation prefixes, quotes, parens, members untouched) so the
     clause shape — and the semantics under the shared normalizer — survive. Separate functions keep
     every existing benchmark/oracle corpus byte-identical and need no call-site churn.
  2. **Run the differential under mess** (`tests/oracle/messy.rs`): zero FN / zero FP on the messy
     corpus, per-title and batch, with **perturbation-rate guards** so the suite fails if messing ever
     silently becomes a no-op. (Scope honesty: the brute still shares the normalizer, so this pass pins
     the engine pipeline + crash-safety on hostile bytes — front-end divergence is the next item's job.)
  3. **Reference-free properties where the oracle is structurally blind** (`tests/adversarial/`):
     - **Self-match** — a title built from a query's own positive terms (mirroring `extract`'s
       joined-positive-stream rule) must match it; messy-query×clean-title and
       clean-query×perturbed-title variants make the two sides meet across *different* bytes. Any
       query↔title pipeline asymmetry (the R11 class) fails with no reference involved.
     - **Metamorphic set-identity** — under the phrase-free default vocab, identity perturbations
       (case, foldable diacritics, whitespace runs, Split punctuation, end-appended junk) must leave
       the full corpus match set **exactly** unchanged, title by title.
     - **Cross-form matrices** — the ADR-054/058/060/061 product contract stated as data: every
       punctuation-fold variant, equivalence form, and multi-word alias form cross-matches every other,
       both directions, including whitespace-run-corrupted quoted phrases / any-of members (the literal
       ecb569f regression) and run-corrupted titles (the `P(T)` overlap scan).
     - **Unicode-soup fuzz** — 30k seeded random strings + pinned nasties through both normalizers,
       `compile_features_readonly`, and `dsl::parse`: no panics, deterministic, sorted+deduped, and the
       two documented view relationships hold for ANY input (`P(T) ⊇ N(T)`;
       `match_features == N(T)`; no-alias ⇒ views identical).
  4. **Pin the structural boundaries that had no test at the boundary**: the posting tier ladder at
     exactly 8→9 (Inline→Heap) and 256→257 (Heap→Roaring) with id preservation; WAL recovery
     **stopping at** a mid-file bit-flipped frame (never resyncing past it — the junk-at-tail test could
     not distinguish these); delete-twice idempotence + reinsert-revives; and a degenerate-input
     differential (pure punctuation, vanishing any-of members, self-contradictory queries, marker soup,
     parse errors) asserting the engine and brute make the SAME call on both ingest paths.
     The mid-file pin **immediately caught a real bug**: `Wal::parse_entries` advanced its cursor past a
     frame's 8-byte len+CRC header before validating, so `skipped_bytes` under-reported the corrupt
     frame's own header — invisible to the old `skipped_bytes > 0` assertion. Fixed here (the parser now
     reports skipped bytes from the end of the last *fully-validated* frame).
- **Alternatives declined:** *cargo-mutants over the whole crate* — the right instinct (measure catching
  power directly) but hours of wall-clock per run on a release-profile LTO crate; targeted hand-mutation
  spot-checks during this work validated the new suites instead. *A second independent normalizer for the
  oracle* — re-declined for the ADR-050 reasons (an unverified second copy in permanent lockstep);
  reference-free properties get independence without a second implementation. *Pulling production eBay
  titles into the repo* — licensing + nondeterminism; the seeded mess layer reproduces the relevant
  hostile surfaces deterministically.
- **Why this is safe / what it buys:** purely additive — no production code changed except a new
  `#[cfg(test)]` module in `src/index.rs` and new pub functions in `src/gen.rs` (test/bench tooling, off
  the hot path; default output byte-identical). The suite now fails on: query↔title normalization drift
  on any messy surface, alias/equivalence/fold cross-form FNs (incl. the R11 whitespace-run class),
  match-set drift under semantics-preserving title noise, normalizer/parser panics on arbitrary unicode,
  posting-tier promotion bugs, WAL resync-past-corruption, and tombstone double-delete regressions —
  all classes that previously reached external review with green tests.
- **See also:** ADR-008 (seeded determinism — the mess layer is deterministic for the same reason),
  ADR-050 (golden front-end pinning — the blindness this complements), ADR-054/058/060/061 (the
  contracts the cross-form matrices state as data), ADR-046 (synthetic IDs — exercised by the OOV mess
  ops). Tests: `tests/adversarial/`, `tests/oracle/{messy,degenerate}.rs`, `src/index.rs`,
  `tests/persistence/wal.rs`, `tests/coverage_gaps/edge_cases.rs`.
