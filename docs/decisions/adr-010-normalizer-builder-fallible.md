# ADR-010: NormalizerBuilder + fallible construction

> [Normalization & vocabulary decisions](areas/normalization-and-vocabulary.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted


- **Context:** The `Normalizer::default_vocab()` constructor was the only way to build a
  normalizer and bundled product-specific vocabulary. It also used `.expect()` on the daachorse
  automaton build — the sole panicking call in library code, violating the no-`unwrap()` invariant
  (ADR-005). Core types lacked `Debug` impls and `Send`/`Sync` was not verified at compile time.
- **Decision:** Four changes: (1) Convert `default_vocab()` to return `Result<Self,
  NormalizerError>`, introducing `NormalizerError` in `error.rs`. (2) Add `NormalizerBuilder` with
  a fluent API for assembling custom vocabularies — phrases, synonyms, aliases, punctuation, and
  numeric context — so the engine is domain-agnostic. `default_vocab()` now builds an empty
  normalizer (no hardcoded
  vocabulary); domain vocabulary is supplied at runtime via the `Vocab` system (ADR-015) or
  directly via `NormalizerBuilder`. (3) Add
  `Debug` impls (derive or manual) to all public types. (4) Add compile-time `Send`/`Sync`
  assertions on all key types in `lib.rs`.
- **Consequence:** Zero panicking calls in library code. Downstream callers can build normalizers
  for any product domain. `Debug` + `Send`/`Sync` make the engine safe for
  production server use (behind `Arc<Mutex<Engine>>`, in `dbg!()` traces, etc.). `NormalizerError`
  wraps the daachorse error as a string to avoid leaking the dependency into the public API.
