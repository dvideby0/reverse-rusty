# ADR-008: Deterministic data generation (seeded PRNG)

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


- **Context:** Benchmarks and correctness tests need synthetic data that models adversarial
  cases (hot-entity skew, broad queries, near-duplicate families).
- **Decision:** Use a deterministic SplitMix64 PRNG with no external crates. All data
  generation in `gen.rs` is seeded and reproducible.
- **Consequence:** Benchmark numbers are reproducible across runs. The oracle test is
  deterministic. Adversarial patterns (skew, families) are configurable parameters, not
  random noise.

