# Engine, errors, dependencies & ops decisions

> [Architecture decision hub](../../DECISIONS.md)

Cross-cutting APIs, typed failures, dependency boundaries, observability, testing, and delivery mechanics.

| ADR | Decision | Summary | Status |
|---|---|---|---|
| [005](../adr-005-typed-errors.md) | Typed errors | Replaces string errors and silent drops with inspectable parser, normalization, and ingest outcomes. | Accepted |
| [007](../adr-007-three-production-dependencies.md) | Initial production dependencies | Adopts specialized automaton, bitmap, and parallelism crates after validating the core design. | Accepted |
| [008](../adr-008-deterministic-data-generation.md) | Deterministic data generation | Uses a seeded generator so benchmarks and differential tests are reproducible. | Accepted |
| [021](../adr-021-durability-failures-observable.md) | Observable durability failures | Routes durability degradation through structured events instead of stderr. | Accepted |
| [022](../adr-022-runtime-settings-api.md) | Runtime settings API | Updates the dynamic configuration subset atomically while rejecting static or invalid keys. | Accepted |
| [023](../adr-023-per-segment-introspection.md) | Per-segment introspection | Exposes segment holes, memory shape, and staleness through lock-free snapshots. | Accepted |
| [024](../adr-024-ci-github-actions.md) | CI mirrors `check.sh` | Makes the local gate the CI source of truth and keeps exploratory benchmarks advisory. | Accepted |
| [028](../adr-028-lean-core-feature-gate.md) | Lean-core feature gate | Keeps the engine usable without server dependencies and enforces that boundary in CI. | Accepted |
| [050](../adr-050-golden-front-end-tests.md) | Golden front-end tests | Pins parser, normalizer, and extractor behavior that a shared-front-end oracle cannot independently catch. | Accepted |
| [052](../adr-052-external-review-hardening.md) | External-review hardening | Collects parser, signature, request-limit, storage-bound, timeout, and bind-default fixes. | Accepted |
| [140](../adr-140-stats-api-contract.md) | Stats REST API contract | Makes native stats strict, truthful about physical rows, and bounded off async workers. | Accepted |
| [141](../adr-141-cat-stats-api-contract.md) | CAT stats API contract | Gives native stats a strict bounded table with familiar CAT controls. | Accepted |
| [142](../adr-142-cat-segments-api-contract.md) | CAT segments API contract | Gives native LSM rows strict familiar CAT controls without fabricating Lucene fields. | Accepted |
| [143](../adr-143-cat-shards-api-contract.md) | CAT shards API contract | Makes logical shard counts and committed assignments strict, bounded, and fail-loud. | Accepted |
| [144](../adr-144-health-api-contract.md) | Health REST API contract | Makes native readiness strict, waitable, bounded, and fail-loud. | Accepted |

---

Shipped changes are recorded in [CHANGELOG.md](../../CHANGELOG.md); unfinished work belongs in
[roadmap.md](../../roadmap.md). Documentation placement rules live in
[the documentation hub](../../README.md).
