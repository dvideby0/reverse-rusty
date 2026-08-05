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
| [145](../adr-145-metrics-api-contract.md) | Metrics REST API contract | Makes Prometheus scrapes strict and coordinator collection complete, bounded, and fail-loud. | Accepted |
| [146](../adr-146-get-vocab-api-contract.md) | Vocabulary read REST API contract | Makes the native vocabulary document strict, round-trippable, cache-safe, and bounded off async workers. | Accepted |
| [147](../adr-147-put-vocab-api-contract.md) | Vocabulary replacement REST API contract | Makes full vocabulary replacement strict, bounded, off-runtime, mode-consistent, and durably fail-loud. | Accepted |
| [148](../adr-148-vocab-learn-api-contract.md) | Vocabulary learning REST API contract | Makes review-first learning strict, bounded, mode-consistent, and explicit about its native scope. | Accepted |
| [149](../adr-149-vocab-learn-apply-api-contract.md) | Vocabulary learn-and-apply REST API contract | Makes stored-corpus learning strict, off-runtime, mode-consistent, and durably fail-loud. | Accepted |
| [150](../adr-150-alias-registry-read-api-contract.md) | Alias-registry read REST API contract | Makes governed-alias review strict, pageable, observable, and bounded off async workers. | Accepted |
| [151](../adr-151-parallel-ci-code-gate-lanes.md) | Parallel CI code-gate lanes | Runs exact core and distributed gates concurrently behind one required result. | Accepted |
| [152](../adr-152-alias-import-api-contract.md) | Alias-import REST API contract | Makes governed Solr imports strict, familiar, idempotent, durable, and mode-consistent. | Accepted |
| [153](../adr-153-alias-learn-apply-api-contract.md) | Alias learn-and-apply REST API contract | Makes stored-corpus alias learning strict, bounded, off-runtime, durable, and mode-consistent. | Accepted |
| [154](../adr-154-alias-discover-api-contract.md) | Alias discovery REST API contract | Makes review-first distributional discovery strict, bounded, off-runtime, and mode-consistent. | Accepted |
| [155](../adr-155-alias-discover-record-api-contract.md) | Alias discover-and-record REST API contract | Makes candidate recording strict, bounded, off-runtime, and explicit about live-only persistence. | Accepted |
| [156](../adr-156-alias-feedback-read-api-contract.md) | Alias-feedback read REST API contract | Makes behavioral-evidence review strict, pageable, bounded, off-runtime, and mode-consistent. | Accepted |
| [157](../adr-157-alias-feedback-reset-api-contract.md) | Alias-feedback reset REST API contract | Makes evidence-window reset strict, linearizable, bounded, observable, and mode-consistent. | Accepted |
| [158](../adr-158-alias-feedback-validate-apply-api-contract.md) | Alias-feedback validate-and-apply REST API contract | Makes evidence stamping and explicit activation strict, idempotent, bounded, durable, and observable. | Accepted |
| [159](../adr-159-get-settings-api-contract.md) | Settings read REST API contract | Makes live configuration reads strict, cache-safe, bounded, observable, and off-runtime. | Accepted |
| [160](../adr-160-put-settings-api-contract.md) | Settings write REST API contract | Makes runtime configuration updates strict, duplicate-safe, bounded, observable, and coherently published off-runtime. | Accepted |
| [168](../adr-168-inactive-rkyv-advisory.md) | Inactive `rkyv` advisory qualification | Narrows one lockfile-only RustSec exception behind an all-feature/all-target activation guard. | Accepted |

---

Shipped changes are recorded in [CHANGELOG.md](../../CHANGELOG.md); unfinished work belongs in
[roadmap.md](../../roadmap.md). Documentation placement rules live in
[the documentation hub](../../README.md).
