# ADR-151: Parallel CI code-gate lanes with one required result

> [Engine, errors, dependencies & ops decisions](areas/engine-quality-and-operations.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

ADR-024 made `engine/check.sh` the single definition of the local and CI
code/correctness/security gate. As the distributed feature and oracle suites grew, the script still
ran every command serially on one CI runner. A representative cached pull-request run on 2026-07-28
spent 19 minutes 48 seconds in `check.sh`: default release tests took 7 minutes 45 seconds (6
minutes 20 seconds compiling), then distributed release tests took 10 minutes 52 seconds (9 minutes
12 seconds compiling). The full required job took 21 minutes 16 seconds even though the default and
distributed feature graphs share no runtime state or ordering requirement.

The separate container harness and Helm jobs already ran concurrently. Parallelizing individual
Cargo processes inside one job was not suitable: both builds would compete for the same four CPUs,
memory, and target-directory locks. Replacing release tests with a cheaper profile would reduce
compile fidelity, and sharding individual integration targets would duplicate the dominant
release-plus-LTO compilation.

## Decision

1. `engine/check.sh` remains the only command definition and its no-argument invocation remains the
   complete local gate. It additionally accepts two exact subsets:

   - `--lane core`: formatting, default and lean-core Clippy, default release tests, audit, deny,
     reference-matcher independence, crash injection, and the file-size advisory;
   - `--lane distributed`: distributed-feature Clippy and distributed release tests.

   `--fast` remains the pre-commit default/lean lint path.

2. GitHub Actions runs those two lanes as independent jobs on separate `ubuntu-24.04` runners.
   Each lane keeps the same Cargo command, release profile, test target set, and failure behavior as
   the full local gate. The core lane retains the former gate cache namespace. The distributed lane
   restores the production distributed-bin cache shared by the container harness and release
   workflow, but does not save test artifacts back into that cache.

3. A small aggregate job retains the established required status name `gate + benchmarks`. It runs
   after both lanes regardless of success, failure, or cancellation, and succeeds only when both
   lane results are `success`. This preserves branch-protection continuity while ensuring a failed
   or canceled lane—and especially the newly separate distributed lane—cannot become advisory.

4. ADR-124's performance contract, the local deploy smoke, and exploratory benchmark captures stay
   on the core pinned runner after core validation. They never execute concurrently with the timing
   gate on that runner. The stateful crash-injection and container-harness scenarios remain serial
   internally.

5. Binary targets with no binary-local tests set `test = false`, avoiding empty libtest harnesses.
   Their normal binaries remain compiled when integration tests require binaries and remain covered
   by `clippy --all-targets`; binaries with real unit tests retain their harnesses.

## Alternatives considered

- **Background default and distributed Cargo commands in `check.sh`.** Rejected because they would
  contend on one four-core runner and one target directory instead of supplying real parallel
  capacity.
- **Shard every integration-test target into a matrix.** Rejected because compilation is much more
  expensive than execution, so each shard would duplicate the dominant work and materially increase
  runner cost.
- **Adopt a non-LTO CI test profile.** Deferred. It can be evaluated separately, but this decision
  first removes serialization without changing code generation or test coverage.
- **Adopt a third-party test runner.** Deferred. Cargo already runs tests within each libtest binary
  in parallel; the measured bottleneck is compilation rather than test scheduling.

## Consequences

- The two longest independent builds overlap, reducing pull-request wall time while preserving every
  command and test.
- Total hosted-runner minutes may rise because the jobs have separate setup and local-crate build
  artifacts. Read-only reuse of the distributed production cache limits duplication and churn, but
  lower latency is intentionally purchased with some additional parallel capacity.
- Local developers retain one complete `./check.sh`; targeted lane invocations are available for
  iteration and CI without duplicating command definitions in workflow YAML.
- The aggregate required check makes lane membership explicit. Adding another independent code-gate
  lane requires adding it both to `check.sh` and to the aggregate job's required results.
