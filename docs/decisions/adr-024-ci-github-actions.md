# ADR-024: CI via GitHub Actions mirroring `check.sh`; commit pressure tests + benchmark baseline

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


- **Context:** The quality gate was `engine/check.sh` (fmt + clippy + test + audit + deny), run by hand
  before pushing — CLAUDE.md called it "the local CI substitute." Three gaps had opened up: (1) nothing
  *enforced* the gate, so an unrun `check.sh` could merge; (2) the pressure suite (`tests/stress.rs`) and
  the benchmark regression baseline (`docs/performance/benchmark-results.txt`) were gitignored — the
  latter silently, via a blanket `*.txt` rule — so both were invisible to any automated runner and easy
  to let rot; (3) the "CI is a non-goal" framing no longer matched the intent to check every PR.
- **Decision:** Add GitHub Actions CI (`.github/workflows/ci.yml`) that **runs `check.sh` itself** rather
  than re-listing the checks — one source of truth, so "green locally" and "green in CI" cannot diverge.
  - **Commit what CI must see.** Un-gitignore `tests/stress.rs` (15 pressure tests + one `#[ignore]`d 10M
    soak) and `benchmark-results.txt`; tighten `.gitignore` so only genuine runtime data (`data/`, loose
    `*.csv`/`*.jsonl`/`*.txt`) stays ignored. The stress suite is now part of `cargo test --release` and
    runs on every PR; the 10M soak stays `#[ignore]`d and runs only on demand (`workflow_dispatch` →
    `run_soak`), as it needs ~minutes and multi-GiB RAM.
  - **Benchmarks run-and-print, never gate.** CI runs the seeded, deterministic `bench`/`segbench`/
    `snapbench` and uploads their console output as an artifact, but `continue-on-error` keeps them from
    failing the build. Throughput is hardware-dependent (the runner is not the reference machine), and the
    machine-independent *structural* invariants stay a **manual** comparison against `benchmark-results.txt`
    — a deliberate choice over a brittle numeric assert that would false-alarm on runner variance.
  - **Reproducibility + local fast-fail.** Pin the toolchain in `engine/rust-toolchain.toml`; cache builds
    with `Swatinem/rust-cache`; install `cargo-audit`/`cargo-deny` as prebuilt binaries. Locally, committed
    git hooks (activated once via `./setup-hooks.sh`) run the fast gate (fmt + clippy, `check.sh --fast`)
    on commit and the full gate on push.
- **Consequence:** Every PR is gated by the same checks a developer runs locally; the pressure tests and
  benchmark baseline are now version-controlled and exercised rather than drifting out-of-tree; and
  benchmark numbers are captured per-PR for review without producing false regressions from runner
  variance. This **supersedes the "local CI substitute" framing**: `check.sh` remains the gate and the
  local entry point, but it is now also the script CI runs — not a stand-in for the absence of CI. Cost:
  PR runs pay the release+LTO compile (mitigated by caching) and the full suite including stress (a few
  minutes); accepted in exchange for the coverage.
- **See also:** [`testing.md`](../testing.md) (the how-we-test guide), ADR-008 (seeded determinism — why the
  benchmarks reproduce), `engine/check.sh`, `.github/workflows/ci.yml`, `engine/rust-toolchain.toml`.

