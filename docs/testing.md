# Testing, benchmarks & CI

How Percolator is verified — the suites, the pressure/soak tests, the benchmarks, the local git
hooks, and the GitHub Actions pipeline. There is **one gate**, [`engine/check.sh`](../engine/check.sh);
everything here is either that gate or a layer around it. Why it's shaped this way →
[`DECISIONS.md`](DECISIONS.md) ADR-024.

## TL;DR

- **Before you push:** run [`engine/check.sh`](../engine/check.sh) — or install the hooks once with
  [`./setup-hooks.sh`](../setup-hooks.sh) and they run it for you.
- **CI runs the same `check.sh`** on every PR and push to `main`, plus the benchmarks. Green locally
  ⇒ green PR.
- Test *counts* are never hand-maintained here — run `cargo test --release` for the live number.

## The one gate: `check.sh`

```
cd engine && export CARGO_TARGET_DIR=/tmp/perc-target   # or just ./engine/check.sh from the root
./check.sh          # full gate: fmt + clippy + test + audit + deny
./check.sh --fast   # quick gate: fmt + clippy only (what the pre-commit hook runs)
```

Every step runs even if an earlier one fails, so one invocation surfaces every problem; the script
exits non-zero if any step failed. It needs the `rustfmt` + `clippy` components (supplied by the
pinned toolchain) and two cargo plugins: `cargo install cargo-audit cargo-deny`.

## Test suites

All live in `engine/` and run under `cargo test --release` (release because the oracle and stress
suites generate large seeded corpora — debug is far too slow). Run one suite with
`cargo test --release --test <name>`; unit tests with `cargo test --release --lib`.

| Suite | Where | Covers |
|---|---|---|
| **Differential oracle** | `tests/oracle.rs` | The **correctness contract** — brute force vs engine, asserting zero false negatives/positives ([`design/README.md`](design/README.md) §2). The load-bearing test; never weaken it. |
| Unit tests | `src/*.rs` | DSL parsing, vocab, WAL framing, loader, anchor filter (inline `#[cfg(test)]` modules). |
| Persistence | `tests/persistence.rs` | Segment round-trip, WAL crash-recovery replay, mmap compaction, durability-failure events. |
| Hardening | `tests/hardening_fixes.rs` | Vocab-epoch staleness, fallible deserialization, reverse-index delete. |
| Coverage gaps | `tests/coverage_gaps.rs` | Parallel matching, compaction, broad-lane isolation, edge cases. |
| Error paths | `tests/error_paths.rs` | API error handling (parse errors, class-D rejection). |
| **Pressure / soak** | `tests/stress.rs` | Mixed read/write/delete churn, parallel-vs-sequential agreement under mutation, metrics/event consistency. Self-contained (seeded `gen`, no data files). |

## Pressure & soak tests

[`tests/stress.rs`](../engine/tests/stress.rs) holds the pressure suite. Its 15 normal tests run as
part of `cargo test --release` (and therefore on every PR). One large-scale test —
`ten_million_queries_mixed_ops` — is `#[ignore]`d because it needs ~4+ GiB and minutes; run it
explicitly:

```
cargo test --release --test stress -- --nocapture                          # the 15, with event logs
cargo test --release --test stress ten_million_queries_mixed_ops -- --ignored --nocapture   # the soak
```

In CI the soak runs only on a manual `workflow_dispatch` with `run_soak = true`.

## Benchmarks

Plain seeded binaries (not `criterion`), reproducible via a fixed seed (`0x00C0FFEE`):
`bench` (build/match throughput + cost-class split + memory), `segbench` (read-amplification vs
segment count), `snapbench` (snapshot-publish cost). **Commands, arguments, the machine-independent
invariants (the regression gate), and the dated capture log all live in one place —
[`performance/benchmark-results.txt`](performance/benchmark-results.txt); narrative analysis in
[`performance/results.md`](performance/results.md).** Don't restate numbers anywhere else.

The regression gate is a **manual** comparison: the *structural* invariants (candidates/title, filter
skip %, cost-class split, false-neg/pos = 0) are fixed by the data + algorithm and must reproduce on
any machine; *throughput* is hardware-dependent and is only ever compared against a prior run on the
same machine. CI runs the benchmarks and uploads their output as an artifact for review but **never
fails on them** (runner variance would false-alarm) — see ADR-024.

## Local workflow: git hooks

Run [`./setup-hooks.sh`](../setup-hooks.sh) once per clone (it points `core.hooksPath` at the
committed [`.githooks/`](../.githooks) dir). Then:

- **pre-commit** → `check.sh --fast` (fmt + clippy) — fast feedback on every commit.
- **pre-push** → `check.sh` (the full gate) — nothing reaches the remote unchecked.

Bypass in an emergency with `git commit --no-verify` / `git push --no-verify`; CI is still the backstop.

## CI: GitHub Actions

[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) runs on every PR, on push to `main`, and on
manual dispatch. One job on `ubuntu-latest`:

1. Toolchain from [`engine/rust-toolchain.toml`](../engine/rust-toolchain.toml) (rustup auto-installs
   the pinned rustc + `rustfmt`/`clippy`).
2. `Swatinem/rust-cache` (the release+LTO build is slow; caching is what keeps PR runs reasonable).
3. `cargo-audit` + `cargo-deny` installed as prebuilt binaries.
4. **`./engine/check.sh`** — the must-pass gate (now including the committed stress suite).
5. Benchmarks — run-and-print, `continue-on-error`, output uploaded as the `benchmark-output` artifact.
6. The 10M soak — only when dispatched with `run_soak = true`.

In-progress runs are cancelled when a newer commit lands on the same ref.

## Adding tests (for agents)

- Integration tests → a file in `engine/tests/`; unit tests → an inline `#[cfg(test)]` module next to
  the code. Keep data generation **seeded** (ADR-008) so the oracle and benchmarks stay reproducible.
- The oracle encodes the [correctness contract](design/README.md). If a change makes it fail, the
  change is wrong — don't relax the oracle.
- **Run `./engine/check.sh` before declaring work done** (or rely on the pre-push hook). CI will run
  exactly this.
