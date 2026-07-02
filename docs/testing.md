# Testing, benchmarks & CI

How Reverse Rusty is verified — the suites, the pressure/soak tests, the benchmarks, the local git
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
cd engine && export CARGO_TARGET_DIR=/tmp/reverse-rusty-target   # or just ./engine/check.sh from the root
./check.sh          # full gate: fmt + clippy + test + audit + deny
./check.sh --fast   # quick gate: fmt + clippy only (what the pre-commit hook runs)
```

Every step runs even if an earlier one fails, so one invocation surfaces every problem; the script
exits non-zero if any step failed. It needs the `rustfmt` + `clippy` components (supplied by the
pinned toolchain) and two cargo plugins: `cargo install cargo-audit cargo-deny`.

It also prints a **non-failing file-size advisory** at the end of every run (full and `--fast`): any
`.rs` file under `src/` or `tests/` over 600 lines is listed as a refactor candidate. It is purely
informational — it never changes the exit status, so an oversized file never blocks a commit, push, or
CI run. Retune the threshold in `size_advisory()` in [`../engine/check.sh`](../engine/check.sh).

## Test suites

All live in `engine/` and run under `cargo test --release` (release because the oracle and stress
suites generate large seeded corpora — debug is far too slow). Run one suite with
`cargo test --release --test <name>`; unit tests with `cargo test --release --lib`.

| Suite | Where | Covers |
|---|---|---|
| **Differential oracle** | `tests/oracle.rs` | The **correctness contract** — brute force vs engine, asserting zero false negatives/positives ([`design/README.md`](design/README.md) §2). The load-bearing test; never weaken it. Includes the **messy-corpus** passes (`messy.rs` — the same contract over `gen::messify_dataset`'s adversarial surfaces, per-title + batch, ADR-063) and the **degenerate-input** differential (`degenerate.rs` — grammar/feature-model edges, engine ≡ brute on both ingest paths). |
| **Adversarial properties** | `tests/adversarial/` | **Reference-free** correctness properties that don't share code with the engine (ADR-063): the self-match diagonal (a query must match a title built from its own positive terms — clean, messy-query×clean-title, clean-query×perturbed-title), metamorphic set-identity under surface noise, the ADR-054/058/060/061 cross-form matrices (incl. the codex-R11 whitespace-run regression), and unicode-soup fuzz (no-panic, determinism, `P(T) ⊇ N(T)`, `match_features == N(T)`). These cover the front-end divergence the differential oracle is structurally blind to. |
| **Independent oracle** | `tests/independent_oracle/` | **Front-end-INDEPENDENT differential** (Phase 0 item 2, ADR-087): the engine diffed against `reverse-rusty-ref-matcher` — a std-only, zero-dependency reimplementation of the parser/normalizer/extractor/predicate from the spec that shares NO front-end code (independence enforced by a `check.sh` `cargo tree` lane). Zero FN/FP over generated default (clean + messy), populated graders/phrases, the multi-word alias two-view (controlled + ~989k-match at-scale), a hand-written **gotcha** table (asserted against both sides), and the env-gated `RR_ORACLE_CORPUS` real corpus (schema below). The differential the in-tree oracle structurally cannot be — closes the ADR-050 blind spot for the covered paths. |
| **Crash injection** | `tests/crash_injection/` | **Real-process SIGKILL** durability torture (Phase 0 item 3, ADR-088): spawns the `crashwriter` bin, delivers a real external SIGKILL mid durable-op (WAL append / flush / compaction / backup / churn / **upsert** / **watermark**), reopens in-process, and diffs the recovered engine against the ADR-087 independent oracle — zero FN on every ACKed write, no resurrection/corruption. `upsert` proves ADR-067 atomic replace (race-immune); `watermark` proves the ADR-066 `ensure_seq_after` re-pin across a second reopen; the **cluster** mid-write analogue is `deploy/harness.sh` leg 3b. The real-kill-mid-syscall check the chmod/torn-tail/CRC *simulations* cannot be. **`#[ignore]`d** (spawns + kills real processes, real fsyncs) behind a `check.sh` `crash injection` lane — see [Crash injection](#crash-injection). |
| **Broad-lane batch** | `tests/broad_batch.rs` | Broad-lane **batch ≡ scalar** equivalence matrix — the load-bearing batch-correctness deliverable ([`design/matching.md`](design/matching.md) §4). |
| Ranking | `tests/ranking.rs` | Engine-level ranking (ADR-059): additive scoring, newest-live-copy tag precedence, and the ranked-set ≡ unranked-set recall guard ([`design/matching.md`](design/matching.md) §5.4). |
| Unit tests | `src/*.rs` | DSL parsing, vocab, WAL framing, loader, anchor filter (inline `#[cfg(test)]` modules). |
| Persistence | `tests/persistence.rs` | Segment round-trip, WAL crash-recovery replay, mmap compaction, durability-failure events. |
| Hardening | `tests/hardening_fixes.rs` | Vocab-epoch staleness, fallible deserialization, reverse-index delete. |
| Coverage gaps | `tests/coverage_gaps.rs` | Parallel matching, compaction, broad-lane isolation, edge cases. |
| Error paths | `tests/error_paths.rs` | API error handling (parse errors, class-D rejection). |
| **Pressure / soak** | `tests/stress.rs` | Mixed read/write/delete churn, parallel-vs-sequential agreement under mutation, metrics/event consistency, and the ADR-099 **proves-work-stopped** cancellation legs (self-calibrating: cancelled wall-clock asserted against the measured uncancelled runtime). Self-contained (seeded `gen`, no data files). |
| **Cluster oracle** | `tests/cluster_oracle.rs` | Multi-shard differential oracle: cluster ≡ single-node ≡ brute, K∈{1,3,8,16} × broad × RF∈{1,2,3}; every placement class + fan-out asserted; dynamic-vocabulary absorb-correctly (hashed new tokens don't broaden, declared + auto-learned aliases make both surface forms match). **Half the Cluster-v1 gate (below).** |
| **Cluster durability** | `tests/cluster_durability_oracle.rs` | A `data_dir` cluster rebuilt from manifest + per-shard segments + coordinator log ≡ pre-crash ≡ brute, K∈{1,3,8} × broad; checkpoint, torn-tail recovery, fail-loud guards, alias-survives-reopen. **Half the Cluster-v1 gate (below).** |

### What the oracle does and does not verify

The differential oracle independently reimplements only the **back half** of the pipeline — candidate
retrieval and exact verification (a brute-force scan with its own `Dict`/`Normalizer` instances). For the
**front half** it calls the engine's own `dsl::parse`, `compile::extract`, and `Normalizer`, and runs them
under the empty `default_vocab`. So a semantic bug in the parser, the feature extractor, or the
normalization model would corrupt the brute-force ground truth and the engine identically — the oracle
would still pass — and the vocab-driven normalization paths (multiword phrases, synonyms, graders) are
never exercised by it at all. Those three front-end stages are instead pinned by **hand-authored golden
tests** (in-module `#[cfg(test)] mod golden` in `src/dsl.rs`, `src/normalize.rs`, `src/compile.rs`), whose
expected values are written from the spec ([`reference/dsl.md`](reference/dsl.md),
[`design/normalization.md`](design/normalization.md), [`design/matching.md`](design/matching.md) §1); the
vocab-driven path is additionally run end-to-end by `zero_false_negatives_with_populated_vocab` in
`tests/oracle.rs`. Rationale + the declined "independent reference extractor" alternative →
[`DECISIONS.md`](DECISIONS.md) ADR-050.

Two further layers close what golden tests can't (ADR-063): the `P(T)` parse-union oracle
(`src/normalize/parse_union_oracle.rs`) independently re-derives the positive title view by exhaustive
parse enumeration, and `tests/adversarial/` asserts **reference-free properties** — self-match,
metamorphic set-identity, cross-form matrices — whose ground truth is the contract itself, so a bug in
ANY shared front-end stage (including a query-side vs title-side asymmetry, the historical escape class)
fails them directly. The oracle's corpora also now include adversarial surfaces: `tests/oracle/messy.rs`
re-runs the differential over `gen::messify_dataset` output (case noise, whitespace runs, punctuation,
unicode junk, out-of-dict tokens), and `tests/oracle/degenerate.rs` pins grammar/feature-model edge
inputs. When adding corpus-driven tests, prefer running them messy unless there's a reason not to.

The **third layer (ADR-087)** is the one the blind-spot statement above could not have: a *differential*
against a front-end-INDEPENDENT reference. `tests/independent_oracle/` diffs the engine against
`reverse-rusty-ref-matcher` — a std-only crate that reimplements the parser, normalizer, extractor, and
predicate from the spec, depending on nothing in `reverse-rusty` (enforced by the `ref-matcher
independence` `check.sh` lane). So a parser/normalizer/extractor bug no longer corrupts both sides; it
shows up as a divergence. It runs over the same default vocab the in-tree oracle uses *and* populated
grader/phrase + multi-word-alias-two-view vocabularies, plus a hand-written gotcha table whose
expectations are the human tiebreaker. It does not replace the golden tests or the in-tree oracle — it
is the differential complement they structurally cannot be (full rationale, incl. why this revisits
ADR-050's declined "independent extractor", → [`DECISIONS.md`](DECISIONS.md) ADR-087).

**Real-corpus hook.** `tests/independent_oracle/corpus.rs` runs the same engine-vs-reference diff over a
user-supplied corpus when `RR_ORACLE_CORPUS` points at a JSONL file, and is skipped (passing) when the
variable is unset — so CI and the public repo never see real data (it stays entirely outside the tree).
Each line is one JSON object, two shapes (other keys ignored):

```jsonl
{"query": "1994 upper deck michael jordan -reprint"}   # a saved search (numbered in file order)
{"title": "1994 Upper Deck Michael Jordan SP PSA 10"}  # a listing title
```

It runs under the default vocabulary (the front-end check that needs no domain config). Run it with
`RR_ORACLE_CORPUS=/path/to/corpus.jsonl cargo test --release --test independent_oracle corpus`.

### The Cluster-v1 acceptance gate

`tests/cluster_oracle.rs` + `tests/cluster_durability_oracle.rs` are the **named acceptance gate for
Cluster v1** (the in-process multi-shard core + durable reopen + dynamic vocabulary): _cluster ≡
single-node ≡ brute_ and _reopen ≡ pre-crash ≡ brute_, with the dynamic-vocabulary absorb-correctly
assertions baked in (ADR-046). Both already run on the default `cargo test --release`, so the gate is
live — naming them here makes the contract explicit: keep them green, never weaken them. Two further
**lean-core** cluster oracles also run on the default `cargo test --release`:
`tests/cluster_control_plane_oracle.rs` (the `ControlPlane`-seam gate — ADR-037) and
`tests/cluster_allocator_oracle.rs` (the shard→node allocator gate — ADR-042), each asserting
`percolate` is byte-identical across a reassignment/rebalance. The
experimental distributed layers add three more oracles that `check.sh` runs in its
`--features distributed` lane — `tests/cluster_grpc_oracle.rs` (gRPC transport + dict shipping +
replication/recovery; the `block_on` **rayon-fanout** and **single-target-from-a-tokio-worker** guards;
and **remote partial-apply detection** over the wire — ADR-047), `tests/cluster_control_raft_oracle.rs`
(openraft control plane), and `tests/cluster_autoscale_oracle.rs` (autoscaler). Those are
oracle-proven **on localhost**, not a multi-machine gate. The partial-apply → `resync` **convergence**
cycle (ADR-047) is proven deterministically in the lean core by `cluster/coordinator/tests.rs`
(`partial_apply_is_detected_then_resync_converges` + `resync_requeues_when_shard_still_failing`).

## Pressure & soak tests

[`tests/stress/`](../engine/tests/stress/) holds the pressure suite. Its normal tests run as
part of `cargo test --release` (and therefore on every PR). One large-scale test —
`ten_million_queries_mixed_ops` — is `#[ignore]`d because it needs ~4+ GiB and minutes; run it
explicitly:

```
cargo test --release --test stress -- --nocapture                          # the normal suite, with event logs
cargo test --release --test stress ten_million_queries_mixed_ops -- --ignored --nocapture   # the soak
```

In CI the soak runs only on a manual `workflow_dispatch` with `run_soak = true`.

## Crash injection

[`tests/crash_injection/`](../engine/tests/crash_injection/) (ADR-088, Phase 0 item 3) is the
**real-process SIGKILL** durability torture: it spawns the `crashwriter` bin, delivers a real external
SIGKILL while a durable op is in flight, reopens the data dir in-process, and diffs the recovered engine
against the front-end-independent oracle (ADR-087) — proving every acknowledged write survives a crash
(zero false negatives) with no resurrection or corruption. It is the real-kill-mid-syscall check the
existing chmod / torn-tail / CRC *simulations* structurally cannot be.

The seven scenarios are `--workload`s steering the kill into one durable window: `wal_append`, `flush`,
`compaction`, `backup`, `churn` (delete-recovery), **`upsert`** (ADR-067 atomic replace), and
**`watermark`** (ADR-066 `ensure_seq_after` across a *second* reopen). The `upsert` check is
**race-immune** — the worker races upserts ahead of the parent's ACK stream through the stdout pipe
buffer, so the reference cannot assume "unrecorded ⇒ still old"; instead each id carries `qstem`/`qold`/
`qnew` tokens and a `both`-title that matches whichever version survived (`match(both_X) == {X}` catches
a vanish or corruption regardless of the race), with the stronger new-present/old-gone check applied
only to ids whose ACK the parent actually recorded. Its **cluster** analogue lives in the multi-machine
harness ([`deploy/harness.sh`](../deploy/harness.sh) leg 3b): SIGKILL a `shardserver` mid-write-loop,
restart it, converge the queued partial-applies with `POST /_cluster/resync` (ADR-047), and assert every
acknowledged (2xx) write is matchable — zero FN across a real kill mid-write.

The scenarios are `#[ignore]`d (they spawn + kill real processes and do real fsyncs) and run by the
full `check.sh` gate's `crash injection` lane. Run them explicitly with:

```
cargo test --release --test crash_injection -- --ignored --test-threads=1
RR_CRASH_ITERS=20 cargo test --release --test crash_injection -- --ignored --test-threads=1   # a deeper soak
```

`RR_CRASH_ITERS` (default 3) scales the kill/reopen cycles per scenario; a nightly job can bump it. To
confirm the harness still BITES, the suite's module header documents five mutations (drop recovered
inserts → FN; skip delete replay → FP; don't kill → the killed-assert fires; neuter the upsert
insert-half → "id VANISHED"; neuter `ensure_seq_after` → the watermark canary resurrects while churn
stays green) — all verified RED during development.

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
5. **`./deploy/local-smoke.sh --prebuilt`** — the Tier 5 M1 deployable smoke (ADR-098): both local
   modes (single-node + in-process cluster) end-to-end over the release bin — ingest, search,
   SIGTERM-restart-reopen, restore-from-backup. A deployment gate over the built artifact, like the
   harness; `check.sh` stays the engine-gate SSOT.
6. Benchmarks — run-and-print, `continue-on-error`, output uploaded as the `benchmark-output` artifact.
7. The 10M soak — only when dispatched with `run_soak = true`.

In-progress runs are cancelled when a newer commit lands on the same ref.

## The multi-machine harness (ADR-072)

The compose-based lifecycle suite — the analogue of the localhost oracles across **real container
network boundaries** (kill-and-recover, rolling restarts, coordinator restart, live handoff under
load, all on the fully secured ADR-071 mesh):

```bash
./deploy/harness.sh                                 # builds the image from source (slow first time)
./deploy/harness.sh --prebuilt engine/target/release  # wrap prebuilt LINUX bins (the CI path)
```

Requires Docker (compose v2), `curl`, `jq`, `openssl`. Generates an ephemeral CA + corpus per run
(nothing committed), brings up `deploy/compose.harness.yml` (3 durable shard nodes + a handoff
target + the REST coordinator + a 3-node control-plane quorum), runs the assertion legs, and tears
everything down — exit 0 ⇔ PASS. CI runs it on every PR as the `multi-machine harness` job
(natively built bins wrapped via `deploy/Dockerfile.prebuilt`). Its assertions are black-box REST
invariants: a dead shard **fails loud** (502, never a silently truncated result), every lifecycle
event lands **≡ the percolate baseline**, and every acknowledged write stays matchable across a
live cross-node handoff.

## Adding tests (for agents)

- Integration tests → a file in `engine/tests/`; unit tests → an inline `#[cfg(test)]` module next to
  the code. Keep data generation **seeded** (ADR-008) so the oracle and benchmarks stay reproducible.
- The oracle encodes the [correctness contract](design/README.md). If a change makes it fail, the
  change is wrong — don't relax the oracle.
- **Run `./engine/check.sh` before declaring work done** (or rely on the pre-push hook). CI will run
  exactly this.
