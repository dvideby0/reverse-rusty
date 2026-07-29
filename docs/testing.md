# Testing, benchmarks & CI

How Reverse Rusty is verified — the suites, the pressure/soak tests, the benchmarks, the local git
hooks, and the GitHub Actions pipeline. There is **one logical code/correctness/security gate**,
[`engine/check.sh`](../engine/check.sh). Its exact core and distributed subsets run concurrently in
CI behind one required result (ADR-151); the no-argument local command still runs both. ADR-124's
hardware-scoped performance blocker stays on the pinned core runner. Why these boundaries exist →
[`DECISIONS.md`](DECISIONS.md) ADR-024/124/151.

## TL;DR

- **Before you push:** run [`engine/check.sh`](../engine/check.sh) — or install the hooks once with
  [`./setup-hooks.sh`](../setup-hooks.sh) and they run it for you.
- **CI runs the same `check.sh` lanes** on every PR and push to `main`: core and distributed execute
  on separate runners, and one aggregate required check demands both. Green locally predicts the
  code gate; only the pinned core runner can issue the performance timing verdict.
- Test *counts* are never hand-maintained here — run `cargo test --release` for the live number.

## The code gate: `check.sh`

```
cd engine && export CARGO_TARGET_DIR=/tmp/reverse-rusty-target   # or just ./engine/check.sh from the root
./check.sh          # full gate: fmt + clippy + test + audit + deny
./check.sh --fast   # quick gate: fmt + clippy only (what the pre-commit hook runs)
./check.sh --lane core         # default/lean checks + policy + crash injection
./check.sh --lane distributed  # distributed-feature clippy + tests
```

Every selected step runs even if an earlier one fails, so one invocation surfaces every problem;
the script exits non-zero if any step failed. The two CI lane jobs are independent, so a failure in
one does not cancel the other; the required summary fails unless both passed. The core/full command
needs the `rustfmt` + `clippy` components (supplied by the pinned toolchain) and two cargo plugins:
`cargo install cargo-audit cargo-deny`. The distributed-only lane needs only the pinned toolchain.

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
| **Differential oracle** | `tests/oracle/` | The **correctness contract** — shared-front-end brute force vs engine, asserting zero false negatives/positives ([`design/README.md`](design/README.md) §2). The load-bearing retrieval/lowering test; never weaken it. Includes the **messy-corpus** passes (`messy.rs` — the same contract over `gen::messify_dataset`'s adversarial surfaces, per-title + batch, ADR-063) and the **degenerate-input** differential (`degenerate.rs` — grammar/feature-model edges, engine ≡ brute on both ingest paths). |
| **Adversarial properties** | `tests/adversarial/` | **Reference-free** correctness properties that don't share code with the engine (ADR-063): the self-match diagonal (a query must match a title built from its own positive terms — clean, messy-query×clean-title, clean-query×perturbed-title), metamorphic set-identity under surface noise, the ADR-054/058/060/061 cross-form matrices (incl. the codex-R11 whitespace-run regression), and unicode-soup fuzz (no-panic, determinism, `P(T) ⊇ N(T)`, `match_features == N(T)`). These cover the front-end divergence the differential oracle is structurally blind to. |
| **Independent semantic oracle** | `tests/independent_oracle/` | **Front-end and lowering-independent differential** (ADR-087/#123): the engine is diffed against `reverse-rusty-ref-matcher`, a std-only zero-dependency parser/normalizer plus direct grammar predicate tree. The reference contains no query frequencies, retrieval proxies, signature classes, or production exact-store lowering; code independence remains enforced by the `check.sh` `cargo tree` lane. It asserts zero final FN/FP and separately classifies every missed semantic truth as candidate-cover vs post-retrieval loss by observing the real stored posting/filter/lane traversal, with candidate generation judged for recall only. Coverage: generated default (clean + messy), populated aliases/phrases, quoted adjacency, multi-word alias two-view (controlled + ~989k-match at-scale), a hand-authored gotcha table, and the env-gated `RR_ORACLE_CORPUS` real corpus. Structural and human-expectation pins cover every clause boundary, complete multi-token any-of and negated-term predicates, and required/forbidden phrase graphs. |
| **Crash injection** | `tests/crash_injection/` | **Real-process SIGKILL** durability torture (ADR-088): spawns the `crashwriter` bin, delivers a real external SIGKILL mid durable-op (WAL append / flush / compaction / backup / churn / **upsert** / **watermark**), reopens in-process, and diffs the recovered engine against the ADR-087 independent oracle — zero FN on every ACKed write, no resurrection/corruption. `upsert` proves ADR-067 atomic replace (race-immune); `watermark` proves the ADR-066 `ensure_seq_after` re-pin across a second reopen; the **cluster** mid-write analogue is `deploy/harness.sh` leg 3b. The real-kill-mid-syscall check the chmod/torn-tail/CRC *simulations* cannot be. **`#[ignore]`d** (spawns + kills real processes, real fsyncs) behind a `check.sh` `crash injection` lane — see [Crash injection](#crash-injection). |
| **Broad-lane batch** | `tests/broad_batch.rs` | Broad-lane **batch ≡ scalar** equivalence matrix — including positive and negated compound any-of members under materialize/prefilter on and off — the load-bearing batch-correctness deliverable ([`design/matching.md`](design/matching.md) §4). |
| **Quoted phrases** | `tests/quoted_phrases.rs` | ADR-120 hand-authored truth tables for required/forbidden adjacency and order, default split vs configured fold punctuation, alternate alias paths without conjunction weakening, independent-reference agreement, and requested-columnar batch parity through the positioned fallback. |
| **Ranked batch** | `tests/ranked_batch.rs` | ADR-112 **batch ≡ per-title scalar top-K** differential (winners + totals; order-dependent rank counters deliberately uncompared): scope × strategy × materialize/prefilter × chunk size × K × threshold, filtered, ADR-106 dedup-heavy with tombstoned leader/member, multi-word-alias forced-inline, admission rejects, expired-deadline all-or-nothing. Cluster legs: `cluster_oracle/ranked_batch` (batch ≡ per-title distributed ≡ standalone; cross-title fetch dedup + under-credit 413; admission), `cluster_grpc_oracle/ranked_batch` (real servers: batch ≡ single RPCs ≡ reference; frame-cap refusal; deadline), and the durability `ranked` leg carries the batch view through checkpoint/reopen/backup-restore (mmap-backed coverage). |
| **Exhaustive delivery** | `tests/exhaustive_delivery.rs` + coordinator exhaustive units + `tests/cluster_grpc_oracle/exhaustive.rs` + server job tests | ADR-114 bounded chunks ≡ compatibility all-ID collection across A/B/C/D/H and both scopes; legacy duplicate physical rows (including an older matching body beneath a newer non-match), pre-setup cancellation, cancellation polling inside both duplicate-selection and newest-live ranked-metadata 2K-copy reverse-index scans, and per-member polling through a 2K-member all-dead canonical-body group; cluster ownership/resequencing plus fail-closed broad-evaluator admission; a cross-owner partially-applied upsert is refused before emission until resync, an injected successful-lower/failing-later initial bulk ingest revokes convergence authority despite an empty repair map, populated remote attachment remains unauthoritative, and a second coordinator is rejected by an attested node lease even while every shared shard is empty; a successful concurrent re-placement is held behind the full-stream mutation barrier and barrier-before-logical lock order is pinned; injected sink failure at every chunk boundary (never a terminal summary), final-send post-polling, zero-chunk out-of-band cancellation, cancellation during a backpressured completion send retaining the `cancelled` lifecycle, terminal status withheld until completion dequeue (queued-response drop fails without summary and its first invalid cause survives a later DELETE), and deterministic cancellation/deadline-versus-dequeue arbitration through one terminal transition; gRPC full-channel deadline bounding, pre-spawn node admission, queued-closure permit expiry, immediate watcher-sender release after worker start, rejection of caller deadlines above the server-owned ceiling, Tokio capacity validation without constructor panics, non-runtime-worker timer construction, real-wire exact scored stream + frame-cap failure; score-presence checksum collision regression; boot-unique job-generation namespace; raw-semantic HTTP idempotency across standalone tag-dictionary growth plus ambiguous synthetic-boost collision rejection, strict pre-claim query/method handling, NDJSON framing/header and local/coordinator route parity, and busy admission preserving retained history; shutdown/cancelled write-barrier waits and disconnected consumers release without completion. |
| Ranking | `tests/ranking.rs` + cluster ranking suites + server handler tests | Compatibility ranking (ADR-059/075) plus bounded typed ranking (ADR-107/108/110): collect-all/full-sort differentials, signed/saturating scores, ties, filters/scopes, newest-live duplicate precedence, K/threshold bounds, every A/B/C/D/H class, dynamic vocabulary/canonical bodies, strict HTTP ingest, permit deadlines, and winner-only fail-closed enrichment ([`design/matching.md`](design/matching.md) §5.4). |
| Unit tests | `src/*.rs` | DSL parsing, vocab, WAL framing, loader, anchor filter (inline `#[cfg(test)]` modules). |
| Persistence | `tests/persistence/` + storage/cluster unit modules | Current segment-v10 / compiler-semantics-v6 / manifest-v7 / WAL-v7 paths plus all supported legacy reads and semantic fences: round-trip, crash replay, mmap compaction, source-generation/watermark continuity, tags/rank/placement/compound/phrase columns, pre-dedup ranking-count migration, malformed-column/program refusal, checkpoint/reopen, backup/restore, and rebuild-only cluster migrations. The authoritative readable/current version matrix is [`operations/rolling-upgrade.md`](operations/rolling-upgrade.md). |
| Hardening | `tests/hardening_fixes/` | Vocab-epoch staleness, fallible deserialization, reverse-index delete. |
| Coverage gaps | `tests/coverage_gaps/` | Parallel matching, compaction, broad-lane isolation, edge cases. |
| Error paths | `tests/error_paths.rs` | API error handling (parse errors, class-D rejection). |
| **Pressure / soak** | `tests/stress/` | Mixed read/write/delete churn, parallel-vs-sequential agreement under mutation, metrics/event consistency, and the ADR-099 **proves-work-stopped** cancellation legs (self-calibrating: cancelled wall-clock asserted against the measured uncancelled runtime). ADR-123 adds deterministic 256-work counter tests for scalar + columnar in-segment cancellation/no-partial cleanup, collector-failure precedence at a coincident poll, and an ignored dense-posting/body-group before/after overshoot benchmark. Self-contained (seeded `gen`, no data files). |
| **Cluster oracle** | `tests/cluster_oracle/` | Multi-shard differential oracle: cluster ≡ single-node ≡ brute, K∈{1,3,8,16} × broad × RF∈{1,2,3}; every A/B/C/D/H placement class + fan-out asserted; filters/ranking/ties, any-of, canonical bodies, dynamic vocabulary, repeated resize, and ADR-109 owned replies with `duplicate_emissions == 0`. ADR-110 compares bounded distributed top-K against single-node collect-all/full-sort over K/threshold matrices, including global-threshold overflow, K=0, stale placement, and missing current source. **Half the Cluster-v1 gate (below).** |
| **Cluster durability** | `tests/cluster_durability_oracle/` | A `data_dir` cluster rebuilt from manifest + per-shard segments + coordinator log tail ≡ pre-crash ≡ brute, K∈{1,3,8} × broad; checkpoint, both compaction paths, backup/restore, torn-tail recovery, migration fences, vocabulary rebuild, resize/reopen, ownership-generation preservation, and ADR-110 top-K/winner-source continuity. **Half the Cluster-v1 gate (below).** |
| **Distributed gRPC** | `tests/cluster_grpc_oracle/` | Localhost wire oracle for co-location, RF>1 failover, peer recovery under writes, retained-member fingerprints, live handoff/reassignment/reconcile, protocol ownership attestation, missing/stale peer refusal, and zero-FN result identity. ADR-110 adds real-wire top-K/source streaming, exact response caps, mixed-version refusal, one absolute deadline, post-freeze signed priority, and bounded failover/recovery/handoff reads. Requires localhost TCP permission. |
| **Cluster scale soak** | `tests/cluster_soak/` | The **≥20M multi-shard scale proof** (ADR-104, the scale half of Distributed-v1 criterion 12): a durable K=8 in-process cluster at 20M queries ≡ the single-node engine over 50k titles, planted absolute-FN sentinels, mirrored live mutations (incl. a synthetic-ID retrievability check), and a checkpoint → reopen re-verify. `#[ignore]`d, **run explicitly by name only — in no gate and no CI workflow** (a one-off acceptance run; numbers pinned in [`performance/benchmark-results.txt`](performance/benchmark-results.txt)) — see [Pressure & soak](#pressure--soak-tests). |

### What the oracle does and does not verify

The main differential oracle independently checks the **retrieval and lowering back half**: it scans
every extracted query predicate for brute-force truth, then compares that result with the signature
index + exact store. It deliberately shares the production `dsl::parse`, `compile::extract`, and
`Normalizer` front end. The suite exercises default, populated-vocabulary, alias, phrase, messy, and
degenerate paths, but a front-end semantic bug can still corrupt both sides identically.

Hand-authored golden tests pin parser/normalizer/extractor expectations from
[`reference/dsl.md`](reference/dsl.md), [`design/normalization.md`](design/normalization.md), and
[`design/matching.md`](design/matching.md). The populated-vocabulary differential lives under
`tests/oracle/`. ADR-050 records the original shared-front-end gap; ADR-087 subsequently added the
separate independent reference below.

Two further layers close what golden tests can't (ADR-063): the `P(T)` parse-union oracle
(`src/normalize/parse_union_oracle.rs`) independently re-derives the positive title view by exhaustive
parse enumeration, and `tests/adversarial/` asserts **reference-free properties** — self-match,
metamorphic set-identity, cross-form matrices — whose ground truth is the contract itself, so a bug in
ANY shared front-end stage (including a query-side vs title-side asymmetry, the historical escape class)
fails them directly. The oracle's corpora also now include adversarial surfaces: `tests/oracle/messy.rs`
re-runs the differential over `gen::messify_dataset` output (case noise, whitespace runs, punctuation,
unicode junk, out-of-dict tokens), and `tests/oracle/degenerate.rs` pins grammar/feature-model edge
inputs. When adding corpus-driven tests, prefer running them messy unless there's a reason not to.

The **third layer (ADR-087, hardened by issue #123)** is a differential against a front-end and
compiler-lowering **independent** reference.
`tests/independent_oracle/` diffs the engine against
`reverse-rusty-ref-matcher` — a std-only crate that independently parses and normalizes the DSL, then
retains it as explicit `RequiredTerm` / `RequiredPhrase` / `RequiredAnyOf` and complete forbidden
semantic clauses. It depends on nothing in `reverse-rusty` (enforced by the `ref-matcher independence`
`check.sh` lane) and contains none of the production compiler's frequencies, rarest-member proxies,
signature classes, or exact-store columns. A code-local parser/normalizer bug or a shared lowering
mistake therefore shows up as a divergence. It runs over the same default vocab the in-tree oracle uses
*and* populated
alias/phrase + multi-word-alias-two-view vocabularies, plus a hand-written gotcha table whose
expectations are the human tiebreaker. It does not replace the golden tests or the in-tree oracle — it
is the differential complement they structurally cannot be. Final match sets are compared exactly
(zero FN and zero FP). Candidate generation is compared **only for recall**: an engine miss is
classified through a candidate-only traversal of the stored indexes as a candidate-cover miss or a
post-retrieval verification miss;
extra candidates are explicitly legal. Human semantic pins for every clause boundary, complete
multi-token any-of predicates, and required/forbidden phrase adjacency remain load-bearing (full
rationale → [`DECISIONS.md`](DECISIONS.md) ADR-087/118/119/120).

**Real-corpus hook.** `tests/independent_oracle/corpus.rs` runs the same engine-vs-reference diff over a
user-supplied corpus when `RR_ORACLE_CORPUS` points at a JSONL file, and is skipped (passing) when the
variable is unset — so CI and the public repo never see real data (it stays entirely outside the tree).
Each line is one JSON object, two shapes (other keys ignored):

```jsonl
{"query": "2024 north star wireless mouse -refurbished"} # a saved search (numbered in file order)
{"title": "2024 North Star Wireless Mouse Pro New"}      # a listing title
```

It runs under the default vocabulary (the front-end check that needs no domain config). Run it with
`RR_ORACLE_CORPUS=/path/to/corpus.jsonl cargo test --release --test independent_oracle corpus`.

### The Cluster-v1 acceptance gate

`tests/cluster_oracle/` + `tests/cluster_durability_oracle/` are the **named acceptance gate for
Cluster v1** (the in-process multi-shard core + durable reopen + dynamic vocabulary): _cluster ≡
single-node ≡ brute_ and _reopen ≡ pre-crash ≡ brute_, with the dynamic-vocabulary absorb-correctly
assertions baked in (ADR-046). Both already run on the default `cargo test --release`, so the gate is
live — naming them here makes the contract explicit: keep them green, never weaken them. Two further
**lean-core** cluster oracles also run on the default `cargo test --release`:
`tests/cluster_control_plane_oracle.rs` (the `ControlPlane`-seam gate — ADR-037) and
`tests/cluster_allocator_oracle.rs` (the shard→node allocator gate — ADR-042), each asserting
`percolate` is byte-identical across a reassignment/rebalance. The
experimental distributed layers add three more oracles that `check.sh` runs in its
`--features distributed` lane — `tests/cluster_grpc_oracle/` (gRPC transport + dict shipping +
replication/recovery; the `block_on` **rayon-fanout** and **single-target-from-a-tokio-worker** guards;
**remote partial-apply detection** over the wire — ADR-047; ADR-109 generation/configuration,
ownership-applied reply, recovery/fingerprint, old-peer, and stale-peer guards; and ADR-110 bounded
top-K/fetch, cap, deadline, mixed-version, failover, and handoff guards),
`tests/cluster_control_raft_oracle.rs`
(openraft control plane), and `tests/cluster_autoscale_oracle.rs` (autoscaler). Those are
oracle-proven **on localhost**. The Compose harness additionally crosses real single-host container
network/process boundaries, but neither is an independent multi-machine gate. The partial-apply → `resync` **convergence**
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

CI runs the exact target weekly (Monday 03:37 UTC) in
[`soak.yml`](../.github/workflows/soak.yml), with runner metadata, full output, and
`/usr/bin/time -v` retained for 90 days. It remains manually runnable from that workflow; the
backward-compatible `CI → run_soak = true` dispatch remains too.

[`tests/cluster_soak/`](../engine/tests/cluster_soak/) holds the **cluster scale soak** (ADR-104) —
`twenty_million_multi_shard_soak`, the ≥20M multi-shard proof described in the suite table above. It
is also `#[ignore]`d, but unlike the 10M soak it is **wired into no CI dispatch at all**: it was a
one-off local acceptance run (~4 min, ~16 GB peak RSS, ~3.2 GB temp disk on the capture machine),
kept in-tree so the ADR-104 evidence is reproducible. Run it explicitly, scaling down via env knobs
for a harness smoke:

```
cargo test --release --test cluster_soak -- --ignored --nocapture            # the canonical 20M / 50k / K=8 run
RR_CLUSTER_SOAK_QUERIES=200000 RR_CLUSTER_SOAK_TITLES=5000 \
  cargo test --release --test cluster_soak -- --ignored --nocapture          # ~3s harness smoke
```

(`RR_CLUSTER_SOAK_QUERIES` / `_TITLES` / `_SHARDS` size the run; `RR_CLUSTER_SOAK_DIR` relocates the
durable cluster's temp dir.)

## Crash injection

[`tests/crash_injection/`](../engine/tests/crash_injection/) (ADR-088) is the
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
only to ids whose ACK the parent actually recorded. Its **cluster** analogue lives in the container-network
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

Plain seeded binaries (not `criterion`), reproducible via fixed seeds:
`bench` (build/match throughput + cost-class split + memory), `segbench` (read-amplification vs
segment count), `snapbench` (snapshot-publish cost), `clusterbench` (routing fan-out), and
`rankbench` (bounded-ranking behavior/cost, including named CPU profiles). `rankbench` keeps its
four historical arguments and optionally accepts `<profile_file> <profile_name>`; omission runs
`static_v1`. Profile tests cover strict config admission, pinned fingerprints, title-dependent
scalar/batch equivalence, unchanged Boolean membership, semantic compound-group counts across
retrieval-proxy and exact-predicate deduplication, source-driven legacy migration, REST selection,
and fail-loud unknown profiles.
**Commands, arguments, the broader machine-independent invariants, and the dated capture log live
in one place —
[`performance/benchmark-results.txt`](performance/benchmark-results.txt); narrative analysis in
[`performance/results.md`](performance/results.md).** Don't restate numbers anywhere else.

ADR-124 adds a deliberately smaller automated contract in `perfgate`:

- runner: public standard `ubuntu-24.04` x64 (4 vCPU / 16 GiB), release+LTO,
  `RAYON_NUM_THREADS=4`;
- workload: 1M queries / 20k titles / 5% broad / seed `0x00C0FFEE`;
- exact structure: classes, dictionary, posting shape, candidate sum/p95/p99/max, match sum;
- resources: persistent `retain_source=false` resident bytes and logical durable bytes may grow at
  most 5%; durable file count is exact;
- timing: seven p50/p95/p99 rounds plus nine selective and columnar throughput windows. Absolute
  safety limits always apply: p50 ≤ 7.5 µs, p95 ≤ 75 µs, p99 ≤ 100 µs, selective throughput ≥
  120k titles/s, and columnar throughput ≥ 180k titles/s. Once reviewed history is populated, the
  additional relative band is `max(30% of the reference median, 3 × historical MAD)`; a timing-only
  breach retries the whole timing window once. Structure/resources never retry.

After an intentional workload-semantic rewrite, the checked-in baseline may temporarily carry no
timing source runs or samples. In that explicit all-empty state, `perfgate check` still blocks on
deterministic structure, resources, and the absolute timing safety limits; only the more sensitive
variance-band comparison is pending. Partial histories fail validation. The normal `rebaseline`
command requires five fresh reviewed CI reports and atomically adds the relative history without
removing the safety limits.

The reviewed baseline is
[`performance/perf-baseline.json`](performance/perf-baseline.json). Every PR uploads its
`perf-current.json`. Deep `bench`/`segbench`/`snapbench`/`clusterbench`/`rankbench` sweeps remain advisory
because they include signals too noisy or expensive to block a merge.

An intentional rebaseline requires at least five distinct CI reports and a reason; the command
refuses local/mixed contracts and never selects a retry over a first attempt:

```bash
cd engine
RR_PERF_ACCEPT_REBASELINE=1 cargo run --release --bin perfgate -- \
  rebaseline ../docs/performance/perf-baseline.json \
  "why the reviewed performance shape changed" \
  /path/to/run-{1,2,3,4,5}/perf-current.json
git diff -- ../docs/performance/perf-baseline.json   # review before commit
```

Outside the pinned runner, `perfgate capture /tmp/perf-current.json` is diagnostic only; `check`
fails loud on the runner mismatch rather than making a cross-machine timing claim.

## Local workflow: git hooks

Run [`./setup-hooks.sh`](../setup-hooks.sh) once per clone (it points `core.hooksPath` at the
committed [`.githooks/`](../.githooks) dir). Then:

- **pre-commit** → `check.sh --fast` (fmt + clippy) — fast feedback on every commit.
- **pre-push** → `check.sh` (the full gate) — nothing reaches the remote unchecked.

Bypass in an emergency with `git commit --no-verify` / `git push --no-verify`; CI is still the backstop.

## CI: GitHub Actions

[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) runs on every PR, on push to `main`, and on
manual dispatch. ADR-151 splits the logical gate across pinned `ubuntu-24.04` runners:

1. Both jobs install the toolchain from
   [`engine/rust-toolchain.toml`](../engine/rust-toolchain.toml). The core lane preserves its
   existing `Swatinem/rust-cache` namespace; the distributed lane reads the production distributed
   cache without saving test artifacts back into it.
2. The **core lane** installs prebuilt `cargo-audit` + `cargo-deny`, then runs
   **`./engine/check.sh --lane core`**: format, default/lean Clippy, default release tests (including
   the committed stress suite), dependency policy, reference independence, and crash injection.
3. Concurrently, the **distributed lane** runs
   **`./engine/check.sh --lane distributed`**: the same distributed-feature Clippy and release
   tests that the complete local command runs.
4. A final **`gate + benchmarks`** result retains the established required-check name and succeeds
   only when both lanes succeeded.
5. On the core runner, **`./deploy/local-smoke.sh --prebuilt`** performs the deployable smoke
   (ADR-098): both local
   modes (single-node + in-process cluster) end-to-end over the release bin — ingest, search,
   SIGTERM-restart-reopen, restore-from-backup. A deployment gate over the built artifact, like the
   harness; `check.sh` stays the engine-gate SSOT.
6. **`perfgate check`** follows core validation on that same otherwise-idle pinned runner — the
   merge-blocking ADR-124 performance/resource contract; current JSON
   uploaded in the `benchmark-output` artifact.
7. Deep benchmarks — run-and-print, `continue-on-error`, diagnostic output uploaded with the gate
   report.
8. The 10M soak — only when this compatibility workflow is dispatched with `run_soak = true`.

In-progress runs are cancelled when a newer commit lands on the same ref.

[`soak.yml`](../.github/workflows/soak.yml) independently schedules the same exact 10M target
weekly and supports manual dispatch. Schedule delay does not affect correctness; its evidence
artifact is retained for 90 days. The 20M cluster soak remains local/manual because its recorded
~16 GiB peak would consume the whole standard runner.

The `helm chart` job additionally runs the compose↔chart **topology-parity** and **version-drift**
tripwires (`deploy/check-topology-parity.sh`, `deploy/check-versions.sh` — ADR-098). A `v*` tag
triggers [`release.yml`](../.github/workflows/release.yml): version preflight → build → smoke the
exact candidate image (Compose + kind/Helm + parity) → publish to GHCR (`vX.Y.Z` + `X.Y.Z` — the chart's default `image.tag` — + `sha-<short>`,
never `:latest`); a `workflow_dispatch` run is the same pipeline with publishing skipped — the
no-tag rehearsal.

## The container-network harness (ADR-072)

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
everything down — exit 0 ⇔ PASS. CI runs it on every PR with natively built binaries wrapped by
`deploy/Dockerfile.prebuilt` (the workflow retains the historical `multi-machine harness` job label),
followed by the production-compose
smoke (`deploy/cluster-smoke.sh`) on the same image (ADR-098). Its assertions are black-box REST
invariants: a dead shard **fails loud** (502, never a silently truncated result), every lifecycle
event lands **≡ the percolate baseline**, and every acknowledged write stays matchable across a
live cross-process handoff. Because all containers share one host, this proves process/network
boundaries but not independent-machine failure domains.

## Adding tests (for agents)

- Small integration targets may be one file in `engine/tests/`; larger targets follow the existing
  `tests/<target>/main.rs` + focused module pattern. Unit tests stay in an inline `#[cfg(test)]` module
  next to the code. Keep data generation **seeded** (ADR-008) so the oracle and benchmarks remain
  reproducible.
- The oracle encodes the [correctness contract](design/README.md). If a change makes it fail, the
  change is wrong — don't relax the oracle.
- **Run `./engine/check.sh` before declaring work done** (or rely on the pre-push hook). CI will run
  exactly this.
