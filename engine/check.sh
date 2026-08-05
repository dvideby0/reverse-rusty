#!/usr/bin/env bash
# Local hardening gate for the Reverse Rusty engine crate.
#
# Runs the checks a CI pipeline would, in one shot: formatting, lints, tests,
# security advisories, and dependency/license policy. Run it before pushing or
# opening a PR — every step must pass for the gate to succeed.
#
#   Usage:  ./check.sh                    # full gate (every lane)
#           ./check.sh --fast             # quick gate (fmt + clippy only)
#           ./check.sh --lane core        # core/default code, policy, and crash lanes
#           ./check.sh --lane distributed # distributed clippy + tests
#
# Requires the rustfmt + clippy components (rustup) and two cargo plugins:
#   cargo install cargo-audit cargo-deny
#
# All steps run even if an earlier one fails, so a single invocation surfaces
# every problem at once; the script exits non-zero if any step failed.
#
# It also prints a non-failing advisory listing source files over 600 lines
# (refactor candidates). The advisory is informational only — it never affects
# the exit status, so an oversized file never blocks a commit, push, or CI run.

set -uo pipefail

# The no-argument command remains the complete local gate. CI selects the two
# independent lanes on separate runners, then requires both results; this keeps
# one command definition without serializing unlike feature builds.
fast=0
core=1
distributed=1
case "${1:-}" in
    "")
        [ "$#" -eq 0 ] || {
            echo "usage: ./check.sh [--fast | --lane core | --lane distributed]" >&2
            exit 2
        }
        ;;
    --fast)
        [ "$#" -eq 1 ] || {
            echo "usage: ./check.sh [--fast | --lane core | --lane distributed]" >&2
            exit 2
        }
        fast=1
        distributed=0
        ;;
    --lane)
        [ "$#" -eq 2 ] || {
            echo "usage: ./check.sh [--fast | --lane core | --lane distributed]" >&2
            exit 2
        }
        case "$2" in
            core)
                distributed=0
                ;;
            distributed)
                core=0
                ;;
            *)
                echo "unknown check lane: $2 (expected core or distributed)" >&2
                exit 2
                ;;
        esac
        ;;
    *)
        echo "usage: ./check.sh [--fast | --lane core | --lane distributed]" >&2
        exit 2
        ;;
esac

# Operate on the crate this script lives in, regardless of the caller's CWD.
cd "$(dirname "$0")"

# Keep build artifacts out of the source tree; share the dir the rest of the
# project uses (see CLAUDE.md). Respect an explicit override if already set.
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/reverse-rusty-target}"

failures=()

run() {
    local name="$1"
    shift
    printf '\n\033[1;34m==> %s\033[0m\n' "$name"
    if "$@"; then
        printf '\033[1;32m    OK: %s\033[0m\n' "$name"
    else
        printf '\033[1;31m    FAIL: %s\033[0m\n' "$name"
        failures+=("$name")
    fi
}

# cargo-audit scans every package resolved into Cargo.lock, including optional
# dependencies that no target or feature can activate. RUSTSEC-2026-0235 is one
# such edge today (ADR-168). Keep the exception safe by failing if *any* rkyv
# version enters the complete all-feature/all-target graph; that forces the
# advisory disposition to be revisited before rkyv can ship.
assert_rkyv_inactive() {
    local tree
    tree=$(cargo tree --all-features --target all --prefix none) || return 1
    if grep -q '^rkyv ' <<<"$tree"; then
        printf 'rkyv is active in the all-feature/all-target dependency graph; remove or reassess RUSTSEC-2026-0235\n' >&2
        return 1
    fi
}

# Advisory (non-failing): list source files over the line threshold as refactor
# candidates. Informational only — it never touches `failures` or the exit
# status. Scans the crate's own src/ + tests/ (.rs); bump `threshold` to retune.
size_advisory() {
    local threshold=600 big
    big=$(find src tests -name '*.rs' -type f 2>/dev/null | while read -r f; do
        n=$(wc -l <"$f" | tr -d '[:space:]')
        [ "$n" -gt "$threshold" ] && printf '%6d  %s\n' "$n" "$f"
    done | sort -rn)
    [ -z "$big" ] && return 0
    printf '\n\033[1;33m==> file-size advisory: %s file(s) over %d lines — consider refactoring (non-blocking)\033[0m\n' \
        "$(printf '%s\n' "$big" | grep -c .)" "$threshold"
    printf '%s\n' "$big"
    printf '\033[0;33m    advisory only — does not fail the gate\033[0m\n'
}

if [ "$core" -eq 1 ]; then
    run "rustfmt (--check)"    cargo fmt --check
    run "clippy (-D warnings)" cargo clippy --all-targets --release -- -D warnings
    # Lean-core lane: lints the library + non-server bins with the server/observability
    # stack gated off, so a stray `use` of a server-only crate in library code fails the
    # gate. Keeps the `--no-default-features` build (the lean dependency surface) honest.
    run "clippy (lean core)"   cargo clippy --no-default-features --release -- -D warnings
fi
if [ "$core" -eq 1 ] && [ "$fast" -eq 0 ]; then
    # The Cluster-v1 acceptance gate (tests/cluster_oracle.rs +
    # tests/cluster_durability_oracle.rs — see docs/testing.md) runs here on the default
    # feature set; the distributed-gated cluster oracles run in the `distributed` lane below.
    run "tests (--release)"    cargo test --release
fi
if [ "$distributed" -eq 1 ]; then
    # Distributed (gRPC ShardServer) lane: the default lanes never compile the
    # `distributed` feature, so without this the cluster gRPC code + its oracle would
    # rot. Uses the pure-Rust `protox` build-dep — no system `protoc` needed.
    run "clippy (distributed)" cargo clippy --features distributed --all-targets --release -- -D warnings
    run "tests (distributed)"  cargo test --features distributed --release
fi
if [ "$core" -eq 1 ] && [ "$fast" -eq 0 ]; then
    run "cargo audit"          cargo audit --ignore RUSTSEC-2026-0235
    run "inactive rkyv guard"  assert_rkyv_inactive
    # --all-features so the license/ban policy covers the DISTRIBUTED dependency graph
    # (the tonic TLS stack, ADR-071) — not just the default-feature tree.
    run "cargo deny"           cargo deny --all-features check
    # Independence gate (ADR-087): the front-end-INDEPENDENT correctness reference
    # (reverse-rusty-ref-matcher, used only by tests/independent_oracle) must reuse NONE of the
    # engine — that is the whole point. If `reverse-rusty` appears in its normal-dependency tree
    # the contract is broken, so fail loud. `--prefix none` prints each crate flush-left as
    # `name version (src)`; the anchored `^reverse-rusty ` (trailing space) matches the engine crate
    # EXACTLY, so neither the reference's own `reverse-rusty-ref-matcher` name nor the checkout path
    # trips it.
    run "ref-matcher independence" bash -c \
        '! cargo tree -q -p reverse-rusty-ref-matcher --edges normal --prefix none 2>/dev/null | grep -q "^reverse-rusty "'
    # Crash-injection lane (ADR-088): spawn the `crashwriter` bin,
    # SIGKILL it mid durable-op (WAL append / flush / compaction / backup / churn),
    # then diff the reopened engine against the front-end-independent oracle (zero
    # false negatives on every acked write). The scenarios are `#[ignore]`d (they
    # spawn + kill real processes and do real fsyncs) so the default `cargo test`
    # stays fast; run them explicitly here. `--test-threads=1` keeps concurrent
    # SIGKILLs from thrashing; `RR_CRASH_ITERS` (small default) scales the
    # kill/reopen cycles — a nightly job can bump it.
    run "crash injection" cargo test --release --test crash_injection -- --ignored --test-threads=1
fi

# Non-failing refactor nudge. The core lane owns it so split CI does not print
# the same advisory twice; full and --fast local runs remain unchanged.
if [ "$core" -eq 1 ]; then
    size_advisory
fi

printf '\n'
if [ "${#failures[@]}" -eq 0 ]; then
    printf '\033[1;32mAll checks passed.\033[0m\n'
    exit 0
fi

printf '\033[1;31m%d check(s) failed:\033[0m\n' "${#failures[@]}"
for f in "${failures[@]}"; do
    printf '  - %s\n' "$f"
done
exit 1
