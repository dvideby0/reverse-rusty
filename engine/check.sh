#!/usr/bin/env bash
# Local hardening gate for the Reverse Rusty engine crate.
#
# Runs the checks a CI pipeline would, in one shot: formatting, lints, tests,
# security advisories, and dependency/license policy. Run it before pushing or
# opening a PR — every step must pass for the gate to succeed.
#
#   Usage:  ./check.sh           # full gate (fmt + clippy + test + audit + deny)
#           ./check.sh --fast    # quick gate (fmt + clippy only) — used by the pre-commit hook
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

# --fast skips the slow steps (test/audit/deny) so it can run on every commit;
# the full gate still runs on push and in CI. Keeping both behind one script
# means the checks are defined in exactly one place.
fast=0
if [ "${1:-}" = "--fast" ]; then
    fast=1
fi

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

run "rustfmt (--check)"    cargo fmt --check
run "clippy (-D warnings)" cargo clippy --all-targets --release -- -D warnings
# Lean-core lane: lints the library + non-server bins with the server/observability
# stack gated off, so a stray `use` of a server-only crate in library code fails the
# gate. Keeps the `--no-default-features` build (the lean dependency surface) honest.
run "clippy (lean core)"   cargo clippy --no-default-features --release -- -D warnings
if [ "$fast" -eq 0 ]; then
    # The Cluster-v1 acceptance gate (tests/cluster_oracle.rs +
    # tests/cluster_durability_oracle.rs — see docs/testing.md) runs here on the default
    # feature set; the distributed-gated cluster oracles run in the `distributed` lane below.
    run "tests (--release)"    cargo test --release
    # Distributed (gRPC ShardServer) lane: the default lanes never compile the
    # `distributed` feature, so without this the cluster gRPC code + its oracle would
    # rot. Uses the pure-Rust `protox` build-dep — no system `protoc` needed.
    run "clippy (distributed)" cargo clippy --features distributed --all-targets --release -- -D warnings
    run "tests (distributed)"  cargo test --features distributed --release
    run "cargo audit"          cargo audit
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
fi

# Non-failing refactor nudge. Runs in --fast and full, so it shows on commit,
# push, and CI; printed just before the summary to stay visible.
size_advisory

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
