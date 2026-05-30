#!/usr/bin/env bash
# Local hardening gate for the Percolator engine crate.
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
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/perc-target}"

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

run "rustfmt (--check)"    cargo fmt --check
run "clippy (-D warnings)" cargo clippy --all-targets --release -- -D warnings
if [ "$fast" -eq 0 ]; then
    run "tests (--release)"    cargo test --release
    run "cargo audit"          cargo audit
    run "cargo deny"           cargo deny check
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
