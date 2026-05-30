#!/usr/bin/env bash
# One-time per-clone setup: point git at the committed, version-controlled hooks
# in .githooks/ (git never tracks .git/hooks itself). Re-running is harmless.
#
#   ./setup-hooks.sh
#
# After this, `git commit` runs the fast gate (fmt + clippy) and `git push` runs
# the full gate (engine/check.sh). Bypass either with --no-verify if needed.
set -euo pipefail

cd "$(dirname "$0")"

chmod +x .githooks/pre-commit .githooks/pre-push
git config core.hooksPath .githooks

printf 'Git hooks activated: core.hooksPath -> %s\n' "$(git config core.hooksPath)"
printf '  pre-commit: engine/check.sh --fast (fmt + clippy)\n'
printf '  pre-push:   engine/check.sh        (fmt + clippy + test + audit + deny)\n'
