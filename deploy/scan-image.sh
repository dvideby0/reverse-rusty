#!/usr/bin/env bash
# Container image vulnerability scan (Phase 0 item 5 / threat model). Scans the
# Reverse Rusty node image with Trivy and prints OS-package + binary findings.
#
# The image is built from `debian:trixie-slim` (deploy/Dockerfile); this surfaces
# the base-image CVE hygiene. The APPLICATION's own dependency surface is covered
# separately by `cargo audit` + `cargo deny` in `engine/check.sh`.
#
# Usage:
#   deploy/scan-image.sh [IMAGE]            # HIGH,CRITICAL summary (default reverse-rusty:latest)
#   deploy/scan-image.sh [IMAGE] --full     # the full table, all severities
#   deploy/scan-image.sh [IMAGE] --strict    # exit non-zero if any HIGH/CRITICAL is found
#
# Uses a local `trivy` if installed, else the official `aquasec/trivy` image via the
# Docker socket. Triage of the current baseline: docs/operations/threat-model.md.
set -euo pipefail

IMAGE="reverse-rusty:latest"
SEVERITY="HIGH,CRITICAL"
EXIT_CODE=0
for arg in "$@"; do
  case "$arg" in
    --full)   SEVERITY="" ;;                 # empty => Trivy reports every severity
    --strict) EXIT_CODE=1 ;;                  # fail the script on a HIGH/CRITICAL hit
    -*)       echo "unknown flag: $arg" >&2; exit 2 ;;
    *)        IMAGE="$arg" ;;
  esac
done

# Common Trivy args: vuln scanner only, fixed/affected, the chosen severity + exit policy.
args=(image --scanners vuln --exit-code "$EXIT_CODE")
[[ -n "$SEVERITY" ]] && args+=(--severity "$SEVERITY")
args+=("$IMAGE")

echo "==> scanning $IMAGE (severity=${SEVERITY:-ALL}, strict-exit=$EXIT_CODE)"
if command -v trivy >/dev/null 2>&1; then
  exec trivy "${args[@]}"
elif command -v docker >/dev/null 2>&1; then
  # Mount the Docker socket so Trivy reads the LOCAL image from the daemon (no registry
  # push needed); the cache volume persists the vuln DB across runs.
  exec docker run --rm \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v rr-trivy-cache:/root/.cache/ \
    aquasec/trivy:latest "${args[@]}"
else
  echo "scan-image.sh: needs either a local 'trivy' or 'docker' to run the scanner" >&2
  exit 2
fi
