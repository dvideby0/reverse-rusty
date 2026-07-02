#!/usr/bin/env bash
# Version-consistency guard (Tier 5 M2, ADR-098).
#
#   deploy/check-versions.sh            # per-PR drift check: engine crate == Helm appVersion
#   deploy/check-versions.sh vX.Y.Z     # release preflight: the tag must ALSO equal both
#
# The release workflow runs the tag form as step 0 — fail loud on a version mismatch BEFORE
# spending minutes compiling. Pure grep/sed, no dependencies.
set -euo pipefail

cd "$(dirname "$0")/.."
fail() { echo "FAIL: $*" >&2; exit 1; }

crate=$(sed -n 's/^version *= *"\([^"]*\)".*/\1/p' engine/Cargo.toml | head -1)
chart=$(sed -n 's/^appVersion:[[:space:]]*//p' deploy/helm/reverse-rusty/Chart.yaml | head -1 | tr -d '"')
[[ -n "$crate" ]] || fail "could not read version from engine/Cargo.toml"
[[ -n "$chart" ]] || fail "could not read appVersion from deploy/helm/reverse-rusty/Chart.yaml"

[[ "$crate" == "$chart" ]] || fail "version drift: engine/Cargo.toml=$crate vs Chart appVersion=$chart
Bump BOTH files to the same version before tagging."

if [[ -n "${1:-}" ]]; then
  tag=$1
  [[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "tag '$tag' is not semver-shaped (vX.Y.Z)"
  [[ "${tag#v}" == "$crate" ]] || fail "tag $tag != crate/chart version $crate
Bump engine/Cargo.toml [package].version AND deploy/helm/reverse-rusty/Chart.yaml appVersion to ${tag#v}, or tag v$crate."
  echo "PASS: tag $tag == crate $crate == chart appVersion $chart"
else
  echo "PASS: crate $crate == chart appVersion $chart"
fi
