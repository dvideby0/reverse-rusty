#!/usr/bin/env bash
# Topology-parity tripwire (Tier 5 M2, ADR-098): the production Compose file
# (deploy/compose.cluster.yml) and the rendered Helm chart (default values) must describe
# the SAME topology — shard/control counts, the coordinator's routing flags, and the mesh
# ports. This is a grep-level drift check, not a semantic differ: it catches the "someone
# scaled one side and not the other" class of drift the deployability review flagged.
#
# Runs per-PR in the `helm chart` CI job and again in the release gate.
# Requires: docker (compose v2), helm.
set -euo pipefail

cd "$(dirname "$0")/.."
fail() { echo "FAIL: $*" >&2; exit 1; }
for tool in docker helm; do command -v "$tool" >/dev/null || fail "missing tool: $tool"; done

# Render both sides. The compose render is exactly the CI lint invocation (dummy env).
compose=$(RR_CLUSTER_TOKEN=parity RR_AUTH_TOKEN=parity RR_CERT_DIR=./deploy/certs RR_PORT=127.0.0.1:9200 \
  docker compose --project-directory . -f deploy/compose.cluster.yml config 2>/dev/null) ||
  fail "compose config failed"
chart=$(helm template rr deploy/helm/reverse-rusty 2>/dev/null) || fail "helm template failed"

expect_eq() { # label lhs rhs
  [[ "$2" == "$3" ]] || fail "$1 diverged: compose=$2 vs helm=$3"
  echo "  ok: $1 = $2"
}
expect_both() { # label needle
  grep -qF -- "$2" <<<"$compose" || fail "$1: '$2' missing from the compose render"
  grep -qF -- "$2" <<<"$chart" || fail "$1: '$2' missing from the helm render"
  echo "  ok: $1 present on both sides"
}

echo "==> topology parity (compose.cluster.yml ≡ helm default render)"

# Count a flag only where it renders as an ARGUMENT (a `- --flag` list item in both the
# compose `config` output and the helm manifests) — never in comments or scripts, which
# also mention the flags.
count_arg() { grep -cE "^[[:space:]]*- +--$2\$" <<<"$1" || true; }

# 1. Shard positions: the coordinator's --shard-endpoint count must match, and compose's
#    own shard service count must match its endpoint count (internal consistency).
c_shard_eps=$(count_arg "$compose" "shard-endpoint")
h_shard_eps=$(count_arg "$chart" "shard-endpoint")
expect_eq "shard endpoints (positions)" "$c_shard_eps" "$h_shard_eps"
c_shard_svcs=$(grep -cE '^  shard[0-9]+:' <<<"$compose")
[[ "$c_shard_svcs" == "$c_shard_eps" ]] ||
  fail "compose internal drift: $c_shard_svcs shard services vs $c_shard_eps --shard-endpoint flags"

# 2. Control quorum: compose control services == both sides' --control-endpoint counts.
c_ctrl_svcs=$(grep -cE '^  control[0-9]+:' <<<"$compose")
c_ctrl_eps=$(count_arg "$compose" "control-endpoint")
h_ctrl_eps=$(count_arg "$chart" "control-endpoint")
expect_eq "control endpoints" "$c_ctrl_eps" "$h_ctrl_eps"
[[ "$c_ctrl_svcs" == "$c_ctrl_eps" ]] ||
  fail "compose internal drift: $c_ctrl_svcs control services vs $c_ctrl_eps --control-endpoint flags"

# 3. The coordinator's routing posture (ADR-083/086) is on in BOTH shipped topologies.
expect_both "route-by-assignments" "--route-by-assignments"

# 4. The mesh ports agree (shard gRPC / control gRPC / coordinator REST).
for port in 50051 50061 9200; do
  expect_both "port $port" "$port"
done

echo "PASS: compose and helm describe the same topology ($c_shard_eps shards, $c_ctrl_svcs control nodes)"
