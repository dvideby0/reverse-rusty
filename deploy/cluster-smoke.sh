#!/usr/bin/env bash
# A minimal smoke test for the PRODUCTION compose (deploy/compose.cluster.yml,
# ADR-081): stand the cluster up from the image, ingest one query over REST, prove a
# title percolates to it, then tear down. This is NOT the lifecycle harness — the
# kill/restart/handoff/recover legs live in deploy/harness.sh (ADR-072). This only
# answers "does the shipped production compose come up green and serve a match?".
#
# Usage:
#   deploy/cluster-smoke.sh                 # build the image from source, then smoke
#   RR_IMAGE=reverse-rusty:latest deploy/cluster-smoke.sh   # reuse an existing image
#
# Requires: docker (compose v2), curl, jq, openssl. Exits 0 on PASS.
set -euo pipefail

cd "$(dirname "$0")/.."
REPO_ROOT=$(pwd)
COMPOSE_FILE="$REPO_ROOT/deploy/compose.cluster.yml"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/rr_smoke.XXXXXX")

export RR_IMAGE="${RR_IMAGE:-reverse-rusty:smoke}"
export RR_CLUSTER_TOKEN="smoke-mesh-secret-$$"
export RR_AUTH_TOKEN="smoke-auth-secret-$$"
export RR_CERT_DIR="$WORK/certs"
export RR_PORT="127.0.0.1:19200"   # avoid colliding with a real 9200 on the host
BASE="http://127.0.0.1:19200"

# A dedicated project name + the repo root as project dir (so RR_CERT_DIR=./... and
# any relative path resolve from the repo root, not deploy/).
compose() { docker compose -p rrsmoke --project-directory "$REPO_ROOT" -f "$COMPOSE_FILE" "$@"; }
auth=(-H "authorization: Bearer $RR_AUTH_TOKEN")

cleanup() {
  status=$?
  [[ $status -ne 0 ]] && { echo "--- smoke FAILED (exit $status); recent logs:" >&2; compose logs --tail 100 >&2 || true; }
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$WORK"
  exit $status
}
trap cleanup EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }

for tool in docker curl jq openssl; do command -v "$tool" >/dev/null || fail "missing tool: $tool"; done

echo "==> mesh certs (SANs cover the compose service names)"
deploy/gen-mesh-certs.sh "$RR_CERT_DIR" shard0 shard1 shard2 coordinator control0 control1 control2 localhost >/dev/null

if ! docker image inspect "$RR_IMAGE" >/dev/null 2>&1; then
  echo "==> build image $RR_IMAGE (slow on a cold cache)"
  docker build -q -f deploy/Dockerfile -t "$RR_IMAGE" "$REPO_ROOT" >/dev/null
fi

echo "==> up (K=3 shards + coordinator + control plane), waiting for healthy"
compose down -v --remove-orphans >/dev/null 2>&1 || true
compose up -d --wait --quiet-pull

echo "==> wait for coordinator green"
for _ in $(seq 1 60); do
  [[ "$(curl -fs "$BASE/_health" 2>/dev/null | jq -r '.status' 2>/dev/null)" == "green" ]] && break
  sleep 1
done
[[ "$(curl -fs "$BASE/_health" | jq -r '.status')" == "green" ]] || fail "coordinator never went green"

echo "==> ingest one query (auth-gated write) and percolate a matching title"
code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$BASE/_doc/smoke1" "${auth[@]}" \
  -H 'content-type: application/json' -d '{"query":"1990 topps smokeplayer"}')
[[ "$code" == "201" || "$code" == "200" ]] || fail "ingest rejected (HTTP $code)"

hits=$(curl -s -X POST "$BASE/_search" -H 'content-type: application/json' \
  -d '{"document":{"title":"1990 topps smokeplayer psa 10"},"size":10}' | jq -c '[.hits.hits[]._id]|sort')
[[ "$hits" == '["smoke1"]' ]] || fail "percolate did not return the ingested query (got $hits)"

total=$(curl -fs "$BASE/_stats" | jq '.total_queries')
[[ "$total" -ge 1 ]] || fail "stats reports no queries (total=$total)"

echo "PASS: production compose came up green and served a match ($total query, hits=$hits)"
