#!/usr/bin/env bash
# Minimal Kubernetes smoke test for the Helm chart (deploy/helm/reverse-rusty, ADR-084):
# spin up a throwaway kind cluster, install the chart (TLS off + dev tokens for
# simplicity), wait for the coordinator to be Ready, ingest one query over REST, prove a
# title percolates to it, then tear down. The k8s analogue of deploy/cluster-smoke.sh.
#
# NOT run in CI — CI validates the chart structurally (helm lint + helm template +
# kubeconform -strict, see .github/workflows/ci.yml `helm-chart`). This needs a real
# cluster, so it is an operator/dev convenience.
#
# Usage:
#   deploy/k8s-smoke.sh                                   # create kind cluster, build+load the image
#   RR_IMAGE=reverse-rusty:latest deploy/k8s-smoke.sh     # reuse an existing image (still kind-loaded)
#   RR_KEEP=1 deploy/k8s-smoke.sh                         # leave the kind cluster up for inspection
#
# Requires: kind, kubectl, helm, docker, curl, jq. Exits 0 on PASS.
set -euo pipefail

cd "$(dirname "$0")/.."
REPO_ROOT=$(pwd)
CHART="$REPO_ROOT/deploy/helm/reverse-rusty"
CLUSTER="rr-smoke"
NS="rr-smoke"
RELEASE="rr"
COORD="$RELEASE-reverse-rusty-coordinator"
export RR_IMAGE="${RR_IMAGE:-reverse-rusty:k8s-smoke}"
LOCAL_PORT=19200
TOKEN="smoke-$$"
AUTH="smokeauth-$$"

fail() { echo "FAIL: $*" >&2; exit 1; }
for tool in kind kubectl helm docker curl jq; do
  command -v "$tool" >/dev/null || fail "missing tool: $tool"
done

PF_PID=""
cleanup() {
  status=$?
  [[ -n "$PF_PID" ]] && kill "$PF_PID" 2>/dev/null || true
  if [[ $status -ne 0 ]]; then
    echo "--- k8s smoke FAILED (exit $status); pods + recent coordinator logs:" >&2
    kubectl -n "$NS" get pods >&2 2>/dev/null || true
    kubectl -n "$NS" logs -l app.kubernetes.io/component=coordinator --tail 80 >&2 2>/dev/null || true
  fi
  if [[ "${RR_KEEP:-}" == "1" ]]; then
    echo "RR_KEEP=1 — leaving kind cluster '$CLUSTER' up (remove: kind delete cluster --name $CLUSTER)"
  else
    kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  fi
  exit $status
}
trap cleanup EXIT

echo "==> build image $RR_IMAGE (slow on a cold cache)"
docker image inspect "$RR_IMAGE" >/dev/null 2>&1 || docker build -q -f deploy/Dockerfile -t "$RR_IMAGE" "$REPO_ROOT" >/dev/null

echo "==> create kind cluster '$CLUSTER' + load the image"
kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
kind create cluster --name "$CLUSTER" >/dev/null
kind load docker-image "$RR_IMAGE" --name "$CLUSTER" >/dev/null

echo "==> helm install (TLS off, dev tokens — smoke only)"
helm install "$RELEASE" "$CHART" -n "$NS" --create-namespace \
  --set image.repository="${RR_IMAGE%:*}" --set image.tag="${RR_IMAGE##*:}" --set image.pullPolicy=IfNotPresent \
  --set tls.enabled=false \
  --set clusterToken.create=true --set clusterToken.value="$TOKEN" \
  --set auth.create=true --set auth.value="$AUTH" \
  --set persistence.size=1Gi \
  --wait --timeout 5m >/dev/null

echo "==> wait for coordinator rollout"
kubectl -n "$NS" rollout status "deploy/$COORD" --timeout=180s

echo "==> port-forward + REST smoke"
kubectl -n "$NS" port-forward "svc/$COORD" "$LOCAL_PORT:9200" >/dev/null 2>&1 &
PF_PID=$!
BASE="http://127.0.0.1:$LOCAL_PORT"
for _ in $(seq 1 30); do
  [[ "$(curl -fs "$BASE/_health" 2>/dev/null | jq -r '.status' 2>/dev/null)" == "green" ]] && break
  sleep 2
done
[[ "$(curl -fs "$BASE/_health" | jq -r '.status')" == "green" ]] || fail "coordinator never went green"

echo "==> ingest one query (auth-gated write) and percolate a matching title"
code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$BASE/_doc/smoke1" \
  -H "authorization: Bearer $AUTH" -H 'content-type: application/json' \
  -d '{"query":"1990 topps smokeplayer"}')
[[ "$code" == "201" || "$code" == "200" ]] || fail "ingest rejected (HTTP $code)"

hits=$(curl -s -X POST "$BASE/_search" -H 'content-type: application/json' \
  -d '{"document":{"title":"1990 topps smokeplayer psa 10"},"size":10}' | jq -c '[.hits.hits[]._id]|sort')
[[ "$hits" == '["smoke1"]' ]] || fail "percolate did not return the ingested query (got $hits)"

echo "PASS: Helm chart came up on kind and served a match (hits=$hits)"
