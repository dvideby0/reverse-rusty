#!/usr/bin/env bash
# The multi-machine harness (ADR-072, Distributed-v1 criterion 3): drives a fully
# secured containerized cluster (deploy/compose.harness.yml) through its lifecycle —
# kill-and-recover, rolling restart, coordinator restart, live handoff under load —
# asserting through the REST surface that every event preserves the percolate
# baseline and that a degraded cluster FAILS LOUD (a dead shard is a 502, never a
# silently truncated result).
#
# Usage:
#   deploy/harness.sh                      # build the image from source (slow first time)
#   deploy/harness.sh --prebuilt DIR       # wrap prebuilt linux bins from DIR (the CI path)
#
# Requires: docker (compose v2), curl, jq, openssl. Exits 0 on PASS.
set -euo pipefail

cd "$(dirname "$0")/.."
REPO_ROOT=$(pwd)
COMPOSE_FILE="$REPO_ROOT/deploy/compose.harness.yml"
BASE="http://127.0.0.1:19200"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/rr_harness.XXXXXX")

PREBUILT_DIR=""
if [[ "${1:-}" == "--prebuilt" ]]; then
  PREBUILT_DIR="${2:?--prebuilt needs a bin dir}"
fi

export RR_IMAGE="reverse-rusty-harness:dev"
export RR_CLUSTER_TOKEN="harness-mesh-secret-$$"
export RR_CERT_DIR="$WORK/certs"
export RR_CORPUS_DIR="$WORK/corpus"

# A dedicated project name isolates the harness from any manually started stack
# on the same compose file (stale volumes from a debug session must never leak in).
compose() { docker compose -p rrharness -f "$COMPOSE_FILE" "$@"; }

# Every HTTP call goes through one wrapper with a hard cap, so a stalled request
# fails its leg LOUDLY rather than hanging the harness for hours (the run-9 lesson).
# A slow operation (the handoff) passes a larger budget explicitly.
HTTP_TIMEOUT=15
rqcurl() { curl -s --max-time "$HTTP_TIMEOUT" "$@"; }

cleanup() {
  status=$?
  if [[ $status -ne 0 ]]; then
    echo "--- harness FAILED (exit $status); recent container logs:" >&2
    compose logs --tail 200 >&2 || true
  fi
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$WORK"
  exit $status
}
trap cleanup EXIT

step() { printf '\n==> %s\n' "$*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

for tool in docker curl jq openssl; do
  command -v "$tool" >/dev/null || fail "missing required tool: $tool"
done

# ---------------------------------------------------------------------------
step "ephemeral mesh identity (self-signed; SANs cover every node name)"
mkdir -p "$RR_CERT_DIR"
# CA:FALSE is load-bearing: webpki rejects a CA-marked cert presented as the
# END-ENTITY (openssl's `req -x509` default adds CA:true), while a plain
# self-signed EE cert is accepted both as the served identity and as the
# client's trust anchor — the same shape the in-test rcgen certs use.
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -sha256 -nodes \
  -keyout "$RR_CERT_DIR/node.key" -out "$RR_CERT_DIR/node.crt" \
  -days 2 -subj "/CN=rr-harness" \
  -addext "basicConstraints=critical,CA:FALSE" \
  -addext "subjectAltName=DNS:shard0,DNS:shard1,DNS:shard2,DNS:target,DNS:control0,DNS:control1,DNS:control2,DNS:localhost" \
  >/dev/null 2>&1
cp "$RR_CERT_DIR/node.crt" "$RR_CERT_DIR/ca.crt"
chmod 644 "$RR_CERT_DIR"/* # the container user (uid 10001) must read them

# ---------------------------------------------------------------------------
step "deterministic corpus (200 selective + any-of + broad queries)"
mkdir -p "$RR_CORPUS_DIR"
{
  for i in $(seq 0 79); do
    year=$((1986 + i % 39))
    echo "$((1000 + i)),\"$year topps rareplayer$i\""
  done
  for i in $(seq 0 39); do
    echo "$((2000 + i)),\"(rareplayer$i,rareplayer$((i + 200)))\""
  done
  for i in $(seq 0 39); do
    year=$((1986 + i % 39))
    echo "$((3000 + i)),\"$year fleer rareplayer$((i + 40))\""
  done
  # broad (hot-only) queries ride the replicated lane.
  for i in $(seq 0 19); do
    year=$((1986 + i))
    echo "$((4000 + i)),\"$year topps\""
  done
} > "$RR_CORPUS_DIR/queries.csv"

PROBES=()
for i in 0 7 19 33 41 55 63 77; do
  year=$((1986 + i % 39))
  PROBES+=("$year topps rareplayer$i psa 10")
done
for i in 3 21 38; do
  PROBES+=("rareplayer$((i + 200)) gem mint")
done
PROBES+=("1990 topps sealed box")

# ---------------------------------------------------------------------------
if [[ -n "$PREBUILT_DIR" ]]; then
  step "image from prebuilt bins ($PREBUILT_DIR)"
  # The bins run in a linux/amd64 container — guard against wrapping host-native
  # (e.g. macOS) binaries, which fail with an opaque container exit. `file` is
  # best-effort; if absent we proceed (CI builds on a linux runner anyway).
  if command -v file >/dev/null && ! file "$PREBUILT_DIR/server" | grep -qi 'ELF'; then
    fail "prebuilt $PREBUILT_DIR/server is not a Linux (ELF) binary — the container needs linux/amd64 bins (build on a Linux host/runner, or omit --prebuilt to build from source)"
  fi
  # Stage the bins into a scratch dir and build with THAT as the context: the repo
  # root cannot be the context because .dockerignore excludes target/ (COPY would
  # not see the bins), and a bins-only context skips shipping the repo to dockerd.
  mkdir -p "$WORK/image"
  cp "$PREBUILT_DIR/server" "$PREBUILT_DIR/shardserver" "$PREBUILT_DIR/controlserver" \
    "$WORK/image/"
  docker build -q -f deploy/Dockerfile.prebuilt -t "$RR_IMAGE" "$WORK/image" >/dev/null
else
  step "image from source (slow on a cold cache)"
  docker build -q -f deploy/Dockerfile -t "$RR_IMAGE" "$REPO_ROOT" >/dev/null
fi

step "cluster up (3 durable shards + target + coordinator + 3-node control plane)"
# Defensive: clear any prior harness state (a crashed run's volumes would otherwise
# leak stale durable shards into this run).
compose down -v --remove-orphans >/dev/null 2>&1 || true
compose up -d --wait --quiet-pull

percolate() { # title -> sorted id list (or "HTTP:<code>" on non-200)
  local out code body
  out=$(rqcurl -w '\n%{http_code}' -X POST "$BASE/_search" \
    -H 'content-type: application/json' \
    -d "$(jq -nc --arg t "$1" '{document:{title:$t}, include_broad:true, size:500}')")
  code=${out##*$'\n'}
  body=${out%$'\n'*}
  if [[ "$code" != "200" ]]; then
    echo "HTTP:$code"
  else
    echo "$body" | jq -c '[.hits.hits[]._id] | sort'
  fi
}

put_doc() { # id dsl -> http code
  rqcurl -o /dev/null -w '%{http_code}' -X PUT "$BASE/_doc/$1" \
    -H 'content-type: application/json' \
    -d "$(jq -nc --arg q "$2" '{query:$q}')"
}

snapshot_baseline() { # writes one result per probe to the named file
  local file=$1 r
  : > "$file"
  for t in "${PROBES[@]}"; do
    r=$(percolate "$t")
    [[ "$r" == HTTP:* ]] && fail "baseline percolate failed for '$t': $r"
    echo "$r" >> "$file"
  done
}

assert_equals_baseline() { # file label
  local file=$1 label=$2 i=0 r want
  while IFS= read -r want; do
    r=$(percolate "${PROBES[$i]}")
    [[ "$r" == "$want" ]] || fail "$label: probe '${PROBES[$i]}' diverged: got $r want $want"
    i=$((i + 1))
  done < "$file"
}

wait_for_green() { # label
  for _ in $(seq 1 60); do
    if [[ "$(rqcurl -f "$BASE/_health" 2>/dev/null | jq -r '.status' 2>/dev/null)" == "green" ]]; then
      return 0
    fi
    sleep 1
  done
  fail "$1: coordinator never reported green"
}

# ---------------------------------------------------------------------------
step "leg 0 — baseline over the secured mesh"
wait_for_green "startup"
total=$(rqcurl -f "$BASE/_stats" | jq '.total_queries')
[[ "$total" -gt 0 ]] || fail "corpus did not load (total_queries=$total)"
[[ "$(put_doc 9001 'zzharness gem mint')" == "201" ]] || fail "live write rejected"
PROBES+=("zzharness gem mint psa 10")
snapshot_baseline "$WORK/baseline.txt"
grep -q '9001' <(tail -1 "$WORK/baseline.txt") || fail "live write not matchable"
echo "    baseline captured (${#PROBES[@]} probes, $total queries)"

# ---------------------------------------------------------------------------
step "leg 1 — kill a shard: degraded percolates FAIL LOUD, never truncate"
docker kill -s KILL "$(compose ps -q shard2)" >/dev/null
sleep 1
loud=0 i=0
while IFS= read -r want; do
  r=$(percolate "${PROBES[$i]}")
  if [[ "$r" == HTTP:* ]]; then
    [[ "$r" == "HTTP:502" ]] || fail "dead-shard probe returned $r (want 502)"
    loud=$((loud + 1))
  else
    [[ "$r" == "$want" ]] || fail "dead-shard probe '${PROBES[$i]}' SILENTLY diverged: $r vs $want"
  fi
  i=$((i + 1))
done < "$WORK/baseline.txt"
[[ $loud -gt 0 ]] || fail "no probe routed to the dead shard — broaden the probe set"
echo "    $loud/$i probes failed loud; every success matched baseline exactly"

step "leg 1b — restart the node: durable self-restore + channel reconnect"
compose start shard2 >/dev/null
for _ in $(seq 1 60); do
  r=$(percolate "${PROBES[0]}")
  [[ "$r" != HTTP:* ]] && break
  sleep 1
done
assert_equals_baseline "$WORK/baseline.txt" "after kill+recover"
echo "    recovered; all probes ≡ baseline"

# ---------------------------------------------------------------------------
step "leg 2 — rolling restart of every shard node"
for s in shard0 shard1 shard2; do
  compose restart "$s" >/dev/null
  for _ in $(seq 1 60); do
    r=$(percolate "${PROBES[0]}")
    [[ "$r" != HTTP:* ]] && break
    sleep 1
  done
done
assert_equals_baseline "$WORK/baseline.txt" "after rolling restart"
echo "    all probes ≡ baseline"

# ---------------------------------------------------------------------------
step "leg 3 — coordinator restart (stateless re-mint; shards stay authoritative)"
compose restart coordinator >/dev/null
wait_for_green "coordinator restart"
total2=$(rqcurl -f "$BASE/_stats" | jq '.total_queries')
[[ "$total2" -ge "$total" ]] || fail "corpus shrank across coordinator restart ($total2 < $total)"
assert_equals_baseline "$WORK/baseline.txt" "after coordinator restart"
echo "    reconnected to populated shards; all probes ≡ baseline (incl. the live write)"

# ---------------------------------------------------------------------------
step "leg 3b — SIGKILL a shard MID-WRITE: every acknowledged in-flight write survives"
# The single-node real-SIGKILL crash suite (engine/tests/crash_injection, ADR-088)
# proves an acked write survives a kill BETWEEN ops; this is its cluster analogue —
# kill a shard while writes are STREAMING through it. A write that routed to the dead
# shard returns 200 "partial" (durably logged at the coordinator + queued for repair,
# ADR-047); one that applied cleanly returns 201. EVERY acknowledged (2xx) id must be
# matchable after the shard restarts and we converge the queued repairs with
# /_cluster/resync — zero false negatives across a real kill mid-write.
kw="$WORK/killwrite"
: > "$kw.accepted"
(
  set +e # a write landing in the dead window may fail; record only the acknowledged ones
  for i in $(seq 0 199); do
    code=$(put_doc $((9300 + i)) "zzkill$i unique$i")
    [[ "$code" == "201" || "$code" == "200" ]] && echo "$((9300 + i))" >> "$kw.accepted"
    sleep 0.05
  done
) &
kw_pid=$!
sleep 1.5 # let a batch of writes land cleanly before the kill
docker kill -s KILL "$(compose ps -q shard0)" >/dev/null
echo "    SIGKILLed shard0 mid-write loop"
sleep 2 # a window of writes streams at the dead shard (logged + queued, ADR-047)
compose start shard0 >/dev/null # restart promptly, while the writer is still streaming
wait "$kw_pid" # the writer finishes its loop across the kill + restart
# Converge the partial-applies that queued while shard0 was down (ADR-047 repair path).
# /_health stays YELLOW (not green) while repairs are pending, so resync BEFORE the
# green gate; the retry loop also rides out shard0's restart — a resync only converges
# once shard0 is reachable again (its re-driven writes land).
converged=0 resync=""
for _ in $(seq 1 60); do
  resync=$(rqcurl -X POST "$BASE/_cluster/resync" -H 'content-type: application/json' -d '{}')
  echo "$resync" | jq -e '.still_pending == 0' >/dev/null 2>&1 && { converged=1; break; }
  sleep 1
done
[[ "$converged" -eq 1 ]] || fail "partial-apply repairs never converged after restart: $resync"
wait_for_green "after mid-write kill"
accepted=$(grep -c . "$kw.accepted" || true)
[[ "$accepted" -gt 0 ]] || fail "no writes were acknowledged around the mid-write kill window"
# Every acknowledged write must now be matchable (recall of acknowledged writes).
miss=0
while IFS= read -r id; do
  i=$((id - 9300))
  r=$(percolate "zzkill$i unique$i")
  [[ "$r" == HTTP:* ]] && fail "post-recovery percolate failed: $r"
  echo "$r" | jq -e --argjson id "$id" 'index($id) != null' >/dev/null || miss=$((miss + 1))
done < "$kw.accepted"
[[ $miss -eq 0 ]] || fail "$miss acknowledged writes unmatchable after the mid-write kill+restart+resync (FN!)"
assert_equals_baseline "$WORK/baseline.txt" "after mid-write kill"
echo "    $accepted acked writes; zero FN after kill+restart+resync ($resync); probes ≡ baseline"

# ---------------------------------------------------------------------------
step "leg 4 — live handoff under load (position 1: shard1 → target)"
writer_log="$WORK/writer.log"
: > "$writer_log.accepted"
(
  set +e  # a fence-window 503 is expected; never let it kill the writer
  ok=0
  for i in $(seq 0 199); do
    code=$(put_doc $((9100 + i)) "zzload$i unique$i")
    if [[ "$code" == "201" || "$code" == "200" ]]; then
      ok=$((ok + 1))
      echo "$((9100 + i))" >> "$writer_log.accepted"
    fi
    sleep 0.05
  done
  echo "$ok" > "$writer_log.count"
) &
writer_pid=$!
sleep 1
handoff=$(curl -s --max-time 120 -X POST "$BASE/_cluster/handoff" \
  -H 'content-type: application/json' \
  -d '{"position":1,"source":"https://shard1:50051","target":"https://target:50051"}' \
  || echo '{"error":"handoff request timed out or failed"}')
echo "$handoff" | jq -e '.acknowledged == true' >/dev/null \
  || fail "handoff not acknowledged: $handoff"
wait "$writer_pid"
accepted=$(cat "$writer_log.count")
[[ "$accepted" -gt 0 ]] || fail "the write loop never succeeded around the handoff window"
# Every ACCEPTED write must be matchable after the move (recall of acknowledged writes).
miss=0
while IFS= read -r id; do
  i=$((id - 9100))
  r=$(percolate "zzload$i unique$i sealed")
  [[ "$r" == HTTP:* ]] && fail "post-handoff percolate failed: $r"
  echo "$r" | jq -e --argjson id "$id" 'index($id) != null' >/dev/null || miss=$((miss + 1))
done < <(head -20 "$writer_log.accepted")
[[ $miss -eq 0 ]] || fail "$miss acknowledged writes unmatchable after the handoff (FN!)"
assert_equals_baseline "$WORK/baseline.txt" "after handoff"
echo "    handoff complete under load ($accepted writes accepted); zero FN; probes ≡ baseline"

# ---------------------------------------------------------------------------
step "leg 5 — control-plane rolling restart (durable Raft state)"
for c in control0 control1 control2; do
  compose restart "$c" >/dev/null
done
for c in control0 control1 control2; do
  for _ in $(seq 1 30); do
    state=$(docker inspect -f '{{.State.Health.Status}}' "$(compose ps -q "$c")" 2>/dev/null || echo starting)
    [[ "$state" == "healthy" ]] && break
    sleep 1
  done
  [[ "$state" == "healthy" ]] || fail "$c did not come back healthy"
done
echo "    all three manager nodes resumed from durable state"

printf '\nPASS: multi-machine harness — all legs green\n'
