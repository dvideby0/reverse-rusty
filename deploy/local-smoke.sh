#!/usr/bin/env bash
# The deployable smoke for the two LOCAL modes (Tier 5 M1, ADR-098): single-node
# (`server --data-dir`) and the in-process cluster (`server --cluster --shards K
# --data-dir`). For each mode: start → ingest (auth-gated) → search → restart on the
# same data dir → search again → restore from a `POST /_backup` copy — proving the
# documented M1 surface (_doc/_bulk/_search/_mpercolate/_health/_stats/_metrics/
# _backup + restart-reopen) and the ADR-062 auth posture (a tokened server 401s an
# unauthenticated write). No containers: this is the mode a `cargo build` user runs.
# The container modes have their own smokes (cluster-smoke.sh, k8s-smoke.sh) and the
# lifecycle harness (harness.sh); SIGKILL durability is tests/crash_injection (ADR-088)
# — the restart here is a graceful SIGTERM reopen.
#
# Usage:
#   deploy/local-smoke.sh                      # cargo-build the server bin, then smoke
#   deploy/local-smoke.sh --prebuilt DIR       # use DIR/server (the CI path)
#
# Requires: curl, jq (+ cargo unless --prebuilt). Exits 0 on PASS.
set -euo pipefail

cd "$(dirname "$0")/.."
REPO_ROOT=$(pwd)
WORK=$(mktemp -d "${TMPDIR:-/tmp}/rr_local_smoke.XXXXXX")
SERVER_PID=""

# With RR_AUTH_TOKEN set the server gates mutating/admin endpoints behind this bearer
# (ADR-062) — the smoke asserts that posture instead of just documenting it.
export RR_AUTH_TOKEN="local-smoke-$$"
auth=(-H "authorization: Bearer $RR_AUTH_TOKEN")

cleanup() {
  status=$?
  [[ -n "$SERVER_PID" ]] && kill -KILL "$SERVER_PID" 2>/dev/null || true
  if [[ $status -ne 0 ]]; then
    echo "--- local smoke FAILED (exit $status); recent server logs:" >&2
    for log in "$WORK"/*.log; do
      [[ -f "$log" ]] && { echo "--- $log:" >&2; tail -40 "$log" >&2; }
    done
  fi
  rm -rf "$WORK"
  exit $status
}
trap cleanup EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }

# Every request is deadline-capped so a wedged server fails the smoke instead of
# hanging it (the harness.sh lesson).
req() { curl --max-time 15 "$@"; }

PREBUILT=""
if [[ "${1:-}" == "--prebuilt" ]]; then
  [[ -n "${2:-}" ]] || fail "--prebuilt requires a directory"
  PREBUILT=$2
fi

for tool in curl jq; do command -v "$tool" >/dev/null || fail "missing tool: $tool"; done

if [[ -n "$PREBUILT" ]]; then
  SERVER_BIN="$PREBUILT/server"
  [[ -x "$SERVER_BIN" ]] || fail "no executable server bin at $SERVER_BIN"
  echo "==> using prebuilt server bin: $SERVER_BIN"
else
  command -v cargo >/dev/null || fail "missing tool: cargo (or pass --prebuilt DIR)"
  echo "==> cargo build --release --bin server (default features)"
  (cd engine && cargo build --release --bin server)
  SERVER_BIN="${CARGO_TARGET_DIR:-$REPO_ROOT/engine/target}/release/server"
  [[ -x "$SERVER_BIN" ]] || fail "built server bin not found at $SERVER_BIN"
fi

start_server() { # $1 = log file, rest = server args
  local log=$1
  shift
  "$SERVER_BIN" "$@" >>"$log" 2>&1 &
  SERVER_PID=$!
}

# Graceful stop: SIGTERM flushes the memtable before exit, so the relaunch exercises
# the clean reopen path. A server that ignores SIGTERM for 15s is itself a failure.
stop_server() {
  [[ -n "$SERVER_PID" ]] || return 0
  kill -TERM "$SERVER_PID" 2>/dev/null || true
  for _ in $(seq 1 30); do
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
      SERVER_PID=""
      return 0
    fi
    sleep 0.5
  done
  fail "server (pid $SERVER_PID) did not exit within 15s of SIGTERM"
}

wait_green() { # $1 = base URL
  for _ in $(seq 1 60); do
    [[ "$(req -fs "$1/_health" 2>/dev/null | jq -r '.status' 2>/dev/null)" == "green" ]] && return 0
    sleep 0.5
  done
  return 1
}

# Cost classification is FREQUENCY-based (the dict's 64-bit common mask), and a bulk
# batch finalizes the mask from its own corpus — so on a micro-corpus like this one
# EVERY term is top-64 "hot" and a bulk-ingested any-of query lands in class C (the
# broad lane, quarantined by default). The live PUT path on a fresh dict classifies
# the same query selective, and the in-process cluster (frozen-empty dict ⇒ synthetic
# ids, never masked) stays selective everywhere — the classification is corpus- and
# path-dependent BY DESIGN. So the recurring match probes pass `include_broad: true`
# (a pure superset — never drops a selective match), making them classification-
# independent across modes and restart/restore legs; default visibility is pinned
# once, pre-restart, where it is deterministic in both modes (query 1 via PUT on a
# fresh engine is always selective).
search_hits() { # $1 = base URL → sorted matching query ids for the canonical title
  req -s -X POST "$1/_search" -H 'content-type: application/json' \
    -d '{"document":{"title":"1990 topps smokeplayer psa 10"},"include_broad":true,"size":10}' |
    jq -c '[.hits.hits[]._id]|sort'
}

assert_matches() { # $1 = base URL, $2 = label
  local base=$1 label=$2 hits mp
  hits=$(search_hits "$base")
  [[ "$hits" == '[1]' ]] || fail "[$label] /_search hits=$hits, want [1]"
  # Three-document batch: a match, a MUST_NOT suppression (correctness, not just
  # liveness — forbidden terms are verified even through the broad lane), and an
  # any-of group match.
  mp=$(req -s -X POST "$base/_mpercolate" -H 'content-type: application/json' -d '{
      "include_broad": true,
      "documents": [
        {"title": "1990 topps smokeplayer psa 10"},
        {"title": "replica vintage smokejacket size L"},
        {"title": "smokestar gem mint 10"}
      ]}' | jq -c '[.responses[] | [.hits.hits[]._id] | sort]')
  [[ "$mp" == '[[1],[],[3]]' ]] || fail "[$label] /_mpercolate got $mp, want [[1],[],[3]]"
}

run_mode() { # $1 = mode name, $2 = port, rest = extra server flags
  local mode=$1 port=$2
  shift 2
  local base="http://127.0.0.1:$port"
  local data="$WORK/$mode"
  local log="$WORK/$mode.log"
  local code errors total ack hits

  echo "==> [$mode] start: server $* --port $port --data-dir $data"
  start_server "$log" "$@" --port "$port" --data-dir "$data"
  wait_green "$base" || fail "[$mode] server never went green"

  # ADR-062 posture: with a token set, a write WITHOUT the bearer is a 401.
  code=$(req -s -o /dev/null -w '%{http_code}' -X PUT "$base/_doc/1" \
    -H 'content-type: application/json' -d '{"query":"1990 topps smokeplayer"}')
  [[ "$code" == "401" ]] || fail "[$mode] unauthenticated write not rejected (HTTP $code, want 401)"

  # Ingest. Document ids are u64 logical ids in BOTH modes (the `_doc/{id}` route
  # extracts Path<u64>), so ids must be numeric.
  code=$(req -s -o /dev/null -w '%{http_code}' -X PUT "$base/_doc/1" "${auth[@]}" \
    -H 'content-type: application/json' -d '{"query":"1990 topps smokeplayer"}')
  [[ "$code" == "201" || "$code" == "200" ]] || fail "[$mode] ingest rejected (HTTP $code)"
  code=$(req -s -o /dev/null -w '%{http_code}' "$base/_doc/1")
  [[ "$code" == "200" ]] || fail "[$mode] stored-query read-back failed (HTTP $code)"

  errors=$(req -s -X POST "$base/_bulk" "${auth[@]}" -H 'content-type: application/x-ndjson' \
    --data-binary $'{"index":{"_id":2}}\n{"query":"vintage smokejacket -replica"}\n{"index":{"_id":3}}\n{"query":"(smokeplayer,smokestar) gem"}\n' |
    jq '.errors')
  [[ "$errors" == "false" ]] || fail "[$mode] bulk ingest reported errors"

  # Default visibility, pinned once where it is deterministic (see the note above
  # search_hits): the selective path serves without include_broad.
  hits=$(req -s -X POST "$base/_search" -H 'content-type: application/json' \
    -d '{"document":{"title":"1990 topps smokeplayer psa 10"},"size":10}' |
    jq -c '[.hits.hits[]._id]|sort')
  [[ "$hits" == '[1]' ]] || fail "[$mode] default-visibility /_search hits=$hits, want [1]"

  assert_matches "$base" "$mode"

  total=$(req -fs "$base/_stats" | jq '.total_queries')
  [[ "$total" == "3" ]] || fail "[$mode] stats total_queries=$total, want 3"
  req -fs "$base/_metrics" | grep -q '# HELP' || fail "[$mode] /_metrics is not Prometheus text"

  # Snapshot the durable state server-side (auth-gated write); restored below.
  ack=$(req -s -X POST "$base/_backup" "${auth[@]}" -H 'content-type: application/json' \
    -d "{\"dest\":\"$WORK/$mode-backup\"}" | jq '.acknowledged')
  [[ "$ack" == "true" ]] || fail "[$mode] backup not acknowledged"

  # Restart-reopen: every acked write must survive an operational restart.
  stop_server
  echo "==> [$mode] restart on the same --data-dir (reopen)"
  start_server "$log" "$@" --port "$port" --data-dir "$data"
  wait_green "$base" || fail "[$mode] server never went green after restart"
  assert_matches "$base" "$mode after restart"
  total=$(req -fs "$base/_stats" | jq '.total_queries')
  [[ "$total" == "3" ]] || fail "[$mode] post-restart total_queries=$total, want 3"
  stop_server

  # Restore proof: a backup you cannot open is not a backup (restore = open, ADR-079).
  echo "==> [$mode] restore: open the backup copy"
  start_server "$log" "$@" --port "$port" --data-dir "$WORK/$mode-backup"
  wait_green "$base" || fail "[$mode] restored server never went green"
  hits=$(search_hits "$base")
  [[ "$hits" == '[1]' ]] || fail "[$mode] restored backup lost the stored query (hits=$hits)"
  stop_server

  echo "PASS [$mode]"
}

run_mode single 19301
run_mode cluster 19302 --cluster --shards 3

echo "PASS: both local modes served, survived a restart, and restored from backup"
