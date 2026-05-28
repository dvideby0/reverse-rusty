#!/usr/bin/env bash
# Head-to-head benchmark: Percolator vs mokaccino
#
# Usage:
#   ./run.sh                    # default: 100k queries, 10k titles
#   ./run.sh 500000 20000       # custom scale
#   ./run.sh 1000000 20000 0.05 2.0 0xC0FFEE   # all params
#
# Args: [num_queries] [num_titles] [broad_frac] [skew] [seed]

set -euo pipefail
cd "$(dirname "$0")"

echo "=== Building mokabench (release, LTO) ==="
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/mokabench-target}"
cargo build --release 2>&1

BINARY="$CARGO_TARGET_DIR/release/mokabench"
echo ""
echo "=== Running benchmark ==="
echo ""

# Pass all args through; defaults are 100k queries, 10k titles
"$BINARY" "$@" 2>&1 | tee "$(dirname "$0")/../benchmark-mokaccino.txt"

echo ""
echo "Output saved to benchmark-mokaccino.txt in the project root."
