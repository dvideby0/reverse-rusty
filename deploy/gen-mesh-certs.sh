#!/usr/bin/env bash
# Mint a self-signed mesh identity for a Reverse Rusty cluster (ADR-071): one
# EC P-256 cert whose SANs cover every service DNS name, served by every node and
# trusted as the CA by every client. This is the production analogue of the
# ephemeral cert block in deploy/harness.sh — same shape, longer validity, and a
# refuse-to-clobber guard so you don't silently rotate a live cluster's identity.
#
# Usage:
#   deploy/gen-mesh-certs.sh [OUTPUT_DIR] [SERVICE_NAME...]
#   deploy/gen-mesh-certs.sh                       # ./deploy/certs, default service names
#   deploy/gen-mesh-certs.sh /etc/rr/certs shard0 shard1 coordinator control0
#
# Writes ca.crt + node.crt + node.key (world-readable 644 — the container user,
# uid 10001, must read them through the read-only /certs mount). Requires openssl.
#
# This is a BOOTSTRAP identity: one shared cert for the whole mesh, which is the
# simplest thing that authenticates the network. For stronger isolation, issue
# per-node certs from a real CA and point --tls-ca at that CA bundle — the bins
# accept any CA the same way (see docs/operations/cluster-deployment.md §2).
set -euo pipefail

OUT_DIR="${1:-./deploy/certs}"
shift || true
# Default SANs cover the deploy/compose.cluster.yml service names + localhost (for
# a host-side probe). Pass your own list to match a custom topology.
if [[ $# -gt 0 ]]; then
  SERVICES=("$@")
else
  SERVICES=(shard0 shard1 shard2 coordinator control0 control1 control2 localhost)
fi

command -v openssl >/dev/null || { echo "FATAL: openssl not found" >&2; exit 1; }

mkdir -p "$OUT_DIR"
# Refuse to overwrite an existing identity: rotating a live mesh's cert is a
# deliberate act (every node must adopt the new CA together), never an accident of
# re-running this script.
if [[ -e "$OUT_DIR/node.key" ]]; then
  echo "FATAL: $OUT_DIR/node.key already exists — refusing to overwrite a mesh identity." >&2
  echo "       Remove the old certs first if you intend to rotate (and redeploy every node)." >&2
  exit 1
fi

# Build the SAN extension: DNS:name,DNS:name,...
san=""
for s in "${SERVICES[@]}"; do
  san+="${san:+,}DNS:${s}"
done

# CA:FALSE is load-bearing: webpki (the bins' TLS verifier) rejects a CA-marked
# cert presented as the END-ENTITY, while a plain self-signed EE cert is accepted
# both as the served identity and as the client's trust anchor.
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -sha256 -nodes \
  -keyout "$OUT_DIR/node.key" -out "$OUT_DIR/node.crt" \
  -days 825 -subj "/CN=reverse-rusty-mesh" \
  -addext "basicConstraints=critical,CA:FALSE" \
  -addext "subjectAltName=${san}" \
  >/dev/null 2>&1

# The CA bundle a client verifies against IS the self-signed cert (it is its own
# issuer), so node.crt doubles as ca.crt.
cp "$OUT_DIR/node.crt" "$OUT_DIR/ca.crt"
chmod 644 "$OUT_DIR"/ca.crt "$OUT_DIR"/node.crt "$OUT_DIR"/node.key

echo "Wrote mesh identity to $OUT_DIR:"
echo "  ca.crt node.crt node.key  (valid 825 days, SANs: ${SERVICES[*]})"
echo "Point RR_CERT_DIR at this directory (see deploy/cluster.env.example)."
