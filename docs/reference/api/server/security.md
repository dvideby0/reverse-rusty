# Security

> [Server & shared behavior](../server.md) · [REST API hub](../../api.md)

The server binds **`127.0.0.1` (loopback) by default** (ADR-052) — not reachable beyond the local
host. To serve other hosts, set `--host 0.0.0.0` (or a specific interface) and gate the
mutating/admin endpoints with **bearer-token auth** (ADR-062):

```bash
export RR_AUTH_TOKEN=$(openssl rand -hex 32)
cargo run --release --bin server -- --host 0.0.0.0
# clients:
curl -X PUT localhost:9200/_doc/1 -H "Authorization: Bearer $RR_AUTH_TOKEN" \
  -H 'content-type: application/json' -d '{"query": "wireless mouse"}'
```

With a token configured (`RR_AUTH_TOKEN` env var or `--auth-token`; the env var is preferred — flag
values appear in process listings), **every non-GET/HEAD request requires
`Authorization: Bearer <token>`** except the explicit read-via-POST allowlist:
`POST /_search`, `POST /v2/_search`, `POST /_mpercolate`, `POST /_percolate/jobs`, and the
`POST`/`DELETE /v2/_pit` lifecycle. `POST /v2/_mpercolate` is **not** on that allowlist and currently
requires the token. Exhaustive job inspection/streaming stays open through GET; cancelling a job with
DELETE is protected. The default-deny rule also covers `_doc` writes, `_bulk`, `_flush`, `_compact`,
`_forcemerge`, `_backup`, `_vocab` writes (including `/_vocab/learn*` and
`/_vocab/aliases/*`), `_settings` writes, and any future mutating endpoint. `--auth-protect-reads`
extends the gate to every read surface in the allowlist/GET/HEAD set; only `GET`/`HEAD /_health`
remains open for liveness probes.

Failures return **401** with the standard error envelope (`"type": "security_exception"`) and an
RFC 6750 `WWW-Authenticate: Bearer` challenge (`error="invalid_token"` when a wrong token was
presented), increment `auth_failures_total{reason="missing"|"invalid"}` in `/_metrics`, and log a
structured warning. The token comparison is constant-time. An empty/non-printable token, a
set-but-not-UTF-8 `RR_AUTH_TOKEN`, or `--auth-protect-reads` without a token refuses startup
(fail-loud — a malformed token never silently disables auth); binding a non-loopback interface
*without* auth logs a startup warning.

**`POST /_backup` is privileged operator surface.** It writes a snapshot to an arbitrary
server-side `dest` path with the server process's filesystem permissions (UID), so it grants
filesystem-write on the host to anyone who can call it. It is in the default-deny set above and
**must stay behind auth** on any non-loopback bind — never expose it unauthenticated.

With **no token configured the server behaves exactly as before** (no auth — strictly opt-in). The
transport is plain HTTP either way: a bearer token is only as private as the link it crosses, so on
an untrusted network still front the server with a reverse proxy that terminates TLS. The *gRPC*
shard/control transports have their own mesh TLS and bearer-token configuration; see the
[`threat model`](../../../operations/threat-model.md).
