# Server & shared behavior

> [REST API hub](../api.md)

Configuration and HTTP behavior that apply across endpoint categories.

Server concurrency, live settings, and segment-introspection behavior are governed by ADR-016,
ADR-022, and ADR-023; use the [architecture decision hub](../../DECISIONS.md) for rationale.

| Reference | What it covers |
|---|---|
| [Running and configuring the server](server/running.md) | Server command, flags, graceful shutdown, and ranking-profile loading. |
| [HTTP security](server/security.md) | Bind defaults, bearer-token protection, failure behavior, and the privileged backup boundary. |
| [`GET /` and `HEAD /`](server/api-root.md) | Product, version, connectivity, and coordinator identity response. |
| [Coordinator mode](server/coordinator-mode.md) | In-process and remote cluster assembly, mesh settings, cross-topology behavior, and cluster-only route overview. |

Endpoint-specific behavior belongs on the focused contract page in the corresponding API category.
