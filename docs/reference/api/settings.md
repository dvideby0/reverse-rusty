# Settings APIs

> [REST API hub](../api.md)

Inspect or change the live dynamic engine configuration.

| API | What it does | Availability |
|---|---|---|
| [`GET\|HEAD /_settings`](settings/read-settings.md) | Read current settings, optional defaults, and coordinator per-shard configuration. | Single-node and coordinator modes |
| [`PUT /_settings`](settings/update-settings.md) | Strictly publish a live-only dynamic settings patch. | Single-node; coordinator validates then returns 501 with the restart alternative |
