# Documents APIs

> [REST API hub](../api.md) · Query language: [DSL reference](../dsl.md)

Register, retrieve, existence-check, and delete stored queries.

| API | What it does | Availability |
|---|---|---|
| [`PUT /_doc/{id}`](documents/put-document.md) | Atomically register, replace, or create-only a query with optional metadata and typed priority. | Single-node and coordinator modes |
| [`GET\|HEAD /_doc/{id}`](documents/get-document.md) | Retrieve a stored query with strict source filtering, or check existence without a body. | Single-node and coordinator modes |
| [`DELETE /_doc/{id}`](documents/delete-document.md) | Remove one logical query with explicit distributed partial-repair behavior. | Single-node and coordinator modes |
