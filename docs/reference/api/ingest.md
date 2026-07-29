# Ingest & lifecycle APIs

> [REST API hub](../api.md)

Bulk mutation and durable segment lifecycle operations.

| API | What it does | Availability |
|---|---|---|
| [`POST /_bulk`](ingest/bulk.md) | Strict NDJSON index/create batch with ordered per-item outcomes. | Single-node and coordinator modes |
| [`GET\|POST /_flush`](ingest/flush.md) | Publish memtables with strict force/wait controls and shard results. | Single-node and coordinator modes |
| [`POST /_checkpoint`](ingest/checkpoint.md) | Seal and commit a cluster durability boundary. | Coordinator mode |
| [`POST /_compact` and `POST /_forcemerge`](ingest/compaction.md) | Run native force-all compaction or its strict compatibility alias. | Single-node only |
| [`POST /_backup`](ingest/backup.md) | Snapshot a durable single engine or in-process cluster to a fresh server-side directory. | Durable local modes; remote coordinator returns 400 |
