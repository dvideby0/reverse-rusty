# ADR-139: Backup REST API contract — strict native boundary and no-clobber commit

> [Ingestion, storage & durability decisions](areas/ingestion-storage-and-durability.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

- **Context.** `POST /_backup` already produced oracle-proven, manifest-selected, verified backups
  (ADR-079), but its HTTP boundary had no route-level tests. It silently ignored query parameters
  and unknown JSON fields, inherited the 100 MiB ingest body limit, returned different
  standalone/coordinator timing shapes, and performed a potentially multi-second copy directly on
  a Tokio runtime worker. Storage refusal used `Path::exists()`, which misses a dangling symlink,
  while the final ordinary rename could replace an entry created after the precheck. A deterministic
  staging name also let independent processes targeting the same destination interfere with one
  another.

- **Compatibility boundary.** Elasticsearch and OpenSearch expose
  `PUT`/`POST /_snapshot/{repository}/{snapshot}` after a repository is registered; creation is
  asynchronous by default and integrates with snapshot/task status
  ([Elasticsearch create snapshot](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-snapshot-create),
  [OpenSearch create snapshot](https://docs.opensearch.org/latest/api-reference/snapshots/create-snapshot/)).
  Reverse Rusty has no repository registry, named-snapshot catalog, selective-index snapshot, or
  snapshot-task status API. Its operation instead takes one arbitrary fresh server-side path and
  waits for a complete verified copy. A `/_snapshot` alias would therefore invent repository and
  asynchronous semantics; the endpoint remains explicitly native.

- **Decision — strict shared transport.** Standalone and coordinator modes accept POST only, no
  query parameters, and one JSON object containing exactly one non-empty, non-NUL `dest`. The route
  has a 64 KiB limit, preserves structured 413 extraction, requires JSON content type, rejects
  malformed/duplicate/unknown fields before writer admission, and returns structured 405 with
  `Allow: POST`. Existing storage classifications remain fail-loud: non-durable/existing
  destination is 400, standalone persistence degradation is 503, and copy/verification failures
  are server errors.

- **Decision — synchronous truthful result.** Success still waits for source validation, copy,
  staged verification, and destination promotion. Both modes return integer `took`, precise
  `took_ms`, `acknowledged:true`, and the echoed server-side `dest`; an in-process cluster also
  returns the checkpoint `epoch` created by that backup. Both modes now account for backup request
  count and duration consistently.

- **Decision — execution.** Copy and verification run in `spawn_blocking`, including the
  in-process cluster's checkpoint. The blocking closure owns the same standalone engine mutex or
  cluster writer-serialization plus read-lock scope required by ADR-079, so no mutation can retire
  a selected file between manifest read and copy. Reads continue from published snapshots. Each
  server has one backup-admission permit, acquired before spawning and moved into the blocking
  closure. Excess calls wait asynchronously and disappear cleanly if cancelled while waiting.
  Once admitted, disconnecting the HTTP future neither cancels the backup nor releases admission
  early; this avoids both an HTTP cancellation seam inside filesystem commit and an unbounded
  detached blocking-worker queue. An independently spawned completion reporter owns the blocking
  join handle and records the final log plus status metric before forwarding the outcome to a
  still-connected handler, so a detached disk/corruption/promotion failure is never silent.

- **Decision — destination and staging ownership.** Every operation atomically reserves a unique
  sibling `<dest>.backup.tmp.<pid>.<sequence>` directory. It never removes or writes another
  process's staging tree. Destination preflight uses `symlink_metadata`, so a dangling symlink is
  occupied. On Linux/Android and Apple platforms, final promotion uses the OS no-replace rename
  flag; an entry that appears after preflight yields `DestExists` and remains untouched. If the
  platform, kernel, or target filesystem cannot provide atomic no-replace rename, promotion fails
  closed rather than falling back to a check-then-standard-rename race. A failed pre-commit attempt
  removes only its own staging tree; a process crash can leave that uniquely named tree for
  operator cleanup but never a half-populated final destination.

- **Safety and proof.** The manifest/WAL/checkpoint selection, verification, restore path, and write
  exclusion from ADR-079 are unchanged, so match-set and recovery equivalence remain owned by the
  persistence and cluster durability oracles. New storage tests pin dangling-symlink refusal,
  no-clobber final promotion, unique staging reservations, and failure cleanup. New standalone and
  coordinator route tests pin strict validation, body bounds, stable precondition errors, common
  timing fields, cluster epoch reporting, verified output, runtime responsiveness, bounded
  pre-spawn admission, completion after a dropped admitted request, and status accounting for a
  detached failure.
