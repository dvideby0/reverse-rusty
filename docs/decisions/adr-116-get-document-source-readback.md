# ADR-116: Get-document source read-back and the `sources.dat` metadata footer

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted

- **Context.** `GET /_doc/{id}` returned only the query DSL. Metadata accepted by `PUT` was
  write-only over REST, the response omitted the familiar ES/OS `_index` and `_version` fields,
  common `_source` projections were silently ignored, and `HEAD` behavior was accidental and
  untested. The parity audit tracked tag read-back as its remaining document-API polish item
  (ADR-064). Reconstructing tags only from `TagId`s is incomplete: post-freeze cluster tags use
  one-way synthetic ids.
- **Decision.** The source store now retains a canonical document: query text, write version, an
  engine-owned non-zero **source generation**, and the validated/scalar-coerced raw `(key,value)`
  tags. The existing `sources.dat` **v2** query index/blob remains unchanged and gains a marked
  metadata directory/blob plus a fixed footer before its CRC. Metadata-footer v2 adds the generation;
  footer v1 remains readable as legacy generation zero. Query-only hit enrichment and its
  pre-allocation byte bound therefore do not decode or allocate tags. Resident and lazy-overlay modes
  share the same representation; flush/checkpoint, WAL replay, cluster build, resize, and vocabulary
  rebuild all carry the raw metadata.

  Every engine-owned accepted write reserves a generation independent of the caller-visible
  `_version` and stores it in both durable domains. `.seg` **v8** appends the exact-row generation
  column; standalone engine manifest **v6** is the loud rollback fence (otherwise legacy recovery
  could skip an unreadable segment and serve a partial corpus). Public low-level segment builders and
  pre-v8 files use generation zero. Reopen seeds the counter above both the exact and source maxima;
  compaction and in-place vocabulary recompilation preserve it. Content-fingerprint and blue/green
  rebuild gathers first require source version/generation agreement and complete raw tags, so peer
  copy elision or re-placement cannot bless stale sidecar data. Exact metadata resolution chooses the
  greatest live generation across the memtable and every base segment—not storage-tier order—because
  supported additive live-then-bulk histories can put the newer row in a base segment. Lazy open
  validates tag encoding and UTF-8 without allocating owned tag strings, and query-only winner
  enrichment reads only the original query index/blob.
- **REST contract.** Found documents return `_index: "queries"`, numeric `_id`, the stored
  `_version`, `found: true`, and canonical `_source` (`query` plus a `tags` object when tagged).
  Missing documents return 404 with `_index`, `_id`, and `found: false`. `_source=false`,
  `_source_includes`, and `_source_excludes` (including singular aliases and `*`/`?` patterns) are
  supported; unknown parameters reject with 400. `HEAD /_doc/{id}` is a documented, tested
  bodyless 200/404 existence check over exact-index liveness, independent of source-sidecar health.
  Single-node and in-process coordinator modes are identical;
  remote v1 coordinators retain the existing loud 501 because the richer source shape is not on
  the gRPC wire.
- **Compatibility.** v1 files migrate to extended v2 and original v2 files read unchanged. Their
  query text remains available only with a pre-v8 exact row (both sides carry legacy generation
  zero); their unknown write version is recovered from that live row, and dense legacy tags are
  reconstructed through the persisted `TagDict`. A footer-backed source whose stored version **or
  internal generation** disagrees with the greatest live exact generation fails with
  `source_unavailable` rather than combining writes—even when both client versions are `1`. A
  pre-footer synthetic tag has no
  reversible string; that rare response is explicitly marked
  `_source_metadata.complete: false` until re-PUT, never silently presented as complete. A live
  exact row whose source sidecar is missing fails with `source_unavailable` rather than masquerading
  as a 404; HEAD still answers from index liveness. The v2 CRC and atomic rename discipline are
  unchanged.
- **Safety and cost.** Source metadata remains outside candidate gating, exact verification, and
  the title hot path, so the lossless-cover contract is untouched. Owned metadata is decoded only
  for a document point-read or a rebuild gather; lazy-open validation is allocation-free.
  Query-only search enrichment still clones exactly the bounded query bytes it did before and does
  not touch metadata pages.
- **Proof.** Handler tests pin found/missing envelopes, version and canonical tag read-back,
  projections, source suppression, unknown-parameter rejection, and HEAD in both local modes.
  Persistence tests pin v1 migration, original-v2 and metadata-footer-v1 compatibility, old-reader
  query visibility, resident/lazy metadata-footer round-trips, `.seg` v8 and manifest-v6 fencing, and
  a same-client-version stale-sidecar reopen refusal. Snapshot and gather units pin same-version
  cross-generation refusal, additive live-then-bulk generation selection, and atomic rejection of
  vocabulary changes that make an acknowledged query unrebuildable. Cluster durability tests pin
  metadata across checkpoint/reopen and across a synthetic-tag reopen plus vocabulary rebuild.
