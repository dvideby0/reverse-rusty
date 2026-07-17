# ADR-067: Atomic upsert — `PUT /_doc/{id}` is replace-by-id (ES `index` semantics)

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted

- **Context.** [ADR-064](adr-064-percolator-drop-in-parity-audit.md) item 1, the first item of the
  drop-in-parity work package and the audit's headline *update* divergence: a re-PUT through
  `try_insert_live_with_tags` inserted a **second live copy without tombstoning the old one**, so the id
  kept matching under **either** version's semantics until an explicit DELETE (live-verified in the
  audit: re-PUT then DELETE reported `deleted_count: 2`) — while ES `index`, and the reference
  percolator write path, are replace-by-id. The documented workaround, DELETE-then-PUT, has a
  **no-match window** across two snapshot publishes — and a worse, latent one in the WAL: a crash
  between the two frames recovers the *deleted* state without the insert. This ADR builds directly on
  [ADR-066](adr-066-tombstone-durability-at-commit.md) (tombstone durability at the commit point) —
  without it, the upsert's tombstoned prior copies would resurrect after any flush + restart,
  re-creating the exact divergence this item closes.

- **Decision.** `PUT /_doc/{id}` becomes an **atomic upsert**: insert the new version and tombstone
  every prior live copy of the id, under **one** writer-lock critical section, **one** WAL frame, and
  **one** snapshot publish.
  - **Engine:** `Engine::try_upsert_live[_with_tags]` — parse (front-door limits; a malformed query
    never reaches the WAL and never deletes), append **one** `Upsert` WAL frame (op 4, WAL **v4**;
    payload byte-identical to Insert), then run the shared apply funnel `apply_upsert`: capture the
    prior live copies (segments + memtable, the delete funnel's reverse-index walk), insert the new
    version into the memtable, and — only if the insert was accepted — tombstone the captured copies
    and publish the new source text. Returns `UpsertOutcome::{Created, Updated{replaced},
    RejectedClassD}`.
  - **Failure atomicity:** a WAL append failure rejects the whole upsert (nothing applied, prior copies
    intact — the ADR-013 fail-closed rule). A **class-D rejection of the new version leaves the prior
    copies live**: capture-before-insert + tombstone-after-acceptance makes "a failed replace never
    deletes" structural (ES parity: a failed `index` op leaves the old document). Replay reaches the
    same class-D verdict (the classification is structural — no positive features), so live ≡ replay.
  - **Recovery:** the frame replays through the same funnel, governed by the ADR-066 watermark with a
    **split by state domain**: the *insert half always replays* (the new memtable copy exists only in
    this frame — a flush would have reset the WAL and dropped it), prior **memtable** copies are always
    re-tombstoned (they are WAL-truth, recreated by earlier replayed frames), and prior **segment**
    copies are tombstoned only for frames *above* the watermark (below it they are baked in the
    manifest's bitmaps — and a same-id query bulk-ingested *after* the frame lives in those segments
    and must not be erased; the ADR-066 ordering inversion, upsert edition).
  - **REST:** `put_doc` answers **201 `created`** for a fresh id and **200 `updated`** for a
    replacement (the ES status split), with `result` carrying the same word. Parse / class-D / WAL
    failures keep their existing 400/400/503 envelopes — now guaranteed to leave the prior version
    live and matchable.
  - **Discovered + fixed along the way — the fresh-path recovery gap.** The upsert tag tests exposed
    that `Engine::open` with **no manifest yet** returned a fresh engine *without replaying the
    existing WAL tail*: every acknowledged write a start-empty-and-PUT server had taken was silently
    lost on its first crash-restart (the suite even carried a workaround comment — "open replays the
    WAL only when one exists"). The fresh path now runs the **same replay loop** as the manifest path
    (watermark 0), completing ADR-013's stated recovery contract; no new ADR (the decision was made in
    ADR-013 — this honors it).

- **Why this is safe.** The upsert composes two already-proven mutations (insert + tombstone-by-id)
  inside the existing single-writer critical section; it touches neither signature gating nor the
  verifier, so the lossless-cover contract is untouched. Readers see the old version until the one
  publish, the new version after — never both, never neither. Crash recovery applies both halves or
  neither (one frame). The legacy multi-copy state (ids re-PUT before this ADR) is healed by the next
  upsert of that id: *every* prior live copy is captured and tombstoned, not just the newest.

- **Scope.** Single-node Engine + REST (`/_doc` PUT). `/_bulk` keeps its additive ingest semantics
  (the reference workload uses bulk for fresh loads; bulk replace-by-id would need per-item upsert
  through the WAL-less segment path — out of scope). The in-process `insert_live` /
  `try_insert_live*` APIs are unchanged (additive, the cluster's building block). **Cluster upsert is
  deferred** to the Distributed-v1 program (ADR-065 — the cluster REST surface lands first;
  `ClusterEngine::add_query` remains additive in this increment, its log replay being the cluster's
  own ordering problem). **Superseded for clustered data by ADR-109/110:** exact distributed bounded
  reduction makes the cluster method insert-only under a unique logical-id invariant; standalone
  engine ingestion remains additive, and cluster replacement uses `upsert_query`.

- **Alternatives.** (1) *DELETE-then-PUT inside the handler* — rejected: two WAL frames (the crash
  window between them recovers the delete without the insert), two engine mutations with an
  observable empty window unless the publish is suppressed, and `deleted` events for what is
  semantically an update. (2) *Reuse the Insert frame + an `is_upsert` flag byte* — rejected: a v3
  reader would misparse the flag as the tag section's first bytes; a new op code is the
  precedent-consistent additive change (ADR-066). (3) *Tombstone-then-insert order in the funnel* —
  rejected: a class-D rejection of the new version would already have deleted the old one; capturing
  first and tombstoning after acceptance makes the never-deletes-on-failure property structural
  rather than requiring an undo path.

- **Testing.** `tests/persistence/upsert.rs` (9): the ADR-064 acceptance pin (re-PUT a *narrower*
  query — old semantics stop matching immediately; `deleted_count` back to 1), legacy multi-copy heal,
  crash atomicity from a bare WAL tail, flush + reopen (the ADR-066 substrate), upsert → bulk-same-id →
  crash (segment half skipped, insert half replayed), the memtable-prior re-tombstone despite the
  watermark (the state-domain split), class-D rejection leaving the old version live across a crash,
  the sequence re-pin after reopen-with-reset-WAL, and tags surviving recovery (filtered percolation
  reaches the replacement, not the replaced). `tests/persistence/wal.rs`:
  `writes_before_first_manifest_survive_crash` (the fresh-path gap pin — inserts, an upsert, and a
  delete recovered with no manifest). Handler tests (`handlers/doc/tests.rs`): 201-created /
  200-updated split, snapshot flip with no either-version window, `deleted_count: 1` after a re-PUT,
  and rejected re-PUTs (parse + class D) leaving the old version live. WAL unit test: the Upsert frame
  round-trips with tags alongside legacy ops. Full suite + `check.sh` green.

- **Consequences.** `PUT /_doc/{id}` now means what ES callers expect: replace-by-id, atomically, with
  the 201/200 created/updated split — closing ADR-064 item 1 and removing the audit's headline write
  divergence. The DELETE-then-PUT recipe is obsolete (and its crash window gone). A failed re-PUT
  leaves the old version serving. Fresh-start servers no longer lose their WAL on a pre-first-flush
  crash. Remaining ADR-064 items (2–6) continue under their own ADRs; ADR-064 item 5 (`maybe_flush`
  on the REST PUT path) now applies to the upsert path when it lands.
