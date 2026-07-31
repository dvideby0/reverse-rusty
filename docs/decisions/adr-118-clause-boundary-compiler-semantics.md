# ADR-118: Clause-boundary compiler semantics + durable migration

> [Distributed v1 — the ADR-065 graduation program decisions](areas/distributed-v1-graduation.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

- **Context.** Query extraction jointly normalizes consecutive positive bare terms so configured
  multi-word entities are recognized on the query side just as they are in titles. The implementation
  did not actually enforce “consecutive”: it accumulated every positive top-level bare term and
  flushed only after walking the whole AST. Any intervening negation, phrase, or any-of clause was
  skipped while the remaining terms were concatenated. With the active alias `new york ↔ ny`, the
  query `new -used york` was therefore compiled as required `term:new_york` plus forbidden `used`.
  A title such as `new compact device york` satisfies the DSL but does not contain the
  contiguous alias entity, so its signatures cannot retrieve that query. This is a real violation of
  the lossless-cover contract, not merely an exact-matcher discrepancy. The hazard is broader than
  aliases: the fabricated stream can carry caller-defined number context across a clause too.
  With `model` configured as numeric context, `model -used 1994` could incorrectly normalize the
  fabricated `model 1994` stream differently from a satisfying title where another token separates
  them. Both production extraction
  paths and the code-independent reference matcher had copied the same interpretation of the prose,
  so the existing differential stayed green.

- **Decision — maximal positive runs.** Mutable `extract`, frozen-dict `extract_readonly`, and the
  reference matcher now normalize each **maximal uninterrupted run of positive bare terms**
  independently. Every other clause is a hard boundary: a negated term/phrase/any-of, a positive
  phrase, or a positive any-of flushes the current run before that clause is lowered. Thus
  `new york` is still jointly normalized and can recognize the configured entity, while
  `new -used york` produces separate required `new` and `york` predicates. Forbidden features remain
  structurally separate and never become candidate gates.

- **Decision — identify durable compiler semantics.** Segment header bytes `12..16`, previously a
  reserved zero word, now carry `compiler_semantics_version`: zero means the legacy global gathering
  and one means maximal-run lowering. This is deliberately separate from the cumulative segment
  layout version. An old reader can safely execute a query compiled by the new lowering, but the new
  reader must identify old materializations that may require source-driven recompilation. Mechanical
  compaction and re-anchoring preserve the minimum source semantics version; only re-extraction from
  raw DSL produces the current stamp. Unknown future semantics fail loud.

- **Decision — upgrade before serving.** Every live semantics-zero materialization is rebuilt;
  absence of aliases is not proof of equivalence because phrase consumption, alias state, and number
  context are also stream-sensitive.
  - standalone `Engine::open[_with_vocab]` replays the WAL, verifies a complete exact↔source corpus,
    interns every feature newly exposed by splitting the legacy stream, re-resolves equivalences,
    recompiles every live query, commits the expanded dict + current-stamped segment + manifest, and
    only then returns. The interning step prevents a migrated synthetic ID from diverging when a later
    standalone insert assigns the same name a dense ID. The migration refuses a degraded/partially
    attached recovery before writing anything, and refuses multiple live physical predicates sharing
    one logical ID because the canonical one-document source sidecar cannot losslessly reconstruct
    those distinct rows. A crash after the old manifest rename but before its WAL checkpoint can
    expose the same insert/upsert in both a committed segment and the recoverable WAL prefix; replay
    recognizes that exact `(logical id, source generation)` mutation and materializes it only once.
    It does not skip all frames below the manifest watermark, because a compaction can watermark an
    unrelated insert that remains memtable/WAL-only. WAL-replayed source overlays are durably written
    before the replacement manifest captures them and resets the WAL. Missing/stale/ambiguous source
    or a failed durable commit refuses startup;
  - `open_with_vocab` installs transient equivalence groups before replay and migration. The
    compatibility `open(normalizer)` + later `adopt_vocab` path conservatively forces one
    equivalence-aware rebuild for every non-empty recovered corpus. A process-local migration flag
    would be unsafe across a stop between migration commit and adoption; `open_with_vocab` is the
    efficient normal path;
  - durable `ClusterEngine::open` temporarily attaches committed local segments inside the recovery
    transaction, folds the coordinator-log tail by logical ID without first interpreting its legacy
    placement, and performs the blue/green source rebuild under the same normalizer. The compiler
    migration extends the persisted feature dictionary only with newly exposed component features;
    every existing frequency and top-64 mask bit remains frozen. Re-ranking from the post-delete live
    corpus could otherwise move an unrelated default-visible class-A query behind class C's
    `include_broad` boundary. The rebuild re-places the corpus exactly once at one new generation,
    bumps the control document, and checkpoints the new registry atomically before returning.
    Replaying the tail through the current placement validator first would reject a valid legacy write
    whose clause-boundary fix changes its target. Raw tags in that already-acknowledged tail are
    resolved against the persisted frozen tag dictionary before the rebuild, marking them as stored
    carry-through rather than fresh ingestion. Tightening `max_tags` therefore cannot make migration
    omit a previously accepted tagged row;
  - cluster manifest v7 records the compiler-semantics stamp independently of its segment registry,
    so an empty checkpoint base plus a non-empty legacy coordinator-log tail still rebuilds before
    serving. The same manifest selects one generation-named source sidecar per shard. A blue/green
    rebuild writes only those new files and atomically selects them with the new segment registry;
    failure before that commit leaves the old manifest and old source corpus intact and retryable.
    The manifest reader validates the registry, next-segment-id, and source-file columns against
    `num_shards` before any backup/open caller indexes them. Backups copy exactly the selected
    sidecars. Superseded source files are benign orphans;
  - durable shard checkpoint v2 records the same compiler-semantics stamp and its selected source
    sidecar, covering an empty base plus a retained translog tail. Durable shard self-restart refuses
    a legacy checkpoint before attaching segments or replaying the tail. A shard-local rewrite cannot
    safely preserve a selectively placed row because splitting the fabricated feature can change its
    ring positions or visibility mode; only the coordinator can rebuild and commit the whole
    placement generation;
  - raw/shared segment attach and ordinary peer recovery from a still-legacy source likewise refuse.
    The coordinator's boot transaction has a private migration-only attach/copy seam, but it never
    publishes those rows and immediately replaces the complete cluster before returning. A compiler
    semantics value newer than this binary understands is an unsupported compatibility fence and is
    fatal to standalone open too—it is never treated as ordinary skippable corruption;
  - the distributed dict-adoption, add-shard, fingerprint, and peer-recovery exchanges attest the
    current compiler-semantics version. A missing protobuf field reads as legacy zero and fails
    before state adoption, so a mixed compiler mesh cannot silently create incompatible placement.
    The recovery receiver validates the mandatory first manifest frame before opening or renaming
    any target file, so refusing an old peer cannot damage the target's existing durable commit;
  - source-driven rebuild and WAL replay parse acknowledged DSL with the durable format's structural
    ceilings (`u32` text length and `u16` clause/group counts), not today's runtime policy or default.
    Coordinator-log and shard-translog apply paths use the same rule and fail loud on structural
    corruption rather than silently skipping a row. Tightening settings—or having originally
    accepted a supported value above the default—therefore cannot make recovery discard or reject an
    acknowledged query.

- **Decision — keep runtime and durable state coherent on failed source persistence.** A persistent
  standalone vocabulary change refuses to start when durability is already degraded. If writing the
  replacement source sidecar fails after successful recompilation, the engine still installs the
  complete green segment in memory so the new normalizer and exact plans agree; it marks persistence
  degraded and does not advance the manifest or reset the WAL. Restart therefore selects the old
  durable state, while the still-running process never serves a new normalizer over stale exact plans.

- **Why this is safe.** The change is compile/recovery-only: title normalization, signature probing,
  exact verification, and the allocation-free match hot path are untouched. Splitting a fabricated
  entity back into the source query's actual positive runs restores the DSL predicate and its
  lossless signature cover. Migration always rebuilds from the complete raw source set; it never
  attempts to reverse-engineer source clauses from a compiled integer plan. Standalone and
  coordinator recovery retain their old commit point until the complete replacement is durable, so
  every crash point selects either the old base plus its log tail and source corpus or the new base
  plus its generation-selected source corpus—never a partial mixture. Compiler-only rebuilds also
  retain the old mask/visibility boundary while appending missing dense features. Ownerless,
  shard-local, and mixed-wire paths fail closed instead of guessing at placement.

- **Proof.** The regression matrix covers boundaries formed by a negated term, negated phrase,
  negated any-of, positive phrase, and positive any-of through both mutable build and read-only
  recompile paths. The reference matcher has a direct unit matrix over the same cases. Persistence
  tests downgrade the header stamp and prove one-shot standalone migration, migration without any
  alias (number-context case), dense-ID stabilization across a later live insert, equivalence-aware
  `open` + `adopt_vocab`, genuine duplicate-row refusal, manifest-captured insert/upsert WAL
  de-duplication while a watermarked memtable-only insert still replays, no subset commit after a
  corrupt-segment skip, WAL-tail source survival across a second reopen, recovery above the default
  clause limit, and fail-loud raw attach/future semantics, plus coherent in-memory matching after a
  source write failure. Manifest units reject mismatched per-shard columns before backup code can
  index them. Cluster coverage proves an RF=2 durable reopen rebuilds, bumps placement generation
  exactly once, and reopens idempotently; folds a legacy placement-divergent tail before validation;
  migrates an empty base with a tail; preserves a tagged tail accepted above the reopened
  `max_tags`; preserves a rank-65 query's default visibility after deletes by retaining the frozen
  mask; and preserves the old source corpus across a failed manifest commit so the next open can
  retry. Durable-shard tests prove self-restart refuses both a legacy segment base and an empty legacy
  base with an unsealed translog tail without advancing `shard.ckpt`, and replays an acknowledged
  query above today's default clause limit. Distributed units pin fail-closed compiler-semantics
  handshakes and manifest refusal before target-file processing.

- **Oracle follow-up.** ADR-087 remains code-independent, but this finding proved code independence
  is not the same as semantic independence when both implementations translate the same ambiguous
  prose the same way. The human-expectation regression remains the tiebreaker, and
  [issue #123](https://github.com/rusty-ports/reverse-rusty/issues/123) subsequently replaced the
  reference's proxy-shaped extraction with a direct semantic clause tree so production lowering
  assumptions are absent by construction.
