# ADR-118: Clause-boundary compiler semantics + durable migration

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted

- **Context.** Query extraction jointly normalizes consecutive positive bare terms so configured
  multi-word entities are recognized on the query side just as they are in titles. The implementation
  did not actually enforce “consecutive”: it accumulated every positive top-level bare term and
  flushed only after walking the whole AST. Any intervening negation, phrase, or any-of clause was
  skipped while the remaining terms were concatenated. With the active alias `new york ↔ ny`, the
  query `new -used york` was therefore compiled as required `term:new_york` plus forbidden `used`.
  A title such as `new vintage collectible york` satisfies the DSL but does not contain the
  contiguous alias entity, so its signatures cannot retrieve that query. This is a real violation of
  the lossless-cover contract, not merely an exact-matcher discrepancy. The hazard is broader than
  aliases: the fabricated stream can carry grader or number context across a clause too. Legacy
  `pop -used 1994`, for example, normalized the fabricated `pop 1994` stream and could type `1994`
  differently from a satisfying title where another token separates them. Both production extraction
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
  absence of aliases is not proof of equivalence because phrase consumption, grader state, and number
  context are also stream-sensitive.
  - standalone `Engine::open[_with_vocab]` replays the WAL, verifies a complete exact↔source corpus,
    interns every feature newly exposed by splitting the legacy stream, re-resolves equivalences,
    recompiles every live query, commits the expanded dict + current-stamped segment + manifest, and
    only then returns. The interning step prevents a migrated synthetic ID from diverging when a later
    standalone insert assigns the same name a dense ID. Missing/stale source or a failed durable
    commit refuses startup;
  - durable `ClusterEngine::open` temporarily attaches committed local segments inside the recovery
    transaction, replays the coordinator-log tail, performs the existing blue/green source rebuild
    under the same normalizer while re-minting the dict, re-places at one new generation, bumps the
    control document, and
    checkpoints the new registry atomically before returning;
  - durable shard self-restart refuses a legacy base. A shard-local rewrite cannot safely preserve a
    selectively placed row because splitting the fabricated feature can change its ring positions or
    visibility mode; only the coordinator can rebuild and commit the whole placement generation;
  - raw/shared segment attach and ordinary peer recovery from a still-legacy source likewise refuse.
    The coordinator's boot transaction has a private migration-only attach/copy seam, but it never
    publishes those rows and immediately replaces the complete cluster before returning.

- **Why this is safe.** The change is compile/recovery-only: title normalization, signature probing,
  exact verification, and the allocation-free match hot path are untouched. Splitting a fabricated
  entity back into the source query's actual positive runs restores the DSL predicate and its
  lossless signature cover. Migration always rebuilds from the complete raw source set; it never
  attempts to reverse-engineer source clauses from a compiled integer plan. Standalone and
  coordinator recovery retain their old commit point until the complete replacement is durable, so
  every crash point selects either the old base plus its log tail or the new base—never a partial
  mixture. Ownerless and shard-local paths fail closed instead of guessing at placement.

- **Proof.** The regression matrix covers boundaries formed by a negated term, negated phrase,
  negated any-of, positive phrase, and positive any-of through both mutable build and read-only
  recompile paths. The reference matcher has a direct unit matrix over the same cases. Persistence
  tests downgrade the header stamp and prove one-shot standalone migration, migration without any
  alias (number-context case), dense-ID stabilization across a later live insert, and fail-loud raw
  attach. Cluster coverage proves an RF=2 durable reopen rebuilds, bumps placement generation exactly
  once, and reopens idempotently. A durable-shard test proves self-restart refuses without consuming
  its unsealed translog tail or advancing `shard.ckpt`. The segment codec pins
  current/legacy/future stamp behavior.

- **Oracle follow-up.** ADR-087 remains code-independent, but this finding proved code independence
  is not the same as semantic independence when both implementations translate the same ambiguous
  prose the same way. The human-expectation regression is now the tiebreaker for this case;
  [issue #123](https://github.com/dvideby0/reverse-rusty/issues/123) tracks strengthening the
  reference around a semantic AST/model so shared lowering assumptions are harder to duplicate.
